// Package pipeline encadena las tres etapas del ETL: censo de jugadores por
// tier, descubrimiento de ids de partida y descarga del detalle.
//
// Las etapas 2 y 3 corren solapadas: en cuanto un jugador devuelve ids, los
// trabajadores de descarga ya estan tirando de ellos. El limitador de rate es
// el unico que marca el ritmo, asi que sobra con tener goroutines de mas
// esperando turno: ninguna se adelanta a los limites del host.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/javierlog08/lol-etl-go/internal/config"
	"github.com/javierlog08/lol-etl-go/internal/riot"
	"github.com/javierlog08/lol-etl-go/internal/store"
)

type Options struct {
	Regions         []string
	Tiers           []string
	Queue           int    // 420 ranked solo; 0 = todas
	QueueName       string // RANKED_SOLO_5x5, para league-v4
	Patch           string // "16.20"; vacio = no filtrar
	Since           time.Duration
	Timelines       bool
	DiscoverWorkers int
	FetchWorkers    int
	MaxPlayers      int // tope del censo por region, 0 = sin tope
	DiscoverLimit   int // tope de jugadores a consultar por region, 0 = todos
	DryRun          bool
}

type Stats struct {
	Players         atomic.Int64
	IDsSeen         atomic.Int64
	IDsNew          atomic.Int64
	MatchesStored   atomic.Int64
	TimelinesStored atomic.Int64
	OffPatch        atomic.Int64
	Errors          atomic.Int64
}

type Runner struct {
	client *riot.Client
	store  *store.Store
	log    *slog.Logger
	opts   Options
	Stats  Stats
}

func New(c *riot.Client, s *store.Store, log *slog.Logger, o Options) *Runner {
	if o.DiscoverWorkers <= 0 {
		o.DiscoverWorkers = 16
	}
	if o.FetchWorkers <= 0 {
		o.FetchWorkers = 24
	}
	if o.QueueName == "" {
		o.QueueName = "RANKED_SOLO_5x5"
	}
	return &Runner{client: c, store: s, log: log, opts: o}
}

// ---------- etapa 1: censo ----------

// Census recorre league-v4 por tier y division y deja el padron en players,
// con el tier etiquetado en el momento de la captura.
func (r *Runner) Census(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(r.opts.Regions))

	for _, region := range r.opts.Regions {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			if err := r.censusRegion(ctx, region); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}(region)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

func (r *Runner) censusRegion(ctx context.Context, region string) error {
	reg := config.Regions[region]
	seen := 0

	for _, tier := range r.opts.Tiers {
		if config.IsApex(tier) {
			entries, err := r.client.ApexEntries(ctx, reg.Platform, config.ApexEndpoint(tier), r.opts.QueueName)
			if err != nil {
				return err
			}
			n, err := r.flushPlayers(ctx, region, tier, entries)
			if err != nil {
				return err
			}
			seen += n
			r.log.Info("censo apex", "region", region, "tier", tier, "jugadores", n)
			continue
		}

		for _, div := range config.Divisions {
			for page := 1; ; page++ {
				entries, err := r.client.LeagueEntries(ctx, reg.Platform, r.opts.QueueName, tier, div, page)
				if err != nil {
					return err
				}
				if len(entries) == 0 {
					break
				}
				n, err := r.flushPlayers(ctx, region, tier, entries)
				if err != nil {
					return err
				}
				seen += n
				if page%25 == 0 {
					r.log.Info("censo", "region", region, "tier", tier, "div", div,
						"pagina", page, "acumulado", seen)
				}
				if r.opts.MaxPlayers > 0 && seen >= r.opts.MaxPlayers {
					r.log.Info("censo cortado por --max-players", "region", region, "jugadores", seen)
					return nil
				}
			}
			r.log.Info("censo division lista", "region", region, "tier", tier, "div", div, "acumulado", seen)
		}
	}
	_ = r.store.SaveProgress(ctx, "census:"+region, bson.D{
		{Key: "tiers", Value: r.opts.Tiers},
		{Key: "players", Value: seen},
	})
	return nil
}

func (r *Runner) flushPlayers(ctx context.Context, region, tier string, entries []riot.LeagueEntry) (int, error) {
	ps := make([]store.Player, 0, len(entries))
	for _, e := range entries {
		if e.PUUID == "" {
			continue
		}
		t := e.Tier
		if t == "" {
			t = tier
		}
		ps = append(ps, store.Player{
			PUUID: e.PUUID, Region: region, Tier: t, Rank: e.Rank,
			LeaguePoints: e.LeaguePoints, Wins: e.Wins, Losses: e.Losses,
			Inactive: e.Inactive, Queue: r.opts.QueueName,
		})
	}
	if r.opts.DryRun {
		r.Stats.Players.Add(int64(len(ps)))
		return len(ps), nil
	}
	if _, err := r.store.UpsertPlayers(ctx, ps); err != nil {
		return 0, err
	}
	r.Stats.Players.Add(int64(len(ps)))
	return len(ps), nil
}

// ---------- etapas 2 y 3: descubrir y descargar ----------

type idJob struct {
	matchID  string
	regional string
}

// Crawl descubre ids de los jugadores pendientes del censo y descarga las
// partidas que no estan ya en la base.
func (r *Runner) Crawl(ctx context.Context) error {
	startTime := int64(0)
	if r.opts.Since > 0 {
		startTime = time.Now().Add(-r.opts.Since).Unix()
	}

	idsCh := make(chan idJob, 4096)
	fetchCh := make(chan idJob, 4096)

	// filtro: deduplica en memoria y descarta de un tiron lo que ya esta en Mongo
	var filterWG sync.WaitGroup
	filterWG.Add(1)
	go func() {
		defer filterWG.Done()
		defer close(fetchCh)
		r.filterIDs(ctx, idsCh, fetchCh)
	}()

	// descargadores
	var fetchWG sync.WaitGroup
	docsCh := make(chan writeItem, 512)
	for i := 0; i < r.opts.FetchWorkers; i++ {
		fetchWG.Add(1)
		go func() {
			defer fetchWG.Done()
			for job := range fetchCh {
				r.fetchOne(ctx, job, docsCh)
			}
		}()
	}

	// escritor unico: agrupa en lotes para no pelear por el pool de Mongo
	var writeWG sync.WaitGroup
	writeWG.Add(1)
	go func() {
		defer writeWG.Done()
		r.writer(ctx, docsCh)
	}()

	// productores de ids, uno por region
	var discWG sync.WaitGroup
	for _, region := range r.opts.Regions {
		discWG.Add(1)
		go func(region string) {
			defer discWG.Done()
			if err := r.discoverRegion(ctx, region, startTime, idsCh); err != nil &&
				!errors.Is(err, context.Canceled) {
				r.log.Error("descubrimiento", "region", region, "err", err)
				r.Stats.Errors.Add(1)
			}
		}(region)
	}

	discWG.Wait()
	close(idsCh)
	filterWG.Wait()
	fetchWG.Wait()
	close(docsCh)
	writeWG.Wait()
	return ctx.Err()
}

func (r *Runner) discoverRegion(ctx context.Context, region string, startTime int64, out chan<- idJob) error {
	reg := config.Regions[region]
	const batch = 500
	queried := 0

	for {
		limit := int64(batch)
		if r.opts.DiscoverLimit > 0 {
			remaining := r.opts.DiscoverLimit - queried
			if remaining <= 0 {
				r.log.Info("descubrimiento cortado por -discover-limit",
					"region", region, "jugadores", queried)
				return nil
			}
			limit = int64(min(batch, remaining))
		}
		pending, err := r.store.PlayersPending(ctx, region, r.opts.Tiers, r.opts.Patch, limit)
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}
		queried += len(pending)

		var wg sync.WaitGroup
		sem := make(chan struct{}, r.opts.DiscoverWorkers)
		for _, puuid := range pending {
			wg.Add(1)
			sem <- struct{}{}
			go func(puuid string) {
				defer wg.Done()
				defer func() { <-sem }()
				ids, err := r.client.MatchIDs(ctx, reg.Regional, puuid, r.opts.Queue, 0, 100, startTime)
				if err != nil {
					if !errors.Is(err, riot.ErrNotFound) && !errors.Is(err, context.Canceled) {
						r.log.Warn("ids", "puuid", puuid[:8], "err", err)
						r.Stats.Errors.Add(1)
					}
					return
				}
				r.Stats.IDsSeen.Add(int64(len(ids)))
				for _, id := range ids {
					select {
					case out <- idJob{matchID: id, regional: reg.Regional}:
					case <-ctx.Done():
						return
					}
				}
			}(puuid)
		}
		wg.Wait()

		if !r.opts.DryRun {
			if err := r.store.MarkDiscovered(ctx, pending, r.opts.Patch); err != nil {
				return err
			}
		} else {
			// en dry-run no se marca nada, asi que no hay siguiente pagina que pedir
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// filterIDs deduplica contra lo visto en esta corrida y contra la base, en
// lotes, para no hacer una consulta por id.
func (r *Runner) filterIDs(ctx context.Context, in <-chan idJob, out chan<- idJob) {
	seen := make(map[string]struct{}, 1<<16)
	buf := make([]idJob, 0, 500)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		ids := make([]string, len(buf))
		for i, j := range buf {
			ids[i] = j.matchID
		}
		known, err := r.store.ExistingIDs(ctx, "matches", ids)
		if err != nil {
			r.log.Error("consulta de existentes", "err", err)
			r.Stats.Errors.Add(1)
			known = map[string]struct{}{}
		}
		var tl map[string]struct{}
		if r.opts.Timelines {
			tl, err = r.store.ExistingIDs(ctx, "timelines", ids)
			if err != nil {
				tl = map[string]struct{}{}
			}
		}
		for _, j := range buf {
			_, haveMatch := known[j.matchID]
			_, haveTL := tl[j.matchID]
			if haveMatch && (!r.opts.Timelines || haveTL) {
				continue // nada que traer de esta partida
			}
			r.Stats.IDsNew.Add(1)
			if r.opts.DryRun {
				// Medir volumen no debe costar lo mismo que descargarlo: en
				// dry-run se cuentan los ids nuevos y no se pide el detalle.
				continue
			}
			select {
			case out <- j:
			case <-ctx.Done():
				return
			}
		}
		buf = buf[:0]
	}

	for {
		select {
		case j, ok := <-in:
			if !ok {
				flush()
				return
			}
			if _, dup := seen[j.matchID]; dup {
				continue
			}
			seen[j.matchID] = struct{}{}
			buf = append(buf, j)
			if len(buf) >= 500 {
				flush()
			}
		case <-ctx.Done():
			return
		}
	}
}

type writeItem struct {
	coll string
	doc  bson.D
}

func (r *Runner) fetchOne(ctx context.Context, job idJob, out chan<- writeItem) {
	raw, err := r.client.Match(ctx, job.regional, job.matchID)
	if err != nil {
		if !errors.Is(err, riot.ErrNotFound) && !errors.Is(err, context.Canceled) {
			r.log.Warn("match", "id", job.matchID, "err", err)
			r.Stats.Errors.Add(1)
		}
		return
	}
	if r.opts.Patch != "" && !matchesPatch(raw, r.opts.Patch) {
		r.Stats.OffPatch.Add(1)
		return
	}
	doc, err := store.RawDoc(raw)
	if err != nil {
		r.Stats.Errors.Add(1)
		return
	}
	select {
	case out <- writeItem{coll: "matches", doc: doc}:
	case <-ctx.Done():
		return
	}

	if !r.opts.Timelines {
		return
	}
	traw, err := r.client.Timeline(ctx, job.regional, job.matchID)
	if err != nil {
		if !errors.Is(err, riot.ErrNotFound) && !errors.Is(err, context.Canceled) {
			r.Stats.Errors.Add(1)
		}
		return
	}
	tdoc, err := store.RawDoc(traw)
	if err != nil {
		r.Stats.Errors.Add(1)
		return
	}
	select {
	case out <- writeItem{coll: "timelines", doc: tdoc}:
	case <-ctx.Done():
	}
}

func (r *Runner) writer(ctx context.Context, in <-chan writeItem) {
	const flushAt = 100
	buckets := map[string][]bson.D{}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	flush := func(coll string) {
		docs := buckets[coll]
		if len(docs) == 0 {
			return
		}
		n, err := r.store.UpsertRaw(ctx, coll, docs)
		if err != nil {
			r.log.Error("escritura", "coll", coll, "err", err)
			r.Stats.Errors.Add(1)
		} else if coll == "matches" {
			r.Stats.MatchesStored.Add(int64(n))
		} else {
			r.Stats.TimelinesStored.Add(int64(n))
		}
		buckets[coll] = docs[:0]
	}

	for {
		select {
		case it, ok := <-in:
			if !ok {
				for coll := range buckets {
					flush(coll)
				}
				return
			}
			buckets[it.coll] = append(buckets[it.coll], it.doc)
			if len(buckets[it.coll]) >= flushAt {
				flush(it.coll)
			}
		case <-ticker.C:
			for coll := range buckets {
				flush(coll)
			}
		case <-ctx.Done():
			return
		}
	}
}

// matchesPatch compara el prefijo de gameVersion ("16.20.1.x") con el parche
// pedido sin deserializar el documento entero.
func matchesPatch(raw json.RawMessage, patch string) bool {
	var probe struct {
		Info struct {
			GameVersion string `json:"gameVersion"`
		} `json:"info"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	return strings.HasPrefix(probe.Info.GameVersion, patch+".")
}
