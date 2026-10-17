// Command etl baja data publica del API de Riot a la MongoDB del proyecto
// cabecera. Tres subcomandos:
//
//	census  — padron de jugadores por region y rango de elo (league-v4)
//	crawl   — ids de partida de los jugadores del padron + detalle (match-v5)
//	run     — census y luego crawl
//	stats   — que hay ya en la base, por parche y coleccion
//
// Ejemplos:
//
//	etl run    -regions LA1,NA1 -tiers DIAMOND-CHALLENGER -timelines
//	etl census -regions EUW1,KR -tiers GOLD-MASTER
//	etl crawl  -regions LA1 -tiers DIAMOND-CHALLENGER -patch 16.20
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/javierlog08/lol-etl-go/internal/config"
	"github.com/javierlog08/lol-etl-go/internal/pipeline"
	"github.com/javierlog08/lol-etl-go/internal/riot"
	"github.com/javierlog08/lol-etl-go/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]

	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	regionsSpec := fs.String("regions", env("ETL_REGIONS", "LA1"),
		"plataformas separadas por coma: LA1,NA1,EUW1,KR")
	tiersSpec := fs.String("tiers", env("ETL_TIERS", "DIAMOND-CHALLENGER"),
		"rango (DIAMOND-CHALLENGER) o lista (GOLD,PLATINUM)")
	queue := fs.Int("queue", envInt("ETL_QUEUE", 420), "cola de match-v5; 0 = todas")
	queueName := fs.String("queue-name", env("ETL_QUEUE_NAME", "RANKED_SOLO_5x5"), "cola de league-v4")
	patch := fs.String("patch", env("ETL_PATCH", ""),
		"parche a conservar, p.ej. 16.20; vacio = el actual de Data Dragon; 'any' = no filtrar")
	since := fs.Duration("since", envDuration("ETL_SINCE", 14*24*time.Hour),
		"ventana de historial a pedir")
	timelines := fs.Bool("timelines", envBool("ETL_TIMELINES", false),
		"bajar tambien timelines (7x disco, 2x llamadas)")
	discWorkers := fs.Int("discover-workers", envInt("ETL_DISCOVER_WORKERS", 16), "goroutines pidiendo ids")
	fetchWorkers := fs.Int("fetch-workers", envInt("ETL_FETCH_WORKERS", 24), "goroutines bajando partidas")
	maxPlayers := fs.Int("max-players", envInt("ETL_MAX_PLAYERS", 0), "tope del censo por region (0 = sin tope)")
	discoverLimit := fs.Int("discover-limit", envInt("ETL_DISCOVER_LIMIT", 0),
		"tope de jugadores a consultar por region; con -dry-run sirve para muestrear volumen")
	dryRun := fs.Bool("dry-run", false, "no escribe en Mongo; sirve para medir volumen")
	logLevel := fs.String("log-level", env("ETL_LOG_LEVEL", "info"), "debug|info|warn|error")
	_ = fs.Parse(os.Args[2:])

	log := newLogger(*logLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	regions, err := config.ParseRegions(*regionsSpec)
	if err != nil {
		log.Error("regiones", "err", err)
		os.Exit(1)
	}
	tiers, err := config.ParseTiers(*tiersSpec)
	if err != nil {
		log.Error("tiers", "err", err)
		os.Exit(1)
	}

	st, err := store.Connect(ctx, config.MongoURI(), config.DBName())
	if err != nil {
		log.Error("mongo", "err", err)
		os.Exit(1)
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = st.Close(shutdown)
	}()
	if err := st.EnsureIndexes(ctx); err != nil {
		log.Error("indices", "err", err)
		os.Exit(1)
	}

	if cmd == "stats" {
		key, err := config.APIKey()
		if err == nil {
			st.SetKeyFingerprint(config.KeyFingerprint(key))
			fmt.Printf("key_fp actual: %s\n", config.KeyFingerprint(key))
		}
		if err := printStats(ctx, st); err != nil {
			log.Error("stats", "err", err)
			os.Exit(1)
		}
		return
	}

	key, err := config.APIKey()
	if err != nil {
		log.Error("api key", "err", err)
		os.Exit(1)
	}

	// El limitador arranca con limites de dev key y se reajusta con la
	// cabecera X-App-Rate-Limit de la primera respuesta, asi que una
	// production key se aprovecha entera sin configurar nada.
	lim := riot.NewLimiter(0.95)
	client := riot.NewClient(key, lim, log)

	// Riot cifra los puuid por key: la misma cuenta devuelve puuid distinto con
	// cada una, dentro de los participants de cada partida incluidos. Marcamos
	// la data con la huella de la key y purgamos el padron de otras keys, que
	// solo daria 400 "Exception decrypting".
	fp := config.KeyFingerprint(key)
	st.SetKeyFingerprint(fp)
	if n, err := st.PurgeStalePlayers(ctx); err != nil {
		log.Warn("no pude purgar el padron de otras keys", "err", err)
	} else if n > 0 {
		log.Warn("padron de otra API key purgado", "jugadores", n, "key_fp", fp)
	}

	targetPatch := *patch
	switch targetPatch {
	case "any":
		targetPatch = ""
	case "":
		p, err := riot.CurrentPatch(ctx)
		if err != nil {
			log.Error("no pude leer el parche actual de Data Dragon", "err", err)
			os.Exit(1)
		}
		targetPatch = p
		log.Info("parche detectado", "patch", targetPatch)
	}

	opts := pipeline.Options{
		Regions: regions, Tiers: tiers,
		Queue: *queue, QueueName: *queueName,
		Patch: targetPatch, Since: *since,
		Timelines:       *timelines,
		DiscoverWorkers: *discWorkers,
		FetchWorkers:    *fetchWorkers,
		MaxPlayers:      *maxPlayers,
		DiscoverLimit:   *discoverLimit,
		DryRun:          *dryRun,
	}
	run := pipeline.New(client, st, log, opts)

	log.Info("arranque",
		"cmd", cmd, "regiones", regions, "tiers", tiers,
		"patch", orAny(targetPatch), "timelines", *timelines,
		"since", since.String(), "dry_run", *dryRun)

	done := make(chan struct{})
	go progress(ctx, run, log, done)

	start := time.Now()
	switch cmd {
	case "census":
		err = run.Census(ctx)
	case "crawl":
		err = run.Crawl(ctx)
	case "run":
		if err = run.Census(ctx); err == nil {
			err = run.Crawl(ctx)
		}
	default:
		usage()
		os.Exit(2)
	}
	close(done)

	s := &run.Stats
	log.Info("fin",
		"duracion", time.Since(start).Round(time.Second).String(),
		"jugadores", s.Players.Load(),
		"ids_vistos", s.IDsSeen.Load(),
		"ids_nuevos", s.IDsNew.Load(),
		"partidas", s.MatchesStored.Load(),
		"timelines", s.TimelinesStored.Load(),
		"fuera_de_parche", s.OffPatch.Load(),
		"errores", s.Errors.Load())

	if err != nil && ctx.Err() == nil {
		log.Error("termino con error", "err", err)
		os.Exit(1)
	}
}

// progress informa cada 30s para que un crawl de horas no parezca colgado.
func progress(ctx context.Context, r *pipeline.Runner, log *slog.Logger, done <-chan struct{}) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	var lastMatches int64
	for {
		select {
		case <-t.C:
			s := &r.Stats
			cur := s.MatchesStored.Load()
			log.Info("avance",
				"jugadores", s.Players.Load(),
				"ids_vistos", s.IDsSeen.Load(),
				"ids_nuevos", s.IDsNew.Load(),
				"partidas", cur,
				"partidas_por_min", (cur-lastMatches)*2,
				"timelines", s.TimelinesStored.Load(),
				"errores", s.Errors.Load())
			lastMatches = cur
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func printStats(ctx context.Context, st *store.Store) error {
	for _, coll := range []string{"players", "matches", "timelines"} {
		n, err := st.Count(ctx, coll, bson.D{})
		if err != nil {
			return err
		}
		fmt.Printf("%-10s %d documentos\n", coll, n)
	}
	for _, t := range config.Tiers {
		n, err := st.Count(ctx, "players", bson.D{{Key: "tier", Value: t}})
		if err != nil {
			return err
		}
		if n > 0 {
			fmt.Printf("  players tier %-12s %d\n", t, n)
		}
	}
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `uso: etl <census|crawl|run|stats> [flags]

  census  padron de jugadores por region y rango de elo (league-v4)
  crawl   ids de partida de los pendientes del padron + detalle (match-v5)
  run     census y luego crawl
  stats   recuento de lo que ya hay en Mongo

ejemplos:
  etl run    -regions LA1,NA1 -tiers DIAMOND-CHALLENGER -timelines
  etl census -regions EUW1,KR -tiers GOLD-MASTER
  etl crawl  -regions LA1 -tiers DIAMOND-CHALLENGER -patch 16.20 -dry-run

flags: etl run -h
`)
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	// JSON a stdout: es lo que espera el driver awslogs de ECS.
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
}

func orAny(p string) string {
	if p == "" {
		return "cualquiera"
	}
	return p
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	switch os.Getenv(k) {
	case "1", "true", "TRUE", "yes":
		return true
	case "0", "false", "FALSE", "no":
		return false
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
