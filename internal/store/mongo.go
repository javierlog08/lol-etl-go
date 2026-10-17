// Package store escribe en la MongoDB del proyecto cabecera, con la misma
// forma de documento que usa el analisis en Python: el JSON de match-v5 crudo
// con clave metadata.matchId.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Store struct {
	client *mongo.Client
	db     *mongo.Database
	keyFP  string // huella de la API key: los puuid van cifrados por key
}

// SetKeyFingerprint marca con que key se trajo la data que se escriba.
func (s *Store) SetKeyFingerprint(fp string) { s.keyFP = fp }

func Connect(ctx context.Context, uri, dbName string) (*Store, error) {
	cl, err := mongo.Connect(options.Client().ApplyURI(uri).
		SetMaxPoolSize(64).
		SetRetryWrites(true))
	if err != nil {
		return nil, fmt.Errorf("conectando a mongo: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.Ping(pctx, nil); err != nil {
		return nil, fmt.Errorf("mongo no responde en %s: %w", uri, err)
	}
	return &Store{client: cl, db: cl.Database(dbName)}, nil
}

func (s *Store) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

// EnsureIndexes replica los indices del proyecto cabecera y agrega los que
// necesita el crawl (parche, tier, pendientes de descubrir).
func (s *Store) EnsureIndexes(ctx context.Context) error {
	specs := []struct {
		coll  string
		keys  bson.D
		uniq  bool
		name  string
		parts bool
	}{
		{coll: "matches", keys: bson.D{{Key: "metadata.matchId", Value: 1}}, uniq: true},
		{coll: "matches", keys: bson.D{{Key: "info.gameCreation", Value: 1}}},
		{coll: "matches", keys: bson.D{{Key: "info.queueId", Value: 1}}},
		{coll: "matches", keys: bson.D{{Key: "info.gameVersion", Value: 1}}},
		{coll: "matches", keys: bson.D{{Key: "info.participants.puuid", Value: 1}}},
		{coll: "matches", keys: bson.D{{Key: "info.participants.championName", Value: 1}, {Key: "info.participants.teamPosition", Value: 1}}},
		{coll: "timelines", keys: bson.D{{Key: "metadata.matchId", Value: 1}}, uniq: true},
		{coll: "players", keys: bson.D{{Key: "puuid", Value: 1}}, uniq: true},
		{coll: "players", keys: bson.D{{Key: "region", Value: 1}, {Key: "tier", Value: 1}}},
		{coll: "players", keys: bson.D{{Key: "key_fp", Value: 1}}},
		{coll: "matches", keys: bson.D{{Key: "_etl.key_fp", Value: 1}}},
		{coll: "players", keys: bson.D{{Key: "discovered_patch", Value: 1}}},
		{coll: "etl_progress", keys: bson.D{{Key: "task_name", Value: 1}}, uniq: true},
	}
	for _, sp := range specs {
		opts := options.Index()
		if sp.uniq {
			opts.SetUnique(true)
		}
		if _, err := s.db.Collection(sp.coll).Indexes().CreateOne(ctx,
			mongo.IndexModel{Keys: sp.keys, Options: opts}); err != nil {
			return fmt.Errorf("indice en %s: %w", sp.coll, err)
		}
	}
	return nil
}

// Player es la fila de censo. Se escribe con $set para no pisar los campos
// que ya puso el ETL de Python (game_name, summoner_level...).
type Player struct {
	PUUID        string
	Region       string
	Tier         string
	Rank         string
	LeaguePoints int
	Wins         int
	Losses       int
	Inactive     bool
	Queue        string
}

func (s *Store) UpsertPlayers(ctx context.Context, ps []Player) (int, error) {
	if len(ps) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	models := make([]mongo.WriteModel, 0, len(ps))
	for _, p := range ps {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "puuid", Value: p.PUUID}}).
			SetUpdate(bson.D{{Key: "$set", Value: bson.D{
				{Key: "puuid", Value: p.PUUID},
				{Key: "region", Value: p.Region},
				{Key: "tier", Value: p.Tier},
				{Key: "rank", Value: p.Rank},
				{Key: "league_points", Value: p.LeaguePoints},
				{Key: "wins", Value: p.Wins},
				{Key: "losses", Value: p.Losses},
				{Key: "inactive", Value: p.Inactive},
				{Key: "queue", Value: p.Queue},
				{Key: "key_fp", Value: s.keyFP},
				{Key: "tier_snapshot_at", Value: now},
			}}}).
			SetUpsert(true))
	}
	res, err := s.db.Collection("players").BulkWrite(ctx, models,
		options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, err
	}
	return int(res.UpsertedCount + res.ModifiedCount), nil
}

// PlayersPending devuelve puuids del censo a los que aun no se les ha pedido
// el historial en este parche. Es lo que hace el crawl reanudable.
func (s *Store) PlayersPending(ctx context.Context, region string, tiers []string, patch string, limit int64) ([]string, error) {
	// key_fp: un puuid obtenido con otra key devuelve 400 "Exception decrypting",
	// asi que los jugadores de otra huella no se consultan nunca.
	filter := bson.D{
		{Key: "region", Value: region},
		{Key: "tier", Value: bson.D{{Key: "$in", Value: tiers}}},
		{Key: "key_fp", Value: s.keyFP},
		{Key: "discovered_patch", Value: bson.D{{Key: "$ne", Value: patch}}},
	}
	opts := options.Find().SetProjection(bson.D{{Key: "puuid", Value: 1}})
	if limit > 0 {
		opts.SetLimit(limit)
	}
	cur, err := s.db.Collection("players").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []string
	for cur.Next(ctx) {
		var row struct {
			PUUID string `bson:"puuid"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, err
		}
		out = append(out, row.PUUID)
	}
	return out, cur.Err()
}

// MarkDiscovered anota que a estos jugadores ya se les pidio el historial del
// parche, para que un reinicio no repita las llamadas.
func (s *Store) MarkDiscovered(ctx context.Context, puuids []string, patch string) error {
	if len(puuids) == 0 {
		return nil
	}
	_, err := s.db.Collection("players").UpdateMany(ctx,
		bson.D{{Key: "puuid", Value: bson.D{{Key: "$in", Value: puuids}}}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "discovered_patch", Value: patch},
			{Key: "discovered_at", Value: time.Now().UTC()},
		}}})
	return err
}

// ExistingIDs filtra los ids que ya estan en una coleccion. Es el ahorro mas
// grande del ETL: cada partida se descubre ~10 veces, una por participante.
func (s *Store) ExistingIDs(ctx context.Context, coll string, ids []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(ids))
	const chunk = 1000
	for i := 0; i < len(ids); i += chunk {
		end := min(i+chunk, len(ids))
		cur, err := s.db.Collection(coll).Find(ctx,
			bson.D{{Key: "metadata.matchId", Value: bson.D{{Key: "$in", Value: ids[i:end]}}}},
			options.Find().SetProjection(bson.D{{Key: "metadata.matchId", Value: 1}}))
		if err != nil {
			return nil, err
		}
		for cur.Next(ctx) {
			var row struct {
				Metadata struct {
					MatchID string `bson:"matchId"`
				} `bson:"metadata"`
			}
			if err := cur.Decode(&row); err != nil {
				cur.Close(ctx)
				return nil, err
			}
			out[row.Metadata.MatchID] = struct{}{}
		}
		cur.Close(ctx)
		if err := cur.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// RawDoc convierte el JSON del API a BSON preservando enteros como enteros
// (el parser de extended JSON relajado mapea 420 a int32, no a double).
func RawDoc(raw json.RawMessage) (bson.D, error) {
	var doc bson.D
	if err := bson.UnmarshalExtJSON(raw, false, &doc); err != nil {
		return nil, fmt.Errorf("json->bson: %w", err)
	}
	return doc, nil
}

func (s *Store) UpsertRaw(ctx context.Context, coll string, docs []bson.D) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	models := make([]mongo.WriteModel, 0, len(docs))
	for _, d := range docs {
		id, ok := matchIDOf(d)
		if !ok {
			continue
		}
		// _etl.key_fp: los puuid de participants pertenecen a esa key.
		d = append(d, bson.E{Key: "_etl", Value: bson.D{
			{Key: "key_fp", Value: s.keyFP},
			{Key: "at", Value: time.Now().UTC()},
		}})
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "metadata.matchId", Value: id}}).
			SetUpdate(bson.D{{Key: "$set", Value: d}}).
			SetUpsert(true))
	}
	if len(models) == 0 {
		return 0, nil
	}
	res, err := s.db.Collection(coll).BulkWrite(ctx, models,
		options.BulkWrite().SetOrdered(false))
	if err != nil {
		// Un duplicado en carrera no invalida el lote: ordered=false ya escribio el resto.
		if mongo.IsDuplicateKeyError(err) {
			return len(models), nil
		}
		return 0, err
	}
	return int(res.UpsertedCount + res.ModifiedCount), nil
}

func matchIDOf(d bson.D) (string, bool) {
	for _, e := range d {
		if e.Key != "metadata" {
			continue
		}
		md, ok := e.Value.(bson.D)
		if !ok {
			return "", false
		}
		for _, m := range md {
			if m.Key == "matchId" {
				if s, ok := m.Value.(string); ok {
					return s, true
				}
			}
		}
	}
	return "", false
}

// Progress guarda el avance de una tarea para poder reanudar.
func (s *Store) SaveProgress(ctx context.Context, task string, state bson.D) error {
	_, err := s.db.Collection("etl_progress").UpdateOne(ctx,
		bson.D{{Key: "task_name", Value: task}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "task_name", Value: task},
			{Key: "state", Value: state},
			{Key: "updated_at", Value: time.Now().UTC()},
		}}},
		options.UpdateOne().SetUpsert(true))
	return err
}

func (s *Store) Count(ctx context.Context, coll string, filter bson.D) (int64, error) {
	return s.db.Collection(coll).CountDocuments(ctx, filter)
}

// PurgeStalePlayers borra del padron los jugadores capturados con otra key:
// sus puuid estan cifrados con esa key y no sirven para nada.
func (s *Store) PurgeStalePlayers(ctx context.Context) (int64, error) {
	res, err := s.db.Collection("players").DeleteMany(ctx,
		bson.D{{Key: "key_fp", Value: bson.D{{Key: "$ne", Value: s.keyFP}}}})
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, nil
}
