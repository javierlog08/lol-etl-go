package riot

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// LeagueEntry es una fila de league-v4. Desde 2025 trae puuid, asi que no hace
// falta summoner-v4 para resolverlo: ahorra una llamada por jugador.
type LeagueEntry struct {
	PUUID        string `json:"puuid"`
	Tier         string `json:"tier"`
	Rank         string `json:"rank"`
	LeaguePoints int    `json:"leaguePoints"`
	Wins         int    `json:"wins"`
	Losses       int    `json:"losses"`
	Inactive     bool   `json:"inactive"`
	Veteran      bool   `json:"veteran"`
	FreshBlood   bool   `json:"freshBlood"`
	HotStreak    bool   `json:"hotStreak"`
}

// LeagueEntries pagina un tier con divisiones (IRON..DIAMOND). Una pagina
// vacia marca el final.
func (c *Client) LeagueEntries(ctx context.Context, platform, queue, tier, division string, page int) ([]LeagueEntry, error) {
	path := fmt.Sprintf("/lol/league/v4/entries/%s/%s/%s", queue, tier, division)
	q := url.Values{"page": {strconv.Itoa(page)}}
	body, err := c.get(ctx, platform, "league-v4.entries", path, q)
	if err != nil {
		return nil, err
	}
	var out []LeagueEntry
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("league-v4 entries: %w", err)
	}
	return out, nil
}

// ApexEntries pide un tier sin divisiones (MASTER, GRANDMASTER, CHALLENGER),
// que devuelve la liga entera en una sola respuesta.
func (c *Client) ApexEntries(ctx context.Context, platform, endpoint, queue string) ([]LeagueEntry, error) {
	path := fmt.Sprintf("/lol/league/v4/%s/by-queue/%s", endpoint, queue)
	body, err := c.get(ctx, platform, "league-v4."+endpoint, path, nil)
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Tier    string        `json:"tier"`
		Entries []LeagueEntry `json:"entries"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return nil, fmt.Errorf("league-v4 %s: %w", endpoint, err)
	}
	for i := range wrap.Entries {
		if wrap.Entries[i].Tier == "" {
			wrap.Entries[i].Tier = wrap.Tier
		}
	}
	return wrap.Entries, nil
}

// MatchIDs lista partidas de un jugador. startTime acota al parche en curso y
// evita bajar ids que luego habria que descartar.
func (c *Client) MatchIDs(ctx context.Context, regional, puuid string, queue, start, count int, startTime int64) ([]string, error) {
	q := url.Values{
		"start": {strconv.Itoa(start)},
		"count": {strconv.Itoa(count)},
	}
	if queue > 0 {
		q.Set("queue", strconv.Itoa(queue))
	}
	if startTime > 0 {
		q.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	path := "/lol/match/v5/matches/by-puuid/" + url.PathEscape(puuid) + "/ids"
	body, err := c.get(ctx, regional, "match-v5.ids", path, q)
	if err != nil {
		return nil, err
	}
	var out []string
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("match-v5 ids: %w", err)
	}
	return out, nil
}

// Match devuelve el documento crudo de match-v5. Se guarda tal cual para que
// el analisis en Python siga leyendo la misma forma.
func (c *Client) Match(ctx context.Context, regional, matchID string) (json.RawMessage, error) {
	body, err := c.get(ctx, regional, "match-v5.match", "/lol/match/v5/matches/"+url.PathEscape(matchID), nil)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(body), nil
}

func (c *Client) Timeline(ctx context.Context, regional, matchID string) (json.RawMessage, error) {
	path := "/lol/match/v5/matches/" + url.PathEscape(matchID) + "/timeline"
	body, err := c.get(ctx, regional, "match-v5.timeline", path, nil)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(body), nil
}

// CurrentPatch consulta Data Dragon (sin key) y devuelve el parche corto,
// p.ej. "16.20" a partir de "16.20.1".
func CurrentPatch(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://ddragon.leagueoflegends.com/api/versions.json", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var versions []string
	if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
		return "", err
	}
	if len(versions) == 0 {
		return "", fmt.Errorf("data dragon no devolvio versiones")
	}
	parts := strings.Split(versions[0], ".")
	if len(parts) < 2 {
		return versions[0], nil
	}
	return parts[0] + "." + parts[1], nil
}
