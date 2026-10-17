// Package config resuelve regiones, rangos de elo y parametros del crawl.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Region asocia el host de plataforma (league-v4, summoner-v4) con el host
// regional (account-v1, match-v5). Los limites de rate se aplican por host,
// asi que dos regiones de la misma ruta comparten cupo en match-v5.
type Region struct {
	Platform string
	Regional string
}

var Regions = map[string]Region{
	"BR1":  {"br1.api.riotgames.com", "americas.api.riotgames.com"},
	"LA1":  {"la1.api.riotgames.com", "americas.api.riotgames.com"},
	"LA2":  {"la2.api.riotgames.com", "americas.api.riotgames.com"},
	"NA1":  {"na1.api.riotgames.com", "americas.api.riotgames.com"},
	"EUN1": {"eun1.api.riotgames.com", "europe.api.riotgames.com"},
	"EUW1": {"euw1.api.riotgames.com", "europe.api.riotgames.com"},
	"RU":   {"ru.api.riotgames.com", "europe.api.riotgames.com"},
	"TR1":  {"tr1.api.riotgames.com", "europe.api.riotgames.com"},
	"JP1":  {"jp1.api.riotgames.com", "asia.api.riotgames.com"},
	"KR":   {"kr.api.riotgames.com", "asia.api.riotgames.com"},
	"OC1":  {"oc1.api.riotgames.com", "sea.api.riotgames.com"},
	"SG2":  {"sg2.api.riotgames.com", "sea.api.riotgames.com"},
	"TW2":  {"tw2.api.riotgames.com", "sea.api.riotgames.com"},
	"VN2":  {"vn2.api.riotgames.com", "sea.api.riotgames.com"},
	// PH2 y TH2 no resuelven en DNS (comprobado oct 2026): Riot sirve
	// Filipinas y Tailandia desde SG2. No añadirlos.
}

// All devuelve las 14 plataformas vivas, para -regions all.
func All() []string {
	out := make([]string, 0, len(Regions))
	for r := range Regions {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// Tiers en orden ascendente. Los tres ultimos son apex: no tienen divisiones
// y se piden por endpoints propios (challengerleagues, etc).
var Tiers = []string{
	"IRON", "BRONZE", "SILVER", "GOLD", "PLATINUM",
	"EMERALD", "DIAMOND", "MASTER", "GRANDMASTER", "CHALLENGER",
}

var Divisions = []string{"I", "II", "III", "IV"}

func IsApex(tier string) bool {
	switch tier {
	case "MASTER", "GRANDMASTER", "CHALLENGER":
		return true
	}
	return false
}

// ApexEndpoint devuelve el segmento de league-v4 para un tier apex.
func ApexEndpoint(tier string) string {
	switch tier {
	case "MASTER":
		return "masterleagues"
	case "GRANDMASTER":
		return "grandmasterleagues"
	case "CHALLENGER":
		return "challengerleagues"
	}
	return ""
}

func tierIndex(t string) int {
	for i, x := range Tiers {
		if x == t {
			return i
		}
	}
	return -1
}

// ParseTiers acepta tanto una lista ("GOLD,PLATINUM") como un rango
// ("DIAMOND-CHALLENGER"), y devuelve los tiers en orden ascendente.
func ParseTiers(spec string) ([]string, error) {
	spec = strings.ToUpper(strings.TrimSpace(spec))
	if spec == "" {
		return nil, fmt.Errorf("rango de tiers vacio")
	}
	if strings.Contains(spec, "-") {
		parts := strings.SplitN(spec, "-", 2)
		lo, hi := tierIndex(strings.TrimSpace(parts[0])), tierIndex(strings.TrimSpace(parts[1]))
		if lo < 0 || hi < 0 {
			return nil, fmt.Errorf("tier desconocido en el rango %q", spec)
		}
		if lo > hi {
			lo, hi = hi, lo
		}
		return append([]string(nil), Tiers[lo:hi+1]...), nil
	}
	var out []string
	for _, t := range strings.Split(spec, ",") {
		t = strings.TrimSpace(t)
		if tierIndex(t) < 0 {
			return nil, fmt.Errorf("tier desconocido %q", t)
		}
		out = append(out, t)
	}
	return out, nil
}

// ParseRegions valida una lista de plataformas separadas por coma.
func ParseRegions(spec string) ([]string, error) {
	if strings.EqualFold(strings.TrimSpace(spec), "all") {
		return All(), nil
	}
	var out []string
	for _, r := range strings.Split(strings.ToUpper(spec), ",") {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if _, ok := Regions[r]; !ok {
			return nil, fmt.Errorf("region desconocida %q", r)
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("ninguna region valida en %q", spec)
	}
	return out, nil
}

// APIKey lee la key de RIOT_API_KEY o de un fichero (RIOT_API_KEY_FILE), que
// es como la inyecta ECS desde Secrets Manager montado como archivo.
func APIKey() (string, error) {
	if k := strings.TrimSpace(os.Getenv("RIOT_API_KEY")); k != "" {
		return k, nil
	}
	if f := os.Getenv("RIOT_API_KEY_FILE"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			return "", fmt.Errorf("leyendo RIOT_API_KEY_FILE: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return "", fmt.Errorf("falta RIOT_API_KEY (o RIOT_API_KEY_FILE)")
}

// MongoURI nunca lleva credenciales por defecto: si tu despliegue las usa,
// van en la variable de entorno, no en el codigo.
func MongoURI() string {
	if u := os.Getenv("MONGO_URI"); u != "" {
		return u
	}
	return "mongodb://localhost:27017"
}

func DBName() string {
	if n := os.Getenv("MONGO_DB"); n != "" {
		return n
	}
	return "lol_analyze"
}

// KeyFingerprint identifica la API key sin guardarla.
//
// Riot cifra los puuid POR KEY: la misma cuenta devuelve un puuid distinto con
// cada key, y eso incluye los puuid dentro de info.participants de cada
// partida. Data crawleada con dos keys distintas no se puede cruzar por puuid,
// asi que cada documento lleva la huella de la key que lo trajo y el ETL solo
// consulta jugadores de su propia huella.
func KeyFingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:12]
}
