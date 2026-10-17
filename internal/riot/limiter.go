package riot

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Los limites de Riot vienen en cabeceras con la forma "500:10,30000:600"
// (peticiones:segundos) y se aplican por host y, ademas, por metodo. El
// limitador no cablea ningun numero: arranca conservador y se reconfigura con
// lo que diga la cabecera en la primera respuesta, asi que el mismo binario
// funciona con dev key (20:1,100:120) y con production key sin tocar nada.

type window struct {
	limit int
	span  time.Duration
	hits  []time.Time // timestamps dentro de la ventana, en orden
}

type bucket struct {
	mu         sync.Mutex
	windows    []window
	retryUntil time.Time // fijado por un 429 con Retry-After
	configured bool
}

// Limiter mantiene un bucket por clave (host para el limite de app,
// host+metodo para el limite de metodo).
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	margin  float64 // fraccion del limite que usamos (0.95 deja aire)
}

func NewLimiter(margin float64) *Limiter {
	if margin <= 0 || margin > 1 {
		margin = 0.95
	}
	return &Limiter{buckets: map[string]*bucket{}, margin: margin}
}

func (l *Limiter) get(key string) *bucket {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		// Arranque conservador: limites de dev key hasta que una cabecera diga otra cosa.
		b = &bucket{windows: []window{
			{limit: 18, span: time.Second},
			{limit: 95, span: 2 * time.Minute},
		}}
		l.buckets[key] = b
	}
	return b
}

// cap aplica el margen de seguridad sin llegar nunca a 0.
func (l *Limiter) cap(limit int) int {
	c := int(float64(limit) * l.margin)
	if c < 1 {
		c = 1
	}
	return c
}

// Acquire bloquea hasta que la clave tiene cupo en todas sus ventanas.
func (l *Limiter) Acquire(ctx context.Context, keys ...string) error {
	for {
		wait := time.Duration(0)
		for _, k := range keys {
			if d := l.reserve(k); d > wait {
				wait = d
			}
		}
		if wait == 0 {
			return nil
		}
		// Alguna clave no tenia cupo: nadie consumio nada, esperamos y reintentamos.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// reserve devuelve 0 y consume una ranura, o el tiempo que falta para tenerla.
func (l *Limiter) reserve(key string) time.Duration {
	b := l.get(key)
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	if now.Before(b.retryUntil) {
		return b.retryUntil.Sub(now)
	}
	var wait time.Duration
	for i := range b.windows {
		w := &b.windows[i]
		cutoff := now.Add(-w.span)
		// purga de hits caducados
		k := 0
		for ; k < len(w.hits) && w.hits[k].Before(cutoff); k++ {
		}
		w.hits = w.hits[k:]
		if len(w.hits) >= l.cap(w.limit) {
			if d := w.hits[0].Add(w.span).Sub(now); d > wait {
				wait = d
			}
		}
	}
	if wait > 0 {
		return wait
	}
	for i := range b.windows {
		b.windows[i].hits = append(b.windows[i].hits, now)
	}
	return 0
}

// Observe reconfigura las ventanas con la cabecera X-App-Rate-Limit o
// X-Method-Rate-Limit. Solo lo hace una vez por bucket: los contadores vivos
// se conservan al cambiar de forma, pero recortados a la nueva ventana.
func (l *Limiter) Observe(key, header string) {
	if header == "" {
		return
	}
	parsed := parseLimitHeader(header)
	if len(parsed) == 0 {
		return
	}
	b := l.get(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.configured {
		return
	}
	// Historial mas completo de las ventanas previas: el de mayor span.
	var history []time.Time
	for _, p := range b.windows {
		if len(p.hits) > len(history) {
			history = p.hits
		}
	}
	b.windows = parsed
	now := time.Now()
	for i := range b.windows {
		cutoff := now.Add(-b.windows[i].span)
		for _, h := range history {
			if h.After(cutoff) {
				b.windows[i].hits = append(b.windows[i].hits, h)
			}
		}
	}
	b.configured = true
}

// Penalize frena el bucket el tiempo que pida un 429.
func (l *Limiter) Penalize(key string, d time.Duration) {
	if d <= 0 {
		d = time.Second
	}
	b := l.get(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	until := time.Now().Add(d)
	if until.After(b.retryUntil) {
		b.retryUntil = until
	}
}

// Describe devuelve los limites vigentes de un bucket, para log.
func (l *Limiter) Describe(key string) string {
	b := l.get(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	parts := make([]string, 0, len(b.windows))
	for _, w := range b.windows {
		parts = append(parts, strconv.Itoa(w.limit)+":"+strconv.Itoa(int(w.span.Seconds())))
	}
	return strings.Join(parts, ",")
}

func parseLimitHeader(h string) []window {
	var out []window
	for _, chunk := range strings.Split(h, ",") {
		kv := strings.SplitN(strings.TrimSpace(chunk), ":", 2)
		if len(kv) != 2 {
			continue
		}
		n, err1 := strconv.Atoi(strings.TrimSpace(kv[0]))
		s, err2 := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err1 != nil || err2 != nil || n <= 0 || s <= 0 {
			continue
		}
		out = append(out, window{limit: n, span: time.Duration(s) * time.Second})
	}
	return out
}
