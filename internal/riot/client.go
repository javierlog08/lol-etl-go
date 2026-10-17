// Package riot envuelve el API publico de Riot: limites de rate por host y
// por metodo, reintentos y los endpoints que usa el ETL.
package riot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Cloudflare responde 403 code 1010 a clientes sin User-Agent de navegador,
// aunque la key sea valida. No quitar.
const userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) " +
	"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

var (
	ErrNotFound     = errors.New("riot: 404 not found")
	ErrUnauthorized = errors.New("riot: 401/403 key invalida o sin permisos")
)

type Client struct {
	key     string
	http    *http.Client
	lim     *Limiter
	log     *slog.Logger
	retries int
}

func NewClient(key string, lim *Limiter, log *slog.Logger) *Client {
	return &Client{
		key: key,
		lim: lim,
		log: log,
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 64,
				MaxConnsPerHost:     128,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		retries: 5,
	}
}

// get hace una peticion respetando los dos buckets que aplican: el de app
// (por host) y el de metodo (host+metodo). method es un identificador estable
// del endpoint, no la ruta con ids dentro.
func (c *Client) get(ctx context.Context, host, method, path string, q url.Values) ([]byte, error) {
	appKey := "app:" + host
	methKey := "method:" + host + ":" + method

	raw := "https://" + host + path
	if len(q) > 0 {
		raw += "?" + q.Encode()
	}

	var lastErr error
	for attempt := 0; attempt <= c.retries; attempt++ {
		if err := c.lim.Acquire(ctx, appKey, methKey); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-Riot-Token", c.key)
		req.Header.Set("User-Agent", userAgent)
		// Sin tocar Accept-Encoding: net/http lo pone y descomprime solo. Si se
		// pone a mano, Go asume que el cuerpo lo gestiona el llamante y entrega
		// gzip crudo.

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if !sleepCtx(ctx, backoff(attempt)) {
				return nil, ctx.Err()
			}
			continue
		}

		// Los limites reales llegan en cada respuesta; el limitador se ajusta.
		c.lim.Observe(appKey, resp.Header.Get("X-App-Rate-Limit"))
		c.lim.Observe(methKey, resp.Header.Get("X-Method-Rate-Limit"))

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusOK:
			if readErr != nil {
				lastErr = readErr
				continue
			}
			return body, nil

		case resp.StatusCode == http.StatusTooManyRequests:
			d := retryAfter(resp.Header)
			// Penaliza el bucket que Riot indique: app, method o el del servicio.
			switch resp.Header.Get("X-Rate-Limit-Type") {
			case "method":
				c.lim.Penalize(methKey, d)
			default:
				c.lim.Penalize(appKey, d)
			}
			c.log.Warn("429", "host", host, "method", method, "retry_after", d.String(),
				"type", resp.Header.Get("X-Rate-Limit-Type"))
			lastErr = fmt.Errorf("riot: 429 en %s", method)

		case resp.StatusCode == http.StatusNotFound:
			return nil, ErrNotFound

		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			return nil, fmt.Errorf("%w (%d en %s)", ErrUnauthorized, resp.StatusCode, method)

		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("riot: %d en %s", resp.StatusCode, method)
			if !sleepCtx(ctx, backoff(attempt)) {
				return nil, ctx.Err()
			}

		default:
			return nil, fmt.Errorf("riot: %d en %s: %s", resp.StatusCode, method, truncate(body, 200))
		}
	}
	return nil, fmt.Errorf("agotados los reintentos: %w", lastErr)
}

func backoff(attempt int) time.Duration {
	d := time.Duration(1<<uint(attempt)) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func retryAfter(h http.Header) time.Duration {
	if v := h.Get("Retry-After"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n+1) * time.Second
		}
	}
	return 2 * time.Second
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
