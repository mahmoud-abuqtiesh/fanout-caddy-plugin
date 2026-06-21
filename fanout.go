// Package fanout implements a Caddy HTTP handler that resolves a multivalue
// DNS A/AAAA record and broadcasts every incoming request to ALL resolved
// backends, rather than load-balancing to a single one (as reverse_proxy does).
//
// It is intended for control-plane fan-out — e.g. pushing a token to every
// Janus WebRTC server registered behind a single Multivalue Answer record.
// Backend responses are intentionally discarded; the client receives a 200
// once every backend has been sent the request (best-effort, bounded by
// timeout).
package fanout

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
)

const (
	defaultTimeout = 5 * time.Second
	defaultMaxBody = 1 << 20 // 1 MiB
)

// Fanout broadcasts each request to all IPs a DNS name currently resolves to.
type Fanout struct {
	// Upstream is the DNS name to resolve (a multivalue A/AAAA record).
	Upstream string `json:"upstream,omitempty"`

	// Port is appended to every resolved IP. Defaults to 80 (via AUpstreams).
	Port string `json:"port,omitempty"`

	// Refresh is how often the DNS record is re-resolved. Defaults to 1m
	// (via AUpstreams) when zero.
	Refresh caddy.Duration `json:"refresh,omitempty"`

	// Versions restricts resolution to ipv4 and/or ipv6.
	Versions *reverseproxy.IPVersions `json:"versions,omitempty"`

	// Resolver optionally overrides the DNS resolver used for lookups.
	Resolver *reverseproxy.UpstreamResolver `json:"resolver,omitempty"`

	// Timeout bounds each fan-out send. Defaults to 5s when zero.
	Timeout caddy.Duration `json:"timeout,omitempty"`

	// MaxBody is the maximum request body size buffered for fan-out, in bytes.
	// Defaults to 1 MiB when zero. Requests with larger bodies get 413.
	MaxBody int64 `json:"max_body,omitempty"`

	aSource  *reverseproxy.AUpstreams
	ctx      caddy.Context
	logger   *zap.Logger
	mu       sync.RWMutex
	handlers map[string]*reverseproxy.Handler
}

// CaddyModule returns the Caddy module information.
func (*Fanout) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.fanout",
		New: func() caddy.Module { return new(Fanout) },
	}
}

// Provision sets up the DNS resolver and handler cache.
func (f *Fanout) Provision(ctx caddy.Context) error {
	f.ctx = ctx
	f.logger = ctx.Logger()
	f.handlers = make(map[string]*reverseproxy.Handler)

	if f.Upstream == "" {
		return fmt.Errorf("fanout: an upstream DNS name is required")
	}

	// Reuse Caddy's own dynamic A/AAAA source for resolution + caching/refresh.
	f.aSource = &reverseproxy.AUpstreams{
		Name:     f.Upstream,
		Port:     f.Port,
		Refresh:  f.Refresh,
		Versions: f.Versions,
		Resolver: f.Resolver,
	}
	if err := f.aSource.Provision(ctx); err != nil {
		return fmt.Errorf("fanout: provisioning DNS resolver for %q: %w", f.Upstream, err)
	}
	return nil
}

// Cleanup tears down every cached upstream handler.
func (f *Fanout) Cleanup() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for addr, h := range f.handlers {
		_ = h.Cleanup()
		delete(f.handlers, addr)
	}
	return nil
}

// ServeHTTP resolves the current backend set and broadcasts the request to all
// of them. It is terminal: it writes the client response itself and never calls
// next.
func (f *Fanout) ServeHTTP(w http.ResponseWriter, r *http.Request, _ caddyhttp.Handler) error {
	// Resolve on the ORIGINAL request — it carries the *caddy.Replacer that
	// AUpstreams.GetUpstreams requires (it type-asserts without checking).
	ups, err := f.aSource.GetUpstreams(r)
	if err != nil {
		f.logger.Error("fanout: DNS resolution failed",
			zap.String("name", f.Upstream), zap.Error(err))
		return caddyhttp.Error(http.StatusBadGateway, err)
	}

	addrs := make([]string, 0, len(ups))
	seen := make(map[string]bool, len(ups))
	for _, u := range ups {
		if u == nil || u.Dial == "" || seen[u.Dial] {
			continue
		}
		seen[u.Dial] = true
		addrs = append(addrs, u.Dial)
	}
	if len(addrs) == 0 {
		f.logger.Warn("fanout: no upstreams resolved", zap.String("name", f.Upstream))
		return caddyhttp.Error(http.StatusBadGateway,
			fmt.Errorf("fanout: no upstreams resolved for %q", f.Upstream))
	}

	handlers := f.handlersFor(addrs)
	if len(handlers) == 0 {
		return caddyhttp.Error(http.StatusBadGateway,
			fmt.Errorf("fanout: no usable upstream handlers for %q", f.Upstream))
	}

	// Buffer the body once so every backend gets an independent reader.
	body, ok, err := f.readBody(r)
	if err != nil {
		return caddyhttp.Error(http.StatusBadGateway,
			fmt.Errorf("fanout: reading request body: %w", err))
	}
	if !ok {
		return caddyhttp.Error(http.StatusRequestEntityTooLarge,
			fmt.Errorf("fanout: request body exceeds max_body"))
	}

	timeout := time.Duration(f.Timeout)
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	// WithoutCancel decouples the broadcast from client disconnect; the timeout
	// prevents goroutine leaks on a slow/dead backend.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), timeout)
	defer cancel()

	origVars, _ := r.Context().Value(caddyhttp.VarsCtxKey).(map[string]any)

	var failures int64
	var wg sync.WaitGroup
	for _, h := range handlers {
		wg.Add(1)
		go func(h *reverseproxy.Handler) {
			defer wg.Done()
			clone := f.cloneRequest(r, ctx, origVars, body)
			// Discard the backend's response; we only care that it was sent.
			rec := caddyhttp.NewResponseRecorder(&nopResponseWriter{}, nil,
				func(int, http.Header) bool { return false })
			if err := h.ServeHTTP(rec, clone, caddyhttp.HandlerFunc(emptyNext)); err != nil {
				atomic.AddInt64(&failures, 1)
				f.logger.Warn("fanout: upstream send failed",
					zap.String("upstream", h.Upstreams[0].Dial), zap.Error(err))
			}
		}(h)
	}
	wg.Wait()

	if int(failures) >= len(handlers) {
		return caddyhttp.Error(http.StatusBadGateway,
			fmt.Errorf("fanout: all %d upstream(s) failed", len(handlers)))
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

// readBody buffers the request body up to MaxBody. The bool is false if the body
// exceeds the limit.
func (f *Fanout) readBody(r *http.Request) ([]byte, bool, error) {
	if r.Body == nil {
		return nil, true, nil
	}
	limit := f.MaxBody
	if limit <= 0 {
		limit = defaultMaxBody
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	_ = r.Body.Close()
	if err != nil {
		return nil, true, err
	}
	if int64(len(body)) > limit {
		return nil, false, nil
	}
	return body, true, nil
}

// cloneRequest builds an independent copy of r targeted at one backend. Each
// clone gets its own vars map and replacer because reverse_proxy mutates both
// during ServeHTTP, and they are not safe for concurrent use.
func (f *Fanout) cloneRequest(r *http.Request, ctx context.Context, origVars map[string]any, body []byte) *http.Request {
	vars := make(map[string]any)
	if origVars != nil {
		vars = maps.Clone(origVars)
	}
	cctx := context.WithValue(ctx, caddyhttp.VarsCtxKey, vars)
	cctx = context.WithValue(cctx, caddy.ReplacerCtxKey, caddy.NewReplacer())

	clone := r.Clone(cctx)
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.ContentLength = int64(len(body))
	clone.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return clone
}

// handlersFor returns the cached reverse_proxy handler for each address,
// provisioning new ones and evicting vanished ones as the resolved set changes.
func (f *Fanout) handlersFor(addrs []string) []*reverseproxy.Handler {
	// Fast path: the cache already matches the resolved set exactly.
	f.mu.RLock()
	if len(addrs) == len(f.handlers) {
		all := true
		for _, addr := range addrs {
			if f.handlers[addr] == nil {
				all = false
				break
			}
		}
		if all {
			result := make([]*reverseproxy.Handler, 0, len(addrs))
			for _, addr := range addrs {
				result = append(result, f.handlers[addr])
			}
			f.mu.RUnlock()
			return result
		}
	}
	f.mu.RUnlock()

	// Slow path: reconcile the cache with the current resolved set.
	f.mu.Lock()
	defer f.mu.Unlock()

	active := make(map[string]bool, len(addrs))
	result := make([]*reverseproxy.Handler, 0, len(addrs))
	for _, addr := range addrs {
		active[addr] = true
		h := f.handlers[addr]
		if h == nil {
			h = &reverseproxy.Handler{Upstreams: reverseproxy.UpstreamPool{{Dial: addr}}}
			if err := h.Provision(f.ctx); err != nil {
				f.logger.Error("fanout: provisioning upstream handler",
					zap.String("addr", addr), zap.Error(err))
				continue
			}
			f.handlers[addr] = h
		}
		result = append(result, h)
	}

	// Evict handlers for addresses no longer resolved. Defer Cleanup past the
	// fan-out timeout so an in-flight goroutine can't use a torn-down handler.
	grace := time.Duration(f.Timeout) + 5*time.Second
	if grace <= 0 {
		grace = defaultTimeout + 5*time.Second
	}
	for addr, h := range f.handlers {
		if !active[addr] {
			delete(f.handlers, addr)
			stale := h
			time.AfterFunc(grace, func() { _ = stale.Cleanup() })
		}
	}
	return result
}

func emptyNext(http.ResponseWriter, *http.Request) error { return nil }

// Interface guards.
var (
	_ caddy.Provisioner           = (*Fanout)(nil)
	_ caddy.CleanerUpper          = (*Fanout)(nil)
	_ caddyhttp.MiddlewareHandler = (*Fanout)(nil)
)
