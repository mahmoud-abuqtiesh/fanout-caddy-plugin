# caddy-fanout

A Caddy v2 HTTP handler that resolves a **multivalue DNS A/AAAA record** and
**broadcasts every incoming request to all resolved backends**m instead of
load-balancing to a single one the way `reverse_proxy { dynamic a … }` does.

Every backend's response is buffered, and one is replayed to the client
verbatim — which one depends on `response_mode`:

- **`all_success`** (default) — every backend must return 2xx, or the client
  gets `502`. Otherwise one of the 2xx responses is replayed. For control-plane
  broadcasts (e.g. pushing a token to every WebRTC server) where a backend
  that missed the write must surface as a failure.
- **`lowest_status`** — replay the response with the numerically lowest HTTP
  status (`2xx < 3xx < 4xx < 5xx`) across all backends, so a real answer from
  one backend is never masked by another backend's error. For fanning a
  global-domain query out to every cell when only one cell owns the resource:
  the owning cell's `200` wins over the other cells' `404`s; if no cell has
  it, the client correctly gets `404`. A transport failure (backend
  unreachable/timed out) is treated as `502` and never wins over a real
  response; if every backend fails, the client gets `502`.

```caddyfile
http://service.example.com {
    fanout example.maqsam.com 8080 {
        refresh       10s              # re-resolve interval    (default 1m)
        timeout       5s                # per-send timeout       (default 5s)
        versions      ipv4              # restrict to ipv4/ipv6  (default both)
        max_body      1MB               # max buffered body      (default 1MiB)
        resolver      10.0.0.2 10.0.0.3 # override DNS resolver  (default system)
        response_mode lowest_status     # all_success|lowest_status (default all_success)
    }
}
```

`fanout <name> <port>` is the only required form; everything in the block is
optional. The directive is registered to order **before `reverse_proxy`**
automatically, so no `order` line is needed.

## How it works

- DNS resolution + caching/refresh reuses Caddy's own `reverseproxy.AUpstreams`.
- Each resolved `ip:port` gets a cached single-upstream `reverseproxy.Handler`,
  so forwarding (headers, `X-Forwarded-*`, connection pooling) matches the rest
  of your config. Handlers for vanished IPs are evicted and cleaned up after a
  grace period.
- Per request the body is buffered once; the request is cloned per backend (each
  with its own vars map and replacer, since reverse_proxy mutates both) and sent
  concurrently against a discard writer.

## Limitations

- No websocket / streaming fan-out: responses are buffered in full, so `1xx`
  informational responses (`100 Continue`, `101 Switching Protocols`) aren't
  proxied. `lowest_status` ignores 1xx when picking a response — broadcast is
  for regular HTTP requests such as signaling or fanned-out queries.
- Request bodies are buffered to memory, capped by `max_body`.
