# caddy-fanout

A Caddy v2 HTTP handler that resolves a **multivalue DNS A/AAAA record** and
**broadcasts every incoming request to all resolved backends**m instead of
load-balancing to a single one the way `reverse_proxy { dynamic a … }` does.

Backend responses are discarded. The client gets `200` once every backend has
been sent the request (best-effort, bounded by `timeout`); `502` only if every
backend fails or the name resolves to nothing.


## Caddyfile

```caddyfile
http://service.example.com {
    fanout example.maqsam.com 8080 {
        refresh  10s              # re-resolve interval   (default 1m)
        timeout  5s               # per-send timeout      (default 5s)
        versions ipv4             # restrict to ipv4/ipv6 (default both)
        max_body 1MB              # max buffered body     (default 1MiB)
        resolver 10.0.0.2 10.0.0.3 # override DNS resolver (default system)
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

- No websocket / streaming fan-out (responses are dropped; broadcast is for
  regular HTTP requests such as signaling).
- Request bodies are buffered to memory, capped by `max_body`.
