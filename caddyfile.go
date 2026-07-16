package fanout

import (
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/dustin/go-humanize"
)

func init() {
	caddy.RegisterModule(&Fanout{})
	httpcaddyfile.RegisterHandlerDirective("fanout", parseCaddyfile)
	// fanout is terminal like reverse_proxy; order it alongside it.
	httpcaddyfile.RegisterDirectiveOrder("fanout", httpcaddyfile.Before, "reverse_proxy")
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var f Fanout
	if err := f.UnmarshalCaddyfile(h.Dispenser); err != nil {
		return nil, err
	}
	return &f, nil
}

// UnmarshalCaddyfile parses the fanout directive:
//
//	fanout [<name> <port>] {
//	    name          <name>
//	    port          <port>
//	    refresh       <duration>
//	    timeout       <duration>
//	    versions      ipv4|ipv6 ...
//	    max_body      <size>
//	    response_mode all_success|lowest_status
//	}
func (f *Fanout) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume the directive name

	args := d.RemainingArgs()
	switch len(args) {
	case 0:
	case 1:
		f.Upstream = args[0]
	case 2:
		f.Upstream = args[0]
		f.Port = args[1]
	default:
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		switch d.Val() {
		case "name":
			if !d.NextArg() {
				return d.ArgErr()
			}
			f.Upstream = d.Val()
		case "port":
			if !d.NextArg() {
				return d.ArgErr()
			}
			f.Port = d.Val()
		case "refresh":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := caddy.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("parsing refresh: %v", err)
			}
			f.Refresh = caddy.Duration(dur)
		case "timeout":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := caddy.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("parsing timeout: %v", err)
			}
			f.Timeout = caddy.Duration(dur)
		case "max_body":
			if !d.NextArg() {
				return d.ArgErr()
			}
			size, err := humanize.ParseBytes(d.Val())
			if err != nil {
				return d.Errf("parsing max_body: %v", err)
			}
			f.MaxBody = int64(size)
		case "resolver":
			addrs := d.RemainingArgs()
			if len(addrs) == 0 {
				return d.ArgErr()
			}
			f.Resolver = &reverseproxy.UpstreamResolver{Addresses: addrs}
		case "response_mode":
			if !d.NextArg() {
				return d.ArgErr()
			}
			switch d.Val() {
			case "all_success", "lowest_status":
				f.ResponseMode = d.Val()
			default:
				return d.Errf("unknown response_mode: %s (want all_success|lowest_status)", d.Val())
			}
		case "versions":
			versions := d.RemainingArgs()
			if len(versions) == 0 {
				return d.ArgErr()
			}
			f.Versions = &reverseproxy.IPVersions{}
			t := true
			for _, v := range versions {
				switch v {
				case "ipv4":
					f.Versions.IPv4 = &t
				case "ipv6":
					f.Versions.IPv6 = &t
				default:
					return d.Errf("unknown IP version: %s", v)
				}
			}
		default:
			return d.Errf("unrecognized fanout option: %s", d.Val())
		}
	}

	if f.Upstream == "" {
		return d.Err("fanout: an upstream DNS name is required")
	}
	return nil
}
