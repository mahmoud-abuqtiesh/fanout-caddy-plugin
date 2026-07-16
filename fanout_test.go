package fanout

import (
	"net/http"
	"testing"
)

func TestPickResponseLowestStatus(t *testing.T) {
	mk := func(status int) backendResult {
		return backendResult{status: status, ok: status >= 200 && status < 300, hasResponse: true}
	}

	cases := []struct {
		name    string
		results []backendResult
		wantIdx int
		wantOK  bool
	}{
		{"200 beats 404s", []backendResult{mk(200), mk(404), mk(404)}, 0, true},
		{"all 404", []backendResult{mk(404), mk(404)}, 0, true},
		{"200 beats 500", []backendResult{mk(200), mk(500)}, 0, true},
		{"all 500", []backendResult{mk(500), mk(500)}, 0, true},
		{
			"transport failure never beats a 200",
			[]backendResult{{status: 0, hasResponse: false}, mk(200)},
			1, true,
		},
		{
			"all failed signals 502 with no replay",
			[]backendResult{{status: 0, hasResponse: false}, {status: 0, hasResponse: false}},
			-1, false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			idx, replay := pickResponse(c.results, "lowest_status")
			if replay != c.wantOK {
				t.Fatalf("replay = %v, want %v", replay, c.wantOK)
			}
			if replay && idx != c.wantIdx {
				t.Fatalf("idx = %d, want %d", idx, c.wantIdx)
			}
			if replay && c.results[idx].status != c.results[c.wantIdx].status {
				t.Fatalf("picked status %d, want %d", c.results[idx].status, c.results[c.wantIdx].status)
			}
		})
	}
}

func TestPickResponseAllSuccess(t *testing.T) {
	mk := func(status int, ok bool) backendResult {
		return backendResult{status: status, ok: ok, hasResponse: true}
	}

	for _, mode := range []string{"", "all_success"} {
		t.Run("mode="+mode, func(t *testing.T) {
			idx, replay := pickResponse([]backendResult{mk(200, true), mk(200, true)}, mode)
			if !replay || idx != 0 {
				t.Fatalf("all-2xx: got idx=%d replay=%v, want idx=0 replay=true", idx, replay)
			}

			_, replay = pickResponse([]backendResult{mk(200, true), mk(404, false)}, mode)
			if replay {
				t.Fatalf("one non-2xx: got replay=true, want false (502)")
			}
		})
	}
}

func TestPickResponseIgnores1xx(t *testing.T) {
	results := []backendResult{
		{status: http.StatusSwitchingProtocols, hasResponse: true},
		{status: 200, ok: true, hasResponse: true},
	}
	idx, replay := pickResponse(results, "lowest_status")
	if !replay || idx != 1 {
		t.Fatalf("1xx should lose to 200: got idx=%d replay=%v", idx, replay)
	}
}
