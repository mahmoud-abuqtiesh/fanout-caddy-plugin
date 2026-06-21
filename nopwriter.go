package fanout

import "net/http"

// nopResponseWriter discards everything written to it. It backs the
// ResponseRecorder used for fan-out targets whose responses are dropped.
//
// Header() must return a non-nil http.Header because reverse_proxy copies the
// upstream response headers into it.
type nopResponseWriter struct {
	header http.Header
}

func (w *nopResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *nopResponseWriter) Write(b []byte) (int, error) { return len(b), nil }

func (w *nopResponseWriter) WriteHeader(int) {}
