// Package s3fault is a fault-injecting reverse proxy for S3 traffic. Every
// S3 client in a test (the controller's store readers, restic, barman) points
// at the proxy, and the proxy forwards to the real S3 server behind it, which
// is RustFS 1.0.0 in the recorders and the e2e tier and the s3fake server in
// the fast tier. Rules added while the test runs slow, fail, refuse, drop or
// hold the requests they match, so a test can say "the store answers 503 from
// step 3 to step 6" or "every read takes 100 ms".
//
// It stands in for the network and the store's failure modes, which none of
// the real programs can be asked to produce on cue. It models, per matching
// request: added latency; an S3 error response with a status and code, as the
// store would send it; a refused connection (the connection is closed with no
// response); a response dropped after the upstream applied the request (the
// write landed, the client sees a broken connection); and a request held until
// the rule is released or the client gives up. It deliberately does not model
// partial bodies, slow bodies, TLS failures or DNS failures, and it reads only
// path-style addressing (http://host/bucket/key), which is what every client
// in this repository uses against RustFS.
//
// It is test support: only tests and the fixture recorders import it.
package s3fault

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Match selects the requests a Rule applies to. An empty field matches every
// request.
type Match struct {
	// Methods are the HTTP methods, such as GET or PUT.
	Methods []string
	// Bucket is the bucket, the first path segment.
	Bucket string
	// KeyPrefix and KeySuffix constrain the object key, the path after the
	// bucket. A bucket-level request (a listing) has an empty key, so a rule
	// with either set never matches one.
	KeyPrefix string
	KeySuffix string
	// ExceptKeySuffix leaves out keys with this suffix, such as backup.info,
	// from a rule that would match them otherwise.
	ExceptKeySuffix string
	// Query requires a query parameter to be present, such as list-type for a
	// ListObjectsV2 request or uploads for a multipart upload.
	Query string
	// UserAgent is a substring of the User-Agent header, which tells the
	// clients apart (minio-go, restic, Botocore for barman).
	UserAgent string
}

// matches reports whether a request falls under the Match.
func (m Match) matches(r *http.Request, bucket, key string) bool {
	if len(m.Methods) > 0 {
		found := false
		for _, method := range m.Methods {
			found = found || strings.EqualFold(method, r.Method)
		}
		if !found {
			return false
		}
	}
	if m.Bucket != "" && m.Bucket != bucket {
		return false
	}
	if (m.KeyPrefix != "" || m.KeySuffix != "") && key == "" {
		return false
	}
	if !strings.HasPrefix(key, m.KeyPrefix) || !strings.HasSuffix(key, m.KeySuffix) {
		return false
	}
	if m.ExceptKeySuffix != "" && strings.HasSuffix(key, m.ExceptKeySuffix) {
		return false
	}
	if m.Query != "" && !r.URL.Query().Has(m.Query) {
		return false
	}
	return m.UserAgent == "" || strings.Contains(r.UserAgent(), m.UserAgent)
}

// Rule is one fault. The first rule that matches a request decides what
// happens to it; a request no rule matches is forwarded unchanged.
type Rule struct {
	Match
	// Latency is waited before anything else happens to the request.
	Latency time.Duration
	// Status, when not zero, answers the request with this HTTP status and
	// an S3 error document carrying Code, without forwarding it.
	Status int
	// Code is the S3 error code of the error document, such as
	// ServiceUnavailable or AccessDenied. It defaults to one derived from
	// Status.
	Code string
	// Refuse closes the client's connection without a response and without
	// forwarding the request.
	Refuse bool
	// DropResponse forwards the request, waits for the upstream's answer, and
	// then closes the client's connection without passing it on.
	DropResponse bool
	// Hold blocks the request until Release is called on the rule's handle,
	// or the client's request is cancelled. After the release the request
	// goes on to the rule's other effects, or upstream when it has none.
	Hold bool
	// Times limits how many requests the rule applies to. Zero applies it to
	// every matching request until the rule is removed.
	Times int
}

// Handle refers to a rule added to a Proxy.
type Handle struct {
	proxy    *Proxy
	rule     Rule
	mu       sync.Mutex
	hits     int
	released chan struct{}
	once     sync.Once
}

// Hits returns how many requests the rule has applied to so far.
func (h *Handle) Hits() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hits
}

// Release lets every request the rule holds, and every later one, through.
func (h *Handle) Release() {
	h.once.Do(func() { close(h.released) })
}

// Remove takes the rule out of the proxy and releases what it holds.
func (h *Handle) Remove() {
	h.Release()
	h.proxy.mu.Lock()
	defer h.proxy.mu.Unlock()
	for i, r := range h.proxy.rules {
		if r == h {
			h.proxy.rules = append(h.proxy.rules[:i], h.proxy.rules[i+1:]...)
			return
		}
	}
}

// take reports whether the rule still applies, and counts the request when it
// does.
func (h *Handle) take() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rule.Times > 0 && h.hits >= h.rule.Times {
		return false
	}
	h.hits++
	return true
}

// Proxy forwards S3 requests to one upstream server and applies its rules.
// It is an http.Handler; serve it with httptest.NewServer or http.Server.
type Proxy struct {
	upstream *url.URL
	forward  *httputil.ReverseProxy
	mu       sync.Mutex
	rules    []*Handle
}

// New builds a Proxy in front of the S3 server at the upstream URL, such as
// http://127.0.0.1:9000. It returns an error when the URL doesn't parse.
//
// The proxy forwards each request with its Host header and path unchanged, so
// the SigV4 signature the client computed for the proxy's address stays
// valid only when the upstream does not check the host. RustFS and s3fake
// both accept it: the client signs the host it dials, and the upstream checks
// the signature against the Host header it receives, which is the same.
func New(upstream string) (*Proxy, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse upstream %q: %w", upstream, err)
	}
	forward := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			r.Out.Host = r.In.Host
			r.Out.URL.Path = r.In.URL.Path
			r.Out.URL.RawPath = r.In.URL.RawPath
		},
		FlushInterval: -1,
	}
	return &Proxy{upstream: target, forward: forward}, nil
}

// Add appends a rule and returns its handle. Rules are tried in the order
// they were added.
func (p *Proxy) Add(rule Rule) *Handle {
	h := &Handle{proxy: p, rule: rule, released: make(chan struct{})}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, h)
	return h
}

// Clear removes every rule and releases every held request.
func (p *Proxy) Clear() {
	p.mu.Lock()
	rules := p.rules
	p.rules = nil
	p.mu.Unlock()
	for _, h := range rules {
		h.Release()
	}
}

// split returns the bucket and key of a path-style request path.
func split(r *http.Request) (bucket, key string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ = strings.Cut(path, "/")
	return bucket, key
}

// rule returns the first rule that matches the request and still applies,
// or nil.
func (p *Proxy) rule(r *http.Request) *Handle {
	bucket, key := split(r)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.rules {
		if h.rule.matches(r, bucket, key) && h.take() {
			return h
		}
	}
	return nil
}

// ServeHTTP applies the first matching rule to the request and forwards what
// the rule lets through.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := p.rule(r)
	if h == nil {
		p.forward.ServeHTTP(w, r)
		return
	}
	rule := h.rule
	if rule.Latency > 0 {
		if !sleep(r.Context(), rule.Latency) {
			return
		}
	}
	if rule.Hold {
		select {
		case <-h.released:
		case <-r.Context().Done():
			return
		}
	}
	switch {
	case rule.Refuse:
		hangUp(w)
	case rule.Status != 0:
		writeError(w, r, rule.Status, rule.Code)
	case rule.DropResponse:
		p.forward.ServeHTTP(discard{header: http.Header{}}, r)
		hangUp(w)
	default:
		p.forward.ServeHTTP(w, r)
	}
}

// sleep waits for d, and returns false when the request is cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// hangUp closes the client's connection without writing a response. The
// client sees the connection reset or closed mid-request.
func hangUp(w http.ResponseWriter) {
	if hijacker, ok := w.(http.Hijacker); ok {
		if conn, _, err := hijacker.Hijack(); err == nil {
			_ = conn.Close()
			return
		}
	}
	panic(http.ErrAbortHandler)
}

// writeError answers with an S3 error document, in the form S3 servers send
// one: an XML Error element with the code, a message, the resource and a
// request ID.
func writeError(w http.ResponseWriter, r *http.Request, status int, code string) {
	if code == "" {
		code = map[int]string{
			http.StatusForbidden:           "AccessDenied",
			http.StatusNotFound:            "NoSuchKey",
			http.StatusInternalServerError: "InternalError",
			http.StatusServiceUnavailable:  "ServiceUnavailable",
			http.StatusBadGateway:          "BadGateway",
		}[status]
		if code == "" {
			code = "InternalError"
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<Error><Code>%s</Code><Message>injected by s3fault</Message><Resource>%s</Resource><RequestId>s3fault</RequestId></Error>`,
		code, r.URL.Path)
}

// discard is a ResponseWriter that throws the upstream's answer away, for a
// rule that drops the response after the write landed.
type discard struct{ header http.Header }

func (d discard) Header() http.Header       { return d.header }
func (discard) Write(b []byte) (int, error) { return len(b), nil }
func (discard) WriteHeader(int)             {}
