package s3fault_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fault"
)

const bucket = "store"

// fixture is an s3fake server with a few objects and an s3fault proxy in
// front of it.
type fixture struct {
	fake  *s3fake.Server
	proxy *s3fault.Proxy
	front *httptest.Server
}

// newFixture starts the fake, fills it and puts a proxy in front.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	fake := s3fake.New(t, bucket)
	fake.Put(bucket, "cluster/base/backup.info", s3fake.Object{Body: []byte("backup_label='x'\n")})
	fake.Put(bucket, "cluster/base/data.tar", s3fake.Object{Body: bytes.Repeat([]byte("0123456789"), 1000)})
	fake.Put(bucket, "cluster/wals/000000010000000000000001", s3fake.Object{Size: 16})
	proxy, err := s3fault.New(fake.URL)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(proxy)
	t.Cleanup(front.Close)
	return &fixture{fake: fake, proxy: proxy, front: front}
}

// client builds a minio client for a URL the way bootstrap.S3Prober does,
// with a fixed region so no location request comes first, and with the
// given retry budget (1 means a single attempt).
func (f *fixture) client(t *testing.T, url string, retries int) *minio.Client {
	t.Helper()
	c, err := minio.New(strings.TrimPrefix(url, "http://"), &minio.Options{
		Creds:      credentials.NewStaticV4(f.fake.AccessKey, f.fake.SecretKey, ""),
		Region:     "us-east-1",
		MaxRetries: retries,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// ctx returns a context that ends with the test or after ten seconds.
func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

// list returns the keys and sizes minio lists under a prefix.
func list(t *testing.T, c *minio.Client) ([]minio.ObjectInfo, error) {
	t.Helper()
	var out []minio.ObjectInfo
	for o := range c.ListObjects(ctx(t), bucket, minio.ListObjectsOptions{Prefix: "cluster/", Recursive: true}) {
		if o.Err != nil {
			return out, o.Err
		}
		out = append(out, o)
	}
	return out, nil
}

// get reads one object through minio.
func get(t *testing.T, c *minio.Client, key string) ([]byte, error) {
	t.Helper()
	obj, err := c.GetObject(ctx(t), bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}

// raw sends one unsigned-but-keyed request and returns the status and body.
// s3fake checks only the access key, so a bare Authorization header is enough.
func raw(t *testing.T, base, path, access string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx(t), http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+access+"/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func TestNoRulesIsTransparent(t *testing.T) {
	f := newFixture(t)
	direct := f.client(t, f.fake.URL, 1)
	proxied := f.client(t, f.front.URL, 1)

	want, err := list(t, direct)
	if err != nil {
		t.Fatal(err)
	}
	got, err := list(t, proxied)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) || len(want) != 3 {
		t.Fatalf("listing through the proxy has %d objects, direct %d", len(got), len(want))
	}
	for i := range want {
		w, g := want[i], got[i]
		if w.Key != g.Key || w.Size != g.Size || w.ETag != g.ETag || !w.LastModified.Equal(g.LastModified) {
			t.Errorf("object %d: proxy %+v, direct %+v", i, g, w)
		}
	}

	for _, key := range []string{"cluster/base/backup.info", "cluster/base/data.tar"} {
		w, err := get(t, direct, key)
		if err != nil {
			t.Fatal(err)
		}
		g, err := get(t, proxied, key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(w, g) {
			t.Errorf("GET %s through the proxy differs from direct", key)
		}
	}

	// The raw HTTP bodies match byte for byte too: a listing, a GET, a
	// ranged GET and an error document.
	for _, path := range []string{
		"/" + bucket + "?list-type=2&prefix=cluster%2F",
		"/" + bucket + "/cluster/base/data.tar",
		"/" + bucket + "/cluster/base/missing",
		"/nobucket?list-type=2",
	} {
		ws, wb := raw(t, f.fake.URL, path, f.fake.AccessKey)
		gs, gb := raw(t, f.front.URL, path, f.fake.AccessKey)
		if ws != gs || !bytes.Equal(wb, gb) {
			t.Errorf("GET %s: proxy %d %q, direct %d %q", path, gs, gb, ws, wb)
		}
	}
}

func TestLatency(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, f.front.URL, 1)
	h := f.proxy.Add(s3fault.Rule{Match: s3fault.Match{KeySuffix: "backup.info"}, Latency: 300 * time.Millisecond})

	start := time.Now()
	if _, err := get(t, c, "cluster/base/data.tar"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("unmatched GET took %v", d)
	}

	start = time.Now()
	body, err := get(t, c, "cluster/base/backup.info")
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Errorf("matched GET took %v, want at least 300ms", d)
	}
	if string(body) != "backup_label='x'\n" {
		t.Errorf("body %q after latency", body)
	}
	if h.Hits() != 1 {
		t.Errorf("hits %d, want 1", h.Hits())
	}
}

func TestErrorStatus(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, f.front.URL, 1)
	f.proxy.Add(s3fault.Rule{Match: s3fault.Match{Methods: []string{"GET"}, Query: "list-type"}, Status: http.StatusServiceUnavailable})
	f.proxy.Add(s3fault.Rule{Match: s3fault.Match{KeyPrefix: "cluster/base/"}, Status: http.StatusForbidden, Code: "AccessDenied"})

	_, err := list(t, c)
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Code != "ServiceUnavailable" {
		t.Errorf("listing: status %d code %q (%v), want 503 ServiceUnavailable", resp.StatusCode, resp.Code, err)
	}

	_, err = get(t, c, "cluster/base/backup.info")
	resp = minio.ToErrorResponse(err)
	if resp.StatusCode != http.StatusForbidden || resp.Code != "AccessDenied" {
		t.Errorf("GET: status %d code %q (%v), want 403 AccessDenied", resp.StatusCode, resp.Code, err)
	}

	_, err = c.StatObject(ctx(t), bucket, "cluster/base/backup.info", minio.StatObjectOptions{})
	resp = minio.ToErrorResponse(err)
	if resp.StatusCode != http.StatusForbidden || resp.Code != "AccessDenied" {
		t.Errorf("HEAD: status %d code %q (%v), want 403 AccessDenied", resp.StatusCode, resp.Code, err)
	}

	// A key outside the rule still reads.
	if _, err := get(t, c, "cluster/wals/000000010000000000000001"); err != nil {
		t.Errorf("unmatched GET: %v", err)
	}
}

func TestErrorStatusTimesLetsRetrySucceed(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, f.front.URL, 3)
	h := f.proxy.Add(s3fault.Rule{Match: s3fault.Match{KeySuffix: "backup.info"}, Status: http.StatusServiceUnavailable, Times: 1})

	body, err := get(t, c, "cluster/base/backup.info")
	if err != nil {
		t.Fatalf("minio did not get through after one 503: %v", err)
	}
	if string(body) != "backup_label='x'\n" {
		t.Errorf("body %q", body)
	}
	if h.Hits() != 1 {
		t.Errorf("hits %d, want 1", h.Hits())
	}
}

func TestRefuse(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, f.front.URL, 1)
	h := f.proxy.Add(s3fault.Rule{Match: s3fault.Match{Methods: []string{"PUT"}}, Refuse: true})

	_, err := c.PutObject(ctx(t), bucket, "cluster/new", strings.NewReader("hello"), 5, minio.PutObjectOptions{})
	if err == nil {
		t.Fatal("PUT through a refusing rule succeeded")
	}
	if minio.ToErrorResponse(err).StatusCode != 0 {
		t.Errorf("refused PUT got an HTTP answer: %v", err)
	}
	if _, ok := f.fake.Get(bucket, "cluster/new"); ok {
		t.Error("refused PUT reached the upstream")
	}
	if h.Hits() != 1 {
		t.Errorf("hits %d, want 1", h.Hits())
	}
	if _, err := get(t, c, "cluster/base/backup.info"); err != nil {
		t.Errorf("GET beside a PUT rule: %v", err)
	}
}

func TestDropResponse(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, f.front.URL, 1)
	f.proxy.Add(s3fault.Rule{Match: s3fault.Match{Methods: []string{"PUT"}, KeyPrefix: "cluster/new"}, DropResponse: true})

	_, err := c.PutObject(ctx(t), bucket, "cluster/new", strings.NewReader("hello"), 5, minio.PutObjectOptions{})
	if err == nil {
		t.Fatal("PUT through a dropping rule succeeded")
	}
	if minio.ToErrorResponse(err).StatusCode != 0 {
		t.Errorf("dropped PUT got an HTTP answer: %v", err)
	}
	obj, ok := f.fake.Get(bucket, "cluster/new")
	if !ok || string(obj.Body) != "hello" {
		t.Errorf("dropped PUT did not land upstream: %v %q", ok, obj.Body)
	}
}

// waitForHits waits until the rule behind h has applied to n requests. For a
// held rule that means the requests have reached the hold, since the proxy
// counts a request when it picks the rule, before it waits.
//
// It fails the test when a request finishes on done first, or when 5 seconds
// pass.
func waitForHits(t *testing.T, h *s3fault.Handle, n int, done <-chan error) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for h.Hits() < n {
		select {
		case err := <-done:
			t.Fatalf("held request finished before it reached the hold: %v", err)
		case <-deadline:
			t.Fatalf("hits %d after 5s, want %d", h.Hits(), n)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestHold(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, f.front.URL, 1)
	h := f.proxy.Add(s3fault.Rule{Match: s3fault.Match{KeySuffix: "backup.info"}, Hold: true})

	done := make(chan error, 1)
	go func() {
		_, err := get(t, c, "cluster/base/backup.info")
		done <- err
	}()
	waitForHits(t, h, 1, done)
	select {
	case err := <-done:
		t.Fatalf("held GET finished before the release: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if h.Hits() != 1 {
		t.Errorf("hits %d while held, want 1", h.Hits())
	}
	h.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("released GET: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("released GET did not finish")
	}

	// A held request with an error status gets the error after the release.
	f.proxy.Clear()
	h = f.proxy.Add(s3fault.Rule{Match: s3fault.Match{KeySuffix: "backup.info"}, Hold: true, Status: http.StatusServiceUnavailable})
	go func() {
		_, err := get(t, c, "cluster/base/backup.info")
		done <- err
	}()
	waitForHits(t, h, 1, done)
	h.Remove()
	select {
	case err := <-done:
		if minio.ToErrorResponse(err).StatusCode != http.StatusServiceUnavailable {
			t.Errorf("held-then-released GET with a status: %v, want 503", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GET did not finish after Remove")
	}

	// A client that gives up leaves the hold.
	f.proxy.Add(s3fault.Rule{Match: s3fault.Match{KeySuffix: "backup.info"}, Hold: true})
	short, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := c.StatObject(short, bucket, "cluster/base/backup.info", minio.StatObjectOptions{})
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "deadline") {
		t.Errorf("held HEAD with a deadline: %v", err)
	}
}
