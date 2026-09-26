package s3fake_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
)

// TestProberRequestsMatchRecordedTranscripts runs the current
// bootstrap.S3Prober against the fake, loaded with each recorded store, and
// checks that it sends the requests its transcript recorded against RustFS:
// the same methods, paths and queries in the same order. The replay tests
// send the recorded requests, so they stay green when the prober changes a
// query; this test fails then, and the transcripts need recording again with
// hack/fixtures/barman-stores.sh.
//
// A continuation token is compared by the key it resumes after, since the
// fake's tokens carry their own marker.
func TestProberRequestsMatchRecordedTranscripts(t *testing.T) {
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		store := barmanstore.MustLoad(t, name)
		for server := range store.Verdicts {
			recorded, err := barmanstore.Transcript(name, server)
			if err != nil {
				t.Fatalf("transcript %s/%s: %v", name, server, err)
			}
			t.Run(name+"/"+server, func(t *testing.T) {
				srv := s3fake.New(t, "recorded")
				if err := store.Upload(srv, "recorded", name); err != nil {
					t.Fatal(err)
				}
				var (
					mu   sync.Mutex
					sent []string
				)
				logged := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					sent = append(sent, requestLine(t, r.Method, r.URL.EscapedPath(), r.URL.RawQuery))
					mu.Unlock()
					srv.ServeHTTP(w, r)
				}))
				t.Cleanup(logged.Close)

				at := bootstrap.Location{
					Endpoint: logged.URL, Bucket: "recorded", Prefix: name + "/" + server,
					AccessKey: srv.AccessKey, SecretKey: srv.SecretKey,
				}
				ctx := context.Background()
				if _, err := (bootstrap.S3Prober{}).HasBaseBackup(ctx, at); err != nil {
					t.Fatalf("HasBaseBackup: %v", err)
				}
				if _, err := (bootstrap.S3Prober{}).BaseBackups(ctx, at); err != nil {
					t.Fatalf("BaseBackups: %v", err)
				}

				want := make([]string, 0, len(recorded))
				for _, ex := range recorded {
					want = append(want, requestLine(t, ex.Method, ex.Path, ex.Query))
				}
				mu.Lock()
				defer mu.Unlock()
				for i := range max(len(sent), len(want)) {
					var got, rec string
					if i < len(sent) {
						got = sent[i]
					}
					if i < len(want) {
						rec = want[i]
					}
					if got != rec {
						t.Errorf("request #%d\nprober: %s\nrecorded: %s", i, got, rec)
					}
				}
			})
		}
	}
}

// requestLine returns a request's method, path and raw query as one line,
// with the value of a continuation-token parameter replaced by the key the
// token resumes after.
func requestLine(t *testing.T, method, path, rawQuery string) string {
	t.Helper()
	parts := strings.Split(rawQuery, "&")
	for i, p := range parts {
		escaped, ok := strings.CutPrefix(p, "continuation-token=")
		if !ok {
			continue
		}
		token, err := url.QueryUnescape(escaped)
		if err != nil {
			t.Fatalf("continuation token %q: %v", escaped, err)
		}
		parts[i] = "continuation-token=<after " + tokenKey(t, token) + ">"
	}
	return method + " " + path + "?" + strings.Join(parts, "&")
}
