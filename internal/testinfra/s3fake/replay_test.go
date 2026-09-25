package s3fake_test

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
)

// TestReplayRustFSBehaviour replays rustfs-behaviour.json, RustFS 1.0.0's
// answers to paging, delimiter, start-after, range, HEAD and error requests,
// against the fake loaded with the same objects.
func TestReplayRustFSBehaviour(t *testing.T) {
	b, err := barmanstore.RustFSBehaviour()
	if err != nil {
		t.Fatal(err)
	}
	srv := s3fake.New(t, b.Bucket)
	for _, o := range b.Objects {
		obj := s3fake.Object{Size: o.Size, LastModified: o.LastModified, ETag: o.ETag}
		if o.Body != nil {
			obj.Body = []byte(*o.Body)
		}
		srv.Put(b.Bucket, o.Key, obj)
	}
	if len(b.Exchanges) == 0 {
		t.Fatal("the recording has no exchanges")
	}
	replay(t, srv, b.Exchanges)
}

// TestReplayRecordedStoreTranscripts replays every transcript-*.json of the
// recorded barman stores, the requests bootstrap.S3Prober made against
// RustFS, against the fake loaded with the store under its own name.
func TestReplayRecordedStoreTranscripts(t *testing.T) {
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		store := barmanstore.MustLoad(t, name)
		for server := range store.Verdicts {
			exchanges, err := barmanstore.Transcript(name, server)
			if err != nil {
				t.Fatalf("transcript %s/%s: %v", name, server, err)
			}
			t.Run(name+"/"+server, func(t *testing.T) {
				srv := s3fake.New(t, "recorded")
				// Store.Upload drops the recorded ETag, so the objects go in
				// here with it.
				for _, o := range store.Objects {
					obj := s3fake.Object{Size: o.Size, LastModified: o.LastModified, ETag: o.ETag}
					if o.Body != nil {
						obj.Body = []byte(*o.Body)
					}
					srv.Put("recorded", name+"/"+o.Key, obj)
				}
				replay(t, srv, exchanges)
			})
		}
	}
}

// replay sends each recorded request to the fake as recorded, with the
// continuation tokens RustFS gave (the fake takes them), and compares the
// answer: the status and the headers the recording kept, and the body, field
// by field for a listing and byte for byte otherwise. A listing's
// Content-Length is left out, since the fake escapes the quotes in an ETag.
func replay(t *testing.T, srv *s3fake.Server, exchanges []barmanstore.Exchange) {
	for i, ex := range exchanges {
		target := srv.URL + ex.Path
		if ex.Query != "" {
			target += "?" + ex.Query
		}
		req, err := http.NewRequest(ex.Method, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		key := srv.AccessKey
		if ex.WrongKey {
			key = "wrong-access-key"
		}
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+key+
			"/20260925/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0")
		if ex.Range != "" {
			req.Header.Set("Range", ex.Range)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		where := func() string { return fmt.Sprintf("%s #%d %s %s?%s", ex.Step, i, ex.Method, ex.Path, ex.Query) }
		if resp.StatusCode != ex.Status {
			t.Errorf("%s: status %d, RustFS %d", where(), resp.StatusCode, ex.Status)
		}
		listing := strings.Contains(ex.Query, "list-type=2") && ex.Status == http.StatusOK
		for name, want := range ex.Headers {
			if listing && name == "Content-Length" {
				continue
			}
			if got := resp.Header.Get(name); got != want {
				t.Errorf("%s: header %s %q, RustFS %q", where(), name, got, want)
			}
		}
		if listing {
			got, want := parseListing(t, body), parseListing(t, []byte(ex.Body))
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s: listing differs\nfake:   %+v\nRustFS: %+v", where(), got, want)
			}
			continue
		}
		if string(body) != ex.Body {
			t.Errorf("%s: body\nfake:   %q\nRustFS: %q", where(), body, ex.Body)
		}
	}
}

// listing is a ListBucketResult reduced to what a client reads. The
// continuation tokens are replaced by the key they resume after.
type listing struct {
	Name                  string `xml:"Name"`
	Prefix                string `xml:"Prefix"`
	MaxKeys               int    `xml:"MaxKeys"`
	KeyCount              int    `xml:"KeyCount"`
	ContinuationToken     string `xml:"ContinuationToken"`
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		ETag         string `xml:"ETag"`
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		Owner        struct {
			DisplayName string `xml:"DisplayName"`
			ID          string `xml:"ID"`
		} `xml:"Owner"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	} `xml:"Contents"`
	CommonPrefixes []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
	Delimiter    string `xml:"Delimiter"`
	EncodingType string `xml:"EncodingType"`
	StartAfter   string `xml:"StartAfter"`
}

// parseListing decodes a ListBucketResult body and replaces its tokens by the
// key they name.
func parseListing(t *testing.T, body []byte) listing {
	t.Helper()
	var l listing
	if err := xml.Unmarshal(body, &l); err != nil {
		t.Fatalf("decode listing %q: %v", body, err)
	}
	l.ContinuationToken = tokenKey(t, l.ContinuationToken)
	l.NextContinuationToken = tokenKey(t, l.NextContinuationToken)
	return l
}

// tokenKey returns the key a continuation token resumes after: the decoded
// token up to its bracketed marker.
func tokenKey(t *testing.T, token string) string {
	t.Helper()
	if token == "" {
		return ""
	}
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("token %q: %v", token, err)
	}
	s := string(raw)
	if i := strings.LastIndex(s, "["); i >= 0 && strings.HasSuffix(s, "]") {
		return s[:i]
	}
	t.Fatalf("token %q has no bracketed marker", s)
	return ""
}
