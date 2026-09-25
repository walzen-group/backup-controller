package bootstrap

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// fakeS3 starts an HTTP server that answers the three S3 calls S3Prober makes
// (GetBucketLocation, ListObjectsV2 and GetObject) for one bucket, over the
// objects given as key to contents. Every other request gets a 400. The server
// stops when the test ends.
func fakeS3(t *testing.T, bucket string, objects map[string]string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		name, key, _ := strings.Cut(path, "/")
		if name != bucket {
			http.Error(w, "no such bucket", http.StatusNotFound)
			return
		}
		query := r.URL.Query()
		switch {
		case key == "" && query.Has("location"):
			_, _ = fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
		case key == "" && query.Get("list-type") == "2":
			writeListing(w, bucket, query.Get("prefix"), objects)
		case key != "" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
			body, ok := objects[key]
			if !ok {
				http.Error(w, "no such key", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			w.Header().Set("Last-Modified", "Mon, 21 Sep 2026 00:00:00 GMT")
			w.Header().Set("ETag", `"fake"`)
			if r.Method == http.MethodGet {
				_, _ = fmt.Fprint(w, body)
			}
		default:
			http.Error(w, "the fake does not serve "+r.URL.String(), http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// writeListing writes a ListObjectsV2 result holding every object whose key
// starts with prefix, in key order, in one untruncated page.
func writeListing(w http.ResponseWriter, bucket, prefix string, objects map[string]string) {
	type content struct {
		Key          string
		Size         int
		LastModified string
		ETag         string
	}
	type result struct {
		XMLName     xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
		Name        string
		Prefix      string
		KeyCount    int
		MaxKeys     int
		IsTruncated bool
		Contents    []content
	}

	var keys []string
	for key := range objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	out := result{Name: bucket, Prefix: prefix, KeyCount: len(keys), MaxKeys: 1000}
	for _, key := range keys {
		out.Contents = append(out.Contents, content{
			Key:          key,
			Size:         len(objects[key]),
			LastModified: "2026-09-21T00:00:00.000Z",
			ETag:         `"fake"`,
		})
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(out)
}

// storeAt builds the ObjectStore from store() with its endpointURL set to the
// given URL, so ResolveLocation points S3Prober at a fake S3 server.
func storeAt(endpoint string) *unstructured.Unstructured {
	s := store()
	_ = unstructured.SetNestedField(s.Object, endpoint, "spec", "configuration", "endpointURL")
	return s
}
