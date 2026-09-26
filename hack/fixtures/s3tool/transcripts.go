package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
)

// rustfsWriter is a barmanstore.Writer over a minio client for RustFS. It
// uploads an object recorded without a body (data and WAL) as zeros of its
// recorded size, so listings show the recorded size.
type rustfsWriter struct {
	ctx context.Context
	c   *minio.Client
}

// PutObject uploads body, or size zero bytes when body is nil. RustFS sets
// the ETag and the time itself, so modified is not used.
func (w rustfsWriter) PutObject(bucket, key string, body []byte, size int64, _ time.Time) error {
	var r io.Reader = io.LimitReader(zeros{}, size)
	if body != nil {
		r, size = strings.NewReader(string(body)), int64(len(body))
	}
	_, err := w.c.PutObject(w.ctx, bucket, key, r, size, minio.PutObjectOptions{})
	return err
}

// zeros reads as an endless run of zero bytes.
type zeros struct{}

// Read fills p with zeros.
func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// stamp is what a store object carries in RustFS after an upload, and what
// the manifest recorded for it.
type stamp struct {
	freshETag, freshTime string // as RustFS reports them now
	etag, lastModified   string // as recorded
	freshHTTP, httpTime  string // the Last-Modified header forms
}

// writeTranscripts re-records the transcript of every server of every
// recorded store: it writes each store from the embedded manifests into the
// bucket under its name, runs bootstrap.S3Prober against it through the
// recording proxy as writeTranscript does, and writes
// <out>/<store>/transcript-<server>.json. The manifests and verdicts stay as
// they are.
//
// RustFS picks an uploaded object's ETag and time itself, and the upload
// can't send barman's data, so the ETags and times RustFS answers with would
// differ from the manifest the replay tests load into s3fake. Each ETag and
// Last-Modified in a recorded answer (listing elements and GET or HEAD
// headers) is therefore put back to the manifest's value for that key. Every
// other byte of each answer is RustFS's.
//
// It returns the first upload, listing, prober or write error.
func writeTranscripts(ctx context.Context, endpoint, bucket, out string) error {
	c, err := client(endpoint, os.Getenv("AWS_ACCESS_KEY_ID"))
	if err != nil {
		return err
	}
	names, err := barmanstore.Names()
	if err != nil {
		return err
	}
	for _, name := range names {
		store, err := barmanstore.Load(name)
		if err != nil {
			return err
		}
		if err := store.Upload(rustfsWriter{ctx: ctx, c: c}, bucket, name); err != nil {
			return fmt.Errorf("upload %s: %w", name, err)
		}
		// A listing gives each time in milliseconds, as a listing answer
		// shows it; a HEAD gives whole seconds only.
		listed := map[string]minio.ObjectInfo{}
		for info := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: name + "/", Recursive: true}) {
			if info.Err != nil {
				return fmt.Errorf("list %s: %w", name, info.Err)
			}
			listed[info.Key] = info
		}
		stamps := map[string]stamp{}
		for _, o := range store.Objects {
			key := name + "/" + o.Key
			info, ok := listed[key]
			if !ok {
				return fmt.Errorf("%s is missing after the upload", key)
			}
			stamps[key] = stamp{
				freshETag:    `"` + strings.Trim(info.ETag, `"`) + `"`,
				freshTime:    info.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
				freshHTTP:    info.LastModified.UTC().Format(http.TimeFormat),
				etag:         o.ETag,
				lastModified: o.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
				httpTime:     o.LastModified.UTC().Format(http.TimeFormat),
			}
		}
		servers := make([]string, 0, len(store.Verdicts))
		for server := range store.Verdicts {
			servers = append(servers, server)
		}
		sort.Strings(servers)
		for _, server := range servers {
			exchanges, err := recordTranscript(ctx, endpoint, bucket, name, server)
			if err != nil {
				return fmt.Errorf("%s/%s: %w", name, server, err)
			}
			for i := range exchanges {
				restamp(&exchanges[i], bucket, stamps)
			}
			if err := writeJSON(filepath.Join(out, name, "transcript-"+server+".json"), exchanges); err != nil {
				return err
			}
		}
	}
	return nil
}

// restamp puts the recorded ETag and time back into one exchange: in the
// ETag and Last-Modified headers of a request for a store object, and in
// each listing element of a store object.
func restamp(ex *barmanstore.Exchange, bucket string, stamps map[string]stamp) {
	if s, ok := stamps[strings.TrimPrefix(ex.Path, "/"+bucket+"/")]; ok {
		if ex.Headers["ETag"] == s.freshETag {
			ex.Headers["ETag"] = s.etag
		}
		if ex.Headers["Last-Modified"] == s.freshHTTP {
			ex.Headers["Last-Modified"] = s.httpTime
		}
	}
	if !strings.Contains(ex.Body, "<Contents>") {
		return
	}
	for key, s := range stamps {
		element := "<ETag>" + s.freshETag + "</ETag><Key>" + key + "</Key><LastModified>" + s.freshTime + "</LastModified>"
		ex.Body = strings.ReplaceAll(ex.Body, element,
			"<ETag>"+s.etag+"</ETag><Key>"+key+"</Key><LastModified>"+s.lastModified+"</LastModified>")
	}
}
