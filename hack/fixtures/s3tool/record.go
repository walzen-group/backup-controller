package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
)

// kept reports whether the manifest keeps an object's full body: the files
// the controller or CloudNativePG read, as opposed to data and WAL.
func kept(key string) bool {
	return path.Base(key) == "backup.info" || strings.HasSuffix(key, ".history")
}

// writeManifest lists every object under prefix in the bucket and writes the
// store's manifest.json: each object's key relative to prefix, size, time and
// ETag, and the body of each kept file.
func writeManifest(ctx context.Context, endpoint, bucket, prefix, out string) error {
	c, err := client(endpoint, os.Getenv("AWS_ACCESS_KEY_ID"))
	if err != nil {
		return err
	}
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	m := barmanstore.Manifest{Store: strings.TrimSuffix(prefix, "/"), Objects: []barmanstore.Object{}}
	for info := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if info.Err != nil {
			return info.Err
		}
		o := barmanstore.Object{
			Key:          strings.TrimPrefix(info.Key, prefix),
			Size:         info.Size,
			LastModified: info.LastModified.UTC(),
			ETag:         `"` + strings.Trim(info.ETag, `"`) + `"`,
		}
		if kept(info.Key) {
			body, err := read(ctx, c, bucket, info.Key)
			if err != nil {
				return err
			}
			s := string(body)
			o.Body = &s
		}
		m.Objects = append(m.Objects, o)
	}
	sort.Slice(m.Objects, func(i, j int) bool { return m.Objects[i].Key < m.Objects[j].Key })
	return writeJSON(out, m)
}

// read downloads one object.
func read(ctx context.Context, c *minio.Client, bucket, key string) ([]byte, error) {
	o, err := c.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = o.Close() }()
	return io.ReadAll(o)
}

// recorder is a reverse proxy to RustFS that keeps every exchange passing
// through it, labelled with the step the recorder is in.
type recorder struct {
	mu        sync.Mutex
	step      string
	exchanges []barmanstore.Exchange
	addr      string
	stop      func()
}

// kept headers are the response headers a transcript keeps.
var keptHeaders = []string{"Content-Type", "Content-Length", "Content-Range", "ETag", "Last-Modified", "Accept-Ranges"}

// startRecorder serves a recording proxy to the endpoint on a loopback port.
func startRecorder(endpoint string) (*recorder, error) {
	target, err := url.Parse("http://" + endpoint)
	if err != nil {
		return nil, err
	}
	rec := &recorder{}
	forward := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(target)
		r.Out.Host = r.In.Host
		r.Out.URL.Path = r.In.URL.Path
		r.Out.URL.RawPath = r.In.URL.RawPath
	}}
	forward.ModifyResponse = func(resp *http.Response) error {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		req := resp.Request
		ex := barmanstore.Exchange{
			Method:  req.Method,
			Path:    req.URL.EscapedPath(),
			Query:   req.URL.RawQuery,
			Range:   req.Header.Get("Range"),
			Status:  resp.StatusCode,
			Headers: map[string]string{},
			Body:    string(body),
		}
		ex.WrongKey = strings.Contains(req.Header.Get("Authorization"), "Credential=wrong-key/")
		for _, h := range keptHeaders {
			if v := resp.Header.Get(h); v != "" {
				ex.Headers[h] = v
			}
		}
		rec.mu.Lock()
		ex.Step = rec.step
		rec.exchanges = append(rec.exchanges, ex)
		rec.mu.Unlock()
		return nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: forward} //nolint:gosec // a local recorder on loopback.
	go func() { _ = server.Serve(listener) }()
	rec.addr = listener.Addr().String()
	rec.stop = func() { _ = server.Close() }
	return rec, nil
}

// at sets the step the next exchanges are recorded under.
func (r *recorder) at(step string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.step = step
}

// writeTranscript runs the controller's S3 reader, bootstrap.S3Prober,
// against one server of a recorded store through a recording proxy, and
// writes every exchange to out: the requests HasBaseBackup and BaseBackups
// make, and RustFS's answers.
func writeTranscript(ctx context.Context, endpoint, bucket, prefix, server, out string) error {
	rec, err := startRecorder(endpoint)
	if err != nil {
		return err
	}
	defer rec.stop()

	at := bootstrap.Location{
		Endpoint:  "http://" + rec.addr,
		Bucket:    bucket,
		Prefix:    strings.Trim(prefix, "/") + "/" + server,
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}
	rec.at("prober-has-base-backup")
	if _, err := (bootstrap.S3Prober{}).HasBaseBackup(ctx, at); err != nil {
		return fmt.Errorf("HasBaseBackup: %w", err)
	}
	rec.at("prober-base-backups")
	if _, err := (bootstrap.S3Prober{}).BaseBackups(ctx, at); err != nil {
		return fmt.Errorf("BaseBackups: %w", err)
	}
	return writeJSON(out, rec.exchanges)
}

// writeBehaviour writes 1005 small objects and the odd keys under behaviour/
// in the bucket, and records how RustFS answers the requests a reader makes
// over them: listings in pages of 1000 and of 7, a delimited listing, a
// listing after a start key, raw listings over keys with a space, "+", "="
// and "&" (with and without encoding-type=url and fetch-owner=true), a byte
// range, a HEAD, a missing key, a missing bucket and an unknown access key.
// It writes rustfs-behaviour.json to out.
func writeBehaviour(ctx context.Context, endpoint, bucket, out string) error {
	direct, err := client(endpoint, os.Getenv("AWS_ACCESS_KEY_ID"))
	if err != nil {
		return err
	}
	for i := range 1005 {
		key := fmt.Sprintf("behaviour/dir-%d/object-%04d.txt", i%3, i)
		body := fmt.Sprintf("object %d\n", i)
		if _, err := direct.PutObject(ctx, bucket, key, strings.NewReader(body), int64(len(body)), minio.PutObjectOptions{}); err != nil {
			return err
		}
	}
	// Keys with characters a URL-encoded listing escapes: a space, "+", "="
	// and "&", in a key and in the prefixes a delimiter rolls up.
	for _, odd := range oddKeys {
		if _, err := direct.PutObject(ctx, bucket, odd, strings.NewReader("odd\n"), 4, minio.PutObjectOptions{}); err != nil {
			return err
		}
	}

	rec, err := startRecorder(endpoint)
	if err != nil {
		return err
	}
	defer rec.stop()
	c, err := client(rec.addr, os.Getenv("AWS_ACCESS_KEY_ID"))
	if err != nil {
		return err
	}
	drain := func(opts minio.ListObjectsOptions) error {
		for info := range c.ListObjects(ctx, bucket, opts) {
			if info.Err != nil {
				return info.Err
			}
		}
		return nil
	}
	steps := []struct {
		name string
		run  func() error
	}{
		{"list-all", func() error { return drain(minio.ListObjectsOptions{Prefix: "behaviour/", Recursive: true}) }},
		{"list-pages-of-7", func() error {
			return drain(minio.ListObjectsOptions{Prefix: "behaviour/dir-0/object-00", Recursive: true, MaxKeys: 7})
		}},
		{"list-delimited", func() error { return drain(minio.ListObjectsOptions{Prefix: "behaviour/"}) }},
		{"list-start-after", func() error {
			return drain(minio.ListObjectsOptions{Prefix: "behaviour/dir-1/", Recursive: true, StartAfter: "behaviour/dir-1/object-0990.txt"})
		}},
		{"list-odd-key", func() error { return drain(minio.ListObjectsOptions{Prefix: "behaviour/dir-0/a ", Recursive: true}) }},
		// Raw ListObjectsV2 requests over the odd keys, with a prefix,
		// delimiter and start key that hold odd characters too: with and
		// without encoding-type=url, and with and without fetch-owner=true.
		{"raw-list-odd-amp-url", func() error { return rawList(ctx, rec.addr, bucket, oddQuery("&", true, false)) }},
		{"raw-list-odd-amp-url-owner", func() error { return rawList(ctx, rec.addr, bucket, oddQuery("&", true, true)) }},
		{"raw-list-odd-amp-plain", func() error { return rawList(ctx, rec.addr, bucket, oddQuery("&", false, false)) }},
		{"raw-list-odd-slash-url", func() error { return rawList(ctx, rec.addr, bucket, oddQuery("/", true, false)) }},
		{"raw-list-odd-slash-plain", func() error { return rawList(ctx, rec.addr, bucket, oddQuery("/", false, false)) }},
		{"get-range", func() error {
			opts := minio.GetObjectOptions{}
			if err := opts.SetRange(2, 5); err != nil {
				return err
			}
			o, err := c.GetObject(ctx, bucket, "behaviour/dir-0/object-0000.txt", opts)
			if err != nil {
				return err
			}
			_, err = io.ReadAll(o)
			return errors.Join(err, o.Close())
		}},
		{"head", func() error {
			_, err := c.StatObject(ctx, bucket, "behaviour/dir-0/object-0000.txt", minio.StatObjectOptions{})
			return err
		}},
		{"get-missing-key", func() error {
			o, err := c.GetObject(ctx, bucket, "behaviour/missing", minio.GetObjectOptions{})
			if err != nil {
				return err
			}
			_, err = io.ReadAll(o)
			_ = o.Close()
			return expectError(err)
		}},
		{"list-missing-bucket", func() error {
			return expectError(drain2(ctx, c, "no-such-bucket"))
		}},
		{"list-wrong-key", func() error {
			wrong, err := client(rec.addr, "wrong-key")
			if err != nil {
				return err
			}
			return expectError(drain2(ctx, wrong, bucket))
		}},
	}
	for _, s := range steps {
		rec.at(s.name)
		if err := s.run(); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
	}

	b := barmanstore.Behaviour{Bucket: bucket, Exchanges: rec.exchanges, Objects: []barmanstore.Object{}}
	for info := range direct.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: "behaviour/", Recursive: true}) {
		if info.Err != nil {
			return info.Err
		}
		body, err := read(ctx, direct, bucket, info.Key)
		if err != nil {
			return err
		}
		s := string(body)
		b.Objects = append(b.Objects, barmanstore.Object{
			Key: info.Key, Size: info.Size, LastModified: info.LastModified.UTC(),
			ETag: `"` + strings.Trim(info.ETag, `"`) + `"`, Body: &s,
		})
	}
	return writeJSON(out, b)
}

// drain2 lists a bucket's root with a client and returns the listing's error.
func drain2(ctx context.Context, c *minio.Client, bucket string) error {
	for info := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: "behaviour/", Recursive: true}) {
		if info.Err != nil {
			return info.Err
		}
	}
	return nil
}

// expectError turns a missing error into one, for the steps that must fail.
func expectError(err error) error {
	if err == nil {
		return errors.New("the request succeeded, want an error")
	}
	return nil
}

// oddKeys are the behaviour keys with characters a URL-encoded listing
// escapes: a space, "+", "=" and "&", in the key's last part and in the
// part a delimiter of "/" or "&" rolls up into a common prefix.
var oddKeys = []string{
	"behaviour/dir-0/a key+with=odd&chars.txt",
	"behaviour/odd dir+a=b&c/x y.txt",
	"behaviour/odd dir+a=b&c/p+q=r&s.txt",
	"behaviour/odd file+1=2.txt",
}

// oddQuery builds the query of a ListObjectsV2 over the odd keys: prefix
// "behaviour/odd ", the given delimiter, and start-after "behaviour/odd a+b=c",
// a key that sorts before every odd key and holds "+" and "=".
//
// Parameters:
//   - delimiter: the delimiter to send.
//   - encoded: whether to send encoding-type=url.
//   - owner: whether to send fetch-owner=true.
//
// It returns the query string, with a space escaped as %20.
func oddQuery(delimiter string, encoded, owner bool) string {
	v := url.Values{}
	v.Set("list-type", "2")
	v.Set("prefix", "behaviour/odd ")
	v.Set("delimiter", delimiter)
	v.Set("start-after", "behaviour/odd a+b=c")
	if encoded {
		v.Set("encoding-type", "url")
	}
	if owner {
		v.Set("fetch-owner", "true")
	}
	return strings.ReplaceAll(v.Encode(), "+", "%20")
}

// rawList sends one ListObjectsV2 request with exactly the given query,
// signed with the environment's credentials, to the bucket at endpoint. The
// recorder uses it for the queries minio-go never sends, such as a listing
// without encoding-type=url.
//
// It returns an error when the request fails or the answer is not 200 OK.
func rawList(ctx context.Context, endpoint, bucket, query string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+endpoint+"/"+bucket+"?"+query, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Amz-Content-Sha256", emptySHA256)
	req = signer.SignV4(*req, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	return nil
}

// emptySHA256 is the hex SHA-256 of an empty body, the payload hash of a GET.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
