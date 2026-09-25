// Package s3fake is an in-memory S3 server for the fast test tier. It stands
// in for RustFS 1.0.0, the S3 implementation walzen prod stores its backups
// on, so that the controller's S3 readers (bootstrap.S3Prober and
// restic.S3Store, both through minio-go) run against a server in unit-test
// time.
//
// It models the calls those readers make, as RustFS answers them: the bucket
// location, ListObjectsV2 with prefix, delimiter, start-after, a page of at
// most 1000 keys and continuation tokens, and URL encoding of the keys when
// the client asks for it; GetObject with byte ranges, HeadObject, PutObject
// (plain and aws-chunked bodies) and DeleteObject; and RustFS's error
// documents for a missing bucket, a missing key and an unknown access key.
// The recordings in the barmanstore package hold RustFS's own answers to the
// same requests, and this package's tests replay them against the fake.
//
// It deliberately does not model: signature verification (only the access
// key in the Authorization header is checked), multipart uploads, versioning,
// virtual-hosted addressing, TLS, latency or failures (put an s3fault.Proxy in
// front for those), or eventual consistency.
//
// It is test support: only tests import it.
package s3fake

import (
	"bufio"
	"bytes"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 digests of the body.
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Object is one stored object.
type Object struct {
	// Body is the content. When it is nil, the object is a placeholder of
	// Size zero bytes, which is how the recorded stores keep data files whose
	// content no reader looks at.
	Body []byte
	// Size is the length the listing reports. It is len(Body) when Body is
	// set.
	Size int64
	// LastModified is the time the listing and the headers report.
	LastModified time.Time
	// ETag is the quoted entity tag. It defaults to the MD5 of the body.
	ETag string
}

// Server is a running fake S3 server. Its URL field is the endpoint to give
// a client, such as http://127.0.0.1:41234.
type Server struct {
	*httptest.Server
	// AccessKey and SecretKey are the credentials the server accepts. Only
	// the access key is checked.
	AccessKey string
	SecretKey string

	mu      sync.Mutex
	buckets map[string]map[string]Object
	clock   func() time.Time
}

// New starts a fake S3 server with the credentials "fake-access-key" and
// "fake-secret-key" and the given buckets, each empty. The server stops when
// the test ends.
func New(t testing.TB, buckets ...string) *Server {
	t.Helper()
	s := &Server{
		AccessKey: "fake-access-key",
		SecretKey: "fake-secret-key",
		buckets:   map[string]map[string]Object{},
		clock:     func() time.Time { return time.Now().UTC().Truncate(time.Millisecond) },
	}
	for _, b := range buckets {
		s.buckets[b] = map[string]Object{}
	}
	s.Server = httptest.NewServer(s)
	t.Cleanup(s.Close)
	return s
}

// Endpoint returns the server's address without the scheme, the form
// minio.New takes.
func (s *Server) Endpoint() string {
	return strings.TrimPrefix(s.URL, "http://")
}

// CreateBucket adds an empty bucket, or leaves an existing one as it is.
func (s *Server) CreateBucket(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets[name] == nil {
		s.buckets[name] = map[string]Object{}
	}
}

// Put stores one object in a bucket, creating the bucket when it is missing.
// A zero LastModified is set to the current time, and an empty ETag to the
// MD5 of the body.
func (s *Server) Put(bucket, key string, o Object) {
	if o.Body != nil {
		o.Size = int64(len(o.Body))
	}
	if o.LastModified.IsZero() {
		o.LastModified = s.clock()
	}
	if o.ETag == "" {
		o.ETag = etag(o.content())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets[bucket] == nil {
		s.buckets[bucket] = map[string]Object{}
	}
	s.buckets[bucket][key] = o
}

// PutObject stores one object for the builders that write into a store; it
// satisfies barmanstore.Writer.
func (s *Server) PutObject(bucket, key string, body []byte, size int64, modified time.Time) error {
	s.Put(bucket, key, Object{Body: body, Size: size, LastModified: modified})
	return nil
}

// Keys returns the keys of a bucket that start with prefix, in order.
func (s *Server) Keys(bucket, prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.buckets[bucket] {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// Get returns one object and whether it exists.
func (s *Server) Get(bucket, key string) (Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.buckets[bucket][key]
	return o, ok
}

// content returns the object's bytes, zeros for a placeholder.
func (o Object) content() []byte {
	if o.Body != nil {
		return o.Body
	}
	return make([]byte, o.Size)
}

// etag returns the quoted MD5 of a body, the ETag of a single-part upload.
func etag(body []byte) string {
	sum := md5.Sum(body) //nolint:gosec // S3 ETags are MD5 digests.
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// ServeHTTP answers one S3 request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", "s3fake")
	if !s.authorized(r) {
		s.fail(w, r, http.StatusForbidden, "InvalidAccessKeyId", "The Access Key Id you provided does not exist in our records.", "", "")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	query := r.URL.Query()

	s.mu.Lock()
	objects, exists := s.buckets[bucket]
	s.mu.Unlock()
	if bucket == "" {
		s.fail(w, r, http.StatusNotImplemented, "NotImplemented", "The fake does not list buckets.", "", "")
		return
	}
	if !exists && (key != "" || r.Method != http.MethodPut) {
		s.fail(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", bucket, "")
		return
	}

	switch {
	case key == "" && r.Method == http.MethodPut:
		s.CreateBucket(bucket)
		w.WriteHeader(http.StatusOK)
	case key == "" && r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case key == "" && r.Method == http.MethodGet && query.Has("location"):
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, xml.Header+`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
	case key == "" && r.Method == http.MethodGet && query.Get("list-type") == "2":
		s.list(w, r, bucket, objects)
	case key == "":
		s.fail(w, r, http.StatusNotImplemented, "NotImplemented", "The fake does not serve "+r.Method+" "+r.URL.RawQuery+" on a bucket.", bucket, "")
	case query.Has("uploads") || query.Has("uploadId"):
		s.fail(w, r, http.StatusNotImplemented, "NotImplemented", "The fake does not take multipart uploads.", bucket, key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.get(w, r, bucket, key)
	case r.Method == http.MethodPut:
		s.put(w, r, bucket, key)
	case r.Method == http.MethodDelete:
		s.mu.Lock()
		delete(s.buckets[bucket], key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		s.fail(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", bucket, key)
	}
}

// authorized reports whether the request carries the server's access key in
// a SigV4 Authorization header or a presigned query.
func (s *Server) authorized(r *http.Request) bool {
	credential := r.URL.Query().Get("X-Amz-Credential")
	if auth := r.Header.Get("Authorization"); auth != "" {
		_, after, ok := strings.Cut(auth, "Credential=")
		if !ok {
			return false
		}
		credential = after
	}
	access, _, _ := strings.Cut(credential, "/")
	return access == s.AccessKey
}

// errorDocument is the XML body S3 sends with an error status.
type errorDocument struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	BucketName string   `xml:"BucketName,omitempty"`
	Key        string   `xml:"Key,omitempty"`
	Resource   string   `xml:"Resource"`
	RequestID  string   `xml:"RequestId"`
}

// fail writes an S3 error response. A HEAD request gets the status alone, as
// S3 sends no body with a HEAD.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, code, message, bucket, key string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(errorDocument{
		Code: code, Message: message, BucketName: bucket, Key: key, Resource: r.URL.Path, RequestID: "s3fake",
	})
}

// listContent is one object in a ListObjectsV2 result.
type listContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// commonPrefix is one rolled-up prefix in a delimited listing.
type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// listResult is the ListBucketResult document of ListObjectsV2.
type listResult struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []listContent  `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

// maxPage is the most keys one ListObjectsV2 page holds, on S3 and on RustFS.
const maxPage = 1000

// list answers ListObjectsV2. Keys and rolled-up prefixes come in byte order
// after start-after or after the key the continuation token names, at most
// max-keys (1000 at most) of them together. The continuation token is the
// base64 of the last key or prefix of the previous page.
func (s *Server) list(w http.ResponseWriter, r *http.Request, bucket string, objects map[string]Object) {
	query := r.URL.Query()
	prefix, delimiter := query.Get("prefix"), query.Get("delimiter")
	maxKeys := maxPage
	if raw := query.Get("max-keys"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			s.fail(w, r, http.StatusBadRequest, "InvalidArgument", "max-keys must be a non-negative integer", bucket, "")
			return
		}
		maxKeys = min(n, maxPage)
	}
	after := query.Get("start-after")
	token := query.Get("continuation-token")
	if token != "" {
		raw, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			s.fail(w, r, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect", bucket, "")
			return
		}
		after = string(raw)
	}

	s.mu.Lock()
	keys := make([]string, 0, len(objects))
	for k := range objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	snapshot := make(map[string]Object, len(keys))
	for _, k := range keys {
		snapshot[k] = objects[k]
	}
	s.mu.Unlock()
	sort.Strings(keys)

	encode := func(v string) string { return v }
	if query.Get("encoding-type") == "url" {
		encode = func(v string) string { return strings.ReplaceAll(url.QueryEscape(v), "%2F", "/") }
	}

	out := listResult{
		Name: bucket, Prefix: encode(prefix), StartAfter: encode(query.Get("start-after")),
		ContinuationToken: token, MaxKeys: maxKeys, Delimiter: encode(delimiter),
	}
	if query.Get("encoding-type") == "url" {
		out.EncodingType = "url"
	}
	seen := map[string]bool{}
	last := ""
	for _, k := range keys {
		entry := k
		if delimiter != "" {
			if i := strings.Index(k[len(prefix):], delimiter); i >= 0 {
				entry = k[:len(prefix)+i+len(delimiter)]
			}
		}
		if entry <= after || seen[entry] {
			continue
		}
		if out.KeyCount == maxKeys {
			out.IsTruncated = true
			out.NextContinuationToken = base64.StdEncoding.EncodeToString([]byte(last))
			break
		}
		seen[entry] = true
		last = entry
		out.KeyCount++
		if entry != k {
			out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: encode(entry)})
			continue
		}
		o := snapshot[k]
		out.Contents = append(out.Contents, listContent{
			Key:          encode(k),
			LastModified: o.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         o.ETag,
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(out)
}

// get answers GetObject and HeadObject, with a single byte range when the
// request has a Range header.
func (s *Server) get(w http.ResponseWriter, r *http.Request, bucket, key string) {
	o, ok := s.Get(bucket, key)
	if !ok {
		s.fail(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.", bucket, key)
		return
	}
	body := o.content()
	status := http.StatusOK
	if spec := r.Header.Get("Range"); spec != "" {
		start, end, ok := parseRange(spec, int64(len(body)))
		if !ok {
			s.fail(w, r, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable", bucket, key)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		body = body[start : end+1]
		status = http.StatusPartialContent
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("ETag", o.ETag)
	w.Header().Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
	w.WriteHeader(status)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

// parseRange reads a Range header of the form bytes=a-b, bytes=a- or
// bytes=-n against a body of the given size, and returns the first and last
// byte offsets.
func parseRange(spec string, size int64) (int64, int64, bool) {
	raw, ok := strings.CutPrefix(spec, "bytes=")
	if !ok || strings.Contains(raw, ",") {
		return 0, 0, false
	}
	first, last, _ := strings.Cut(raw, "-")
	if first == "" {
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		return max(size-n, 0), size - 1, size > 0
	}
	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if last != "" {
		if end, err = strconv.ParseInt(last, 10, 64); err != nil || end < start {
			return 0, 0, false
		}
		end = min(end, size-1)
	}
	return start, end, true
}

// put answers PutObject. A body sent with the streaming SigV4 payload
// (aws-chunked, as minio-go sends one over plain HTTP) is decoded first.
func (s *Server) put(w http.ResponseWriter, r *http.Request, bucket, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.fail(w, r, http.StatusBadRequest, "IncompleteBody", err.Error(), bucket, key)
		return
	}
	if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		if body, err = decodeChunked(body); err != nil {
			s.fail(w, r, http.StatusBadRequest, "IncompleteBody", err.Error(), bucket, key)
			return
		}
	}
	o := Object{Body: body}
	s.Put(bucket, key, o)
	stored, _ := s.Get(bucket, key)
	w.Header().Set("ETag", stored.ETag)
	w.WriteHeader(http.StatusOK)
}

// decodeChunked removes the aws-chunked framing: chunks of the form
// "<hex size>;chunk-signature=<sig>\r\n<data>\r\n", ending with a chunk of
// size zero and optional trailers.
func decodeChunked(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	reader := bufio.NewReader(bytes.NewReader(raw))
	for {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}
		sizeField, _, _ := strings.Cut(strings.TrimSpace(header), ";")
		size, err := strconv.ParseInt(sizeField, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("chunk size %q: %w", sizeField, err)
		}
		if size == 0 {
			return out.Bytes(), nil
		}
		if _, err := io.CopyN(&out, reader, size); err != nil {
			return nil, fmt.Errorf("read chunk: %w", err)
		}
		if _, err := reader.Discard(2); err != nil {
			return nil, fmt.Errorf("read chunk end: %w", err)
		}
	}
}
