// Package s3fake is an in-memory S3 server for the fast test tier. It stands
// in for RustFS 1.0.0, the S3 implementation walzen prod stores its backups
// on, so that the controller's S3 readers (bootstrap.S3Prober and
// restic.S3Store, both through minio-go) run against a server in unit-test
// time.
//
// It models the calls those readers make, as RustFS answers them: the bucket
// location, ListObjectsV2 with prefix, delimiter, start-after, a page of at
// most 1000 keys and continuation tokens (also across rolled-up prefixes),
// URL encoding of the keys when the client asks for it, and the elements of
// a listing in RustFS's order; GetObject with a closed, open or suffix byte
// range and a 416 past the end, HeadObject, PutObject with the Content-Type
// RustFS stores, and DeleteObject of a present or missing key; and RustFS's
// error documents for a missing bucket (on GET ?location and on a listing), a
// missing key (on GET and HEAD), an unknown access key (on GET ?location), a
// bad or negative max-keys and a continuation token that is not base64. The
// recordings in the barmanstore package hold RustFS's own answers to these
// requests, and this package's tests replay them against the fake.
//
// PutObject also decodes an aws-chunked body, the way minio-go sends one over
// plain HTTP. The recordings hold only a plain PUT, so no RustFS answer
// checks that decoding; the tests that write through minio-go, such as the
// s3fault package's, use it.
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
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	pathpkg "path"
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
	// ContentType is the Content-Type a GET or HEAD answers with. It
	// defaults to application/octet-stream.
	ContentType string
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
		_, _ = io.WriteString(w, xmlHeader+`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
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

// xmlHeader is the XML declaration RustFS starts every document with, with
// no line break after it.
const xmlHeader = `<?xml version="1.0" encoding="UTF-8"?>`

// errorDocument is the XML body RustFS sends with an error status: the code
// and the message alone, without the BucketName, Key, Resource and RequestId
// fields AWS S3 adds.
type errorDocument struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// fail writes an S3 error response. The bucket and key name the resource
// for a reader of the code; RustFS leaves them out of the document. A HEAD
// request gets the status and the Content-Type alone, as RustFS sends no body
// with a HEAD.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, code, message, _, _ string) {
	w.Header().Set("Content-Type", "application/xml")
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xmlHeader)
	_ = xml.NewEncoder(w).Encode(errorDocument{
		Code: code, Message: message,
	})
}

// listContent is one object in a ListObjectsV2 result.
type listContent struct {
	ETag         string `xml:"ETag"`
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Owner        *owner `xml:"Owner,omitempty"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// owner is the Owner element RustFS puts in each listed object when the
// client asks for it with fetch-owner=true.
type owner struct {
	DisplayName string `xml:"DisplayName"`
	ID          string `xml:"ID"`
}

// rustfsOwner is the owner RustFS 1.0.0 reports for its root user.
var rustfsOwner = &owner{DisplayName: "rustfs", ID: "c19050dbcee97fda828689dda99097a6321af2248fa760517237346e5d9c8a66"}

// commonPrefix is one rolled-up prefix in a delimited listing.
type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// listResult is the ListBucketResult document of ListObjectsV2, with its
// elements in the order RustFS writes them.
type listResult struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	Contents              []listContent  `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
}

// tokenSuffix ends every continuation token the fake hands out. RustFS 1.0.0
// appends a cache marker in brackets to the last key before it base64-encodes
// the token, such as "[rustfs_cache:v2,id:<uuid>,src:walker,gen:live]"; the
// fake appends its own, so that it takes its own tokens and RustFS's alike.
const tokenSuffix = "[s3fake]"

// encodeToken returns the continuation token that resumes after key.
func encodeToken(key string) string {
	return base64.StdEncoding.EncodeToString([]byte(key + tokenSuffix))
}

// decodeToken returns the key a continuation token resumes after: the
// decoded token without its bracketed marker. A token without a marker names
// the key itself, as RustFS 1.0.0 takes one. It returns false when the token
// is not base64.
func decodeToken(token string) (string, bool) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", false
	}
	s := string(raw)
	if i := strings.LastIndex(s, "["); i >= 0 && strings.HasSuffix(s, "]") {
		return s[:i], true
	}
	return s, true
}

// urlEncode encodes a key for a listing asked for with encoding-type=url, as
// RustFS does: every byte a query escapes, a space as %20, and the slash left
// as it is.
func urlEncode(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(v), "+", "%20"), "%2F", "/")
}

// maxPage is the most keys one ListObjectsV2 page holds, on S3 and on RustFS.
const maxPage = 1000

// list answers ListObjectsV2. Keys and rolled-up prefixes come in byte order
// after start-after or after the key the continuation token names, at most
// max-keys (1000 at most) of them together. The continuation token is the
// base64 of the last key or prefix of the previous page with a bracketed
// marker after it, the shape of a RustFS token. With encoding-type=url the keys
// and rolled-up prefixes are URL-encoded; the Prefix, Delimiter and StartAfter
// fields come back as the client sent them, as RustFS returns them.
func (s *Server) list(w http.ResponseWriter, r *http.Request, bucket string, objects map[string]Object) {
	query := r.URL.Query()
	prefix, delimiter := query.Get("prefix"), query.Get("delimiter")
	maxKeys := maxPage
	if raw := query.Get("max-keys"); raw != "" {
		n, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			s.fail(w, r, http.StatusBadRequest, "InvalidArgument", "invalid query: max-keys: "+raw, bucket, "")
			return
		case n < 0:
			s.fail(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid max keys", bucket, "")
			return
		}
		maxKeys = min(n, maxPage)
	}
	after := query.Get("start-after")
	token := query.Get("continuation-token")
	if token != "" {
		key, ok := decodeToken(token)
		if !ok {
			s.fail(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid continuation token", bucket, "")
			return
		}
		after = key
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
		encode = urlEncode
	}

	out := listResult{
		Name: bucket, Prefix: prefix, StartAfter: query.Get("start-after"),
		ContinuationToken: token, MaxKeys: maxKeys, Delimiter: delimiter,
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
			out.NextContinuationToken = encodeToken(last)
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
		item := listContent{
			Key:          encode(k),
			LastModified: o.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         o.ETag,
			Size:         o.Size,
			StorageClass: "STANDARD",
		}
		if query.Get("fetch-owner") == "true" {
			item.Owner = rustfsOwner
		}
		out.Contents = append(out.Contents, item)
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, xmlHeader)
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
	w.Header().Set("Content-Type", o.contentType())
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
	o := Object{Body: body, ContentType: r.Header.Get("Content-Type")}
	if o.ContentType == "" {
		o.ContentType = typeByExtension(key)
	}
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

// contentType returns the Content-Type the object is served with.
func (o Object) contentType() string {
	if o.ContentType != "" {
		return o.ContentType
	}
	return "application/octet-stream"
}

// typeByExtension returns the media type RustFS 1.0.0 stores for an object
// written without a Content-Type: the type of the key's extension without
// parameters, such as text/plain for a .txt key, and application/octet-stream
// when the extension has none.
func typeByExtension(key string) string {
	if t, _, err := mime.ParseMediaType(mime.TypeByExtension(pathpkg.Ext(key))); err == nil {
		return t
	}
	return "application/octet-stream"
}
