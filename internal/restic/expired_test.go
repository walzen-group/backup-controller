package restic

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
)

// emptyFirstPage is a ListObjectsV2 answer that holds no key and says more
// follow. S3 may cut a page short at any count, none included.
const emptyFirstPage = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>repo</Name><Prefix>locks/</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter><IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken></ListBucketResult>`

// firstPageThenCancel answers the first page of a ListObjectsV2 with
// emptyFirstPage and calls cancel when the client closes that answer, which
// minio-go does once it has read the page and before it asks for the next.
// Every other request goes to next.
type firstPageThenCancel struct {
	next   http.RoundTripper
	cancel context.CancelFunc
}

// RoundTrip serves one request as the type's comment says.
func (f firstPageThenCancel) RoundTrip(r *http.Request) (*http.Response, error) {
	query := r.URL.Query()
	if query.Get("list-type") != "2" || query.Has("continuation-token") {
		return f.next.RoundTrip(r)
	}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": {"application/xml"}},
		Body:          cancelOnClose{Reader: strings.NewReader(emptyFirstPage), cancel: f.cancel},
		ContentLength: int64(len(emptyFirstPage)),
		Request:       r,
	}, nil
}

// cancelOnClose is a response body that calls cancel when it is closed.
type cancelOnClose struct {
	io.Reader
	cancel context.CancelFunc
}

// Close calls cancel.
func (c cancelOnClose) Close() error {
	c.cancel()
	return nil
}

// TestAListingCutShortByTheContextIsAnError checks that S3Store.List fails
// when its context ends between two pages of the listing. minio-go then
// stops without an error, and a List that took the names so far as all of
// them would report a repository whose lock is live as unlocked. The s3fake
// server holds a lock; the first page comes back empty and truncated, and
// the context ends once minio-go has read it. Finding W5.
func TestAListingCutShortByTheContextIsAnError(t *testing.T) {
	server := s3fake.New(t, "repo")
	server.Put("repo", "locks/0123abcd", s3fake.Object{Body: []byte("lock")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := minio.New(server.Endpoint(), &minio.Options{
		Creds:     credentials.NewStaticV4(server.AccessKey, server.SecretKey, ""),
		Transport: firstPageThenCancel{next: http.DefaultTransport, cancel: cancel},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &S3Store{client: client, at: Location{Endpoint: server.Endpoint(), Bucket: "repo"}}

	names, err := store.List(ctx, "locks")

	if err == nil {
		t.Fatalf("List gave no error and %v, although the context ended before the second page", names)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled", err)
	}
}
