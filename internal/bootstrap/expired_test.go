package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// expired returns a context whose deadline has already passed.
func expired(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

// doneBaseAt serves the recorded done-base store and returns the Location of
// app-pg in it.
func doneBaseAt(t *testing.T) Location {
	t.Helper()
	server := recordedS3(t, barmanstore.MustLoad(t, "done-base"))
	return Location{Endpoint: server.URL, Bucket: "backups", Prefix: "app/app-pg",
		AccessKey: server.AccessKey, SecretKey: server.SecretKey}
}

// TestAnExpiredContextNeverReadsAsAnEmptyArchive checks Survey on a context
// that is already done, against the recorded done-base store. minio-go's
// listing ends without an error item when its context is done, so a Survey
// that took the silent end as the whole listing would report the full
// archive as empty, and the webhook would start an empty database over it.
// Finding W5.
func TestAnExpiredContextNeverReadsAsAnEmptyArchive(t *testing.T) {
	at := doneBaseAt(t)

	archive, err := S3Prober{}.Survey(expired(t), at, nil)

	if err == nil {
		t.Fatalf("Survey on an expired context gave no error: empty = %v, found = %v, backups = %d", archive.Empty, archive.Found, archive.Backups)
	}
	var late *OutOfTimeError
	if !errors.As(err, &late) {
		t.Errorf("err = %v, want an *OutOfTimeError", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

// TestAnExpiredContextNeverListsNoBaseBackups checks BaseBackups on a context
// that is already done, against the recorded done-base store. The RestoreRun
// controller reads an empty list as "no completed backup", so the silent end
// of minio-go's listing must come back as an error. Finding W5.
func TestAnExpiredContextNeverListsNoBaseBackups(t *testing.T) {
	at := doneBaseAt(t)

	backups, err := S3Prober{}.BaseBackups(expired(t), at)

	if err == nil {
		t.Fatalf("BaseBackups on an expired context gave no error and %d backups", len(backups))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

// atTheDeadline answers a list by doing it, then waiting for the request's
// context to end and returning success, the way a Kubernetes read that
// finishes just as the budget runs out leaves the caller an expired context
// and no error.
func atTheDeadline(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.List(ctx, list, opts...); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

// TestAReadThatEndsAtTheDeadlineNeverAdmitsInitdbOverAnArchive checks that a
// Kubernetes list which succeeds just as the budget runs out does not let
// the webhook admit a Cluster to initdb over the recorded done-base store.
// Survey then runs on an expired context, and minio-go's listing ends
// silently. Before the fix the main path admitted with no patch ("no base
// backup") and the opt-out path admitted "opted out". Finding W5.
func TestAReadThatEndsAtTheDeadlineNeverAdmitsInitdbOverAnArchive(t *testing.T) {
	cases := []struct {
		name    string
		cluster func(t *testing.T) *unstructured.Unstructured
		kind    string
	}{{
		name:    "the RestoreRun list on the main path",
		cluster: func(t *testing.T) *unstructured.Unstructured { return cluster(t, nil) },
		kind:    "RestoreRunList",
	}, {
		name:    "the Cluster list on the opt-out path",
		cluster: optedOut,
		kind:    ClusterListGVK.Kind,
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := recordedS3(t, barmanstore.MustLoad(t, "done-base"))
			raw, err := json.Marshal(tc.cluster(t))
			if err != nil {
				t.Fatalf("marshal the cluster: %v", err)
			}
			kind := tc.kind
			c := newBuilder(t).
				WithObjects(secret()).
				WithRuntimeObjects(storeAt(server.URL)).
				WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					gvk, err := c.GroupVersionKindFor(list)
					if err != nil {
						return err
					}
					if gvk.Kind == kind {
						return atTheDeadline(ctx, c, list, opts...)
					}
					return c.List(ctx, list, opts...)
				}}).Build()
			decider := &Decider{Client: c, Prober: S3Prober{}, Budget: 300 * time.Millisecond}

			response := decider.Handle(context.Background(), admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Operation: admissionv1.Create,
					Namespace: "app",
					Name:      "app-pg",
					Object:    runtime.RawExtension{Raw: raw},
				},
			})

			if response.Allowed {
				t.Fatalf("the cluster was admitted over the done-base archive: patches = %d, result = %v", len(response.Patches), response.Result)
			}
		})
	}
}

// expiresAfterNextCheck is a context that ends right after the first Err
// call made once it is armed. That call still sees the context live; every
// later one sees it canceled. It models a deadline that passes between a
// check of the context and the next use of it, which a real deadline hits
// only by chance.
type expiresAfterNextCheck struct {
	context.Context
	cancel context.CancelFunc
	armed  atomic.Bool
	fired  atomic.Bool
}

// Err returns nil and cancels the context on the first call after arming,
// and the embedded context's error otherwise.
func (c *expiresAfterNextCheck) Err() error {
	if c.armed.Load() && c.fired.CompareAndSwap(false, true) {
		c.cancel()
		return nil
	}
	return c.Context.Err()
}

// armOnBaseListing passes every request to next, and arms ctx when the
// client closes the answer to the listing of base/. minio-go closes that
// answer before backupIDs checks the context, so the context ends right
// after that check and before the next listing starts.
type armOnBaseListing struct {
	next http.RoundTripper
	ctx  *expiresAfterNextCheck
}

// RoundTrip serves one request as the type's comment says.
func (a armOnBaseListing) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := a.next.RoundTrip(r)
	query := r.URL.Query()
	if err != nil || query.Get("list-type") != "2" || !strings.HasSuffix(query.Get("prefix"), "/base/") {
		return response, err
	}
	response.Body = armOnClose{ReadCloser: response.Body, ctx: a.ctx}
	return response, nil
}

// armOnClose is a response body that arms ctx when it is closed.
type armOnClose struct {
	io.ReadCloser
	ctx *expiresAfterNextCheck
}

// Close closes the body and arms ctx.
func (a armOnClose) Close() error {
	err := a.ReadCloser.Close()
	a.ctx.armed.Store(true)
	return err
}

// TestAContextThatEndsAfterTheBaseListingNeverReadsAsAnEmptyPrefix checks
// Survey on the recorded wal-only store, whose prefix holds WAL and no
// base/, when the context ends after backupIDs has returned and before the
// one-key listing of the server prefix. minio-go then ends that listing
// with no item and no error, and a Survey that took the silence as an empty
// prefix would let the webhook admit initdb into a prefix that holds WAL.
// minio.DefaultTransport is swapped for the test so the client minio.New
// builds for the http endpoint goes through armOnBaseListing. Finding W7.
func TestAContextThatEndsAfterTheBaseListingNeverReadsAsAnEmptyPrefix(t *testing.T) {
	server := recordedS3(t, barmanstore.MustLoad(t, "wal-only"))
	at := Location{Endpoint: server.URL, Bucket: "backups", Prefix: "app/app-pg",
		AccessKey: server.AccessKey, SecretKey: server.SecretKey}

	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx := &expiresAfterNextCheck{Context: parent, cancel: cancel}

	original := minio.DefaultTransport
	t.Cleanup(func() { minio.DefaultTransport = original })
	minio.DefaultTransport = func(secure bool) (*http.Transport, error) {
		transport := &http.Transport{}
		transport.RegisterProtocol("http", armOnBaseListing{next: http.DefaultTransport, ctx: ctx})
		return transport, nil
	}

	archive, err := S3Prober{}.Survey(ctx, at, nil)

	if !ctx.fired.Load() {
		t.Fatalf("the context never ended after the base/ listing, so the test checked nothing (err = %v)", err)
	}
	if err == nil {
		t.Fatalf("Survey gave no error after the context ended: empty = %v, backups = %d", archive.Empty, archive.Backups)
	}
	if archive.Empty {
		t.Errorf("Survey read the wal-only prefix as empty")
	}
	var late *OutOfTimeError
	if !errors.As(err, &late) {
		t.Errorf("err = %v, want an *OutOfTimeError", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled", err)
	}
}
