package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
			c := fake.NewClientBuilder().WithScheme(scheme(t)).
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
