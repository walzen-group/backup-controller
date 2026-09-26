package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fault"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// stalled answers a read by waiting for the request's context to end, the
// way a client-go read against an API server that does not answer returns
// once its context passes its deadline.
func stalled(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// decideStalled sends a create of the default Cluster to a Decider with a
// 300 ms budget whose Kubernetes client stalls on the reads funcs picks.
func decideStalled(t *testing.T, funcs interceptor.Funcs) admission.Response {
	t.Helper()
	raw, err := json.Marshal(cluster(t, nil))
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(secret()).
		WithRuntimeObjects(store()).
		WithInterceptorFuncs(funcs).Build()
	decider := &Decider{Client: c, Prober: stubProber{has: false}, Budget: 300 * time.Millisecond}
	return decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
}

// TestAStalledKubernetesReadNamesTheBudgetAndTheStep checks that when the
// budget runs out during a Kubernetes read, the HTTP 500 says the webhook ran
// out of its budget and which read it was waiting for. Finding W4,
// designs/webhook.md B: "the webhook ran out of its 10s budget while <step>".
func TestAStalledKubernetesReadNamesTheBudgetAndTheStep(t *testing.T) {
	cases := []struct {
		name  string
		funcs interceptor.Funcs
		step  string
	}{{
		name: "the ObjectStore get",
		funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == ObjectStoreGVK.Kind {
				return stalled(ctx)
			}
			return c.Get(ctx, key, obj, opts...)
		}},
		step: "reading the ObjectStore app/app-pg-store",
	}, {
		name: "the Cluster list",
		funcs: interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if list.GetObjectKind().GroupVersionKind().Kind == ClusterListGVK.Kind {
				return stalled(ctx)
			}
			return c.List(ctx, list, opts...)
		}},
		step: "listing the Clusters",
	}, {
		name: "the RestoreRun list",
		funcs: interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if list.GetObjectKind().GroupVersionKind().Kind != ClusterListGVK.Kind {
				return stalled(ctx)
			}
			return c.List(ctx, list, opts...)
		}},
		step: "listing the RestoreRuns",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := decideStalled(t, tc.funcs)

			if response.Allowed {
				t.Fatal("the cluster was admitted")
			}
			if response.Result.Code != http.StatusInternalServerError {
				t.Errorf("code = %d, want %d", response.Result.Code, http.StatusInternalServerError)
			}
			want := "the webhook ran out of its 300ms budget while " + tc.step
			if !strings.Contains(response.Result.Message, want) {
				t.Errorf("the error %q does not say %q", response.Result.Message, want)
			}
		})
	}
}

// TestAFailedGetIsNamedWhenTheBudgetRunsOut checks that when a backup.info
// read failed and the budget then ran out, the refusal names the file that
// failed to read, since that file might have been the DONE one, instead of
// saying none of the files read completed.
func TestAFailedGetIsNamedWhenTheBudgetRunsOut(t *testing.T) {
	recorded := withoutNewest(t, barmanstore.MustLoad(t, "many-failed"))
	ids := recorded.Backups()["app-pg"]
	newest := ids[len(ids)-1]
	endpoint, proxy := faultyS3(t, recordedS3(t, recorded))
	denied := proxy.Add(s3fault.Rule{
		Match:  s3fault.Match{Methods: []string{"GET"}, KeySuffix: newest + "/backup.info"},
		Status: http.StatusForbidden,
		Code:   "AccessDenied",
	})
	proxy.Add(slowInfo)

	response := decideWithin(t, cluster(t, nil), endpoint, time.Second)

	if response.Allowed {
		t.Fatalf("the cluster was admitted: %v", response.Patches)
	}
	if denied.Hits() == 0 {
		t.Fatal("no GET reached the AccessDenied rule")
	}
	message := response.Result.Message
	for _, want := range []string{"ran out of time", "base/" + newest + "/backup.info", "AccessDenied"} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal %q does not say %q", message, want)
		}
	}
	if strings.Contains(message, "none of them completed") {
		t.Errorf("the refusal %q says none completed although one could not be read", message)
	}
}
