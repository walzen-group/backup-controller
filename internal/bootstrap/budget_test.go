package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	c := newBuilder(t).
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
