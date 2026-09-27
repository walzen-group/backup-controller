package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	c := newBuilder(t).
		WithObjects(secret()).
		WithRuntimeObjects(store()).
		WithInterceptorFuncs(funcs).Build()
	decider := &Decider{Client: c, Prober: stubProber{has: false}, Budget: 300 * time.Millisecond}
	return create(t, decider, cluster(t, nil))
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

// stalledMapper is a RESTMapper whose lookups wait until release is closed,
// as controller-runtime's lazy mapper waits on a discovery call to an API
// server that does not answer. It has no way to take a context.
type stalledMapper struct {
	meta.RESTMapper
	release chan struct{}
}

// RESTMapping waits for release, then asks the wrapped mapper.
func (m stalledMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	<-m.release
	return m.RESTMapper.RESTMapping(gk, versions...)
}

// RESTMappings waits for release, then asks the wrapped mapper.
func (m stalledMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	<-m.release
	return m.RESTMapper.RESTMappings(gk, versions...)
}

// A lookup of the served version that stalls in discovery ends with the
// webhook's budget: the refusal comes back within the budget and says the
// webhook ran out of it while reading the ObjectStore.
func TestAStalledMapperLookupEndsWithTheBudget(t *testing.T) {
	raw, err := json.Marshal(cluster(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	c := newBuilder(t).WithObjects(secret()).WithRuntimeObjects(store()).Build()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	decider := &Decider{Client: c, Mapper: stalledMapper{RESTMapper: c.RESTMapper(), release: release},
		Prober: stubProber{has: false}, Budget: 300 * time.Millisecond}

	done := make(chan admission.Response, 1)
	go func() {
		done <- decider.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create, Namespace: "app", Name: "app-pg", Object: runtime.RawExtension{Raw: raw},
		}})
	}()
	var response admission.Response
	select {
	case response = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not answer within 5s of a 300ms budget")
	}

	if response.Allowed || response.Result.Code != http.StatusInternalServerError {
		t.Fatalf("allowed = %v, code = %d; want a refusal with %d", response.Allowed, response.Result.Code, http.StatusInternalServerError)
	}
	want := "the webhook ran out of its 300ms budget while reading the ObjectStore app/app-pg-store"
	if !strings.Contains(response.Result.Message, want) {
		t.Errorf("the error %q does not say %q", response.Result.Message, want)
	}
}
