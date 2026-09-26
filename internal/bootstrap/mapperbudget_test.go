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
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

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

// Warm looks up Cluster and ObjectStore with no version, so a lazy mapper
// reads both groups' discovery before the first admission request.
func TestWarmLooksUpBothGroups(t *testing.T) {
	c := newBuilder(t).Build()
	seen := &recordingMapper{RESTMapper: c.RESTMapper()}
	if err := Warm(seen); err != nil {
		t.Fatalf("warm: %v", err)
	}
	for _, gk := range []schema.GroupKind{clusterKind, objectStoreKind} {
		if !seen.looked[gk] {
			t.Errorf("Warm did not look up %s", gk)
		}
	}
}

// recordingMapper records each group and kind looked up with no version.
type recordingMapper struct {
	meta.RESTMapper
	looked map[schema.GroupKind]bool
}

// RESTMapping records gk when no version is given, then asks the wrapped
// mapper.
func (m *recordingMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	m.record(gk, versions)
	return m.RESTMapper.RESTMapping(gk, versions...)
}

// RESTMappings records gk when no version is given, then asks the wrapped
// mapper.
func (m *recordingMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	m.record(gk, versions)
	return m.RESTMapper.RESTMappings(gk, versions...)
}

// record notes gk when versions is empty.
func (m *recordingMapper) record(gk schema.GroupKind, versions []string) {
	if len(versions) > 0 {
		return
	}
	if m.looked == nil {
		m.looked = map[schema.GroupKind]bool{}
	}
	m.looked[gk] = true
}
