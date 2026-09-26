package main

import (
	"testing"

	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// lookupRecorder is a RESTMapper that records each group and kind looked up
// with no version, the lookup that makes controller-runtime's lazy mapper
// read every version of a group.
type lookupRecorder struct {
	meta.RESTMapper
	looked map[schema.GroupKind]bool
}

// RESTMapping records gk when no version is given.
func (m *lookupRecorder) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if len(versions) == 0 {
		m.looked[gk] = true
	}
	return m.RESTMapper.RESTMapping(gk, versions...)
}

// The startup check warms the manager's mapper for the bootstrap webhook:
// it looks up Cluster and ObjectStore, so the webhook's first admission
// request finds both groups cached and runs no discovery call.
func TestTheStartupCheckWarmsTheWebhooksGroups(t *testing.T) {
	cluster := schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}
	defaults := meta.NewDefaultRESTMapper(nil)
	defaults.Add(cluster, meta.RESTScopeNamespace)
	defaults.Add(bootstrap.ObjectStoreGVK, meta.RESTScopeNamespace)
	mapper := &lookupRecorder{RESTMapper: defaults, looked: map[schema.GroupKind]bool{}}

	checkServedVersions(mapper)

	for _, gk := range []schema.GroupKind{cluster.GroupKind(), bootstrap.ObjectStoreGVK.GroupKind()} {
		if !mapper.looked[gk] {
			t.Errorf("the startup check did not look up %s", gk)
		}
	}
}
