package runs

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// When the API server serves neither VolSync's ReplicationSource nor
// CloudNativePG's Cluster at the version the controller uses, the startup
// error names both on one line. A newline would split the one log entry.
func TestTwoUnservedKindsMakeOneLine(t *testing.T) {
	volsync := schema.GroupVersion{Group: volsyncSourceKind.Group, Version: "v1beta1"}
	cnpg := schema.GroupVersion{Group: webhookClusterKind.Group, Version: "v2"}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{volsync, cnpg})
	mapper.Add(volsync.WithKind(volsyncSourceKind.Kind), meta.RESTScopeNamespace)
	mapper.Add(cnpg.WithKind(webhookClusterKind.Kind), meta.RESTScopeNamespace)

	err := CheckServedVersions(mapper)
	if err == nil {
		t.Fatal("CheckServedVersions() = nil, want both kinds reported")
	}
	if text := err.Error(); strings.Contains(text, "\n") || !strings.Contains(text, "ReplicationSource") || !strings.Contains(text, "Cluster") {
		t.Errorf("Error() = %q, want both kinds on one line", text)
	}
	var unserved *UnservedError
	if !errors.As(err, &unserved) {
		t.Errorf("errors.As(%v, *UnservedError) = false, want the kinds' errors kept", err)
	}
}
