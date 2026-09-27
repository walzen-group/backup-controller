package quiesce

import (
	"reflect"
	"testing"
)

// TestInventoryIDsReadAnRBACNameWithAColon checks that inventoryIDs accepts
// the id fluxcd/cli-utils v1.2.3 writes for an RBAC object whose name holds
// ":". ObjMetadata.String writes ":" as "__" for RBAC kinds, and
// ParseObjMetadata reads such an id (pkg/object/objmetadata.go:32-35,
// 69-102, 115-128). A Kustomization that also applies a RoleBinding named
// app:reader must not refuse the run.
func TestInventoryIDsReadAnRBACNameWithAColon(t *testing.T) {
	ids := []any{
		map[string]any{"id": "notes_notes_apps_Deployment", "v": "v1"},
		map[string]any{"id": "notes_app__reader_rbac.authorization.k8s.io_RoleBinding", "v": "v1"},
		map[string]any{"id": "_app__reader_rbac.authorization.k8s.io_ClusterRole", "v": "v1"},
	}
	if err := inventoryIDs(kustomizationOf(ids)); err != nil {
		t.Fatalf("inventoryIDs refused ids that cli-utils writes: %v", err)
	}
}

// TestParseInventoryIDFollowsCliUtils checks each field that parseInventoryID
// reads against ParseObjMetadata of fluxcd/cli-utils v1.2.3
// (pkg/object/objmetadata.go:69-102): the namespace ends at the first "_",
// the kind and group start after the last two "_", the rest is the name, and
// "__" in the name is ":".
func TestParseInventoryIDFollowsCliUtils(t *testing.T) {
	for id, want := range map[string]inventoryID{
		"notes_notes_apps_Deployment":                             {Namespace: "notes", Name: "notes", Group: "apps", Kind: "Deployment"},
		"notes_app__reader_rbac.authorization.k8s.io_RoleBinding": {Namespace: "notes", Name: "app:reader", Group: "rbac.authorization.k8s.io", Kind: "RoleBinding"},
		"_system__a__b_rbac.authorization.k8s.io_ClusterRole":     {Name: "system:a:b", Group: "rbac.authorization.k8s.io", Kind: "ClusterRole"},
		"notes_data__Secret":                                      {Namespace: "notes", Name: "data", Group: "", Kind: "Secret"},
	} {
		got, ok := parseInventoryID(id)
		if !ok || !reflect.DeepEqual(got, want) {
			t.Errorf("parseInventoryID(%q) = %+v, %v; want %+v", id, got, ok, want)
		}
	}
	for _, id := range []string{"notes", "notes_Deployment", ""} {
		if _, ok := parseInventoryID(id); ok {
			t.Errorf("parseInventoryID(%q) accepted an id without four fields", id)
		}
	}
}
