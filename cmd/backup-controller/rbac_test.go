package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// grant is one permission the controller needs: the API group, the resource
// and the verbs a ClusterRole rule has to allow, and why the controller needs
// them.
type grant struct {
	group    string
	resource string
	verbs    []string
	why      string
}

// grants lists every permission this controller needs, each with the reason
// it needs it.
//
// The first block is for lib-volume-populator's informers. The library builds
// an informer per resource and waits for all of them to sync before the
// controller runs at all (populator-machinery/controller.go:287-291 and
// :542). A missing read there stops everything while the Deployment still
// reports Available. That is how v0.1.1 shipped without pods: its RBAC listed
// the calls the callbacks make, and the informers' reads are not among them.
var grants = []grant{
	{"", "persistentvolumeclaims", []string{"get", "list", "watch"}, "the library's claim informer"},
	{"", "persistentvolumes", []string{"get", "list", "watch"}, "the library's volume informer"},
	{"", "pods", []string{"get", "list", "watch"}, "the library's pod informer, built whether or not a populator pod is used"},
	{"storage.k8s.io", "storageclasses", []string{"get", "list", "watch"}, "the library's storage class informer, which reads the binding mode"},
	{"backup.wlz.li", "volumerestores", []string{"get", "list", "watch"}, "the library's informer over the data source kind"},

	{"", "persistentvolumeclaims", []string{"create", "patch", "delete"}, "the prime claim, and a RestoreRun's scratch claim"},
	{"", "persistentvolumes", []string{"patch"}, "rebinding the volume to the app's claim"},
	{"", "secrets", []string{"get", "create", "delete"}, "the repository Secret copied for the length of a restore"},
	{"", "events", []string{"create", "patch"}, "the recorder the library hands to the callbacks"},
	{"backup.wlz.li", "volumerestores", []string{"create"}, "a RestoreRun's point-in-time VolumeRestore"},
	{"backup.wlz.li", "volumerestores", []string{"update"}, "the populator's backup.wlz.li/volume-populator finalizer on a VolumeRestore"},
	{"backup.wlz.li", "volumerestores/status", []string{"patch", "update"}, "the conditions reported on a VolumeRestore"},
	{"volsync.backube", "replicationdestinations", []string{"get", "list", "watch", "create", "delete"}, "one destination per restore"},

	{"volsync.backube", "replicationsources", []string{"get", "list", "watch", "create", "update", "patch"}, "each enabled claim's source, which the controller writes and triggers"},
	{"backup.wlz.li", "backupruns", []string{"get", "list", "watch", "create", "update", "delete"}, "the runs, the finalizer each carries, and the scheduled runs"},
	{"backup.wlz.li", "restoreruns", []string{"get", "list", "watch", "update", "delete"}, "the runs, the finalizer each carries, and the webhook's lookup of a waiting run"},
	{"backup.wlz.li", "backupruns/status", []string{"patch", "update"}, "a BackupRun's phase and conditions"},
	{"backup.wlz.li", "restoreruns/status", []string{"patch", "update"}, "a RestoreRun's phase and conditions"},
	{"events.k8s.io", "events", []string{"create", "patch"}, "an event on a run each time its Ready reason changes"},

	{"", "namespaces", []string{"get", "list", "watch"}, "the scheduler reads each namespace's backup.wlz.li/schedule"},
	{"postgresql.cnpg.io", "clusters", []string{"get", "list", "delete"}, "a database run reads its Cluster, and a restore deletes it"},
	{"postgresql.cnpg.io", "backups", []string{"get", "create"}, "a base backup on demand"},
	{"apps", "deployments", []string{"get", "list", "patch"}, "quiesce scales a marked Deployment to zero and back"},
	{"apps", "statefulsets", []string{"get", "list", "patch"}, "quiesce scales a marked StatefulSet to zero and back"},
	{"kustomize.toolkit.fluxcd.io", "kustomizations", []string{"get", "patch"}, "quiesce suspends the Kustomization that would put the replicas back"},
	{"kueue.x-k8s.io", "workloads", []string{"get", "create", "delete"}, "the Workload that admits a run"},
	{"kueue.x-k8s.io", "workloads/status", []string{"update"}, "the PodsReady condition on that Workload"},
	{"kueue.x-k8s.io", "localqueues", []string{"list"}, "the queue a namespace's runs are admitted through"},

	// The OwnerReferencesPermissionEnforcement admission plugin refuses an
	// owner reference with blockOwnerDeletion unless the writer may update
	// the owner's finalizers. The controller writes such references from the
	// Workload, the scratch claim and the VolumeRestore to their run, and from
	// each ReplicationSource to its claim.
	{"backup.wlz.li", "backupruns/finalizers", []string{"update"}, "owner references to a BackupRun, under OwnerReferencesPermissionEnforcement"},
	{"backup.wlz.li", "restoreruns/finalizers", []string{"update"}, "owner references to a RestoreRun, under OwnerReferencesPermissionEnforcement"},
	{"", "persistentvolumeclaims/finalizers", []string{"update"}, "owner references to a claim, under OwnerReferencesPermissionEnforcement"},
}

// TestTheClusterRoleCoversEverythingTheControllerDoes checks that the
// ClusterRole in deploy/rbac.yaml allows every verb in grants.
//
// v0.1.1 installed cleanly, reported Running and Available, and filled
// nothing, because its ClusterRole could not list pods. Nothing caught it. The
// offline check compared deploy/rbac.yaml against a table in the docs, and
// both had been written from the same wrong premise.
func TestTheClusterRoleCoversEverythingTheControllerDoes(t *testing.T) {
	role := readClusterRole(t, filepath.Join("..", "..", "deploy", "rbac.yaml"))

	for _, want := range grants {
		for _, verb := range want.verbs {
			if !allows(role, want.group, want.resource, verb) {
				t.Errorf("the ClusterRole does not allow %s on %s, needed for %s",
					verb, resourceName(want.group, want.resource), want.why)
			}
		}
	}
}

// TestTheChartGrantsTheSameRulesAsDeploy checks that the chart's
// templates/rbac.yaml names a rule for every resource in grants. The chart and
// deploy/ install the same controller, so a permission added to one and
// forgotten in the other gives an install that works only one way.
func TestTheChartGrantsTheSameRulesAsDeploy(t *testing.T) {
	chart, err := os.ReadFile(filepath.Join("..", "..", "chart", "templates", "rbac.yaml"))
	if err != nil {
		t.Fatalf("read the chart's rbac: %v", err)
	}
	text := string(chart)

	for _, want := range grants {
		line := fmt.Sprintf("resources: [%q", want.resource)
		if !strings.Contains(text, line) && !strings.Contains(text, fmt.Sprintf(`"%s"`, want.resource)) {
			t.Errorf("the chart names no rule for %s, which deploy/ grants for %s",
				resourceName(want.group, want.resource), want.why)
		}
	}
}

// allows reports whether any rule in the role grants the verb, or the "*"
// wildcard, on the resource in the API group.
func allows(role *rbacv1.ClusterRole, group, resource, verb string) bool {
	for _, rule := range role.Rules {
		if !contains(rule.APIGroups, group) || !contains(rule.Resources, resource) {
			continue
		}
		if contains(rule.Verbs, verb) || contains(rule.Verbs, "*") {
			return true
		}
	}
	return false
}

// contains reports whether haystack holds needle.
func contains(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

// resourceName returns the resource qualified by its API group, such as
// clusters.postgresql.cnpg.io, or the bare resource for the core group.
func resourceName(group, resource string) string {
	if group == "" {
		return resource
	}
	return resource + "." + group
}

// readClusterRole returns the first ClusterRole in the multi-document YAML
// file at path, and fails the test when the file holds none.
func readClusterRole(t *testing.T, path string) *rbacv1.ClusterRole {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, document := range strings.Split(string(content), "\n---") {
		if !strings.Contains(document, "kind: ClusterRole\n") {
			continue
		}
		role := &rbacv1.ClusterRole{}
		if err := yaml.Unmarshal([]byte(document), role); err != nil {
			t.Fatalf("parse the ClusterRole in %s: %v", path, err)
		}
		if role.Kind == "ClusterRole" {
			return role
		}
	}
	t.Fatalf("no ClusterRole in %s", path)
	return nil
}
