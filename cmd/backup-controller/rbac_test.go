package main

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
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

	{"", "persistentvolumeclaims", []string{"create", "patch", "delete"}, "the prime claim, the restore Job UID recorded on it, and a RestoreRun's scratch claim"},
	{"", "persistentvolumes", []string{"patch"}, "rebinding the volume to the app's claim"},
	{"", "secrets", []string{"get", "create", "delete"}, "the repository Secret copied for the length of a restore"},
	{"", "events", []string{"create", "patch"}, "the recorder the library hands to the callbacks"},
	{"backup.wlz.li", "volumerestores", []string{"update"}, "the populator's backup.wlz.li/volume-populator finalizer on a VolumeRestore"},
	{"backup.wlz.li", "volumerestores/status", []string{"patch", "update"}, "the conditions reported on a VolumeRestore"},

	// populator.OrphanReconciler, in the run manager, for a claim being
	// deleted whose VolumeRestore is gone. It watches claims and
	// VolumeRestores through the manager's cache, reads both, stops the claim's
	// restore Jobs through the API reader, and deletes the claim's objects in
	// the controller namespace before it patches the library's finalizer off.
	{"", "persistentvolumeclaims", []string{"get", "list", "watch", "patch", "delete"}, "the orphan reconciler: claims being deleted, their prime claim, and the library's finalizer"},
	{"backup.wlz.li", "volumerestores", []string{"get", "list", "watch"}, "the orphan reconciler: the live check that the VolumeRestore is gone, and its delete events"},
	{"", "pods", []string{"list"}, "the populator and the orphan reconciler: the pods of every restore Job of a claim, by its restore-claim label, which they wait for"},
	{"", "secrets", []string{"delete"}, "the orphan reconciler: the Secret copy of a claim whose VolumeRestore is gone"},
	{"events.k8s.io", "events", []string{"create", "patch"}, "the orphan reconciler's WaitingForMover and DataSourceGone events"},

	{"volsync.backube", "replicationsources", []string{"get", "list", "watch", "create", "update", "patch"}, "each enabled claim's source, which the controller writes and triggers"},
	{"coordination.k8s.io", "leases", []string{"get", "list", "create", "update", "delete"}, "the Leases a BackupRun or RestoreRun takes on a claim and its repository before it starts a mover, in every namespace runs live in"},
	{"backup.wlz.li", "backupruns", []string{"get", "list", "watch", "create", "update", "delete"}, "the runs, the finalizer each carries, and the scheduled runs"},
	{"backup.wlz.li", "restoreruns", []string{"get", "list", "watch", "update", "delete"}, "the runs, the finalizer each carries, and the webhook's lookup of a waiting run"},
	{"backup.wlz.li", "backupruns/status", []string{"patch", "update"}, "a BackupRun's phase and conditions"},
	{"backup.wlz.li", "restoreruns/status", []string{"patch", "update"}, "a RestoreRun's phase and conditions"},
	{"events.k8s.io", "events", []string{"create", "patch"}, "an event on a run each time its Ready reason changes"},
	// The restore Job (internal/restorejob): the controller creates it
	// suspended, reads it by name through the uncached reader, resumes and
	// suspends it with a merge patch and deletes it with Foreground
	// propagation (api.go). A backup lists the controller's restore Jobs by
	// label for its exclusion check (restic-jobs step 7c). Stop lists the
	// Job's pods by their controller-uid label.
	{"batch", "jobs", []string{"get", "list", "create", "patch", "delete"}, "the restore Job a RestoreRun or the populator runs restic in, and the exclusion check over restore Jobs"},
	{"", "pods", []string{"list"}, "restorejob.Stop: the pods of a restore Job, listed by controller-uid, which it waits for"},

	{"", "namespaces", []string{"get", "list", "watch"}, "the scheduler reads each namespace's backup.wlz.li/schedule"},
	{"", "namespaces", []string{"get"}, "a restore Job follows the privileged-movers annotation of its namespace"},
	{"postgresql.cnpg.io", "clusters", []string{"get", "list", "delete"}, "a database run reads its Cluster, and a restore deletes it"},
	{"barmancloud.cnpg.io", "objectstores", []string{"get", "list"}, "the webhook reads the admitted Cluster's ObjectStore, and lists them all once for the collision check"},
	{"postgresql.cnpg.io", "backups", []string{"get", "create"}, "a base backup on demand"},
	{"apps", "deployments", []string{"get", "list"}, "quiesce finds and reads the marked Deployments"},
	{"apps", "statefulsets", []string{"get", "list"}, "quiesce finds and reads the marked StatefulSets"},
	{"apps", "deployments/scale", []string{"get", "update"}, "quiesce scales a marked Deployment to zero and back"},
	{"apps", "statefulsets/scale", []string{"get", "update"}, "quiesce scales a marked StatefulSet to zero and back"},
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

// TestTheManifestsGrantWhatTheControllerNeeds checks that the ClusterRole in
// deploy/rbac.yaml allows every verb in grants, and that the ClusterRole in
// the chart's templates/rbac.yaml grants exactly the same permissions.
//
// v0.1.1 installed cleanly, reported Running and Available, and filled
// nothing, because its ClusterRole could not list pods. Nothing caught it. The
// offline check compared deploy/rbac.yaml against a table in the docs, and
// both had been written from the same wrong premise.
//
// The chart and deploy/ install the same controller, so a permission added to
// one and forgotten in the other gives an install that works only one way.
// Both roles are reduced to single permissions (see permissions), so two rules
// in one file and one merged rule in the other compare equal. An extra or
// missing verb, resource, API group or resource name fails the test. The
// envtest TestEnvtestTheControllerChangesWorkloadsOnlyThroughScale checks
// that the shipped role cannot write a workload itself.
func TestTheManifestsGrantWhatTheControllerNeeds(t *testing.T) {
	role := readClusterRole(t, filepath.Join("..", "..", "deploy", "rbac.yaml"))
	for _, want := range grants {
		for _, verb := range want.verbs {
			if !allows(role, want.group, want.resource, verb) {
				t.Errorf("the ClusterRole does not allow %s on %s, needed for %s",
					verb, resourceName(want.group, want.resource), want.why)
			}
		}
	}

	chart := readChartRules(t, filepath.Join("..", "..", "chart", "templates", "rbac.yaml"))
	inDeploy, inChart := permissions(role.Rules), permissions(chart)
	for _, missing := range difference(inDeploy, inChart) {
		t.Errorf("deploy/ grants %s and the chart does not", missing)
	}
	for _, extra := range difference(inChart, inDeploy) {
		t.Errorf("the chart grants %s and deploy/ does not", extra)
	}
}

// permission is one verb on one resource, as a single ClusterRole rule could
// grant it. name is the one object the rule is limited to through
// resourceNames, or empty when the rule covers every object of the resource.
type permission struct {
	group, resource, name, verb string
}

// String returns the permission in a form for a test message, such as
// "get on leases.coordination.k8s.io named
// backup-controller-quiesce".
func (p permission) String() string {
	text := p.verb + " on " + resourceName(p.group, p.resource)
	if p.name != "" {
		text += " named " + p.name
	}
	return text
}

// permissions expands rules into the set of single permissions they grant:
// one for each API group, resource, resource name (or none) and verb of each
// rule.
func permissions(rules []rbacv1.PolicyRule) map[permission]bool {
	set := map[permission]bool{}
	for _, rule := range rules {
		names := rule.ResourceNames
		if len(names) == 0 {
			names = []string{""}
		}
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				for _, name := range names {
					for _, verb := range rule.Verbs {
						set[permission{group, resource, name, verb}] = true
					}
				}
			}
		}
	}
	return set
}

// difference returns the permissions in a that b lacks, sorted so a failure
// reads the same on every run.
func difference(a, b map[permission]bool) []permission {
	var out []permission
	for p := range a {
		if !b[p] {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// readChartRules returns the rules of the ClusterRole in the chart template at
// path, and fails the test when it cannot find or parse them.
//
// The template is read without Helm. Its rules block, from the line "rules:"
// to the document separator after it, holds no template actions, so it parses
// as plain YAML. The test fails when that block gains an action ("{{"),
// because the parse would then no longer match what Helm renders.
func readChartRules(t *testing.T, path string) []rbacv1.PolicyRule {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	text := string(content)
	start := strings.Index(text, "\nrules:\n")
	if start < 0 {
		t.Fatalf("no rules block in %s", path)
	}
	block := text[start+1:]
	if end := strings.Index(block, "\n---"); end >= 0 {
		block = block[:end]
	}
	if strings.Contains(block, "{{") {
		t.Fatalf("the rules block in %s holds a template action, so it cannot be read without rendering the chart", path)
	}
	var role struct {
		Rules []rbacv1.PolicyRule `json:"rules"`
	}
	if err := yaml.Unmarshal([]byte(block), &role); err != nil {
		t.Fatalf("parse the rules in %s: %v", path, err)
	}
	if len(role.Rules) == 0 {
		t.Fatalf("the rules block in %s holds no rules", path)
	}
	return role.Rules
}

// allows reports whether any rule in the role grants the verb, or the "*"
// wildcard, on the resource in the API group.
func allows(role *rbacv1.ClusterRole, group, resource, verb string) bool {
	for _, rule := range role.Rules {
		if !slices.Contains(rule.APIGroups, group) || !slices.Contains(rule.Resources, resource) {
			continue
		}
		if slices.Contains(rule.Verbs, verb) || slices.Contains(rule.Verbs, "*") {
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
