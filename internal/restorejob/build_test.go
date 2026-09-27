package restorejob_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/walzen-group/backup-controller/internal/restorejob"
)

// fullID is a snapshot ID of the form restic prints with --no-trunc.
const fullID = "4f2b0c9d8e7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a2b1c"

// runSpec returns the Spec a RestoreRun would pass for an in-place restore.
func runSpec() restorejob.Spec {
	return restorejob.Spec{
		Name:      "restore-3a7c-0",
		Namespace: "app",
		Origin:    restorejob.Origin{Kind: restorejob.OriginRestoreRun, UID: "run-uid"},
		Owner: metav1.OwnerReference{
			APIVersion: "backup.wlz.li/v1alpha1", Kind: "RestoreRun", Name: "r", UID: "run-uid",
			Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
		},
		SnapshotID: fullID,
		Claim:      "data",
		Repository: "restic-data",
		Image:      "quay.io/backube/volsync:0.16.0",
		Delete:     true,
	}
}

func build(t *testing.T, spec restorejob.Spec) *batchv1.Job {
	t.Helper()
	job, err := restorejob.Build(spec)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

// container returns the pod's container or init container of the given name.
func container(t *testing.T, job *batchv1.Job, name string) corev1.Container {
	t.Helper()
	pod := job.Spec.Template.Spec
	for _, c := range slices.Concat(pod.InitContainers, pod.Containers) {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no container %s in %+v", name, pod)
	return corev1.Container{}
}

func TestBuildNamesTheFullSnapshotID(t *testing.T) {
	job := build(t, runSpec())
	restore := container(t, job, "restore")
	want := []string{"restore", fullID, "--target", "/data", "--include-xattr", "user.*", "--retry-lock", "30m", "--delete"}
	if !slices.Equal(restore.Args, want) {
		t.Errorf("args = %q, want %q", restore.Args, want)
	}
	if got := job.Annotations[restorejob.AnnotationSnapshotID]; got != fullID {
		t.Errorf("snapshot-id annotation = %q, want %q", got, fullID)
	}
	if job.Annotations[restorejob.AnnotationClaim] != "data" || job.Annotations[restorejob.AnnotationRepository] != "restic-data" {
		t.Errorf("annotations = %v, want the claim and the repository Secret", job.Annotations)
	}
	if job.Name != "restore-3a7c-0" || job.Namespace != "app" {
		t.Errorf("job = %s/%s, want app/restore-3a7c-0", job.Namespace, job.Name)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].UID != "run-uid" || !*job.OwnerReferences[0].Controller {
		t.Errorf("ownerReferences = %+v, want the run as controller", job.OwnerReferences)
	}

	populator := runSpec()
	populator.Delete = false
	restore = container(t, build(t, populator), "restore")
	if slices.Contains(restore.Args, "--delete") {
		t.Errorf("args = %q, want no --delete for a populator restore", restore.Args)
	}
}

func TestBuildRefusesAnIDThatIsNot64Hex(t *testing.T) {
	for name, id := range map[string]string{
		"empty":      "",
		"short":      fullID[:8],
		"upper case": strings.ToUpper(fullID),
		"too long":   fullID + "0",
		"not hex":    fullID[:63] + "g",
	} {
		t.Run(name, func(t *testing.T) {
			spec := runSpec()
			spec.SnapshotID = id
			job, err := restorejob.Build(spec)
			var specErr *restorejob.SpecError
			if !errors.As(err, &specErr) || specErr.Field != "SnapshotID" {
				t.Fatalf("Build = %v, want a *SpecError on SnapshotID", err)
			}
			if job != nil {
				t.Errorf("Build returned a Job with its error: %+v", job)
			}
		})
	}
}

// Build creates the Job suspended, so no pod runs restic until the caller
// has recorded the Job and resumes it. The Job's own labels are never empty
// and never carry Kueue's queue label: the API server copies the pod
// template's labels onto a Job created with none, queue label included,
// and Kueue then manages the Job and resumes it on its own.
func TestBuildCreatesTheJobSuspendedWithoutTheQueueLabel(t *testing.T) {
	spec := runSpec()
	spec.PodLabels = map[string]string{"kueue.x-k8s.io/queue-name": "backups"}
	for name, origin := range map[string]restorejob.Origin{
		"RestoreRun": {Kind: restorejob.OriginRestoreRun, UID: "run-uid"},
		"claim":      {Kind: restorejob.OriginClaim, UID: "claim-uid"},
	} {
		t.Run(name, func(t *testing.T) {
			spec.Origin = origin
			job := build(t, spec)
			if job.Spec.Suspend == nil || !*job.Spec.Suspend {
				t.Errorf("spec.suspend = %v, want true", job.Spec.Suspend)
			}
			if len(job.Labels) == 0 {
				t.Error("the Job has no labels; the API server would copy the pod template's onto it")
			}
			if _, ok := job.Labels["kueue.x-k8s.io/queue-name"]; ok {
				t.Errorf("Job labels = %v, want no queue label", job.Labels)
			}
		})
	}
}

// The restore container runs restic itself, with no shell, from the
// repository Secret, and mounts the claim at /data, the path the snapshot
// holds its files under. The arguments are checked against real restic in
// internal/restic (restorejob_conformance_test.go).
func TestBuildRunsResticOnTheClaim(t *testing.T) {
	restore := container(t, build(t, runSpec()), "restore")
	if !slices.Equal(restore.Command, []string{"restic"}) {
		t.Errorf("command = %q, want [restic]", restore.Command)
	}
	if len(restore.EnvFrom) != 1 || restore.EnvFrom[0].SecretRef == nil || restore.EnvFrom[0].SecretRef.Name != "restic-data" {
		t.Errorf("envFrom = %+v, want the repository Secret", restore.EnvFrom)
	}
	data := slices.IndexFunc(restore.VolumeMounts, func(m corev1.VolumeMount) bool { return m.MountPath == "/data" })
	volumes := build(t, runSpec()).Spec.Template.Spec.Volumes
	claim := slices.IndexFunc(volumes, func(v corev1.Volume) bool {
		return v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == "data"
	})
	if data < 0 || claim < 0 || restore.VolumeMounts[data].Name != volumes[claim].Name {
		t.Errorf("mounts = %+v, volumes = %+v; want claim data at /data", restore.VolumeMounts, volumes)
	}
}

// In a namespace that lets movers run privileged, restic runs as root with
// the capabilities that restore file ownership, as VolSync's mover does.
func TestBuildFollowsPrivilegedMovers(t *testing.T) {
	annotated := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Annotations: map[string]string{restorejob.AnnotationPrivilegedMovers: "True"}}}
	for _, ns := range []*corev1.Namespace{annotated, {}} {
		privileged := restorejob.PrivilegedMovers(ns)
		spec := runSpec()
		spec.Privileged = privileged
		sc := container(t, build(t, spec), "restore").SecurityContext
		if root := sc.RunAsUser != nil && *sc.RunAsUser == 0; root != privileged || (len(sc.Capabilities.Add) > 0) != privileged {
			t.Errorf("annotations %v: securityContext = %+v, want privileged %t", ns.Annotations, sc, privileged)
		}
	}
}

// restic exits 10 for a missing repository and 12 for a wrong password, and
// no retry changes either, so the Job fails at once on them. A pod that
// Kubernetes or Kueue stops does not count against the backoff limit.
func TestBuildFailurePolicy(t *testing.T) {
	spec := build(t, runSpec()).Spec
	rules := spec.PodFailurePolicy.Rules
	if len(rules) != 3 || rules[0].Action != batchv1.PodFailurePolicyActionFailJob || !slices.Equal(rules[0].OnExitCodes.Values, []int32{10, 12}) {
		t.Errorf("podFailurePolicy = %+v, want FailJob on 10 and 12 first", spec.PodFailurePolicy)
	}
	for _, rule := range rules[1:] {
		if rule.Action != batchv1.PodFailurePolicyActionIgnore {
			t.Errorf("rule = %+v, want Ignore for a pod Kubernetes or Kueue stops", rule)
		}
	}
}
