package restorejob_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

func TestBuildRefusesAnOriginOfNoKnownKind(t *testing.T) {
	spec := runSpec()
	spec.Origin.Kind = 0
	var specErr *restorejob.SpecError
	if _, err := restorejob.Build(spec); !errors.As(err, &specErr) || specErr.Field != "Origin" {
		t.Fatalf("Build = %v, want a *SpecError on Origin", err)
	}
	spec = runSpec()
	spec.Origin.UID = ""
	if _, err := restorejob.Build(spec); !errors.As(err, &specErr) || specErr.Field != "Origin" {
		t.Fatalf("Build = %v, want a *SpecError on Origin for an empty UID", err)
	}
}

func TestBuildRunsResticWithoutAShell(t *testing.T) {
	job := build(t, runSpec())
	pod := job.Spec.Template.Spec
	if len(pod.InitContainers) != 1 || len(pod.Containers) != 1 {
		t.Fatalf("containers = %d init, %d main; want one of each", len(pod.InitContainers), len(pod.Containers))
	}
	unlock := container(t, job, "unlock")
	restore := container(t, job, "restore")
	for _, c := range []corev1.Container{unlock, restore} {
		if !slices.Equal(c.Command, []string{"restic"}) {
			t.Errorf("%s command = %q, want [restic]", c.Name, c.Command)
		}
		if c.Image != "quay.io/backube/volsync:0.16.0" {
			t.Errorf("%s image = %q", c.Name, c.Image)
		}
		if c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
			t.Errorf("%s terminationMessagePolicy = %q", c.Name, c.TerminationMessagePolicy)
		}
		if len(c.EnvFrom) != 1 || c.EnvFrom[0].SecretRef == nil || c.EnvFrom[0].SecretRef.Name != "restic-data" {
			t.Errorf("%s envFrom = %+v, want the repository Secret", c.Name, c.EnvFrom)
		}
		if !slices.Equal(c.Env, []corev1.EnvVar{{Name: "RESTIC_CACHE_DIR", Value: "/cache"}}) {
			t.Errorf("%s env = %+v, want RESTIC_CACHE_DIR=/cache", c.Name, c.Env)
		}
	}
	if !slices.Equal(unlock.Args, []string{"unlock"}) {
		t.Errorf("unlock args = %q, want [unlock]", unlock.Args)
	}
	if i := slices.Index(restore.Args, "--retry-lock"); i < 0 || i+1 >= len(restore.Args) || restore.Args[i+1] != "30m" {
		t.Errorf("restore args = %q, want --retry-lock 30m", restore.Args)
	}
	if mounts(unlock) != "cache:/cache,tempdir:/tmp" {
		t.Errorf("unlock mounts = %s, want the cache and /tmp only", mounts(unlock))
	}
	if mounts(restore) != "data:/data,cache:/cache,tempdir:/tmp" {
		t.Errorf("restore mounts = %s", mounts(restore))
	}
	if pod.RestartPolicy != corev1.RestartPolicyNever || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Errorf("pod restartPolicy %q, automountServiceAccountToken %v; want Never and false",
			pod.RestartPolicy, pod.AutomountServiceAccountToken)
	}
	if pod.ServiceAccountName != "" || pod.NodeName != "" || pod.NodeSelector != nil || pod.Affinity != nil {
		t.Errorf("pod = %+v, want the default ServiceAccount and no placement", pod)
	}
}

func mounts(c corev1.Container) string {
	var out []string
	for _, m := range c.VolumeMounts {
		out = append(out, m.Name+":"+m.MountPath)
	}
	return strings.Join(out, ",")
}

func TestBuildVolumes(t *testing.T) {
	spec := runSpec()
	spec.CacheStorageClassName = ptr.To("zfs-ephemeral")
	spec.CacheCapacity = ptr.To(resource.MustParse("3Gi"))
	pod := build(t, spec).Spec.Template.Spec
	byName := map[string]corev1.Volume{}
	for _, v := range pod.Volumes {
		byName[v.Name] = v
	}
	if len(byName) != 3 {
		t.Fatalf("volumes = %+v, want data, cache and tempdir", pod.Volumes)
	}
	if c := byName["data"].PersistentVolumeClaim; c == nil || c.ClaimName != "data" || c.ReadOnly {
		t.Errorf("data volume = %+v, want the claim data, writable", byName["data"])
	}
	eph := byName["cache"].Ephemeral
	if eph == nil || eph.VolumeClaimTemplate == nil {
		t.Fatalf("cache volume = %+v, want a generic ephemeral volume", byName["cache"])
	}
	claim := eph.VolumeClaimTemplate.Spec
	if claim.StorageClassName == nil || *claim.StorageClassName != "zfs-ephemeral" ||
		!slices.Equal(claim.AccessModes, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}) ||
		claim.Resources.Requests.Storage().Cmp(resource.MustParse("3Gi")) != 0 || claim.DataSource != nil {
		t.Errorf("cache claim = %+v", claim)
	}
	if e := byName["tempdir"].EmptyDir; e == nil || e.Medium != corev1.StorageMediumMemory {
		t.Errorf("tempdir = %+v, want an in-memory emptyDir", byName["tempdir"])
	}

	claim = build(t, runSpec()).Spec.Template.Spec.Volumes[1].Ephemeral.VolumeClaimTemplate.Spec
	if claim.StorageClassName != nil || claim.Resources.Requests.Storage().Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("default cache claim = %+v, want the default class and 1Gi", claim)
	}
}

func TestBuildFollowsPrivilegedMovers(t *testing.T) {
	annotated := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app",
		Annotations: map[string]string{restorejob.AnnotationPrivilegedMovers: "True"}}}
	for _, tc := range []struct {
		name       string
		ns         *corev1.Namespace
		privileged bool
	}{
		{"annotated", annotated, true},
		{"false", &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{restorejob.AnnotationPrivilegedMovers: "false"}}}, false},
		{"not annotated", &corev1.Namespace{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := restorejob.PrivilegedMovers(tc.ns); got != tc.privileged {
				t.Fatalf("PrivilegedMovers = %v, want %v", got, tc.privileged)
			}
			spec := runSpec()
			spec.Privileged = restorejob.PrivilegedMovers(tc.ns)
			job := build(t, spec)
			for _, name := range []string{"unlock", "restore"} {
				checkSecurityContext(t, container(t, job, name), tc.privileged)
			}
		})
	}
}

func checkSecurityContext(t *testing.T, c corev1.Container, privileged bool) {
	t.Helper()
	sc := c.SecurityContext
	if sc == nil || sc.Capabilities == nil {
		t.Fatalf("%s securityContext = %+v", c.Name, sc)
	}
	if !slices.Equal(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) || sc.AllowPrivilegeEscalation == nil ||
		*sc.AllowPrivilegeEscalation || sc.Privileged == nil || *sc.Privileged ||
		sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Errorf("%s securityContext = %+v, want VolSync's restricted settings", c.Name, sc)
	}
	add := []corev1.Capability{"DAC_OVERRIDE", "CHOWN", "FOWNER"}
	if privileged {
		if sc.RunAsUser == nil || *sc.RunAsUser != 0 || !slices.Equal(sc.Capabilities.Add, add) {
			t.Errorf("%s = user %v, add %v; want user 0 and %v", c.Name, sc.RunAsUser, sc.Capabilities.Add, add)
		}
		return
	}
	if sc.RunAsUser != nil || len(sc.Capabilities.Add) != 0 {
		t.Errorf("%s = user %v, add %v; want neither", c.Name, sc.RunAsUser, sc.Capabilities.Add)
	}
}

func TestBuildTakesThePodSecurityContext(t *testing.T) {
	spec := runSpec()
	spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: ptr.To[int64](1000), FSGroup: ptr.To[int64](1000)}
	job := build(t, spec)
	got := job.Spec.Template.Spec.SecurityContext
	if got == nil || *got.RunAsUser != 1000 || *got.FSGroup != 1000 {
		t.Fatalf("pod securityContext = %+v, want the one passed", got)
	}
	*spec.SecurityContext.RunAsUser = 0
	if *got.RunAsUser != 1000 {
		t.Error("the Job shares the caller's securityContext instead of a copy")
	}
}

func TestBuildFailurePolicy(t *testing.T) {
	spec := build(t, runSpec()).Spec
	if *spec.Completions != 1 || *spec.Parallelism != 1 || *spec.BackoffLimit != 3 {
		t.Errorf("completions %d, parallelism %d, backoffLimit %d; want 1, 1, 3",
			*spec.Completions, *spec.Parallelism, *spec.BackoffLimit)
	}
	if spec.PodReplacementPolicy == nil || *spec.PodReplacementPolicy != batchv1.Failed {
		t.Errorf("podReplacementPolicy = %v, want Failed", spec.PodReplacementPolicy)
	}
	want := []batchv1.PodFailurePolicyRule{
		{Action: batchv1.PodFailurePolicyActionFailJob, OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
			Operator: batchv1.PodFailurePolicyOnExitCodesOpIn, Values: []int32{10, 12}}},
		{Action: batchv1.PodFailurePolicyActionIgnore, OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{
			{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue}}},
		{Action: batchv1.PodFailurePolicyActionIgnore, OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{
			{Type: "TerminationTarget", Status: corev1.ConditionTrue}}},
	}
	if spec.PodFailurePolicy == nil || !equalRules(spec.PodFailurePolicy.Rules, want) {
		t.Errorf("podFailurePolicy = %+v, want %+v", spec.PodFailurePolicy, want)
	}
}

func equalRules(a, b []batchv1.PodFailurePolicyRule) bool {
	return slices.EqualFunc(a, b, func(x, y batchv1.PodFailurePolicyRule) bool {
		if x.Action != y.Action || (x.OnExitCodes == nil) != (y.OnExitCodes == nil) || x.OnExitCodes != nil && x.OnExitCodes.ContainerName != nil {
			return false
		}
		if x.OnExitCodes != nil && (x.OnExitCodes.Operator != y.OnExitCodes.Operator || !slices.Equal(x.OnExitCodes.Values, y.OnExitCodes.Values)) {
			return false
		}
		return slices.EqualFunc(x.OnPodConditions, y.OnPodConditions, func(p, q batchv1.PodFailurePolicyOnPodConditionsPattern) bool {
			return p.Type == q.Type && p.Status == q.Status
		})
	})
}

func TestBuildControllerLabelsWinOverMoverPodLabels(t *testing.T) {
	spec := runSpec()
	spec.PodLabels = map[string]string{
		"app.kubernetes.io/component":  "other",
		restorejob.LabelRestoreRun:     "x",
		"kueue.x-k8s.io/queue-name":    "backups",
		"app.kubernetes.io/managed-by": "someone",
	}
	job := build(t, spec)
	want := map[string]string{
		"app.kubernetes.io/managed-by": "backup-controller",
		"app.kubernetes.io/component":  "restore",
		restorejob.LabelRestoreRun:     "run-uid",
	}
	for k, v := range want {
		if job.Labels[k] != v {
			t.Errorf("Job label %s = %q, want %q", k, job.Labels[k], v)
		}
		if job.Spec.Template.Labels[k] != v {
			t.Errorf("pod label %s = %q, want %q", k, job.Spec.Template.Labels[k], v)
		}
	}
	if job.Spec.Template.Labels["kueue.x-k8s.io/queue-name"] != "backups" {
		t.Errorf("pod labels = %v, want the queue label", job.Spec.Template.Labels)
	}
	if _, ok := job.Labels["kueue.x-k8s.io/queue-name"]; ok {
		t.Error("the queue label is on the Job; VolSync puts moverPodLabels on the pod only")
	}
	if spec.PodLabels["app.kubernetes.io/component"] != "other" {
		t.Error("Build changed the caller's map")
	}
}

func TestBuildLabelsAPopulatorJobByClaim(t *testing.T) {
	spec := runSpec()
	spec.Origin = restorejob.Origin{Kind: restorejob.OriginClaim, UID: "claim-uid"}
	job := build(t, spec)
	if job.Labels[restorejob.LabelRestoreClaim] != "claim-uid" || job.Spec.Template.Labels[restorejob.LabelRestoreClaim] != "claim-uid" {
		t.Errorf("labels = %v, want restore-claim", job.Labels)
	}
	if _, ok := job.Labels[restorejob.LabelRestoreRun]; ok {
		t.Errorf("labels = %v, want no restore-run label", job.Labels)
	}
	for k, v := range restorejob.ManagedLabels() {
		if job.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, job.Labels[k], v)
		}
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
