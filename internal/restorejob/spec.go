// Package restorejob builds, reads and stops the controller's own restic
// restore Job, the one Job that writes a restored volume for a RestoreRun and
// for the VolumeRestore populator.
//
// The Job runs restic from the declared restic image with restic itself as
// the container command, so restic gets SIGTERM directly and exits at once
// when the Job is stopped. It restores one snapshot named by its full ID, so
// restic never picks a snapshot on its own. Its result is read from the
// Job's terminal conditions only; the exit code and restic's last lines,
// which the pods carry, are shown to a person and decide nothing. The package
// holds the Job's shape (Build), its reading (Read) and its stop (Stop), and
// no knowledge of the RestoreRun or the populator.
package restorejob

import (
	"fmt"
	"maps"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// The labels and annotations every restore Job carries. The admission policy
// on the controller's Jobs, the exclusion of restores and backups on one
// claim, and a run's takeover of its own Job after a restart match on them.
const (
	// LabelManagedBy and LabelComponent, with the values ManagedBy and
	// Component, mark a Job and its pods as the controller's restore Job.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	LabelComponent = "app.kubernetes.io/component"
	ManagedBy      = "backup-controller"
	Component      = "restore"
	// LabelRestoreRun holds the UID of the RestoreRun a Job restores for.
	LabelRestoreRun = "backup.wlz.li/restore-run"
	// LabelRestoreClaim holds the UID of the claim the populator fills.
	LabelRestoreClaim = "backup.wlz.li/restore-claim"
	// AnnotationSnapshotID holds the full ID of the snapshot the Job
	// restores.
	AnnotationSnapshotID = "backup.wlz.li/snapshot-id"
	// AnnotationClaim holds the name of the claim the Job writes. A claim
	// name can be longer than a label value, so it is an annotation.
	AnnotationClaim = "backup.wlz.li/claim"
	// AnnotationRepository holds the name of the repository Secret.
	AnnotationRepository = "backup.wlz.li/repository"
	// AnnotationPrivilegedMovers is VolSync's namespace annotation that lets
	// a mover run as root with the capabilities to restore file ownership.
	AnnotationPrivilegedMovers = "volsync.backube/privileged-movers"
)

// The mount points and the flags of the restore, as VolSync's restic mover
// uses them (mover-restic/entry.sh and mover/restic/mover.go in VolSync
// 0.16.0).
const (
	dataDir      = "/data"
	cacheDir     = "/cache"
	tempDir      = "/tmp"
	volumeData   = "data"
	volumeCache  = "cache"
	volumeTemp   = "tempdir"
	retryLock    = "30m"
	backoffLimit = 3
)

// terminationTarget is the pod condition Kueue's pod integration sets on a
// pod it stops (kueue v0.19.5 pkg/controller/jobs/pod/pod_controller.go:70).
const terminationTarget corev1.PodConditionType = "TerminationTarget"

// defaultCacheCapacity is the size of the cache volume when the settings
// name none, the size VolSync's mover uses (mover/restic/mover.go:200).
var defaultCacheCapacity = resource.MustParse("1Gi")

// OriginKind says what a restore Job restores for.
type OriginKind int

// The two things a restore Job restores for.
const (
	// OriginRestoreRun is a RestoreRun's item; Origin.UID is the run's UID.
	OriginRestoreRun OriginKind = iota + 1
	// OriginClaim is a claim the populator fills; Origin.UID is the claim's
	// UID.
	OriginClaim
)

// Origin names what a restore Job restores for. It sets the label
// LabelRestoreRun or LabelRestoreClaim on the Job and its pods.
type Origin struct {
	// Kind is OriginRestoreRun or OriginClaim.
	Kind OriginKind
	// UID is the run's or the claim's UID.
	UID types.UID
}

// labelKey returns the label that names the origin, or false for a kind
// that is not one of the two.
func (o Origin) labelKey() (string, bool) {
	switch o.Kind {
	case OriginRestoreRun:
		return LabelRestoreRun, true
	case OriginClaim:
		return LabelRestoreClaim, true
	default:
		return "", false
	}
}

// Spec holds everything that decides a restore Job. Build turns it into the
// Job, and nothing else writes the Job's shape.
type Spec struct {
	// Name is the Job's name, which the caller derives from what it restores
	// for, so it finds the Job again after a restart.
	Name string
	// Namespace is the Job's namespace: a RestoreRun's own, or the
	// controller's for the populator.
	Namespace string
	// Origin names what the Job restores for.
	Origin Origin
	// Owner is the Job's only owner reference: a controller reference to the
	// RestoreRun, or a plain reference to the populator's prime claim, so the
	// garbage collector removes a Job its owner no longer needs.
	Owner metav1.OwnerReference
	// SnapshotID is the full, 64-character ID of the snapshot to restore.
	SnapshotID string
	// Claim is the name of the claim the Job writes, mounted at /data.
	Claim string
	// Repository is the name of the Secret that holds the repository's
	// environment (RESTIC_REPOSITORY, RESTIC_PASSWORD and the S3 keys).
	Repository string
	// Image is the restic image, the controller's --restore-image.
	Image string
	// Delete passes --delete, so files the snapshot does not hold are
	// removed. A RestoreRun and the populator set it on every Job.
	Delete bool
	// Privileged runs restic as root with the capabilities to restore file
	// ownership. The caller sets it from PrivilegedMovers on the Job's
	// namespace.
	Privileged bool
	// SecurityContext is the pod's securityContext, the settings'
	// moverSecurityContext. Build copies it.
	SecurityContext *corev1.PodSecurityContext
	// PodLabels are the settings' moverPodLabels, put on the pod only. The
	// controller's labels win over a key they share.
	PodLabels map[string]string
	// CacheStorageClassName is the storage class of the cache volume; nil
	// uses the cluster's default class.
	CacheStorageClassName *string
	// CacheCapacity is the size of the cache volume; nil means 1Gi.
	CacheCapacity *resource.Quantity
}

// SpecError reports a Spec that Build refuses to turn into a Job. It is the
// controller's own bug, so the caller fails the restore loudly and creates
// nothing.
type SpecError struct {
	// Field is the Spec field at fault.
	Field string
	// Problem says what is wrong with it.
	Problem string
}

// Error names the field and the problem.
func (e *SpecError) Error() string {
	return fmt.Sprintf("restore Job spec: %s %s", e.Field, e.Problem)
}

// ManagedLabels returns the labels every restore Job and its pods carry, to
// select the controller's restore Jobs by.
func ManagedLabels() map[string]string {
	return map[string]string{LabelManagedBy: ManagedBy, LabelComponent: Component}
}

// PrivilegedMovers reports whether a namespace lets movers run privileged,
// the way VolSync decides it: the annotation AnnotationPrivilegedMovers with
// the value "true" in any case (internal/controller/utils/namespace.go:48 in
// VolSync 0.16.0).
//
// Parameters:
//   - ns is the namespace the Job runs in, as the caller read it.
//
// It returns false for a namespace without the annotation.
func PrivilegedMovers(ns *corev1.Namespace) bool {
	return strings.ToLower(ns.Annotations[AnnotationPrivilegedMovers]) == "true"
}

// Build returns the restore Job a Spec describes.
//
// Parameters:
//   - spec holds every input of the Job. Build reads it and changes nothing
//     in it.
//
// It returns the Job to create, or a *SpecError and no Job when the snapshot
// ID is not exactly 64 lower-case hex characters or the origin is not one of
// the two kinds. A shorter ID would let restic resolve a snapshot of its own
// choice, such as the only one there is.
//
// The Job is created suspended (spec.suspend true), so the Job controller
// starts no pod for it and no restic runs until the caller has recorded the
// Job and resumes it (see API.ResumeJob). A create answered with an error
// can still store the Job later, and such a Job, or one whose record was
// lost, never writes. Kueue leaves a Job without its queue label alone while
// manageJobsWithoutQueueName is false, as the cluster sets it (kueue v0.19.5
// pkg/controller/jobframework/reconciler.go:368-374), and admits the Job's
// pod through its pod integration once the Job is resumed. So the Job's own
// labels are never empty and never carry the queue label, which goes on the
// pod template only: the API server copies the template's labels onto a
// Job created with none, and Kueue would then manage the Job and resume it
// itself.
//
// The Job runs one pod at a time and fails after four failed pods, or at once
// when restic reports that the repository is missing (exit 10) or the
// password is wrong (exit 12). A pod that Kubernetes or Kueue stops is not
// counted as a failure, and a replacement pod starts only once the old one
// has fully ended. An init container runs restic unlock, which removes only
// stale locks; the restore then waits up to 30 minutes for a live one.
func Build(spec Spec) (*batchv1.Job, error) {
	if !fullID(spec.SnapshotID) {
		return nil, &SpecError{Field: "SnapshotID", Problem: fmt.Sprintf("%q is not a full 64-character lower-case hex ID", spec.SnapshotID)}
	}
	key, ok := spec.Origin.labelKey()
	if !ok || spec.Origin.UID == "" {
		return nil, &SpecError{Field: "Origin", Problem: fmt.Sprintf("%+v names no RestoreRun or claim", spec.Origin)}
	}
	labels := ManagedLabels()
	labels[key] = string(spec.Origin.UID)
	podLabels := maps.Clone(spec.PodLabels)
	if podLabels == nil {
		podLabels = map[string]string{}
	}
	maps.Copy(podLabels, labels)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            spec.Name,
			Namespace:       spec.Namespace,
			Labels:          labels,
			Annotations:     annotations(spec),
			OwnerReferences: []metav1.OwnerReference{spec.Owner},
		},
		Spec: batchv1.JobSpec{
			Suspend:              ptr.To(true),
			Completions:          ptr.To[int32](1),
			Parallelism:          ptr.To[int32](1),
			BackoffLimit:         ptr.To[int32](backoffLimit),
			PodReplacementPolicy: ptr.To(batchv1.Failed),
			PodFailurePolicy:     failurePolicy(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec:       podSpec(spec),
			},
		},
	}, nil
}

// fullID reports whether an ID is exactly 64 lower-case hex characters, the
// form restic resolves straight to one snapshot file.
func fullID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range []byte(id) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// annotations returns the Job's annotations for a Spec.
func annotations(spec Spec) map[string]string {
	return map[string]string{
		AnnotationSnapshotID: spec.SnapshotID,
		AnnotationClaim:      spec.Claim,
		AnnotationRepository: spec.Repository,
	}
}

// failurePolicy returns the Job's pod failure policy. A rule without a
// container name applies to every container and init container.
func failurePolicy() *batchv1.PodFailurePolicy {
	return &batchv1.PodFailurePolicy{Rules: []batchv1.PodFailurePolicyRule{
		{
			Action: batchv1.PodFailurePolicyActionFailJob,
			OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
				Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
				Values:   []int32{exitNoRepository, exitWrongPassword},
			},
		},
		ignoreCondition(corev1.DisruptionTarget),
		ignoreCondition(terminationTarget),
	}}
}

// ignoreCondition returns a rule that does not count a pod with the given
// condition True against the backoff limit.
func ignoreCondition(typ corev1.PodConditionType) batchv1.PodFailurePolicyRule {
	return batchv1.PodFailurePolicyRule{
		Action:          batchv1.PodFailurePolicyActionIgnore,
		OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{{Type: typ, Status: corev1.ConditionTrue}},
	}
}

// podSpec returns the pod of the restore Job: the unlock init container and
// the restore container, with the claim, the cache and an in-memory /tmp.
func podSpec(spec Spec) corev1.PodSpec {
	unlock := container(spec, "unlock", []string{"unlock"})
	unlock.VolumeMounts = []corev1.VolumeMount{
		{Name: volumeCache, MountPath: cacheDir},
		{Name: volumeTemp, MountPath: tempDir},
	}
	restore := container(spec, "restore", restoreArgs(spec))
	restore.VolumeMounts = []corev1.VolumeMount{
		{Name: volumeData, MountPath: dataDir},
		{Name: volumeCache, MountPath: cacheDir},
		{Name: volumeTemp, MountPath: tempDir},
	}
	return corev1.PodSpec{
		RestartPolicy:                corev1.RestartPolicyNever,
		AutomountServiceAccountToken: ptr.To(false),
		SecurityContext:              spec.SecurityContext.DeepCopy(),
		InitContainers:               []corev1.Container{unlock},
		Containers:                   []corev1.Container{restore},
		Volumes:                      volumes(spec),
	}
}

// restoreArgs returns restic's arguments for the restore. They are passed as
// exec arguments with no shell, so the xattr pattern needs no quoting.
func restoreArgs(spec Spec) []string {
	args := []string{
		"restore", spec.SnapshotID,
		"--target", dataDir,
		"--include-xattr", "user.*",
		"--retry-lock", retryLock,
	}
	if spec.Delete {
		args = append(args, "--delete")
	}
	return args
}

// container returns a container that runs restic with the given arguments,
// the repository's environment and VolSync's mover security settings.
//
// Parameters:
//   - spec supplies the image, the repository Secret and whether the
//     container runs privileged.
//   - name is the container's name, which the item's message shows.
//   - args are restic's arguments.
func container(spec Spec, name string, args []string) corev1.Container {
	return corev1.Container{
		Name:    name,
		Image:   spec.Image,
		Command: []string{"restic"},
		Args:    args,
		EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: spec.Repository},
		}}},
		// An explicit variable wins over one of the same name from envFrom.
		Env:                      []corev1.EnvVar{{Name: "RESTIC_CACHE_DIR", Value: cacheDir}},
		SecurityContext:          securityContext(spec.Privileged),
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}
}

// securityContext returns the container settings of VolSync's restic mover
// (mover/restic/mover.go:490-496 and :590-605 in VolSync 0.16.0): no
// capabilities, no privilege escalation and a read-only root filesystem, and
// for a privileged mover root with the three capabilities that restore file
// ownership and permissions.
func securityContext(privileged bool) *corev1.SecurityContext {
	sc := &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		Privileged:               ptr.To(false),
		ReadOnlyRootFilesystem:   ptr.To(true),
	}
	if privileged {
		sc.RunAsUser = ptr.To[int64](0)
		sc.Capabilities.Add = []corev1.Capability{"DAC_OVERRIDE", "CHOWN", "FOWNER"}
	}
	return sc
}

// volumes returns the pod's volumes: the claim to restore into, a generic
// ephemeral volume for restic's cache that lives and dies with the pod, and
// an in-memory /tmp.
func volumes(spec Spec) []corev1.Volume {
	capacity := defaultCacheCapacity
	if spec.CacheCapacity != nil {
		capacity = *spec.CacheCapacity
	}
	cache := corev1.PersistentVolumeClaimSpec{
		AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		StorageClassName: spec.CacheStorageClassName,
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: capacity},
		},
	}
	return []corev1.Volume{
		{Name: volumeData, VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: spec.Claim},
		}},
		{Name: volumeCache, VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{Spec: cache},
			},
		}},
		{Name: volumeTemp, VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
		}},
	}
}
