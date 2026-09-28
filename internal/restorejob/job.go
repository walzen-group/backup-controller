// Package restorejob builds the Job that writes one restic snapshot into a
// claim. The RestoreRun and the populator use it, because VolSync restores
// by time only, and the controller restores the exact snapshot it selected.
//
// The Job runs restic from the image the controller gets in --restore-image,
// which is VolSync's mover image, so the restic that restores is the one that
// wrote the repository. It restores the way VolSync's mover does
// (mover-restic/entry.sh in VolSync 0.16.0): the snapshot's root is the
// content of the volume, and it goes into /data.
package restorejob

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// Label marks every restore Job and its pod. Its value is the name of what
// asked for the restore, such as the RestoreRun.
const Label = "backup.wlz.li/restore"

// Spec says what one restore Job writes where.
type Spec struct {
	// Namespace and Name identify the Job. The claim and the repository
	// Secret are in the same namespace.
	Namespace, Name string
	// Image holds restic, from the controller's --restore-image.
	Image string
	// Claim is the claim the Job writes into, mounted at /data.
	Claim string
	// RepositorySecret is the Secret that names the restic repository, with
	// RESTIC_REPOSITORY, RESTIC_PASSWORD and the S3 credentials, as VolSync
	// reads it.
	RepositorySecret string
	// SnapshotID is the full ID of the snapshot to restore.
	SnapshotID string
	// SecurityContext is the pod's security context, from the
	// VolumeRestore's or the RestoreRun's moverSecurityContext. Nil runs
	// restic as the image's user, which can set every file's owner.
	SecurityContext *corev1.PodSecurityContext
	// Owner is the value of Label on the Job and its pod.
	Owner string
	// OwnerReferences are set on the Job. A RestoreRun owns its Jobs, so a
	// failed Job it keeps for its logs goes away with the run. The
	// populator's Jobs live in another namespace than their claim and have
	// none.
	OwnerReferences []metav1.OwnerReference
	// Labels are more labels for the Job and its pod, such as the Kueue
	// queue label from a VolumeRestore's moverPodLabels. With that label,
	// Kueue's Job integration admits the Job through the queue.
	Labels map[string]string
}

// New returns the Job that restores the snapshot into the claim.
//
// Parameters:
//   - spec says what the Job restores and where.
//
// restic restores the snapshot into /data and, with --delete, removes every
// file there that the snapshot does not hold, so the claim holds exactly the
// snapshot afterwards. With --retry-lock, restic waits up to 5 minutes for
// an exclusive lock on the repository, such as a retime's, to go away before
// it gives up. The restic cache lives in an emptyDir. The Job tries 4 times
// before it fails, so a short S3 outage does not fail the restore. The Job's
// success or failure is its only result; no log is read.
func New(spec Spec) *batchv1.Job {
	labels := map[string]string{Label: spec.Owner}
	for k, v := range spec.Labels {
		labels[k] = v
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: spec.Namespace, Name: spec.Name, Labels: labels, OwnerReferences: spec.OwnerReferences},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To[int32](3),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyNever,
					SecurityContext: spec.SecurityContext,
					Containers: []corev1.Container{{
						Name:    "restic",
						Image:   spec.Image,
						Command: []string{"restic"},
						Args: []string{
							"restore", spec.SnapshotID,
							"--retry-lock", "5m",
							"--target", "/data",
							"--delete",
							"--include-xattr", "user.*",
						},
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: spec.RepositorySecret}},
						}},
						Env: []corev1.EnvVar{
							{Name: "RESTIC_CACHE_DIR", Value: "/cache"},
							{Name: "HOME", Value: "/tmp"},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
							{Name: "cache", MountPath: "/cache"},
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: spec.Claim}}},
						{Name: "cache", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
}

// Outcome is what a restore Job reports about itself.
type Outcome int

const (
	// Running is a Job that has neither succeeded nor failed yet.
	Running Outcome = iota
	// Succeeded is a Job with the condition Complete.
	Succeeded
	// Failed is a Job with the condition Failed: restic failed on every
	// try.
	Failed
)

// Result reads the outcome of a restore Job from its conditions.
func Result(job *batchv1.Job) Outcome {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return Succeeded
		case batchv1.JobFailed:
			return Failed
		}
	}
	return Running
}
