package runs

import (
	"context"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// wait is what a restore item waits for, as a Ready reason and a message.
// An empty reason means the item does not wait.
type wait struct {
	reason, message string
}

// restoreVolume moves one volume item a step further.
//
// Parameters:
//   - run is the RestoreRun. Its status is written when the item records a
//     Job name.
//   - index is the item's position in status.items, which goes into the
//     Job's name.
//   - item is the volume item.
//
// A Pending item first needs a claim no pod mounts: for an in-place restore
// it waits with reason ClaimInUse while a pod mounts the claim, and for an
// into restore it creates the claim (see intoClaim). It then records the
// Job's name, writes the status, creates the restore Job for the selected
// snapshot, and moves to Running once the create went through. A Running
// item follows its Job: Complete succeeds the item, Failed fails it, and a
// Job that is gone fails it too, since the claim may hold a partial restore.
// The Job stays until the run ends (see release), so a pass whose status
// write fails finds the Job's result again. It returns what the item waits
// for, and an error when an API call fails.
func (r *RestoreRunReconciler) restoreVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (wait, error) {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		if run.Spec.Into != "" {
			if err := r.intoClaim(ctx, run, item); err != nil || item.Phase == backupv1alpha1.ItemFailed {
				return wait{}, err
			}
		} else {
			holder, err := claimHolder(ctx, r.Reader, run.Namespace, item.Name)
			if err != nil {
				return wait{}, err
			}
			if holder != "" {
				return wait{backupv1alpha1.ReasonClaimInUse, fmt.Sprintf(
					"claim %s is mounted by pod %s; list its workload in spec.pauseDuringRestore, or stop it, and the restore starts on its own", item.Name, holder)}, nil
			}
		}
		secret, err := r.sourceRepository(ctx, run, item)
		if err != nil {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return wait{}, nil
		}
		// The Job's name is recorded before the Job exists, so a pass that
		// stops after the create still deletes it at the end. The item stays
		// Pending until the create went through; a pass that finds the name
		// recorded creates the Job again, which gives AlreadyExists when the
		// earlier create reached the API server.
		if item.Job == "" {
			item.Job = fmt.Sprintf("restore-%s-%d", string(run.UID)[:8], index)
			if err := r.writeStatus(ctx, run); err != nil {
				return wait{}, err
			}
		}
		job := restorejob.New(restorejob.Spec{
			Namespace: run.Namespace, Name: item.Job, Image: r.RestoreImage, Claim: item.Name,
			RepositorySecret: secret.Name, SnapshotID: item.SnapshotID,
			SecurityContext: r.moverSecurityContext(ctx, run, item), Owner: run.Name,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, restoreRunKind)},
		})
		if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return wait{}, fmt.Errorf("create restore Job %s: %w", item.Job, err)
		}
		item.Phase = backupv1alpha1.ItemRunning

	case backupv1alpha1.ItemRunning:
		job := &batchv1.Job{}
		err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Job}, job)
		switch {
		case apierrors.IsNotFound(err):
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("restore Job %s was deleted before it finished; claim %s may hold part of snapshot %s", item.Job, item.Name, item.Snapshot)
		case err != nil:
			return wait{}, fmt.Errorf("get restore Job %s: %w", item.Job, err)
		default:
			switch restorejob.Result(job) {
			case restorejob.Running:
				return wait{}, nil
			case restorejob.Succeeded:
				item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
			case restorejob.Failed:
				item.Phase = backupv1alpha1.ItemFailed
				item.Message = fmt.Sprintf("restic failed to restore snapshot %s into claim %s; see kubectl logs job/%s", item.Snapshot, item.Name, item.Job)
			}
		}
	}
	return wait{}, nil
}

// intoClaim makes sure the claim of an into restore exists and is the run's.
//
// It creates the claim spec.into names when no claim has that name, sized by
// spec.intoSize or else by the source claim, in the source claim's storage
// class, and records on the item that the run created it. The claim carries
// no owner reference, so it stays when the run is deleted. When a claim of
// that name exists and the run did not create it, the item fails: the run
// writes only into a claim it created. It returns an error when an API call
// fails.
func (r *RestoreRunReconciler) intoClaim(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) error {
	existing := &corev1.PersistentVolumeClaim{}
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, existing)
	if err == nil {
		if !item.CreatedClaim {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("claim %s already exists; an into restore writes only into a claim it creates, so give spec.into a name no claim has", item.Name)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get claim %s: %w", item.Name, err)
	}

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: item.Name, Labels: map[string]string{restorejob.Label: run.Name}},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}
	size := run.Spec.IntoSize
	if run.Spec.Claim != "" {
		source := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Claim}, source); err != nil {
			return fmt.Errorf("get claim %s: %w", run.Spec.Claim, err)
		}
		claim.Spec.StorageClassName = source.Spec.StorageClassName
		if size == nil {
			if request, ok := source.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
				size = &request
			}
		}
	}
	if size == nil {
		item.Phase, item.Message = backupv1alpha1.ItemFailed, "spec.intoSize is required when spec.claim names no claim to take the size from"
		return nil
	}
	claim.Spec.Resources.Requests = corev1.ResourceList{corev1.ResourceStorage: *size}
	item.CreatedClaim = true
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	if err := r.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create claim %s: %w", item.Name, err)
	}
	return nil
}

// moverSecurityContext returns the pod security context of one restore Job.
//
// Parameters:
//   - run is the RestoreRun.
//   - item is the volume item the Job restores.
//
// It returns spec.moverSecurityContext when the run sets one. Otherwise it
// returns the moverSecurityContext of the VolumeRestore of the source claim:
// the item's own claim for a restore in place, and spec.claim for an into
// restore. It returns nil when there is none.
func (r *RestoreRunReconciler) moverSecurityContext(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) *corev1.PodSecurityContext {
	if run.Spec.MoverSecurityContext != nil {
		return run.Spec.MoverSecurityContext
	}
	name := item.Name
	if run.Spec.Into != "" {
		name = run.Spec.Claim
	}
	if name == "" {
		return nil
	}
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, claim); err != nil {
		return nil
	}
	vr, err := volumeRestoreFor(ctx, r.Reader, claim)
	if err != nil {
		return nil
	}
	return vr.Spec.MoverSecurityContext
}

// deleteJob deletes a restore Job and, in the background, its pods. A Job
// that is already gone is not an error.
func deleteJob(ctx context.Context, c client.Client, namespace, name string) error {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if err := c.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete restore Job %s: %w", name, err)
	}
	return nil
}

// jobPodLeft returns the name of a pod of a restore Job that has not
// stopped, or an empty name when none is left. A Job's pods carry the label
// batch.kubernetes.io/job-name, which the Job controller sets. A deleted
// Job's pods keep running until the garbage collector deletes them and their
// grace period ends.
//
// Parameters:
//   - namespace is the Job's namespace.
//   - job is the Job's name.
//
// It returns an error when the pods can't be listed.
func jobPodLeft(ctx context.Context, c client.Reader, namespace, job string) (string, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{batchv1.JobNameLabel: job}); err != nil {
		return "", fmt.Errorf("list the pods of restore Job %s: %w", job, err)
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return pod.Name, nil
		}
	}
	return "", nil
}

// claimHolder returns the name of a pod that mounts a claim and is not done,
// or an empty name when no such pod exists. It reads the pods from the API
// server, since a restore must not start while a pod still writes the claim.
func claimHolder(ctx context.Context, c client.Reader, namespace, claim string) (string, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace)); err != nil {
		return "", fmt.Errorf("list the pods in %s: %w", namespace, err)
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
				return pod.Name, nil
			}
		}
	}
	return "", nil
}

// restoreDatabase moves one database item a step further.
//
// Parameters:
//   - run is the RestoreRun. Its status is written before the Cluster is
//     deleted.
//   - item is the Cluster item.
//
// A Pending item records the Cluster's UID and the phase Deleted, writes the
// status, and then deletes the Cluster with that UID as a precondition, so
// the delete never reaches a Cluster created since. The webhook recovers a
// Cluster only for a run whose item says Deleted, so the record has to be in
// place before anything can create the Cluster again.
//
// The pass that deletes the Cluster reports that it waits for the shutdown,
// so the run resumes nothing in that pass. A Deleted item waits until the old
// Cluster's pods and claims are gone, and
// then until the Cluster's owner (Flux or tofu) creates it again. A Cluster
// that carries the old UID is deleted again. A new Cluster that carries
// backup.wlz.li/restore-run with the run's name is the run's recovery, and
// the item moves to Recovering. A new Cluster without it came back without
// the run's recovery, and the item fails. A Recovering item succeeds once the
// Cluster is healthy, and fails when the Cluster is deleted.
//
// It returns what the item waits for, and an error when an API call fails.
func (r *RestoreRunReconciler) restoreDatabase(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (wait, error) {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		if err != nil {
			return wait{}, err
		}
		if !found {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the Cluster %s no longer exists", item.Name)
			return wait{}, nil
		}
		item.ClusterUID, item.Phase = string(cluster.GetUID()), backupv1alpha1.ItemDeleted
		if err := r.writeStatus(ctx, run); err != nil {
			return wait{}, err
		}
		if err := deleteClusterWithUID(ctx, r.Client, run.Namespace, item.Name, cluster.GetUID()); err != nil {
			return wait{}, err
		}
		// The old instance shuts down now. The app stays paused and Flux
		// suspended until a later pass sees its pods and claims gone.
		return wait{backupv1alpha1.ReasonShutdown, fmt.Sprintf("waiting for the old Cluster %s to be deleted", item.Name)}, nil

	case backupv1alpha1.ItemDeleted:
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		if err != nil {
			return wait{}, err
		}
		switch {
		case found && string(cluster.GetUID()) == item.ClusterUID:
			if cluster.GetDeletionTimestamp() == nil {
				if err := deleteClusterWithUID(ctx, r.Client, run.Namespace, item.Name, cluster.GetUID()); err != nil {
					return wait{}, err
				}
			}
			return wait{backupv1alpha1.ReasonShutdown, fmt.Sprintf("waiting for the old Cluster %s to be deleted", item.Name)}, nil
		case found && cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun] == run.Name:
			item.Phase = backupv1alpha1.ItemRecovering
			return wait{}, nil
		case found:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the Cluster %s came back without this run's recovery", item.Name)
			return wait{}, nil
		}
		left, err := bootstrap.InstanceLeft(ctx, r.Reader, run.Namespace, item.Name)
		if err != nil {
			return wait{}, err
		}
		if left != "" {
			return wait{backupv1alpha1.ReasonShutdown, fmt.Sprintf("waiting for %s of the deleted Cluster to be gone before anything creates it again", left)}, nil
		}
		return wait{backupv1alpha1.ReasonRecreate, fmt.Sprintf("waiting for the owner of Cluster %s (its Flux Kustomization or terragrunt unit) to create it again", item.Name)}, nil

	case backupv1alpha1.ItemRecovering:
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		switch {
		case err != nil:
			return wait{}, err
		case !found:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, "the recovered Cluster was deleted"
		case clusterPhase(cluster) == healthyPhase:
			item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
		default:
			return wait{backupv1alpha1.ReasonRunning, fmt.Sprintf("Cluster %s is recovering: %s", item.Name, clusterPhase(cluster))}, nil
		}
	}
	return wait{}, nil
}

// deleteClusterWithUID deletes a Cluster only while it still has the given
// UID. A Cluster that is gone, or that another Cluster of the same name
// replaced, is not an error.
func deleteClusterWithUID(ctx context.Context, c client.Client, namespace, name string, uid types.UID) error {
	cluster, found, err := getCluster(ctx, c, namespace, name)
	if err != nil || !found {
		return err
	}
	err = c.Delete(ctx, cluster, client.Preconditions{UID: &uid})
	if err == nil || apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return fmt.Errorf("delete Cluster %s/%s: %w", namespace, name, err)
}
