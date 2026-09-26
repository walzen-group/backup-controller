package runs

import (
	"context"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The tests in this file check an into restore from a claim (spec.claim with
// spec.into). It restores through a ReplicationDestination the run creates,
// as an into restore from a repository does, so the mover's log confirms the
// snapshot (designs/restorerun.md C-pop-1, R1).

// sourceOnNode returns the app's claim, as claim does, with the node the
// scheduler selected for its volume.
func sourceOnNode() *corev1.PersistentVolumeClaim {
	source := claim()
	source.Annotations[selectedNodeAnnotation] = "worker-1"
	return source
}

// An into restore from a claim creates a plain claim with the source claim's
// size, class and node and no data source, and a ReplicationDestination in
// the app's namespace whose mover writes the snapshot the checks selected
// into it, with the source's repository, cache class and queue label. No
// VolumeRestore is created. The run succeeds once the mover's log names that
// snapshot, and deletes its destination. Before, the run created a
// VolumeRestore and a claim naming it, and reported Succeeded once the claim
// was Bound, with no evidence of which snapshot the populator restored.
func TestAnIntoRestoreFromAClaimWritesThroughItsOwnDestination(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(intoMonday), sourceOnNode(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create

	run := readRestoreRun(t, c)
	scratch := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "notes-data-monday", scratch)
	request := scratch.Spec.Resources.Requests[corev1.ResourceStorage]
	if request.Cmp(resource.MustParse("1Gi")) != 0 || scratch.Spec.StorageClassName == nil || *scratch.Spec.StorageClassName != "zfs" ||
		scratch.Annotations[selectedNodeAnnotation] != "worker-1" || scratch.Spec.DataSourceRef != nil || scratch.Spec.DataSource != nil {
		t.Fatalf("claim = %+v, annotations %v; want 1Gi of zfs on worker-1 with no data source", scratch.Spec, scratch.Annotations)
	}
	if !metav1.IsControlledBy(scratch, run) {
		t.Errorf("claim owners = %v, want the run as controller", scratch.OwnerReferences)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}, &backupv1alpha1.VolumeRestore{}); !apierrors.IsNotFound(err) {
		t.Errorf("VolumeRestore notes-data-monday: %v, want none created", err)
	}

	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning || item.Destination != destinationName(restoreUID, 0) {
		t.Fatalf("item = %+v; want Running with destination %s", item, destinationName(restoreUID, 0))
	}
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, item.Destination, rd)
	spec := rd.Spec.Restic
	if rd.Spec.Trigger == nil || rd.Spec.Trigger.Manual != string(restoreUID) || spec == nil ||
		spec.CopyMethod != volsyncv1alpha1.CopyMethodDirect || spec.DestinationPVC == nil || *spec.DestinationPVC != "notes-data-monday" ||
		spec.Repository != repoN || spec.RestoreAsOf == nil || *spec.RestoreAsOf != "2026-09-21T05:00:02Z" || spec.Previous != nil ||
		spec.CacheStorageClassName == nil || *spec.CacheStorageClassName != "zfs-ephemeral" || !spec.EnableFileDeletion ||
		spec.MoverPodLabels["kueue.x-k8s.io/queue-name"] != "backups" {
		t.Fatalf("destination = %+v, restic %+v; want monday's snapshot written into notes-data-monday from %s", rd.Spec, spec, repoN)
	}

	completeVolume(t, c)
	run = stepUntilFinished(t, r, c, 2)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded {
		t.Fatalf("phase = %q, item = %+v (%s); want Succeeded", run.Status.Phase, run.Status.Items[0], readyMessage(run.Status.Conditions))
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the run's deleted", names)
	}
}
