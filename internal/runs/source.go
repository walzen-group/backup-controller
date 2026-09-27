package runs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// retryable reports whether an error from a read may go away when the read is
// tried again. That is a failed call to the API server, other than one that
// found nothing, and a request that never reached it. A missing object, a kind
// the cluster doesn't serve, and an error the caller built from what it read
// are not retryable. A failed lookup of a served version and a request at a
// version the API server has stopped serving are retryable (see
// served.Transient); the second comes back as NotFound and never means
// the object is missing.
//
// It sorts the errors of a function that doesn't return refusals, such as
// bootstrap.ResolveLocation, which wraps each failed read and describes a
// missing field in an error of its own.
func retryable(err error) bool {
	if served.Transient(err) {
		return true
	}
	if err == nil || apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return false
	}
	var status apierrors.APIStatus
	var request *url.Error
	switch {
	case errors.As(err, &status):
		return true
	case errors.As(err, &request):
		// url.Parse reports a malformed URL as a url.Error too.
		return request.Op != "parse"
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// moverCPU is the CPU request on each backup mover. It sets the mover's share
// of CPU on a busy worker, and it is the amount the scheduler must find free
// on the worker that holds the volume.
var moverCPU = resource.MustParse("500m")

// enabledClaims lists the claims in a namespace that carry the annotation
// backup.wlz.li/enabled: "true", sorted by name. A claim that is being deleted
// is left out.
func enabledClaims(ctx context.Context, c client.Reader, namespace string) ([]corev1.PersistentVolumeClaim, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, claims, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the claims in %s: %w", namespace, err)
	}
	var enabled []corev1.PersistentVolumeClaim
	for _, claim := range claims.Items {
		if backupv1alpha1.Enabled(claim.Annotations) && claim.DeletionTimestamp.IsZero() {
			enabled = append(enabled, claim)
		}
	}
	sort.Slice(enabled, func(i, j int) bool { return enabled[i].Name < enabled[j].Name })
	return enabled, nil
}

// volumeRestoreFor returns the VolumeRestore that describes a claim's restic
// repository. For a claim whose spec.dataSourceRef names a VolumeRestore, it
// is that one. A fixed-name claim names none, and for it the VolumeRestore
// with the claim's own name is used.
//
// It returns a refusal naming the claim when that VolumeRestore doesn't
// exist, and a plain error when the read fails for another reason.
func volumeRestoreFor(ctx context.Context, c client.Reader, claim *corev1.PersistentVolumeClaim) (*backupv1alpha1.VolumeRestore, error) {
	name := claim.Name
	if ref := claim.Spec.DataSourceRef; ref != nil && ref.Kind == "VolumeRestore" {
		name = ref.Name
	}
	vr := &backupv1alpha1.VolumeRestore{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: name}, vr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, refuse(backupv1alpha1.ItemReasonVolumeRestoreMissing, "claim %s has no VolumeRestore %s to name its repository", claim.Name, name)
		}
		return nil, fmt.Errorf("get VolumeRestore %s/%s: %w", claim.Namespace, name, err)
	}
	return vr, nil
}

// volumeAffinity returns a node affinity that places a pod on the node that
// holds the claim's volume. It copies the required node affinity from the
// claim's PersistentVolume. zfs-localpv writes that affinity on every volume
// it provisions, and it holds whether or not a pod mounts the claim.
//
// It returns a refusal when the claim isn't bound yet, when its
// PersistentVolume doesn't exist, or when the PersistentVolume declares no
// required node affinity. A failed read of the PersistentVolume comes back as
// a plain error, which the caller retries.
func volumeAffinity(ctx context.Context, c client.Reader, claim *corev1.PersistentVolumeClaim) (*corev1.Affinity, error) {
	if claim.Spec.VolumeName == "" || claim.Status.Phase != corev1.ClaimBound {
		return nil, refuse(backupv1alpha1.ItemReasonClaimNotBound, "claim %s is not bound to a volume yet", claim.Name)
	}
	pv := &corev1.PersistentVolume{}
	if err := c.Get(ctx, types.NamespacedName{Name: claim.Spec.VolumeName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, refuse(backupv1alpha1.ItemReasonVolumeMissing, "claim %s is bound to the PersistentVolume %s, which does not exist", claim.Name, claim.Spec.VolumeName)
		}
		return nil, fmt.Errorf("get PersistentVolume %s: %w", claim.Spec.VolumeName, err)
	}
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return nil, refuse(backupv1alpha1.ItemReasonNoNodeAffinity, "the PersistentVolume %s declares no node affinity to place the mover by", pv.Name)
	}
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: pv.Spec.NodeAffinity.Required.DeepCopy(),
		},
	}, nil
}

// sourceSettings are the parts of a claim's ReplicationSource that come from
// outside the source: from the claim, its VolumeRestore, its volume and the
// namespace.
type sourceSettings struct {
	// vr is the claim's VolumeRestore, which names the repository, the cache
	// class and capacity, and the mover's security context.
	vr *backupv1alpha1.VolumeRestore

	// affinity places the mover on the node that holds the claim's volume.
	affinity *corev1.Affinity

	// retain is the retention policy from the claim's annotations.
	retain *volsyncv1alpha1.ResticRetainPolicy

	// pruneInterval is the namespace's prune interval in days.
	pruneInterval int32
}

// sourceSettingsFor reads the settings of a claim's ReplicationSource.
//
// Parameters:
//   - reader reads the claim's VolumeRestore and PersistentVolume and the
//     Namespace. Callers pass the uncached Reader.
//   - claim is the claim to back up, as stored.
//
// It returns a refusal when one of the settings is missing or does not
// parse: the claim has no VolumeRestore (see volumeRestoreFor), is not bound
// or its volume declares no node affinity (see volumeAffinity), its
// retention annotations don't give a policy (see retention), or the
// namespace's prune interval doesn't parse (see pruneIntervalFor). Any other
// failed read comes back as a plain error, which the caller retries.
// ensureSource and the quiesce pre-check both call it, so an item the one
// refuses the other refuses with the same message.
func sourceSettingsFor(ctx context.Context, reader client.Reader, claim *corev1.PersistentVolumeClaim) (sourceSettings, error) {
	vr, err := volumeRestoreFor(ctx, reader, claim)
	if err != nil {
		return sourceSettings{}, err
	}
	affinity, err := volumeAffinity(ctx, reader, claim)
	if err != nil {
		return sourceSettings{}, err
	}
	retain, err := retention(claim)
	if err != nil {
		return sourceSettings{}, err
	}
	pruneInterval, err := pruneIntervalFor(ctx, reader, claim.Namespace)
	if err != nil {
		return sourceSettings{}, err
	}
	return sourceSettings{vr: vr, affinity: affinity, retain: retain, pruneInterval: pruneInterval}, nil
}

// foreignSource returns the refusal for a ReplicationSource of the claim's
// name that this controller did not write, which it never writes over.
//
// Parameters:
//   - name is the claim's name, which the source shares.
//
// It returns a *refusalError with reason SourceNotManaged.
func foreignSource(name string) error {
	return refuse(backupv1alpha1.ItemReasonSourceNotManaged, "the ReplicationSource %s exists and was not written by backup-controller; remove it so the controller can write its own", name)
}

// ensureSource creates or updates the ReplicationSource that backs up a claim,
// with the run's tag as its manual trigger. Setting the tag is what starts
// VolSync's backup.
//
// Parameters:
//   - c reads any ReplicationSource that already exists and writes it.
//   - reader reads the claim's VolumeRestore and PersistentVolume and the
//     Namespace, and lists the BackupRuns when the source is in use.
//   - claim is the claim to back up. The ReplicationSource takes its name.
//   - tag is the run's manual trigger, from TriggerFor. VolSync starts a
//     backup when spec.trigger.manual holds a value it hasn't completed yet.
//   - lock is the run's item, which takes the Leases of the claim and its
//     repository (see acquireLeases) right before the write.
//
// It returns the ReplicationSource as it stands after the call, a hold, and
// an error. It writes nothing and returns the source as read in these cases:
//   - The source already carries tag, whether VolSync is still syncing it or
//     has completed it. A lost status write after the first write gets here.
//   - The source is in use (see inUse) with another tag, or with none. It
//     then also returns what holder gives: a hold of kind holdSourceBusy
//     while a live run waits for that tag, or a *refusalError with reason
//     SourceAbandoned when no run does.
//   - Another live run holds the Lease of the claim or its repository (see
//     acquireLeases), or a live RestoreRun's mover works on either (see
//     otherMover). It then also returns a hold of kind holdSourceBusy that
//     names that run. The run waits.
//
// It returns a refusal, and writes nothing, when a ReplicationSource of the
// same name exists without the label app.kubernetes.io/managed-by:
// backup-controller, or when one of the settings below is missing or doesn't
// parse. It also returns a refusal when the API server rejects the source as
// invalid. Any other failed read or write comes back as a plain error, which
// the caller retries. That includes a Conflict or AlreadyExists when another
// writer changed or created the source after the read the write is based on.
// A check that stops the write with the zero hold also gives a plain error
// (see heldOf).
//
// The checks run inside the mutate function of controllerutil.CreateOrUpdate,
// on the object whose resourceVersion the update carries. When another run
// writes its tag after that read, the API server refuses the update with a
// Conflict, and the next pass decides again from the stored source. A check
// on an earlier read would let two runs pass it and the second overwrite the
// first run's tag; VolSync's running sync would then complete the second tag
// with a clone cut for the first run.
//
// On an idle source, or a new one, the controller writes every field of the
// spec. The repository, the cache class and the mover's security context
// come from the claim's VolumeRestore. The retention comes from the claim's
// annotations, the prune interval from the namespace's annotation, and the
// mover's node affinity from the claim's volume. The source carries a
// controller reference to the claim, so a claim deleted for a restore takes
// its source with it, and the next run writes a new one. spec.restic.unlock
// gets the same value as the trigger, so every new backup first removes the
// stale locks a killed mover can leave in the repository.
//
// A source of the same name that this controller didn't write is left alone.
// Something else declares it, and writing over it would start a fight that the
// other writer wins on its next reconcile.
func ensureSource(ctx context.Context, c client.Client, reader client.Reader, claim *corev1.PersistentVolumeClaim, tag string, lock leaseHolder) (*volsyncv1alpha1.ReplicationSource, hold, error) {
	settings, err := sourceSettingsFor(ctx, reader, claim)
	if err != nil {
		return nil, hold{}, err
	}
	write := sourceWrite{c: c, reader: reader, claim: claim, tag: tag, lock: lock, settings: settings}
	source := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: claim.Namespace},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, source, func() error {
		fill, err := write.check(ctx, source)
		if err != nil || !fill {
			return err
		}
		return write.fill(source)
	})
	if busy, found := heldOf(err); found {
		return source, busy, nil
	}
	reason, refused := asItemFailure(err)
	switch {
	case reason == backupv1alpha1.ItemReasonSourceAbandoned:
		return source, hold{}, err
	case refused:
		return nil, hold{}, err
	case apierrors.IsInvalid(err):
		return nil, hold{}, refuse(backupv1alpha1.ItemReasonSourceRefused, "the API server refused ReplicationSource %s/%s: %v", claim.Namespace, claim.Name, err)
	case err != nil:
		return nil, hold{}, fmt.Errorf("write ReplicationSource %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	return source, hold{}, nil
}

// sourceWrite holds what ensureSource needs inside the mutate function of
// controllerutil.CreateOrUpdate.
type sourceWrite struct {
	// c writes the Leases.
	c client.Client
	// reader reads the Leases, the BackupRuns and the restore Jobs, uncached.
	reader client.Reader
	// claim is the claim to back up.
	claim *corev1.PersistentVolumeClaim
	// tag is the manual trigger of the run.
	tag string
	// lock is the item of the run that takes the Leases.
	lock leaseHolder
	// settings are the settings of the source from sourceSettingsFor.
	settings sourceSettings
}

// check decides whether ensureSource writes the source.
//
// Parameters:
//   - source is the source that CreateOrUpdate read. It has no
//     resourceVersion when the source does not exist.
//
// It returns true when the run can write its tag. It returns false and no
// error when the source already carries the tag. It returns a *heldError
// when the run must wait: holder, acquireLeases or otherMover gave a hold.
// It returns the refusal from foreignSource for a source that this
// controller did not write. It returns the error from holder,
// acquireLeases or otherMover when one of them fails.
func (w sourceWrite) check(ctx context.Context, source *volsyncv1alpha1.ReplicationSource) (bool, error) {
	// CreateOrUpdate hands a source it didn't find over empty, with no
	// resourceVersion. Anything else is the stored source the update
	// will be based on.
	if source.ResourceVersion != "" {
		switch {
		case source.Labels[backupv1alpha1.LabelManagedBy] != backupv1alpha1.ManagedByValue:
			return false, foreignSource(w.claim.Name)
		case manualTag(source) == w.tag:
			// Unchanged, so CreateOrUpdate writes nothing.
			return false, nil
		case inUse(source):
			busy, err := holder(ctx, w.reader, source)
			if err != nil {
				return false, err
			}
			return false, &heldError{hold: busy}
		}
	}
	repository := w.settings.vr.Spec.Repository
	// The Leases make the backup and a restore of the claim or its
	// repository exclusive: of two runs that get here in the same
	// instant, only one creates each Lease.
	busy, err := acquireLeases(ctx, w.c, w.reader, leaseRequest{holder: w.lock, namespace: w.claim.Namespace, claim: w.claim.Name, secret: repository})
	if err != nil {
		return false, err
	}
	if busy.held() {
		return false, &heldError{hold: busy}
	}
	// A restore of the claim or its repository that already has its
	// restore Job goes first, and so does a Job whose pods may still
	// write after its run has ended. The check runs here, right before
	// the write, so a restore that created its Job after an earlier
	// check is still seen.
	restoring, err := otherMover(ctx, w.reader, w.claim.Namespace, w.claim.Name, repository, restoreMover)
	if err != nil {
		return false, err
	}
	if restoring.held() {
		return false, &heldError{hold: restoring}
	}
	return true, nil
}

// fill writes the label, the owner reference and the spec onto the source,
// as ensureSource describes.
//
// Parameters:
//   - source is the source that CreateOrUpdate writes next.
//
// It returns an error when the owner reference cannot be set.
func (w sourceWrite) fill(source *volsyncv1alpha1.ReplicationSource) error {
	vr, settings, claim, tag := w.settings.vr, w.settings, w.claim, w.tag
	if source.Labels == nil {
		source.Labels = map[string]string{}
	}
	source.Labels[backupv1alpha1.LabelManagedBy] = backupv1alpha1.ManagedByValue
	if err := controllerutil.SetControllerReference(claim, source, w.c.Scheme()); err != nil {
		return err
	}
	source.Spec = volsyncv1alpha1.ReplicationSourceSpec{
		SourcePVC: claim.Name,
		Trigger:   &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: tag},
		Restic: &volsyncv1alpha1.ReplicationSourceResticSpec{
			ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
				CopyMethod: volsyncv1alpha1.CopyMethodClone,
				// VolSync builds the clone and the cache afresh on every
				// run, so both come from the class the VolumeRestore names
				// for its cache, whose reclaim policy is Delete.
				StorageClassName: vr.Spec.CacheStorageClassName,
			},
			Repository: vr.Spec.Repository,
			// A new value makes every mover pod of this sync run
			// `restic unlock` before `restic backup`, until a Job
			// succeeds and VolSync records it in status.restic.lastUnlocked
			// (VolSync v0.16.0 internal/controller/mover/restic/mover.go:373-376,
			// 631-635, 669-674). entry.sh runs plain `restic unlock`
			// (mover-restic/entry.sh:173-174), which removes only stale
			// locks (restic 0.18.1 cmd/restic/cmd_unlock.go:51-54,
			// internal/repository/lock.go:274-294). A lock from another
			// host, which a killed mover's always is, counts as stale once
			// it is older than 30 minutes (internal/restic/lock.go:252-288).
			// The field is written only here, onto a new or idle source,
			// which has no mover Job for the change to replace.
			Unlock:                tag,
			PruneIntervalDays:     &settings.pruneInterval,
			Retain:                settings.retain,
			CacheCapacity:         vr.Spec.CacheCapacity,
			CacheStorageClassName: vr.Spec.CacheStorageClassName,
			MoverConfig: volsyncv1alpha1.MoverConfig{
				MoverSecurityContext: vr.Spec.MoverSecurityContext,
				MoverAffinity:        settings.affinity,
				MoverResources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: moverCPU},
				},
			},
		},
	}
	return nil
}
