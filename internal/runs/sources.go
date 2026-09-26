package runs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// errSourceBusy is the error ensureSource returns, wrapped in a sourceHeld,
// when the claim's ReplicationSource is in use (see inUse) with a tag other
// than the run's that a live run waits for (see holder).
// The run waits and tries again later. If it wrote its own tag at that point,
// VolSync's running sync would complete the new tag with a clone cut for the
// older one, and a change to the mover's spec would make VolSync replace the
// running mover Job, which kills restic and leaves its lock in the repository.
var errSourceBusy = errors.New("the ReplicationSource is still completing another run's backup")

// refusal is an error that no retry can fix: something a run names is
// missing or not marked for backup, or a claim's settings don't give a
// source that VolSync can run. A BackupRun that meets one while it plans ends
// with reason Invalid, and an item that meets one fails. Any other error,
// such as a timeout from the API server, is returned so the reconcile runs
// again.
type refusal struct{ message string }

// Error returns the message, which names the object and what is wrong
// with it.
func (e refusal) Error() string { return e.message }

// refuse returns a refusal whose message is built from format and args the
// way fmt.Sprintf builds it.
func refuse(format string, args ...any) error {
	return refusal{fmt.Sprintf(format, args...)}
}

// isRefusal reports whether err, or an error it wraps, is a refusal or an
// invalidSetting, which no retry can fix either.
func isRefusal(err error) bool {
	var refused refusal
	var bad invalidSetting
	return errors.As(err, &refused) || errors.As(err, &bad)
}

// retryable reports whether an error from a read may go away when the read is
// tried again. That is a failed call to the API server, other than one that
// found nothing, and a request that never reached it. A missing object, a kind
// the cluster doesn't serve, and an error the caller built from what it read
// are not retryable.
//
// It sorts the errors of a function that doesn't return refusals, such as
// bootstrap.ResolveLocation, which wraps each failed read and describes a
// missing field in an error of its own.
func retryable(err error) bool {
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
			return nil, refuse("claim %s has no VolumeRestore %s to name its repository", claim.Name, name)
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
		return nil, refuse("claim %s is not bound to a volume yet", claim.Name)
	}
	pv := &corev1.PersistentVolume{}
	if err := c.Get(ctx, types.NamespacedName{Name: claim.Spec.VolumeName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, refuse("claim %s is bound to the PersistentVolume %s, which does not exist", claim.Name, claim.Spec.VolumeName)
		}
		return nil, fmt.Errorf("get PersistentVolume %s: %w", claim.Spec.VolumeName, err)
	}
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return nil, refuse("the PersistentVolume %s declares no node affinity to place the mover by", pv.Name)
	}
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: pv.Spec.NodeAffinity.Required.DeepCopy(),
		},
	}, nil
}

// resticSpan matches a span in the form restic's --keep-within takes: one or
// more counts of years, months, days or hours, such as 30d or 1y6m.
var resticSpan = regexp.MustCompile(`^([0-9]+[ymdh])+$`)

// retention builds the restic retention policy for a claim's
// ReplicationSource from the claim's annotations: backup.wlz.li/retain-last,
// retain-hourly, retain-daily, retain-weekly, retain-monthly, retain-yearly
// and retain-within.
//
// It returns a refusal naming the claim and the annotation when a count isn't
// a positive integer, or when retain-within isn't a span such as 30d. It also
// returns a refusal when the claim sets none of them, because a policy with
// no rule would make restic keep every snapshot.
func retention(claim *corev1.PersistentVolumeClaim) (*volsyncv1alpha1.ResticRetainPolicy, error) {
	policy := &volsyncv1alpha1.ResticRetainPolicy{}
	counts := []struct {
		annotation string
		field      **int32
	}{
		{backupv1alpha1.AnnotationRetainHourly, &policy.Hourly},
		{backupv1alpha1.AnnotationRetainDaily, &policy.Daily},
		{backupv1alpha1.AnnotationRetainWeekly, &policy.Weekly},
		{backupv1alpha1.AnnotationRetainMonthly, &policy.Monthly},
		{backupv1alpha1.AnnotationRetainYearly, &policy.Yearly},
	}
	set := false

	// VolSync's CRD types the last count as a string and the other counts as
	// integers, so retain-last is parsed only to check it and is stored as
	// the annotation wrote it.
	if value, ok := claim.Annotations[backupv1alpha1.AnnotationRetainLast]; ok {
		if _, err := positiveCount(value); err != nil {
			return nil, refuse("claim %s has %s %q, which is not a positive count", claim.Name, backupv1alpha1.AnnotationRetainLast, value)
		}
		policy.Last = &value
		set = true
	}
	for _, c := range counts {
		value, ok := claim.Annotations[c.annotation]
		if !ok {
			continue
		}
		n, err := positiveCount(value)
		if err != nil {
			return nil, refuse("claim %s has %s %q, which is not a positive count", claim.Name, c.annotation, value)
		}
		*c.field = &n
		set = true
	}
	if value, ok := claim.Annotations[backupv1alpha1.AnnotationRetainWithin]; ok {
		if !resticSpan.MatchString(value) {
			return nil, refuse("claim %s has %s %q, which is not a span such as 30d or 1y6m", claim.Name, backupv1alpha1.AnnotationRetainWithin, value)
		}
		policy.Within = &value
		set = true
	}

	if !set {
		return nil, refuse("claim %s names no retention; set at least one of %s, %s, %s, %s, %s, %s or %s",
			claim.Name, backupv1alpha1.AnnotationRetainLast, backupv1alpha1.AnnotationRetainHourly,
			backupv1alpha1.AnnotationRetainDaily, backupv1alpha1.AnnotationRetainWeekly,
			backupv1alpha1.AnnotationRetainMonthly, backupv1alpha1.AnnotationRetainYearly,
			backupv1alpha1.AnnotationRetainWithin)
	}
	return policy, nil
}

// positiveCount parses a retention count from an annotation value. It returns
// an error unless the value is an integer of at least 1.
func positiveCount(value string) (int32, error) {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("not a positive count")
	}
	return int32(n), nil
}

// inUse reports whether VolSync may still be working on a source: it holds a
// manual tag VolSync hasn't completed (see busy), or VolSync has recorded the
// start of a sync in status.lastSyncStartTime and not yet cleared it. The
// controller writes nothing onto such a source unless the tag is its own, and
// then it doesn't write either.
func inUse(source *volsyncv1alpha1.ReplicationSource) bool {
	return busy(source) || (source.Status != nil && source.Status.LastSyncStartTime != nil)
}

// errSourceAbandoned is the error ensureSource returns, wrapped in a
// sourceHeld, when the claim's ReplicationSource is busy with a tag that no
// run waits for any more. The item fails at once: waiting would not end, and
// writing a new tag would have VolSync complete it with the older sync.
var errSourceAbandoned = errors.New("the ReplicationSource is still retrying a backup no run waits for")

// sourceHeld is the error that says why a run may not write the claim's
// ReplicationSource: another run waits for its tag (it matches errSourceBusy),
// or no run does (it matches errSourceAbandoned). Its message is written for
// the run's Ready condition or the item's message.
type sourceHeld struct {
	abandoned bool
	message   string
}

// Error returns the message, which names the source and the run or tag that
// holds it.
func (e *sourceHeld) Error() string { return e.message }

// Is makes errors.Is match errSourceBusy for a source a live run holds, and
// errSourceAbandoned for one no run waits for.
func (e *sourceHeld) Is(target error) bool {
	if e.abandoned {
		return target == errSourceAbandoned
	}
	return target == errSourceBusy
}

// holder returns why the run with the trigger tag may not write the
// ReplicationSource source, which is in use (see inUse) with another tag or
// with none.
//
// Parameters:
//   - reader lists the BackupRuns in the source's namespace. The caller
//     passes the uncached Reader: a run is created before it tags a source,
//     and a cache could lag behind that.
//   - source is the source as stored.
//
// A source whose open tag (see busy) belongs to a live run, and a source
// VolSync syncs with no open tag, give a sourceHeld that matches
// errSourceBusy; the run waits. A source whose open tag no live run holds
// gives one that matches errSourceAbandoned; the item fails. A tag is live
// when the namespace holds a BackupRun whose UID is the tag without its
// "backuprun-" prefix, which is not being deleted and not finished, and whose
// item for the claim is Pending or Running. Pending counts, because a run
// that wrote its tag and then lost its status write still has the item
// Pending. The tag's format is the same since v0.7.x, so tags written by
// those versions are judged the same way. A failed list comes back as a plain
// error, and the caller retries; no decision is made on it.
func holder(ctx context.Context, reader client.Reader, source *volsyncv1alpha1.ReplicationSource) error {
	if !busy(source) {
		return &sourceHeld{message: fmt.Sprintf("ReplicationSource %s is still syncing; VolSync has not recorded the end of its last sync", source.Name)}
	}
	open := manualTag(source)
	runs := &backupv1alpha1.BackupRunList{}
	if err := reader.List(ctx, runs, client.InNamespace(source.Namespace)); err != nil {
		return fmt.Errorf("list BackupRuns in %s: %w", source.Namespace, err)
	}
	owner := ""
	for i := range runs.Items {
		run := &runs.Items[i]
		if TriggerFor(run.UID) != open {
			continue
		}
		owner = run.Name
		if run.DeletionTimestamp != nil || run.Status.Phase.Finished() {
			break
		}
		for _, item := range run.Status.Items {
			if item.Kind == "ReplicationSource" && item.Name == source.Name &&
				(item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning) {
				return &sourceHeld{message: fmt.Sprintf("ReplicationSource %s is still completing the backup of BackupRun %s", source.Name, run.Name)}
			}
		}
		break
	}
	return &sourceHeld{abandoned: true, message: abandonedMessage(source, owner)}
}

// abandonedMessage returns the item's message for a source busy with a tag
// no run waits for. It says what is going on and what a person can do. owner
// is the name of the BackupRun the tag belongs to, or empty when no such run
// exists; the message then names the tag.
func abandonedMessage(source *volsyncv1alpha1.ReplicationSource, owner string) string {
	of := "the trigger " + manualTag(source)
	if owner != "" {
		of = "BackupRun " + owner
	}
	started := "VolSync has not started that sync yet"
	if source.Status != nil && source.Status.LastSyncStartTime != nil {
		started = fmt.Sprintf("VolSync started it at %s and retries it with the clone it cut then until a mover succeeds",
			source.Status.LastSyncStartTime.UTC().Format(time.RFC3339))
	}
	mover := ""
	if source.Status != nil && source.Status.LatestMoverStatus != nil {
		mover = fmt.Sprintf(" The last mover result VolSync recorded is %s: %s.",
			source.Status.LatestMoverStatus.Result, lastLines(source.Status.LatestMoverStatus.Logs, 5))
	}
	return fmt.Sprintf("ReplicationSource %[1]s is still retrying the backup of %[2]s, which no run waits for any more. "+
		"%[3]s; a new trigger would be completed by that older backup, so this run leaves the source alone.%[4]s "+
		"Fix what the mover reports and VolSync finishes on its own. "+
		"To give that backup up, delete the ReplicationSource %[1]s while no pod of Job volsync-src-%[1]s is running; "+
		"the next backup unlocks the repository first.",
		source.Name, of, started, mover)
}

// lastLines returns the last n non-empty lines of logs, joined by " / ".
func lastLines(logs string, n int) string {
	var lines []string
	for _, line := range strings.Split(logs, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " / ")
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
// It returns the ReplicationSource as it stands after the call. It writes
// nothing and returns the source as read in these cases:
//   - The source already carries tag, whether VolSync is still syncing it or
//     has completed it. A lost status write after the first write gets here.
//   - The source is in use (see inUse) with another tag, or with none. It
//     then also returns the sourceHeld from holder: one that matches
//     errSourceBusy while a live run waits for that tag, or one that matches
//     errSourceAbandoned when no run does.
//   - Another live run holds the Lease of the claim or its repository (see
//     acquireLeases), or a live RestoreRun's mover works on either (see
//     otherMover). It then also returns a sourceHeld that matches
//     errSourceBusy and names that run; the run waits.
//
// It returns a refusal, and writes nothing, when a ReplicationSource of the
// same name exists without the label app.kubernetes.io/managed-by:
// backup-controller, or when one of the settings below is missing or doesn't
// parse. It also returns a refusal when the API server rejects the source as
// invalid. Any other failed read or write comes back as a plain error, which
// the caller retries. That includes a Conflict or AlreadyExists when another
// writer changed or created the source after the read the write is based on.
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
func ensureSource(ctx context.Context, c client.Client, reader client.Reader, claim *corev1.PersistentVolumeClaim, tag string, lock leaseHolder) (*volsyncv1alpha1.ReplicationSource, error) {
	vr, err := volumeRestoreFor(ctx, reader, claim)
	if err != nil {
		return nil, err
	}
	affinity, err := volumeAffinity(ctx, reader, claim)
	if err != nil {
		return nil, err
	}
	retain, err := retention(claim)
	if err != nil {
		return nil, err
	}
	pruneInterval, err := pruneIntervalFor(ctx, reader, claim.Namespace)
	if err != nil {
		return nil, err
	}

	source := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: claim.Namespace},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, source, func() error {
		// CreateOrUpdate hands a source it didn't find over empty, with no
		// resourceVersion. Anything else is the stored source the update
		// will be based on.
		if source.ResourceVersion != "" {
			switch {
			case source.Labels[backupv1alpha1.LabelManagedBy] != backupv1alpha1.ManagedByValue:
				return refuse("the ReplicationSource %s exists and was not written by backup-controller; remove it so the controller can write its own", claim.Name)
			case manualTag(source) == tag:
				// Unchanged, so CreateOrUpdate writes nothing.
				return nil
			case inUse(source):
				return holder(ctx, reader, source)
			}
		}
		// The Leases make the backup and a restore of the claim or its
		// repository exclusive: of two runs that get here in the same
		// instant, only one creates each Lease.
		busy, err := acquireLeases(ctx, c, reader, lock, claim.Namespace, claim.Name, vr.Spec.Repository)
		if err != nil {
			return err
		}
		if busy != "" {
			return &sourceHeld{message: busy}
		}
		// A restore of the claim or its repository that already has its
		// ReplicationDestination goes first, as one started before the
		// controller took Leases does. The check runs here, right before
		// the write, so a restore that created its destination after an
		// earlier check is still seen.
		restoring, err := otherMover(ctx, reader, claim.Namespace, claim.Name, vr.Spec.Repository, restoreMover)
		if err != nil {
			return err
		}
		if restoring != "" {
			return &sourceHeld{message: restoring}
		}
		if source.Labels == nil {
			source.Labels = map[string]string{}
		}
		source.Labels[backupv1alpha1.LabelManagedBy] = backupv1alpha1.ManagedByValue
		if err := controllerutil.SetControllerReference(claim, source, c.Scheme()); err != nil {
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
				PruneIntervalDays:     &pruneInterval,
				Retain:                retain,
				CacheCapacity:         vr.Spec.CacheCapacity,
				CacheStorageClassName: vr.Spec.CacheStorageClassName,
				MoverConfig: volsyncv1alpha1.MoverConfig{
					MoverSecurityContext: vr.Spec.MoverSecurityContext,
					MoverAffinity:        affinity,
					MoverResources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: moverCPU},
					},
				},
			},
		}
		return nil
	})
	switch {
	case errors.Is(err, errSourceBusy), errors.Is(err, errSourceAbandoned):
		return source, err
	case isRefusal(err):
		return nil, err
	case apierrors.IsInvalid(err):
		return nil, refuse("the API server refused ReplicationSource %s/%s: %v", claim.Namespace, claim.Name, err)
	case err != nil:
		return nil, fmt.Errorf("write ReplicationSource %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	return source, nil
}
