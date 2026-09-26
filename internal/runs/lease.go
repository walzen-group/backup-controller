package runs

import (
	"context"
	"fmt"
	"slices"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// labelLeaseHolderKind names the kind of run that holds a Lease,
	// BackupRun or RestoreRun.
	labelLeaseHolderKind = "backup.wlz.li/lease-holder-kind"
	// labelLeaseHolderUID holds the UID of the run that holds a Lease, so a
	// run can list its own Leases to release them.
	labelLeaseHolderUID = "backup.wlz.li/lease-holder-uid"
	// annotationLeaseHolderName names the run that holds a Lease. It is an
	// annotation because a run's name can be longer than a label value.
	annotationLeaseHolderName = "backup.wlz.li/lease-holder-name"
	// annotationLeaseItems lists, separated by commas, the names of the
	// holder run's items the Lease is held for.
	annotationLeaseItems = "backup.wlz.li/lease-items"
	// labelLeaseScope marks a Lease that guards a namespace's quiesce (see
	// scopeQuiesce). Claim and repository Leases carry no scope label.
	labelLeaseScope = "backup.wlz.li/lease-scope"
	// scopeQuiesce is the value of labelLeaseScope on a quiesce Lease.
	scopeQuiesce = "quiesce"
	// quiesceLeaseName is the Lease one run at a time takes in a namespace
	// before it stops that namespace's workloads.
	quiesceLeaseName = "backup-controller-quiesce"
)

// leaseHolder is a run's item that takes the Leases of a claim and its
// repository before it creates its mover object.
type leaseHolder struct {
	// kind is BackupRun or RestoreRun.
	kind string
	// run is the run's namespace, name and UID.
	run metav1.Object
	// item is the name of the item in the run's status the Leases are held
	// for.
	item string
	// scope is scopeQuiesce for the Lease that guards a namespace's quiesce
	// (see acquireQuiesceLease), and "" for a claim or repository Lease.
	scope string
}

// claimLeaseName returns the name of the Lease that guards the claim with the
// given UID.
func claimLeaseName(uid types.UID) string { return "backup-controller-claim-" + string(uid) }

// repositoryLeaseName returns the name of the Lease that guards the restic
// repository whose Secret has the given UID.
func repositoryLeaseName(uid types.UID) string { return "backup-controller-repo-" + string(uid) }

// acquireLeases takes the Leases that let only one run at a time start a
// mover on a claim and on its restic repository.
//
// Parameters:
//   - c creates and updates the Leases. The reads go through the reader.
//   - reader reads the claim, the Secret, the Leases and the holder run,
//     uncached, so a Lease another run took a moment ago is seen.
//   - holder is the run and the item that take the Leases.
//   - namespace is the run's namespace, where the Leases live.
//   - claim is the name of the claim. With an empty name the run takes no
//     claim Lease, as for a restore into a claim that does not exist yet.
//   - secret is the name of the repository Secret. With an empty name the
//     run takes no repository Lease.
//
// It returns "" once the run holds every Lease, and otherwise a message for
// the Ready condition that names the run that holds one; the run then waits
// with reason SourceBusy and tries again on a later pass. A repository Secret
// that does not exist comes back as a refusal (see leaseNamesFor), and the
// caller fails the item with nothing started. Any other failed API call comes
// back as an error, and the caller retries.
//
// A backup and a restore of the same claim or repository call it right
// before they create their mover object. otherMover's look at the objects
// themselves cannot be atomic with that create, and two runs that passed it
// in the same instant would both start a mover. Creating a Lease is atomic:
// of two runs that create the same one, the API server lets exactly one
// succeed. The run takes no Lease for a claim that does not exist, since
// nothing can back that claim up; such a run goes as far as startItem or
// restoreVolume, which fails its item with a message naming the claim.
//
// The claim Lease is taken before the repository Lease, always. A run that
// holds a repository Lease therefore already holds its claim Lease, and never
// waits for a Lease again; a run that waits holds at most a claim Lease
// another run is not waiting for. No two runs wait for each other.
//
// A Lease another run holds is taken over when that run is stale: it no
// longer exists (or its name now belongs to a run with another UID), it has
// finished, or none of the items the Lease names is Pending or Running any
// more. The takeover is an update carrying the resourceVersion that was read,
// so of two runs taking over the same Lease one gets a Conflict and waits. A
// live holder's Lease is never taken over, and a run being deleted counts as
// live until its finalizer has released its Leases.
func acquireLeases(ctx context.Context, c client.Client, reader client.Reader, holder leaseHolder, namespace, claim, secret string) (string, error) {
	names, err := leaseNamesFor(ctx, reader, namespace, claim, secret)
	if err != nil {
		return "", err
	}
	for _, name := range names {
		busy, err := acquireLease(ctx, c, reader, holder, namespace, name)
		if err != nil || busy != "" {
			return busy, err
		}
	}
	return "", nil
}

// leaseNamesFor returns the names of the Leases a run takes on a claim and on
// the restic repository whose Secret has the given name, in the order
// acquireLeases takes them: the claim's first.
//
// Parameters:
//   - reader reads the claim and the Secret. Callers pass the uncached
//     Reader, and acquireLeases and leaseHeldElsewhere share this function,
//     so a pre-check looks at exactly the Leases the run would take later.
//   - namespace is the run's namespace, which holds both objects.
//   - claim is the name of the claim. An empty name adds no claim Lease.
//   - secret is the name of the repository Secret. An empty name adds no
//     repository Lease.
//
// It returns the Lease names, and an error when a read fails or the Secret
// is missing. A claim that reads NotFound gets no Lease, because nothing can
// write to it. A repository Secret that reads NotFound comes back as a
// refusal (see isRefusal): a mover started without the repository Lease
// would be unguarded once the Secret appears, so the caller fails the item
// with nothing started. Any other failed read comes back as an error.
func leaseNamesFor(ctx context.Context, reader client.Reader, namespace, claim, secret string) ([]string, error) {
	var names []string
	if claim != "" {
		pvc := &corev1.PersistentVolumeClaim{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, pvc)
		switch {
		case err == nil:
			names = append(names, claimLeaseName(pvc.UID))
		case !apierrors.IsNotFound(err):
			return nil, fmt.Errorf("get claim %s/%s: %w", namespace, claim, err)
		}
	}
	if secret != "" {
		s := &corev1.Secret{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secret}, s)
		switch {
		case err == nil:
			names = append(names, repositoryLeaseName(s.UID))
		case apierrors.IsNotFound(err):
			return nil, refuse("repository Secret %s does not exist in this namespace, so the run can't take the Lease that keeps other runs' movers off the repository",
				secret)
		default:
			return nil, fmt.Errorf("get repository Secret %s/%s: %w", namespace, secret, err)
		}
	}
	return names, nil
}

// acquireLease takes one Lease for a run, as acquireLeases describes.
//
// Parameters:
//   - holder is the run and the item that take the Lease.
//   - namespace and name place and name the Lease.
//
// It returns "" once the run holds the Lease, a message naming the holder
// when another live run holds it (see leaseBusyMessage), and an error when
// an API call fails.
func acquireLease(ctx context.Context, c client.Client, reader client.Reader, holder leaseHolder, namespace, name string) (string, error) {
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	stamp(lease, holder, []string{holder.item})
	err := c.Create(ctx, lease)
	if err == nil {
		return "", nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create Lease %s/%s: %w", namespace, name, err)
	}

	held := &coordinationv1.Lease{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, held); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("Lease %s was released while this run took it; this run tries again", name), nil
		}
		return "", fmt.Errorf("get Lease %s/%s: %w", namespace, name, err)
	}
	items := leaseItems(held)
	if holderUID(held) == string(holder.run.GetUID()) {
		if holder.scope == scopeQuiesce {
			// A quiesce Lease is held for the run, not for one of its items,
			// so the run that holds it keeps it without a write.
			return "", nil
		}
		if slices.Contains(items, holder.item) {
			return "", nil
		}
		// The same run takes the Lease for another item, as a namespace
		// run does for two claims that share a repository.
		stamp(held, holder, append(items, holder.item))
		return updateLease(ctx, c, held)
	}

	live, err := holderLive(ctx, reader, held)
	if err != nil {
		return "", err
	}
	if live {
		return leaseBusyMessage(held), nil
	}
	stamp(held, holder, []string{holder.item})
	return updateLease(ctx, c, held)
}

// leaseBusyMessage returns the Ready message for a run that waits while
// another live run holds a Lease it needs (see acquireLease).
//
// Parameters:
//   - lease is the Lease as stored, whose labels and annotations name the
//     holder.
//
// The message names the holder, and for a quiesce Lease also the workloads
// the holder stopped.
func leaseBusyMessage(lease *coordinationv1.Lease) string {
	kind := lease.Labels[labelLeaseHolderKind]
	name := lease.Annotations[annotationLeaseHolderName]
	if lease.Labels[labelLeaseScope] == scopeQuiesce {
		return fmt.Sprintf("%s %s has stopped the workloads of this namespace (Lease %s); this run stops them once that run has given them back",
			kind, name, lease.Name)
	}
	return fmt.Sprintf("%s %s holds Lease %s for %s; this run starts once that run has finished with it",
		kind, name, lease.Name, strings.Join(leaseItems(lease), ", "))
}

// updateLease writes a Lease this run took over or extended, based on the
// resourceVersion it was read at. A Conflict means another run wrote it
// first, and comes back as a message to wait with.
func updateLease(ctx context.Context, c client.Client, lease *coordinationv1.Lease) (string, error) {
	if err := c.Update(ctx, lease); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return fmt.Sprintf("Lease %s changed while this run took it; this run tries again", lease.Name), nil
		}
		return "", fmt.Errorf("update Lease %s/%s: %w", lease.Namespace, lease.Name, err)
	}
	return "", nil
}

// stamp writes holder and the items onto the Lease: spec.holderIdentity and
// the holder label carry the run's UID, and the kind label and the name
// annotation name the run for the message another run waits with.
func stamp(lease *coordinationv1.Lease, holder leaseHolder, items []string) {
	uid := string(holder.run.GetUID())
	lease.Spec.HolderIdentity = &uid
	if lease.Labels == nil {
		lease.Labels = map[string]string{}
	}
	lease.Labels[backupv1alpha1.LabelManagedBy] = backupv1alpha1.ManagedByValue
	lease.Labels[labelLeaseHolderKind] = holder.kind
	lease.Labels[labelLeaseHolderUID] = uid
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[annotationLeaseHolderName] = holder.run.GetName()
	lease.Annotations[annotationLeaseItems] = strings.Join(items, ",")
	if holder.scope != "" {
		lease.Labels[labelLeaseScope] = holder.scope
	}
}

// holderUID returns the UID of the run that holds the Lease, or "" when it
// names none.
func holderUID(lease *coordinationv1.Lease) string {
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

// leaseItems returns the names of the holder's items the Lease is held for.
func leaseItems(lease *coordinationv1.Lease) []string {
	value := lease.Annotations[annotationLeaseItems]
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

// backupLeaseItemKind and restoreLeaseItemKind are the kinds of the items
// that take Leases: a BackupRun's volume item and a RestoreRun's claim item.
// A Lease names its items by name alone, and a run's Cluster item can share a
// claim's name, so holderLive matches the kind as well.
const (
	backupLeaseItemKind  = "ReplicationSource"
	restoreLeaseItemKind = "PersistentVolumeClaim"
)

// holderLive reports whether the run that holds a Lease still needs it.
//
// Parameters:
//   - reader reads the holder run, uncached.
//   - lease is the Lease as stored. It lives in its holder's namespace.
//
// It returns true while the holder needs the Lease, and an error when the
// read of the holder fails.
//
// A claim or repository Lease is live while the run exists with the UID the
// Lease names, has not finished, and has an item the Lease names that is
// Pending or Running. Only an item of the kind that takes Leases counts (see
// backupLeaseItemKind), so a Cluster item with the claim's name does not
// keep the claim's Lease. A quiesce Lease is live while the holder's stored
// status does not show the workloads given back (see durablyRestarted): the
// holder exists with the UID the Lease names, has not finished, and has no
// plan yet or has not recorded its restart as done. A run being deleted
// counts as live until its finalizer has released the Lease. A Lease that
// names no run kind this controller knows is left alone, and counts as live.
func holderLive(ctx context.Context, reader client.Reader, lease *coordinationv1.Lease) (bool, error) {
	key := types.NamespacedName{Namespace: lease.Namespace, Name: lease.Annotations[annotationLeaseHolderName]}
	items := leaseItems(lease)
	quiesce := lease.Labels[labelLeaseScope] == scopeQuiesce
	switch lease.Labels[labelLeaseHolderKind] {
	case "BackupRun":
		run := &backupv1alpha1.BackupRun{}
		if err := reader.Get(ctx, key, run); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("get BackupRun %s: %w", key, err)
		}
		if string(run.UID) != holderUID(lease) || run.Status.Phase.Finished() {
			return false, nil
		}
		if quiesce {
			return !durablyRestarted(run), nil
		}
		for _, item := range run.Status.Items {
			if item.Kind == backupLeaseItemKind && slices.Contains(items, item.Name) &&
				(item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning) {
				return true, nil
			}
		}
		return false, nil
	case "RestoreRun":
		run := &backupv1alpha1.RestoreRun{}
		if err := reader.Get(ctx, key, run); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("get RestoreRun %s: %w", key, err)
		}
		if string(run.UID) != holderUID(lease) || run.Status.Phase.Finished() {
			return false, nil
		}
		if quiesce {
			return !durablyRestarted(run), nil
		}
		for _, item := range run.Status.Items {
			if item.Kind == restoreLeaseItemKind && slices.Contains(items, item.Name) && !finished(item) {
				return true, nil
			}
		}
		return false, nil
	}
	return true, nil
}

// releaseLeases deletes the claim and repository Leases a run holds in its
// namespace once every item each Lease names is done.
//
// Parameters:
//   - run is the BackupRun or RestoreRun whose Leases go, found by the UID
//     label.
//   - done tells whether the item with the given name is done. finish and
//     finalize pass a function that accepts every item; work passes one that
//     accepts the items that finished and whose movers are gone.
//
// It returns nil once every such Lease is gone or left alone, and an error
// when the list or a delete fails. A quiesce Lease is skipped: it goes only
// once the run has given the workloads back, which releaseQuiesceLeases
// waits for (see holderLive).
//
// The delete carries the UID and the resourceVersion of the Lease as read, so
// a Lease another run has taken over since is left alone. A Lease that is
// already gone, or changed since the read, is not an error: the next call
// sees it as it is now. Any other failed call comes back as an error, and the
// caller retries; a Lease left behind is taken over under acquireLeases'
// rule once its run has finished.
func releaseLeases(ctx context.Context, c client.Client, reader client.Reader, run metav1.Object, done func(item string) bool) error {
	leases := &coordinationv1.LeaseList{}
	if err := reader.List(ctx, leases, client.InNamespace(run.GetNamespace()),
		client.MatchingLabels{labelLeaseHolderUID: string(run.GetUID())}); err != nil {
		return fmt.Errorf("list the Leases of %s: %w", run.GetName(), err)
	}
	for i := range leases.Items {
		lease := &leases.Items[i]
		if lease.Labels[labelLeaseScope] != "" {
			continue
		}
		if holderUID(lease) != string(run.GetUID()) || !allDoneItems(leaseItems(lease), done) {
			continue
		}
		uid, version := lease.UID, lease.ResourceVersion
		err := c.Delete(ctx, lease, client.Preconditions{UID: &uid, ResourceVersion: &version})
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return fmt.Errorf("delete Lease %s/%s: %w", lease.Namespace, lease.Name, err)
		}
	}
	return nil
}

// leaseReleaseError returns the error for a failed release of the Leases a
// run holds (see releaseLeases).
//
// Parameters:
//   - run is the run whose Leases stayed. Its UID goes into the advice.
//   - err is the error from releaseLeases.
//
// It returns a *releaseError, which releaseFailure puts on the run's Ready
// condition. The error names the Lease release as the step that failed, and
// its advice tells a person which label finds the Leases to delete by hand.
func leaseReleaseError(run metav1.Object, err error) error {
	return &releaseError{
		action: "release the Leases it holds on its claims and repositories",
		advice: fmt.Sprintf("Fix the cause, or delete the Leases labelled %s=%s yourself; either way the run then finishes by itself.",
			labelLeaseHolderUID, run.GetUID()),
		err: err,
	}
}

// releaseQuiesceLeases deletes the quiesce Lease a run holds in its
// namespace.
//
// Parameters:
//   - run is the BackupRun or RestoreRun whose quiesce Lease goes, found by
//     the UID label.
//
// It returns nil once the Lease is gone or left alone, and an error when the
// list or the delete fails. A run calls it once its stored status shows the
// workloads back, so another run may take the Lease over.
//
// The delete carries the UID and the resourceVersion of the Lease as read, so
// a Lease another run has taken over since is left alone. A Lease that is
// already gone, or changed since the read, is not an error: the next call
// sees it as it is now. Any other failed call comes back as an error, and the
// caller logs it and goes on; a Lease left behind is stale under holderLive's
// rule and is taken over by the next run.
func releaseQuiesceLeases(ctx context.Context, c client.Client, reader client.Reader, run metav1.Object) error {
	leases := &coordinationv1.LeaseList{}
	if err := reader.List(ctx, leases, client.InNamespace(run.GetNamespace()), client.MatchingLabels{
		labelLeaseHolderUID: string(run.GetUID()),
		labelLeaseScope:     scopeQuiesce,
	}); err != nil {
		return fmt.Errorf("list the quiesce Leases of %s: %w", run.GetName(), err)
	}
	for i := range leases.Items {
		lease := &leases.Items[i]
		if holderUID(lease) != string(run.GetUID()) {
			continue
		}
		uid, version := lease.UID, lease.ResourceVersion
		err := c.Delete(ctx, lease, client.Preconditions{UID: &uid, ResourceVersion: &version})
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return fmt.Errorf("delete Lease %s/%s: %w", lease.Namespace, lease.Name, err)
		}
	}
	return nil
}

// leaseHeldElsewhere returns a message naming the run that holds the Lease of
// the given claim or restic repository, or "" when the asking run holds them,
// no Lease exists, or the holder is stale (see holderLive). It is what a
// pre-check calls before it stops anything: the run then waits with reason
// SourceBusy and keeps the app running.
//
// Parameters:
//   - reader reads the claim, the Secret and the Leases. Callers pass the
//     uncached Reader: a Lease a run takes in this instant must be seen.
//   - run is the asking run; only its UID is read, so its own Lease is free.
//   - namespace, claim and secret name the claim and the repository Secret,
//     as acquireLeases takes them.
//
// The Lease names come from leaseNamesFor, the same function acquireLeases
// resolves them with, so this check can never look at a Lease the run would
// not take. A repository Secret that does not exist comes back as the
// refusal leaseNamesFor gives; the caller leaves it to the item's start,
// which fails the item. Any other failed read comes back as an error, and
// the caller retries with nothing stopped.
func leaseHeldElsewhere(ctx context.Context, reader client.Reader, run metav1.Object, namespace, claim, secret string) (string, error) {
	names, err := leaseNamesFor(ctx, reader, namespace, claim, secret)
	if err != nil {
		return "", err
	}
	for _, name := range names {
		held := &coordinationv1.Lease{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, held)
		switch {
		case apierrors.IsNotFound(err):
			continue
		case err != nil:
			return "", fmt.Errorf("get Lease %s/%s: %w", namespace, name, err)
		}
		if holderUID(held) == string(run.GetUID()) {
			continue
		}
		live, err := holderLive(ctx, reader, held)
		if err != nil {
			return "", err
		}
		if live {
			return leaseBusyMessage(held), nil
		}
	}
	return "", nil
}

// allDoneItems reports whether done accepts every item.
func allDoneItems(items []string, done func(string) bool) bool {
	for _, item := range items {
		if !done(item) {
			return false
		}
	}
	return true
}
