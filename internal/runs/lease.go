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
}

// claimLeaseName returns the name of the Lease that guards the claim with the
// given UID.
func claimLeaseName(uid types.UID) string { return "backup-controller-claim-" + string(uid) }

// repositoryLeaseName returns the name of the Lease that guards the restic
// repository whose Secret has the given UID.
func repositoryLeaseName(uid types.UID) string { return "backup-controller-repo-" + string(uid) }

// acquireLeases takes the Leases that let one run at a time start a mover
// on a claim and on its restic repository. A backup and a restore of the
// same claim or repository call it right before they create their mover
// object; otherMover's check on the objects themselves can't be atomic with
// that create, and two runs that pass it in the same instant would both start
// a mover. Creating a Lease is atomic: of two runs that create the same one,
// the API server lets exactly one succeed.
//
// Parameters:
//   - c creates, updates and reads the Leases. Reads go through reader.
//   - reader reads the claim, the Secret and the holder run, uncached.
//   - holder is the run and item that takes the Leases.
//   - namespace is the run's namespace, where the Leases live.
//   - claim names the claim; empty takes no claim Lease, as for a restore
//     into a claim that does not exist yet. A claim that does not exist takes
//     no Lease either: nothing can back it up.
//   - secret names the repository Secret; empty takes no repository Lease.
//
// It returns "" once the run holds every Lease, and otherwise a message for
// the Ready condition that names the run that holds one. The run then waits
// with reason SourceBusy and tries again on a later pass. A Secret that does
// not exist, or any failed API call other than those below, comes back as an
// error, and the caller retries.
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
	var names []string
	if claim != "" {
		pvc := &corev1.PersistentVolumeClaim{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, pvc)
		switch {
		case err == nil:
			names = append(names, claimLeaseName(pvc.UID))
		case !apierrors.IsNotFound(err):
			return "", fmt.Errorf("get claim %s/%s: %w", namespace, claim, err)
		}
	}
	if secret != "" {
		s := &corev1.Secret{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secret}, s); err != nil {
			return "", fmt.Errorf("get repository Secret %s/%s: %w", namespace, secret, err)
		}
		names = append(names, repositoryLeaseName(s.UID))
	}
	for _, name := range names {
		busy, err := acquireLease(ctx, c, reader, holder, namespace, name)
		if err != nil || busy != "" {
			return busy, err
		}
	}
	return "", nil
}

// acquireLease takes one Lease for holder, as acquireLeases describes. It
// returns "" when holder holds it, a message naming the holder when another
// live run does, and an error for a failed API call.
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
		return fmt.Sprintf("%s %s holds Lease %s for %s; this run starts once that run has finished with it",
			held.Labels[labelLeaseHolderKind], held.Annotations[annotationLeaseHolderName], name, strings.Join(items, ", ")), nil
	}
	stamp(held, holder, []string{holder.item})
	return updateLease(ctx, c, held)
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

// holderLive reports whether the run that holds the Lease still needs it:
// the run exists with the UID the Lease names, has not finished, and has an
// item the Lease names that is Pending or Running. A Lease that names no run
// kind this controller knows is left alone, and counts as live. A failed
// read comes back as an error.
func holderLive(ctx context.Context, reader client.Reader, lease *coordinationv1.Lease) (bool, error) {
	key := types.NamespacedName{Namespace: lease.Namespace, Name: lease.Annotations[annotationLeaseHolderName]}
	items := leaseItems(lease)
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
		for _, item := range run.Status.Items {
			if slices.Contains(items, item.Name) &&
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
		for _, item := range run.Status.Items {
			if slices.Contains(items, item.Name) && !finished(item) {
				return true, nil
			}
		}
		return false, nil
	}
	return true, nil
}

// releaseLeases deletes the Leases the run holds in its namespace whose items
// have all finished by done's account. finish and finalize pass a done that
// accepts every item; work passes one that accepts the items that finished.
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

// allDoneItems reports whether done accepts every item.
func allDoneItems(items []string, done func(string) bool) bool {
	for _, item := range items {
		if !done(item) {
			return false
		}
	}
	return true
}
