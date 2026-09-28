// Package lease keeps two runs of the controller from acting on the same
// claim, restic repository, Cluster or set of paused workloads at the same
// time.
//
// Each guarded object has a coordination.k8s.io/v1 Lease in its namespace.
// The Lease's spec.holderIdentity names the run that may act on the object.
// A run takes the Lease before it acts and deletes it when it is done. A
// Lease whose holder no longer needs it, because the run is gone or has
// finished, is taken over, so a crashed or deleted run never blocks the
// object for good.
//
// The API server decides every race: a create of a Lease that exists fails,
// and an update or delete that carries an old resourceVersion fails. So of two
// runs that try to take one Lease at the same moment, exactly one holds it.
package lease

import (
	"context"
	"fmt"
	"strings"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Prefixes of the Lease names. A Lease name is the prefix followed by the
// name of the guarded object, such as backup-claim-data.
const (
	// ClaimPrefix guards the data of a claim: a backup, a restore or the
	// populator writes or reads it.
	ClaimPrefix = "backup-claim-"
	// RepositoryPrefix guards a restic repository, named by its Secret: a
	// mover, a restore Job or a retime reads or writes it.
	RepositoryPrefix = "backup-repository-"
	// ClusterPrefix guards a CloudNativePG Cluster: a database backup or a
	// database restore acts on it.
	ClusterPrefix = "backup-cluster-"
	// PauseName guards the paused workloads of a namespace. Only one run at a
	// time pauses and resumes them.
	PauseName = "backup-pause"
)

// Holder identifies the run that holds a Lease.
type Holder struct {
	// Kind is the kind of the run, BackupRun or RestoreRun.
	Kind string
	// Name is the name of the run in the Lease's namespace.
	Name string
	// UID is the UID of the run. A new run with an old run's name has
	// another UID, so it never counts as the old holder.
	UID types.UID
}

// String returns the identity written into the Lease, "<Kind>/<Name>/<UID>".
func (h Holder) String() string {
	return h.Kind + "/" + h.Name + "/" + string(h.UID)
}

// ParseHolder reads an identity that Holder.String wrote.
//
// Parameters:
//   - identity is the spec.holderIdentity of a Lease.
//
// It returns the holder and true, or false when the identity doesn't have
// the three parts, for example when something other than this controller
// wrote the Lease.
func ParseHolder(identity string) (Holder, bool) {
	parts := strings.SplitN(identity, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return Holder{}, false
	}
	return Holder{Kind: parts[0], Name: parts[1], UID: types.UID(parts[2])}, true
}

// Liveness tells whether the holder of a Lease still needs it.
//
// Parameters:
//   - namespace is the namespace of the Lease and of the run.
//   - holder is the run that holds the Lease.
//
// It returns true while the run exists with the holder's UID and has not
// finished. It returns an error when the run can't be read; the caller then
// takes nothing over.
type Liveness func(ctx context.Context, namespace string, holder Holder) (bool, error)

// Leases takes and releases the Leases of the controller's runs.
type Leases struct {
	// Client creates, updates and deletes Leases.
	Client client.Client
	// Reader reads Leases from the API server, not from a cache, so a
	// decision never rests on a Lease the cache has not seen yet.
	Reader client.Reader
	// Alive tells whether the holder of a Lease still needs it.
	Alive Liveness
}

// Take takes a Lease for a run, or reports the run that holds it.
//
// Parameters:
//   - namespace is the namespace of the guarded object and of the run.
//   - name is the Lease's name: a prefix of this package followed by the
//     guarded object's name, or PauseName.
//   - holder is the run that wants to act on the object.
//
// It returns true when the run holds the Lease: it created it, it already
// held it, or it took it over from a holder that no longer needs it. It
// returns false and the other holder's identity when another run holds the
// Lease and still needs it, or when another run won a race for it in this
// call. It returns an error when an API call fails, or when a Lease of that
// name holds an identity this package did not write; such a Lease is never
// taken over, since something outside the controller made it.
func (l *Leases) Take(ctx context.Context, namespace, name string, holder Holder) (bool, string, error) {
	identity := holder.String()
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &identity},
	}
	err := l.Client.Create(ctx, lease)
	if err == nil {
		return true, "", nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return false, "", fmt.Errorf("create Lease %s/%s: %w", namespace, name, err)
	}

	current := &coordinationv1.Lease{}
	if err := l.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, current); err != nil {
		if apierrors.IsNotFound(err) {
			// The holder released it between the create and the read.
			// The next pass creates it again.
			return false, "", nil
		}
		return false, "", fmt.Errorf("get Lease %s/%s: %w", namespace, name, err)
	}
	other := ""
	if current.Spec.HolderIdentity != nil {
		other = *current.Spec.HolderIdentity
	}
	if other == identity {
		return true, "", nil
	}
	owner, ok := ParseHolder(other)
	if !ok {
		return false, other, fmt.Errorf("the Lease %s/%s is held by %q, which is not a run of this controller", namespace, name, other)
	}
	alive, err := l.Alive(ctx, namespace, owner)
	if err != nil {
		return false, other, fmt.Errorf("check the holder %s of Lease %s/%s: %w", other, namespace, name, err)
	}
	if alive {
		return false, other, nil
	}

	// The update carries the resourceVersion of the read, so it fails when
	// another run took the Lease over first.
	current.Spec.HolderIdentity = &identity
	if err := l.Client.Update(ctx, current); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return false, other, nil
		}
		return false, other, fmt.Errorf("take over Lease %s/%s from %s: %w", namespace, name, other, err)
	}
	return true, "", nil
}

// Release deletes a Lease that a run holds.
//
// Parameters:
//   - namespace and name identify the Lease, as Take got them.
//   - holder is the run that is done with the guarded object.
//
// It deletes the Lease only while it still names the holder, with the
// resourceVersion of that read as a precondition, so it never deletes a
// Lease that another run took over in the meantime. A Lease that is gone or
// names another holder is not an error. It returns an error when the read or
// the delete fails.
func (l *Leases) Release(ctx context.Context, namespace, name string, holder Holder) error {
	current := &coordinationv1.Lease{}
	if err := l.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, current); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get Lease %s/%s: %w", namespace, name, err)
	}
	if current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != holder.String() {
		return nil
	}
	precondition := client.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion}
	if err := l.Client.Delete(ctx, current, precondition); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return fmt.Errorf("delete Lease %s/%s: %w", namespace, name, err)
	}
	return nil
}
