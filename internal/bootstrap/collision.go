package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/walzen-group/backup-controller/internal/served"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// archiveHolder finds an existing Cluster that already archives to the same
// bucket and prefix as the Cluster being admitted, whatever endpointURL either
// names. Two databases archiving to one prefix interleave their WAL and leave
// the archive unrestorable.
//
// Parameters:
//   - c lists the Clusters and the ObjectStores.
//   - mapper looks up the versions at which the API server serves them.
//     Both lists go out at those versions (see served.List).
//   - namespace and name identify the Cluster being admitted. A Cluster with
//     the same namespace and name is skipped, so a recreate of the same
//     database doesn't collide with its own record.
//   - at is where the admitted Cluster would archive, from ResolveLocation.
//
// It returns the holder as "namespace/name", or an empty string when no
// Cluster archives there. It returns an error when the Cluster list or the
// ObjectStore list fails for any reason, such as a server timeout. Another
// Cluster may archive to the same prefix, so the caller refuses the create
// instead of missing a collision.
//
// It lists every Cluster in every namespace and, once there is an archiving
// one, every ObjectStore in every namespace. Each archiving Cluster's store is
// looked up by namespace and name in that list, and the two locations are
// compared with sameArchive: bucket and prefix, never the endpoint, since two
// endpoint names can reach one service. Two Clusters can reach one prefix
// through differently named ObjectStores, so comparing store names would miss
// them. The check reads no Secret. Where a Cluster archives is written in its
// ObjectStore, so a holder whose credentials are missing is still found. The
// whole check costs two list calls however many databases the cluster holds,
// which keeps a disaster-recovery recreate of every Cluster at once inside
// the webhook's budget. A Cluster whose ObjectStore is not in the list or
// names no s3:// destination archives nowhere, so it is skipped.
func archiveHolder(
	ctx context.Context,
	c client.Reader,
	mapper meta.RESTMapper,
	namespace, name string,
	at Location,
) (string, error) {
	clusters, err := served.List(ctx, c, mapper, clusterKind)
	if err != nil {
		return "", fmt.Errorf("list the Clusters: %w", err)
	}

	// The ObjectStores are listed once, on the first archiving Cluster, so a
	// cluster with no other archiving Cluster lists none.
	var stores map[types.NamespacedName]*unstructured.Unstructured
	for i := range clusters.Items {
		other := &clusters.Items[i]
		if other.GetNamespace() == namespace && other.GetName() == name {
			continue
		}
		store, serverName, found := Archiver(other)
		if !found {
			continue
		}
		if stores == nil {
			var err error
			if stores, err = objectStores(ctx, c, mapper); err != nil {
				return "", err
			}
		}
		theirStore, ok := stores[types.NamespacedName{Namespace: other.GetNamespace(), Name: store}]
		if !ok {
			// Not in the list is what NotFound means for a single read.
			continue
		}
		theirs, err := storeLocation(theirStore, serverName)
		var noDestination *destinationError
		if errors.As(err, &noDestination) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("find where %s/%s archives: %w", other.GetNamespace(), other.GetName(), err)
		}
		if theirs.sameArchive(at) {
			return fmt.Sprintf("%s/%s", other.GetNamespace(), other.GetName()), nil
		}
	}
	return "", nil
}

// objectStores lists every Barman Cloud ObjectStore in every namespace, for
// the collision check.
//
// It returns the stores keyed by namespace and name. The map is never nil, so
// the caller can tell a list made from one not made yet. It returns an error
// when the list fails for any reason, NotFound included: without the list the
// check can't know where the other Clusters archive, so the caller refuses.
// It lists at the version mapper looks up (see served.List); a list at a
// version the API server has stopped serving fails the same way.
func objectStores(ctx context.Context, c client.Reader, mapper meta.RESTMapper) (map[types.NamespacedName]*unstructured.Unstructured, error) {
	list, err := served.List(ctx, c, mapper, objectStoreKind)
	if err != nil {
		return nil, fmt.Errorf("list the ObjectStores: %w", err)
	}
	stores := make(map[types.NamespacedName]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		store := &list.Items[i]
		stores[types.NamespacedName{Namespace: store.GetNamespace(), Name: store.GetName()}] = store
	}
	return stores, nil
}
