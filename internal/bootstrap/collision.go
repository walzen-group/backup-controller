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
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
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

	stores := &storeIndex{c: c, mapper: mapper}
	for i := range clusters.Items {
		other := &clusters.Items[i]
		if other.GetNamespace() == namespace && other.GetName() == name {
			continue
		}
		store, serverName, found := Archiver(other)
		if !found {
			continue
		}
		theirStore, err := stores.get(ctx, types.NamespacedName{Namespace: other.GetNamespace(), Name: store})
		if err != nil {
			return "", err
		}
		holds, err := archivesAt(other, theirStore, serverName, at)
		if err != nil {
			return "", err
		}
		if holds {
			return fmt.Sprintf("%s/%s", other.GetNamespace(), other.GetName()), nil
		}
	}
	return "", nil
}

// storeIndex gives archiveHolder the ObjectStores by namespace and name. It
// lists the ObjectStores once, on the first archiving Cluster, so a cluster
// with no other archiving Cluster lists none.
type storeIndex struct {
	// c lists the ObjectStores.
	c client.Reader
	// mapper looks up the version at which the API server serves them.
	mapper meta.RESTMapper
	// stores holds the list from objectStores. It is nil until the first get.
	stores map[types.NamespacedName]*unstructured.Unstructured
}

// get returns the ObjectStore with the given key, or nil when the list does
// not hold it. It returns the error of objectStores when the list fails.
func (x *storeIndex) get(ctx context.Context, key types.NamespacedName) (*unstructured.Unstructured, error) {
	if x.stores == nil {
		stores, err := objectStores(ctx, x.c, x.mapper)
		if err != nil {
			return nil, err
		}
		x.stores = stores
	}
	return x.stores[key], nil
}

// archivesAt reports whether another Cluster archives to a Location, for
// archiveHolder.
//
// Parameters:
//   - other is the other Cluster, which the error names.
//   - store is the ObjectStore that other names, from the list of
//     objectStores, or nil when the list does not hold it.
//   - serverName is the server name of other, as Archiver returns it.
//   - at is where the admitted Cluster would archive.
//
// It returns true when other archives to the bucket and prefix of at (see
// sameArchive). It returns false when store is nil or names no s3://
// destination, since other then archives nowhere. It returns an error when
// the destination of store cannot be read.
func archivesAt(other, store *unstructured.Unstructured, serverName string, at Location) (bool, error) {
	if store == nil {
		// Not in the list is what NotFound means for a single read.
		return false, nil
	}
	theirs, err := storeLocation(store, serverName)
	var noDestination *destinationError
	if errors.As(err, &noDestination) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("find where %s/%s archives: %w", other.GetNamespace(), other.GetName(), err)
	}
	return theirs.sameArchive(at), nil
}

// sharedArchive refuses the create of a Cluster whose archive another
// Cluster already holds (see archiveHolder).
//
// Parameters:
//   - mapper is the RESTMapper that the create looks versions up with.
//   - c is the create. Its at field is where the Cluster would archive.
//
// It returns the refusal and true when another Cluster archives there, or
// when the lists fail. It returns false when the create goes on.
//
// Two databases that archive to one prefix interleave their WAL and leave the
// archive unrestorable, which is silent and permanent. Nothing else on the
// cluster can see this problem before it occurs: each Cluster is valid on its
// own, and the pair is the problem. Where a Cluster archives does not depend
// on how it bootstraps, so a Cluster that declares its own bootstrap is
// checked too.
func (d *Decider) sharedArchive(ctx context.Context, mapper meta.RESTMapper, c creation) (admission.Response, bool) {
	holder, err := archiveHolder(ctx, d.Client, mapper, c.req.Namespace, c.req.Name, c.at)
	if err != nil {
		return readFailed(ctx, c.logger, c.budget, "listing the Clusters and their ObjectStores", err), true
	}
	if holder == "" {
		return admission.Response{}, false
	}
	c.logger.Info("refusing the Cluster", "reason", "another database archives here", "holder", holder, "prefix", c.at.Prefix)
	return admission.Denied(fmt.Sprintf(
		"%s already archives to %s/%s. Two databases writing one archive interleave their WAL and leave it unrestorable. Give this Cluster an archive of its own, or a serverName that is not %q. The check compares bucket and prefix whatever the endpointURL says, because two endpoints can name one service; if %s really archives to a different S3 service, give one of the two its own prefix in destinationPath.",
		holder, c.at.Bucket, c.at.Prefix, c.serverName, holder,
	)), true
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
