package runs

import (
	"context"
	"fmt"
	"slices"
	"strings"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/served"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// volsyncSourceKind is the VolSync kind the controller reads and writes, at
// the one version its Go types come from: github.com/backube/volsync
// api/v1alpha1. VolSync has published no other version up to 0.16.0 and on
// its main branch (checked on 2026-09-26), so there is nothing the
// controller could fall back to, and it acts on no guess when v1alpha1 is
// gone.
var volsyncSourceKind = volsyncv1alpha1.GroupVersion.WithKind("ReplicationSource")

// webhookClusterKind is CloudNativePG's Cluster at the one version the
// bootstrap webhook's rules name (deploy/webhook.yaml and
// chart/templates/webhook.yaml, apiVersions ["v1"]). With the rules'
// matchPolicy Equivalent, the API server converts a creation at any other
// served version to v1 and calls the webhook, for as long as it serves v1.
var webhookClusterKind = cnpg.ClusterGVK

// UnservedError reports a kind the API server serves only at versions
// other than the one the controller uses.
type UnservedError struct {
	// Want is the kind at the version the controller uses.
	Want schema.GroupVersionKind
	// Served are the versions the API server serves the kind at, such as
	// postgresql.cnpg.io/v2.
	Served []string
}

// Error returns the sentence for a person that names the kind, the
// versions served and the version the controller uses, and says what
// follows. No decision reads it.
func (e *UnservedError) Error() string {
	if e.Want.GroupKind() == webhookClusterKind.GroupKind() {
		return fmt.Sprintf("the API server serves CloudNativePG's Cluster at %s and no longer at %s, the version the bootstrap "+
			"webhook's rules name, so it creates a Cluster without calling the webhook, and the Cluster starts as an empty database; "+
			"no restore deletes a Cluster until %s is served again or a backup-controller release that registers the new version is "+
			"installed (see docs/compatibility.md)",
			strings.Join(e.Served, ", "), e.Want.GroupVersion(), e.Want.GroupVersion())
	}
	return fmt.Sprintf("the API server serves VolSync's %s at %s and no longer at %s, the one version this backup-controller "+
		"reads and writes; no run creates, reads or deletes a VolSync object until %s is served again or a backup-controller "+
		"release that supports this VolSync version is installed (see docs/compatibility.md)",
		e.Want.Kind, strings.Join(e.Served, ", "), e.Want.GroupVersion(), e.Want.GroupVersion())
}

// unservedAt looks up whether the API server serves a kind at another
// version and no longer at the version the controller uses.
//
// Parameters:
//   - mapper is the client's RESTMapper. The manager's mapper answers from
//     its cache. A lookup that misses makes it read the group's discovery
//     again, and served.VersionGone makes it forget the group after the API
//     server answered a request at a version with a 404.
//   - want is the kind at the version the controller uses.
//
// It returns an *UnservedError that names the versions served, and nil
// when want's version is served, when no version of the kind is served (the
// project is not installed), and when the lookup fails, even for only some
// versions of the group. The callers act only on a real incompatibility,
// and a failed lookup is reported by the requests that follow.
//
// The kind is looked up by group and kind with no version, which makes
// controller-runtime's lazy mapper read the discovery of every version of
// the group. A first lookup at one version reads that version alone, and a
// fresh mapper then answers a later lookup of the group's other kinds from
// what it has read so far, so one of two unserved kinds went unnamed.
func unservedAt(mapper meta.RESTMapper, want schema.GroupVersionKind) *UnservedError {
	versions := served.Versions(mapper, want.GroupKind())
	if len(versions) == 0 || slices.Contains(versions, want.GroupVersion().String()) {
		return nil
	}
	return &UnservedError{Want: want, Served: versions}
}

// volsyncSourceUnserved reports whether the API server serves VolSync's
// ReplicationSource at another version and no longer at v1alpha1, the one
// version the controller's Go types come from.
//
// Parameters:
//   - mapper is the client's RESTMapper (see unservedAt).
//
// It returns an *UnservedError, or nil when the kind is served at v1alpha1
// or when the lookup can't tell. When no version is served, VolSync is not
// installed, and the run's own VolSync requests report that. So a BackupRun
// ends on it only on a real incompatibility.
func volsyncSourceUnserved(mapper meta.RESTMapper) *UnservedError {
	return unservedAt(mapper, volsyncSourceKind)
}

// clusterWebhookUnserved reports whether the API server serves
// CloudNativePG's Cluster at another version and no longer at v1, the
// version the bootstrap webhook's rules name.
//
// Parameters:
//   - mapper is the client's RESTMapper (see unservedAt).
//
// It returns an *UnservedError, or nil while v1 is served or when the
// lookup can't tell. While it returns an error, the API server creates a
// Cluster without calling the webhook, so a Cluster that a restore deleted
// comes back as an empty database. A RestoreRun deletes no Cluster then.
func clusterWebhookUnserved(mapper meta.RESTMapper) *UnservedError {
	return unservedAt(mapper, webhookClusterKind)
}

// CheckServedVersions reports each kind the API server serves only at
// versions other than the one the controller uses. cmd/backup-controller
// logs the error once at startup, so the log shows it before any run does.
//
// Parameters:
//   - mapper is the manager's RESTMapper.
//
// It returns an unservedKindsError that holds the *UnservedError of VolSync's
// ReplicationSource, of CloudNativePG's Cluster, or of both, and nil when
// neither reports.
func CheckServedVersions(mapper meta.RESTMapper) error {
	var errs unservedKindsError
	if err := volsyncSourceUnserved(mapper); err != nil {
		errs = append(errs, err)
	}
	if err := clusterWebhookUnserved(mapper); err != nil {
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

// unservedKindsError is the error of CheckServedVersions: one error for each
// kind that is not served at the version the controller uses.
type unservedKindsError []error

// Error joins the texts of the errors with "; ", so that the startup log
// entry stays on one line.
func (e unservedKindsError) Error() string {
	texts := make([]string, 0, len(e))
	for _, err := range e {
		texts = append(texts, err.Error())
	}
	return strings.Join(texts, "; ")
}

// Unwrap returns the errors, so that errors.As finds each *UnservedError.
func (e unservedKindsError) Unwrap() []error { return e }

// endForVolSync ends a BackupRun that may touch VolSync objects while
// volsyncSourceUnserved reports an incompatible VolSync.
//
// Parameters:
//   - run is the unfinished BackupRun, not being deleted.
//   - unservedErr is the error of volsyncSourceUnserved. Its text becomes
//     the Ready message and each unfinished item's message.
//
// It returns what abort returns: nil once the run has ended, or the error
// of a restart or release that failed, for a retry.
//
// Giving the app back needs no VolSync object, so the run ends through
// abort with reason VolSyncUnsupported: it starts the workloads it stopped,
// resumes the Kustomizations it suspended and releases its Leases. A sync
// VolSync already started goes on under VolSync's own control, as after a
// run's timeout. A later run of the claim holds back while the trigger of
// that sync is open.
func (r *BackupRunReconciler) endForVolSync(ctx context.Context, run *backupv1alpha1.BackupRun, unservedErr *UnservedError) error {
	log.FromContext(ctx).Error(unservedErr, "ending the run: VolSync is not served at the version this controller uses",
		"namespace", run.Namespace, "name", run.Name)
	return r.abort(ctx, run, backupv1alpha1.ReasonVolSyncUnsupported, unservedErr.Error())
}

// serve wraps a reconciler's client and reader so that a request at a
// version the API server has stopped serving is an error the reconcile
// retries, never a NotFound (see served.Client). SetupWithManager calls it.
// A request for a VolSync object at a version VolSync no longer serves then
// fails with an error that names the kind and v1alpha1, and no caller that
// checks for NotFound reads it as an object that is gone.
func serve(c client.Client, reader client.Reader) (client.Client, client.Reader) {
	c = served.Client(c)
	return c, served.Reader(reader, c)
}

// recreateMessage returns the Ready message for a run that has deleted the
// Clusters named in recreate and waits for their owner to create them again
// while the bootstrap webhook can't see a creation.
//
// Parameters:
//   - unservedErr is the error of clusterWebhookUnserved.
//   - recreate names the Clusters to create again.
//
// By then the run has given back what it stopped and resumed the
// Kustomizations it suspended, so nothing holds a creation back. The message
// says what a creation while v1 is not served does, and how to restore a
// Cluster that came back empty.
func recreateMessage(unservedErr *UnservedError, recreate []string) string {
	names := strings.Join(recreate, ", ")
	return fmt.Sprintf("%s. The run has deleted %s and holds nothing back that creates it again. A Cluster created while %s "+
		"is not served starts as an empty database, and its item then fails; restore it with a new RestoreRun once %s is served "+
		"again or a backup-controller release that registers the new version is installed. A Cluster created after that, "+
		"while this run still waits, is recovered", unservedErr.Error(), names, unservedErr.Want.GroupVersion(), unservedErr.Want.GroupVersion())
}

// failBlindClusters fails every Pending Cluster item while the bootstrap
// webhook can't see a creation.
//
// Parameters:
//   - items are the run's items, changed in place.
//   - unservedErr is the error of clusterWebhookUnserved in this pass, or
//     nil. Its text becomes each failed item's message.
//
// It returns true when it failed an item. Each failed item records reason
// ClusterVersionUnsupported, which the run's end reason comes from (see
// endReason). Its message says that the run deleted nothing, since plan and
// work call it before the Cluster is deleted.
func failBlindClusters(items []backupv1alpha1.RestoreItem, unservedErr *UnservedError) bool {
	if unservedErr == nil {
		return false
	}
	failed := false
	for i := range items {
		if items[i].Kind == backupv1alpha1.ItemKindCluster && items[i].Phase == backupv1alpha1.ItemPending {
			items[i].Phase, items[i].Reason = backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClusterVersionUnsupported
			items[i].Message = unservedErr.Error() + ". The run deleted nothing"
			failed = true
		}
	}
	return failed
}
