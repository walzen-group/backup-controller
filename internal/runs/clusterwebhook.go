package runs

import (
	"fmt"
	"slices"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// webhookClusterKind is CloudNativePG's Cluster at the one version the
// bootstrap webhook's rules name (deploy/webhook.yaml and
// chart/templates/webhook.yaml, apiVersions ["v1"]). With the rules'
// matchPolicy Equivalent, the API server converts a creation at any other
// served version to v1 and calls the webhook, for as long as it serves v1.
var webhookClusterKind = ClusterGVK

// servedElsewhere returns the versions at which the API server serves the
// kind of gvk when it serves that kind, but not at gvk's version.
//
// Parameters:
//   - mapper is the client's RESTMapper (see volsyncUnsupported for how the
//     manager's mapper follows a change).
//   - gvk is the kind at the version the caller needs.
//
// It returns nil when gvk's version is served, when no version of the kind
// is served (the project is not installed), and when the lookup fails, even
// for only some versions of the group. The callers act only on a real
// incompatibility, and a failed lookup is reported by the requests that
// follow.
//
// The kind is looked up by group and kind with no version, which makes
// controller-runtime's lazy mapper read the discovery of every version of
// the group. A first lookup at one version reads that version alone, and a
// fresh mapper then answers a later lookup of the group's other kinds from
// what it has read so far, so one of two unserved kinds went unnamed.
func servedElsewhere(mapper meta.RESTMapper, gvk schema.GroupVersionKind) []string {
	versions := served.Versions(mapper, gvk.GroupKind())
	if slices.Contains(versions, gvk.GroupVersion().String()) {
		return nil
	}
	return versions
}

// webhookBlind is what clusterWebhookBlind found: the versions at which the
// API server serves CloudNativePG's Cluster while it no longer serves v1,
// the version the bootstrap webhook's rules name.
type webhookBlind struct {
	// versions are the served versions, such as postgresql.cnpg.io/v2. They
	// are empty while v1 is served or when the lookup can't tell.
	versions []string
}

// blind reports whether the API server would create a Cluster without
// calling the bootstrap webhook.
func (b webhookBlind) blind() bool {
	return len(b.versions) > 0
}

// message names the versions served and v1, and says what follows, for a
// Ready condition, an item or the log. It is rendered only for status and
// logs; no decision reads it.
func (b webhookBlind) message() string {
	return fmt.Sprintf("the API server serves CloudNativePG's Cluster at %s and no longer at %s, the version the bootstrap "+
		"webhook's rules name, so it creates a Cluster without calling the webhook, and the Cluster starts as an empty database; "+
		"no restore deletes a Cluster until %s is served again or a backup-controller release that registers the new version is "+
		"installed (see docs/compatibility.md)",
		strings.Join(b.versions, ", "), webhookClusterKind.GroupVersion(), webhookClusterKind.GroupVersion())
}

// recreateMessage returns the Ready message for a run that has deleted the
// Clusters named in recreate and waits for their owner to create them again
// while b is blind.
//
// By then the run has given back what it stopped and resumed the
// Kustomizations it suspended, so nothing holds a creation back. The message
// says what a creation while v1 is not served does, and how to restore a
// Cluster that came back empty.
func (b webhookBlind) recreateMessage(recreate []string) string {
	names := strings.Join(recreate, ", ")
	return fmt.Sprintf("%s. The run has deleted %s and holds nothing back that creates it again. A Cluster created while %s "+
		"is not served starts as an empty database, and its item then fails; restore it with a new RestoreRun once %s is served "+
		"again or a backup-controller release that registers the new version is installed. A Cluster created after that is "+
		"recovered", b.message(), names, webhookClusterKind.GroupVersion(), webhookClusterKind.GroupVersion())
}

// clusterWebhookBlind looks up whether the API server serves CloudNativePG's
// Cluster at another version and no longer at v1, the version the bootstrap
// webhook's rules name.
//
// Parameters:
//   - mapper is the client's RESTMapper.
//
// It returns the versions served in a webhookBlind, which is blind only
// then. It is not blind while v1 is served or when the lookup can't tell
// (see servedElsewhere). While it is blind, the API server creates a
// Cluster without calling the webhook, so a Cluster a restore deleted comes
// back as an empty database; a RestoreRun deletes no Cluster then.
func clusterWebhookBlind(mapper meta.RESTMapper) webhookBlind {
	return webhookBlind{versions: servedElsewhere(mapper, webhookClusterKind)}
}

// ClusterWebhookBlind is clusterWebhookBlind for cmd/backup-controller,
// which logs the message once at startup, so the controller's log says it
// before any restore does. It returns "" while the webhook sees every
// creation.
func ClusterWebhookBlind(mapper meta.RESTMapper) string {
	b := clusterWebhookBlind(mapper)
	if !b.blind() {
		return ""
	}
	return b.message()
}

// failBlindClusters fails every Pending Cluster item while b is blind.
//
// Parameters:
//   - items are the run's items, changed in place.
//   - b is what clusterWebhookBlind found in this pass. Its message becomes
//     each failed item's message.
//
// It returns the names of the items it failed, and none when b is not
// blind. The caller picks the run's reason from that list alone, so an item
// that failed for another reason is never blamed on the webhook.
//
// Each failed item's message says that the run deleted nothing, since plan
// and work call it before the Cluster is deleted.
func failBlindClusters(items []backupv1alpha1.RestoreItem, b webhookBlind) []string {
	if !b.blind() {
		return nil
	}
	var failed []string
	for i := range items {
		if items[i].Kind == "Cluster" && items[i].Phase == backupv1alpha1.ItemPending {
			items[i].Phase, items[i].Message = backupv1alpha1.ItemFailed, b.message()+". The run deleted nothing"
			failed = append(failed, items[i].Name)
		}
	}
	return failed
}

// failedReason returns the Ready reason a restore that ends with failed
// items ends with.
//
// Parameters:
//   - blindFailed are the items failBlindClusters failed in this pass.
//
// It returns ReasonClusterVersionUnsupported when failBlindClusters failed
// an item, and ReasonFailed otherwise. An item it failed in an earlier pass
// of a run that ends later counts under ReasonFailed; that item's message
// still names the unserved version.
func failedReason(blindFailed []string) string {
	if len(blindFailed) > 0 {
		return backupv1alpha1.ReasonClusterVersionUnsupported
	}
	return backupv1alpha1.ReasonFailed
}
