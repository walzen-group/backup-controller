package runs

import (
	"errors"
	"fmt"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
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
func servedElsewhere(mapper meta.RESTMapper, gvk schema.GroupVersionKind) []string {
	_, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	var partial *apiutil.ErrResourceDiscoveryFailed
	if err == nil || !meta.IsNoMatchError(err) || errors.As(err, &partial) {
		return nil
	}
	return served.Versions(mapper, gvk.GroupKind())
}

// clusterWebhookBlind reports whether the API server serves CloudNativePG's
// Cluster at another version and no longer at v1, the version the bootstrap
// webhook's rules name.
//
// Parameters:
//   - mapper is the client's RESTMapper.
//
// It returns a message that names the versions served and v1, and says what
// follows, or "" while v1 is served or when it can't tell (see
// servedElsewhere). While the message is non-empty, the API server creates
// a Cluster without calling the webhook, so a Cluster a restore deleted
// comes back as an empty database; a RestoreRun deletes no Cluster then.
func clusterWebhookBlind(mapper meta.RESTMapper) string {
	versions := servedElsewhere(mapper, webhookClusterKind)
	if len(versions) == 0 {
		return ""
	}
	return fmt.Sprintf("the API server serves CloudNativePG's Cluster at %s and no longer at %s, the version the bootstrap "+
		"webhook's rules name, so it creates a Cluster without calling the webhook, and the Cluster starts as an empty database; "+
		"no restore deletes a Cluster until %s is served again or a backup-controller release that registers the new version is "+
		"installed (see docs/compatibility.md)",
		strings.Join(versions, ", "), webhookClusterKind.GroupVersion(), webhookClusterKind.GroupVersion())
}

// ClusterWebhookBlind is clusterWebhookBlind for cmd/backup-controller,
// which logs the message once at startup, so the controller's log says it
// before any restore does.
func ClusterWebhookBlind(mapper meta.RESTMapper) string {
	return clusterWebhookBlind(mapper)
}

// failBlindClusters fails every Pending Cluster item with the message of
// clusterWebhookBlind and reports whether there was one.
//
// Parameters:
//   - items are the run's items, changed in place.
//   - message is clusterWebhookBlind's message.
//
// Each such item's message says that the run deleted nothing, since plan and
// restoreDatabase call it before the Cluster is deleted.
func failBlindClusters(items []backupv1alpha1.RestoreItem, message string) bool {
	failed := false
	for i := range items {
		if items[i].Kind == "Cluster" && items[i].Phase == backupv1alpha1.ItemPending {
			items[i].Phase, items[i].Message = backupv1alpha1.ItemFailed, message+". The run deleted nothing"
			failed = true
		}
	}
	return failed
}

// failedReason returns the Ready reason a restore that ends with failed
// items ends with.
//
// Parameters:
//   - items are the run's items.
//   - blind is clusterWebhookBlind's message for this pass.
//
// It returns ReasonClusterVersionUnsupported while blind is non-empty and a
// Cluster item failed, since failBlindClusters then failed it, and
// ReasonFailed otherwise.
func failedReason(items []backupv1alpha1.RestoreItem, blind string) string {
	if blind == "" {
		return backupv1alpha1.ReasonFailed
	}
	for _, item := range items {
		if item.Kind == "Cluster" && item.Phase == backupv1alpha1.ItemFailed {
			return backupv1alpha1.ReasonClusterVersionUnsupported
		}
	}
	return backupv1alpha1.ReasonFailed
}
