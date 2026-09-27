package bootstrap

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// recordedBackup returns the time of the last completed backup that the
// status of an ObjectStore records for one server name.
//
// Parameters:
//   - store is the ObjectStore that resolveStore read.
//   - serverName is the server name of the Cluster, from Archiver. The plugin
//     writes the status under this key.
//
// It returns status.serverRecoveryWindow[serverName].lastSuccessfulBackupTime.
// It returns an empty string when the status, the key or the field is not
// there.
//
// plugin-barman-cloud v0.15.0 keeps one recovery window for each server name
// (api/v1/objectstore_types.go:77-96). After each backup and each catalog
// check, the sidecar sets lastSuccessfulBackupTime to the end of the newest
// completed backup. When the catalog holds no completed backup, it clears
// the field (internal/cnpgi/instance/recovery_window.go:52-61, barman-cloud
// v0.6.0 pkg/catalog/catalog.go:81-88). A failed backup sets only
// lastFailedBackupTime (recovery_window.go:79-85), so a key with no
// lastSuccessfulBackupTime records no backup.
func recordedBackup(store *unstructured.Unstructured, serverName string) string {
	windows, _, _ := unstructured.NestedMap(store.Object, "status", "serverRecoveryWindow")
	window, _ := windows[serverName].(map[string]any)
	last, _ := window["lastSuccessfulBackupTime"].(string)
	return last
}

// contradicted refuses a create when the listing found nothing under the
// prefix of the Cluster, and the status of its ObjectStore records a
// completed backup for the server name. optOut and withoutBaseBackup call it
// before they let the Cluster start empty.
//
// It returns the refusal and true when the status records a backup. It
// returns false when the status records none.
//
// The two sources disagree when the listing reads another place than the
// plugin writes to, for example through another endpoint or other
// credentials. They also disagree when somebody deleted the archive and the
// status still holds the old entry. The webhook cannot tell these cases
// apart, so it refuses and names both facts.
func contradicted(c creation) (admission.Response, bool) {
	if c.recorded == "" {
		return admission.Response{}, false
	}
	c.logger.Info("refusing the Cluster", "reason", "the listing found nothing, and the ObjectStore status records a backup", "prefix", c.at.ServerPrefix(), "lastSuccessfulBackupTime", c.recorded)
	return admission.Denied(recordedButEmpty(c)), true
}

// recordedButEmpty words the refusal of contradicted. It names the prefix, the
// ObjectStore, the server name and the recorded backup time, and gives the
// command that removes a stale entry from the status.
func recordedButEmpty(c creation) string {
	pointer := strings.NewReplacer("~", "~0", "/", "~1").Replace(c.serverName)
	return fmt.Sprintf(
		"The listing of s3://%s/%s found nothing. The status of ObjectStore %s/%s records a completed backup for serverName %q (status.serverRecoveryWindow, lastSuccessfulBackupTime %s). The webhook does not start an empty database while the two disagree. Make sure that the destinationPath, endpointURL and credentials of the ObjectStore reach the bucket that holds the backups. If you deleted that archive on purpose, delete the old entry from the status with this command, then create the Cluster again: kubectl -n %s patch objectstores.barmancloud.cnpg.io %s --subresource=status --type=json -p '[{\"op\":\"remove\",\"path\":\"/status/serverRecoveryWindow/%s\"}]'",
		c.at.Bucket, c.at.ServerPrefix(), c.req.Namespace, c.store, c.serverName, c.recorded,
		c.req.Namespace, c.store, pointer,
	)
}
