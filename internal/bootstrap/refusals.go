package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// readFailed answers a create whose Kubernetes read failed, with an HTTP
// 500, which the API server treats as a refusal.
//
// Parameters:
//   - ctx is Handle's context, the one the budget bounds.
//   - budget is the budget Handle gave itself, which the message names.
//   - step says what the read was, such as "listing the Clusters".
//   - err is the read's error, which the response carries.
//
// When ctx has passed its deadline, the message starts with "the webhook ran
// out of its <budget> budget while <step>", so a stalled API server is told
// apart from a read that failed.
func readFailed(ctx context.Context, logger logr.Logger, budget time.Duration, step string, err error) admission.Response {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("the webhook ran out of its %s budget while %s: %w", budget, step, err)
	}
	logger.Error(err, "cannot decide the Cluster")
	return admission.Errored(http.StatusInternalServerError, err)
}

// surveyFailed answers a create whose Survey failed. An *OutOfTimeError is
// refused with a message a person can act on: the counts read so far, and
// how to clear the failed backups barman never deletes. Any other error is
// an HTTP 500, which the API server treats as a refusal.
func surveyFailed(logger logr.Logger, at Location, err error) admission.Response {
	var late *OutOfTimeError
	if !errors.As(err, &late) {
		logger.Error(err, "cannot read the object store")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	logger.Info("refusing the Cluster", "reason", "the survey ran out of time", "backups", late.Backups, "read", late.Read, "failed", late.Failed)
	base := fmt.Sprintf("s3://%s/%s", at.Bucket, at.BasePrefix())
	if late.Backups == 0 {
		return admission.Denied(fmt.Sprintf(
			"Checking %s ran out of time before the object store listed its base backups. Check that the object store answers, then create the Cluster again.", base,
		))
	}
	if late.Failed != nil {
		return admission.Denied(fmt.Sprintf(
			"Checking %s ran out of time: %d base backups are there, %d backup.info files were read with no completed backup among them, and one failed to read: %v. That file might be the completed backup. Check that the object store serves it, then create the Cluster again. If the store also holds many failed or unfinished backups, which barman never deletes, delete the base/<id>/ directories whose backup.info does not say status=DONE (barman-cloud-backup-delete --backup-id <id> deletes one).",
			base, late.Backups, late.Read, late.Failed,
		))
	}
	return admission.Denied(fmt.Sprintf(
		"Checking %s ran out of time: %d base backups are there, and the newest %d read were none of them completed. barman never deletes failed or unfinished backups. Delete the base/<id>/ directories whose backup.info does not say status=DONE (barman-cloud-backup-delete --backup-id <id> deletes one), then create the Cluster again.",
		base, late.Backups, late.Read,
	))
}

// noDoneBackup words the refusal of a Cluster whose prefix holds objects but
// no completed base backup, and nothing asks for a recovery.
//
// Parameters:
//   - at is the database's Location.
//   - serverName is the directory the Cluster archives under, which the
//     message suggests changing.
//   - backups counts the base backup directories under base/, in any
//     state, as Survey's Archive.Backups does. It picks the wording of the
//     parenthesis.
func noDoneBackup(at Location, serverName string, backups int) string {
	what := "(no base backup under base/, but other objects, such as WAL, under the prefix)"
	switch backups {
	case 0:
	case 1:
		what = "(1 base backup under base/, not DONE)"
	default:
		what = fmt.Sprintf("(%d base backups under base/, none DONE)", backups)
	}
	prefix := fmt.Sprintf("s3://%s/%s", at.Bucket, at.ServerPrefix())
	return fmt.Sprintf(
		"%s holds an archive with no completed base backup %s. A database started empty here could never archive its WAL, because CloudNativePG refuses a prefix that already holds WAL, and a recovery has nothing to start from. If that archive is worth nothing, delete everything under %s and create the Cluster again. Otherwise give this Cluster a serverName that is not %q.",
		prefix, what, prefix, serverName,
	)
}

// oldest returns the end of a refusal message that names the oldest base
// backup and when it finished, or says there is none. It expects the list
// oldest first, as BaseBackups returns it.
func oldest(backups []BaseBackup) string {
	if len(backups) == 0 {
		return "; it holds no completed base backup"
	}
	return fmt.Sprintf("; the oldest, %s, finished at %s", backups[0].ID, backups[0].End.UTC().Format(time.RFC3339))
}

// recordedBackup returns the time of the last completed backup that the
// status of an ObjectStore records for one server name.
//
// Parameters:
//   - store is the ObjectStore that ResolveLocation read.
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

// contradicted refuses a create when the listing found no WAL under wals/ and
// no complete base backup under the prefix of the Cluster, and the status of
// its ObjectStore records a completed backup for the server name. optOut and
// withoutBaseBackup call it before they let the Cluster start empty.
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
	c.logger.Info("refusing the Cluster", "reason", "the listing found no WAL and no complete base backup, and the ObjectStore status records a backup", "prefix", c.at.ServerPrefix(), "lastSuccessfulBackupTime", c.recorded)
	return admission.Denied(recordedButEmpty(c)), true
}

// recordedButEmpty words the refusal of contradicted. It names the prefix, the
// ObjectStore, the server name and the recorded backup time, and gives the
// command that removes a stale entry from the status.
func recordedButEmpty(c creation) string {
	pointer := strings.NewReplacer("~", "~0", "/", "~1").Replace(c.serverName)
	return fmt.Sprintf(
		"The listing of s3://%s/%s found no WAL under wals/ and no complete base backup. The status of ObjectStore %s/%s records a completed backup for serverName %q (status.serverRecoveryWindow, lastSuccessfulBackupTime %s). The webhook does not start an empty database while the two disagree. Make sure that the destinationPath, endpointURL and credentials of the ObjectStore reach the bucket that holds the backups. If you deleted that archive on purpose, delete the old entry from the status with this command, then create the Cluster again: kubectl -n %s patch objectstores.barmancloud.cnpg.io %s --subresource=status --type=json -p '[{\"op\":\"remove\",\"path\":\"/status/serverRecoveryWindow/%s\"}]'",
		c.at.Bucket, c.at.ServerPrefix(), c.req.Namespace, c.store, c.serverName, c.recorded,
		c.req.Namespace, c.store, pointer,
	)
}
