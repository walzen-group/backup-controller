package bootstrap

import (
	"strings"
	"testing"

	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// lastBackup is the lastSuccessfulBackupTime that the recovery window cases
// write into the status of the ObjectStore.
const lastBackup = "2026-09-20T03:00:01Z"

// storeWithWindows builds the ObjectStore from storeAt, pointed at the given
// fake S3 server, with the given status.serverRecoveryWindow. A nil windows
// map leaves the status out.
func storeWithWindows(endpoint string, windows map[string]any) *unstructured.Unstructured {
	s := storeAt(endpoint)
	if windows != nil {
		_ = unstructured.SetNestedMap(s.Object, windows, "status", "serverRecoveryWindow")
	}
	return s
}

// completedWindow is a recovery window as plugin-barman-cloud v0.15.0 writes
// it when the catalog of the server holds a completed backup
// (internal/cnpgi/instance/recovery_window.go:52-61).
func completedWindow() map[string]any {
	return map[string]any{
		"firstRecoverabilityPoint": "2026-09-19T03:00:01Z",
		"lastSuccessfulBackupTime": lastBackup,
	}
}

// TestAnEmptyPrefixIsRefusedWhenTheStoreStatusRecordsABackup checks that the
// webhook does not start an empty database when the listing of the prefix
// finds nothing, but the ObjectStore status records a completed backup for
// the server name. A listing that reads another place than the plugin writes
// would otherwise start an empty database beside a full archive, and the
// plugin would then refuse to archive it. The refusal names the prefix, the empty
// listing and the recorded backup. The case runs for the default path and for
// the opt-out annotation, which both start initdb on an empty prefix.
func TestAnEmptyPrefixIsRefusedWhenTheStoreStatusRecordsABackup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cluster *unstructured.Unstructured
	}{
		{"default", cluster(t, nil)},
		{"opted out", optedOut(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := recordedS3(t, barmanstore.MustLoad(t, "empty"))
			objectStore := storeWithWindows(server.URL, map[string]any{"app-pg": completedWindow()})

			response := decideWith(t, tc.cluster, S3Prober{}, objectStore)

			if response.Allowed {
				t.Fatalf("the cluster was admitted over an empty listing that the store status contradicts (patches %v)", response.Patches)
			}
			message := response.Result.Message
			for _, want := range []string{"s3://backups/app/app-pg/", "status.serverRecoveryWindow", `"app-pg"`, lastBackup, "app/app-pg-store"} {
				if !strings.Contains(message, want) {
					t.Errorf("the refusal %q does not say %q", message, want)
				}
			}
		})
	}
}
