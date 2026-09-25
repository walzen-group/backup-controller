package bootstrap

import (
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestSplitEndpoint checks the host and TLS flag splitEndpoint returns for
// each form an ObjectStore's endpointURL takes, bare hosts with a port among
// them.
func TestSplitEndpoint(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		host     string
		secure   bool
	}{
		{"https://s3.example.com", "s3.example.com", true},
		{"https://s3.example.com:9000", "s3.example.com:9000", true},
		{"http://minio.minio.svc:9000", "minio.minio.svc:9000", false},
		{"s3.example.com", "s3.example.com", true},
		{"s3.example.com:9000", "s3.example.com:9000", true},
		{"10.0.0.1:9000", "10.0.0.1:9000", true},
	} {
		host, secure, err := splitEndpoint(tc.endpoint)
		if err != nil {
			t.Errorf("splitEndpoint(%q): %v", tc.endpoint, err)
			continue
		}
		if host != tc.host || secure != tc.secure {
			t.Errorf("splitEndpoint(%q) = %q, %v, want %q, %v", tc.endpoint, host, secure, tc.host, tc.secure)
		}
	}
	for _, endpoint := range []string{"", "ftp://s3.example.com"} {
		if _, _, err := splitEndpoint(endpoint); err == nil {
			t.Errorf("splitEndpoint(%q) succeeded, want an error", endpoint)
		}
	}
}

// TestAStoreWhoseOnlyBaseBackupFailedLeavesTheClusterAsWritten checks that a
// Cluster whose archive holds only a failed base backup is admitted without a
// patch, through the real S3Prober against the failed-base store that
// barman-cloud 3.20.0 recorded.
//
// barman writes base/<id>/ as soon as a backup starts, so a backup that failed
// or never finished leaves objects under base/. Recovering from such an
// archive fails in CloudNativePG and the Cluster never starts.
//
// The assertion here is a known bug, kept until its fix lands: finding W1,
// designs/webhook.md A. The recorded store also holds the WAL the Cluster
// archived before its backup failed, as every real failed-base archive does.
// A Cluster admitted as written then fails CloudNativePG's
// barman-cloud-check-wal-archive, because the archive is not empty, and never
// starts either. The old hand-written store held no WAL and hid this.
func TestAStoreWhoseOnlyBaseBackupFailedLeavesTheClusterAsWritten(t *testing.T) {
	server := recordedS3(t, barmanstore.MustLoad(t, "failed-base"))

	response := decideOn(t, cluster(t, nil), server)

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("the cluster was rewritten to recover from a failed base backup: %v", response.Patches)
	}
}

// TestAStoreWithADoneBaseBackupRecoversTheCluster checks that a Cluster whose
// archive holds a failed base backup and a completed one is rewritten to
// recover, through the real S3Prober against the recorded done-base store
// with a failed backup added a day before its completed one.
func TestAStoreWithADoneBaseBackupRecoversTheCluster(t *testing.T) {
	done := barmanstore.MustLoad(t, "done-base")
	withFailed, err := done.WithFailedBackup("app-pg", time.Date(2026, 9, 24, 21, 52, 25, 0, time.UTC))
	if err != nil {
		t.Fatalf("add a failed backup: %v", err)
	}
	server := recordedS3(t, withFailed)

	original := cluster(t, nil)
	response := decideOn(t, original, server)

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	patched := applied(t, original, response)
	source, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "source")
	if source != RecoverySource {
		t.Errorf("recovery source = %q, want %q", source, RecoverySource)
	}
}
