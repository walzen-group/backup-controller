package bootstrap

import (
	"testing"

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
// patch, through the real S3Prober against a fake S3 server.
//
// barman writes base/<id>/ as soon as a backup starts, so a backup that failed
// or never finished leaves objects under base/. Recovering from such an
// archive fails in CloudNativePG and the Cluster never starts, while the same
// Cluster bootstrapped as written starts empty and archives from then on.
func TestAStoreWhoseOnlyBaseBackupFailedLeavesTheClusterAsWritten(t *testing.T) {
	server := fakeS3(t, "backups", map[string]string{
		"app/app-pg/base/20260920T030000/backup.info": "backup_id=20260920T030000\nstatus=FAILED\n",
		"app/app-pg/base/20260920T030000/data.tar":    "partial",
	})

	response := decideWith(t, cluster(t, nil), S3Prober{}, storeAt(server.URL))

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("the cluster was rewritten to recover from a failed base backup: %v", response.Patches)
	}
}

// TestAStoreWithADoneBaseBackupRecoversTheCluster checks that a Cluster whose
// archive holds a failed base backup and a completed one is rewritten to
// recover, through the real S3Prober against a fake S3 server.
func TestAStoreWithADoneBaseBackupRecoversTheCluster(t *testing.T) {
	server := fakeS3(t, "backups", map[string]string{
		"app/app-pg/base/20260919T030000/backup.info": "status=FAILED\n",
		"app/app-pg/base/20260920T030000/backup.info": "status=DONE\nend_time=2026-09-20 03:00:20+00:00\n",
		"app/app-pg/base/20260920T030000/data.tar":    "data",
	})

	original := cluster(t, nil)
	response := decideWith(t, original, S3Prober{}, storeAt(server.URL))

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	patched := applied(t, original, response)
	source, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "source")
	if source != RecoverySource {
		t.Errorf("recovery source = %q, want %q", source, RecoverySource)
	}
}
