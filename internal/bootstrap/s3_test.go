package bootstrap

import (
	"strings"
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

// TestAnArchiveWithNoDoneBaseBackupRefusesTheCluster checks that a Cluster
// is refused, and not started empty, when its prefix holds objects but no
// completed base backup, through the real S3Prober against stores that
// barman-cloud 3.20.0 recorded. Finding W1, designs/webhook.md A.
//
// CloudNativePG runs barman-cloud-check-wal-archive before a new database
// archives, and barman fails it on any WAL under the prefix (the recorded
// verdicts of failed-base, started-base and wal-only), so a Cluster started
// empty there would never archive. A recovery has nothing to start from
// either. The failed-base store without its WAL passes barman's check; the
// webhook refuses it too, since its fix (clearing a prefix that holds nothing
// recoverable) costs nothing.
func TestAnArchiveWithNoDoneBaseBackupRefusesTheCluster(t *testing.T) {
	failed := barmanstore.MustLoad(t, "failed-base")
	failedNoWAL := barmanstore.Store{Manifest: barmanstore.Manifest{Store: "failed-base without WAL"}}
	for _, o := range failed.Objects {
		if !strings.Contains(o.Key, "/wals/") {
			failedNoWAL.Objects = append(failedNoWAL.Objects, o)
		}
	}

	for _, tc := range []struct {
		name  string
		store barmanstore.Store
		want  string
	}{
		{"failed-base", failed, "(1 base backup under base/, not DONE)"},
		{"started-base", barmanstore.MustLoad(t, "started-base"), "(1 base backup under base/, not DONE)"},
		{"wal-only", barmanstore.MustLoad(t, "wal-only"), "no base backup under base/, but other objects, such as WAL, under the prefix"},
		{"two-servers", barmanstore.MustLoad(t, "two-servers"), "no base backup under base/"},
		{"failed-base without WAL", failedNoWAL, "(1 base backup under base/, not DONE)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := decideOn(t, cluster(t, nil), recordedS3(t, tc.store))

			if response.Allowed {
				t.Fatalf("the cluster was admitted over an archive with no completed base backup (patches %v)", response.Patches)
			}
			message := response.Result.Message
			for _, want := range []string{"s3://backups/app/app-pg/", tc.want, `serverName that is not "app-pg"`, "delete everything under s3://backups/app/app-pg/"} {
				if !strings.Contains(message, want) {
					t.Errorf("the refusal %q does not say %q", message, want)
				}
			}
		})
	}
}

// TestASiblingPrefixDoesNotCount checks that objects under a prefix that
// merely starts with the Cluster's own, such as app/app-pg/ beside a
// serverName of app-p, leave the Cluster to initdb.
func TestASiblingPrefixDoesNotCount(t *testing.T) {
	c := cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		plugin, _ := spec["plugins"].([]any)[0].(map[string]any)
		plugin["parameters"] = map[string]any{"barmanObjectName": "app-pg-store", "serverName": "app-p"}
	})

	response := decideOn(t, c, recordedS3(t, barmanstore.MustLoad(t, "wal-only")))

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("the cluster was rewritten: %v", response.Patches)
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

// TestTheRefusalCountsTheBaseBackups checks the parenthesis of the refusal
// for a prefix with several base backup directories and none DONE.
func TestTheRefusalCountsTheBaseBackups(t *testing.T) {
	at := Location{Bucket: "backups", Prefix: "app/app-pg"}
	message := noDoneBackup(at, "app-pg", 3)
	if want := "s3://backups/app/app-pg/ holds an archive with no completed base backup (3 base backups under base/, none DONE)."; !strings.HasPrefix(message, want) {
		t.Errorf("refusal %q does not start with %q", message, want)
	}
}
