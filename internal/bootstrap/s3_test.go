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
// either.
func TestAnArchiveWithNoDoneBaseBackupRefusesTheCluster(t *testing.T) {
	failed := barmanstore.MustLoad(t, "failed-base")
	for _, tc := range []struct {
		name  string
		store barmanstore.Store
		want  string
	}{
		{"failed-base", failed, "(1 base backup under base/, not DONE)"},
		{"started-base", barmanstore.MustLoad(t, "started-base"), "(1 base backup under base/, not DONE)"},
		{"wal-only", barmanstore.MustLoad(t, "wal-only"), "no base backup under base/, but other objects, such as WAL, under the prefix"},
		{"two-servers", barmanstore.MustLoad(t, "two-servers"), "no base backup under base/"},
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

// withoutWAL returns the recorded failed-base store without the WAL files
// under wals/, and with other objects in their place: a file under wals/
// that is not a WAL name and a file beside base/. barman-cloud-check-wal-archive
// counts only the WAL files under <server>/wals/ (barman 3.20.0
// clients/cloud_check_wal_archive.py:60-64, cloud.py:2426-2446, xlog.py:39-56,
// 572-574), so it passes on this store.
func withoutWAL(t *testing.T) barmanstore.Store {
	t.Helper()
	failed := barmanstore.MustLoad(t, "failed-base")
	store := barmanstore.Store{Manifest: barmanstore.Manifest{Store: "failed-base without WAL"}}
	for _, o := range failed.Objects {
		if !strings.Contains(o.Key, "/wals/") {
			store.Objects = append(store.Objects, o)
		}
	}
	for _, key := range []string{"app-pg/wals/0000000100000000/notes.txt", "app-pg/README"} {
		body := "not WAL"
		store.Objects = append(store.Objects, barmanstore.Object{Key: key, Size: int64(len(body)), Body: &body})
	}
	return store
}

// TestAPrefixWithoutWALStartsEmpty checks that a Cluster, opted out or not,
// starts with initdb over a prefix that holds a FAILED base backup and other
// objects but no WAL file. barman-cloud-check-wal-archive passes there, so
// the new database archives.
func TestAPrefixWithoutWALStartsEmpty(t *testing.T) {
	for name, c := range map[string]*unstructured.Unstructured{"initdb": cluster(t, nil), "opted out": optedOut(t)} {
		t.Run(name, func(t *testing.T) {
			response := decideOn(t, c, recordedS3(t, withoutWAL(t)))
			if !response.Allowed {
				t.Fatalf("the cluster was refused over a prefix without WAL: %v", response.Result)
			}
			if len(response.Patches) != 0 {
				t.Fatalf("the cluster was rewritten: %v", response.Patches)
			}
		})
	}
}

// TestWALFileFollowsBarman checks walFile against barman 3.20.0: a WAL
// segment, a .partial segment, a backup label file and a .history file count,
// also with one compression suffix of ALLOWED_COMPRESSIONS (cloud.py:90-97,
// 2426-2446; xlog.py:39-56). Other names and a second suffix do not count.
func TestWALFileFollowsBarman(t *testing.T) {
	for key, want := range map[string]bool{
		"app-pg/wals/0000000100000000/000000010000000000000004":                 true,
		"app-pg/wals/0000000100000000/000000010000000000000004.gz":              true,
		"app-pg/wals/0000000100000000/000000010000000000000004.zst":             true,
		"app-pg/wals/0000000100000000/000000010000000000000004.partial":         true,
		"app-pg/wals/0000000100000000/000000010000000000000004.00000028.backup": true,
		"app-pg/wals/00000002.history":                                          true,
		"app-pg/wals/00000002.history.bz2":                                      true,
		"app-pg/wals/0000000100000000/000000010000000000000004.zip":             false,
		"app-pg/wals/0000000100000000/000000010000000000000004.gz.gz":           false,
		"app-pg/wals/0000000100000000/notes.txt":                                false,
		"app-pg/wals/0000000100000000/00000001000000000000004":                  false,
	} {
		if got := walFile(key); got != want {
			t.Errorf("walFile(%q) = %v, want %v", key, got, want)
		}
	}
}
