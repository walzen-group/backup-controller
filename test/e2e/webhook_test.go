//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// webhookName is the name of the webhook entry that decides each Cluster
// create. The API server puts it into the error of a refused create.
const webhookName = "bootstrap.backup.wlz.li"

// clusterManifest returns a Cluster in a namespace, which archives through
// an ObjectStore with its own name as the server name.
//
// Parameters:
//   - namespace is where the Cluster goes.
//   - name is the Cluster's name, and so the archive's server name.
//   - store is the name of the ObjectStore in that namespace.
func clusterManifest(namespace, name, store string) string {
	return fmt.Sprintf(`apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: %[4]s
  namespace: %[1]s
spec:
  instances: 1
  imageName: %[3]s
  storage:
    size: 512Mi
  bootstrap:
    initdb: {database: app, owner: app}
  plugins:
    - name: barman-cloud.cloudnative-pg.io
      isWALArchiver: true
      parameters:
        barmanObjectName: %[2]s
`, namespace, store, postgresImage, name)
}

// TestTheWebhookRefusesASecondClusterOnOneArchive runs Cluster db in one app
// and then creates Clusters in a second namespace whose ObjectStores point at
// the first app's bucket and prefix. Two databases in one archive destroy each
// other's backups, so the webhook refuses a second Cluster db, also when its
// ObjectStore reaches the same S3 service under another endpoint URL. A
// Cluster with another name archives under another server name in the same
// destination, and the webhook admits it. The first Cluster keeps running.
func TestTheWebhookRefusesASecondClusterOnOneArchive(t *testing.T) {
	t.Parallel()
	first := newApp(t, "archive-a")
	first.setUpDatabase("first app")

	second := newApp(t, "archive-b")
	for _, store := range []struct{ name, endpoint string }{
		{"shared", s3Endpoint},
		{"alias", "http://rustfs.s3.svc.cluster.local:9000"},
	} {
		apply(t, fmt.Sprintf(`apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: %[4]s
  namespace: %[1]s
spec:
  configuration:
    destinationPath: s3://postgres/%[2]s/
    endpointURL: %[3]s
    s3Credentials:
      accessKeyId: {name: s3, key: AWS_ACCESS_KEY_ID}
      secretAccessKey: {name: s3, key: AWS_SECRET_ACCESS_KEY}
`, second.ns, first.ns, store.endpoint, store.name))
	}

	for _, store := range []string{"shared", "alias"} {
		_, err := run(t.Context(), clusterManifest(second.ns, "db", store), "apply", "-f", "-")
		if err == nil {
			t.Fatalf("the API server created Cluster db in %s through ObjectStore %s, which archives into s3://postgres/%s/db like Cluster db in %s",
				second.ns, store, first.ns, first.ns)
		}
		if !strings.Contains(err.Error(), webhookName) {
			t.Fatalf("the create through ObjectStore %s failed, but not because the webhook refused it: %v", store, err)
		}
		t.Logf("the webhook refused Cluster db through ObjectStore %s: %v", store, err)
	}

	if _, err := run(t.Context(), clusterManifest(second.ns, "other", "shared"), "apply", "-f", "-"); err != nil {
		t.Errorf("the webhook refused Cluster other, which archives into s3://postgres/%s/other, a prefix no Cluster uses: %v", first.ns, err)
	}
	if got := first.rows(); strings.Join(got, "|") != "first app" {
		t.Errorf("rows of the first app = %q, want [first app]", got)
	}
}

// TestTheWebhookRefusesAnEmptyDatabaseOverAnArchiveWithoutABackup runs
// Cluster db until it has archived WAL, but takes no base backup, and then
// deletes it. When Flux creates it again, the archive holds WAL and no
// completed base backup: a recovery has nothing to start from, and a new
// empty database could never archive into that prefix. The webhook refuses
// the create, and Flux reports the refusal on its Kustomization.
func TestTheWebhookRefusesAnEmptyDatabaseOverAnArchiveWithoutABackup(t *testing.T) {
	t.Parallel()
	a := newApp(t, "no-base")
	a.setUpDatabase("never backed up")
	a.archived()

	kubectl(t, "", "-n", a.ns, "delete", "clusters.postgresql.cnpg.io", "db", "--wait=true", "--timeout=5m")
	a.reconcile()
	waitFor(t, "Flux to report the webhook's refusal", 3*time.Minute, func() (bool, string) {
		var k struct {
			Status struct {
				Conditions []struct{ Type, Status, Message string } `json:"conditions"`
			} `json:"status"`
		}
		if err := getJSON("flux-system", "kustomization", a.ns, &k); err != nil {
			return false, err.Error()
		}
		for _, c := range k.Status.Conditions {
			if c.Type == "Ready" {
				return c.Status == "False" && strings.Contains(c.Message, webhookName), firstLine(c.Message)
			}
		}
		return false, "no Ready condition"
	}, a.describe)
	var c cluster
	if err := getJSON(a.ns, "clusters.postgresql.cnpg.io", "db", &c); err == nil {
		t.Errorf("Cluster db exists again (phase %q); the webhook let an empty database start over an archive without a base backup", c.Status.Phase)
	}
}

// TestAnOptedOutClusterStartsEmptyOnlyOverAnEmptyArchive creates Clusters
// that carry backup.wlz.li/bootstrap: initdb, which asks for an empty
// database. Over an archive that holds nothing, the webhook admits the
// Cluster unchanged and it starts empty. Over the archive of an earlier
// database, which holds WAL, the new database could not archive, so the
// webhook refuses it.
func TestAnOptedOutClusterStartsEmptyOnlyOverAnEmptyArchive(t *testing.T) {
	t.Parallel()
	a := newApp(t, "opt-out")
	a.setUpDatabase("earlier database")
	a.archived()

	optedOut := func(name string) string {
		m := clusterManifest(a.ns, name, "store")
		return strings.Replace(m, "metadata:\n", "metadata:\n  annotations:\n    backup.wlz.li/bootstrap: initdb\n", 1)
	}
	if _, err := run(t.Context(), optedOut("fresh"), "apply", "-f", "-"); err != nil {
		t.Fatalf("the webhook refused Cluster fresh, which opts out over an empty archive: %v", err)
	}

	a.unpublish()
	waitFor(t, "Cluster db to be gone", 5*time.Minute, func() (bool, string) {
		var c cluster
		err := getJSON(a.ns, "clusters.postgresql.cnpg.io", "db", &c)
		return err != nil, "still there"
	}, a.describe)
	_, err := run(t.Context(), optedOut("db"), "apply", "-f", "-")
	if err == nil {
		t.Fatalf("the API server created Cluster db with an empty database over the WAL of the earlier db")
	}
	if !strings.Contains(err.Error(), webhookName) {
		t.Fatalf("the create failed, but not because the webhook refused it: %v", err)
	}
	t.Logf("the webhook refused the opted-out Cluster db: %v", err)
}
