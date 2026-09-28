//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// rebuildManifests returns the app of the rebuild scenario: the claim, its
// VolumeRestore and the Deployment of volumeManifests, whose claim names the
// VolumeRestore in its dataSourceRef as on prod, and the Cluster db.
func rebuildManifests() string {
	cluster := appManifests()
	return volumeManifests() + "---\n" + cluster[strings.Index(cluster, "apiVersion: postgresql.cnpg.io/v1"):]
}

// tearDown removes the app the way a rebuild of the namespace does: it
// deletes the app's Kustomization, so Flux prunes every object it applied,
// and waits up to five minutes until Cluster db and the claim data are gone.
func (a *app) tearDown() {
	a.t.Helper()
	a.unpublish()
	waitFor(a.t, "the app's claim and Cluster to be gone", 5*time.Minute, func() (bool, string) {
		var c cluster
		clusterErr := getJSON(a.ns, "clusters.postgresql.cnpg.io", "db", &c)
		var claim struct{}
		claimErr := getJSON(a.ns, "pvc", "data", &claim)
		return clusterErr != nil && claimErr != nil, fmt.Sprintf("cluster: %v; claim: %v", clusterErr == nil, claimErr == nil)
	}, a.describe)
}

// TestARebuiltNamespaceComesBackToThePausedMoment backs up an app with the
// app paused, then writes more to the claim and the database, and then lets
// Flux remove the whole app and apply it again, as a rebuild of the namespace
// does. With no RestoreRun, the populator fills the new claim from the paused
// snapshot, and the webhook recovers the new Cluster to the end of the base
// backup of the same moment. The claim holds the file and the database the
// rows of the paused backup, and nothing written after it: the database is
// not ahead of the volume.
func TestARebuiltNamespaceComesBackToThePausedMoment(t *testing.T) {
	t.Parallel()
	a := newApp(t, "rebuild")
	a.publish(rebuildManifests())
	a.waitHealthy("Cluster db to come up")
	a.sql("create table notes (id serial primary key, note text)")
	a.sql("insert into notes (note) values ('paused')")
	a.write("paused")

	backedUp := a.backup("paused", "all: true")
	mustSucceed(t, "BackupRun", "paused", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)
	for _, item := range backedUp.Status.Items {
		if item.Kind == "Cluster" && !item.BaseBackupWhilePaused {
			t.Fatalf("the base backup of Cluster db completed after the app was resumed: %+v\n%s", item, a.describe())
		}
	}

	a.write("after the backup")
	a.sql("insert into notes (note) values ('after the backup')")
	a.archived()

	a.tearDown()

	a.publish(rebuildManifests())
	a.waitHealthy("the rebuilt Cluster db")
	a.waitNote("the rebuilt app to read the file of the paused backup", "paused")
	if got := a.rows(); strings.Join(got, "|") != "paused" {
		t.Errorf("rows of the rebuilt database = %q, want [paused]: the database must come back to the paused moment", got)
	}

	var vr backupv1alpha1.VolumeRestore
	if err := getJSON(a.ns, "volumerestore", "data", &vr); err != nil {
		t.Fatal(err)
	}
	t.Logf("the populator filled the claim from %+v", vr.Status.Claims)
}

// TestARebuildAfterAFailedDatabaseBackupComesBackToTheLastPausedMoment backs
// up an app with the app paused, writes more, and then runs a second paused
// backup whose database Backup fails, because barman's S3 key is wrong while
// restic's is right. The failed run's volume snapshot gets no paused tag, so
// the namespace's paused moment stays the first backup's. After more writes,
// a rebuild brings the claim and the database back to the first backup: the
// database never comes back ahead of the volume.
func TestARebuildAfterAFailedDatabaseBackupComesBackToTheLastPausedMoment(t *testing.T) {
	t.Parallel()
	a := newApp(t, "failed-db")
	a.publish(rebuildManifests())
	a.waitHealthy("Cluster db to come up")
	a.sql("create table notes (id serial primary key, note text)")
	a.sql("insert into notes (note) values ('paused')")
	a.write("paused")
	good := a.backup("good", "all: true")
	mustSucceed(t, "BackupRun", "good", good.Status.Phase, good.Status.Conditions, a.describe)

	a.write("second")
	a.sql("insert into notes (note) values ('second')")
	a.archived()
	// The barman-cloud plugin reads the Secret through a cache that keeps an
	// entry for 10 seconds (plugin-barman-cloud v0.15.0,
	// internal/cnpgi/instance/internal/client/client.go). The test writes and
	// switches WAL until an archive attempt fails, which shows that the plugin
	// uses the wrong key, so the backup that follows fails too.
	failures := a.sql("select failed_count from pg_stat_archiver")
	kubectl(t, "", "-n", a.ns, "patch", "secret", "s3", "--type=merge", "-p", `{"stringData":{"AWS_SECRET_ACCESS_KEY":"wrong"}}`)
	waitFor(t, "barman to fail to archive with the wrong key", 3*time.Minute, func() (bool, string) {
		a.sql("insert into notes (note) values ('while the key is wrong')")
		a.sql("select pg_switch_wal()")
		count := a.sql("select failed_count from pg_stat_archiver")
		return count != failures, "failed_count " + count
	}, a.describe)
	failed := a.backup("database-fails", "all: true")
	if failed.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("BackupRun database-fails ended %s, want Failed\n%s", failed.Status.Phase, a.describe())
	}
	for _, item := range failed.Status.Items {
		want := backupv1alpha1.ItemSucceeded
		if item.Kind == "Cluster" {
			want = backupv1alpha1.ItemFailed
		}
		if item.Phase != want {
			t.Fatalf("item %s %s is %s, want %s\n%s", item.Kind, item.Name, item.Phase, want, a.describe())
		}
	}
	if s := newest(t, a.snapshots()); s.hasTag(pausedTag) {
		t.Fatalf("snapshot %s of the run whose database backup failed has tags %v; it must not count as a paused moment", s.ID, s.Tags)
	}
	kubectl(t, "", "-n", a.ns, "patch", "secret", "s3", "--type=merge", "-p", fmt.Sprintf(`{"stringData":{"AWS_SECRET_ACCESS_KEY":%q}}`, secretKey))

	a.write("third")
	a.sql("insert into notes (note) values ('third')")
	a.archived()
	a.tearDown()

	a.publish(rebuildManifests())
	a.waitHealthy("the rebuilt Cluster db")
	a.waitNote("the rebuilt app to read the file of the good backup", "paused")
	if got := a.rows(); strings.Join(got, "|") != "paused" {
		t.Errorf("rows of the rebuilt database = %q, want [paused]: the database must come back to the volume's moment", got)
	}
}

// TestAClusterDeletedAfterARebuildKeepsTheRowsWrittenSince rebuilds a
// namespace from its paused backup, writes a row into the rebuilt database,
// and then deletes Cluster db, as an admin or a forced Flux apply does. The
// claims are bound and hold live data, so the webhook recovers the new
// Cluster to the end of its archive: it keeps the row written after the
// rebuild, although the claims were once filled from the paused moment.
func TestAClusterDeletedAfterARebuildKeepsTheRowsWrittenSince(t *testing.T) {
	t.Parallel()
	a := newApp(t, "rebuild-delete")
	a.publish(rebuildManifests())
	a.waitHealthy("Cluster db to come up")
	a.sql("create table notes (id serial primary key, note text)")
	a.sql("insert into notes (note) values ('paused')")
	a.write("paused")
	backedUp := a.backup("paused", "all: true")
	mustSucceed(t, "BackupRun", "paused", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)

	a.tearDown()
	a.publish(rebuildManifests())
	rebuilt := a.waitHealthy("the rebuilt Cluster db")
	a.waitNote("the rebuilt app to read the file of the paused backup", "paused")

	a.sql("insert into notes (note) values ('after the rebuild')")
	a.archived()
	kubectl(t, "", "-n", a.ns, "delete", "clusters.postgresql.cnpg.io", "db", "--wait=true", "--timeout=5m")
	if uid := a.waitHealthy("Flux to create Cluster db again and the recovery to finish"); uid == rebuilt {
		t.Fatalf("Cluster db still has UID %s after the delete", rebuilt)
	}
	if got, want := a.rows(), []string{"paused", "after the rebuild"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("rows after the second recovery = %q, want %q: the claims hold live data, so the database comes back with its whole archive", got, want)
	}
}

// staleManifests returns the app of rebuildManifests with a second claim,
// extra, which the Deployment mounts at /extra and whose VolumeRestore extra
// names the repository Secret restic-extra.
//
// Parameters:
//   - extraEnabled marks the claim extra for backup when true. When false,
//     the claim and its VolumeRestore stay, and no BackupRun writes to the
//     repository any more.
func staleManifests(extraEnabled bool) string {
	enabled := "true"
	if !extraEnabled {
		enabled = "false"
	}
	extra := fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: extra
spec:
  repository: restic-extra
  cacheStorageClassName: %[1]s
  cacheCapacity: 100Mi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: extra
  annotations:
    backup.wlz.li/enabled: "%[2]s"
    backup.wlz.li/retain-last: "10"
spec:
  storageClassName: %[1]s
  accessModes: [ReadWriteOnce]
  resources:
    requests: {storage: 100Mi}
  dataSourceRef:
    apiGroup: backup.wlz.li
    kind: VolumeRestore
    name: extra
---
`, storageClass, enabled)
	app := strings.Replace(rebuildManifests(), "volumeMounts: [{name: data, mountPath: /data}]",
		"volumeMounts: [{name: data, mountPath: /data}, {name: extra, mountPath: /extra}]", 1)
	app = strings.Replace(app, "          persistentVolumeClaim: {claimName: data}\n",
		"          persistentVolumeClaim: {claimName: data}\n        - name: extra\n          persistentVolumeClaim: {claimName: extra}\n", 1)
	return extra + app
}

// TestARebuildComesBackToTheNewestBackupWhenARepositoryIsStale backs up an
// app with two claims while it is paused, then stops backing up the claim
// extra and keeps its VolumeRestore, as happens when a claim is taken out of
// the backup. A second paused backup covers only the claim data. After more
// writes, a rebuild does not go back to the first backup, the only time both
// repositories hold: there is no paused moment of the whole namespace, so
// the claim data gets its newest snapshot and the database its whole
// archive.
func TestARebuildComesBackToTheNewestBackupWhenARepositoryIsStale(t *testing.T) {
	t.Parallel()
	a := newApp(t, "stale")
	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: restic-extra
  namespace: %[1]s
stringData:
  RESTIC_REPOSITORY: s3:%[4]s/volsync/%[1]s/extra
  RESTIC_PASSWORD: e2e-restic-password
  AWS_ACCESS_KEY_ID: %[2]s
  AWS_SECRET_ACCESS_KEY: %[3]s
`, a.ns, accessKey, secretKey, s3Endpoint))
	a.publish(staleManifests(true))
	a.waitHealthy("Cluster db to come up")
	a.sql("create table notes (id serial primary key, note text)")
	a.sql("insert into notes (note) values ('both')")
	a.write("both")
	kubectl(t, "", "-n", a.ns, "exec", "deploy/app", "--", "sh", "-c", "echo extra > /extra/note && sync")
	both := a.backup("both", "all: true")
	mustSucceed(t, "BackupRun", "both", both.Status.Phase, both.Status.Conditions, a.describe)

	a.publish(staleManifests(false))
	waitFor(t, "Flux to take the claim extra out of the backup", 3*time.Minute, func() (bool, string) {
		var claim struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		if err := getJSON(a.ns, "pvc", "extra", &claim); err != nil {
			return false, err.Error()
		}
		value := claim.Metadata.Annotations[backupv1alpha1.AnnotationEnabled]
		return value == "false", "enabled=" + value
	}, a.describe)
	a.write("data only")
	a.sql("insert into notes (note) values ('data only')")
	dataOnly := a.backup("data-only", "all: true")
	mustSucceed(t, "BackupRun", "data-only", dataOnly.Status.Phase, dataOnly.Status.Conditions, a.describe)
	a.sql("insert into notes (note) values ('after the backups')")
	a.archived()

	a.unpublish()
	waitFor(t, "the app's claims and Cluster to be gone", 5*time.Minute, func() (bool, string) {
		var c cluster
		clusterErr := getJSON(a.ns, "clusters.postgresql.cnpg.io", "db", &c)
		var claim struct{}
		dataErr := getJSON(a.ns, "pvc", "data", &claim)
		extraErr := getJSON(a.ns, "pvc", "extra", &claim)
		return clusterErr != nil && dataErr != nil && extraErr != nil, fmt.Sprintf("cluster: %v; data: %v; extra: %v", clusterErr == nil, dataErr == nil, extraErr == nil)
	}, a.describe)
	a.publish(staleManifests(false))
	a.waitHealthy("the rebuilt Cluster db")
	a.waitNote("the rebuilt app to read the file of the newest backup", "data only")
	if got, want := a.rows(), []string{"both", "data only", "after the backups"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("rows of the rebuilt database = %q, want %q: with no paused moment of the whole namespace, the database comes back with its whole archive", got, want)
	}
}

// TestARebuildWithTheClaimsFilledFirstComesBackToThePausedMoment rebuilds a
// namespace in two steps, as when the Cluster sits in a later Kustomization
// or its first create failed: Flux first applies the claim and the app, the
// populator fills the claim from the paused backup and it binds, and only
// then does Flux apply the Cluster. No database wrote since the claim was
// filled, so the webhook still recovers the Cluster to the paused moment:
// the rows written after the backup are not there.
func TestARebuildWithTheClaimsFilledFirstComesBackToThePausedMoment(t *testing.T) {
	t.Parallel()
	a := newApp(t, "claims-first")
	a.publish(rebuildManifests())
	a.waitHealthy("Cluster db to come up")
	a.sql("create table notes (id serial primary key, note text)")
	a.sql("insert into notes (note) values ('paused')")
	a.write("paused")
	backedUp := a.backup("paused", "all: true")
	mustSucceed(t, "BackupRun", "paused", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)
	a.sql("insert into notes (note) values ('after the backup')")
	a.archived()

	a.tearDown()
	a.publish(volumeManifests())
	a.waitNote("the app to read the file of the paused backup before the Cluster exists", "paused")
	a.publish(rebuildManifests())
	a.waitHealthy("the rebuilt Cluster db")
	if got := a.rows(); strings.Join(got, "|") != "paused" {
		t.Errorf("rows of the rebuilt database = %q, want [paused]: the claim came from the paused moment, so the database has to as well", got)
	}
}

// TestARebuildBringsAHibernatedDatabaseBackWithItsLastRows takes a base
// backup, writes one more row, and hibernates Cluster db. A paused backup of
// the namespace then skips the hibernated Cluster and tags the volume's
// snapshot hibernated/db. After a rebuild the webhook recovers the Cluster
// to the end of its archive, since no WAL came after the paused backup: the
// database holds the row written after its last base backup.
func TestARebuildBringsAHibernatedDatabaseBackWithItsLastRows(t *testing.T) {
	t.Parallel()
	a := newApp(t, "hibernated")
	a.publish(rebuildManifests())
	a.waitHealthy("Cluster db to come up")
	a.sql("create table notes (id serial primary key, note text)")
	a.sql("insert into notes (note) values ('in the base backup')")
	base := a.backup("base", "database: db")
	mustSucceed(t, "BackupRun", "base", base.Status.Phase, base.Status.Conditions, a.describe)
	a.sql("insert into notes (note) values ('after the base backup')")
	a.archived()
	a.write("paused")

	kubectl(t, "", "-n", a.ns, "annotate", "clusters.postgresql.cnpg.io", "db", "cnpg.io/hibernation=on")
	waitFor(t, "Cluster db to hibernate", 5*time.Minute, func() (bool, string) {
		out, err := run(t.Context(), "", "-n", a.ns, "get", "pods", "-l", "cnpg.io/cluster=db", "-o", "name")
		return err == nil && strings.TrimSpace(out) == "", strings.TrimSpace(out)
	}, a.describe)
	paused := a.backup("paused", "all: true")
	mustSucceed(t, "BackupRun", "paused", paused.Status.Phase, paused.Status.Conditions, a.describe)
	if s := newest(t, a.snapshots()); !s.hasTag(pausedTag) || !s.hasTag("hibernated/db") {
		t.Fatalf("snapshot %s has tags %v, want %s and hibernated/db", s.ID, s.Tags, pausedTag)
	}

	a.tearDown()
	a.publish(rebuildManifests())
	a.waitHealthy("the rebuilt Cluster db")
	a.waitNote("the rebuilt app to read the file of the paused backup", "paused")
	if got, want := a.rows(), []string{"in the base backup", "after the base backup"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("rows of the rebuilt database = %q, want %q: the hibernated database had not run since the paused backup", got, want)
	}
}
