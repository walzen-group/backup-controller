//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// pauseAnnotation marks a workload that a BackupRun with all: true pauses.
const pauseAnnotation = "backup.wlz.li/pause-during-backup"

// appManifests returns the objects Flux applies for a test app. They are:
//   - the claim data, marked for backup, and the VolumeRestore data that
//     names the restic repository of the claim;
//   - the Deployment app, which mounts the claim and which a BackupRun with
//     all: true pauses;
//   - the Cluster db with the database app, marked for backup, which
//     archives through the ObjectStore store.
func appManifests() string {
	return fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: data
spec:
  repository: restic
  cacheStorageClassName: %[1]s
  cacheCapacity: 100Mi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  annotations:
    backup.wlz.li/enabled: "true"
    backup.wlz.li/retain-last: "10"
spec:
  storageClassName: %[1]s
  accessModes: [ReadWriteOnce]
  resources:
    requests: {storage: 100Mi}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  annotations:
    %[2]s: "true"
spec:
  replicas: 1
  selector:
    matchLabels: {app: app}
  template:
    metadata:
      labels: {app: app}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: app
          image: %[3]s
          command: [sleep, "86400"]
          volumeMounts: [{name: data, mountPath: /data}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: data}
---
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: db
  annotations:
    backup.wlz.li/enabled: "true"
spec:
  instances: 1
  imageName: %[4]s
  storage:
    size: 512Mi
  postgresql:
    parameters:
      shared_buffers: 32MB
      max_wal_size: 64MB
      min_wal_size: 32MB
  bootstrap:
    initdb: {database: app, owner: app}
  plugins:
    - name: barman-cloud.cloudnative-pg.io
      isWALArchiver: true
      parameters:
        barmanObjectName: store
`, storageClass, pauseAnnotation, appImage, postgresImage)
}

// cluster is the part of a CloudNativePG Cluster the tests read.
type cluster struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// healthy is the status.phase of a Cluster whose instances run.
const healthy = "Cluster in healthy state"

// waitHealthy waits up to ten minutes until the Cluster db reports that it
// is healthy.
//
// Parameters:
//   - what names the wait in the log and in the failure message.
//
// It returns the UID of the Cluster, so a test can tell a new Cluster from
// the one it had before.
func (a *app) waitHealthy(what string) string {
	a.t.Helper()
	var c cluster
	waitFor(a.t, what, 10*time.Minute, func() (bool, string) {
		c = cluster{}
		if err := getJSON(a.ns, "clusters.postgresql.cnpg.io", "db", &c); err != nil {
			return false, err.Error()
		}
		return c.Status.Phase == healthy, c.Status.Phase
	}, a.describe)
	return c.Metadata.UID
}

// sql runs one SQL statement as the user postgres in the database app of the
// Cluster db, through its first instance pod.
//
// Parameters:
//   - statement is the SQL to run. psql stops at the first error.
//
// It returns the output without headers, one row per line. The test fails
// when psql fails or takes more than a minute.
func (a *app) sql(statement string) string {
	a.t.Helper()
	ctx, cancel := context.WithTimeout(a.t.Context(), time.Minute)
	defer cancel()
	out, err := run(ctx, "", "-n", a.ns, "exec", "db-1", "-c", "postgres", "--",
		"psql", "-v", "ON_ERROR_STOP=1", "-d", "app", "-tAc", statement)
	if err != nil {
		a.t.Fatalf("sql %q: %v", statement, err)
	}
	return strings.TrimSpace(out)
}

// archived makes sure that every row written so far is in the WAL archive
// in S3. It switches Postgres to a new WAL file and waits up to three minutes
// until pg_stat_archiver reports the file it left as archived.
func (a *app) archived() {
	a.t.Helper()
	wal := a.sql("select pg_walfile_name(pg_switch_wal())")
	waitFor(a.t, "WAL "+wal+" to be archived", 3*time.Minute, func() (bool, string) {
		last := a.sql("select coalesce(last_archived_wal, '') from pg_stat_archiver")
		return last >= wal, "last archived " + last
	}, a.describe)
}

// rows returns the column note of the table notes, in the order the rows
// were written, or nil when the table is empty.
func (a *app) rows() []string {
	a.t.Helper()
	out := a.sql("select note from notes order by id")
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

// backup runs one BackupRun in the app's namespace and waits up to 15
// minutes for it to end (see backupWithin).
func (a *app) backup(name, spec string) backupv1alpha1.BackupRun {
	a.t.Helper()
	return a.backupWithin(name, spec, 15*time.Minute)
}

// backupWithin runs one BackupRun in the app's namespace.
//
// Parameters:
//   - name is the name of the BackupRun.
//   - spec is the YAML of its spec, for example "database: db".
//   - wait is how long the test waits for the run to end.
//
// It returns the run as it ended, Succeeded or Failed. The test fails when
// the run does not end in time.
func (a *app) backupWithin(name, spec string, wait time.Duration) backupv1alpha1.BackupRun {
	a.t.Helper()
	apply(a.t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: BackupRun\nmetadata:\n  name: %s\n  namespace: %s\nspec:\n%s", name, a.ns, indent(spec, 2)))
	var run backupv1alpha1.BackupRun
	waitFor(a.t, "BackupRun "+name+" to end", wait, func() (bool, string) {
		run = backupv1alpha1.BackupRun{}
		if err := getJSON(a.ns, "backuprun", name, &run); err != nil {
			return false, err.Error()
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("%s %s", run.Status.Phase, ready(run.Status.Conditions))
	}, a.describe)
	return run
}

// restore runs one RestoreRun in the app's namespace.
//
// Parameters:
//   - name is the name of the RestoreRun.
//   - spec is the YAML of its spec, for example "database: db".
//
// It returns the run as it ended, Succeeded or Failed. The test fails when
// the run does not end within 20 minutes.
func (a *app) restore(name, spec string) backupv1alpha1.RestoreRun {
	a.t.Helper()
	apply(a.t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: RestoreRun\nmetadata:\n  name: %s\n  namespace: %s\nspec:\n%s", name, a.ns, indent(spec, 2)))
	var run backupv1alpha1.RestoreRun
	waitFor(a.t, "RestoreRun "+name+" to end", 20*time.Minute, func() (bool, string) {
		run = backupv1alpha1.RestoreRun{}
		if err := getJSON(a.ns, "restorerun", name, &run); err != nil {
			return false, err.Error()
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("%s %s", run.Status.Phase, ready(run.Status.Conditions))
	}, a.describe)
	return run
}

// setUpDatabase brings up the app and writes its first rows.
//
// Parameters:
//   - notes are written into the table notes, one row each, in order.
//
// It publishes the objects of appManifests, waits until the Cluster db is
// healthy, and creates the table notes before it writes the rows.
func (a *app) setUpDatabase(notes ...string) {
	a.t.Helper()
	a.publish(appManifests())
	a.waitHealthy("Cluster db to come up")
	a.sql("create table notes (id serial primary key, note text)")
	for _, n := range notes {
		a.sql(fmt.Sprintf("insert into notes (note) values ('%s')", n))
	}
}

// TestARestoredDatabaseHoldsWhatWasArchived restores the database of a
// running app to the newest backup: the RestoreRun deletes Cluster db, Flux
// creates it again, and the webhook makes it recover from its archive. The
// recovered database holds every row written before the restore started,
// since everything written was archived, also the row written after the base
// backup.
func TestARestoredDatabaseHoldsWhatWasArchived(t *testing.T) {
	t.Parallel()
	a := newApp(t, "db-newest")
	a.setUpDatabase("before the backup")

	if run := a.backup("base", "database: db"); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun base ended %s: %s\n%s", run.Status.Phase, ready(run.Status.Conditions), a.describe())
	}
	a.sql("insert into notes (note) values ('after the backup')")
	a.archived()
	before := a.waitHealthy("Cluster db")

	if run := a.restore("newest", "database: db"); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("RestoreRun newest ended %s: %s\n%s", run.Status.Phase, ready(run.Status.Conditions), a.describe())
	}
	if after := a.waitHealthy("the recovered Cluster db"); after == before {
		t.Fatalf("Cluster db still has UID %s; the restore did not replace it", before)
	}
	if got, want := a.rows(), []string{"before the backup", "after the backup"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("rows after the restore = %q, want %q", got, want)
	}
}

// TestADeletedClusterComesBackWithItsData deletes Cluster db by hand, as an
// admin or a rebuild does. Flux creates it again, and the webhook makes it
// recover from its archive with no RestoreRun: it becomes healthy and holds
// every archived row. The database is idle after its last write, which is
// the case the canary failed on.
func TestADeletedClusterComesBackWithItsData(t *testing.T) {
	t.Parallel()
	a := newApp(t, "db-deleted")
	a.setUpDatabase("first", "second")

	if run := a.backup("base", "database: db"); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun base ended %s: %s\n%s", run.Status.Phase, ready(run.Status.Conditions), a.describe())
	}
	a.archived()
	before := a.waitHealthy("Cluster db")

	kubectl(t, "", "-n", a.ns, "delete", "clusters.postgresql.cnpg.io", "db", "--wait=true", "--timeout=5m")
	if after := a.waitHealthy("Flux to create Cluster db again and the recovery to finish"); after == before {
		t.Fatalf("Cluster db still has UID %s after the delete", before)
	}
	if got, want := a.rows(), []string{"first", "second"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("rows after the recovery = %q, want %q", got, want)
	}
}

// TestASyncedRestoreOfAnIdleAppMatchesTheBackupThatPausedIt backs up the whole
// namespace with the app paused, then leaves the app and the database idle,
// and restores the namespace with syncDatabaseToVolume. The claim holds the
// file and the database the rows from the paused backup, the restore
// finishes, and the app runs again. This is the restore that looped on the
// canary.
func TestASyncedRestoreOfAnIdleAppMatchesTheBackupThatPausedIt(t *testing.T) {
	t.Parallel()
	a := newApp(t, "synced")
	a.setUpDatabase("paused state")
	waitFor(t, "Deployment app to run", 5*time.Minute, func() (bool, string) {
		out, err := run(t.Context(), "", "-n", a.ns, "exec", "deploy/app", "--", "sh", "-c", "echo paused-state > /data/note && sync && cat /data/note")
		return err == nil && strings.TrimSpace(out) == "paused-state", strings.TrimSpace(out)
	}, a.describe)

	if backedUp := a.backup("paused", "all: true"); backedUp.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun paused ended %s: %s\n%s", backedUp.Status.Phase, ready(backedUp.Status.Conditions), a.describe())
	}
	before := a.waitHealthy("Cluster db")

	restored := a.restore("synced", "all: true\nsyncDatabaseToVolume: true\npauseDuringRestore:\n  - {kind: Deployment, name: app}")
	if restored.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("RestoreRun synced ended %s: %s\n%s", restored.Status.Phase, ready(restored.Status.Conditions), a.describe())
	}
	if after := a.waitHealthy("the recovered Cluster db"); after == before {
		t.Fatalf("Cluster db still has UID %s; the restore did not replace it", before)
	}
	if got := a.rows(); strings.Join(got, "|") != "paused state" {
		t.Errorf("rows after the restore = %q, want [paused state]", got)
	}
	waitFor(t, "Deployment app to run again with the restored file", 5*time.Minute, func() (bool, string) {
		out, err := run(t.Context(), "", "-n", a.ns, "exec", "deploy/app", "--", "cat", "/data/note")
		return err == nil && strings.TrimSpace(out) == "paused-state", strings.TrimSpace(out)
	}, a.describe)
}

// ready returns the reason and the message of the Ready condition, as
// "reason: message", or an empty string when the conditions of a run hold
// no Ready condition yet.
func ready(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == backupv1alpha1.ConditionReady {
			return c.Reason + ": " + c.Message
		}
	}
	return ""
}

// TestASyncedRestoreBringsTheFileAndTheRowsBackToThePausedMoment backs up
// the whole namespace with the app paused, then keeps writing: a new file and
// a new row, a newer backup of the claim and of the database that did not
// pause the app, and one more file and row. A restore with
// syncDatabaseToVolume brings the claim and the database back to the paused
// moment: the file and the rows of the paused backup, and nothing written
// after it, although newer backups exist.
func TestASyncedRestoreBringsTheFileAndTheRowsBackToThePausedMoment(t *testing.T) {
	t.Parallel()
	a := newApp(t, "synced-busy")
	a.setUpDatabase("paused state")
	a.write("paused-state")
	paused := a.backup("paused", "all: true")
	mustSucceed(t, "BackupRun", "paused", paused.Status.Phase, paused.Status.Conditions, a.describe)

	a.write("later")
	a.sql("insert into notes (note) values ('later')")
	a.archived()
	for name, spec := range map[string]string{"later-volume": "source: data", "later-database": "database: db"} {
		run := a.backup(name, spec)
		mustSucceed(t, "BackupRun", name, run.Status.Phase, run.Status.Conditions, a.describe)
	}
	a.write("latest")
	a.sql("insert into notes (note) values ('latest')")
	a.archived()

	restored := a.restore("synced", "all: true\nsyncDatabaseToVolume: true\npauseDuringRestore:\n  - {kind: Deployment, name: app}")
	mustSucceed(t, "RestoreRun", "synced", restored.Status.Phase, restored.Status.Conditions, a.describe)
	a.waitHealthy("the recovered Cluster db")
	if got := a.rows(); strings.Join(got, "|") != "paused state" {
		t.Errorf("rows after the synced restore = %q, want [paused state]", got)
	}
	a.waitNote("the app to read the file of the paused backup", "paused-state")
}

// TestADatabaseRestoreToAMomentLandsOnTheBaseBackupBeforeIt takes two base
// backups with rows written before, between and after them. A restore to a
// moment before the first base backup fails with reason NoBackupInReach and
// leaves the Cluster and its rows as they are. A restore to a moment between
// the two base backups brings back the rows of the first one: the end of the
// newest base backup at or before the moment, and nothing written after it.
func TestADatabaseRestoreToAMomentLandsOnTheBaseBackupBeforeIt(t *testing.T) {
	t.Parallel()
	a := newApp(t, "db-as-of")
	a.setUpDatabase("first")
	tooEarly := time.Now().UTC().Add(-time.Hour)
	one := a.backup("one", "database: db")
	mustSucceed(t, "BackupRun", "one", one.Status.Phase, one.Status.Conditions, a.describe)
	between := time.Now().UTC().Add(time.Second).Truncate(time.Second)
	time.Sleep(2 * time.Second)
	a.sql("insert into notes (note) values ('second')")
	a.archived()
	two := a.backup("two", "database: db")
	mustSucceed(t, "BackupRun", "two", two.Status.Phase, two.Status.Conditions, a.describe)
	a.sql("insert into notes (note) values ('third')")
	a.archived()
	before := a.waitHealthy("Cluster db")

	early := a.restore("too-early", "database: db\nrestoreAsOf: "+tooEarly.Format(time.RFC3339))
	if early.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(early.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("RestoreRun too-early ended %s %s, want Failed NoBackupInReach\n%s", early.Status.Phase, ready(early.Status.Conditions), a.describe())
	}
	if uid := a.waitHealthy("Cluster db after the refused restore"); uid != before {
		t.Fatalf("Cluster db has UID %s, was %s: the refused restore replaced it", uid, before)
	}
	if got, want := a.rows(), []string{"first", "second", "third"}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("rows after the refused restore = %q, want %q", got, want)
	}

	asOf := a.restore("as-of", "database: db\nrestoreAsOf: "+between.Format(time.RFC3339))
	mustSucceed(t, "RestoreRun", "as-of", asOf.Status.Phase, asOf.Status.Conditions, a.describe)
	if uid := a.waitHealthy("the recovered Cluster db"); uid == before {
		t.Fatalf("Cluster db still has UID %s; the restore did not replace it", before)
	}
	if got := a.rows(); strings.Join(got, "|") != "first" {
		t.Errorf("rows after the restore to %s = %q, want [first]", between.Format(time.RFC3339), got)
	}
}

// TestADatabaseRestoreKeepsTheAppPausedUntilTheOldClusterIsGone restores
// Cluster db with the app listed in pauseDuringRestore, and samples the
// Deployment's replicas and the old Cluster's pods every second. Once the
// run has paused the app, it never runs again while a pod of the old Cluster
// exists: the app can't write into the database that is being replaced, and
// Flux can't create the Cluster again next to the old instance.
func TestADatabaseRestoreKeepsTheAppPausedUntilTheOldClusterIsGone(t *testing.T) {
	t.Parallel()
	a := newApp(t, "db-order")
	a.setUpDatabase("restored")
	base := a.backup("base", "database: db")
	mustSucceed(t, "BackupRun", "base", base.Status.Phase, base.Status.Conditions, a.describe)
	a.archived()
	old := a.waitHealthy("Cluster db")

	var violation string
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		paused := false
		for {
			var d struct {
				Spec struct{ Replicas int } `json:"spec"`
			}
			var pods struct {
				Items []struct {
					Metadata metav1.ObjectMeta `json:"metadata"`
				} `json:"items"`
			}
			out, err := run(context.Background(), "", "-n", a.ns, "get", "pods", "-l", "cnpg.io/cluster=db", "-o", "json")
			if getJSON(a.ns, "deployment", "app", &d) == nil && err == nil && jsonInto(out, &pods) == nil {
				oldPods := 0
				for _, p := range pods.Items {
					for _, owner := range p.Metadata.OwnerReferences {
						if string(owner.UID) == old {
							oldPods++
						}
					}
				}
				paused = paused || d.Spec.Replicas == 0
				if paused && d.Spec.Replicas > 0 && oldPods > 0 && violation == "" {
					violation = fmt.Sprintf("the app runs with %d replicas while %d pods of the old Cluster exist", d.Spec.Replicas, oldPods)
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	restored := a.restore("ordered", "database: db\npauseDuringRestore:\n  - {kind: Deployment, name: app}")
	close(stop)
	<-done
	mustSucceed(t, "RestoreRun", "ordered", restored.Status.Phase, restored.Status.Conditions, a.describe)
	if violation != "" {
		t.Fatalf("%s\n%s", violation, a.describe())
	}
	a.waitHealthy("the recovered Cluster db")
	if got := a.rows(); strings.Join(got, "|") != "restored" {
		t.Errorf("rows after the restore = %q, want [restored]", got)
	}
}
