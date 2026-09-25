package bootstrap

import (
	"testing"
	"time"
)

// doneInfo is a backup.info as barman-cloud writes it into base/<id>/, for a
// completed backup. It keeps the fields this package reads and one neighbour
// of each kind.
const doneInfo = `backup_id=20260915T073203
begin_time=2026-09-15 07:32:03.851231+00:00
end_time=2026-09-15 07:32:19.219011+00:00
status=DONE
systemid=7685661356187803671
`

// TestParseBackupInfoReadsTheEndToTheMicrosecond checks that a DONE backup is
// reported as complete, with its ID and its end_time kept to the microsecond.
func TestParseBackupInfoReadsTheEndToTheMicrosecond(t *testing.T) {
	backup, done, err := ParseBackupInfo([]byte(doneInfo))
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("a DONE backup was reported as incomplete")
	}
	want := time.Date(2026, 9, 15, 7, 32, 19, 219011000, time.UTC)
	if !backup.End.Equal(want) {
		t.Errorf("end = %s, want %s", backup.End, want)
	}
	if backup.ID != "20260915T073203" {
		t.Errorf("id = %q", backup.ID)
	}
}

// TestABaseBackupIsNamedByItsDirectory checks that a backup.info with no
// backup_id takes its ID from the directory that holds it. barman-cloud writes
// no backup_id into backup.info.
func TestABaseBackupIsNamedByItsDirectory(t *testing.T) {
	// The ID format is barman's, as CloudNativePG reported it for the prod
	// canary's Backup canary-namespace-backup-pg-a9158795: 20260924T224544.
	info := "status=DONE\nend_time=2026-09-24 22:45:54.061271+00:00\n"
	backup, done, err := baseBackupAt("canary-namespace-backup/canary-namespace-backup-pg/base/20260924T224544/backup.info", []byte(info))
	if err != nil || !done {
		t.Fatalf("backup = %+v, done = %v, err = %v", backup, done, err)
	}
	if backup.ID != "20260924T224544" {
		t.Errorf("id = %q, want the directory name", backup.ID)
	}
}

// TestParseBackupInfoSkipsABackupThatDidNotFinish checks that a FAILED backup
// is reported as incomplete, without an error.
func TestParseBackupInfoSkipsABackupThatDidNotFinish(t *testing.T) {
	_, done, err := ParseBackupInfo([]byte("backup_id=x\nstatus=FAILED\n"))
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("a FAILED backup was reported as complete")
	}
}

// TestParseBackupInfoReadsAnEndWithoutMicroseconds checks that an end_time
// written without a fraction of a second parses.
func TestParseBackupInfoReadsAnEndWithoutMicroseconds(t *testing.T) {
	backup, _, err := ParseBackupInfo([]byte("status=DONE\nend_time=2026-09-15 07:32:19+00:00\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !backup.End.Equal(time.Date(2026, 9, 15, 7, 32, 19, 0, time.UTC)) {
		t.Errorf("end = %s", backup.End)
	}
}

// TestBaseBackupAtOrBefore checks that AtOrBefore picks the newest backup
// finished by the given moment, and finds none for a moment before the first
// backup finished.
func TestBaseBackupAtOrBefore(t *testing.T) {
	backups := []BaseBackup{
		{ID: "a", End: time.Date(2026, 9, 13, 3, 0, 20, 0, time.UTC)},
		{ID: "b", End: time.Date(2026, 9, 20, 3, 0, 20, 0, time.UTC)},
	}
	if got, ok := AtOrBefore(backups, time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)); !ok || got.ID != "a" {
		t.Errorf("got %q, %v; want a", got.ID, ok)
	}
	if _, ok := AtOrBefore(backups, time.Date(2026, 9, 13, 3, 0, 19, 0, time.UTC)); ok {
		t.Error("a moment before the first backup finished found one")
	}
}
