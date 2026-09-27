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
	backup, _, err := ParseBackupInfo([]byte("status=DONE\nbegin_time=2026-09-15 07:32:03+00:00\nend_time=2026-09-15 07:32:19+00:00\n"))
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

// TestParseBackupInfoCountsABackupWithBothTimes checks that a backup counts as
// complete when its backup.info sets begin_time and end_time, whatever its
// status. The plugin's catalog applies this rule (barman-cloud v0.6.0
// pkg/catalog/catalog.go:372-374), and barman leaves end_time set when a
// backup fails after it stops (cloud.py:1713-1737). The plugin picks such a
// FAILED backup as the base, so this package must see it too.
func TestParseBackupInfoCountsABackupWithBothTimes(t *testing.T) {
	info := "status=FAILED\nbegin_time=2026-09-15 07:32:03+00:00\nend_time=2026-09-15 07:32:19+00:00\n"
	backup, done, err := ParseBackupInfo([]byte(info))
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("a backup with begin_time and end_time was reported as incomplete")
	}
	if !backup.End.Equal(time.Date(2026, 9, 15, 7, 32, 19, 0, time.UTC)) {
		t.Errorf("end = %s", backup.End)
	}
}

// TestParseBackupInfoSkipsABackupWithoutBothTimes checks that a backup whose
// backup.info writes begin_time or end_time as None is incomplete, whatever
// its status. barman writes an unset field as None (infofile.py:275).
func TestParseBackupInfoSkipsABackupWithoutBothTimes(t *testing.T) {
	for _, info := range []string{
		"status=DONE\nbegin_time=None\nend_time=2026-09-15 07:32:19+00:00\n",
		"status=DONE\nbegin_time=2026-09-15 07:32:03+00:00\nend_time=None\n",
		"status=DONE\nend_time=2026-09-15 07:32:19+00:00\n",
	} {
		_, done, err := ParseBackupInfo([]byte(info))
		if err != nil || done {
			t.Errorf("%q: done = %v, err = %v; want incomplete", info, done, err)
		}
	}
}
