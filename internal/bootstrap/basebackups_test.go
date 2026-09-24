package bootstrap

import (
	"testing"
	"time"
)

// The fields barman-cloud writes into base/<id>/backup.info, trimmed to the
// ones read here and one neighbour of each kind.
const doneInfo = `backup_id=20260915T073203
begin_time=2026-09-15 07:32:03.851231+00:00
end_time=2026-09-15 07:32:19.219011+00:00
status=DONE
systemid=7685661356187803671
`

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

func TestParseBackupInfoSkipsABackupThatDidNotFinish(t *testing.T) {
	_, done, err := ParseBackupInfo([]byte("backup_id=x\nstatus=FAILED\n"))
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Fatal("a FAILED backup was reported as complete")
	}
}

func TestParseBackupInfoReadsAnEndWithoutMicroseconds(t *testing.T) {
	backup, _, err := ParseBackupInfo([]byte("status=DONE\nend_time=2026-09-15 07:32:19+00:00\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !backup.End.Equal(time.Date(2026, 9, 15, 7, 32, 19, 0, time.UTC)) {
		t.Errorf("end = %s", backup.End)
	}
}

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
