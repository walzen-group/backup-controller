package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// BaseBackup is one base backup that barman wrote under a database's base/
// prefix.
type BaseBackup struct {
	// ID is barman's backup ID, such as 20260915T073203. It's the name of the
	// backup's directory under base/.
	ID string
	// End is when the backup finished, from backup.info's end_time. A
	// recovery target has to fall at or after it, because Postgres reaches a
	// consistent state only once the WAL written during the backup is
	// replayed.
	End time.Time
}

// BaseBackups lists the completed base backups of one database, oldest first.
// The webhook uses it to check that a recovery target can be reached, and the
// RestoreRun controller uses it to pick the backup a restore starts from.
//
// The at argument is the database's Location, as ResolveLocation returns it.
//
// barman keeps one directory per backup, and each holds a backup.info file of
// key=value lines. The method lists every object under the location's base
// prefix, downloads each backup.info and parses it with ParseBackupInfo. It
// skips a backup whose status isn't DONE, because a recovery can't start from
// a failed or running backup. It returns an error when the listing fails, when
// a backup.info can't be downloaded or read, or when a DONE backup's end_time
// doesn't parse. A location with no completed backup gives an empty list
// and no error.
func (p S3Prober) BaseBackups(ctx context.Context, at Location) ([]BaseBackup, error) {
	client, err := p.client(at)
	if err != nil {
		return nil, err
	}

	var backups []BaseBackup
	listing := client.ListObjects(ctx, at.Bucket, minio.ListObjectsOptions{Prefix: at.BasePrefix(), Recursive: true})
	for object := range listing {
		if object.Err != nil {
			return nil, fmt.Errorf("list %s/%s: %w", at.Bucket, at.BasePrefix(), object.Err)
		}
		if !strings.HasSuffix(object.Key, "/backup.info") {
			continue
		}
		backup, done, err := readBaseBackup(ctx, client, at.Bucket, object.Key)
		if err != nil {
			return nil, err
		}
		if done {
			backups = append(backups, backup)
		}
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].End.Before(backups[j].End) })
	return backups, nil
}

// readBaseBackup downloads one backup.info from the bucket and parses it with
// baseBackupAt. The key is the object's full key, ending in
// base/<id>/backup.info.
//
// It returns the same values as baseBackupAt. It returns an error, naming the
// object, when the download or the parse fails.
func readBaseBackup(ctx context.Context, client *minio.Client, bucket, key string) (BaseBackup, bool, error) {
	reader, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return BaseBackup{}, false, fmt.Errorf("get %s/%s: %w", bucket, key, err)
	}
	raw, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return BaseBackup{}, false, fmt.Errorf("read %s/%s: %w", bucket, key, err)
	}
	backup, done, err := baseBackupAt(key, raw)
	if err != nil {
		return BaseBackup{}, false, fmt.Errorf("%s/%s: %w", bucket, key, err)
	}
	return backup, done, nil
}

// baseBackupAt parses one backup.info and fills in the backup's ID from the
// object key when the file carries none.
//
// Parameters:
//   - key is the object key the file was read from, which ends in
//     base/<id>/backup.info.
//   - raw is the file's contents.
//
// It returns the same values as ParseBackupInfo. barman-cloud writes no
// backup_id into backup.info, so the name of the directory holding the file is
// the backup's ID. On the prod canary, refusals named an empty backup ID until
// the ID was taken from the key.
func baseBackupAt(key string, raw []byte) (BaseBackup, bool, error) {
	backup, done, err := ParseBackupInfo(raw)
	if err != nil {
		return BaseBackup{}, false, err
	}
	if backup.ID == "" {
		backup.ID = path.Base(path.Dir(key))
	}
	return backup, done, nil
}

// ParseBackupInfo reads a barman backup.info file, given as raw bytes of
// key=value lines, and returns the backup it describes.
//
// The bool is true when the file's status is DONE. For any other status it
// returns false with only the ID filled in (from backup_id, which may be
// empty), and it doesn't read end_time. For a DONE backup it returns an error
// when end_time is missing or isn't in barman's timestamp format. Lines
// without an "=" are ignored.
func ParseBackupInfo(raw []byte) (BaseBackup, bool, error) {
	fields := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), "=")
		if ok {
			fields[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	if err := scanner.Err(); err != nil {
		return BaseBackup{}, false, err
	}

	if fields["status"] != "DONE" {
		return BaseBackup{ID: fields["backup_id"]}, false, nil
	}
	end, err := parseBarmanTime(fields["end_time"])
	if err != nil {
		return BaseBackup{}, false, fmt.Errorf("end_time: %w", err)
	}
	return BaseBackup{ID: fields["backup_id"], End: end}, true, nil
}

// parseBarmanTime parses a timestamp from backup.info, such as
// "2026-09-15 07:32:19.219011+00:00". Python writes it with a space between
// the date and the time, and writes microseconds only when there are any, so
// both layouts are tried.
func parseBarmanTime(value string) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05-07:00",
	} {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparsable time %q", value)
}

// AtOrBefore picks the base backup that a recovery to the moment t would start
// from. It returns the newest backup in the list whose End is at or before t,
// and true. When every backup finished after t, it returns false, because
// Postgres can't recover to a moment before its base backup finished. The list
// doesn't need to be sorted.
func AtOrBefore(backups []BaseBackup, t time.Time) (BaseBackup, bool) {
	var found BaseBackup
	ok := false
	for _, b := range backups {
		if b.End.After(t) {
			continue
		}
		if !ok || b.End.After(found.End) {
			found, ok = b, true
		}
	}
	return found, ok
}
