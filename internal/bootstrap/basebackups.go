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

// BaseBackup is one base backup barman wrote under a database's base/ prefix.
type BaseBackup struct {
	// ID is barman's backup ID, the directory name under base/.
	ID string
	// End is when the backup finished, from backup.info's end_time. A
	// recovery target has to fall at or after it: Postgres reaches a
	// consistent state only once the WAL written during the backup is
	// replayed.
	End time.Time
}

// BaseBackups lists the completed base backups at a location, oldest first.
//
// barman keeps one directory per backup, holding a backup.info of key=value
// lines. A backup whose status is not DONE is skipped: a failed or running
// backup is nothing a recovery can start from.
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
		reader, err := client.GetObject(ctx, at.Bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("get %s/%s: %w", at.Bucket, object.Key, err)
		}
		raw, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, fmt.Errorf("read %s/%s: %w", at.Bucket, object.Key, err)
		}
		backup, done, err := baseBackupAt(object.Key, raw)
		if err != nil {
			return nil, fmt.Errorf("%s/%s: %w", at.Bucket, object.Key, err)
		}
		if done {
			backups = append(backups, backup)
		}
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].End.Before(backups[j].End) })
	return backups, nil
}

// baseBackupAt reads the backup.info stored at key, base/<id>/backup.info.
//
// barman-cloud's backup.info carries no backup_id, so the directory holding it
// names the backup. Measured on the prod canary: a refusal named an empty ID
// until the ID came from the key.
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

// ParseBackupInfo reads the fields of a barman backup.info this controller
// uses, and reports whether the backup completed.
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

// parseBarmanTime reads barman's timestamp, which Python writes with a space
// between date and time and microseconds only when there are any.
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

// AtOrBefore returns the newest base backup that finished at or before t.
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
