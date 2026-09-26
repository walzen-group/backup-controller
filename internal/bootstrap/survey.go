package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

// surveyParallel is how many backup.info GETs one reader keeps in flight.
const surveyParallel = 8

// Archive is what Survey found under a database's prefix.
type Archive struct {
	// Empty is true when nothing at all exists under the server prefix.
	Empty bool
	// Found is a DONE base backup, one that finished at or before the
	// target when a target was given. It is nil when none qualifies.
	Found *BaseBackup
	// Backups counts the base backup directories under base/, in any state.
	Backups int
	// Read counts the backup.info files read, a missing one included.
	Read int
	// Oldest is the DONE backup that finished first. It is set only when
	// Found is nil and every backup.info was read.
	Oldest *BaseBackup
}

// OutOfTimeError is what Survey returns when its context ends before it has
// an answer. Backups and Read are the counts so far, which the webhook's
// refusal names. Failed is the last backup.info read that failed with
// something other than the context ending, or nil; the file it names might
// have been the completed backup, so the refusal names it.
type OutOfTimeError struct {
	Backups int
	Read    int
	Failed  error
	Err     error
}

// Error words the error with the counts.
func (e *OutOfTimeError) Error() string {
	if e.Failed != nil {
		return fmt.Sprintf("ran out of time after reading %d of %d backup.info files, one of which failed (%v): %v", e.Read, e.Backups, e.Failed, e.Err)
	}
	return fmt.Sprintf("ran out of time after reading %d of %d backup.info files: %v", e.Read, e.Backups, e.Err)
}

// Unwrap returns the context's error.
func (e *OutOfTimeError) Unwrap() error { return e.Err }

// Survey reads one database's archive for the webhook: whether anything
// exists under its prefix, and whether a DONE base backup exists that a
// recovery can start from.
//
// Parameters:
//   - at is the database's Location, as ResolveLocation returns it.
//   - target, when not nil, asks for a DONE backup that finished at or
//     before it.
//
// It lists base/ with the "/" delimiter, one entry per backup as barman's
// own catalog does, and reads the backup.info files newest first with at
// most surveyParallel GETs in flight. It stops at the first qualifying DONE
// backup; which worker finds it does not matter, since only existence
// counts. A missing backup.info counts as not DONE, as it does for barman.
// With no backup directory at all, it asks for one key under the server
// prefix to fill in Empty.
//
// It returns an *OutOfTimeError when ctx ends before an answer. It returns
// an error when the client can't be built, when a listing fails, or when a
// GET failed and no qualifying backup was found, since the unread file might
// have been the DONE one.
func (p S3Prober) Survey(ctx context.Context, at Location, target *time.Time) (Archive, error) {
	client, err := p.client(at)
	if err != nil {
		return Archive{}, err
	}
	ids, err := backupIDs(ctx, client, at)
	if err != nil {
		return Archive{}, outOfTime(ctx, err, 0, 0)
	}

	out := Archive{Backups: len(ids)}
	if len(ids) == 0 {
		out.Empty = true
		for object := range client.ListObjectsIter(ctx, at.Bucket, minio.ListObjectsOptions{Prefix: at.ServerPrefix(), Recursive: true, MaxKeys: 1}) {
			if object.Err != nil {
				return Archive{}, outOfTime(ctx, fmt.Errorf("list %s/%s: %w", at.Bucket, at.ServerPrefix(), s3Answer(object.Err)), 0, 0)
			}
			out.Empty = false
			break
		}
		return out, nil
	}

	var oldest *BaseBackup
	read, err := readInfos(ctx, client, at, ids, func(b BaseBackup) bool {
		if target == nil || !b.End.After(*target) {
			found := b
			out.Found = &found
			return true
		}
		if oldest == nil || b.End.Before(oldest.End) {
			first := b
			oldest = &first
		}
		return false
	})
	out.Read = read
	if out.Found != nil {
		return out, nil
	}
	if err != nil {
		return Archive{}, outOfTime(ctx, err, len(ids), read)
	}
	out.Oldest = oldest
	return out, nil
}

// BaseBackups lists the completed base backups of one database, oldest
// first. The RestoreRun controller uses it to pick the backup a restore
// starts from.
//
// The at argument is the database's Location, as ResolveLocation returns it.
//
// It reads the catalog as Survey does, the delimited listing of base/ and
// backup.info files with surveyParallel GETs in flight, but reads every one.
// A missing backup.info counts as not DONE. It returns an error when the
// listing fails, when a backup.info can't be downloaded or read, or when a
// DONE backup's end_time doesn't parse. A location with no completed backup
// gives an empty list and no error.
func (p S3Prober) BaseBackups(ctx context.Context, at Location) ([]BaseBackup, error) {
	client, err := p.client(at)
	if err != nil {
		return nil, err
	}
	ids, err := backupIDs(ctx, client, at)
	if err != nil {
		return nil, err
	}
	var backups []BaseBackup
	if _, err := readInfos(ctx, client, at, ids, func(b BaseBackup) bool {
		backups = append(backups, b)
		return false
	}); err != nil {
		return nil, err
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].End.Before(backups[j].End) })
	return backups, nil
}

// outOfTime wraps err in an *OutOfTimeError when ctx has ended, and returns
// it unchanged otherwise. An err that already is an *OutOfTimeError, as
// readInfos returns, is returned as it is.
func outOfTime(ctx context.Context, err error, backups, read int) error {
	var late *OutOfTimeError
	if errors.As(err, &late) {
		return err
	}
	if ctx.Err() != nil {
		return &OutOfTimeError{Backups: backups, Read: read, Err: ctx.Err()}
	}
	return err
}

// backupIDs lists the backup directories under the location's base/ with the
// "/" delimiter and returns their IDs newest first. barman names a directory
// by the backup's start time, so the IDs sort by time as strings. It reads
// every page before it returns. It returns an error when the listing fails.
func backupIDs(ctx context.Context, client *minio.Client, at Location) ([]string, error) {
	var ids []string
	for object := range client.ListObjectsIter(ctx, at.Bucket, minio.ListObjectsOptions{Prefix: at.BasePrefix()}) {
		if object.Err != nil {
			return nil, fmt.Errorf("list %s/%s: %w", at.Bucket, at.BasePrefix(), s3Answer(object.Err))
		}
		if strings.HasSuffix(object.Key, "/") {
			ids = append(ids, path.Base(strings.TrimSuffix(object.Key, "/")))
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))
	return ids, nil
}

// infoResult is one backup.info read by a readInfos worker.
type infoResult struct {
	backup BaseBackup
	done   bool
	err    error
}

// readInfos reads base/<id>/backup.info for each ID, handing them out in the
// order given to at most surveyParallel workers.
//
// Parameters:
//   - ids are the backup IDs, in the order to read them.
//   - visit is called, from the calling goroutine only, with each DONE
//     backup. It returns true to stop reading.
//
// It returns how many files were read, a missing one included. It returns
// the first GET or parse error, unless visit stopped the reading. When ctx
// ends before every file was read, it returns an *OutOfTimeError carrying
// the counts and the last read that failed for another reason. When it
// returns, every worker has stopped.
func readInfos(ctx context.Context, client *minio.Client, at Location, ids []string, visit func(BaseBackup) bool) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	jobs := make(chan string)
	results := make(chan infoResult, len(ids))
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()

	go func() {
		defer close(jobs)
		for _, id := range ids {
			select {
			case jobs <- id:
			case <-ctx.Done():
				return
			}
		}
	}()
	for range min(surveyParallel, len(ids)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range jobs {
				key := at.BasePrefix() + id + "/backup.info"
				backup, done, err := readBaseBackup(ctx, client, at.Bucket, key)
				if s3Code(err) == "NoSuchKey" {
					err = nil
				}
				results <- infoResult{backup: backup, done: done, err: err}
			}
		}()
	}

	read := 0
	var first, last error
	for read < len(ids) {
		select {
		case r := <-results:
			read++
			if r.err != nil {
				if first == nil {
					first = r.err
				}
				if !errors.Is(r.err, context.DeadlineExceeded) && !errors.Is(r.err, context.Canceled) {
					last = r.err
				}
				continue
			}
			if r.done && visit(r.backup) {
				return read, nil
			}
		case <-ctx.Done():
			return read, &OutOfTimeError{Backups: len(ids), Read: read, Failed: last, Err: ctx.Err()}
		}
	}
	return read, first
}

// s3Code returns the S3 error code anywhere in err's chain, such as NoSuchKey
// or SlowDown, or "" when err carries none.
func s3Code(err error) string {
	var answer minio.ErrorResponse
	if errors.As(err, &answer) {
		return answer.Code
	}
	return ""
}

// s3Answer adds the HTTP status and S3 error code of the store's answer to
// err's message, since minio-go's own message leaves them out, and returns
// err unchanged when it carries no S3 answer. The result wraps err.
func s3Answer(err error) error {
	var answer minio.ErrorResponse
	if !errors.As(err, &answer) || answer.Code == "" {
		return err
	}
	return fmt.Errorf("%w (HTTP %d %s)", err, answer.StatusCode, answer.Code)
}
