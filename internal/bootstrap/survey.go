package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
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
	// Empty is true when <server>/wals/ holds no WAL file (see walsEmpty).
	// Other objects under the server prefix, such as failed base backups, do
	// not count.
	Empty bool
	// Found is a complete base backup, one that finished at or before the
	// target when a target was given. It is nil when none qualifies.
	Found *BaseBackup
	// Backups counts the base backup directories under base/, in any state.
	Backups int
	// Read counts the backup.info files read, a missing one included.
	Read int
	// Oldest is the complete backup that finished first. It is set only when
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
// exists under its prefix, and whether a complete base backup exists that a
// recovery can start from. A complete backup is one whose backup.info sets
// begin_time and end_time (see ParseBackupInfo).
//
// Parameters:
//   - at is the database's Location, as ResolveLocation returns it.
//   - target, when not nil, asks for a complete backup that finished at or
//     before it.
//
// It lists base/ with the "/" delimiter, one entry per backup as barman's
// own catalog does, and reads the backup.info files newest first with at
// most surveyParallel GETs in flight. It stops at the first qualifying complete
// backup; which worker finds it does not matter, since only existence
// counts. A missing backup.info counts as not complete, as it does for barman.
// It fills in Empty with walsEmpty.
//
// It returns an *OutOfTimeError when ctx ends before an answer, during a
// listing too, so an expired ctx never reads as an empty prefix. It returns
// an error when the client can't be built, when a listing fails, or when a
// GET failed and no qualifying backup was found, since the unread file might
// have been the complete one.
func (p S3Prober) Survey(ctx context.Context, at Location, target *time.Time) (Archive, error) {
	client, err := p.client(at)
	if err != nil {
		return Archive{}, err
	}
	ids, err := backupIDs(ctx, client, at)
	if err != nil {
		return Archive{}, outOfTime(ctx, err, 0, 0)
	}
	empty, err := walsEmpty(ctx, client, at)
	if err != nil {
		return Archive{}, err
	}

	if len(ids) == 0 {
		return Archive{Empty: empty}, nil
	}

	q := &qualifier{target: target}
	read, err := readInfos(ctx, client, at, ids, q.visit)
	out := Archive{Empty: empty, Backups: len(ids), Read: read, Found: q.found}
	if out.Found != nil {
		return out, nil
	}
	if err != nil {
		return Archive{}, outOfTime(ctx, err, len(ids), read)
	}
	out.Oldest = q.oldest
	return out, nil
}

// walsEmpty tells Survey if <server>/wals/ of a Location holds no WAL file.
// This is the test of barman-cloud-check-wal-archive, which CloudNativePG
// runs before a new database archives. It lists <server>/wals/ with no
// delimiter and fails on any WAL file (barman 3.20.0
// clients/cloud_check_wal_archive.py:60-64, cloud.py:2426-2446,
// xlog.py:572-574). walFile tells which keys are WAL files.
//
// It returns true when the listing holds no WAL file, and stops at the first
// one. It returns an *OutOfTimeError when ctx ends before the answer, and an
// error when the listing fails.
func walsEmpty(ctx context.Context, client *minio.Client, at Location) (bool, error) {
	prefix := at.ServerPrefix() + "wals/"
	for object := range client.ListObjectsIter(ctx, at.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return false, outOfTime(ctx, fmt.Errorf("list %s/%s: %w", at.Bucket, prefix, s3Answer(object.Err)), 0, 0)
		}
		if walFile(object.Key) {
			return false, nil
		}
	}
	// minio-go ends the listing with no error item when ctx ends, so
	// finding nothing proves the prefix empty only while ctx is live.
	if ctx.Err() != nil {
		return false, outOfTime(ctx, ctx.Err(), 0, 0)
	}
	return true, nil
}

// qualifier keeps what Survey finds in the complete backups that readInfos
// gives to visit.
type qualifier struct {
	// target is the moment that a backup must finish by, or nil for any
	// complete backup.
	target *time.Time
	// found is the first complete backup that finished by target.
	found *BaseBackup
	// oldest is the complete backup with the earliest end among those that
	// finished after target.
	oldest *BaseBackup
}

// visit keeps b as found and returns true when b finished by the target.
// Otherwise it keeps b as oldest when b ended before the oldest so far, and
// returns false.
func (q *qualifier) visit(b BaseBackup) bool {
	if q.target == nil || !b.End.After(*q.target) {
		found := b
		q.found = &found
		return true
	}
	if q.oldest == nil || b.End.Before(q.oldest.End) {
		first := b
		q.oldest = &first
	}
	return false
}

// BaseBackups lists the completed base backups of one database, oldest
// first. The RestoreRun controller uses it to pick the backup a restore
// starts from.
//
// The at argument is the database's Location, as ResolveLocation returns it.
//
// It reads the catalog as Survey does, the delimited listing of base/ and
// backup.info files with surveyParallel GETs in flight, but reads every one.
// A missing backup.info counts as not complete. It returns an error when the
// listing fails, when a backup.info can't be downloaded or read, or when a
// complete backup's end_time doesn't parse, and one wrapping ctx's error when
// ctx ends before every file was listed and read. A location with no
// completed backup gives an empty list and no error.
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
// every page before it returns. It returns an error when the listing fails,
// and one wrapping ctx's error when ctx ended during the listing, since
// minio-go then stops without saying so and the IDs read are not all.
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
	// minio-go ends the listing with no error item when ctx is done, before
	// the first page or between two, so the IDs so far may be none or some.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list %s/%s: the listing stopped early: %w", at.Bucket, at.BasePrefix(), err)
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
//   - visit is called, from the calling goroutine only, with each complete
//     backup. It returns true to stop reading.
//
// It returns how many files were read, a missing one included. It returns
// the first GET or parse error, unless visit stopped the reading. When ctx
// ends before every file was read, it returns an *OutOfTimeError carrying
// the counts and the last read that failed for another reason. When it
// returns, every worker has stopped.
func readInfos(ctx context.Context, client *minio.Client, at Location, ids []string, visit func(BaseBackup) bool) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan infoResult, len(ids))
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()

	jobs := feedIDs(ctx, ids)
	for range min(surveyParallel, len(ids)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			readInfoJobs(ctx, client, at, jobs, results)
		}()
	}
	return collectInfos(ctx, results, len(ids), visit)
}

// feedIDs sends each ID on the channel that it returns, in the order given,
// and closes the channel after the last ID. It stops early when ctx ends.
func feedIDs(ctx context.Context, ids []string) <-chan string {
	jobs := make(chan string)
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
	return jobs
}

// readInfoJobs is one readInfos worker. It reads the backup.info of each ID
// from jobs and sends the result to results. A missing backup.info is a
// result with no error that is not complete.
func readInfoJobs(ctx context.Context, client *minio.Client, at Location, jobs <-chan string, results chan<- infoResult) {
	for id := range jobs {
		key := at.BasePrefix() + id + "/backup.info"
		backup, done, err := readBaseBackup(ctx, client, at.Bucket, key)
		if s3Code(err) == "NoSuchKey" {
			err = nil
		}
		results <- infoResult{backup: backup, done: done, err: err}
	}
}

// collectInfos takes the results of the readInfos workers and gives each
// complete backup to visit.
//
// Parameters:
//   - results carries the result of each read.
//   - total is the number of IDs, which is the number of results to wait
//     for.
//   - visit is the function that readInfos got.
//
// It returns what readInfos returns (see readInfos).
func collectInfos(ctx context.Context, results <-chan infoResult, total int, visit func(BaseBackup) bool) (int, error) {
	read := 0
	var failed readErrors
	for read < total {
		select {
		case r := <-results:
			read++
			if r.err != nil {
				failed.add(r.err)
				continue
			}
			if r.done && visit(r.backup) {
				return read, nil
			}
		case <-ctx.Done():
			return read, &OutOfTimeError{Backups: total, Read: read, Failed: failed.last, Err: ctx.Err()}
		}
	}
	return read, failed.first
}

// readErrors keeps the errors of the backup.info reads for collectInfos.
type readErrors struct {
	// first is the first error of any kind.
	first error
	// last is the last error that is not the end of the context.
	last error
}

// add keeps err as first when no error came before it, and as last when
// err is not a context deadline or cancel.
func (e *readErrors) add(err error) {
	if e.first == nil {
		e.first = err
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		e.last = err
	}
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

// walName is barman's pattern for a file in a WAL archive: a WAL segment,
// optionally with a backup label offset or .partial, or a timeline history
// file (barman 3.20.0 xlog.py:39-56).
var walName = regexp.MustCompile(`^[0-9A-Fa-f]{8}(?:[0-9A-Fa-f]{8}[0-9A-Fa-f]{8}(?:\.[0-9A-Fa-f]{8}\.backup|\.partial)?|\.history)$`)

// walCompressions are the suffixes barman allows on a WAL file name
// (barman 3.20.0 cloud.py:90-97).
var walCompressions = map[string]bool{".gz": true, ".bz2": true, ".xz": true, ".snappy": true, ".zst": true, ".lz4": true}

// walFile tells if an object key under wals/ is a WAL file by barman's rule
// (barman 3.20.0 cloud.py:2426-2446): the base name matches walName, or it
// matches walName after one allowed compression suffix is removed.
func walFile(key string) bool {
	base := path.Base(key)
	if walName.MatchString(base) {
		return true
	}
	ext := path.Ext(base)
	return walCompressions[ext] && walName.MatchString(strings.TrimSuffix(base, ext))
}
