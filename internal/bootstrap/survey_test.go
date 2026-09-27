package bootstrap

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fault"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// slowInfo is the latency the audit measured against: 100 ms for every GET
// of a backup.info. At that speed the v0.8.1 reader, which read the files
// one at a time oldest first, needed 16 s for the many-failed store and ran
// past the webhook's 15 s timeout.
var slowInfo = s3fault.Rule{
	Match:   s3fault.Match{Methods: []string{"GET"}, KeySuffix: "/backup.info"},
	Latency: 100 * time.Millisecond,
}

// decideWithin is decideWith against the endpoint for the real S3Prober,
// with the Decider's budget set to budget (zero keeps the default).
func decideWithin(t *testing.T, c *unstructured.Unstructured, endpoint string, budget time.Duration) admission.Response {
	t.Helper()
	client := newBuilder(t).WithObjects(secret()).
		WithRuntimeObjects(storeAt(endpoint)).Build()
	decider := &Decider{Client: client, Prober: S3Prober{}, Budget: budget}
	return create(t, decider, c)
}

// TestFailedBackupsBeforeADoneOneDoNotRunTheWebhookOutOfTime checks the
// recorded many-failed store (160 FAILED base backups, then a DONE one)
// through the fault proxy at 100 ms per backup.info GET. Finding W2,
// designs/webhook.md B. The Cluster has to be rewritten to recover well
// within the budget, reading at most two batches of backup.info files.
func TestFailedBackupsBeforeADoneOneDoNotRunTheWebhookOutOfTime(t *testing.T) {
	endpoint, proxy := faultyS3(t, recordedS3(t, barmanstore.MustLoad(t, "many-failed")))
	gets := proxy.Add(slowInfo)

	original := cluster(t, nil)
	start := time.Now()
	response := decideWithin(t, original, endpoint, 0)
	elapsed := time.Since(start)

	if !response.Allowed {
		t.Fatalf("the cluster was refused after %s: %v", elapsed, response.Result)
	}
	source, _, _ := unstructured.NestedString(applied(t, original, response), "spec", "bootstrap", "recovery", "source")
	if source != RecoverySource {
		t.Errorf("recovery source = %q, want %q", source, RecoverySource)
	}
	if elapsed > 3*time.Second {
		t.Errorf("the webhook took %s over 160 failed backups, want under 3s", elapsed)
	}
	// A worker may start its next GET before the stop reaches it, so each of
	// the surveyParallel workers can have begun one more.
	if gets.Hits() > 2*surveyParallel {
		t.Errorf("the webhook read %d backup.info files, want at most %d", gets.Hits(), 2*surveyParallel)
	}
}

// withoutNewest returns the recorded store with the newest base backup of
// app-pg removed, so many-failed holds only its 160 FAILED backups and WAL.
func withoutNewest(t *testing.T, recorded barmanstore.Store) barmanstore.Store {
	t.Helper()
	ids := recorded.Backups()["app-pg"]
	gone := "app-pg/base/" + ids[len(ids)-1] + "/"
	out := barmanstore.Store{Manifest: barmanstore.Manifest{Store: recorded.Store + " without its newest backup"}}
	for _, o := range recorded.Objects {
		if !strings.HasPrefix(o.Key, gone) {
			out.Objects = append(out.Objects, o)
		}
	}
	return out
}

// TestTheBudgetRefusesWithAMessage checks that a store whose backup.info
// files can't all be read in the budget is refused, within the budget, with a
// message naming the counts and the barman command that clears failed
// backups. The budget is lowered to 1 s so 160 FAILED backups at 100 ms per
// GET, 8 at a time (2 s), run out of it.
func TestTheBudgetRefusesWithAMessage(t *testing.T) {
	endpoint, proxy := faultyS3(t, recordedS3(t, withoutNewest(t, barmanstore.MustLoad(t, "many-failed"))))
	proxy.Add(slowInfo)

	start := time.Now()
	response := decideWithin(t, cluster(t, nil), endpoint, time.Second)
	elapsed := time.Since(start)

	if response.Allowed {
		t.Fatalf("the cluster was admitted: %v", response.Patches)
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("the refusal took %s, want within the 1s budget plus 0.5s", elapsed)
	}
	message := response.Result.Message
	for _, want := range []string{"s3://backups/app/app-pg/base/", "160 base backups", "barman-cloud-backup-delete --backup-id", "ran out of time"} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal %q does not say %q", message, want)
		}
	}
	if response.Result.Code != 403 {
		t.Errorf("code = %d, want a denial (403)", response.Result.Code)
	}
}

// TestTheBudgetFitsTheManifest checks that the budget leaves 5 s of the
// webhook's timeoutSeconds in deploy/webhook.yaml for TLS, the API server and
// the Kubernetes reads.
func TestTheBudgetFitsTheManifest(t *testing.T) {
	if DefaultBudget+5*time.Second > 15*time.Second {
		t.Errorf("DefaultBudget %s leaves under 5s of the 15s webhook timeout", DefaultBudget)
	}
}

// TestAFailedGetBesideADoneBackupStillRecovers checks that a backup.info the
// store answers with 500 does not stop the webhook from recovering from a
// DONE backup elsewhere, since that backup is proof enough. minio-go retries
// a 500 itself, so the failing GET is still retrying when the DONE one
// answers, and the survey stops it.
func TestAFailedGetBesideADoneBackupStillRecovers(t *testing.T) {
	recorded := barmanstore.MustLoad(t, "many-failed")
	ids := recorded.Backups()["app-pg"]
	endpoint, proxy := faultyS3(t, recordedS3(t, recorded))
	proxy.Add(s3fault.Rule{Match: s3fault.Match{Methods: []string{"GET"}, KeySuffix: ids[len(ids)-2] + "/backup.info"}, Status: 500})

	original := cluster(t, nil)
	response := decideWithin(t, original, endpoint, 0)

	if !response.Allowed || len(response.Patches) == 0 {
		t.Fatalf("the cluster was not recovered: %v", response.Result)
	}
}

// TestAFailedGetWithNoDoneBackupIsRefused checks that a store whose one
// unreadable backup.info might have been the DONE one is refused, never
// started empty: the unread file might have been the only DONE backup.
func TestAFailedGetWithNoDoneBackupIsRefused(t *testing.T) {
	recorded := withoutNewest(t, barmanstore.MustLoad(t, "many-failed"))
	ids := recorded.Backups()["app-pg"]
	endpoint, proxy := faultyS3(t, recordedS3(t, recorded))
	proxy.Add(s3fault.Rule{Match: s3fault.Match{Methods: []string{"GET"}, KeySuffix: ids[0] + "/backup.info"}, Status: http.StatusForbidden, Code: "AccessDenied"})

	response := decideWithin(t, cluster(t, nil), endpoint, 3*time.Second)

	if response.Allowed {
		t.Fatalf("the cluster was admitted: %v", response.Patches)
	}
}

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

// expired returns a context whose deadline has already passed.
func expired(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

// doneBaseAt serves the recorded done-base store and returns the Location of
// app-pg in it.
func doneBaseAt(t *testing.T) Location {
	t.Helper()
	server := recordedS3(t, barmanstore.MustLoad(t, "done-base"))
	return Location{Endpoint: server.URL, Bucket: "backups", Prefix: "app/app-pg",
		AccessKey: server.AccessKey, SecretKey: server.SecretKey}
}

// TestAnExpiredContextNeverListsNoBaseBackups checks BaseBackups on a context
// that is already done, against the recorded done-base store. The RestoreRun
// controller reads an empty list as "no completed backup", so the silent end
// of minio-go's listing must come back as an error. Finding W5.
func TestAnExpiredContextNeverListsNoBaseBackups(t *testing.T) {
	at := doneBaseAt(t)

	backups, err := S3Prober{}.BaseBackups(expired(t), at)

	if err == nil {
		t.Fatalf("BaseBackups on an expired context gave no error and %d backups", len(backups))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

// atTheDeadline answers a list by doing it, then waiting for the request's
// context to end and returning success, the way a Kubernetes read that
// finishes just as the budget runs out leaves the caller an expired context
// and no error.
func atTheDeadline(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
	if err := c.List(ctx, list, opts...); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

// TestAReadThatEndsAtTheDeadlineNeverAdmitsInitdbOverAnArchive checks that a
// Kubernetes list which succeeds just as the budget runs out does not let
// the webhook admit a Cluster to initdb over the recorded done-base store.
// Survey then runs on an expired context, and minio-go's listing ends
// silently. Before the fix the main path admitted with no patch ("no base
// backup") and the opt-out path admitted "opted out". Finding W5.
func TestAReadThatEndsAtTheDeadlineNeverAdmitsInitdbOverAnArchive(t *testing.T) {
	cases := []struct {
		name    string
		cluster func(t *testing.T) *unstructured.Unstructured
		kind    string
	}{{
		name:    "the RestoreRun list on the main path",
		cluster: func(t *testing.T) *unstructured.Unstructured { return cluster(t, nil) },
		kind:    "RestoreRunList",
	}, {
		name:    "the Cluster list on the opt-out path",
		cluster: optedOut,
		kind:    ClusterListGVK.Kind,
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := recordedS3(t, barmanstore.MustLoad(t, "done-base"))
			kind := tc.kind
			c := newBuilder(t).
				WithObjects(secret()).
				WithRuntimeObjects(storeAt(server.URL)).
				WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					gvk, err := c.GroupVersionKindFor(list)
					if err != nil {
						return err
					}
					if gvk.Kind == kind {
						return atTheDeadline(ctx, c, list, opts...)
					}
					return c.List(ctx, list, opts...)
				}}).Build()
			decider := &Decider{Client: c, Prober: S3Prober{}, Budget: 300 * time.Millisecond}

			response := create(t, decider, tc.cluster(t))

			if response.Allowed {
				t.Fatalf("the cluster was admitted over the done-base archive: patches = %d, result = %v", len(response.Patches), response.Result)
			}
		})
	}
}

// expiresAfterNextCheck is a context that ends right after the first Err
// call made once it is armed. That call still sees the context live; every
// later one sees it canceled. It models a deadline that passes between a
// check of the context and the next use of it, which a real deadline hits
// only by chance.
type expiresAfterNextCheck struct {
	context.Context
	cancel context.CancelFunc
	armed  atomic.Bool
	fired  atomic.Bool
}

// Err returns nil and cancels the context on the first call after arming,
// and the embedded context's error otherwise.
func (c *expiresAfterNextCheck) Err() error {
	if c.armed.Load() && c.fired.CompareAndSwap(false, true) {
		c.cancel()
		return nil
	}
	return c.Context.Err()
}

// armOnBaseListing passes every request to next, and arms ctx when the
// client closes the answer to the listing of base/. minio-go closes that
// answer before backupIDs checks the context, so the context ends right
// after that check and before the next listing starts.
type armOnBaseListing struct {
	next http.RoundTripper
	ctx  *expiresAfterNextCheck
}

// RoundTrip serves one request as the type's comment says.
func (a armOnBaseListing) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := a.next.RoundTrip(r)
	query := r.URL.Query()
	if err != nil || query.Get("list-type") != "2" || !strings.HasSuffix(query.Get("prefix"), "/base/") {
		return response, err
	}
	response.Body = armOnClose{ReadCloser: response.Body, ctx: a.ctx}
	return response, nil
}

// armOnClose is a response body that arms ctx when it is closed.
type armOnClose struct {
	io.ReadCloser
	ctx *expiresAfterNextCheck
}

// Close closes the body and arms ctx.
func (a armOnClose) Close() error {
	err := a.ReadCloser.Close()
	a.ctx.armed.Store(true)
	return err
}

// TestAContextThatEndsAfterTheBaseListingNeverReadsAsAnEmptyPrefix checks
// Survey on the recorded wal-only store, whose prefix holds WAL and no
// base/, when the context ends after backupIDs has returned and before the
// listing of <server>/wals/. minio-go then ends that listing
// with no item and no error, and a Survey that took the silence as an empty
// prefix would let the webhook admit initdb into a prefix that holds WAL.
// minio.DefaultTransport is swapped for the test so the client minio.New
// builds for the http endpoint goes through armOnBaseListing. Finding W7.
func TestAContextThatEndsAfterTheBaseListingNeverReadsAsAnEmptyPrefix(t *testing.T) {
	server := recordedS3(t, barmanstore.MustLoad(t, "wal-only"))
	at := Location{Endpoint: server.URL, Bucket: "backups", Prefix: "app/app-pg",
		AccessKey: server.AccessKey, SecretKey: server.SecretKey}

	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx := &expiresAfterNextCheck{Context: parent, cancel: cancel}

	original := minio.DefaultTransport
	t.Cleanup(func() { minio.DefaultTransport = original })
	minio.DefaultTransport = func(_ bool) (*http.Transport, error) {
		transport := &http.Transport{}
		transport.RegisterProtocol("http", armOnBaseListing{next: http.DefaultTransport, ctx: ctx})
		return transport, nil
	}

	archive, err := S3Prober{}.Survey(ctx, at, nil)

	if !ctx.fired.Load() {
		t.Fatalf("the context never ended after the base/ listing, so the test checked nothing (err = %v)", err)
	}
	if err == nil {
		t.Fatalf("Survey gave no error after the context ended: empty = %v, backups = %d", archive.Empty, archive.Backups)
	}
	if archive.Empty {
		t.Errorf("Survey read the wal-only prefix as empty")
	}
	var late *OutOfTimeError
	if !errors.As(err, &late) {
		t.Errorf("err = %v, want an *OutOfTimeError", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled", err)
	}
}
