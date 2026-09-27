package bootstrap

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fault"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}
	client := newBuilder(t).WithObjects(secret()).
		WithRuntimeObjects(storeAt(endpoint)).Build()
	decider := &Decider{Client: client, Prober: S3Prober{}, Budget: budget}
	return decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
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

// v081BaseBackups is the v0.8.1 reader, kept as the reference the new one
// must agree with: a recursive listing of base/, every backup.info read in
// key order, DONE ones sorted by end time.
func v081BaseBackups(t *testing.T, at Location) []BaseBackup {
	t.Helper()
	client, err := S3Prober{}.client(at)
	if err != nil {
		t.Fatal(err)
	}
	var backups []BaseBackup
	for object := range client.ListObjects(context.Background(), at.Bucket, minio.ListObjectsOptions{Prefix: at.BasePrefix(), Recursive: true}) {
		if object.Err != nil {
			t.Fatal(object.Err)
		}
		if !strings.HasSuffix(object.Key, "/backup.info") {
			continue
		}
		backup, done, err := readBaseBackup(context.Background(), client, at.Bucket, object.Key)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			backups = append(backups, backup)
		}
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].End.Before(backups[j].End) })
	return backups
}

// TestTheNewReaderAgreesWithTheOldOnTheRecordedStores checks, for every
// recorded store and each server in it, that BaseBackups returns what the
// v0.8.1 reader did and that Survey finds a DONE backup exactly when that
// list is not empty, is Empty exactly when the recorded barman-cloud-check-wal-archive passed,
// and reports the oldest DONE backup when asked for a target before it.
func TestTheNewReaderAgreesWithTheOldOnTheRecordedStores(t *testing.T) {
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		recorded := barmanstore.MustLoad(t, name)
		server := recordedS3(t, recorded)
		servers := map[string]bool{"app-pg": true}
		for s := range recorded.Backups() {
			servers[s] = true
		}
		for serverName := range servers {
			t.Run(name+"/"+serverName, func(t *testing.T) {
				at := Location{Endpoint: server.URL, Bucket: "backups", Prefix: "app/" + serverName,
					AccessKey: server.AccessKey, SecretKey: server.SecretKey}
				want := v081BaseBackups(t, at)

				got, err := S3Prober{}.BaseBackups(context.Background(), at)
				if err != nil {
					t.Fatalf("BaseBackups: %v", err)
				}
				if len(got) != 0 || len(want) != 0 {
					if !reflect.DeepEqual(got, want) {
						t.Errorf("BaseBackups = %v, the v0.8.1 reader gives %v", got, want)
					}
				}

				archive, err := S3Prober{}.Survey(context.Background(), at, nil)
				if err != nil {
					t.Fatalf("Survey: %v", err)
				}
				if (archive.Found != nil) != (len(want) > 0) {
					t.Errorf("Survey found %v, the v0.8.1 reader lists %d DONE backups", archive.Found, len(want))
				}
				if verdict, ok := recorded.Verdicts[serverName]; ok {
					if passes := verdict.CheckWalArchive.ExitCode == 0; archive.Empty != passes {
						t.Errorf("Survey Empty = %v, barman-cloud-check-wal-archive exit code %d", archive.Empty, verdict.CheckWalArchive.ExitCode)
					}
				}

				if len(want) == 0 {
					return
				}
				before := want[0].End.Add(-time.Second)
				early, err := S3Prober{}.Survey(context.Background(), at, &before)
				if err != nil {
					t.Fatalf("Survey with a target: %v", err)
				}
				if early.Found != nil || early.Oldest == nil || *early.Oldest != want[0] {
					t.Errorf("Survey before the oldest backup = found %v, oldest %v, want none found and oldest %v", early.Found, early.Oldest, want[0])
				}
				last := want[len(want)-1].End
				late, err := S3Prober{}.Survey(context.Background(), at, &last)
				if err != nil {
					t.Fatalf("Survey with a target: %v", err)
				}
				if late.Found == nil {
					t.Errorf("Survey at the newest backup's end found nothing")
				}
			})
		}
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
	proxy.Add(s3fault.Rule{Match: s3fault.Match{Methods: []string{"GET"}, KeySuffix: ids[0] + "/backup.info"}, Status: 500})

	response := decideWithin(t, cluster(t, nil), endpoint, 3*time.Second)

	if response.Allowed {
		t.Fatalf("the cluster was admitted: %v", response.Patches)
	}
}
