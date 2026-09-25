package bootstrap

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fault"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// recordedS3 starts an s3fake server, which stands in for RustFS 1.0.0,
// holding one barman store under s3://backups/app/, where the ObjectStore from
// store() archives. The store is one that barman-cloud 3.20.0 recorded,
// optionally changed by the barmanstore builders. The server accepts the
// credentials that secret() holds, so S3Prober reads it through storeAt or
// decideOn. See the s3fake package for what the server models and leaves out.
// The server stops when the test ends.
func recordedS3(t *testing.T, recorded barmanstore.Store) *s3fake.Server {
	t.Helper()
	server := newFakeS3(t, "backups")
	if err := recorded.Upload(server, "backups", "app"); err != nil {
		t.Fatalf("upload the recorded store %s: %v", recorded.Store, err)
	}
	return server
}

// newFakeS3 starts an empty s3fake server with the given bucket and the
// credentials of secret().
func newFakeS3(t *testing.T, bucket string) *s3fake.Server {
	t.Helper()
	server := s3fake.New(t, bucket)
	server.AccessKey = string(secret().Data["ACCESS_KEY_ID"])
	server.SecretKey = string(secret().Data["ACCESS_SECRET_KEY"])
	return server
}

// faultyS3 puts an s3fault proxy in front of the server and returns the
// proxy's URL and the proxy, so a test adds the rules it needs. The proxy
// stops when the test ends.
func faultyS3(t *testing.T, server *s3fake.Server) (string, *s3fault.Proxy) {
	t.Helper()
	proxy, err := s3fault.New(server.URL)
	if err != nil {
		t.Fatalf("build the fault proxy: %v", err)
	}
	front := httptest.NewServer(proxy)
	t.Cleanup(front.Close)
	return front.URL, proxy
}

// storeAt builds the ObjectStore from store() with its endpointURL set to the
// given URL, so ResolveLocation points S3Prober at a fake S3 server.
func storeAt(endpoint string) *unstructured.Unstructured {
	s := store()
	_ = unstructured.SetNestedField(s.Object, endpoint, "spec", "configuration", "endpointURL")
	return s
}

// decideOn sends a create of the Cluster c to a Decider whose real S3Prober
// reads the given fake S3 server, with any extra objects in existing. It is
// decideWith for the cases whose outcome depends on what the store holds.
func decideOn(t *testing.T, c *unstructured.Unstructured, server *s3fake.Server, existing ...runtime.Object) admission.Response {
	t.Helper()
	return decideWith(t, c, S3Prober{}, storeAt(server.URL), existing...)
}

// sunday is 03:00 UTC on Sunday 2026-09-20, the day before the RestoreRun
// cases' Monday 2026-09-21, which is where they want the completed base
// backup to start.
var sunday = time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC)

// sundayID is barman's backup ID for a backup started at sunday.
const sundayID = "20260920T030000"

// recordedAt loads the recorded store name and shifts it in time so that the
// newest base backup of app-pg starts at start. In done-base and many-failed
// the newest backup is the completed one, so its ID becomes start in barman's
// form and it ends about a second later. The test fails when the store can't
// be loaded or holds no base backup for app-pg.
func recordedAt(t *testing.T, name string, start time.Time) barmanstore.Store {
	t.Helper()
	recorded := barmanstore.MustLoad(t, name)
	ids := recorded.Backups()["app-pg"]
	if len(ids) == 0 {
		t.Fatalf("the recorded store %s holds no base backup for app-pg", name)
	}
	newest, err := time.Parse("20060102T150405", ids[len(ids)-1])
	if err != nil {
		t.Fatalf("recorded backup ID %s: %v", ids[len(ids)-1], err)
	}
	return recorded.Shift(start.Sub(newest))
}
