// Command s3tool is the S3 side of hack/fixtures/barman-stores.sh. It creates
// buckets, runs the fault proxy in front of RustFS, and writes out a recorded
// store: its manifest (every object, with the bodies of the files the
// controller reads) and a transcript of the requests the controller's S3
// reader makes against it, with RustFS's answers.
//
// It is a fixture recorder, run by make fixtures-barman; nothing in the
// controller imports it. Credentials come from AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY.
//
// Usage:
//
//	s3tool mb <endpoint> <bucket>
//	s3tool proxy <listen> <upstream> <bucket> <key prefix>
//	s3tool manifest <endpoint> <bucket> <prefix> <out.json>
//	s3tool transcript <endpoint> <bucket> <prefix> <server> <out.json>
//	s3tool behaviour <endpoint> <bucket> <out.json>
//	s3tool wait-key <endpoint> <bucket> <prefix> <suffix>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fault"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "s3tool:", err)
		os.Exit(1)
	}
}

// run dispatches one subcommand. It returns an error for an unknown or
// malformed command and for any failure of the command itself.
func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command")
	}
	ctx := context.Background()
	switch {
	case args[0] == "mb" && len(args) == 3:
		c, err := client(args[1], os.Getenv("AWS_ACCESS_KEY_ID"))
		if err != nil {
			return err
		}
		return c.MakeBucket(ctx, args[2], minio.MakeBucketOptions{})
	case args[0] == "proxy" && len(args) == 5:
		return proxy(args[1], args[2], args[3], args[4])
	case args[0] == "manifest" && len(args) == 5:
		return writeManifest(ctx, args[1], args[2], args[3], args[4])
	case args[0] == "transcript" && len(args) == 6:
		return writeTranscript(ctx, args[1], args[2], args[3], args[4], args[5])
	case args[0] == "behaviour" && len(args) == 4:
		return writeBehaviour(ctx, args[1], args[2], args[3])
	case args[0] == "wait-key" && len(args) == 5:
		return waitKey(ctx, args[1], args[2], args[3], args[4])
	default:
		return fmt.Errorf("unknown command %q with %d arguments", args[0], len(args)-1)
	}
}

// client builds a minio client for an http endpoint (host:port) with the
// given access key and the secret key from the environment.
func client(endpoint, accessKey string) (*minio.Client, error) {
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
		Secure: false,
	})
}

// proxy serves the s3fault proxy on listen in front of upstream, refusing with
// AccessDenied every write under the key prefix in the bucket except the
// writes of backup.info. barman-cloud-backup then fails while it uploads the
// data and records the failure in a backup.info of status FAILED, the way it
// does when the store refuses it partway through a real backup.
func proxy(listen, upstream, bucket, prefix string) error {
	p, err := s3fault.New(upstream)
	if err != nil {
		return err
	}
	p.Add(s3fault.Rule{
		Match:  s3fault.Match{Methods: []string{http.MethodPut, http.MethodPost}, Bucket: bucket, KeyPrefix: prefix, ExceptKeySuffix: "backup.info"},
		Status: http.StatusForbidden,
		Code:   "AccessDenied",
	})
	return http.ListenAndServe(listen, p) //nolint:gosec // a local recorder's proxy, bound to loopback by its caller.
}

// waitKey polls the bucket every 20 ms until an object under prefix whose key
// ends in suffix exists, and gives up after two minutes. The recorder uses it
// to kill barman-cloud-backup right after the backup.info of status STARTED
// lands.
func waitKey(ctx context.Context, endpoint, bucket, prefix, suffix string) error {
	c, err := client(endpoint, os.Getenv("AWS_ACCESS_KEY_ID"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		for info := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if info.Err == nil && strings.HasSuffix(info.Key, suffix) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("no key under %s ending in %s: %w", prefix, suffix, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// writeJSON writes v as indented JSON to the file at path.
func writeJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644) //nolint:gosec // a checked-in fixture, readable by everyone.
}
