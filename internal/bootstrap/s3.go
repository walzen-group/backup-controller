package bootstrap

import (
	"context"
	"fmt"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Prober answers the base backup question against a real object store.
type S3Prober struct{}

// HasBaseBackup reports whether the location holds at least one object under
// its base prefix.
//
// It asks for one object and stops. barman writes a directory per backup, so
// the presence of anything below <server>/base/ is the whole answer, and a
// store holding years of backups is listed no more expensively than an empty
// one.
func (S3Prober) HasBaseBackup(ctx context.Context, at Location) (bool, error) {
	endpoint, secure, err := splitEndpoint(at.Endpoint)
	if err != nil {
		return false, err
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(at.AccessKey, at.SecretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return false, fmt.Errorf("build the object store client: %w", err)
	}

	// MaxKeys stops the server after one object rather than trusting the
	// caller to break out of the channel early, which would leak the
	// goroutine the SDK spawns for the listing.
	listing := client.ListObjects(ctx, at.Bucket, minio.ListObjectsOptions{
		Prefix:  at.BasePrefix(),
		MaxKeys: 1,
	})
	object, ok := <-listing
	if !ok {
		return false, nil
	}
	if object.Err != nil {
		return false, fmt.Errorf("list %s/%s: %w", at.Bucket, at.BasePrefix(), object.Err)
	}
	return true, nil
}

// splitEndpoint turns an endpoint URL into the host:port and TLS flag the
// client takes. A bare host with no scheme is read as HTTPS, which is what
// every endpoint this cluster talks to uses.
func splitEndpoint(endpoint string) (host string, secure bool, err error) {
	if endpoint == "" {
		return "", false, fmt.Errorf("the object store names no endpointURL")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", false, fmt.Errorf("parse endpointURL %q: %w", endpoint, err)
	}
	switch parsed.Scheme {
	case "":
		return endpoint, true, nil
	case "https":
		return parsed.Host, true, nil
	case "http":
		return parsed.Host, false, nil
	default:
		return "", false, fmt.Errorf("endpointURL %q has scheme %q", endpoint, parsed.Scheme)
	}
}
