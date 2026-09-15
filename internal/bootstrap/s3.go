package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
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

	options := &minio.Options{
		Creds:  credentials.NewStaticV4(at.AccessKey, at.SecretKey, ""),
		Secure: secure,
	}
	if secure {
		transport, err := tlsTransport(at.CABundle)
		if err != nil {
			return false, err
		}
		options.Transport = transport
	}

	client, err := minio.New(endpoint, options)
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

// tlsTransport builds the client's TLS settings: the public roots the image
// ships, plus the store's own endpointCA when it declares one.
//
// Both are needed and neither is assumed. A store fronted by a public
// authority carries no endpointCA and verifies against the roots; one fronted
// by a private authority carries the bundle that signs it. Nothing about
// either is configured on this controller, so an endpoint's CA is described
// once, on the store, where the Barman Cloud plugin reads it too.
func tlsTransport(bundle []byte) (*http.Transport, error) {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		// A system pool that cannot be read is not fatal on its own: a store
		// with an endpointCA needs no public roots at all.
		roots = x509.NewCertPool()
	}
	if len(bundle) > 0 && !roots.AppendCertsFromPEM(bundle) {
		return nil, fmt.Errorf("the endpointCA bundle holds no PEM certificate")
	}

	return &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			MinVersion: tls.VersionTLS12,
		},
	}, nil
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
