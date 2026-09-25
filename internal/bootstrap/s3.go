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

// S3Prober is the Prober that talks to a real S3-compatible object store
// through minio-go. It keeps no state. Each call builds a client from the
// Location it's given.
type S3Prober struct{}

// HasBaseBackup reports whether the location holds at least one object under
// its base prefix. The webhook calls it to decide between recovering a new
// Cluster and leaving it on initdb.
//
// The at argument is the database's Location, as ResolveLocation returns it.
//
// It asks the store for one object and stops. barman writes a directory per
// backup, so anything below <server>/base/ answers the question, and a store
// holding years of backups is as quick to check as an empty one. It returns an
// error when the client can't be built from the location or the listing
// fails.
func (p S3Prober) HasBaseBackup(ctx context.Context, at Location) (bool, error) {
	client, err := p.client(at)
	if err != nil {
		return false, err
	}

	// MaxKeys makes the server stop after one object. Breaking out of the
	// channel early would leak the goroutine the SDK starts for the listing.
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

// client builds a minio client for the location's endpoint and credentials.
// For an HTTPS endpoint it trusts the public roots plus the location's
// CABundle (see tlsTransport). For an http:// endpoint it uses minio's default
// transport. It returns an error when the endpoint is empty or malformed, when
// the CABundle holds no certificate, or when minio rejects the options.
func (S3Prober) client(at Location) (*minio.Client, error) {
	endpoint, secure, err := splitEndpoint(at.Endpoint)
	if err != nil {
		return nil, err
	}

	options := &minio.Options{
		Creds:  credentials.NewStaticV4(at.AccessKey, at.SecretKey, ""),
		Secure: secure,
	}
	if secure {
		transport, err := tlsTransport(at.CABundle)
		if err != nil {
			return nil, err
		}
		options.Transport = transport
	}

	client, err := minio.New(endpoint, options)
	if err != nil {
		return nil, fmt.Errorf("build the object store client: %w", err)
	}
	return client, nil
}

// tlsTransport builds the HTTP transport for an HTTPS endpoint. It trusts the
// public roots the image ships, plus the certificates in the bundle argument.
// The bundle is the store's endpointCA and may be empty. The transport
// requires TLS 1.2 or newer.
//
// A store fronted by a public authority carries no endpointCA and verifies
// against the public roots. A store fronted by a private authority carries the
// bundle that signs it. This controller has no CA settings of its own, so an
// endpoint's CA is described once, on the store, where the Barman Cloud plugin
// reads it too.
//
// It returns an error when the bundle is non-empty and holds no PEM
// certificate.
func tlsTransport(bundle []byte) (*http.Transport, error) {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		// Carry on with an empty pool when the system pool can't be read. A
		// store with an endpointCA needs no public roots at all.
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

// splitEndpoint turns an endpointURL into the host (with its port, when it has
// one) and the TLS flag that minio.New takes. An https:// URL gives TLS and an
// http:// URL gives plain HTTP. A bare host name such as s3.example.com is
// read as HTTPS, which is what every endpoint this cluster talks to uses. A
// bare host with a port, such as s3.example.com:9000, doesn't work: url.Parse
// reads the host name as a scheme, and the function returns an error for it.
// It also returns an error for an empty endpoint, an unparsable one, and any
// other scheme.
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
