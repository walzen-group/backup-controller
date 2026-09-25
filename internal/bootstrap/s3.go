package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Prober is the Prober that talks to a real S3-compatible object store
// through minio-go. It keeps no state. Each call builds a client from the
// Location it's given.
type S3Prober struct{}

// HasBaseBackup reports whether the location holds at least one completed base
// backup, one whose backup.info has status DONE. The webhook calls it to
// decide between recovering a new Cluster and leaving it as written.
//
// The at argument is the database's Location, as ResolveLocation returns it.
//
// barman writes a backup's directory under <server>/base/ when the backup
// starts, so a backup that failed or never finished leaves objects there too,
// and a recovery can't start from one. The method walks the listing in key
// order, reads each backup.info it meets, and stops at the first DONE one.
// barman names each directory by its start time, so the oldest backup is read
// first, and in a store that keeps its backups that one is done. It returns an
// error when the client can't be built from the location, when the listing
// fails, or when a backup.info can't be read or a DONE one has an unparsable
// end_time.
func (p S3Prober) HasBaseBackup(ctx context.Context, at Location) (bool, error) {
	client, err := p.client(at)
	if err != nil {
		return false, err
	}

	// The iterator form has no goroutine behind it, so returning from inside
	// the loop stops the listing without leaking anything.
	listing := client.ListObjectsIter(ctx, at.Bucket, minio.ListObjectsOptions{Prefix: at.BasePrefix(), Recursive: true})
	for object := range listing {
		if object.Err != nil {
			return false, fmt.Errorf("list %s/%s: %w", at.Bucket, at.BasePrefix(), object.Err)
		}
		if !strings.HasSuffix(object.Key, "/backup.info") {
			continue
		}
		_, done, err := readBaseBackup(ctx, client, at.Bucket, object.Key)
		if err != nil {
			return false, err
		}
		if done {
			return true, nil
		}
	}
	return false, nil
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
// http:// URL gives plain HTTP. An endpoint without "://" is a bare host, with
// or without a port, such as s3.example.com or 10.0.0.1:9000, and is read as
// HTTPS, which is what every endpoint this cluster talks to uses. A bare host
// never goes through url.Parse, because url.Parse reads s3.example.com:9000 as
// the scheme s3.example.com and refuses 10.0.0.1:9000 outright. It returns an
// error for an empty endpoint, an unparsable URL, and any scheme other than
// http and https.
func splitEndpoint(endpoint string) (host string, secure bool, err error) {
	if endpoint == "" {
		return "", false, fmt.Errorf("the object store names no endpointURL")
	}
	if !strings.Contains(endpoint, "://") {
		return endpoint, true, nil
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
