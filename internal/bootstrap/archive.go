// Package bootstrap decides how a new CloudNativePG Cluster starts. When the
// Cluster's object store already holds a completed base backup, the package
// rewrites the Cluster to recover from it. When the store holds none, the
// Cluster keeps its initdb bootstrap and starts as an empty database.
//
// Neither kustomize nor OpenTofu can make that choice, because both render
// their manifests before anything has spoken to the object store. An admission
// webhook runs after the object store can be read and before the Cluster is
// persisted. That is the one moment the choice can be made.
//
// The RestoreRun controller in internal/runs also uses Archiver,
// ResolveLocation and a Prober from this package, to check that a database has
// a base backup before it deletes the Cluster.
package bootstrap

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterListGVK is the GroupVersionKind of CloudNativePG's ClusterList. The
// webhook lists every Cluster with it to find out whether another database
// already archives to the prefix a new Cluster is about to use.
var ClusterListGVK = schema.GroupVersionKind{
	Group:   "postgresql.cnpg.io",
	Version: "v1",
	Kind:    "ClusterList",
}

// ObjectStoreGVK is the GroupVersionKind of the Barman Cloud plugin's
// ObjectStore. An ObjectStore holds the bucket, the endpoint and the Secret
// references that a Cluster archives through. ResolveLocation reads it.
var ObjectStoreGVK = schema.GroupVersionKind{
	Group:   "barmancloud.cnpg.io",
	Version: "v1",
	Kind:    "ObjectStore",
}

// Location holds everything needed to ask an object store which base backups
// one database has. ResolveLocation builds it from a Cluster's ObjectStore and
// the Secrets that store names.
type Location struct {
	// Endpoint is the store's spec.configuration.endpointURL.
	Endpoint string
	// Bucket is the bucket named in the store's destinationPath.
	Bucket string
	// Prefix is the path inside the bucket that one database archives under.
	// It's the destinationPath's prefix followed by the server name, with no
	// slash at either end.
	Prefix string
	// AccessKey and SecretKey are the S3 credentials, read from the Secrets
	// that the store's s3Credentials name.
	AccessKey string
	SecretKey string
	// CABundle is the PEM bundle of the authority that signs the endpoint's
	// certificate. It's empty when a public authority signs it. It comes from
	// the store's own endpointCA, the field the Barman Cloud plugin reads for
	// the same reason, so an endpoint's CA is described once and both the
	// plugin and this controller trust it.
	CABundle []byte
}

// BasePrefix returns the key prefix that barman writes base backups under:
// Prefix followed by "/base/", or just "base/" when Prefix is empty. Any
// object below it means the database has something to recover from.
func (l Location) BasePrefix() string {
	return strings.TrimPrefix(l.Prefix+"/base/", "/")
}

// Prober asks an object store which base backups exist at a Location.
// S3Prober is the real one. The interface exists so the webhook and the
// RestoreRun controller can be tested without an object store.
type Prober interface {
	// HasBaseBackup reports whether the location holds at least one
	// completed base backup, one whose backup.info has status DONE.
	HasBaseBackup(ctx context.Context, at Location) (bool, error)
	// BaseBackups lists the location's completed base backups, oldest
	// first.
	BaseBackups(ctx context.Context, at Location) ([]BaseBackup, error)
}

// ResolveLocation works out where one database's backups live and how to
// reach them, by reading its ObjectStore and the Secrets that store names.
//
// Parameters:
//   - c reads the ObjectStore and the Secrets. The webhook and the RestoreRun
//     controller both pass the manager's uncached API reader.
//   - namespace is the Cluster's namespace. The ObjectStore and its Secrets
//     are read from the same namespace.
//   - objectStore is the ObjectStore's name, taken from the barmanObjectName
//     parameter of the Cluster's archiving plugin (see Archiver).
//   - serverName is the directory the database archives under inside the
//     store's prefix. It also comes from Archiver.
//
// It returns a Location whose Prefix joins the destinationPath's prefix and
// serverName. It returns an error when the ObjectStore can't be read, when its
// spec.configuration.destinationPath is missing or isn't an s3:// URL with a
// bucket, when a Secret or key named under s3Credentials or endpointCA is
// missing, or when endpointCA names a Secret with no key. The error says which
// object or field was missing.
//
// A Cluster that names a store that doesn't exist is a configuration mistake.
// The webhook refuses such a Cluster with this error, so nobody gets an empty
// database beside a full archive because a store was misnamed.
func ResolveLocation(
	ctx context.Context,
	c client.Reader,
	namespace, objectStore, serverName string,
) (Location, error) {
	at, store, err := archiveAt(ctx, c, namespace, objectStore, serverName)
	if err != nil {
		return Location{}, err
	}

	accessKey, err := credential(ctx, c, namespace, store, "accessKeyId")
	if err != nil {
		return Location{}, err
	}
	secretKey, err := credential(ctx, c, namespace, store, "secretAccessKey")
	if err != nil {
		return Location{}, err
	}

	bundle, err := endpointCA(ctx, c, namespace, store)
	if err != nil {
		return Location{}, err
	}

	at.AccessKey = accessKey
	at.SecretKey = secretKey
	at.CABundle = bundle
	return at, nil
}

// archiveAt works out where one database archives from its ObjectStore alone,
// reading no Secret. The webhook's collision check calls it for every
// archiving Cluster on the cluster, and ResolveLocation builds on it.
//
// The arguments are those of ResolveLocation. It returns a Location with only
// Endpoint, Bucket and Prefix set, and the ObjectStore it read so the caller
// can follow its Secret references. It returns an error when the ObjectStore
// can't be read, or when its spec.configuration.destinationPath is missing or
// isn't an s3:// URL with a bucket.
func archiveAt(
	ctx context.Context,
	c client.Reader,
	namespace, objectStore, serverName string,
) (Location, *unstructured.Unstructured, error) {
	store := &unstructured.Unstructured{}
	store.SetGroupVersionKind(ObjectStoreGVK)
	key := types.NamespacedName{Namespace: namespace, Name: objectStore}
	if err := c.Get(ctx, key, store); err != nil {
		return Location{}, nil, fmt.Errorf("read ObjectStore %s/%s: %w", namespace, objectStore, err)
	}

	destination, _, err := unstructured.NestedString(store.Object, "spec", "configuration", "destinationPath")
	if err != nil || destination == "" {
		return Location{}, nil, fmt.Errorf("ObjectStore %s/%s has no spec.configuration.destinationPath", namespace, objectStore)
	}

	bucket, prefix, err := splitDestination(destination)
	if err != nil {
		return Location{}, nil, err
	}

	endpoint, _, _ := unstructured.NestedString(store.Object, "spec", "configuration", "endpointURL")

	return Location{
		Endpoint: endpoint,
		Bucket:   bucket,
		Prefix:   strings.Trim(prefix+"/"+serverName, "/"),
	}, store, nil
}

// sameArchive reports whether two Locations name one archive: the same bucket
// and prefix on the same S3 service. Endpoints are compared by host and port,
// ignoring letter case, so https://s3.example.com and S3.example.com are one
// service. An endpoint that splitEndpoint can't read is compared as written.
func (l Location) sameArchive(other Location) bool {
	return l.Bucket == other.Bucket && l.Prefix == other.Prefix &&
		endpointHost(l.Endpoint) == endpointHost(other.Endpoint)
}

// endpointHost returns the host and port an endpointURL points at, in lower
// case, or the endpointURL itself in lower case when splitEndpoint refuses it.
func endpointHost(endpoint string) string {
	host, _, err := splitEndpoint(endpoint)
	if err != nil {
		host = endpoint
	}
	return strings.ToLower(host)
}

// endpointCA reads the PEM bundle that the ObjectStore's
// spec.configuration.endpointCA points at, from a Secret in the given
// namespace.
//
// It returns nil and no error when the store names no endpointCA Secret. An
// endpoint signed by a public authority needs no bundle, and the image ships
// the public roots for that case. It returns an error when endpointCA names a
// Secret with no key, when the Secret can't be read, or when the Secret has no
// entry under that key.
func endpointCA(
	ctx context.Context,
	c client.Reader,
	namespace string,
	store *unstructured.Unstructured,
) ([]byte, error) {
	name, found, err := unstructured.NestedString(store.Object, "spec", "configuration", "endpointCA", "name")
	if err != nil || !found || name == "" {
		return nil, nil
	}
	key, found, err := unstructured.NestedString(store.Object, "spec", "configuration", "endpointCA", "key")
	if err != nil || !found || key == "" {
		return nil, fmt.Errorf("ObjectStore %s/%s names an endpointCA Secret with no key", namespace, store.GetName())
	}

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
		return nil, fmt.Errorf("read endpointCA Secret %s/%s: %w", namespace, name, err)
	}
	bundle, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("endpointCA Secret %s/%s has no key %q", namespace, name, key)
	}
	return bundle, nil
}

// splitDestination splits a destinationPath such as s3://bucket/some/prefix/
// into its bucket ("bucket") and its prefix ("some/prefix", with the slashes
// at either end trimmed).
//
// It accepts only s3:// URLs. It returns an error for any other scheme, for a
// URL that doesn't parse, and for one with no bucket. The other schemes barman
// supports name providers this cluster doesn't use, and guessing at one would
// list objects on the wrong service.
func splitDestination(destination string) (bucket, prefix string, err error) {
	parsed, err := url.Parse(destination)
	if err != nil {
		return "", "", fmt.Errorf("parse destinationPath %q: %w", destination, err)
	}
	if parsed.Scheme != "s3" {
		return "", "", fmt.Errorf("destinationPath %q is not an s3:// URL", destination)
	}
	if parsed.Host == "" {
		return "", "", fmt.Errorf("destinationPath %q names no bucket", destination)
	}
	return parsed.Host, strings.Trim(parsed.Path, "/"), nil
}

// credential reads one of the ObjectStore's two S3 credentials from the Secret
// the store names for it.
//
// The field argument is "accessKeyId" or "secretAccessKey". The function
// follows spec.configuration.s3Credentials.<field> to a Secret name and key in
// the given namespace, and returns the value stored there. It returns an error
// when the store doesn't name both a Secret and a key, when the Secret can't be
// read, or when the key is missing from it.
func credential(
	ctx context.Context,
	c client.Reader,
	namespace string,
	store *unstructured.Unstructured,
	field string,
) (string, error) {
	name, _, err := unstructured.NestedString(store.Object, "spec", "configuration", "s3Credentials", field, "name")
	if err != nil || name == "" {
		return "", fmt.Errorf("ObjectStore %s/%s has no s3Credentials.%s.name", namespace, store.GetName(), field)
	}
	key, _, err := unstructured.NestedString(store.Object, "spec", "configuration", "s3Credentials", field, "key")
	if err != nil || key == "" {
		return "", fmt.Errorf("ObjectStore %s/%s has no s3Credentials.%s.key", namespace, store.GetName(), field)
	}

	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
		return "", fmt.Errorf("read Secret %s/%s: %w", namespace, name, err)
	}
	value, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", namespace, name, key)
	}
	return string(value), nil
}
