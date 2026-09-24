// Package bootstrap decides how a CloudNativePG Cluster starts. A Cluster
// whose object store already holds a base backup is recovered from it; one
// whose store is empty is left to bootstrap an empty database with initdb.
//
// Neither kustomize nor OpenTofu can make that choice, because both render
// their manifests before anything has spoken to the object store. An admission
// webhook runs after the object store can be read and before the Cluster is
// persisted, which is the one moment the choice is available.
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

// ClusterListGVK is CloudNativePG's Cluster list, read to find whether a
// database already archives where a new one is about to.
var ClusterListGVK = schema.GroupVersionKind{
	Group:   "postgresql.cnpg.io",
	Version: "v1",
	Kind:    "ClusterList",
}

// ObjectStoreGVK is the Barman Cloud plugin's store, which holds the bucket,
// the endpoint and the credential references a Cluster archives through.
var ObjectStoreGVK = schema.GroupVersionKind{
	Group:   "barmancloud.cnpg.io",
	Version: "v1",
	Kind:    "ObjectStore",
}

// Location is everything needed to ask the object store whether one database
// has a base backup in it.
//
// CABundle is empty for an endpoint signed by a public authority and holds a
// PEM bundle for one that is not. It comes from the store's own endpointCA,
// the field the Barman Cloud plugin already reads for the same reason, so an
// endpoint is described once and both the plugin and this controller trust it.
type Location struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	CABundle  []byte
}

// BasePrefix is the key prefix barman writes base backups under. A listing
// that returns anything below it means this database has something to recover
// from.
func (l Location) BasePrefix() string {
	return strings.TrimPrefix(l.Prefix+"/base/", "/")
}

// Prober answers what base backups exist at a location. The interface exists
// so the decision logic is testable without an object store.
type Prober interface {
	HasBaseBackup(ctx context.Context, at Location) (bool, error)
	BaseBackups(ctx context.Context, at Location) ([]BaseBackup, error)
}

// ResolveLocation reads the named ObjectStore and the Secret it points at, and
// returns where serverName's backups would be.
//
// The returned error says which object was missing. A Cluster naming a store
// that does not exist is a configuration mistake, and the webhook reports it
// rather than guessing that the store is empty.
func ResolveLocation(
	ctx context.Context,
	c client.Reader,
	namespace, objectStore, serverName string,
) (Location, error) {
	store := &unstructured.Unstructured{}
	store.SetGroupVersionKind(ObjectStoreGVK)
	key := types.NamespacedName{Namespace: namespace, Name: objectStore}
	if err := c.Get(ctx, key, store); err != nil {
		return Location{}, fmt.Errorf("read ObjectStore %s/%s: %w", namespace, objectStore, err)
	}

	destination, _, err := unstructured.NestedString(store.Object, "spec", "configuration", "destinationPath")
	if err != nil || destination == "" {
		return Location{}, fmt.Errorf("ObjectStore %s/%s has no spec.configuration.destinationPath", namespace, objectStore)
	}

	bucket, prefix, err := splitDestination(destination)
	if err != nil {
		return Location{}, err
	}

	endpoint, _, _ := unstructured.NestedString(store.Object, "spec", "configuration", "endpointURL")

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

	return Location{
		Endpoint:  endpoint,
		Bucket:    bucket,
		Prefix:    strings.Trim(prefix+"/"+serverName, "/"),
		AccessKey: accessKey,
		SecretKey: secretKey,
		CABundle:  bundle,
	}, nil
}

// endpointCA reads the store's endpointCA bundle, and returns nothing when the
// store declares none. An endpoint signed by a public authority needs no
// bundle, and the image ships the public roots for that case.
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

// splitDestination turns s3://bucket/some/prefix/ into its bucket and its
// prefix. Only s3:// is handled: the other schemes barman supports name
// providers this cluster does not use, and guessing at one would produce a
// listing against the wrong service.
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

// credential reads one of the store's two S3 keys out of the Secret it names.
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
