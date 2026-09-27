package bootstrap

import (
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// declaredBootstrap names the bootstrap method a Cluster declares for itself,
// one the webhook must leave alone.
//
// It returns the first key under spec.bootstrap, in sorted order, other than
// initdb, such as "recovery" or "pg_basebackup". It returns an empty string
// when spec.bootstrap is missing or holds initdb alone. Those are the Clusters
// the webhook may rewrite: CloudNativePG runs initdb when no method is named,
// and setRecovery replaces initdb and nothing else. Adding a recovery beside
// any other method gives the Cluster two, and CloudNativePG refuses that.
func declaredBootstrap(cluster *unstructured.Unstructured) string {
	bootstrap, _, _ := unstructured.NestedMap(cluster.Object, "spec", "bootstrap")
	methods := make([]string, 0, len(bootstrap))
	for method := range bootstrap {
		if method != "initdb" {
			methods = append(methods, method)
		}
	}
	if len(methods) == 0 {
		return ""
	}
	sort.Strings(methods)
	return methods[0]
}

// OwnerBootstrap names the bootstrap method a Cluster's owner declares, such
// as "pg_basebackup", and returns an empty string when there is none. A
// RestoreRun leaves such a Cluster alone: Flux would create it again with that
// method, and the webhook refuses it while a run waits for it.
//
// It is declaredBootstrap without the recovery this webhook wrote itself,
// whose source is RecoverySource. A Cluster the webhook recovered keeps that
// recovery in its spec, and a later restore of it is the controller's own.
func OwnerBootstrap(cluster *unstructured.Unstructured) string {
	method := declaredBootstrap(cluster)
	if method == "recovery" {
		if source, _, _ := unstructured.NestedString(cluster.Object, "spec", "bootstrap", "recovery", "source"); source == RecoverySource {
			return ""
		}
	}
	return method
}

// Archiver finds where a Cluster archives its WAL. It reads spec.plugins with
// the rules of plugin-barman-cloud v0.15.0 and CloudNativePG 1.30.0, so the
// webhook and the RestoreRun controller look where the plugin writes.
//
// It returns the store, the server name and found. The store is the
// barmanObjectName parameter of the last entry named PluginName, enabled or
// not (plugin internal/cnpgi/operator/config/config.go:289-301). The server
// name starts as the Cluster's name. Each enabled entry named PluginName that
// has a serverName key replaces it, also with an empty value. The last such
// entry wins (config.go:155-161). An empty server name puts the archive
// directly under the destinationPath, and storeLocation looks there too.
//
// found is false when no enabled entry names PluginName, because
// CloudNativePG then does not load the plugin in the instance
// (api/v1/cluster_funcs.go:83-91, internal/cnpi/plugin/client/create.go:50-56).
// found is also false when the store is empty, because the Archive call of
// the plugin then cannot read an ObjectStore with no name (plugin
// internal/cnpgi/common/wal.go:121-129). Such a Cluster backs nothing up and
// has nothing to recover from.
//
// isWALArchiver does not change the result. CloudNativePG gives each WAL
// segment to every loaded plugin that can archive, with or without that field
// (pkg/management/postgres/archiver/archiver.go:165-170, 286,
// internal/cnpi/plugin/client/wal.go:58-80). The plugin does not read it.
func Archiver(cluster *unstructured.Unstructured) (store, serverName string, found bool) {
	plugins, _, _ := unstructured.NestedSlice(cluster.Object, "spec", "plugins")

	serverName = cluster.GetName()
	loaded := false
	for _, item := range plugins {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(entry, "name"); name != PluginName {
			continue
		}
		parameters, _, _ := unstructured.NestedMap(entry, "parameters")
		store, _ = parameters["barmanObjectName"].(string)
		if !pluginEnabled(entry) {
			continue
		}
		loaded = true
		if value, set := parameters["serverName"]; set {
			serverName, _ = value.(string)
		}
	}
	if !loaded || store == "" {
		return "", "", false
	}
	return store, serverName, true
}

// pluginEnabled reports whether a spec.plugins entry is enabled. An entry
// without the enabled field is enabled, as the IsEnabled function of
// CloudNativePG says (api/v1/cluster_funcs.go:411-416).
func pluginEnabled(entry map[string]any) bool {
	enabled, set, _ := unstructured.NestedBool(entry, "enabled")
	return !set || enabled
}
