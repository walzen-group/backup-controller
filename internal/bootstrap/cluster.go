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

// Archiver finds where a Cluster archives its WAL. It looks in spec.plugins for
// the first entry named PluginName with isWALArchiver set to true and a
// non-empty barmanObjectName parameter.
//
// It returns that entry's barmanObjectName as the store and its serverName
// parameter as the server name, with found set to true. When serverName is
// unset, the server name is the Cluster's own name. It returns found as false
// when no entry matches. Such a Cluster backs nothing up and has nothing to
// recover from.
func Archiver(cluster *unstructured.Unstructured) (store, serverName string, found bool) {
	plugins, ok, err := unstructured.NestedSlice(cluster.Object, "spec", "plugins")
	if err != nil || !ok {
		return "", "", false
	}

	for _, entry := range plugins {
		plugin, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(plugin, "name"); name != PluginName {
			continue
		}
		if isArchiver, _, _ := unstructured.NestedBool(plugin, "isWALArchiver"); !isArchiver {
			continue
		}
		store, _, _ = unstructured.NestedString(plugin, "parameters", "barmanObjectName")
		if store == "" {
			continue
		}
		serverName, _, _ = unstructured.NestedString(plugin, "parameters", "serverName")
		if serverName == "" {
			// Barman defaults the server name to the Cluster's name, so an
			// unset parameter means the archive sits under that.
			serverName = cluster.GetName()
		}
		return store, serverName, true
	}
	return "", "", false
}
