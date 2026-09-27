package bootstrap

import (
	"fmt"
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

// archiverShape checks that the entries of the Barman Cloud plugin in a
// Cluster's spec.plugins have the one shape that Archiver and the plugin read
// the same way.
//
// Parameters:
//   - cluster is the Cluster of the create.
//
// It returns nil when no entry has the name PluginName, when the one entry
// with that name is not a WAL archiver, and when the one archiver entry is
// enabled, names a store in barmanObjectName and gives no empty serverName.
// It returns an error that names the problem in every other case.
//
// plugin-barman-cloud v0.15.0 reads barmanObjectName from the last entry
// with the plugin name, and serverName from the last enabled entry that has
// the parameter, also when the value is empty
// (internal/cnpgi/operator/config/config.go:149-161 and :289-301). Archiver
// reads the first archiver entry and uses the Cluster name for an empty
// serverName. With two entries, an empty serverName, a disabled archiver or
// an archiver without a store, the two can disagree about the archive. The
// webhook then refuses the create, because a decision about the wrong
// archive can start an empty database beside a full archive.
func archiverShape(cluster *unstructured.Unstructured) error {
	plugins, _, err := unstructured.NestedSlice(cluster.Object, "spec", "plugins")
	if err != nil {
		return fmt.Errorf("spec.plugins is not a list: %w", err)
	}
	var entries []map[string]any
	for _, entry := range plugins {
		plugin, ok := entry.(map[string]any)
		if ok && plugin["name"] == PluginName {
			entries = append(entries, plugin)
		}
	}
	switch len(entries) {
	case 0:
		return nil
	case 1:
	default:
		return fmt.Errorf("spec.plugins has %d entries for %s; the webhook reads only one", len(entries), PluginName)
	}
	return archiverEntryShape(entries[0])
}

// archiverEntryShape checks the one entry of the Barman Cloud plugin in
// spec.plugins. See archiverShape for the rules and the returned error.
func archiverEntryShape(plugin map[string]any) error {
	isArchiver, _, err := unstructured.NestedBool(plugin, "isWALArchiver")
	if err != nil {
		return fmt.Errorf("the %s entry in spec.plugins has an isWALArchiver that is not true or false: %w", PluginName, err)
	}
	enabled, found, err := unstructured.NestedBool(plugin, "enabled")
	if err != nil {
		return fmt.Errorf("the %s entry in spec.plugins has an enabled that is not true or false: %w", PluginName, err)
	}
	if !isArchiver {
		return nil
	}
	if found && !enabled {
		return fmt.Errorf("the %s entry in spec.plugins is the WAL archiver and has enabled set to false", PluginName)
	}
	parameters, _, err := unstructured.NestedStringMap(plugin, "parameters")
	if err != nil {
		return fmt.Errorf("the %s entry in spec.plugins has parameters that are not strings: %w", PluginName, err)
	}
	if parameters["barmanObjectName"] == "" {
		return fmt.Errorf("the %s entry in spec.plugins is the WAL archiver and has no barmanObjectName parameter", PluginName)
	}
	if serverName, set := parameters["serverName"]; set && serverName == "" {
		return fmt.Errorf("the %s entry in spec.plugins has an empty serverName parameter; remove it to use the Cluster name", PluginName)
	}
	return nil
}
