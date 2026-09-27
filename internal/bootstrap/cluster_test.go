package bootstrap

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestArchiverReadsTheEntryAsThePluginDoes checks that Archiver finds the
// store and the server name that plugin-barman-cloud v0.15.0 archives to,
// for each shape of spec.plugins.
//
// The plugin's config package is internal, so the test cannot call it. Each
// case gives the result of the plugin's own rules, cited by file and line:
//   - config.go:289-301 (NewPlugin) takes barmanObjectName from the last
//     entry named barman-cloud.cloudnative-pg.io, enabled or not.
//   - config.go:155-161 (NewFromCluster) starts from the Cluster name. Each
//     enabled entry with that name that has a serverName key sets the server
//     name, also to "". The last such entry wins.
//   - CloudNativePG 1.30.0 loads the plugin in the instance only when an
//     enabled entry names it (api/v1/cluster_funcs.go:83-91,
//     internal/cnpi/plugin/client/create.go:50-56). It then gives each WAL
//     segment to the plugin, with or without isWALArchiver
//     (pkg/management/postgres/archiver/archiver.go:165-170, 286,
//     internal/cnpi/plugin/client/wal.go:58-80).
//   - An empty barmanObjectName makes the plugin's Archive fail on the read
//     of an ObjectStore with no name (common/wal.go:121-129), so the Cluster
//     archives nowhere.
func TestArchiverReadsTheEntryAsThePluginDoes(t *testing.T) {
	entry := func(fields map[string]any) any {
		out := map[string]any{"name": PluginName}
		for key, value := range fields {
			out[key] = value
		}
		return out
	}
	params := func(pairs ...string) map[string]any {
		out := map[string]any{}
		for i := 0; i+1 < len(pairs); i += 2 {
			out[pairs[i]] = pairs[i+1]
		}
		return out
	}

	cases := []struct {
		name      string
		plugins   []any
		store     string
		server    string
		archiving bool
	}{
		{
			name:    "one entry",
			plugins: []any{entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a")})},
			store:   "a", server: "app-pg", archiving: true,
		},
		{
			name:    "one entry with a serverName",
			plugins: []any{entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a", "serverName", "s1")})},
			store:   "a", server: "s1", archiving: true,
		},
		{
			name: "two entries with other stores: the last one names the store",
			plugins: []any{
				entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a")}),
				entry(map[string]any{"parameters": params("barmanObjectName", "b")}),
			},
			store: "b", server: "app-pg", archiving: true,
		},
		{
			name: "two enabled entries with a serverName: the last one wins",
			plugins: []any{
				entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a", "serverName", "s1")}),
				entry(map[string]any{"parameters": params("barmanObjectName", "a", "serverName", "s2")}),
			},
			store: "a", server: "s2", archiving: true,
		},
		{
			name:    "an empty serverName set on purpose stays empty",
			plugins: []any{entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a", "serverName", "")})},
			store:   "a", server: "", archiving: true,
		},
		{
			name: "the serverName of a disabled entry counts for nothing",
			plugins: []any{
				entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a")}),
				entry(map[string]any{"enabled": false, "parameters": params("barmanObjectName", "b", "serverName", "s2")}),
			},
			store: "b", server: "app-pg", archiving: true,
		},
		{
			name: "a disabled last entry still names the store",
			plugins: []any{
				entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a", "serverName", "s1")}),
				entry(map[string]any{"enabled": false, "parameters": params("barmanObjectName", "b")}),
			},
			store: "b", server: "s1", archiving: true,
		},
		{
			name: "a disabled last entry with no parameters leaves no store",
			plugins: []any{
				entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a")}),
				entry(map[string]any{"enabled": false}),
			},
			archiving: false,
		},
		{
			name:      "a disabled only entry is not loaded",
			plugins:   []any{entry(map[string]any{"enabled": false, "isWALArchiver": true, "parameters": params("barmanObjectName", "a")})},
			archiving: false,
		},
		{
			name:    "an enabled entry without isWALArchiver still archives",
			plugins: []any{entry(map[string]any{"parameters": params("barmanObjectName", "a")})},
			store:   "a", server: "app-pg", archiving: true,
		},
		{
			name:    "an entry set enabled explicitly",
			plugins: []any{entry(map[string]any{"enabled": true, "isWALArchiver": true, "parameters": params("barmanObjectName", "a")})},
			store:   "a", server: "app-pg", archiving: true,
		},
		{
			name:      "an entry with no barmanObjectName archives nowhere",
			plugins:   []any{entry(map[string]any{"isWALArchiver": true, "parameters": params("serverName", "s1")})},
			archiving: false,
		},
		{
			name: "an entry of another plugin counts for nothing",
			plugins: []any{
				entry(map[string]any{"isWALArchiver": true, "parameters": params("barmanObjectName", "a")}),
				map[string]any{"name": "other.example.com", "parameters": params("barmanObjectName", "x", "serverName", "y")},
			},
			store: "a", server: "app-pg", archiving: true,
		},
		{
			name:      "no plugins",
			plugins:   nil,
			archiving: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{}}}
			c.SetName("app-pg")
			if tc.plugins != nil {
				c.Object["spec"].(map[string]any)["plugins"] = tc.plugins
			}

			store, server, archiving := Archiver(c)
			if archiving != tc.archiving {
				t.Fatalf("archiving = %v, want %v (store %q, server %q)", archiving, tc.archiving, store, server)
			}
			if !archiving {
				return
			}
			if store != tc.store || server != tc.server {
				t.Errorf("Archiver = (%q, %q), want (%q, %q)", store, server, tc.store, tc.server)
			}
		})
	}
}
