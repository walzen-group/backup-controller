package versions

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// pin is one version an e2e component's pins.json names for a component
// versions.json lists.
type pin struct {
	// path is where in pins.json the value sits, as dotted keys.
	path string
	// component is the versions.json component the value must match.
	component string
	// field is what of the component it must match: "version" (the default),
	// "chart" for the Helm chart version, "major" when only the major version
	// has to agree, or "image" for the image reference. For "image", path
	// names an object with repository, tag and digest, compared as
	// repository:tag@digest.
	field string
}

// e2ePins lists, per hack/e2e component, the versions its pins.json names
// that versions.json also records. The pins.json files differ in shape per
// component, so each value is addressed by its path.
var e2ePins = map[string][]pin{
	"backup-controller": {
		{path: "backup-controller.requires.cert-manager", component: "cert-manager"},
		{path: "backup-controller.requires.kueue", component: "kueue"},
	},
	"cert-manager": {
		{path: "cert-manager.chart.version", component: "cert-manager"},
		{path: "cert-manager.images.controller.tag", component: "cert-manager"},
		{path: "cert-manager.images.webhook.tag", component: "cert-manager"},
		{path: "cert-manager.images.cainjector.tag", component: "cert-manager"},
		{path: "cert-manager.images.startupapicheck.tag", component: "cert-manager"},
	},
	"cnpg": {
		{path: "cnpg.operator.chart.version", component: "cloudnative-pg", field: "chart"},
		{path: "cnpg.operator.image.tag", component: "cloudnative-pg"},
		{path: "cnpg.plugin.chart.version", component: "plugin-barman-cloud", field: "chart"},
		{path: "cnpg.plugin.image.tag", component: "plugin-barman-cloud"},
		{path: "cnpg.plugin.sidecarImage.tag", component: "plugin-barman-cloud"},
		// The check Cluster runs the operand image prod's Clusters name, by
		// tag and digest.
		{path: "cnpg.postgres.tag", component: "postgresql-operand"},
		{path: "cnpg.postgres", component: "postgresql-operand", field: "image"},
		// The fixtures come from nixpkgs postgresql. barman reads a layout that
		// holds within a major, so only the major has to agree with the operand.
		{path: "cnpg.postgres.tag", component: "postgresql", field: "major"},
	},
	"csi": {
		{path: "versions.csi-driver-host-path", component: "csi-driver-host-path"},
		{path: "versions.external-snapshotter", component: "external-snapshotter"},
	},
	"flux": {
		{path: "flux.version", component: "flux"},
		{path: "flux.images.flux-cli.tag", component: "flux"},
	},
	"kueue": {
		{path: "kueue.chart.version", component: "kueue"},
		{path: "kueue.image.tag", component: "kueue"},
	},
	"rustfs": {
		{path: "rustfs.image.tag", component: "rustfs"},
	},
	"volsync": {
		{path: "volsync.chart.version", component: "volsync"},
		{path: "volsync.image.tag", component: "volsync"},
		{path: "volsync.restic", component: "restic-mover"},
	},
}

// repoRoot returns the module's root, where versions.json sits.
func repoRoot() string { return filepath.Dir(Path()) }

// walk follows doc along the dotted path and returns the value there. It
// returns false when a key is missing.
func walk(doc any, path string) (any, bool) {
	for _, key := range strings.Split(path, ".") {
		m, ok := doc.(map[string]any)
		if !ok {
			return nil, false
		}
		if doc, ok = m[key]; !ok {
			return nil, false
		}
	}
	return doc, true
}

// lookup walks doc along the dotted path and returns the string there. It
// returns false when a key is missing or the value is no string.
func lookup(doc any, path string) (string, bool) {
	v, _ := walk(doc, path)
	s, ok := v.(string)
	return s, ok
}

// lookupImage walks doc along the dotted path to an image object and returns
// it as repository:tag@digest. It returns false when the object is missing or
// lacks one of the three strings.
func lookupImage(doc any, path string) (string, bool) {
	v, ok := walk(doc, path)
	if !ok {
		return "", false
	}
	repo, okRepo := lookup(v, "repository")
	tag, okTag := lookup(v, "tag")
	digest, okDigest := lookup(v, "digest")
	if !okRepo || !okTag || !okDigest {
		return "", false
	}
	return repo + ":" + tag + "@" + digest, true
}

// normalise returns the version in a pinned value: the first word, without a
// leading v and without an image tag's suffix after the first dash (as in
// 1.26.8-alpine3.24 or 18.4-system-trixie).
func normalise(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return ""
	}
	v := strings.TrimPrefix(fields[0], "v")
	v, _, _ = strings.Cut(v, "-")
	return v
}

// major returns the part of a version before its first dot.
func major(v string) string {
	m, _, _ := strings.Cut(v, ".")
	return m
}

// readPins decodes hack/e2e/<name>/pins.json.
func readPins(t *testing.T, name string) any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(), "hack", "e2e", name, "pins.json"))
	if err != nil {
		t.Fatalf("read pins.json of %s: %v", name, err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode pins.json of %s: %v", name, err)
	}
	return doc
}

// TestE2EPinsMatchVersions checks that every version an e2e component pins
// agrees with versions.json, so a bump in one place that misses the other
// fails here. It also fails for a pins.json this test has no entry for, so a
// new e2e component gets its versions compared from the start.
func TestE2EPinsMatchVersions(t *testing.T) {
	f, err := Read()
	if err != nil {
		t.Fatalf("read versions.json: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(repoRoot(), "hack", "e2e", "*", "pins.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("found no hack/e2e/*/pins.json")
	}
	for _, file := range files {
		name := filepath.Base(filepath.Dir(file))
		if _, ok := e2ePins[name]; !ok {
			t.Errorf("hack/e2e/%s/pins.json has no entry in e2ePins; list the versions it pins", name)
		}
	}
	for name, pins := range e2ePins {
		doc := readPins(t, name)
		for _, p := range pins {
			raw, ok := lookup(doc, p.path)
			if p.field == "image" {
				raw, ok = lookupImage(doc, p.path)
			}
			if !ok {
				t.Errorf("hack/e2e/%s/pins.json has no %s at %s", name, valueName(p.field), p.path)
				continue
			}
			c, ok := f.Components[p.component]
			if !ok {
				t.Errorf("versions.json has no component %s, which hack/e2e/%s/pins.json pins at %s", p.component, name, p.path)
				continue
			}
			got := normalise(raw)
			want := c.Version
			switch p.field {
			case "chart":
				want = c.Chart
			case "major":
				got, want = major(got), major(want)
			case "image":
				got, want = raw, c.Image
			}
			if want == "" || got != want {
				t.Errorf("hack/e2e/%s/pins.json %s is %q, versions.json %s %s is %q", name, p.path, raw, p.component, fieldName(p.field), want)
			}
		}
	}
}

// valueName names what a pin reads from pins.json.
func valueName(field string) string {
	if field == "image" {
		return "image object with repository, tag and digest"
	}
	return "string"
}

// fieldName names the versions.json field a pin is compared with.
func fieldName(field string) string {
	if field == "" {
		return "version"
	}
	return field
}

// csiURL matches the repository and release tag in a kubernetes-csi manifest
// URL, and csiRef the image and tag in a registry.k8s.io/sig-storage image
// reference.
var (
	csiURL = regexp.MustCompile(`kubernetes-csi/([a-z-]+)/v([0-9.]+)/`)
	csiRef = regexp.MustCompile(`sig-storage/([a-z-]+):v([0-9.]+)`)
)

// TestE2ECSIManifestsMatchVersions checks that the CSI manifests and the
// snapshot images hack/e2e/csi fetches carry the csi-driver-host-path and
// external-snapshotter releases versions.json records. The manifests of the
// other sidecars (attacher, provisioner, resizer, health monitor) are
// versioned on their own and are left out.
func TestE2ECSIManifestsMatchVersions(t *testing.T) {
	f, err := Read()
	if err != nil {
		t.Fatalf("read versions.json: %v", err)
	}
	var doc struct {
		Manifests []struct {
			URL string `json:"url"`
		} `json:"manifests"`
		Images []struct {
			Ref string `json:"ref"`
		} `json:"images"`
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(), "hack", "e2e", "csi", "pins.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	repoComponent := map[string]string{
		"csi-driver-host-path": "csi-driver-host-path",
		"external-snapshotter": "external-snapshotter",
	}
	imageComponent := map[string]string{
		"csi-snapshotter":     "external-snapshotter",
		"snapshot-controller": "external-snapshotter",
	}
	seen := map[string]bool{}
	for _, m := range doc.Manifests {
		match := csiURL.FindStringSubmatch(m.URL)
		if match == nil {
			continue
		}
		comp, ok := repoComponent[match[1]]
		if !ok {
			continue
		}
		seen[comp] = true
		if want := f.Components[comp].Version; match[2] != want {
			t.Errorf("hack/e2e/csi/pins.json fetches %s at v%s, versions.json %s is %s", m.URL, match[2], comp, want)
		}
	}
	for _, img := range doc.Images {
		match := csiRef.FindStringSubmatch(img.Ref)
		if match == nil {
			continue
		}
		comp, ok := imageComponent[match[1]]
		if !ok {
			continue
		}
		if want := f.Components[comp].Version; match[2] != want {
			t.Errorf("hack/e2e/csi/pins.json runs %s, versions.json %s is %s", img.Ref, comp, want)
		}
	}
	for _, comp := range []string{"csi-driver-host-path", "external-snapshotter"} {
		if !seen[comp] {
			t.Errorf("hack/e2e/csi/pins.json fetches no manifest of %s", comp)
		}
	}
}

// TestE2EComponentsExist checks that every component the Makefile's
// E2E_COMPONENTS names has its directory hack/e2e/<name> with the script
// <name>.sh the Makefile runs, and that every directory under hack/e2e is
// in the list.
func TestE2EComponentsExist(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	line := regexp.MustCompile(`(?m)^E2E_COMPONENTS\s*:?=\s*(.*)$`).FindSubmatch(raw)
	if line == nil {
		t.Fatal("the Makefile sets no E2E_COMPONENTS")
	}
	components := strings.Fields(string(line[1]))
	if len(components) == 0 {
		t.Fatal("E2E_COMPONENTS is empty")
	}
	for _, name := range components {
		script := filepath.Join(repoRoot(), "hack", "e2e", name, name+".sh")
		if _, err := os.Stat(script); err != nil {
			t.Errorf("E2E_COMPONENTS names %s, but hack/e2e/%s/%s.sh is missing: %v", name, name, name, err)
		}
	}
	dirs, err := os.ReadDir(filepath.Join(repoRoot(), "hack", "e2e"))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		if d.IsDir() && !slices.Contains(components, d.Name()) {
			t.Errorf("hack/e2e/%s is not in the Makefile's E2E_COMPONENTS", d.Name())
		}
	}
}
