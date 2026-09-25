package main

import (
	"os/exec"
	"strings"
	"testing"
)

// TestTheBinaryCarriesTheZoneDatabase checks that time/tzdata is among the
// binary's dependencies. The image is FROM scratch and has no
// /usr/share/zoneinfo, so a schedule with a CRON_TZ prefix can load its zone
// only when the binary carries the zone database. Every developer machine has
// the zone files, so parsing a schedule here would pass either way. The test
// reads the build's package list for that reason.
func TestTheBinaryCarriesTheZoneDatabase(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, pkg := range strings.Fields(string(out)) {
		if pkg == "time/tzdata" {
			return
		}
	}
	t.Fatal("time/tzdata is not among the binary's packages; a CRON_TZ schedule fails in the image")
}
