package main

import (
	"os/exec"
	"strings"
	"testing"
)

// The image is FROM scratch and holds no /usr/share/zoneinfo, so a schedule
// with a CRON_TZ prefix loads its zone only if the binary carries the zone
// database. Every developer machine has the files, which is why this reads the
// build's dependencies and does not parse a schedule.
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
