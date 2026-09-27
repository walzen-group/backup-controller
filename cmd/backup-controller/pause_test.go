package main

import (
	"flag"
	"slices"
	"testing"
)

// --pause parses into RunOptions.Paused, and the controller runs without the
// pause when the command line leaves the flag out.
func TestThePauseFlagSetsRunOptionsPaused(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want bool
	}{
		"with --pause":    {args: []string{"--pause"}, want: true},
		"without --pause": {args: nil, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			fs := flag.NewFlagSet("t", flag.ContinueOnError)
			options := runFlags(fs)
			if err := fs.Parse(append([]string{"--restore-image=" + pinnedRestoreImage}, tc.args...)); err != nil {
				t.Fatalf("parse: %v", err)
			}
			got, err := options()
			if err != nil || got.Paused != tc.want {
				t.Errorf("Paused = %t, %v; want %t", got.Paused, err, tc.want)
			}
		})
	}
}

// The chart passes --pause only when the pause value is true.
func TestTheChartPassesPauseOnlyWhenSet(t *testing.T) {
	for value, want := range map[string]bool{"true": true, "false": false} {
		output, err := renderChart(t, "restoreImage="+pinnedRestoreImage, "pause="+value)
		if err != nil {
			t.Fatalf("helm template: %v\n%s", err, output)
		}
		if got := slices.Contains(controllerArgs(t, output), "--pause"); got != want {
			t.Errorf("pause=%s: --pause in the args = %t, want %t", value, got, want)
		}
	}
}
