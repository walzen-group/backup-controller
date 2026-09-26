package volsync

import (
	"testing"
	"time"
)

// A destination counts gone only once the wait has passed since its latest
// delete. One found gone with no record, as after a restart, counts from that
// look. Settled records nothing.
func TestStopsCountFromTheLatestDelete(t *testing.T) {
	var s Stops
	at := time.Date(2026, 9, 26, 7, 0, 0, 0, time.UTC)
	wait := 10 * time.Second

	if s.Settled("ns/a", at, wait) {
		t.Error("Settled with no record = true, want false")
	}
	if s.Gone("ns/a", at.Add(time.Hour), wait) {
		t.Error("Gone with no record = true, want false: the delete's time is unknown")
	}
	if !s.Gone("ns/a", at.Add(time.Hour+wait), wait) {
		t.Error("Gone a wait after the first look = false, want true")
	}

	s.Deleted("ns/b", at)
	s.Deleted("ns/b", at.Add(5*time.Second))
	if s.Gone("ns/b", at.Add(wait), wait) || s.Settled("ns/b", at.Add(wait), wait) {
		t.Error("gone a wait after the first delete, want the second delete to count")
	}
	if !s.Gone("ns/b", at.Add(5*time.Second+wait), wait) || !s.Settled("ns/b", at.Add(5*time.Second+wait), wait) {
		t.Error("not gone a wait after the second delete, want gone")
	}
	if s.Settled("other/b", at.Add(time.Hour), wait) {
		t.Error("another namespace's destination shares the record")
	}
}

// Records older than forgetStopsAfter are dropped at the next look, and a
// destination looked at again after that waits anew. A record the look
// finds before the drop counts, however old.
func TestStopsForgetOldRecords(t *testing.T) {
	var s Stops
	at := time.Date(2026, 9, 26, 7, 0, 0, 0, time.UTC)
	wait := 10 * time.Second

	s.Deleted("ns/old", at)
	s.Deleted("ns/kept", at)
	if !s.Gone("ns/kept", at.Add(2*forgetStopsAfter), wait) {
		t.Error("an old record the look finds = not gone, want gone")
	}
	if len(s.seen) != 0 {
		t.Errorf("records = %v, want every old record dropped", s.seen)
	}
	if s.Gone("ns/old", at.Add(2*forgetStopsAfter), wait) {
		t.Error("a dropped record = gone, want a new wait")
	}
}
