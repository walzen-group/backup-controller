package runs

import (
	"strings"
	"testing"
)

// TestALongNoteIsCutToTheAPILimit checks that fitNote cuts a note longer than
// events.k8s.io/v1 accepts to at most maxNote bytes, on a character boundary,
// so the API server accepts the event.
func TestALongNoteIsCutToTheAPILimit(t *testing.T) {
	note := strings.Repeat("ä", maxNote)
	cut := fitNote(note)
	if len(cut) > maxNote {
		t.Fatalf("len = %d, want at most %d", len(cut), maxNote)
	}
	if !strings.HasPrefix(note, cut) || !strings.HasSuffix(cut, "ä") {
		t.Fatalf("the note was not cut on a character boundary")
	}
}
