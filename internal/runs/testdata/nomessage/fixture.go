// Package nomessage is the fixture of TestTheMessageRuleCatchesEveryShape.
// Each line with a trailing want comment breaks the part of the rule it
// names; no other line may be reported. It is never built.
package nomessage

import (
	"bytes"
	"errors"
	"regexp"
	str "strings"
)

type item struct {
	Message        string
	Logs           string
	LastStartError string
}

type reconciler struct{}

var span = regexp.MustCompile(`^\d+$`) // want matcher

func direct(it item) bool {
	return str.HasPrefix(it.Message, "not started yet: ") // want matcher
}

func throughALocal(it item) bool {
	logs := it.Logs
	return str.Contains(logs, "already locked") // want matcher
}

func (r *reconciler) inAMethod(it item) string {
	_, last, _ := str.Cut(it.Message, ": ") // want matcher
	return last
}

func regexpMethod(s string) bool {
	return span.MatchString(s) // want matcher
}

func comparedField(it item) bool {
	return it.Message == "timed out" // want comparison
}

func comparedLocal(it item) bool {
	var text = it.Message
	copied := text
	return "" != copied // want comparison
}

func comparedStartError(it item) bool {
	return it.LastStartError != "" // want comparison
}

func comparedError(err error) bool {
	return err.Error() == "the Cluster no longer exists" // want comparison
}

func comparedErrorLocal(err error) bool {
	text := err.Error()
	return text != "" // want comparison
}

func switched(it item) int {
	switch it.Logs { // want comparison
	case "":
		return 0
	}
	return 1
}

func switchedCase(it item, s string) int {
	switch s { // want comparison
	case it.Message:
		return 0
	}
	return 1
}

func split(it item) bool {
	return str.Split(it.Message, ": ")[0] == "not started yet" // want matcher
}

func fields(it item) bool {
	return str.Fields(it.Logs)[0] == "Error:" // want matcher
}

func compared(it item) bool {
	return str.Compare(it.Message, "done") == 0 // want matcher
}

func equalBytes(b []byte) bool {
	return bytes.Equal(b, []byte("done")) // want matcher
}

func sliced(it item) bool {
	return it.Message[:3] == "not" // want comparison
}

func indexed(err error) bool {
	return err.Error()[0] == 'n' // want comparison
}

func slicedLocal(it item) bool {
	head := it.Message[:3]
	return head == "not" // want comparison
}

func joined(it item) bool {
	text := it.Message + "; " + it.Logs
	return text != "" // want comparison
}

func joinedInline(err error) bool {
	return "x: "+err.Error() == "x: done" // want comparison
}

func allowed(key string) string {
	return str.TrimPrefix(key, "ns/")
}

func typed(err error) bool {
	return errors.Is(err, errors.ErrUnsupported) && str.Join(nil, "") == ""
}
