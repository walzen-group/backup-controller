package v1alpha1

// RunEnding records why a run ended, from the moment the run decided to end
// it. The run writes it in the same status write that fails the unfinished
// items, and every later pass ends the run with this reason and message, also
// one that waits for its movers to stop or retries a restart.
type RunEnding struct {
	// Reason is the Ready condition's reason the run ends with, such as
	// TimedOut.
	Reason string `json:"reason"`
	// Message is the Ready condition's message the run ends with.
	Message string `json:"message"`
}
