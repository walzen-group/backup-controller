package volsync

import (
	"sync"
	"time"
)

// forgetStopsAfter is how long Stops keeps a record. A record older than
// this is dropped, and a destination looked at again after that counts from
// the pass that looks, as one with no record does.
const forgetStopsAfter = time.Hour

// Stops records when this process last deleted, or first found gone, each
// ReplicationDestination a controller deleted to stop its mover, so that the
// controller counts the mover gone only once enough time has passed since
// the delete (rule X2).
//
// A controller that deletes a destination writes a status or records an
// event, and either can start its next reconcile at once. VolSync may still
// be in the middle of a reconcile of that destination and create the mover's
// Job after a look taken milliseconds after the delete. The time lives in
// memory only. After a restart, or once a record is an hour old, a
// destination found gone has no record, and its time counts from that pass,
// so either makes the wait longer and never shorter. The zero value is ready
// to use, and the methods are safe for concurrent use.
type Stops struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

// Deleted records that a pass deleted the destination named by key at now.
//
// Parameters:
//   - key names the destination, as namespace/name, so that destinations of
//     two namespaces never share a record.
//   - now is the time of the pass that issued the delete. A later delete of
//     the same destination moves the record forward to its own time.
func (s *Stops) Deleted(key string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(now)
	s.seen[key] = now
}

// Gone reports whether at least wait has passed since the destination named
// by key was deleted, for a pass that found the destination gone.
//
// Parameters:
//   - key names the destination, as namespace/name.
//   - now is the time of the pass.
//   - wait is the least time between the delete and a look for the mover
//     that may count it gone.
//
// A destination with no record was deleted at a time this process does not
// know, so Gone records now for it and returns false.
func (s *Stops) Gone(key string, now time.Time, wait time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = map[string]time.Time{}
	}
	at, ok := s.seen[key]
	s.prune(now)
	if !ok {
		s.seen[key] = now
		return false
	}
	// A record prune dropped is older than forgetStopsAfter, so it is
	// settled for any wait shorter than that.
	return now.Sub(at) >= wait
}

// Settled reports whether at least wait has passed since the destination
// named by key was deleted, without recording anything. A destination with
// no record has not settled.
//
// Parameters:
//   - key names the destination, as namespace/name.
//   - now is the time of the pass.
//   - wait is the least time between the delete and a look for the mover
//     that may count it gone.
func (s *Stops) Settled(key string, now time.Time, wait time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.seen[key]
	return ok && now.Sub(at) >= wait
}

// prune drops every record older than forgetStopsAfter at now, so the map
// holds only the destinations stopped in the last hour. The caller holds
// the lock.
func (s *Stops) prune(now time.Time) {
	if s.seen == nil {
		s.seen = map[string]time.Time{}
	}
	for key, at := range s.seen {
		if now.Sub(at) > forgetStopsAfter {
			delete(s.seen, key)
		}
	}
}
