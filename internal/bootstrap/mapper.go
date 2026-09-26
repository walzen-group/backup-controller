package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"github.com/walzen-group/backup-controller/internal/served"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Warm looks up Cluster and ObjectStore in mapper by group and kind with no
// version, so controller-runtime's lazy mapper reads the discovery of
// postgresql.cnpg.io and barmancloud.cnpg.io before the webhook's first
// admission request, and that request finds both groups cached.
//
// Parameters:
//   - mapper is the manager's RESTMapper, the one cmd/backup-controller
//     passes to Decider.
//
// It returns nil when both lookups answered, and also for a kind of which no
// version is served, since that project is simply not installed. A lookup
// that failed comes back as a *served.LookupError, joined with the other's;
// the webhook then looks the group up again on its first request, within
// its budget.
func Warm(mapper meta.RESTMapper) error {
	var errs []error
	for _, gk := range []schema.GroupKind{clusterKind, objectStoreKind} {
		if _, err := served.Kind(mapper, gk); err != nil && !served.IsNotServed(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// boundedMapper is a RESTMapper whose lookups end when ctx ends.
//
// A RESTMapper takes no context, and controller-runtime's lazy mapper runs a
// discovery call when it has not seen a group, or has just forgotten it
// (see served.Rediscover). Handle wraps its mapper in one bound by its
// budget, so a discovery call that stalls ends with the budget like every
// other read, and the refusal says the webhook ran out of it (see
// readFailed).
type boundedMapper struct {
	meta.RESTMapper

	// ctx is Handle's context, which its budget bounds.
	ctx context.Context
}

// RESTMapping asks the wrapped mapper within ctx (see bounded).
func (m boundedMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return bounded(m.ctx, func() (*meta.RESTMapping, error) { return m.RESTMapper.RESTMapping(gk, versions...) })
}

// RESTMappings asks the wrapped mapper within ctx (see bounded).
func (m boundedMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	return bounded(m.ctx, func() ([]*meta.RESTMapping, error) { return m.RESTMapper.RESTMappings(gk, versions...) })
}

// bounded runs lookup and returns its answer, or an error once ctx ends
// first.
//
// Parameters:
//   - ctx bounds the wait.
//   - lookup is the mapper call, which can't be cancelled.
//
// It returns lookup's value and error when lookup answers first. When ctx
// ends first, it returns an error that wraps ctx's error, which is no
// no-match error, so served.Kind reports a *served.LookupError and the
// caller refuses. lookup keeps running in its goroutine until the mapper
// answers, and its answer then goes into the mapper's cache for the next
// request; the channel has room for it, so the goroutine never blocks.
func bounded[T any](ctx context.Context, lookup func() (T, error)) (T, error) {
	type answer struct {
		value T
		err   error
	}
	done := make(chan answer, 1)
	go func() {
		value, err := lookup()
		done <- answer{value: value, err: err}
	}()
	select {
	case a := <-done:
		return a.value, a.err
	case <-ctx.Done():
		var zero T
		return zero, fmt.Errorf("look up the served versions in the API server's discovery: %w", ctx.Err())
	}
}
