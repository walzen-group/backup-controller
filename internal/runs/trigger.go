// Package runs holds the one-shot operations: BackupRun takes a backup now,
// RestoreRun writes a chosen snapshot back. Both are reconciled here, and both
// follow the same rule: whatever the controller creates on the way, it removes
// before it reports a terminal phase, and a finalizer means deleting the run
// cannot skip that.
package runs

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FieldOwner names this controller on the writes it makes.
const FieldOwner = client.FieldOwner("backup-controller")

// TriggerFor returns the manual tag a run writes onto its source.
//
// It is derived from the run's UID, the way the populator derives its
// destination name from a claim's UID, so every step is idempotent: a
// controller that restarts mid-run recomputes the same tag, finds its own work
// in progress and continues, rather than starting a second backup.
func TriggerFor(uid types.UID) string {
	return "backuprun-" + string(uid)
}

// setManualTrigger writes the tag onto the source and touches nothing else.
//
// The schedule is deliberately left alone. A manual tag wins wherever both are
// set, so the schedule goes dormant on its own while the run lasts and resumes
// the moment the tag is cleared. Never writing the field keeps it entirely in
// the hands of whatever declares it, which is Flux for an app and tofu for a
// unit, and keeps this controller out of a fight it would lose on their next
// reconcile.
func setManualTrigger(ctx context.Context, c client.Client, source *volsyncv1alpha1.ReplicationSource, tag string) error {
	return patchTrigger(ctx, c, source, fmt.Sprintf("%q", tag))
}

// clearManualTrigger removes the tag this controller wrote.
//
// A JSON merge patch with an explicit null deletes the key outright, whoever
// set it. That is the property worth having here, because leaving a spent tag
// behind stops a volume's backups with nothing reporting the fact, and it must
// not depend on which manager happens to own the field.
func clearManualTrigger(ctx context.Context, c client.Client, source *volsyncv1alpha1.ReplicationSource) error {
	return patchTrigger(ctx, c, source, "null")
}

// patchTrigger sends a merge patch naming spec.trigger.manual and nothing else,
// so the schedule beside it is never in the payload.
func patchTrigger(ctx context.Context, c client.Client, source *volsyncv1alpha1.ReplicationSource, value string) error {
	body := fmt.Sprintf(`{"spec":{"trigger":{"manual":%s}}}`, value)
	return c.Patch(ctx, source, client.RawPatch(types.MergePatchType, []byte(body)), FieldOwner)
}

// manualTag reads the tag currently set on a source, empty when there is none.
func manualTag(source *volsyncv1alpha1.ReplicationSource) string {
	if source.Spec.Trigger == nil {
		return ""
	}
	return source.Spec.Trigger.Manual
}

// triggerHeldBy describes a trigger that belongs to something else, for the
// condition message. The holder is another run or a hand-written patch, and
// either way this run declines rather than writing over it.
func triggerHeldBy(source *volsyncv1alpha1.ReplicationSource) string {
	return fmt.Sprintf("ReplicationSource %s/%s already holds the manual trigger %q",
		source.Namespace, source.Name, manualTag(source))
}
