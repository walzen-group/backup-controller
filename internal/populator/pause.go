package populator

import (
	"context"
	"fmt"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

// Pause makes the callbacks start no new restore. main calls it once, before
// the library starts, when the controller runs with --pause.
//
// A claim whose restore has started goes on as before (see waitsForPause).
func (c *Callbacks) Pause() {
	c.paused = true
}

// waitsForPause reports whether a claim must wait because the controller
// runs with --pause.
//
// Parameters:
//   - params are the library's params for the claim, checked by
//     validateParams.
//
// It returns true when the callbacks are paused and the claim's restore has
// not started (see restoreStarted), and an error when a read fails. It
// writes nothing, and it logs the first wait of each claim once.
//
// Populate returns nil for a claim that waits, and Complete returns false.
// The library then requeues the claim with its rate limiter and records no
// Warning event (lib-volume-populator v3.3.0 populator-machinery
// controller.go:816-827 and :590-595). An error from Populate would record a
// Warning event on each sync (controller.go:809-813).
func (c *Callbacks) waitsForPause(ctx context.Context, params populatormachinery.PopulatorParams) (bool, error) {
	if !c.paused {
		return false, nil
	}
	started, err := c.restoreStarted(ctx, params.Pvc, params.PvcPrime)
	if err != nil || started {
		return false, err
	}
	if _, noted := c.pauseNoted.LoadOrStore(params.Pvc.UID, true); !noted {
		klog.InfoS("the controller runs with --pause; the claim waits for its restore", "claim", klog.KObj(params.Pvc))
	}
	return true, nil
}

// restoreStarted reports whether the populator has started the restore of a
// claim.
//
// Parameters:
//   - claim is the app claim. Its UID names the restore Job and labels the
//     Job's pods.
//   - prime is the claim's prime claim.
//
// It returns true when the prime claim records a restore Job
// (AnnotationJobUID), when the claim's restore Job exists, or when a pod of
// an earlier restore Job of the claim exists. It returns an error when a
// read fails.
func (c *Callbacks) restoreStarted(ctx context.Context, claim, prime *corev1.PersistentVolumeClaim) (bool, error) {
	if prime.Annotations[AnnotationJobUID] != "" {
		return true, nil
	}
	key := c.jobKey(claim.UID)
	_, err := c.operations.GetJob(ctx, key)
	switch {
	case err == nil:
		return true, nil
	case !apierrors.IsNotFound(err):
		return false, fmt.Errorf("read restore Job %s: %w", key, err)
	}
	pods, err := c.operations.ListClaimPods(ctx, c.namespace, claim.UID)
	if err != nil {
		return false, fmt.Errorf("list the restore pods of claim %s: %w", claim.UID, err)
	}
	return len(pods) > 0, nil
}
