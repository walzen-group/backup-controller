package populator

import "k8s.io/apimachinery/pkg/types"

// Prefix is the domain of the annotations and finalizers the populator library
// writes onto the claims it fills. It is the API group, so every mark this
// controller leaves on an object can be traced to this project. The binary
// passes it to the library as VolumePopulatorConfig.Prefix.
const Prefix = "backup.wlz.li"

// ClaimFinalizer is the finalizer the populator library puts on a claim once
// it has created the claim's prime claim, and removes once it has cleaned up
// after the claim. The library builds it as Prefix + "/" +
// "populate-target-protection" (lib-volume-populator v3.3.0
// populator-machinery/controller.go:69 and :318).
const ClaimFinalizer = Prefix + "/populate-target-protection"

// PrimeClaimName returns the name of the prime claim the populator library
// creates in the controller namespace for the claim with the given UID. The
// library names it "prime-" followed by the claim's UID
// (lib-volume-populator v3.3.0 populator-machinery/controller.go:67 and :720).
func PrimeClaimName(claimUID types.UID) string {
	return "prime-" + string(claimUID)
}

// DestinationName returns the name of the ReplicationDestination that
// Populate creates in the controller namespace for the claim with the given
// UID.
func DestinationName(claimUID types.UID) string {
	return "restore-" + string(claimUID)
}

// moverJobName returns the name of the Job VolSync runs for the restic mover
// of the ReplicationDestination named destination. VolSync names it
// "volsync-dst-" followed by the destination's name and owns it by the
// destination (volsync v0.16.0 internal/controller/mover/restic/mover.go:333-341).
// The Job's pods carry the label job-name with this value.
func moverJobName(destination string) string {
	return "volsync-dst-" + destination
}
