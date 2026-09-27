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

// AnnotationJobUID is the annotation on a prime claim that records the UID of
// the restore Job Populate created for it or took over. Populate resumes a
// Job, which restorejob.Build creates suspended, only once a later pass reads
// this record back and finds the Job's own UID in it.
const AnnotationJobUID = Prefix + "/restore-job-uid"

// PrimeClaimName returns the name of the prime claim the populator library
// creates in the controller namespace for the claim with the given UID. The
// library names it "prime-" followed by the claim's UID
// (lib-volume-populator v3.3.0 populator-machinery/controller.go:67 and :720).
func PrimeClaimName(claimUID types.UID) string {
	return "prime-" + string(claimUID)
}

// JobName returns the name of the restore Job that Populate creates in the
// controller namespace to fill the claim with the given UID.
//
// Parameters:
//   - claimUID is the UID of the app claim being filled. Every callback gets
//     the claim, so each finds the claim's Job again by this name.
//
// The name is "restore-" followed by the UID, 44 characters for a UID of 36,
// so it needs no shortening.
func JobName(claimUID types.UID) string {
	return "restore-" + string(claimUID)
}
