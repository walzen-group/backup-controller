package volsync

import (
	"fmt"
	"hash/crc32"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// SecretCopyName returns the name of the copy of a claim's repository Secret in
// the controller namespace. The name is the claim's UID, so two restores that
// run at once never share a copy. It's also the Secret name that the
// ReplicationDestination built by New refers to.
func SecretCopyName(claimUID types.UID) string {
	return string(claimUID)
}

// MoverJobName returns the name of the Job VolSync runs for the restic mover
// of a ReplicationDestination.
//
// Parameters:
//   - destination is the name of the ReplicationDestination.
//
// The name is "volsync-dst-" followed by the destination's name while that
// fits in 63 characters. A longer one is "volsync-dst-" followed by the crc32
// (IEEE) of the destination's name as eight hex digits, the way VolSync
// shortens it (volsync v0.16.0 internal/controller/utils/utils.go:386-399,
// called from internal/controller/mover/restic/mover.go:333-341). VolSync
// owns the Job by the destination, and the Job's pods carry the label
// job-name with this value.
func MoverJobName(destination string) string {
	const prefix = "volsync-dst-"
	if name := prefix + destination; len(name) <= 63 {
		return name
	}
	return prefix + fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(destination)))
}

// SecretCopy builds a copy of a claim's repository Secret for the controller
// namespace. VolSync looks up a ReplicationDestination's repository Secret in
// the destination's own namespace, which is the controller's, so the restore
// needs the copy there.
//
// Parameters:
//   - repo is the repository Secret in the app's namespace, the one the
//     VolumeRestore names.
//   - claimUID is the UID of the claim being restored. The copy is named after
//     it, through SecretCopyName.
//   - namespace is the controller namespace.
//
// The copy carries the Secret's type and a deep copy of its data, and no
// labels or annotations.
func SecretCopy(repo *corev1.Secret, claimUID types.UID, namespace string) *corev1.Secret {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SecretCopyName(claimUID),
			Namespace: namespace,
		},
		Type: repo.Type,
		Data: make(map[string][]byte, len(repo.Data)),
	}
	for key, value := range repo.Data {
		secret.Data[key] = append([]byte(nil), value...)
	}
	return secret
}
