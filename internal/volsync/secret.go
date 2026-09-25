package volsync

import (
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
	copy := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SecretCopyName(claimUID),
			Namespace: namespace,
		},
		Type: repo.Type,
		Data: make(map[string][]byte, len(repo.Data)),
	}
	for key, value := range repo.Data {
		copy.Data[key] = append([]byte(nil), value...)
	}
	return copy
}
