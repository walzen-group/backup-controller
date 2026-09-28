// Package volsync copies a claim's VolSync repository Secret into the
// controller namespace, where the restore Job that fills the claim reads it.
package volsync

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// SecretCopyName returns the name of the copy of a claim's repository Secret in
// the controller namespace.
//
// Parameters:
//   - claimUID is the UID of the claim being restored. The name is this UID,
//     so two restores that run at once never share a copy.
//
// The restore Job reads its repository settings from the Secret of this
// name.
func SecretCopyName(claimUID types.UID) string {
	return string(claimUID)
}

// SecretCopy builds a copy of a claim's repository Secret for the controller
// namespace.
//
// Parameters:
//   - repo is the repository Secret in the app's namespace, the one the
//     VolumeRestore refers to.
//   - claimUID is the UID of the claim being restored. The copy is named after
//     it, through SecretCopyName.
//   - namespace is the controller namespace.
//
// It returns the copy. The copy has the Secret's type and a deep copy of its
// data, and no labels or annotations.
//
// A pod reads a Secret only from its own namespace. The restore Job runs in
// the controller namespace, so the restore needs the copy there.
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
