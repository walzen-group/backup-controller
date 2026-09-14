package volsync

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// SecretCopyName is the name the repository Secret's copy carries in the
// controller namespace. It is the claim's UID, so two restores running at once
// never collide, and it is the name the ReplicationDestination resolves.
func SecretCopyName(claimUID types.UID) string {
	return string(claimUID)
}

// SecretCopy builds the repository Secret in the controller namespace.
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
