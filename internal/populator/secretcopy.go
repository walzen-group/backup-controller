package populator

import (
	"context"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// SecretCopyName returns the name of the copy of a claim's repository Secret
// in the controller namespace.
//
// Parameters:
//   - claimUID is the UID of the claim being filled. The name is the UID
//     itself, so two restores that run at once never share a copy.
func SecretCopyName(claimUID types.UID) string {
	return string(claimUID)
}

// SecretCopy builds a copy of a claim's repository Secret for the controller
// namespace. The restore Job runs there and takes its environment from a
// Secret in its own namespace, so the restore needs the copy there.
//
// Parameters:
//   - repo is the repository Secret in the app's namespace, the one the
//     VolumeRestore names.
//   - claimUID is the UID of the claim being restored. The copy is named
//     after it, through SecretCopyName.
//   - namespace is the controller namespace.
//
// It returns a Secret with the original's type and a deep copy of its data,
// and no labels or annotations.
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

// RestoreAsOf returns the moment a claim's restore goes back to.
//
// Parameters:
//   - vr is the VolumeRestore the claim names; its spec.restoreAsOf applies
//     to every claim that names it.
//   - claim is the claim being filled; its own backup.wlz.li/restore-as-of
//     annotation comes first. It may be nil.
//
// It returns the annotation's value, else spec.restoreAsOf, else nil, which
// restores the newest snapshot.
func RestoreAsOf(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) *string {
	if claim != nil {
		if value, ok := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; ok {
			return &value
		}
	}
	return vr.Spec.RestoreAsOf
}

// copySecret copies the repository Secret a VolumeRestore names into the
// controller namespace, for the restore Job of one claim.
//
// Parameters:
//   - vr is the VolumeRestore; spec.repository names the Secret.
//   - claim is the claim being filled; the Secret is read in its namespace
//     and the copy is named after its UID.
//
// It returns an error when the original can't be read or the copy can't be
// checked or created. A copy that already exists is left as it is.
func (c *Callbacks) copySecret(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) error {
	repo := &corev1.Secret{}
	if err := c.operations.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: vr.Spec.Repository}, repo); err != nil {
		return fmt.Errorf("get repository Secret %s/%s: %w", claim.Namespace, vr.Spec.Repository, err)
	}
	name := SecretCopyName(claim.UID)
	err := c.operations.Get(ctx, types.NamespacedName{Namespace: c.namespace, Name: name}, &corev1.Secret{})
	switch {
	case err == nil:
		return nil
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("check copied repository Secret %s/%s: %w", c.namespace, name, err)
	}
	if err := c.operations.Create(ctx, SecretCopy(repo, claim.UID, c.namespace)); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create copied repository Secret %s/%s: %w", c.namespace, name, err)
	}
	return nil
}
