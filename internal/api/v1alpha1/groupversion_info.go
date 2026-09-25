// Package v1alpha1 contains the backup.wlz.li API types.
//
// VolumeRestore is a standing declaration that fills a claim when the claim is
// created. BackupRun and RestoreRun are one-shot operations against volumes and
// databases that already exist. A BackupRun takes a backup now, and a
// RestoreRun writes a chosen backup back. docs/restores.md says which kind
// answers which question.
// +kubebuilder:object:generate=true
// +groupName=backup.wlz.li
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion is the group and version this package's types belong to.
var GroupVersion = schema.GroupVersion{Group: "backup.wlz.li", Version: "v1alpha1"}

// SchemeBuilder registers this package's types with a runtime.Scheme. It is
// built from apimachinery's runtime package alone, so importing the API types
// pulls in no controller-runtime code.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme adds this package's types to a runtime.Scheme.
var AddToScheme = SchemeBuilder.AddToScheme

// addKnownTypes registers the three kinds and their lists under GroupVersion,
// along with the metav1 types every group version needs.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&VolumeRestore{}, &VolumeRestoreList{},
		&BackupRun{}, &BackupRunList{},
		&RestoreRun{}, &RestoreRunList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
