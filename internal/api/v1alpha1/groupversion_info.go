// Package v1alpha1 contains the backup.wlz.li API types: VolumeRestore.
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

// SchemeBuilder registers this package's types with a runtime.Scheme. It depends
// only on apimachinery so the API package stays cheap to import.
var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

// AddToScheme adds this package's types to a runtime.Scheme.
var AddToScheme = SchemeBuilder.AddToScheme

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&VolumeRestore{}, &VolumeRestoreList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
