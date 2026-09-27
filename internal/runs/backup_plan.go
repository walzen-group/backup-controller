package runs

import (
	"context"
	"errors"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// plan records one Pending item for each thing the run backs up and moves the
// run to Queued. When items refuses the run (see asRunRefusal), plan ends
// the run as Failed with reason Invalid and the refusal as the message. Any
// other error, such as a timeout from the API server, is returned so the
// reconcile runs again.
//
// Before anything else, plan checks the installed BackupRun CRD (see
// schemaOutdated) and ends a run it refuses with reason CRDOutdated.
func (r *BackupRunReconciler) plan(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	if refused, err := r.schemaOutdated(ctx, run); refused || err != nil {
		return ctrl.Result{}, err
	}
	items, err := r.items(ctx, run)
	if asRunRefusal(err) {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	run.Status.Items = items
	run.Status.Phase = backupv1alpha1.RunPhaseQueued
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonQueued,
		"waiting for the backup queue to admit the run")
	return after(time.Second, r.writeStatus(ctx, run))
}

// schemaOutdated checks that the installed BackupRun CRD declares every
// field the controller writes on a BackupRun, such as status.restartPending,
// and ends the run as Failed with reason CRDOutdated when it does not. Items
// a planned run has not finished fail with the reason CRDOutdated and the
// same message.
//
// It returns true when it ended the run, and an error when the CRD could not
// be read in a way a retry may fix or when ending the run failed. The check
// writes nothing else, so it is safe to repeat.
func (r *BackupRunReconciler) schemaOutdated(ctx context.Context, run *backupv1alpha1.BackupRun) (bool, error) {
	err := crdOutdated(ctx, r.Reader, backupRunsCRD, backupv1alpha1.KindBackupRun, backupv1alpha1.BackupRun{},
		"the run could leave the workloads it stops at 0 replicas")
	var outdated *crdOutdatedError
	if !errors.As(err, &outdated) {
		return false, err
	}
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if backupItemOpen(*item) {
			item.Phase, item.Reason, item.Message = backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonCRDOutdated, outdated.Error()
		}
	}
	return true, r.finish(ctx, run, backupv1alpha1.ReasonCRDOutdated, outdated.Error())
}

// items returns one Pending item for each thing the run's spec names.
// spec.source names one claim, spec.database names one Cluster, and a run
// with neither takes every claim and Cluster in the namespace. A claim's item
// has kind ReplicationSource, after the VolSync object that backs it up.
//
// Everything the run backs up has to be marked backup.wlz.li/enabled: "true",
// so a run and a schedule cover the same set. items returns an
// *invalidSpecError when a named claim or Cluster is missing or not marked,
// and when nothing in the namespace is marked. A cluster without the
// CloudNativePG CRDs holds no Cluster. Any other failed read comes back as a
// plain error.
func (r *BackupRunReconciler) items(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	switch {
	case run.Spec.Source != "":
		return r.claimItem(ctx, run)
	case run.Spec.Database != "":
		return r.clusterItem(ctx, run)
	default:
		return r.namespaceItems(ctx, run)
	}
}

// pendingItem returns a Pending item of the given kind and name.
func pendingItem(kind, name string) backupv1alpha1.BackupItem {
	return backupv1alpha1.BackupItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
}

// claimItem returns the one item of a run whose spec.source names a claim.
//
// Parameters:
//   - run is the BackupRun to plan.
//
// It returns an *invalidSpecError when the claim is missing or not marked
// backup.wlz.li/enabled, and a plain error for any other failed read.
func (r *BackupRunReconciler) claimItem(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, invalidSpec("no claim %s in this namespace", run.Spec.Source)
		}
		return nil, fmt.Errorf("get claim %s: %w", run.Spec.Source, err)
	}
	if !backupv1alpha1.Enabled(claim.Annotations) {
		return nil, invalidSpec("claim %s is not marked %s: \"true\"", claim.Name, backupv1alpha1.AnnotationEnabled)
	}
	return []backupv1alpha1.BackupItem{pendingItem(backupv1alpha1.ItemKindSource, claim.Name)}, nil
}

// clusterItem returns the one item of a run whose spec.database names a
// Cluster.
//
// Parameters:
//   - run is the BackupRun to plan.
//
// It returns an *invalidSpecError when the Cluster is missing or not marked
// backup.wlz.li/enabled, and when the cluster has no CloudNativePG CRDs. It
// returns a plain error for any other failed read.
func (r *BackupRunReconciler) clusterItem(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Spec.Database)
	if err != nil {
		if meta.IsNoMatchError(err) {
			return nil, invalidSpec("no Cluster %s in this namespace; the cluster has no CloudNativePG CRDs", run.Spec.Database)
		}
		return nil, err
	}
	if !found {
		return nil, invalidSpec("no Cluster %s in this namespace", run.Spec.Database)
	}
	if !backupv1alpha1.Enabled(cluster.GetAnnotations()) {
		return nil, invalidSpec("the Cluster %s is not marked %s: \"true\"", cluster.GetName(), backupv1alpha1.AnnotationEnabled)
	}
	return []backupv1alpha1.BackupItem{pendingItem(backupv1alpha1.ItemKindCluster, cluster.GetName())}, nil
}

// namespaceItems returns one item for each claim and each Cluster in the
// run's namespace that is marked backup.wlz.li/enabled.
//
// Parameters:
//   - run is the BackupRun to plan.
//
// It returns an *invalidSpecError when nothing in the namespace is marked,
// and a plain error for a failed read.
func (r *BackupRunReconciler) namespaceItems(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
	if err != nil {
		return nil, err
	}
	clusters, err := cnpg.EnabledClusters(ctx, r.Reader, r.RESTMapper(), run.Namespace)
	if err != nil {
		return nil, err
	}
	var items []backupv1alpha1.BackupItem
	for _, claim := range claims {
		items = append(items, pendingItem(backupv1alpha1.ItemKindSource, claim.Name))
	}
	for _, cluster := range clusters {
		items = append(items, pendingItem(backupv1alpha1.ItemKindCluster, cluster.GetName()))
	}
	if len(items) == 0 {
		return nil, invalidSpec("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
	}
	return items, nil
}
