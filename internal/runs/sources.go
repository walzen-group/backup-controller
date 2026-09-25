package runs

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// errSourceBusy is the error ensureSource returns when the claim's
// ReplicationSource carries another run's manual tag and VolSync hasn't
// finished that backup yet. The run waits and tries again later. If it wrote
// its own tag at that point, the tag would replace the first run's, and the
// first run would wait for a backup that is never taken.
var errSourceBusy = errors.New("the ReplicationSource is still completing another run's backup")

// refusal is an error that no retry can fix: something a run names is
// missing or not marked for backup, or a claim's settings don't give a
// source that VolSync can run. A BackupRun that meets one while it plans ends
// with reason Invalid, and an item that meets one fails. Any other error,
// such as a timeout from the API server, is returned so the reconcile runs
// again.
type refusal struct{ message string }

// Error returns the message, which names the object and what is wrong
// with it.
func (e refusal) Error() string { return e.message }

// refuse returns a refusal whose message is built from format and args the
// way fmt.Sprintf builds it.
func refuse(format string, args ...any) error {
	return refusal{fmt.Sprintf(format, args...)}
}

// isRefusal reports whether err, or an error it wraps, is a refusal or an
// invalidSetting, which no retry can fix either.
func isRefusal(err error) bool {
	var refused refusal
	var bad invalidSetting
	return errors.As(err, &refused) || errors.As(err, &bad)
}

// moverCPU is the CPU request on each backup mover. It sets the mover's share
// of CPU on a busy worker, and it is the amount the scheduler must find free
// on the worker that holds the volume.
var moverCPU = resource.MustParse("500m")

// enabledClaims lists the claims in a namespace that carry the annotation
// backup.wlz.li/enabled: "true", sorted by name. A claim that is being deleted
// is left out.
func enabledClaims(ctx context.Context, c client.Reader, namespace string) ([]corev1.PersistentVolumeClaim, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, claims, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the claims in %s: %w", namespace, err)
	}
	var enabled []corev1.PersistentVolumeClaim
	for _, claim := range claims.Items {
		if backupv1alpha1.Enabled(claim.Annotations) && claim.DeletionTimestamp.IsZero() {
			enabled = append(enabled, claim)
		}
	}
	sort.Slice(enabled, func(i, j int) bool { return enabled[i].Name < enabled[j].Name })
	return enabled, nil
}

// volumeRestoreFor returns the VolumeRestore that describes a claim's restic
// repository. For a claim whose spec.dataSourceRef names a VolumeRestore, it
// is that one. A fixed-name claim names none, and for it the VolumeRestore
// with the claim's own name is used.
//
// It returns a refusal naming the claim when that VolumeRestore doesn't
// exist, and a plain error when the read fails for another reason.
func volumeRestoreFor(ctx context.Context, c client.Reader, claim *corev1.PersistentVolumeClaim) (*backupv1alpha1.VolumeRestore, error) {
	name := claim.Name
	if ref := claim.Spec.DataSourceRef; ref != nil && ref.Kind == "VolumeRestore" {
		name = ref.Name
	}
	vr := &backupv1alpha1.VolumeRestore{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: name}, vr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, refuse("claim %s has no VolumeRestore %s to name its repository", claim.Name, name)
		}
		return nil, fmt.Errorf("get VolumeRestore %s/%s: %w", claim.Namespace, name, err)
	}
	return vr, nil
}

// volumeAffinity returns a node affinity that places a pod on the node that
// holds the claim's volume. It copies the required node affinity from the
// claim's PersistentVolume. zfs-localpv writes that affinity on every volume
// it provisions, and it holds whether or not a pod mounts the claim.
//
// It returns a refusal when the claim isn't bound yet, when its
// PersistentVolume doesn't exist, or when the PersistentVolume declares no
// required node affinity. A failed read of the PersistentVolume comes back as
// a plain error, which the caller retries.
func volumeAffinity(ctx context.Context, c client.Reader, claim *corev1.PersistentVolumeClaim) (*corev1.Affinity, error) {
	if claim.Spec.VolumeName == "" || claim.Status.Phase != corev1.ClaimBound {
		return nil, refuse("claim %s is not bound to a volume yet", claim.Name)
	}
	pv := &corev1.PersistentVolume{}
	if err := c.Get(ctx, types.NamespacedName{Name: claim.Spec.VolumeName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, refuse("claim %s is bound to the PersistentVolume %s, which does not exist", claim.Name, claim.Spec.VolumeName)
		}
		return nil, fmt.Errorf("get PersistentVolume %s: %w", claim.Spec.VolumeName, err)
	}
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return nil, refuse("the PersistentVolume %s declares no node affinity to place the mover by", pv.Name)
	}
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: pv.Spec.NodeAffinity.Required.DeepCopy(),
		},
	}, nil
}

// resticSpan matches a span in the form restic's --keep-within takes: one or
// more counts of years, months, days or hours, such as 30d or 1y6m.
var resticSpan = regexp.MustCompile(`^([0-9]+[ymdh])+$`)

// retention builds the restic retention policy for a claim's
// ReplicationSource from the claim's annotations: backup.wlz.li/retain-last,
// retain-hourly, retain-daily, retain-weekly, retain-monthly, retain-yearly
// and retain-within.
//
// It returns a refusal naming the claim and the annotation when a count isn't
// a positive integer, or when retain-within isn't a span such as 30d. It also
// returns a refusal when the claim sets none of them, because a policy with
// no rule would make restic keep every snapshot.
func retention(claim *corev1.PersistentVolumeClaim) (*volsyncv1alpha1.ResticRetainPolicy, error) {
	policy := &volsyncv1alpha1.ResticRetainPolicy{}
	counts := []struct {
		annotation string
		field      **int32
	}{
		{backupv1alpha1.AnnotationRetainHourly, &policy.Hourly},
		{backupv1alpha1.AnnotationRetainDaily, &policy.Daily},
		{backupv1alpha1.AnnotationRetainWeekly, &policy.Weekly},
		{backupv1alpha1.AnnotationRetainMonthly, &policy.Monthly},
		{backupv1alpha1.AnnotationRetainYearly, &policy.Yearly},
	}
	set := false

	// VolSync's CRD types the last count as a string and the other counts as
	// integers, so retain-last is parsed only to check it and is stored as
	// the annotation wrote it.
	if value, ok := claim.Annotations[backupv1alpha1.AnnotationRetainLast]; ok {
		if _, err := positiveCount(value); err != nil {
			return nil, refuse("claim %s has %s %q, which is not a positive count", claim.Name, backupv1alpha1.AnnotationRetainLast, value)
		}
		policy.Last = &value
		set = true
	}
	for _, c := range counts {
		value, ok := claim.Annotations[c.annotation]
		if !ok {
			continue
		}
		n, err := positiveCount(value)
		if err != nil {
			return nil, refuse("claim %s has %s %q, which is not a positive count", claim.Name, c.annotation, value)
		}
		*c.field = &n
		set = true
	}
	if value, ok := claim.Annotations[backupv1alpha1.AnnotationRetainWithin]; ok {
		if !resticSpan.MatchString(value) {
			return nil, refuse("claim %s has %s %q, which is not a span such as 30d or 1y6m", claim.Name, backupv1alpha1.AnnotationRetainWithin, value)
		}
		policy.Within = &value
		set = true
	}

	if !set {
		return nil, refuse("claim %s names no retention; set at least one of %s, %s, %s, %s, %s, %s or %s",
			claim.Name, backupv1alpha1.AnnotationRetainLast, backupv1alpha1.AnnotationRetainHourly,
			backupv1alpha1.AnnotationRetainDaily, backupv1alpha1.AnnotationRetainWeekly,
			backupv1alpha1.AnnotationRetainMonthly, backupv1alpha1.AnnotationRetainYearly,
			backupv1alpha1.AnnotationRetainWithin)
	}
	return policy, nil
}

// positiveCount parses a retention count from an annotation value. It returns
// an error unless the value is an integer of at least 1.
func positiveCount(value string) (int32, error) {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("not a positive count")
	}
	return int32(n), nil
}

// ensureSource creates or updates the ReplicationSource that backs up a claim,
// with the run's tag as its manual trigger. Setting the tag is what starts
// VolSync's backup.
//
// Parameters:
//   - c writes the ReplicationSource.
//   - reader reads the claim's VolumeRestore and PersistentVolume, the
//     Namespace, and any ReplicationSource that already exists.
//   - claim is the claim to back up. The ReplicationSource takes its name.
//   - tag is the run's manual trigger, from TriggerFor. VolSync starts a
//     backup when spec.trigger.manual holds a value it hasn't completed yet.
//
// It returns the ReplicationSource as written. While an existing source is
// still busy with another run's tag, it returns that source and
// errSourceBusy, and changes nothing. It returns a refusal, and writes
// nothing, when a ReplicationSource of the same name exists without the label
// app.kubernetes.io/managed-by: backup-controller, or when one of the
// settings below is missing or doesn't parse. It also returns a refusal when
// the API server rejects the source as invalid. Any other failed read or
// write comes back as a plain error, which the caller retries.
//
// The controller writes every field of the spec. The repository, the cache
// class and the mover's security context come from the claim's VolumeRestore.
// The retention comes from the claim's annotations, the prune interval from
// the namespace's annotation, and the mover's node affinity from the claim's
// volume. The source carries a controller reference to the claim, so a claim
// deleted for a restore takes its source with it, and the next run writes a
// new one.
//
// A source of the same name that this controller didn't write is left alone.
// Something else declares it, and writing over it would start a fight that the
// other writer wins on its next reconcile.
func ensureSource(ctx context.Context, c client.Client, reader client.Reader, claim *corev1.PersistentVolumeClaim, tag string) (*volsyncv1alpha1.ReplicationSource, error) {
	vr, err := volumeRestoreFor(ctx, reader, claim)
	if err != nil {
		return nil, err
	}
	affinity, err := volumeAffinity(ctx, reader, claim)
	if err != nil {
		return nil, err
	}
	retain, err := retention(claim)
	if err != nil {
		return nil, err
	}
	pruneInterval, err := pruneIntervalFor(ctx, reader, claim.Namespace)
	if err != nil {
		return nil, err
	}

	source := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: claim.Namespace},
	}
	existing := &volsyncv1alpha1.ReplicationSource{}
	err = reader.Get(ctx, client.ObjectKeyFromObject(source), existing)
	switch {
	case err == nil && existing.Labels[backupv1alpha1.LabelManagedBy] != backupv1alpha1.ManagedByValue:
		return nil, refuse("the ReplicationSource %s exists and was not written by backup-controller; remove it so the controller can write its own", claim.Name)
	case err == nil && busy(existing) && manualTag(existing) != tag:
		return existing, errSourceBusy
	case err != nil && !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("get ReplicationSource %s/%s: %w", claim.Namespace, claim.Name, err)
	}

	_, err = controllerutil.CreateOrUpdate(ctx, c, source, func() error {
		if source.Labels == nil {
			source.Labels = map[string]string{}
		}
		source.Labels[backupv1alpha1.LabelManagedBy] = backupv1alpha1.ManagedByValue
		if err := controllerutil.SetControllerReference(claim, source, c.Scheme()); err != nil {
			return err
		}
		source.Spec = volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: claim.Name,
			Trigger:   &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: tag},
			Restic: &volsyncv1alpha1.ReplicationSourceResticSpec{
				ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodClone,
					// VolSync builds the clone and the cache afresh on every
					// run, so both come from the class the VolumeRestore names
					// for its cache, whose reclaim policy is Delete.
					StorageClassName: vr.Spec.CacheStorageClassName,
				},
				Repository:            vr.Spec.Repository,
				PruneIntervalDays:     &pruneInterval,
				Retain:                retain,
				CacheCapacity:         vr.Spec.CacheCapacity,
				CacheStorageClassName: vr.Spec.CacheStorageClassName,
				MoverConfig: volsyncv1alpha1.MoverConfig{
					MoverSecurityContext: vr.Spec.MoverSecurityContext,
					MoverAffinity:        affinity,
					MoverResources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: moverCPU},
					},
				},
			},
		}
		return nil
	})
	if err != nil {
		if apierrors.IsInvalid(err) {
			return nil, refuse("the API server refused ReplicationSource %s/%s: %v", claim.Namespace, claim.Name, err)
		}
		return nil, fmt.Errorf("write ReplicationSource %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	return source, nil
}
