package runs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// defaultTimeout, defaultMaxQuiesce and defaultPruneIntervalDays are the
// settings a namespace gets when it has no annotation for them, or an empty
// one. defaultTimeout is how long a BackupRun may work once admitted,
// defaultMaxQuiesce is how long a BackupRun may keep the quiesced workloads
// stopped, and defaultPruneIntervalDays is how many days pass between prunes
// of a repository. An empty value counts as no annotation, so a Flux
// component can write the key from a substitution that defaults to "".
const (
	defaultTimeout           = 6 * time.Hour
	defaultMaxQuiesce        = 10 * time.Minute
	defaultPruneIntervalDays = int32(1)
)

// namespaceAnnotations returns the annotations of the Namespace with the
// given name. A Namespace that is gone reads as one without annotations, so
// the run falls back to the defaults.
func namespaceAnnotations(ctx context.Context, reader client.Reader, name string) (map[string]string, error) {
	namespace := &corev1.Namespace{}
	if err := reader.Get(ctx, types.NamespacedName{Name: name}, namespace); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get Namespace %s: %w", name, err)
	}
	return namespace.Annotations, nil
}

// timeoutFor returns how long a BackupRun may work once admitted. That is the
// run's own spec.timeout when set, or else the namespace's
// backup.wlz.li/timeout annotation, or else defaultTimeout.
//
// It returns a *refusalError with reason SettingsInvalid when the annotation isn't a positive Go
// duration such as 10h, and any error from reading the Namespace.
func timeoutFor(ctx context.Context, reader client.Reader, run *backupv1alpha1.BackupRun) (time.Duration, error) {
	if run.Spec.Timeout != nil {
		return run.Spec.Timeout.Duration, nil
	}
	return namespaceDuration(ctx, reader, run.Namespace, backupv1alpha1.AnnotationTimeout, defaultTimeout, "10h")
}

// maxQuiesceFor returns how long a BackupRun with spec.all set may keep the
// workloads marked backup.wlz.li/quiesce stopped, counted from
// status.quiescedAt. That is the namespace's backup.wlz.li/max-quiesce
// annotation when set, or else defaultMaxQuiesce.
//
// It returns a *refusalError with reason SettingsInvalid when the annotation isn't a positive Go
// duration such as 20m, and any error from reading the Namespace.
func maxQuiesceFor(ctx context.Context, reader client.Reader, namespace string) (time.Duration, error) {
	return namespaceDuration(ctx, reader, namespace, backupv1alpha1.AnnotationMaxQuiesce, defaultMaxQuiesce, "20m")
}

// namespaceDuration returns the duration that a namespace annotation sets.
//
// Parameters:
//   - annotation is the annotation to read.
//   - fallback is the duration when the annotation is not set or is empty.
//   - example is a valid value that the error message shows.
//
// It returns a *refusalError with reason SettingsInvalid when the value is not a positive Go
// duration, and any error from reading the Namespace.
func namespaceDuration(ctx context.Context, reader client.Reader, namespace, annotation string, fallback time.Duration, example string) (time.Duration, error) {
	annotations, err := namespaceAnnotations(ctx, reader, namespace)
	if err != nil {
		return 0, err
	}
	value := annotations[annotation]
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, refuse(backupv1alpha1.ItemReasonSettingsInvalid, "namespace %s has %s %q, which is not a duration such as %s", namespace, annotation, value, example)
	}
	return d, nil
}

// pruneIntervalFor returns how many days pass between prunes of each
// repository that the namespace's ReplicationSources write. That is the
// namespace's backup.wlz.li/prune-interval-days annotation when set, or else
// defaultPruneIntervalDays.
//
// It returns a *refusalError with reason SettingsInvalid when the annotation isn't a whole number
// of at least 1, and any error from reading the Namespace.
func pruneIntervalFor(ctx context.Context, reader client.Reader, namespace string) (int32, error) {
	annotations, err := namespaceAnnotations(ctx, reader, namespace)
	if err != nil {
		return 0, err
	}
	value := annotations[backupv1alpha1.AnnotationPruneIntervalDays]
	if value == "" {
		return defaultPruneIntervalDays, nil
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 1 {
		return 0, refuse(backupv1alpha1.ItemReasonSettingsInvalid, "namespace %s has %s %q, which is not a positive count of days", namespace, backupv1alpha1.AnnotationPruneIntervalDays, value)
	}
	return int32(n), nil
}
