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

// defaultTimeout and defaultPruneIntervalDays are the settings a namespace
// gets when it has no annotation for them, or an empty one. defaultTimeout is
// how long a BackupRun may work once admitted, and defaultPruneIntervalDays is
// how many days pass between prunes of a repository. An empty value counts as
// no annotation, so a Flux component can write the key from a substitution
// that defaults to "".
const (
	defaultTimeout           = 6 * time.Hour
	defaultPruneIntervalDays = int32(1)
)

// invalidSetting is the error for a namespace annotation that doesn't parse.
// The run or the item fails with a message that names the annotation, and
// the run doesn't retry, because no retry can fix the value.
type invalidSetting struct{ message string }

// Error returns the message, which names the namespace, the annotation and
// its value.
func (e invalidSetting) Error() string { return e.message }

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
// It returns an invalidSetting error when the annotation isn't a positive Go
// duration such as 10h, and any error from reading the Namespace.
func timeoutFor(ctx context.Context, reader client.Reader, run *backupv1alpha1.BackupRun) (time.Duration, error) {
	if run.Spec.Timeout != nil {
		return run.Spec.Timeout.Duration, nil
	}
	annotations, err := namespaceAnnotations(ctx, reader, run.Namespace)
	if err != nil {
		return 0, err
	}
	value := annotations[backupv1alpha1.AnnotationTimeout]
	if value == "" {
		return defaultTimeout, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, invalidSetting{fmt.Sprintf("namespace %s has %s %q, which is not a duration such as 10h", run.Namespace, backupv1alpha1.AnnotationTimeout, value)}
	}
	return d, nil
}

// pruneIntervalFor returns how many days pass between prunes of each
// repository that the namespace's ReplicationSources write. That is the
// namespace's backup.wlz.li/prune-interval-days annotation when set, or else
// defaultPruneIntervalDays.
//
// It returns an invalidSetting error when the annotation isn't a whole number
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
		return 0, invalidSetting{fmt.Sprintf("namespace %s has %s %q, which is not a positive count of days", namespace, backupv1alpha1.AnnotationPruneIntervalDays, value)}
	}
	return int32(n), nil
}
