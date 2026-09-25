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

// The settings a namespace falls back to when it carries no annotation for
// them, or an empty one: how long a run may work once admitted, and how many
// days pass between prunes of a repository. An empty value counts as none so a
// Flux component can write the key from a substitution defaulting to "".
const (
	defaultTimeout           = 6 * time.Hour
	defaultPruneIntervalDays = int32(1)
)

// invalidSetting is a namespace annotation that does not parse. The run or the
// item fails naming it; nothing a retry does can fix it.
type invalidSetting struct{ message string }

func (e invalidSetting) Error() string { return e.message }

// namespaceAnnotations reads the namespace's annotations. A namespace that is
// gone reads as one with none, so the run falls back to the defaults.
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

// timeoutFor is how long the run may work once admitted: its own spec.timeout,
// else the namespace's annotation, else defaultTimeout.
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

// pruneIntervalFor is how many days pass between prunes of the repositories
// the namespace's sources write: the namespace's annotation, else
// defaultPruneIntervalDays.
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
