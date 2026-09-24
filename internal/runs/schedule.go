package runs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// How long a scheduled run may work before it fails, and how long its record
// is kept afterwards.
const (
	scheduledTimeout = 12 * time.Hour
	scheduledTTL     = int32(30 * 24 * 60 * 60)
)

// refresh is the longest a namespace goes between two reconciles. Cluster
// annotations are not watched, so the restore-as-of series of a Cluster
// catches up within this.
const refresh = 5 * time.Minute

// Scheduler creates a BackupRun with all set at each tick of a namespace's
// backup.wlz.li/schedule, and exports the series the backup alerts read.
//
// It keeps no state of its own. The last tick a namespace ran for is the
// newest backup.wlz.li/scheduled-for label among its BackupRuns, so a
// controller that restarts picks up where the runs say it was. A tick missed
// while the controller was down runs once when it comes back; several missed
// ticks run once, for the newest.
type Scheduler struct {
	client.Client

	// Reader lists Clusters without the informer cache.
	Reader client.Reader

	// Now is the clock, injected so tests can move time without sleeping.
	Now func() time.Time
}

// SetupWithManager registers the scheduler. A BackupRun changing requeues its
// namespace, so the tick a finished run was holding back is taken at once.
func (s *Scheduler) SetupWithManager(mgr ctrl.Manager) error {
	if s.Now == nil {
		s.Now = time.Now
	}
	toNamespace := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: o.GetNamespace()}}}
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}).
		Watches(&backupv1alpha1.BackupRun{}, toNamespace).
		Watches(&corev1.PersistentVolumeClaim{}, toNamespace).
		Named("schedule").
		Complete(s)
}

// Reconcile creates the run a namespace's schedule is due for, and requeues
// for the next tick.
func (s *Scheduler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("namespace", req.Name)

	namespace := &corev1.Namespace{}
	if err := s.Get(ctx, req.NamespacedName, namespace); err != nil {
		if apierrors.IsNotFound(err) {
			forget(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := s.exportPinned(ctx, req.Name); err != nil {
		logger.Error(err, "cannot read the restore-as-of annotations")
	}

	spec, ok := namespace.Annotations[backupv1alpha1.AnnotationSchedule]
	if !ok || !namespace.DeletionTimestamp.IsZero() {
		scheduleLabels(req.Name).deleteAll()
		return ctrl.Result{RequeueAfter: refresh}, nil
	}
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		logger.Error(err, "cannot parse the backup schedule", "schedule", spec)
		scheduleInvalid.WithLabelValues(req.Name).Set(1)
		return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	}
	scheduleInvalid.WithLabelValues(req.Name).Set(0)

	runs := &backupv1alpha1.BackupRunList{}
	if err := s.List(ctx, runs, client.InNamespace(req.Name)); err != nil {
		return ctrl.Result{}, fmt.Errorf("list the BackupRuns in %s: %w", req.Name, err)
	}

	now := s.Now()
	s.exportSchedule(namespace, schedule, runs.Items, now)

	baseline := namespace.CreationTimestamp.Time
	unfinished := false
	for _, run := range runs.Items {
		if tick, err := strconv.ParseInt(run.Labels[backupv1alpha1.LabelScheduledFor], 10, 64); err == nil {
			if t := time.Unix(tick, 0); t.After(baseline) {
				baseline = t
			}
		}
		if run.Spec.All && !run.Status.Phase.Finished() {
			unfinished = true
		}
	}

	due := schedule.Next(baseline)
	if !due.After(now) {
		for next := schedule.Next(due); !next.After(now); next = schedule.Next(next) {
			due = next
		}
		// One namespace backup at a time: a second would find every source
		// busy and hold its workloads down waiting. The tick is taken as soon
		// as the running one finishes, because finishing requeues this.
		if unfinished {
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		if err := s.create(ctx, req.Name, due); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("created a scheduled backup", "tick", due.UTC().Format(time.RFC3339))
	}

	wait := min(max(schedule.Next(now).Sub(now), time.Second), refresh)
	return ctrl.Result{RequeueAfter: wait}, nil
}

// create writes the BackupRun for one tick. Its name and label both carry the
// tick, so a second reconcile for the same tick finds it already there.
func (s *Scheduler) create(ctx context.Context, namespace string, tick time.Time) error {
	timeout := metav1.Duration{Duration: scheduledTimeout}
	ttl := scheduledTTL
	run := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scheduled-" + tick.UTC().Format("20060102-1504"),
			Namespace: namespace,
			Labels:    map[string]string{backupv1alpha1.LabelScheduledFor: strconv.FormatInt(tick.Unix(), 10)},
		},
		Spec: backupv1alpha1.BackupRunSpec{All: true, Timeout: &timeout, TTLSecondsAfterFinished: &ttl},
	}
	if err := s.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create BackupRun %s/%s: %w", namespace, run.Name, err)
	}
	return nil
}

// exportSchedule sets the namespace's last success and its schedule interval.
//
// A namespace that has never finished a backup reports its creation time, so
// the overdue alert measures from when the namespace first asked for backups
// rather than firing on a namespace created a minute ago.
func (s *Scheduler) exportSchedule(namespace *corev1.Namespace, schedule cron.Schedule, runs []backupv1alpha1.BackupRun, now time.Time) {
	last := namespace.CreationTimestamp.Time
	for _, run := range runs {
		if run.Spec.All && run.Status.Phase == backupv1alpha1.RunPhaseSucceeded && run.Status.CompletedAt != nil && run.Status.CompletedAt.After(last) {
			last = run.Status.CompletedAt.Time
		}
	}
	lastSuccess.WithLabelValues(namespace.Name).Set(float64(last.Unix()))

	first := schedule.Next(now)
	scheduleInterval.WithLabelValues(namespace.Name).Set(schedule.Next(first).Sub(first).Seconds())
}

// exportPinned sets one series per claim and Cluster in the namespace carrying
// backup.wlz.li/restore-as-of, and drops the series of any that no longer
// does.
func (s *Scheduler) exportPinned(ctx context.Context, namespace string) error {
	restorePinned.DeletePartialMatch(prometheus.Labels{"namespace": namespace})

	claims := &corev1.PersistentVolumeClaimList{}
	if err := s.List(ctx, claims, client.InNamespace(namespace)); err != nil {
		return err
	}
	for _, claim := range claims.Items {
		if _, ok := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; ok {
			restorePinned.WithLabelValues(namespace, "PersistentVolumeClaim", claim.Name).Set(1)
		}
	}

	clusters := &unstructured.UnstructuredList{}
	clusters.SetGroupVersionKind(clusterListGV)
	if err := s.Reader.List(ctx, clusters, client.InNamespace(namespace)); err != nil {
		return err
	}
	for _, cluster := range clusters.Items {
		if _, ok := cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreAsOf]; ok {
			restorePinned.WithLabelValues(namespace, "Cluster", cluster.GetName()).Set(1)
		}
	}
	return nil
}

// scheduleLabels is the label set of one namespace's schedule series.
type scheduleLabels string

func (n scheduleLabels) deleteAll() {
	lastSuccess.DeleteLabelValues(string(n))
	scheduleInterval.DeleteLabelValues(string(n))
	scheduleInvalid.DeleteLabelValues(string(n))
}

// forget drops every series of a namespace that no longer exists.
func forget(namespace string) {
	scheduleLabels(namespace).deleteAll()
	restorePinned.DeletePartialMatch(prometheus.Labels{"namespace": namespace})
}
