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
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// scheduledTTL is the spec.ttlSecondsAfterFinished of each BackupRun the
// scheduler creates: 30 days, in seconds. The scheduler sets no spec.timeout,
// so a scheduled run takes its timeout from the namespace, the same way a
// manual run does.
const scheduledTTL = int32(30 * 24 * 60 * 60)

// refresh is the longest the scheduler waits before it reconciles a namespace
// again. The scheduler doesn't watch Clusters, so when a Cluster's
// backup.wlz.li/restore-as-of annotation changes, the
// backup_controller_restore_pinned series catches up within this time. The
// exception is a schedule that doesn't parse, which is read again after 10
// minutes.
const refresh = 5 * time.Minute

// Scheduler is the reconciler that runs each namespace's backup schedule. At
// each tick of the cron schedule in a Namespace's backup.wlz.li/schedule
// annotation, it creates a BackupRun with spec.all set. It also exports the
// metrics that the backup alerts read.
//
// It keeps no state of its own. The last tick it ran for a namespace is the
// newest backup.wlz.li/scheduled-for label among the namespace's BackupRuns,
// so a controller that restarts picks up where the runs say it was. The ticks
// missed while the controller was down produce one run, for the newest of
// them. When a tick is due and nothing in the namespace carries
// backup.wlz.li/enabled: "true", the scheduler creates no run and records a
// Warning event on the Namespace.
type Scheduler struct {
	client.Client

	// Reader reads CloudNativePG Clusters. The manager passes its API reader,
	// which reads from the API server without the informer cache.
	Reader client.Reader

	// Recorder records the Warning event on a Namespace whose tick is due
	// while nothing in it carries backup.wlz.li/enabled: "true". When it is
	// nil, no event is recorded.
	Recorder events.EventRecorder

	// Now returns the current time. Tests set it to move the clock without
	// sleeping, and SetupWithManager sets it to time.Now when it is nil.
	Now func() time.Time
}

// SetupWithManager registers the scheduler with the manager. It reconciles
// Namespaces, and a change to a BackupRun or a claim requeues the namespace
// that holds it. A finished run therefore lets the tick it was holding back
// run at once, and a newly marked claim lets a waiting tick run.
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

// Reconcile creates the BackupRun that a namespace's schedule is due for, if
// there is one, and requeues the namespace for its next tick.
//
// Each reconcile also exports the namespace's metrics: the restore-pinned
// series and, for a namespace with a schedule, the last success, the schedule
// interval and whether the schedule parses. A namespace without
// backup.wlz.li/schedule, or one being deleted, loses its schedule series. A
// namespace that no longer exists loses all of its series.
//
// A due tick waits while a BackupRun with spec.all set is unfinished, and
// while nothing in the namespace is marked enabled. Reconcile returns an
// error when the BackupRuns can't be listed or the new run can't be created.
// A schedule that doesn't parse is logged and sets the schedule-invalid series
// to 1, with no error returned.
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
		// Only one namespace backup runs at a time. A second one would find
		// every source busy, and it would hold its workloads down while it
		// waited. The tick runs as soon as the current run finishes, because
		// the change to that BackupRun requeues the namespace.
		if unfinished {
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		// Flux creates a Namespace before the claims in it, so a tick can
		// already be due when the schedule arrives. The tick waits here until
		// something is marked enabled. A marked claim requeues the namespace
		// at once, and a marked Cluster is seen at the next refresh.
		marked, err := s.anythingEnabled(ctx, req.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
		if marked {
			if err := s.create(ctx, req.Name, due); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("created a scheduled backup", "tick", due.UTC().Format(time.RFC3339))
		} else if s.Recorder != nil {
			s.Recorder.Eventf(namespace, nil, corev1.EventTypeWarning, "NothingEnabled", "Schedule",
				"the tick at %s is due, but nothing in this namespace is marked %s: \"true\"",
				due.UTC().Format(time.RFC3339), backupv1alpha1.AnnotationEnabled)
		}
	}

	wait := min(max(schedule.Next(now).Sub(now), time.Second), refresh)
	return ctrl.Result{RequeueAfter: wait}, nil
}

// anythingEnabled reports whether a claim or a Cluster in the namespace
// carries backup.wlz.li/enabled: "true". A BackupRun with spec.all set fails
// when nothing does, so the scheduler checks this before it creates one.
func (s *Scheduler) anythingEnabled(ctx context.Context, namespace string) (bool, error) {
	claims, err := enabledClaims(ctx, s.Client, namespace)
	if err != nil || len(claims) > 0 {
		return len(claims) > 0, err
	}
	clusters, err := enabledClusters(ctx, s.Reader, namespace)
	return len(clusters) > 0, err
}

// create creates the BackupRun for one tick of the namespace's schedule. The
// run is named scheduled-YYYYMMDD-HHMM after the tick in UTC, and its
// backup.wlz.li/scheduled-for label holds the tick in Unix seconds. Because
// the name comes from the tick, a second reconcile for the same tick gets
// AlreadyExists, which create treats as success.
func (s *Scheduler) create(ctx context.Context, namespace string, tick time.Time) error {
	ttl := scheduledTTL
	run := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scheduled-" + tick.UTC().Format("20060102-1504"),
			Namespace: namespace,
			Labels:    map[string]string{backupv1alpha1.LabelScheduledFor: strconv.FormatInt(tick.Unix(), 10)},
		},
		Spec: backupv1alpha1.BackupRunSpec{All: true, TTLSecondsAfterFinished: &ttl},
	}
	if err := s.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create BackupRun %s/%s: %w", namespace, run.Name, err)
	}
	return nil
}

// exportSchedule sets the namespace's last-success and schedule-interval
// series.
//
// The last success is the newest completion time among the namespace's
// succeeded BackupRuns with spec.all set. A namespace that has never finished
// such a backup reports its creation time, so the overdue alert measures from
// when the namespace first asked for backups, and a namespace created a
// minute ago doesn't fire it. The interval is the time between the next two
// ticks after now.
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

// exportPinned sets the restore-pinned series to 1 for each claim and Cluster
// in the namespace that carries backup.wlz.li/restore-as-of. It first deletes
// all of the namespace's restore-pinned series, so an object that has lost
// the annotation loses its series. It returns an error when the claims or the
// Clusters can't be listed.
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

// scheduleLabels is a namespace name, used as the label value of that
// namespace's schedule series.
type scheduleLabels string

// deleteAll deletes the namespace's last-success, schedule-interval and
// schedule-invalid series.
func (n scheduleLabels) deleteAll() {
	lastSuccess.DeleteLabelValues(string(n))
	scheduleInterval.DeleteLabelValues(string(n))
	scheduleInvalid.DeleteLabelValues(string(n))
}

// forget deletes every series of a namespace. Reconcile calls it when the
// Namespace no longer exists.
func forget(namespace string) {
	scheduleLabels(namespace).deleteAll()
	restorePinned.DeletePartialMatch(prometheus.Labels{"namespace": namespace})
}
