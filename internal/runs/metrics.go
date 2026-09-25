package runs

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// lastSuccess, scheduleInterval, scheduleInvalid and restorePinned are the
// series the backup alerts read. They are registered in controller-runtime's
// registry, so the manager's metrics listener serves them. The populator
// library serves a registry of its own on another port, and nothing outside
// the library can register into that one.
var (
	lastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backup_controller_namespace_last_success_timestamp_seconds",
		Help: "When the namespace's newest backup of everything it marks enabled finished successfully, or when the namespace was created if none has.",
	}, []string{"namespace"})

	scheduleInterval = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backup_controller_namespace_schedule_interval_seconds",
		Help: "Seconds between two ticks of the namespace's backup schedule.",
	}, []string{"namespace"})

	scheduleInvalid = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backup_controller_namespace_schedule_invalid",
		Help: "1 while the namespace's backup.wlz.li/schedule annotation cannot be parsed, so no backup runs.",
	}, []string{"namespace"})

	restorePinned = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backup_controller_restore_pinned",
		Help: "1 for each claim or Cluster carrying backup.wlz.li/restore-as-of, which pins every later automatic restore of it to one moment.",
	}, []string{"namespace", "kind", "name"})
)

// init registers the series in controller-runtime's metrics registry.
func init() {
	metrics.Registry.MustRegister(lastSuccess, scheduleInterval, scheduleInvalid, restorePinned)
}
