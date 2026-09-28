//go:build e2e

package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// loadNamespaces is the number of namespaces of the load scenario, about as
// many as prod runs. admittedAtOnce is the quota of the ClusterQueue backup
// (hack/kind/manifests/cluster.yaml), and memoryLimit is the controller's
// memory limit on prod.
const (
	loadNamespaces = 70
	admittedAtOnce = 5
	memoryLimit    = "512Mi"
)

// loadManifests returns the objects of one app of the load scenario: the
// claim data with its VolumeRestore, and a Deployment that writes one file
// into the claim when it starts. They are applied without Flux, since the
// scenario only backs them up.
func loadManifests() string {
	return fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: data
spec:
  repository: restic
  cacheStorageClassName: %[1]s
  cacheCapacity: 100Mi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  annotations:
    backup.wlz.li/enabled: "true"
    backup.wlz.li/retain-last: "3"
spec:
  storageClassName: %[1]s
  accessModes: [ReadWriteOnce]
  resources:
    requests: {storage: 100Mi}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  annotations:
    %[2]s: "true"
spec:
  replicas: 1
  selector:
    matchLabels: {app: app}
  template:
    metadata:
      labels: {app: app}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: app
          image: %[3]s
          command: [sh, -c, "echo load > /data/note && sync && exec sleep 86400"]
          volumeMounts: [{name: data, mountPath: /data}]
          resources:
            requests: {cpu: 1m, memory: 8Mi}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: data}
`, storageClass, pauseAnnotation, appImage)
}

// loadSample is what the load scenario reads from the cluster every five
// seconds.
type loadSample struct {
	// admitted is the number of Workloads Kueue has admitted and that have
	// not finished.
	admitted int
	// memory is the working set of the controller's container, in bytes.
	memory int64
}

// sampleLoad reads one loadSample.
//
// It counts the admitted Workloads in all namespaces (see admittedWorkloads),
// and reads the memory of the controller's container from the kubelet's
// summary of its node. It returns an error when either read fails.
func sampleLoad(t *testing.T) (loadSample, error) {
	admitted, err := admittedWorkloads(t)
	if err != nil {
		return loadSample{}, err
	}
	memory, err := controllerMemory(t)
	return loadSample{admitted: admitted, memory: memory}, err
}

// admittedWorkloads returns how many Kueue Workloads in all namespaces are
// admitted and not finished: the runs and populator restores that hold a
// slot of the queue right now. It returns an error when the list fails.
func admittedWorkloads(t *testing.T) (int, error) {
	var workloads struct {
		Items []struct {
			Status struct {
				Conditions []struct{ Type, Status string } `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	out, err := run(t.Context(), "", "get", "workloads.kueue.x-k8s.io", "-A", "-o", "json")
	if err != nil {
		return 0, err
	}
	if err := jsonInto(out, &workloads); err != nil {
		return 0, err
	}
	count := 0
	for _, w := range workloads.Items {
		admitted, finished := false, false
		for _, c := range w.Status.Conditions {
			admitted = admitted || (c.Type == "Admitted" && c.Status == "True")
			finished = finished || (c.Type == "Finished" && c.Status == "True")
		}
		if admitted && !finished {
			count++
		}
	}
	return count, nil
}

// controllerMemory returns the working set of the controller's container, in
// bytes, as the kubelet of its node reports it in its stats summary.
func controllerMemory(t *testing.T) (int64, error) {
	var pods struct {
		Items []struct {
			Metadata struct{ Name string }     `json:"metadata"`
			Spec     struct{ NodeName string } `json:"spec"`
		} `json:"items"`
	}
	out, err := run(t.Context(), "", "-n", controllerNamespace, "get", "pods", "-l", controllerSelector, "-o", "json")
	if err != nil {
		return 0, err
	}
	if err := jsonInto(out, &pods); err != nil || len(pods.Items) == 0 {
		return 0, fmt.Errorf("no controller pod: %v", err)
	}
	pod := pods.Items[0]
	var summary struct {
		Pods []struct {
			PodRef struct {
				Name, Namespace string
			} `json:"podRef"`
			Containers []struct {
				Memory struct {
					WorkingSetBytes int64 `json:"workingSetBytes"`
				} `json:"memory"`
			} `json:"containers"`
		} `json:"pods"`
	}
	out, err = run(t.Context(), "", "get", "--raw", "/api/v1/nodes/"+pod.Spec.NodeName+"/proxy/stats/summary")
	if err != nil {
		return 0, err
	}
	if err := jsonInto(out, &summary); err != nil {
		return 0, err
	}
	for _, p := range summary.Pods {
		if p.PodRef.Name == pod.Metadata.Name && p.PodRef.Namespace == controllerNamespace && len(p.Containers) > 0 {
			return p.Containers[0].Memory.WorkingSetBytes, nil
		}
	}
	return 0, fmt.Errorf("the summary of node %s has no pod %s", pod.Spec.NodeName, pod.Metadata.Name)
}

// TestSeventyNamespacesBackUpAtOneTick gives 70 namespaces the same schedule,
// so their BackupRuns are created in the same minute, with the controller
// limited to 512 MiB as on prod. Kueue admits at most five runs at once,
// every run succeeds, the controller is never restarted or OOM killed, and
// the test logs the controller's highest memory use.
func TestSeventyNamespacesBackUpAtOneTick(t *testing.T) {
	kubectl(t, "", "-n", controllerNamespace, "set", "resources", "deployment/backup-controller", "--limits=memory="+memoryLimit)
	kubectl(t, "", "-n", controllerNamespace, "rollout", "status", "deployment/backup-controller", "--timeout=3m")

	apps := make([]*app, loadNamespaces)
	for i := range apps {
		apps[i] = newApp(t, fmt.Sprintf("load-%02d", i))
		kubectl(t, loadManifests(), "-n", apps[i].ns, "apply", "-f", "-")
	}
	for _, a := range apps {
		waitFor(t, "the app of "+a.ns+" to write its file", 10*time.Minute, func() (bool, string) {
			out, err := run(t.Context(), "", "-n", a.ns, "exec", "deploy/app", "--", "cat", "/data/note")
			return err == nil && firstLine(out) == "load", firstLine(out)
		}, a.describe)
	}

	// The tick is two minutes ahead, so every namespace carries the schedule
	// before it is due.
	tick := time.Now().UTC().Add(2 * time.Minute).Truncate(time.Minute)
	schedule := fmt.Sprintf("CRON_TZ=UTC %d %d * * *", tick.Minute(), tick.Hour())
	for _, a := range apps {
		kubectl(t, "", "annotate", "--overwrite", "namespace", a.ns, backupv1alpha1.AnnotationSchedule+"="+schedule)
	}
	t.Logf("70 namespaces are due at %s", tick.Format(time.RFC3339))

	var mu sync.Mutex
	most, peak := 0, int64(0)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			if s, err := sampleLoad(t); err == nil {
				mu.Lock()
				most, peak = max(most, s.admitted), max(peak, s.memory)
				mu.Unlock()
			}
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	for _, a := range apps {
		waitFor(t, "the scheduled BackupRun of "+a.ns+" to end", 60*time.Minute, func() (bool, string) {
			var runs struct {
				Items []backupv1alpha1.BackupRun `json:"items"`
			}
			out, err := run(t.Context(), "", "-n", a.ns, "get", "backupruns", "-o", "json")
			if err != nil || jsonInto(out, &runs) != nil || len(runs.Items) == 0 {
				return false, "no run yet"
			}
			r := runs.Items[0]
			if r.Status.Phase.Finished() && r.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
				t.Errorf("the scheduled run of %s ended %s: %s", a.ns, r.Status.Phase, ready(r.Status.Conditions))
			}
			return r.Status.Phase.Finished(), string(r.Status.Phase)
		}, a.describe)
	}
	close(stop)
	<-done

	t.Logf("at most %d runs were admitted at once; the controller used at most %d MiB", most, peak>>20)
	if most > admittedAtOnce {
		t.Errorf("%d runs were admitted at once, want at most %d", most, admittedAtOnce)
	}
	var pods struct {
		Items []struct {
			Status struct {
				ContainerStatuses []struct {
					RestartCount int `json:"restartCount"`
					LastState    struct {
						Terminated *struct{ Reason string } `json:"terminated"`
					} `json:"lastState"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	out := kubectl(t, "", "-n", controllerNamespace, "get", "pods", "-l", controllerSelector, "-o", "json")
	if err := jsonInto(out, &pods); err != nil {
		t.Fatal(err)
	}
	for _, p := range pods.Items {
		for _, c := range p.Status.ContainerStatuses {
			if c.RestartCount > 0 {
				reason := ""
				if c.LastState.Terminated != nil {
					reason = c.LastState.Terminated.Reason
				}
				t.Errorf("the controller restarted %d times during the load, last with %q", c.RestartCount, reason)
			}
		}
	}
}
