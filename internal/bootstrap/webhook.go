package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/synced"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/go-logr/logr"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// WebhookPath is the URL path the admission server serves Decider on. The
	// MutatingWebhookConfiguration in deploy/ points at the same path.
	WebhookPath = "/mutate-postgresql-cnpg-io-v1-cluster"

	// PluginName is the name of the Barman Cloud plugin. Archiver looks for it
	// in a Cluster's spec.plugins, and setRecovery writes it into the
	// externalClusters entry it adds.
	PluginName = "barman-cloud.cloudnative-pg.io"

	// SkipCheckAnnotation lets a recovered database archive into the prefix it
	// restored from. CloudNativePG refuses to archive into a non-empty prefix
	// from a freshly bootstrapped database, and every restore is one.
	// setRecovery sets it to "enabled".
	SkipCheckAnnotation = "cnpg.io/skipEmptyWalArchiveCheck"

	// OptOutAnnotation, set to OptOutValue on a Cluster, asks for an empty
	// database whatever the object store holds. Without it there is no way to
	// discard a database, because deleting the Cluster would restore it again.
	OptOutAnnotation = "backup.wlz.li/bootstrap"

	// OptOutValue is the value OptOutAnnotation must have to opt out. The
	// webhook ignores any other value, so a typo can't silently leave a
	// database empty.
	OptOutValue = "initdb"

	// RecoverySource is the name of the externalClusters entry this webhook
	// adds, and the value it writes to spec.bootstrap.recovery.source. The two
	// have to match, and no other component uses the name, so one constant
	// serves both. keepRecovery reads the source back to recognise a
	// Cluster this webhook recovered.
	RecoverySource = "backup-controller"
)

// Decider is the mutating admission webhook for CloudNativePG Clusters. It
// chooses a new Cluster's bootstrap at admission time, and it keeps a
// recovered Cluster valid when Flux applies it again.
type Decider struct {
	// Client reads ObjectStores, Secrets, Clusters and RestoreRuns. The
	// handler only ever reads, so a client.Reader is enough.
	// cmd/backup-controller passes the manager's uncached API reader, which
	// keeps the controller's Secret grant at get. A cached client would need
	// list and watch on every Secret in the cluster and would hold them all
	// in memory.
	Client client.Reader
	// Prober asks the object store which base backups exist.
	// cmd/backup-controller passes S3Prober, and the tests pass a stub.
	Prober Prober
	// Snapshots lists the snapshots of the namespace's restic repositories,
	// to find the namespace's paused moment for a Cluster created without a
	// RestoreRun (see automaticRecovery).
	Snapshots restic.Lister
}

// Handle answers one admission request for a Cluster. For a new Cluster whose
// object store already holds a base backup, it rewrites the Cluster to recover
// from that backup.
//
// The req argument is the admission request. Its Operation, DryRun, Object and
// OldObject decide what happens:
//   - An update goes to keepRecovery, on a dry run too.
//   - Any operation other than create or update is allowed unchanged.
//   - A dry-run create is allowed unchanged without reading anything.
//   - A create is allowed unchanged when the Cluster has no archiving plugin
//     (see Archiver).
//   - A create is refused when the ObjectStore can't be resolved, or when
//     another Cluster anywhere already archives to the same bucket and prefix.
//   - A Cluster that carries OptOutAnnotation set to OptOutValue starts
//     empty when nothing is archived under its prefix, and is refused
//     otherwise (see optedOut).
//   - A Cluster that declares its own spec.bootstrap.recovery is allowed
//     unchanged, unless a RestoreRun is waiting for it. Then it's refused.
//   - With no completed base backup in the store, the create is decided by
//     withoutBaseBackup: refused when a RestoreRun waits or when the archive
//     holds WAL, and allowed unchanged, to start empty, when nothing at all
//     is archived.
//   - A create is refused when the RestoreRun's item names a base backup to
//     stop at that the store no longer holds (see recoveryFor).
//   - Otherwise the response patches the Cluster to recover from its
//     archive (see setRecovery). With a RestoreRun, the recovery stops at the
//     end of the item's base backup when the item says so, and replays the
//     whole archive otherwise. Without one, it stops at the end of the base
//     backup of the namespace's paused moment when the whole namespace comes
//     back from that moment (see automaticRecovery), and replays the whole
//     archive otherwise. When a RestoreRun waits for the Cluster, the patch also
//     sets the backup.wlz.li/restore-run annotation to the run's name.
//
// Failures to read the store, list Clusters or RestoreRuns, or talk to the
// object store return an HTTP 500 error response, which the API server treats
// as a refusal. A body that isn't a valid Cluster returns an HTTP 400.
func (d *Decider) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues(
		"cluster", fmt.Sprintf("%s/%s", req.Namespace, req.Name),
	)

	if req.Operation == admissionv1.Update {
		return keepRecovery(req)
	}
	if req.Operation != admissionv1.Create {
		return admission.Allowed("not a creation or an update")
	}

	// A dry-run decision is thrown away: the API server never persists a
	// mutating patch from a dry-run request, so choosing a bootstrap here
	// changes nothing.
	//
	// Enforcing here does do something, and it is a deadlock. Flux dry-runs
	// every object in a Kustomization before it applies any of them, so on an
	// app's first deploy this handler is asked about a Cluster whose
	// ObjectStore sits in the same set and does not exist yet. Refusing fails
	// the dry-run, Flux applies nothing, the ObjectStore is never created, and
	// the next reconcile asks the same question and gets the same answer.
	//
	// The real request arrives with DryRun unset and is decided in full.
	if req.DryRun != nil && *req.DryRun {
		return admission.Allowed("dry run")
	}

	cluster := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.Object.Raw, cluster); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// A Cluster deleted a moment ago leaves its instance pod shutting down
	// and its PVCs until the garbage collector removes them. A new Cluster of
	// the same name would meet them, so it waits: Flux tries the create again
	// on its next reconcile.
	left, err := InstanceLeft(ctx, d.Client, req.Namespace, req.Name)
	if err != nil {
		logger.Error(err, "cannot check for an earlier instance")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if left != "" {
		logger.Info("refusing the Cluster", "reason", "an earlier instance is left", "left", left)
		return admission.Denied(fmt.Sprintf("%s of an earlier Cluster %s/%s still exists. The create is refused until it is gone; Flux applies the Cluster again on its next reconcile.", left, req.Namespace, req.Name))
	}

	_, declared, _ := unstructured.NestedMap(cluster.Object, "spec", "bootstrap", "recovery")

	store, serverName, found := Archiver(cluster)
	if !found {
		logger.Info("leaving the Cluster alone", "reason", "it archives nowhere")
		return admission.Allowed("no archiving plugin")
	}

	at, err := ResolveLocation(ctx, d.Client, req.Namespace, store, serverName)
	if err != nil {
		// The store is named and unreadable. Refusing is the failure that gets
		// noticed; allowing would create an empty database beside a full
		// archive and report success.
		logger.Error(err, "cannot read the object store")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Two databases archiving to one prefix interleave their WAL and leave the
	// archive unrestorable, which is silent and permanent. Nothing else on the
	// cluster can see this coming: each Cluster is valid on its own, and the
	// pair is the problem. Where a Cluster archives does not depend on how it
	// bootstraps, so a Cluster declaring its own recovery is checked too.
	holder, err := archiveHolder(ctx, d.Client, req.Namespace, req.Name, at)
	if err != nil {
		logger.Error(err, "cannot check which databases archive here")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if holder != "" {
		logger.Info("refusing the Cluster", "reason", "another database archives here", "holder", holder, "prefix", at.Prefix)
		return admission.Denied(fmt.Sprintf(
			"%s already archives to %s/%s. Two databases writing one archive interleave their WAL and leave it unrestorable. Give this Cluster an archive of its own, or a serverName that is not %q.",
			holder, at.Bucket, at.Prefix, serverName,
		))
	}

	if cluster.GetAnnotations()[OptOutAnnotation] == OptOutValue {
		return d.optedOut(ctx, logger, at)
	}

	run, err := waitingRun(ctx, d.Client, req.Namespace, req.Name)
	if err != nil {
		logger.Error(err, "cannot read the namespace's RestoreRuns")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// A Cluster that already asks for a recovery was written that way on
	// purpose, by the Flux component or a unit's restore input. It keeps its
	// own target, unless a RestoreRun is waiting to recover it: then two
	// targets name one database, and neither may win by accident.
	if declared {
		if run != nil {
			return admission.Denied(fmt.Sprintf(
				"RestoreRun %s is waiting to recover %s/%s, and the Cluster declares its own spec.bootstrap.recovery. Remove the declared recovery (the terragrunt restore input or the postgres-recovery component), or delete the RestoreRun.",
				run.Name, req.Namespace, req.Name,
			))
		}
		logger.Info("leaving the Cluster alone", "reason", "it already declares a recovery")
		return admission.Allowed("already recovering")
	}

	backups, err := d.Prober.BaseBackups(ctx, at)
	if err != nil {
		logger.Error(err, "cannot list the base backups")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if len(backups) == 0 {
		return d.withoutBaseBackup(ctx, logger, at, run)
	}
	target, err := recoveryFor(req.Name, run, backups)
	if err != nil {
		return admission.Denied(err.Error())
	}
	if run == nil {
		target, err = d.automaticRecovery(ctx, at, req.Namespace, req.Name, backups)
		if err != nil {
			logger.Error(err, "cannot work out the namespace's paused moment")
			return admission.Errored(http.StatusInternalServerError, err)
		}
	}

	if err := setRecovery(cluster, store, serverName, target); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if run != nil {
		annotations := cluster.GetAnnotations()
		annotations[backupv1alpha1.AnnotationRestoreRun] = run.Name
		cluster.SetAnnotations(annotations)
	}

	patched, err := json.Marshal(cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	logger.Info("recovering the Cluster from its object store", "prefix", at.BasePrefix(), "baseBackup", target.backupID)
	return admission.PatchResponseFromRaw(req.Object.Raw, patched)
}

// waitingRun finds the RestoreRun that deleted a Cluster and is waiting for it
// to be created again, so the webhook can recover the new Cluster for that
// run.
//
// It lists the RestoreRuns in the given namespace and returns the first one
// that hasn't finished, isn't being deleted, and has an entry in status.items
// for a Cluster with the given name in phase Deleted. It returns nil when no
// run matches, and an error when the list fails.
func waitingRun(ctx context.Context, c client.Reader, namespace, name string) (*backupv1alpha1.RestoreRun, error) {
	runs := &backupv1alpha1.RestoreRunList{}
	if err := c.List(ctx, runs, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list RestoreRuns in %s: %w", namespace, err)
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if run.Status.Phase.Finished() || !run.DeletionTimestamp.IsZero() {
			continue
		}
		for _, item := range run.Status.Items {
			if item.Kind == "Cluster" && item.Name == name && item.Phase == backupv1alpha1.ItemDeleted {
				return run, nil
			}
		}
	}
	return nil, nil
}

// withoutBaseBackup decides the create of a Cluster whose store holds no
// completed base backup.
//
// Parameters:
//   - at is the database's Location.
//   - run is the RestoreRun waiting for the Cluster, or nil.
//
// A waiting RestoreRun asks for a recovery there is nothing to recover from,
// so the create is refused. Without a run, the Cluster may start as a new
// empty database only when nothing at all is stored under its archive
// prefix. An archive with WAL and no completed base backup refuses the
// create: there is nothing to recover from, and the barman-cloud plugin
// refuses to archive a new database into a prefix that holds WAL, so the new
// database could never be backed up. A failed listing gives an HTTP 500, so
// the create is tried again.
func (d *Decider) withoutBaseBackup(ctx context.Context, logger logr.Logger, at Location, run *backupv1alpha1.RestoreRun) admission.Response {
	if run != nil {
		return admission.Denied(fmt.Sprintf(
			"RestoreRun %s asks for a recovery, and %s/%s holds no completed base backup to recover from.",
			run.Name, at.Bucket, at.BasePrefix(),
		))
	}
	empty, err := d.Prober.ArchiveEmpty(ctx, at)
	if err != nil {
		logger.Error(err, "cannot list the archive")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if !empty {
		logger.Info("refusing the Cluster", "reason", "WAL and no completed base backup", "prefix", at.ArchivePrefix())
		return admission.Denied(fmt.Sprintf(
			"%s/%s holds WAL of an earlier database and no completed base backup. There is nothing to recover from, and a new empty database could never archive into that prefix. Delete everything under %s/%s and create the Cluster again, or give this Cluster another serverName.",
			at.Bucket, at.ArchivePrefix(), at.Bucket, at.ArchivePrefix(),
		))
	}
	logger.Info("leaving the Cluster to initdb", "reason", "nothing is archived yet", "prefix", at.ArchivePrefix())
	return admission.Allowed("nothing archived yet")
}

// automaticRecovery works out where a Cluster created without a RestoreRun
// recovers to, for example when Flux applies a rebuilt namespace or an admin
// deleted the Cluster.
//
// Parameters:
//   - at is the Cluster's archive Location.
//   - namespace and name identify the new Cluster.
//   - backups are the completed base backups in the Cluster's store.
//
// When every claim marked backup.wlz.li/enabled comes back from the
// namespace's paused moment (see synced.Moment and claimsFromMoment), the
// Cluster recovers to:
//   - the end of the base backup that the moment's snapshot names for this
//     Cluster, which the BackupRun took while the app was paused, when the
//     store still holds it;
//   - else, when the snapshot carries restic.HibernatedTag for this Cluster
//     and no WAL reached the archive after the moment, the end of the
//     archive: the hibernated database has not run since;
//   - else the end of the newest base backup that finished at or before the
//     moment, for example when retention pruned the tagged one or the
//     Cluster was not part of that BackupRun;
//   - else, when no base backup is that old, the end of the archive. The
//     Cluster did not exist at the moment, or retention pruned every base
//     backup that old; in the second case the database comes back ahead of
//     the volumes, since nothing older is left to recover from.
//
// A claim the populator already filled from the moment counts only while no
// WAL reached the archive after its restore started: a database that wrote
// since then ran next to the claim, and the claim holds live data.
//
// In every other case the claims hold live data or come back with their
// newest snapshots, and the Cluster recovers to the end of its archive, its
// newest state. It returns an error when a read fails, and the create is
// tried again.
func (d *Decider) automaticRecovery(ctx context.Context, at Location, namespace, name string, backups []BaseBackup) (recoveryTarget, error) {
	if d.Snapshots == nil {
		return recoveryTarget{}, nil
	}
	repositories, err := synced.Repositories(ctx, d.Client, d.Snapshots, namespace)
	if err != nil {
		return recoveryTarget{}, err
	}
	moment, ok := synced.Moment(repositories)
	if !ok {
		return recoveryTarget{}, nil
	}
	fromMoment, filled, err := claimsFromMoment(ctx, d.Client, namespace, moment)
	if err != nil || !fromMoment {
		return recoveryTarget{}, err
	}
	if filled != nil {
		written, err := d.Prober.WALSince(ctx, at, *filled)
		if err != nil || written {
			return recoveryTarget{}, err
		}
	}
	for _, snapshots := range repositories {
		if snapshot, found := synced.At(snapshots, moment); found {
			if id, tagged := restic.PausedBaseBackup(snapshot, name); tagged && slices.ContainsFunc(backups, func(b BaseBackup) bool { return b.ID == id }) {
				return recoveryTarget{backupID: id}, nil
			}
			if slices.Contains(snapshot.Tags, restic.HibernatedTag(name)) {
				// The database was hibernated at the moment. When no WAL
				// came after the moment, it has not run since, and the end
				// of its archive is its state at the moment.
				woken, err := d.Prober.WALSince(ctx, at, moment)
				if err != nil || !woken {
					return recoveryTarget{}, err
				}
			}
			break
		}
	}
	if backup, found := AtOrBefore(backups, moment); found {
		return recoveryTarget{backupID: backup.ID}, nil
	}
	return recoveryTarget{}, nil
}

// claimsFromMoment reports whether every claim of the namespace marked
// backup.wlz.li/enabled comes back from the paused moment.
//
// Parameters:
//   - namespace is the namespace of the claims.
//   - moment is the namespace's paused moment.
//
// Every such claim has to name a VolumeRestore in its dataSourceRef and have
// no pin (backup.wlz.li/restore-as-of on the claim or spec.restoreAsOf on the
// VolumeRestore): a pinned claim is filled from the pin's time. A claim that
// is not bound yet comes back from the moment, since the populator fills it
// from the moment's snapshot. A bound claim comes back from the moment only
// when its VolumeRestore records that the populator filled it with the
// moment's snapshot; any other bound claim holds data the populator did not
// write.
//
// It returns true and the earliest time at which the restore of a bound
// claim started, or nil when no claim is bound yet, so the caller can check
// that no database wrote since. It returns an error when a read fails.
func claimsFromMoment(ctx context.Context, c client.Reader, namespace string, moment time.Time) (bool, *time.Time, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, claims, client.InNamespace(namespace)); err != nil {
		return false, nil, fmt.Errorf("list the claims in %s: %w", namespace, err)
	}
	var filled *time.Time
	for _, claim := range claims.Items {
		if !backupv1alpha1.Enabled(claim.Annotations) {
			continue
		}
		ref := claim.Spec.DataSourceRef
		if ref == nil || ref.Kind != "VolumeRestore" || ref.APIGroup == nil || *ref.APIGroup != backupv1alpha1.GroupVersion.Group {
			return false, nil, nil
		}
		if _, pinned := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; pinned {
			return false, nil, nil
		}
		vr := &backupv1alpha1.VolumeRestore{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, vr); err != nil {
			return false, nil, client.IgnoreNotFound(err)
		}
		if vr.Spec.RestoreAsOf != nil {
			return false, nil, nil
		}
		if claim.Status.Phase != corev1.ClaimBound {
			continue
		}
		i := slices.IndexFunc(vr.Status.Claims, func(e backupv1alpha1.ClaimRestoreStatus) bool {
			return e.UID == claim.UID && e.SnapshotTime != nil && e.SnapshotTime.UTC().Truncate(time.Second).Equal(moment) && e.StartedAt != nil
		})
		if i < 0 {
			return false, nil, nil
		}
		if started := vr.Status.Claims[i].StartedAt.Time; filled == nil || started.Before(*filled) {
			filled = &started
		}
	}
	return true, filled, nil
}

// optedOut decides the create of a Cluster that carries
// backup.wlz.li/bootstrap: initdb, which asks for an empty database.
//
// Parameters:
//   - at is the database's Location.
//
// The Cluster starts empty only when nothing is archived under its archive
// prefix. Over an archive that holds anything, the barman-cloud plugin
// refuses to archive the new database, so it could never be backed up, and
// the create is refused with the steps to take. A failed listing gives an
// HTTP 500, so the create is tried again.
func (d *Decider) optedOut(ctx context.Context, logger logr.Logger, at Location) admission.Response {
	empty, err := d.Prober.ArchiveEmpty(ctx, at)
	if err != nil {
		logger.Error(err, "cannot list the archive")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if !empty {
		logger.Info("refusing the Cluster", "reason", "opted out over an archive", "prefix", at.ArchivePrefix())
		return admission.Denied(fmt.Sprintf(
			"The Cluster asks for an empty database (%s: %s), and %s/%s holds the archive of an earlier one. The new database could not archive there. Delete everything under %s/%s first, or give this Cluster another serverName.",
			OptOutAnnotation, OptOutValue, at.Bucket, at.ArchivePrefix(), at.Bucket, at.ArchivePrefix(),
		))
	}
	logger.Info("leaving the Cluster to initdb", "reason", "opted out by annotation, nothing archived yet")
	return admission.Allowed("opted out")
}

// recoveryTarget says where the recovery of a new Cluster stops.
type recoveryTarget struct {
	// backupID is the base backup the recovery starts from and stops at the
	// end of, or "" for a recovery that replays the whole WAL archive.
	backupID string
}

// recoveryFor works out where the recovery of a new Cluster stops.
//
// Parameters:
//   - name is the Cluster's name.
//   - run is the RestoreRun waiting for the Cluster, or nil.
//   - backups are the completed base backups in the Cluster's store.
//
// When the run's item for the Cluster has stopAtBaseBackup set, the recovery
// stops at the end of the item's base backup. It returns an error, which
// refuses the create, when that base backup is no longer in the store. In
// every other case the recovery replays the whole archive, which Postgres
// always completes.
func recoveryFor(name string, run *backupv1alpha1.RestoreRun, backups []BaseBackup) (recoveryTarget, error) {
	if run == nil {
		return recoveryTarget{}, nil
	}
	for _, item := range run.Status.Items {
		if item.Kind != "Cluster" || item.Name != name || !item.StopAtBaseBackup {
			continue
		}
		for _, b := range backups {
			if b.ID == item.BaseBackup {
				return recoveryTarget{backupID: b.ID}, nil
			}
		}
		return recoveryTarget{}, fmt.Errorf("RestoreRun %s recovers to the end of base backup %s, which the store no longer holds as a completed base backup. Delete the RestoreRun and create a new one", run.Name, item.BaseBackup)
	}
	return recoveryTarget{}, nil
}

// keepRecovery answers an update of a Cluster. When the old Cluster was
// recovered by this webhook (its spec.bootstrap.recovery.source is
// RecoverySource) and the new one carries spec.bootstrap.initdb, it returns a
// patch that removes initdb. Every other update is allowed unchanged.
//
// Flux applies the Cluster from git on every reconcile, and git holds initdb.
// Server-side apply keeps the recovery written at creation and adds initdb
// back, and CloudNativePG refuses a Cluster with two bootstrap methods. That
// refusal fails the dry run Flux runs first, so keepRecovery runs on dry runs
// too. It reads nothing from the cluster, and CloudNativePG ignores
// spec.bootstrap once a Cluster exists, so dropping initdb changes nothing
// about the database.
func keepRecovery(req admission.Request) admission.Response {
	old := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.OldObject.Raw, old); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	source, _, _ := unstructured.NestedString(old.Object, "spec", "bootstrap", "recovery", "source")
	if source != RecoverySource {
		return admission.Allowed("not recovered by this webhook")
	}

	cluster := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.Object.Raw, cluster); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if _, found, _ := unstructured.NestedMap(cluster.Object, "spec", "bootstrap", "initdb"); !found {
		return admission.Allowed("no initdb to drop")
	}
	unstructured.RemoveNestedField(cluster.Object, "spec", "bootstrap", "initdb")

	patched, err := json.Marshal(cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, patched)
}

// archiveHolder finds an existing Cluster that already archives to the same
// bucket and archive prefix as the Cluster being admitted. Two databases
// archiving to one prefix interleave their WAL and leave the archive
// unrestorable.
//
// Parameters:
//   - namespace and name identify the Cluster being admitted. A Cluster with
//     the same namespace and name is skipped, so the same database created
//     again, as a restore does, doesn't collide with its own archive.
//   - at is where the admitted Cluster would archive, from ResolveLocation.
//
// It returns the holder as "namespace/name", or an empty string when no
// Cluster archives there.
//
// It lists every Cluster in every namespace and reads each one's bucket and
// prefix from its ObjectStore (see archivePath). The endpoint URL is not
// compared: two URLs can name one S3 service, and a collision that slips
// through destroys both archives. The prefix has no trailing slash on either
// side, and the comparison is exact, so a database named db and one named
// db-old are two archives. A Cluster whose ObjectStore does not exist
// archives nowhere and is skipped. Any other failure to read a Cluster's
// ObjectStore returns an error, which refuses the create with an HTTP 500
// until the read works, since a skipped Cluster might hold the prefix.
func archiveHolder(
	ctx context.Context,
	c client.Reader,
	namespace, name string,
	at Location,
) (string, error) {
	clusters := &unstructured.UnstructuredList{}
	clusters.SetGroupVersionKind(ClusterListGVK)
	if err := c.List(ctx, clusters); err != nil {
		return "", fmt.Errorf("list the Clusters: %w", err)
	}

	for i := range clusters.Items {
		other := &clusters.Items[i]
		if other.GetNamespace() == namespace && other.GetName() == name {
			continue
		}
		store, serverName, found := Archiver(other)
		if !found {
			continue
		}
		_, bucket, prefix, err := archivePath(ctx, c, other.GetNamespace(), store, serverName)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("read the archive of Cluster %s/%s: %w", other.GetNamespace(), other.GetName(), err)
		}
		if bucket == at.Bucket && prefix == at.Prefix {
			return fmt.Sprintf("%s/%s", other.GetNamespace(), other.GetName()), nil
		}
	}
	return "", nil
}

// Archiver finds where a Cluster archives its WAL. It looks in spec.plugins for
// the first entry named PluginName with isWALArchiver set to true and a
// non-empty barmanObjectName parameter.
//
// It returns that entry's barmanObjectName as the store and its serverName
// parameter as the server name, with found set to true. When serverName is
// unset, the server name is the Cluster's own name. It returns found as false
// when no entry matches. Such a Cluster backs nothing up and has nothing to
// recover from.
func Archiver(cluster *unstructured.Unstructured) (store, serverName string, found bool) {
	plugins, ok, err := unstructured.NestedSlice(cluster.Object, "spec", "plugins")
	if err != nil || !ok {
		return "", "", false
	}

	for _, entry := range plugins {
		plugin, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(plugin, "name"); name != PluginName {
			continue
		}
		if isArchiver, _, _ := unstructured.NestedBool(plugin, "isWALArchiver"); !isArchiver {
			continue
		}
		store, _, _ = unstructured.NestedString(plugin, "parameters", "barmanObjectName")
		if store == "" {
			continue
		}
		serverName, _, _ = unstructured.NestedString(plugin, "parameters", "serverName")
		if serverName == "" {
			// Barman defaults the server name to the Cluster's name, so an
			// unset parameter means the archive sits under that.
			serverName = cluster.GetName()
		}
		return store, serverName, true
	}
	return "", "", false
}

// setRecovery rewrites a Cluster, in place, to recover from its own archive.
//
// Parameters:
//   - cluster is the Cluster being created. It's modified directly.
//   - store and serverName say where the Cluster archives, as Archiver
//     returns them. The recovery reads from the same place.
//   - target says where the recovery stops. A backup ID gives
//     recoveryTarget backupID with targetImmediate: the recovery starts from
//     that base backup and stops at its end. No backup ID gives no
//     recoveryTarget, and the recovery replays the whole archive. Postgres
//     reaches both targets, whether or not anything was written after them.
//
// It replaces spec.bootstrap.initdb with spec.bootstrap.recovery, carrying
// over initdb's database, owner and secret. It adds an externalClusters entry
// named RecoverySource that reads through the Barman Cloud plugin, or replaces
// an entry of that name if one is there. It sets SkipCheckAnnotation to
// "enabled" so the recovered database can archive into the prefix it restored
// from. It returns an error only when the unstructured object can't be read or
// written at those paths.
func setRecovery(cluster *unstructured.Unstructured, store, serverName string, target recoveryTarget) error {
	recovery := map[string]any{"source": RecoverySource}
	if target.backupID != "" {
		// CloudNativePG 1.30 accepts targetImmediate only together with
		// backupID (internal/webhook/v1/cluster_webhook.go:1485-1494), and the
		// barman-cloud plugin starts from the backup that backupID names
		// (barman-cloud v0.6.0 pkg/catalog/catalog.go:143-147).
		recovery["recoveryTarget"] = map[string]any{"backupID": target.backupID, "targetImmediate": true}
	}

	// The application database, its owning role and the Secret holding that
	// role's password carry over from initdb.
	//
	// Dropping them breaks the app. CloudNativePG defaults a recovery's
	// database and owner to "app", so a Cluster that was created with
	// database "canary" comes back with its data in "canary" and an empty
	// "app" beside it, and the <cluster>-app Secret the workload reads points
	// at the empty one. The restore then looks like data loss, though the
	// data is all there.
	for _, field := range []string{"database", "owner"} {
		value, found, err := unstructured.NestedString(cluster.Object, "spec", "bootstrap", "initdb", field)
		if err == nil && found && value != "" {
			recovery[field] = value
		}
	}
	if secret, found, err := unstructured.NestedMap(cluster.Object, "spec", "bootstrap", "initdb", "secret"); err == nil && found {
		recovery["secret"] = secret
	}

	unstructured.RemoveNestedField(cluster.Object, "spec", "bootstrap", "initdb")

	if err := unstructured.SetNestedMap(cluster.Object, recovery, "spec", "bootstrap", "recovery"); err != nil {
		return fmt.Errorf("set the recovery bootstrap: %w", err)
	}

	entry := map[string]any{
		"name": RecoverySource,
		"plugin": map[string]any{
			"name": PluginName,
			"parameters": map[string]any{
				"barmanObjectName": store,
				"serverName":       serverName,
			},
		},
	}

	external, _, err := unstructured.NestedSlice(cluster.Object, "spec", "externalClusters")
	if err != nil {
		return fmt.Errorf("read the external clusters: %w", err)
	}
	replaced := false
	for i, existing := range external {
		item, ok := existing.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(item, "name"); name == RecoverySource {
			external[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		external = append(external, entry)
	}
	if err := unstructured.SetNestedSlice(cluster.Object, external, "spec", "externalClusters"); err != nil {
		return fmt.Errorf("set the external clusters: %w", err)
	}

	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[SkipCheckAnnotation] = "enabled"
	cluster.SetAnnotations(annotations)
	return nil
}
