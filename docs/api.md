# API

One custom resource. A claim names it in `dataSourceRef`, and it says which
restic repository to restore from.

## VolumeRestore

| | |
| --- | --- |
| Group and version | `backup.wlz.li/v1alpha1` |
| Kind | `VolumeRestore` |
| Scope | namespaced, in the app's namespace beside the claim |
| Short name | `vrestore` |

The group matches the convention kuport set with `kuport.wlz.li`. Settle the
group and the kind with Sam before the CRD is generated, because changing either
after a release means every claim in the infrastructure repository changes with
it.

### Spec

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: notes-data
  namespace: notes
spec:
  # The Secret in this namespace holding the restic repository URL, its
  # password and the object store keys. The same Secret the app's
  # ReplicationSource names, written by the backups/pvc component.
  repository: notes-restic-data

  # Optional. Restore the newest snapshot taken at or before this time, in
  # RFC3339. Left out, the newest snapshot in the repository is used.
  restoreAsOf: "2026-09-13T00:00:00Z"

  # Optional. Labels put on the mover pod, so the restore is admitted by the
  # cluster's backup queue the way every other mover is.
  moverPodLabels:
    kueue.x-k8s.io/queue-name: backup

  # Optional. Passed through to the ReplicationDestination unchanged.
  moverSecurityContext:
    runAsUser: 26
    runAsGroup: 26
    fsGroup: 26
```

| Field | Required | Reaches |
| --- | --- | --- |
| `repository` | yes | ReplicationDestination `spec.restic.repository`, after the Secret is copied |
| `restoreAsOf` | no | ReplicationDestination `spec.restic.restoreAsOf` |
| `moverPodLabels` | no | ReplicationDestination `spec.restic.moverPodLabels` |
| `moverSecurityContext` | no | ReplicationDestination `spec.restic.moverSecurityContext` |

Every field that exists is a passthrough. Add a field only when a
ReplicationDestination field has to be reachable from a claim, and name it
after the field it reaches.

### Status

```yaml
status:
  conditions:
    - type: Ready
      status: "False"
      reason: Restoring
      message: waiting for ReplicationDestination restore-3f2a1c7e in backup-system
  claims:
    - name: notes-data
      uid: 3f2a1c7e-...
      phase: Restoring
      startedAt: "2026-09-14T09:12:03Z"
```

| Field | Holds |
| --- | --- |
| `conditions[type=Ready]` | False while any claim naming this object is being filled, True when none is |
| `claims[]` | one entry per claim currently being populated from this object, with the phase and when it started |

A VolumeRestore is a standing declaration rather than a one-shot job, so a
restore that finished leaves no entry. What a reader wants from the status is
whether something is happening now and where to look.

Report kstatus-compatible conditions, so a Flux Kustomization with `wait: true`
can gate on the object and a claim's restore shows up in `flux get`.

## Worked example

An app named notes, with one backed-up volume, on the infrastructure
repository's Flux layout. Three files in the app's base change, and this is all
of it:

```yaml
# base/volume-data/pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${APP}
  annotations:
    backup.walzen.org/schedule: ${BACKUP_SCHEDULE}
    backup.walzen.org/retain-last: "10"
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: zfs
  resources:
    requests:
      storage: 1Gi
  dataSourceRef:
    apiGroup: backup.wlz.li
    kind: VolumeRestore
    name: ${APP}
---
# base/volume-data/restore.yaml
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: ${APP}
spec:
  repository: ${APP}-restic
  moverPodLabels:
    kueue.x-k8s.io/queue-name: ${BACKUP_QUEUE}
```

Two annotations the claim carries today are gone with the destination they
configured: `backup.walzen.org/restore-trigger`, which existed to re-run a
ReplicationDestination, and `backup.walzen.org/volume-at`, which existed to
place a restore mover. Deleting the claim is the trigger, and the scheduler
places the volume. [integration.md](integration.md) has the full change to the
component.

## Validation the CRD carries

| Rule | Why |
| --- | --- |
| `repository` is a required, non-empty DNS-1123 name | a missing repository leaves claims Pending with nothing to read |
| `restoreAsOf` matches RFC3339 when present | VolSync rejects it later and less clearly |
| `moverPodLabels` keys and values are valid label syntax | the same |

Reject what the API server can reject. A claim that cannot be filled should fail
at apply time rather than sit Pending while someone reads controller logs.
