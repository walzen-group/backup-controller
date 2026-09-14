# Decisions

Each entry records what was chosen, what else was on the table, and what would
have gone wrong. A reader who arrives at one of the rejected options should be
able to tell from the entry alone why it was rejected.

## Build a populator rather than patch VolSync

VolSync's populator requires `copyMethod: Snapshot` because it fills the prime
claim from `status.latestImage`. Teaching it to restore directly into an empty
prime claim is a modest change to their controller, and it would leave this
repository unnecessary.

It was rejected on update cadence. A fork cannot take a VolSync release until
someone has rebased onto it, and VolSync is the component performing the
cluster's backups. Being slow to patch the backup tool is a worse position than
owning a small controller. The walzen infrastructure already carries one fork,
of the Helm provider, and that one is tolerable because it waits on a pull
request that will merge. Here upstream may well decline: their populator is
snapshot-based by design, so that one restore can seed any number of claims from
a single image, and a Direct populator restores once per claim.

A controller can also be adopted one volume at a time, because a claim chooses
its path by what `dataSourceRef` names. A fork changes behaviour for every app
at once.

## Use the library's provider callbacks rather than a populator pod

`lib-volume-populator` accepts either a `PodConfig`, where the library runs a
pod of yours against the prime claim, or a `ProviderFunctionConfig` of three
callbacks.

A pod would have to do the restore, which means restic in an image of ours, the
repository credentials mounted into it, a second implementation of something
VolSync already does, and a restore that runs outside the cluster's backup
queue. Every one of those is a standing cost.

The callbacks let the fill step be "create a ReplicationDestination and wait",
so VolSync's own mover does every byte with the credentials and the queue it
already uses. The controller holds no repository credentials, mounts no volume
and contains no restic code.

## Define a kind of our own rather than reuse ReplicationDestination

A claim could keep naming `kind: ReplicationDestination` in `dataSourceRef`, and
nothing in the app would change.

VolSync registers its own populator for that kind. Two populators watching the
same kind would both act on the same claims, and the result depends on which one
gets there first. That is a conflict rather than a preference, so the data source
is a kind of ours.

The kind carries a second advantage: a claim that says `kind: VolumeRestore`
says what fills it, and an app on the old path and an app on the new one are
distinguishable by reading either claim.

## Copy the repository Secret for the length of a restore

VolSync resolves `spec.restic.repository` in the ReplicationDestination's own
namespace. The prime claim is created in the controller's namespace, so the
destination is there too, and the app's repository Secret is in the app's
namespace.

The controller copies that Secret into its namespace, named for the claim's UID,
and deletes it in `PopulateCleanupFn`. That gives the controller a ClusterRole
with `get` on Secrets, which is the one line in the install worth arguing about.

The alternative is a repository Secret that lives in the controller's namespace
from the start, written by the same environment unit that publishes the backup
credentials today. It needs no Secret read across namespaces. It was not taken
because the repository URL differs per volume, so it would mean one Secret per
volume in a namespace no app owns, and a naming convention tying them together;
the app's own Secret already exists and already says which repository the volume
uses.

Revisit this if the ClusterRole's reach becomes the objection. Narrowing `get`
on Secrets to the namespaces that carry a VolumeRestore needs a Role per
namespace and something to create it, which is more moving parts than the copy.

## Keep the prime claim's storage class and node

The library copies the app claim's access modes, resources, storage class and,
for a WaitForFirstConsumer class, its `volume.kubernetes.io/selected-node`
annotation. A mutator hook could change any of them before the prime claim is
created.

None is changed. The volume the app ends up with has to be the volume the app
asked for, and the node has to be the one the scheduler picked for the pod, or
the pod and its volume end up on different workers. That last case is not
hypothetical: the snapshot path produced exactly that on the walzen test cluster
on 2026-09-13, a claim annotated for one worker whose volume was created on
another.

## Report status on the VolumeRestore rather than only in logs

A restore that is running, and a restore that has failed, both have to be
visible without reading controller logs. The object carries kstatus-compatible
conditions and a list of the claims being filled, so `kubectl get` answers the
question and a Flux Kustomization with `wait: true` can gate on it.
