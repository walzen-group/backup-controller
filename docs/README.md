# Documents

| Document | For |
| --- | --- |
| [overview.md](overview.md) | the fill problem, the populator mechanism, and what an app keeps |
| [architecture.md](architecture.md) | the three parts of the binary, every object the controller writes, the node a volume lands on, and the failure cases |
| [api.md](api.md) | VolumeRestore, BackupRun and RestoreRun, field by field |
| [namespace-backups.md](namespace-backups.md) | the annotations, the scheduler, the runs, quiesce and the metrics, measured on the prod canary |
| [restores.md](restores.md) | what fills a claim, what overwrites one, how a database restores, and which to reach for |
| [packaging.md](packaging.md) | the release assets, the repository layout, the RBAC, and the workflow |
| [integration.md](integration.md) | how the infrastructure repository installs the release and declares backups |
| [decisions.md](decisions.md) | what was chosen, what was rejected, and what would have gone wrong |
| [implementation-plan.md](implementation-plan.md) | the original v0.1 build plan, kept as a record |

## Reading order

Someone running backups reads namespace-backups, then restores. An
implementer reads overview and architecture, and consults api and packaging
while working. Someone deciding whether the project should exist reads overview
and decisions.

## What is settled and what is not

| Settled | Open |
| --- | --- |
| the populator mechanism and the three callbacks | whether the repository Secret is copied or lives in the controller's namespace, if the ClusterRole's reach becomes an objection |
| the API group `backup.wlz.li`, with VolumeRestore, BackupRun and RestoreRun at v1alpha1 | how long a large volume's restore runs against the library's requeue behaviour, which only a large restore answers |
| VolSync and the barman-cloud plugin move the data; the controller schedules, triggers and checks | |
| every backed-up claim the infrastructure repository writes names a VolumeRestore; neither delivery path renders the ReplicationDestination populator any more | |
