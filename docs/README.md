# Documents

| Document | For |
| --- | --- |
| [overview.md](overview.md) | the problem, the mechanism, and what an app keeps |
| [architecture.md](architecture.md) | the object flow, the library, the node the volume lands on, and every failure case |
| [api.md](api.md) | the VolumeRestore resource, field by field, with a worked example |
| [packaging.md](packaging.md) | the release assets, the repository layout, the RBAC, and the workflow to copy |
| [integration.md](integration.md) | the terragrunt module and unit, the Flux component change, and how to prove it on the test cluster |
| [decisions.md](decisions.md) | what was chosen, what was rejected, and what would have gone wrong |
| [implementation-plan.md](implementation-plan.md) | the work in order, with what proves each step |

## Reading order

An implementer reads overview, architecture, then implementation-plan, and
consults api and packaging while working. Someone deciding whether the project
should exist reads overview and decisions.

## What is settled and what is not

| Settled | Open |
| --- | --- |
| the populator mechanism and the three callbacks | the API group and kind name, which need Sam's word before a CRD ships |
| VolSync does the data movement | whether the repository Secret is copied or lives in the controller's namespace, if the ClusterRole's reach becomes an objection |
| a release carries the rendered manifests, the CRDs and a chart | whether the Flux component keeps both paths after the new one is proven |
