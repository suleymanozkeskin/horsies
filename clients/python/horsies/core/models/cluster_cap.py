"""The cluster-wide cap of RUNNING tasks (`AppConfig.cluster_wide_cap`).

The same union is the value of `WorkerStateSnapshot.cluster_wide_cap`: the
cap that the worker ran with.
"""

from typing import Annotated, ClassVar, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class ClusterCap(BaseModel):
    """At most ``tasks`` tasks are RUNNING at once across every queue of the
    cluster. ``AppConfig`` validation refuses ``tasks`` below 1 and above
    the bound that a worker takes (`CONFIG_INVALID_CLUSTER_CAP`)."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['cluster_cap'] = 'cluster_cap'
    tasks: int = Field(..., description='The most RUNNING tasks of the cluster')


class NoClusterCap(BaseModel):
    """No cluster-wide cap: only the queue and worker limits apply. The
    default."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['no_cluster_cap'] = 'no_cluster_cap'


ClusterCapSetting = Annotated[
    Union[ClusterCap, NoClusterCap],
    Field(discriminator='kind'),
]
"""The cluster-wide cap of RUNNING tasks, or none."""
