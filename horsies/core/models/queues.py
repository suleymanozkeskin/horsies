# app/core/models/queues.py
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field


class QueueMode(Enum):
    CUSTOM = 'custom'
    DEFAULT = 'default'


class CustomQueueConfig(BaseModel):
    """
    name: name of the queue. Usage: `@task(queue="name")`
    priority: 1 means first to be executed, 100 means last to be executed.
    max_concurrency: max number of tasks that can be executed at the same time for this queue.
        App level concurrency is still respected.
        If you have set app level concurrency to 5 but queue level to 10,
        it will still be limited to 5.
        ``None`` means uncapped: no per-queue limit is enforced or counted
        (worker- and cluster-level limits still apply), mirroring
        ``cluster_wide_cap=None``. ``0`` is valid and pauses claiming from
        the queue. Negative values are rejected.
    """

    name: str
    priority: int = Field(default=1, ge=1, le=100)
    max_concurrency: Annotated[int, Field(ge=0)] | None = 5
