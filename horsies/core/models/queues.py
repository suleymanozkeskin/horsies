# app/core/models/queues.py
from enum import Enum
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
    """

    name: str
    priority: int = Field(default=1, ge=1, le=100)
    max_concurrency: int = 5
