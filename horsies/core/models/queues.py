# app/core/models/queues.py
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from horsies.core.errors import ConfigurationError, ErrorCode

# Length of the `queue_name` database columns (tasks and workflow tasks).
QUEUE_NAME_MAX_LENGTH = 100


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

    The name must not be empty, must not start or end with whitespace, and
    must not be longer than ``QUEUE_NAME_MAX_LENGTH`` characters.
    """

    model_config = ConfigDict(extra='forbid')

    name: str
    priority: int = Field(default=1, ge=1, le=100)
    max_concurrency: Annotated[int, Field(ge=0)] | None = 5

    @model_validator(mode='after')
    def validate_name(self) -> Self:
        """Refuse a queue name that no task can use or the database cannot store."""
        problem = _queue_name_problem(self.name)
        if problem is not None:
            raise ConfigurationError(
                message=f'invalid queue name {self.name!r}: {problem}',
                code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
                notes=[f'CustomQueueConfig(name={self.name!r})'],
                help_text=(
                    'use a non-empty name without leading or trailing '
                    f'whitespace, at most {QUEUE_NAME_MAX_LENGTH} characters'
                ),
            )
        return self


def _queue_name_problem(name: str) -> str | None:
    """Return the reason a queue name is refused, or None when it is accepted."""
    if name == '':
        return 'the name is empty'
    if name != name.strip():
        return 'the name has whitespace at the start or the end'
    if len(name) > QUEUE_NAME_MAX_LENGTH:
        return (
            f'the name has {len(name)} characters; '
            f'the maximum is {QUEUE_NAME_MAX_LENGTH}'
        )
    return None
