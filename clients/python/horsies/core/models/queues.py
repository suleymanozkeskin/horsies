# app/core/models/queues.py
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, assert_never

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from horsies.core.errors import ConfigurationError, HorsiesErrorCode
from horsies.core.types.result import Err, Ok, Result

QUEUE_NAME_MAX_BYTES = 52
"""The longest queue name, in bytes of its UTF-8 text.

The insert trigger of a task sends ``pg_notify('task_queue_' || queue_name)``.
PostgreSQL refuses a channel name over 63 bytes, and the prefix takes 11, so
a longer name fails every send to that queue. The schema checks the same
bound (``octet_length(queue_name) <= 52``).
"""


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

    The name must not be empty, must not start or end with whitespace, must
    be valid UTF-8 text, and must not be longer than ``QUEUE_NAME_MAX_BYTES``
    bytes of that text.
    """

    model_config = ConfigDict(extra='forbid')

    name: str
    priority: int = Field(default=1, ge=1, le=100)
    max_concurrency: Annotated[int, Field(ge=0)] | None = 5

    @model_validator(mode='after')
    def validate_name(self) -> Self:
        """Refuse a queue name that no task can use or the database cannot store."""
        match check_queue_name(self.name):
            case Ok():
                return self
            case Err(refusal):
                raise ConfigurationError(
                    message=(
                        f'invalid queue name {self.name!r}: '
                        f'{_describe_refusal(refusal)}'
                    ),
                    code=HorsiesErrorCode.CONFIG_INVALID_QUEUE_MODE,
                    notes=[f'CustomQueueConfig(name={self.name!r})'],
                    help_text=(
                        'use a non-empty name without leading or trailing '
                        f'whitespace, at most {QUEUE_NAME_MAX_BYTES} bytes '
                        'of UTF-8 text'
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class QueueNameEmpty:
    """The queue name is the empty string."""


@dataclass(frozen=True, slots=True)
class QueueNameEdgeWhitespace:
    """The queue name starts or ends with whitespace."""


@dataclass(frozen=True, slots=True)
class QueueNameNotUtf8:
    """The queue name holds a lone surrogate, so it has no UTF-8 text."""


@dataclass(frozen=True, slots=True)
class QueueNameTooLong:
    """The UTF-8 text of the queue name is longer than ``QUEUE_NAME_MAX_BYTES``."""

    byte_length: int


type QueueNameRefusal = (
    QueueNameEmpty | QueueNameEdgeWhitespace | QueueNameNotUtf8 | QueueNameTooLong
)


def check_queue_name(name: str) -> Result[str, QueueNameRefusal]:
    """Accept a usable queue name, or name the one rule that it breaks.

    It checks the rules in this order: empty, edge whitespace, UTF-8 text,
    byte length. It does not read the app config.
    """
    if name == '':
        return Err(QueueNameEmpty())
    if name != name.strip():
        return Err(QueueNameEdgeWhitespace())
    try:
        byte_length = len(name.encode('utf-8'))
    except UnicodeEncodeError:
        return Err(QueueNameNotUtf8())
    if byte_length > QUEUE_NAME_MAX_BYTES:
        return Err(QueueNameTooLong(byte_length=byte_length))
    return Ok(name)


def _describe_refusal(refusal: QueueNameRefusal) -> str:
    match refusal:
        case QueueNameEmpty():
            return 'the name is empty'
        case QueueNameEdgeWhitespace():
            return 'the name has whitespace at the start or the end'
        case QueueNameNotUtf8():
            return 'the name holds a lone surrogate and has no UTF-8 text'
        case QueueNameTooLong(byte_length=byte_length):
            return (
                f'the name has {byte_length} bytes of UTF-8 text; '
                f'the maximum is {QUEUE_NAME_MAX_BYTES}'
            )
        case _ as unreachable:
            assert_never(unreachable)
