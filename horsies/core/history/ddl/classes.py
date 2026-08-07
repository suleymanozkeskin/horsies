"""Finite retention-class registration.

A retention class is immutable once registered: enqueue snapshots the class
onto every request, so changing a duration in place would rewrite promises
already made. Registration therefore either creates the class — metadata
row plus its RANGE parent attached under the LIST hierarchy — verifies an
identical existing registration, or refuses with the conflicting facts.
Changing a duration means registering a new class key.

Class keys come from the caller: key-derivation policy (how a configured
duration names its class) belongs to the configuration layer, not to DDL.
The key must be usable inside a relation name, which is stricter than the
column bound and enforced here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import is_safe_identifier
from ..names import RETENTION_CLASSES, TASK_HISTORY_PARENT
from ..partitions.catalog import RetentionClassRow, read_retention_class
from .tables import FOREVER_CLASS_KEY

DEFAULT_RETENTION_CLASS_KEY = 'standard_30d'
"""The class every enqueue stamps unless the caller says otherwise.

The ratified resolution: the default is an immutable 30-day finite
class; forever is chosen only by an EXPLICIT ``None``. A forever
default would exempt every unconfigured task from retention — the
unpurgeable-rows hole this program exists to close."""

DEFAULT_RETENTION_DURATION = timedelta(days=30)
"""The default class's immutable duration."""


def resolve_retention_class_key(requested: str | None) -> str:
    """The stamped class key for one enqueue.

    ``None`` is the explicit forever choice; any string names a class
    verbatim. The absent-argument default lives at the enqueue
    signatures as ``DEFAULT_RETENTION_CLASS_KEY``, so absence and
    explicit forever cannot be confused.
    """
    return FOREVER_CLASS_KEY if requested is None else requested


@dataclass(frozen=True, slots=True)
class ClassRegistered:
    """The class row and its RANGE parent now exist."""

    class_key: str
    finite_parent_name: str


@dataclass(frozen=True, slots=True)
class ClassAlreadyRegistered:
    """An identical registration already exists; nothing changed."""

    class_key: str


@dataclass(frozen=True, slots=True)
class ClassConflict:
    """The key exists with different facts; classes are immutable."""

    class_key: str
    existing_duration: timedelta | None
    existing_partition_interval: timedelta | None


type ClassRegistration = ClassRegistered | ClassAlreadyRegistered | ClassConflict


def finite_class_parent_name(class_key: str) -> str:
    """The RANGE parent relation name for one finite class."""
    name = f'{TASK_HISTORY_PARENT}_{class_key}'
    if not is_safe_identifier(name):
        raise ValueError(
            f'class key {class_key!r} does not form a safe relation name'
        )
    return name


async def register_finite_retention_class(
    connection: AsyncConnection,
    *,
    class_key: str,
    duration: timedelta,
    partition_interval: timedelta = timedelta(days=1),
) -> ClassRegistration:
    """Create or verify one finite class inside the caller's transaction."""
    if duration <= timedelta(0):
        raise ValueError('retention duration must be positive')
    if partition_interval != timedelta(days=1):
        raise ValueError('finite classes use daily partition intervals')
    parent_name = finite_class_parent_name(class_key)

    existing = await read_retention_class(connection, class_key)
    match existing:
        case RetentionClassRow(
            duration=existing_duration,
            partition_interval=existing_interval,
            finite_parent_name=existing_parent,
        ) if (
            existing_duration == duration
            and existing_interval == partition_interval
            and existing_parent == parent_name
        ):
            return ClassAlreadyRegistered(class_key=class_key)
        case RetentionClassRow():
            return ClassConflict(
                class_key=class_key,
                existing_duration=existing.duration,
                existing_partition_interval=existing.partition_interval,
            )
        case None:
            pass

    await connection.execute(
        text(
            f"""
            INSERT INTO {RETENTION_CLASSES} (
                class_key, duration, partition_interval,
                finite_parent_name, created_at
            ) VALUES (
                :class_key, :duration, :partition_interval,
                :finite_parent_name, statement_timestamp()
            )
            """
        ),
        {
            'class_key': class_key,
            'duration': duration,
            'partition_interval': partition_interval,
            'finite_parent_name': parent_name,
        },
    )
    await connection.execute(
        text(
            f"""
            CREATE TABLE {parent_name}
                PARTITION OF {TASK_HISTORY_PARENT}
                FOR VALUES IN ('{class_key}')
                PARTITION BY RANGE (retention_anchor_at)
            """
        )
    )
    return ClassRegistered(class_key=class_key, finite_parent_name=parent_name)
