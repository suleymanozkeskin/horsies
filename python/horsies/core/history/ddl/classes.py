"""Finite retention-class registration.

A retention class is immutable once registered: enqueue snapshots the class
onto every request, so changing a duration in place would rewrite promises
already made. Registration therefore either creates the class — a ``PERIOD``
row with its whole days, plus its RANGE parent on ``terminal_at`` attached
under the LIST hierarchy — verifies an identical existing registration, or
refuses with the conflicting facts. Changing a duration means registering a
new class key.

Class keys come from the caller. A finite class key is a function of its
period (`horsies.core.history.periods`), so a key always means one
duration. The key must be usable inside a relation name, which is stricter
than the column bound and enforced here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..names import RETENTION_CLASSES, TASK_HISTORY_PARENT
from ..partitions.catalog import (
    RetentionClassRow,
    history_class_parent_name,
    read_retention_class,
)
from horsies._catalog.retention_class import (
    DEFAULT_RETENTION_CLASS_KEY as DEFAULT_RETENTION_CLASS_KEY,
)

from ..periods import RetentionClassKey, RetentionDays
from .tables import FOREVER_CLASS_KEY

DEFAULT_RETENTION_DAYS: Final = RetentionDays(30)
"""The default period. ``DEFAULT_RETENTION_CLASS_KEY`` (generated from
horsies-core) is its class key."""

DEFAULT_RETENTION_DURATION: Final = timedelta(days=DEFAULT_RETENTION_DAYS)
"""The default class's immutable duration."""


def resolve_retention_class_key(requested: RetentionClassKey | None) -> str:
    """The stamped class key for one enqueue.

    ``None`` is the explicit forever choice; a period class key is stamped
    as it is. The absent-argument default lives at the enqueue signatures
    as ``DEFAULT_RETENTION_CLASS_KEY``, so absence and explicit forever
    cannot be confused.
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
    return history_class_parent_name(class_key)


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
    if duration % timedelta(days=1) != timedelta(0):
        raise ValueError('a retention period is a whole number of days')
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
                class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
            ) VALUES (
                :class_key, 'PERIOD', :retention_days, NULL, statement_timestamp()
            )
            """
        ),
        {
            'class_key': class_key,
            'retention_days': duration.days,
        },
    )
    await connection.execute(
        text(
            f"""
            CREATE TABLE {parent_name}
                PARTITION OF {TASK_HISTORY_PARENT}
                FOR VALUES IN ('{class_key}')
                PARTITION BY RANGE (terminal_at)
            """
        )
    )
    return ClassRegistered(class_key=class_key, finite_parent_name=parent_name)
