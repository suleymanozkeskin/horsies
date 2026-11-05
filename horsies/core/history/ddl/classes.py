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

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

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
    declared_duration: timedelta
    existing_duration: timedelta | None
    existing_partition_interval: timedelta | None
    existing_parent_name: str | None


@dataclass(frozen=True, slots=True)
class ClassNotRegistered:
    """No registry row has the key; registration creates it."""

    class_key: str


type ClassRegistration = ClassRegistered | ClassAlreadyRegistered | ClassConflict
type RegistrationComparison = (
    ClassNotRegistered | ClassAlreadyRegistered | ClassConflict
)

FINITE_PARTITION_INTERVAL: Final = timedelta(days=1)
"""The only partition interval a finite class has."""


def finite_class_parent_name(class_key: str) -> str:
    """The RANGE parent relation name for one finite class."""
    name = f'{TASK_HISTORY_PARENT}_{class_key}'
    if not is_safe_identifier(name):
        raise ValueError(
            f'class key {class_key!r} does not form a safe relation name'
        )
    return name


def compare_registration(
    existing: RetentionClassRow | None,
    *,
    class_key: str,
    duration: timedelta,
) -> RegistrationComparison:
    """How one declared finite class stands against its registry row.

    Pure: the caller reads the row. The interval and the parent name are
    compared with the duration, because a finite class always has the
    daily interval and the parent named from its key. A row that differs
    in any of the three is a conflict.
    """
    match existing:
        case None:
            return ClassNotRegistered(class_key=class_key)
        case RetentionClassRow(
            duration=existing_duration,
            partition_interval=existing_interval,
            finite_parent_name=existing_parent,
        ) if (
            existing_duration == duration
            and existing_interval == FINITE_PARTITION_INTERVAL
            and existing_parent == finite_class_parent_name(class_key)
        ):
            return ClassAlreadyRegistered(class_key=class_key)
        case RetentionClassRow():
            return ClassConflict(
                class_key=class_key,
                declared_duration=duration,
                existing_duration=existing.duration,
                existing_partition_interval=existing.partition_interval,
                existing_parent_name=existing.finite_parent_name,
            )


def describe_class_conflict(conflict: ClassConflict) -> str:
    """One line that names the key, the registered facts and the declaration."""
    match conflict.existing_duration:
        case None:
            registered = 'with no duration (forever)'
        case existing_duration:
            registered = f'with duration {existing_duration}'
    match conflict.existing_partition_interval:
        case interval if interval == FINITE_PARTITION_INTERVAL:
            pass
        case interval:
            registered += f', partition interval {interval}'
    match conflict.existing_parent_name:
        case parent if parent == finite_class_parent_name(conflict.class_key):
            pass
        case parent:
            registered += f', parent relation {parent!r}'
    return (
        f'retention class {conflict.class_key!r} is registered {registered}; '
        f'this configuration declares duration {conflict.declared_duration}'
    )


async def find_declared_class_conflicts(
    connection: AsyncConnection,
    declared: Sequence[tuple[str, timedelta]],
) -> tuple[ClassConflict, ...]:
    """The declared classes whose registry row disagrees with the declaration.

    Read-only: one registry read for each declared key, in the caller's
    transaction. It does not commit and does not register anything. A key
    with no row is not a conflict, because registration creates the row.
    A database without the registry table has no row, so nothing conflicts.
    """
    match declared:
        case ():
            return ()
        case _:
            pass
    registry_present = (
        await connection.execute(
            text('SELECT to_regclass(:relation) IS NOT NULL'),
            {'relation': RETENTION_CLASSES},
        )
    ).scalar_one()
    match bool(registry_present):
        case False:
            return ()
        case True:
            pass
    conflicts: list[ClassConflict] = []
    for class_key, duration in declared:
        existing = await read_retention_class(connection, class_key)
        match compare_registration(
            existing, class_key=class_key, duration=duration,
        ):
            case ClassConflict() as conflict:
                conflicts.append(conflict)
            case ClassNotRegistered() | ClassAlreadyRegistered():
                pass
    return tuple(conflicts)


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
    if partition_interval != FINITE_PARTITION_INTERVAL:
        raise ValueError('finite classes use daily partition intervals')
    parent_name = finite_class_parent_name(class_key)

    existing = await read_retention_class(connection, class_key)
    match compare_registration(
        existing, class_key=class_key, duration=duration,
    ):
        case ClassAlreadyRegistered() as already_registered:
            return already_registered
        case ClassConflict() as conflict:
            return conflict
        case ClassNotRegistered():
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
