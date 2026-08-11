# horsies/core/models/retention.py
"""Data-lifecycle configuration: how long records live and how they age.

Split out of `RecoveryConfig` because the two answer different
questions. Recovery is about work that has gone wrong — stale claims,
dead runners, thresholds for noticing. Retention is about work that went
right and is now history: how long its record survives, how partitions
are kept ahead of writes, and how the maintenance owner ages them out.
A knob that decides when a record is deleted was never a recovery knob.

`paused_workflow_auto_cancel_after` moves here with them: expiring a
workflow that has sat paused past a declared age is a data-lifecycle
policy on workflow rows, not a response to a crash.
"""

from __future__ import annotations

from typing import Annotated, Final, Self

from datetime import timedelta

from pydantic import BaseModel, Field, model_validator

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
)
from horsies.core.history.commands import is_safe_identifier
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.ddl.tables import FOREVER_CLASS_KEY
from horsies.core.history.names import (
    HEARTBEAT_CLASS_KEY,
    MAX_RETENTION_CLASS_KEY_LENGTH,
    POSTGRES_IDENTIFIER_LIMIT,
    TASK_HISTORY_PARENT,
)


RESERVED_RETENTION_CLASS_KEYS: frozenset[str] = frozenset(
    {DEFAULT_RETENTION_CLASS_KEY, FOREVER_CLASS_KEY, HEARTBEAT_CLASS_KEY}
)
"""Keys the library owns. A declaration may not redefine one: their
durations are fixed by the library and a conflicting redeclaration would
be refused by the registration machinery at startup anyway."""


QUEUE_DERIVED_CLASS_PREFIX: Final = 'q_'
"""Namespace for classes derived from `queue_retention`.

Reserved: a declared class may not start with it. Adopters do not name
these classes, so the prefix keeps a hand-written declaration from
colliding with one the mapping generates.
"""


class DurationNotWholeSeconds(Exception):
    """A duration that cannot be rendered into a class key exactly.

    Raised rather than rounded. The key encodes the duration, so a
    rounded key would name a retention period the class does not have.
    """


def render_duration(duration: timedelta) -> str:
    """A duration as the shortest exact unit: `7d`, `36h`, `90m`, `45s`.

    Largest-first so common horizons stay short — the rendering is part
    of a relation name and every character is budget.
    """
    total_seconds = duration.total_seconds()
    if total_seconds != int(total_seconds):
        raise DurationNotWholeSeconds(repr(duration))
    total = int(total_seconds)
    for unit, size in (('d', 86_400), ('h', 3_600), ('m', 60)):
        if total % size == 0:
            return f'{total // size}{unit}'
    return f'{total}s'


def derived_queue_class_key(queue_name: str, duration: timedelta) -> str:
    """The retention class a queue mapping stands for.

    The duration is IN the key, so changing a queue's mapping mints a new
    class rather than redefining an existing one. That is what lets
    records enqueued under the old promise age out under it while new
    records take the new one — a class has exactly one duration, and
    rewriting it would silently restate the promise already made to rows
    sitting in the old class's partitions.
    """
    return (
        f'{QUEUE_DERIVED_CLASS_PREFIX}{queue_name}_'
        f'{render_duration(duration)}'
    )


def resolve_queue_retention_class(
    retention: object, queue_name: str
) -> str | None:
    """The class a queue's mapping stands for, or the default class.

    The single definition every enqueue path resolves through — immediate
    sends, delayed sends, cron fires, and workflow backing tasks. They
    reach their configuration by different routes, so the routes differ
    but the rule must not: a queue mapped here means the same thing
    whatever put the task on it.

    `retention` is typed as `object` because callers reach it through
    attributes that are not statically known (`broker.app.config`,
    `app.config`). Anything that is not a `RetentionConfig` carries no
    mapping, so the default class is the answer.

    Returns `None` for a queue mapped to forever — `None` is the explicit
    forever choice everywhere in this codebase, not an absence.
    """
    if not isinstance(retention, RetentionConfig):
        return DEFAULT_RETENTION_CLASS_KEY
    if queue_name not in retention.queue_retention:
        return DEFAULT_RETENTION_CLASS_KEY
    duration = retention.queue_retention[queue_name]
    if duration is None:
        return None
    return derived_queue_class_key(queue_name, duration)


def _class_key_length_error(
    key: str,
    *,
    help_text: str,
    extra_notes: tuple[str, ...] = (),
) -> ConfigurationError | None:
    """Refuse a class key too long for the relations it will name.

    Shared by both key sources — keys an adopter declares and keys
    derived from a queue mapping — because both end up interpolated into
    the same relation names and both fail the same way.

    Refusing here is the whole point. The key is accepted by
    `is_safe_identifier` up to the identifier limit itself, so without
    this check an over-long key registers successfully and only fails
    later, inside the maintenance pass, on a database row that removing
    the declaration no longer reaches.
    """
    if len(key) <= MAX_RETENTION_CLASS_KEY_LENGTH:
        return None
    longest = f'{TASK_HISTORY_PARENT}_{key}_2026_08_11_enqueued_idx'
    return ConfigurationError(
        message=(
            f'retention class key {key!r} is {len(key)} characters; '
            f'the limit is {MAX_RETENTION_CLASS_KEY_LENGTH}'
        ),
        code=ErrorCode.CONFIG_INVALID_RECOVERY,
        notes=[
            *extra_notes,
            'the key is interpolated into every relation the class owns',
            f'longest of them: {longest}',
            f'that is {len(longest)} bytes against PostgreSQL\'s '
            f'{POSTGRES_IDENTIFIER_LIMIT}-byte identifier limit',
            'over-long names are truncated rather than rejected, so two '
            'of a leaf\'s indexes can collide on one name',
        ],
        help_text=help_text,
    )


class RetentionClassConfig(BaseModel):
    """One adopter-declared finite retention class.

    Declaring a class does not create it. The maintenance owner registers
    every declared class at startup and on each pass, exactly as it
    registers the classes the library ships — so DDL stays out of adopter
    hands and registration keeps its single owner.

    ``duration`` is a MINIMUM, not an exact age. History leaves span one
    day, and a leaf is dropped only once its whole day is past the
    duration, so a row survives between ``duration`` and
    ``duration + 1 day``. Sub-day durations are therefore safe — they
    never under-retain — but they cannot expire faster than daily
    partition granularity allows.
    """

    key: str
    duration: timedelta


class RetentionConfig(BaseModel):
    """How long records live, and how their storage is kept ahead."""

    worker_state_retention_hours: Annotated[
        int | None, Field(ge=1, le=24 * 365),
    ] = Field(
        default=24 * 7,
        description='How long to keep worker_state snapshots in hours; set None to disable pruning',
    )

    terminal_record_retention_hours: Annotated[
        int | None, Field(ge=1, le=24 * 365 * 5),
    ] = Field(
        default=24 * 30,
        description=(
            'How long to keep terminal WORKFLOW records '
            '(workflows/workflow_tasks rows) in hours; set None to '
            'disable pruning. Terminal task rows move to the '
            'task-history archive at terminalization and age by their '
            'retention class, not by this window'
        ),
    )

    paused_workflow_auto_cancel_after: timedelta | None = Field(
        default=None,
        description=(
            'Age past which a PAUSED workflow is expired by policy '
            '(WorkflowStatus.EXPIRED); None disables the sweep — no '
            'deployment changes behavior without declaring the rule'
        ),
    )

    history_leaf_horizon_days: Annotated[int, Field(ge=2, le=14)] = Field(
        default=3,
        description=(
            'Complete future daily history leaves the maintenance owner '
            'keeps created ahead of writes; floor 2 is the coverage '
            'health red line, ceiling 14 bounds catalog pre-creation'
        ),
    )

    heartbeat_leaf_horizon_hours: Annotated[int, Field(ge=2, le=48)] = Field(
        default=6,
        description=(
            'Complete future hourly heartbeat leaves kept created '
            'ahead of writes'
        ),
    )

    retention_classes: tuple[RetentionClassConfig, ...] = Field(
        default=(),
        description=(
            'Additional finite retention classes this deployment '
            'declares; the maintenance owner registers each one and '
            'tasks may then be sent into it by key'
        ),
    )

    queue_retention: dict[str, timedelta | None] = Field(
        default_factory=dict,
        description=(
            'Per-queue retention: how long a task sent on this queue '
            'keeps its history record, or None to keep it forever. The '
            'class is resolved and recorded when the task is sent, so a '
            'later edit governs later sends and never rewrites the '
            'promise made to records already enqueued. An explicit '
            'retention_class_key at the send call overrides the mapping'
        ),
    )

    partition_maintenance_interval_s: Annotated[
        int, Field(ge=60, le=3_600),
    ] = Field(
        default=900,
        description=(
            'Seconds between coverage-ensure passes; any value in '
            'bounds refreshes coverage well before either horizon '
            'floor can lapse'
        ),
    )

    retention_sweep_interval_s: Annotated[int, Field(ge=30, le=86_400)] = Field(
        default=300,
        description=(
            'Seconds between retention sweep passes (30s-24h). Frequent small '
            'sweeps keep each pass short instead of accumulating an hourly spike'
        ),
    )

    retention_delete_batch_size: Annotated[int, Field(ge=50, le=10_000)] = Field(
        default=500,
        description=(
            'Rows per retention DELETE batch (50-10000). Bounds per-statement '
            'duration, row locks, and WAL; each batch commits independently'
        ),
    )

    @model_validator(mode='after')
    def paused_expiry_age_is_positive(self) -> Self:
        """A zero or negative age would expire every pause instantly."""
        if (
            self.paused_workflow_auto_cancel_after is not None
            and self.paused_workflow_auto_cancel_after <= timedelta(0)
        ):
            raise ValueError(
                'paused_workflow_auto_cancel_after must be a positive '
                'duration; use None to disable the sweep'
            )
        return self

    def registrable_classes(self) -> tuple[tuple[str, timedelta], ...]:
        """Every finite class the maintenance owner must register.

        Declared classes and the classes a queue mapping derives are the
        same kind of thing to the registrar: a key and a duration it
        must create a partition for. They are returned together so the
        registration path cannot serve one source and miss the other —
        a queue mapped to a class that was never registered would send
        tasks into a partition that does not exist.

        Queues mapped to None contribute nothing: forever is a class the
        library already owns.
        """
        derived = tuple(
            (derived_queue_class_key(queue_name, duration), duration)
            for queue_name, duration in self.queue_retention.items()
            if duration is not None
        )
        declared = tuple(
            (declared.key, declared.duration)
            for declared in self.retention_classes
        )
        return declared + derived

    @model_validator(mode='after')
    def validate_queue_retention(self) -> Self:
        """Refuse a mapping whose derived class could not be created.

        The queue name is not merely a lookup key here — it is spliced
        into the class key and from there into relation names, so it
        carries the same constraints a declared key does and is checked
        the same way.
        """
        report = ValidationReport('retention')
        for queue_name, duration in self.queue_retention.items():
            if not is_safe_identifier(queue_name):
                report.add(
                    ConfigurationError(
                        message=(
                            f'queue {queue_name!r} cannot be mapped: the '
                            'name is not a usable identifier'
                        ),
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[
                            'the queue name becomes part of the derived '
                            'retention class and its relation names',
                        ],
                        help_text=(
                            'use letters, digits and underscores, '
                            'starting with a letter or underscore'
                        ),
                    )
                )
                continue
            if duration is None:
                # Forever is an existing library class, so it derives
                # nothing and has no key to bound.
                continue
            if duration <= timedelta(0):
                report.add(
                    ConfigurationError(
                        message=(
                            f'queue {queue_name!r} maps to a non-positive '
                            'retention'
                        ),
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[f'duration={duration!r}'],
                        help_text=(
                            'map a positive duration, or None to keep '
                            'records forever'
                        ),
                    )
                )
                continue
            try:
                derived = derived_queue_class_key(queue_name, duration)
            except DurationNotWholeSeconds:
                report.add(
                    ConfigurationError(
                        message=(
                            f'queue {queue_name!r} maps to {duration!r}, '
                            'which is not a whole number of seconds'
                        ),
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[
                            'the duration is written into the class key, '
                            'so it must render exactly',
                        ],
                        help_text='round the mapping to whole seconds',
                    )
                )
                continue
            too_long = _class_key_length_error(
                derived,
                help_text=(
                    f'shorten the queue name; {queue_name!r} leaves '
                    f'{len(derived) - len(queue_name)} characters of the '
                    'key to the prefix and duration'
                ),
                extra_notes=(
                    f'queue {queue_name!r} derives the class {derived!r}',
                ),
            )
            if too_long is not None:
                report.add(too_long)
        raise_collected(report)
        return self

    @model_validator(mode='after')
    def validate_retention_classes(self) -> Self:
        """Reject declarations the registration machinery could not honour.

        Every problem is collected, so a config with several bad
        declarations reports all of them once rather than one per run.
        """
        report = ValidationReport('retention')
        seen: set[str] = set()
        for declared in self.retention_classes:
            key = declared.key
            if key in RESERVED_RETENTION_CLASS_KEYS:
                report.add(
                    ConfigurationError(
                        message=f'retention class {key!r} is reserved',
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[
                            'the library owns this key and fixes its '
                            'duration',
                            f'reserved: {sorted(RESERVED_RETENTION_CLASS_KEYS)}',
                        ],
                        help_text='choose a different key',
                    )
                )
            elif key.startswith(QUEUE_DERIVED_CLASS_PREFIX):
                report.add(
                    ConfigurationError(
                        message=(
                            f'retention class {key!r} uses the reserved '
                            f'{QUEUE_DERIVED_CLASS_PREFIX!r} prefix'
                        ),
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[
                            'keys with this prefix are generated from '
                            'queue_retention and their durations come '
                            'from that mapping',
                        ],
                        help_text=(
                            'choose a key without the prefix, or map the '
                            'queue in queue_retention instead'
                        ),
                    )
                )
            elif not is_safe_identifier(key):
                report.add(
                    ConfigurationError(
                        message=f'retention class {key!r} is not a usable '
                                'identifier',
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[
                            'the key becomes part of a PostgreSQL relation '
                            'name for the class partition',
                        ],
                        help_text=(
                            'use letters, digits and underscores, starting '
                            'with a letter or underscore'
                        ),
                    )
                )
            else:
                too_long = _class_key_length_error(
                    key,
                    help_text='shorten the key',
                )
                if too_long is not None:
                    report.add(too_long)
            if key in seen:
                report.add(
                    ConfigurationError(
                        message=f'retention class {key!r} declared twice',
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=['a class has exactly one duration'],
                        help_text='remove the duplicate declaration',
                    )
                )
            seen.add(key)
            if declared.duration <= timedelta(0):
                report.add(
                    ConfigurationError(
                        message=f'retention class {key!r} has a '
                                'non-positive duration',
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[f'duration={declared.duration!r}'],
                        help_text='declare a positive duration',
                    )
                )
        raise_collected(report)
        return self
