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

from typing import Annotated, Self

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
from horsies.core.history.names import HEARTBEAT_CLASS_KEY


RESERVED_RETENTION_CLASS_KEYS: frozenset[str] = frozenset(
    {DEFAULT_RETENTION_CLASS_KEY, FOREVER_CLASS_KEY, HEARTBEAT_CLASS_KEY}
)
"""Keys the library owns. A declaration may not redefine one: their
durations are fixed by the library and a conflicting redeclaration would
be refused by the registration machinery at startup anyway."""


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

    @model_validator(mode='after')
    def validate_retention_classes(self) -> Self:
        """Reject declarations the registration machinery could not honour.

        Every problem is collected, so a config with several bad
        declarations reports all of them once rather than one per run.
        """
        report = ValidationReport('recovery')
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
