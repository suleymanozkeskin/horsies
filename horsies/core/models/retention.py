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

from pydantic import BaseModel, ConfigDict, Field, model_validator

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
)
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DAYS,
)
from horsies.core.history.periods import (
    MAX_RETENTION_DAYS,
    PeriodRefused,
    RetentionClassKey,
    RetentionDays,
    parse_retention_period,
    period_class_key,
)
from horsies.core.types.result import Err, Ok


def resolve_queue_retention_class(
    retention: object, queue_name: str
) -> RetentionClassKey | None:
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
    match retention:
        case RetentionConfig() as config:
            return config.queue_class(queue_name)
        case _:
            return DEFAULT_RETENTION_CLASS_KEY


def _validated_days(period: timedelta) -> RetentionDays:
    """The days of a period that the config validator accepted.

    The readers derive the days from the public fields on every call, so
    the fields stay the only copy of the configuration. A period that fails
    here was changed after validation, which is a defect.
    """
    match parse_retention_period(period):
        case Ok(ok_value=days):
            return days
        case Err(err_value=refused):
            raise AssertionError(
                f'retention period changed after validation: {refused.reason}'
            )


def _period_error(subject: str, refused: PeriodRefused) -> ConfigurationError:
    """The refusal of a period that is not whole days within the bounds."""
    return ConfigurationError(
        message=f'{subject}: {refused.reason}',
        code=ErrorCode.CONFIG_INVALID_RETENTION,
        notes=[
            'a retention class is identified by its period, and '
            'history partitions are daily',
        ],
        help_text=(
            f'use whole days from 1 to {MAX_RETENTION_DAYS}, or None '
            'to keep records forever'
        ),
    )


class RetentionConfig(BaseModel):
    """How long records live, and how their storage is kept ahead."""

    model_config = ConfigDict(extra='forbid')


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

    retention_periods: tuple[timedelta, ...] = Field(
        default=(),
        description=(
            'Finite periods a send may choose with '
            'with_options(retention=...) besides the 30-day default and '
            'the mapped periods. Whole days, 1 to 1825. The maintenance '
            'owner registers the class of each one'
        ),
    )

    queue_retention: dict[str, timedelta | None] = Field(
        default_factory=dict,
        description=(
            'Per-queue retention: how long a task sent on this queue '
            'keeps its history record (whole days, 1 to 1825), or None '
            'to keep it forever. The class is resolved and recorded when '
            'the task is sent, so a later edit governs later sends and '
            'never rewrites the promise made to records already enqueued. '
            'An explicit retention at the send call overrides the mapping'
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

    @model_validator(mode='before')
    @classmethod
    def reject_removed_retention_classes(cls, data: object) -> object:
        """Fail closed on the named classes that 0.6.1 replaced.

        A class is identified by its period now, so a name with its own
        duration cannot be declared. A configuration that still declares
        one is corrected, never silently ignored.
        """
        match data:
            case dict() if 'retention_classes' in data:
                raise ValueError(
                    'retention_classes was removed in 0.6.1: a retention '
                    'class is identified by its period. Map a queue to the '
                    'period in queue_retention, or list a period that sends '
                    'choose explicitly in retention_periods and send with '
                    'with_options(retention=timedelta(days=...))'
                )
            case _:
                return data

    def registrable_classes(
        self,
    ) -> tuple[tuple[RetentionClassKey, timedelta], ...]:
        """Every finite class the maintenance owner must register.

        One class for each distinct finite period that a send can stamp:
        the mapped periods and ``retention_periods``. Queues with the same
        period share the class. The default class is registered by the
        library, so it is not repeated here. Queues mapped to None
        contribute nothing: forever is a class the library owns.
        """
        return tuple(
            (period_class_key(days), timedelta(days=days))
            for days in sorted(self._mapped_days() | self._listed_days())
            if days != DEFAULT_RETENTION_DAYS
        )

    def sendable_periods(self) -> frozenset[RetentionDays]:
        """The finite periods a send may choose explicitly.

        The default period, every mapped period, and ``retention_periods``:
        the periods whose classes this process's maintenance owner registers.
        """
        return (
            self._mapped_days()
            | self._listed_days()
            | {DEFAULT_RETENTION_DAYS}
        )

    def queue_class(self, queue_name: str) -> RetentionClassKey | None:
        """The class a send on this queue stamps when it chooses none.

        The mapped period's class, None for a queue mapped to forever, or
        the default class for an unmapped queue.
        """
        match queue_name in self.queue_retention:
            case False:
                return DEFAULT_RETENTION_CLASS_KEY
            case True:
                pass
        match self.queue_retention[queue_name]:
            case None:
                return None
            case period:
                return period_class_key(_validated_days(period))

    def _mapped_days(self) -> frozenset[RetentionDays]:
        """The days of every finite mapped period, read from the field."""
        return frozenset(
            _validated_days(period)
            for period in self.queue_retention.values()
            if period is not None
        )

    def _listed_days(self) -> frozenset[RetentionDays]:
        """The days of every listed period, read from the field."""
        return frozenset(
            _validated_days(period) for period in self.retention_periods
        )

    @model_validator(mode='after')
    def validate_retention_periods(self) -> Self:
        """Refuse a mapped or listed period that is not whole days in bounds.

        A queue name is only a lookup key, so any configured queue name can
        be mapped (`AppConfig` checks that it is configured). Every problem
        is collected and reported together. Nothing is cached: the readers
        derive the days from the fields, so an edit to a field is what the
        next send stamps.
        """
        report = ValidationReport('retention')
        for queue_name, period in self.queue_retention.items():
            match period:
                case None:
                    pass
                case finite:
                    match parse_retention_period(finite):
                        case Ok():
                            pass
                        case Err(err_value=refused):
                            report.add(_period_error(
                                f'queue {queue_name!r} maps to {finite}', refused,
                            ))
        listed: set[RetentionDays] = set()
        for period in self.retention_periods:
            match parse_retention_period(period):
                case Err(err_value=refused):
                    report.add(_period_error(
                        f'retention period {period}', refused,
                    ))
                case Ok(ok_value=days) if days in listed:
                    report.add(
                        ConfigurationError(
                            message=f'retention period {period} listed twice',
                            code=ErrorCode.CONFIG_INVALID_RETENTION,
                            notes=['a period has exactly one class'],
                            help_text='remove the repeated period',
                        )
                    )
                case Ok(ok_value=days):
                    listed.add(days)
        raise_collected(report)
        return self
