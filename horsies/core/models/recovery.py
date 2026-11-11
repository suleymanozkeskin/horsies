# horsies/core/models/recovery.py
from __future__ import annotations
from dataclasses import asdict, dataclass
from typing import Annotated, Final, Self, cast
from pydantic import BaseModel, ConfigDict, Field, model_validator
from horsies.core.defaults import WORKER_STATE_LIVE_WINDOW_S
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
)


DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT: Final = 25
"""Reaper passes with a persistent recovery raise before a workflow fails."""


WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS: Final = WORKER_STATE_LIVE_WINDOW_S * 1_000 // 2
"""Largest `worker_state_snapshot_interval_ms`: half the live window.

A worker reads as live only while its latest snapshot is inside the live
window. At half the window, the other half is room for a late snapshot
write."""


FAIL_CLOSED_WINDOW_DIVISOR: Final = 2
"""The fail-closed window is `running_stale_threshold_ms` divided by this.

With `auto_fail_stale_running` on, a worker stops when no runner heartbeat
lands for this window. The rest of the threshold is the time left to kill
its children before the reaper acts."""


RUNNER_BEAT_LANDING_ALLOWANCE_MS: Final = 2_000
"""Time a runner heartbeat may take to land, counted from its planned start.

Two beats land one `runner_heartbeat_interval_ms` apart, plus the time the
second one takes. So the fail-closed window must hold one interval plus
this allowance, or the worker stops while every beat lands. The allowance
holds the heartbeat statement, a reconnect, and the wake-up of the
heartbeat thread."""


def fail_closed_threshold_floor_ms(runner_heartbeat_interval_ms: int) -> int:
    """Smallest `running_stale_threshold_ms` with `auto_fail_stale_running` on.

    Its fail-closed window holds one beat interval plus
    `RUNNER_BEAT_LANDING_ALLOWANCE_MS`.
    """
    return FAIL_CLOSED_WINDOW_DIVISOR * (
        runner_heartbeat_interval_ms + RUNNER_BEAT_LANDING_ALLOWANCE_MS
    )


MOVED_TO_RETENTION: frozenset[str] = frozenset({
    'retention_classes',
    'terminal_record_retention_hours',
    'worker_state_retention_hours',
    'retention_sweep_interval_s',
    'retention_delete_batch_size',
    'history_leaf_horizon_days',
    'heartbeat_leaf_horizon_hours',
    'partition_maintenance_interval_s',
    'paused_workflow_auto_cancel_after',
})
"""Fields that now live on `AppConfig.retention`. Named here so the
refusal can point at the exact successor rather than a page."""


@dataclass(slots=True, frozen=True)
class OrphanTaskAuditReport:
    """Metrics for one bounded orphan workflow-task page."""

    rows_selected: int
    candidates_returned: int
    cancelled: int
    duration_ms: int

    def health(self) -> dict[str, int | str]:
        return {
            'state': 'ready',
            **asdict(self),
            'refusals': 0,
            'errors': 0,
        }


class RecoveryConfig(BaseModel):
    """
    Configuration for automatic stale task handling and crash recovery.

    Stale task detection and recovery:
    - CLAIMED tasks that never start executing: safe to auto-requeue
    - RUNNING tasks that go stale: mark as FAILED (may not be idempotent)

    All time values are in milliseconds for consistency with timeout_ms.

    Fields:
    - auto_requeue_stale_claimed: If True, automatically requeue tasks stuck in CLAIMED
    - claimed_stale_threshold_ms: Milliseconds without heartbeat before CLAIMED task is stale
    - auto_terminate_orphaned_workflow_tasks: If True (default), cancel orphaned workflow
      tasks (CLAIMED, workflow_task linkage missing or terminal) at finalization and in the
      reaper. If False, they are left CLAIMED for inspection — never requeued (futile, they
      cannot reach RUNNING) and never deleted by retention (their workflow is retained while
      the backing task is non-terminal). Orphans are never requeued regardless of this flag.
    - auto_fail_stale_running: If True, automatically mark stale RUNNING tasks as FAILED
    - running_stale_threshold_ms: Milliseconds without heartbeat before RUNNING task is stale
    - finalizing_stale_threshold_ms: Milliseconds a task may remain finalizing before recovery
    - crashed_worker_recovery_grace_ms: Grace before recovering a terminal task whose workflow progression was not applied
    - phase2_quarantine_after_attempts: Recovery passes an unresolvable phase-2 pending row may retain before it is quarantined
    - workflow_recovery_failure_limit: Reaper passes with a persistent recovery raise before a workflow fails closed
    - check_interval_ms: How often the reaper checks for stale tasks
    - orphan_task_audit_interval_ms: How often the orphan cursor scans one page
    - runner_heartbeat_interval_ms: How often the worker process writes the heartbeats of its RUNNING tasks
    - claimer_heartbeat_interval_ms: How often CLAIMED tasks send heartbeats
    - worker_state_snapshot_interval_ms: How often each worker persists a monitoring snapshot row
      (at most WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS, half the live window)

    Retention fields live on `AppConfig.retention` (`RetentionConfig`).
    """

    model_config = ConfigDict(extra='forbid')

    auto_requeue_stale_claimed: bool = Field(
        default=True,
        description='Automatically requeue tasks stuck in CLAIMED (safe - user code never ran)',
    )
    claimed_stale_threshold_ms: Annotated[int, Field(ge=1_000, le=3_600_000)] = Field(
        default=120_000,  # 2 minutes
        description='Milliseconds without claimer heartbeat before CLAIMED task is considered stale (1s-1hr)',
    )

    auto_terminate_orphaned_workflow_tasks: bool = Field(
        default=True,
        description=(
            'Cancel orphaned workflow tasks (CLAIMED, workflow_task linkage missing '
            'or terminal) at finalization and in the reaper. When False they are left '
            'CLAIMED for inspection: never requeued (futile - they cannot reach RUNNING) '
            'and never deleted by retention while the backing task is non-terminal'
        ),
    )

    auto_fail_stale_running: bool = Field(
        default=True,
        description=(
            'Automatically mark stale RUNNING tasks as FAILED (not safe to requeue); '
            'when on, a worker whose runner heartbeats cannot land for half of '
            'running_stale_threshold_ms kills its tasks and stops'
        ),
    )
    running_stale_threshold_ms: Annotated[int, Field(ge=1_000, le=7_200_000)] = Field(
        default=300_000,  # 5 minutes
        description=(
            'Milliseconds without runner heartbeat before RUNNING task is considered '
            'stale (1s-2hr); with auto_fail_stale_running on, at least '
            '2 x (runner_heartbeat_interval_ms + '
            f'{RUNNER_BEAT_LANDING_ALLOWANCE_MS}ms)'
        ),
    )
    finalizing_stale_threshold_ms: Annotated[
        int, Field(ge=1_000, le=7_200_000),
    ] = Field(
        default=300_000,
        description='Milliseconds a completed child may wait for parent finalization before recovery (1s-2hr)',
    )
    crashed_worker_recovery_grace_ms: Annotated[
        int, Field(ge=0, le=3_600_000),
    ] = Field(
        default=10_000,
        description=(
            'Grace (ms) before the reaper recovers a workflow task whose '
            'underlying task is terminal but whose workflow progression was '
            'not applied (the parent finalizer committed the task terminal in '
            'one transaction, then advances the workflow DAG in a second). '
            'Within this window the finalizer is presumed still in flight, so '
            'the reaper leaves the task alone instead of racing it. Set well '
            'above the healthy task-terminal-to-progression latency (~seconds) '
            'and below the crash-recovery SLO you can tolerate; a genuine crash '
            'in that gap recovers after the grace plus one reaper sweep. 0 '
            'disables the grace (immediate recovery, 0s-1hr). Not coupled to '
            'heartbeat intervals — tune it independently.'
        ),
    )

    phase2_quarantine_after_attempts: Annotated[
        int, Field(ge=3, le=1_000),
    ] = Field(
        default=25,
        description=(
            'Recovery passes an unresolvable phase-2 pending row may retain '
            'before it is quarantined. Retaining dispositions are either '
            'transient races, which resolve within one or two passes, or '
            'structural conflicts, which never resolve: 25 gives a transient '
            'an order of magnitude more passes than it needs while bounding '
            'a structural row to 25 logged errors before its evidence moves '
            'to the quarantine table and discovery stops retrying it. Floor '
            '3 keeps a burst of transient failures from quarantining healthy '
            'recovery; both directions stay recoverable because quarantine '
            'preserves the evidence. A progression that raises a persistent '
            'error counts too, and at the bound its workflow fails closed '
            '(3-1000).'
        ),
    )

    workflow_recovery_failure_limit: Annotated[
        int, Field(ge=3, le=1_000),
    ] = Field(
        default=DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT,
        description=(
            'Reaper passes in which the recovery of a workflow may raise a '
            'persistent error before the workflow fails with '
            'WORKFLOW_RECOVERY_LIMIT_REACHED. A connection, lock or timeout '
            'error does not count, so a database outage cannot fail a '
            'workflow. At the default check interval, the default limit is '
            'reached 24 intervals after the first pass, about 720 seconds. '
            'The count is stored, so a worker '
            'restart keeps it, and it has no reset (3-1000).'
        ),
    )

    check_interval_ms: Annotated[int, Field(ge=1_000, le=600_000)] = Field(
        default=30_000,  # 30 seconds
        description='How often the reaper checks for stale tasks in milliseconds (1s-10min)',
    )

    orphan_task_audit_interval_ms: Annotated[
        int,
        Field(ge=1_000, le=86_400_000),
    ] = Field(
        default=60_000,
        description=(
            'Milliseconds between bounded orphan workflow-task audit pages '
            '(1s-24hr)'
        ),
    )

    runner_heartbeat_interval_ms: Annotated[int, Field(ge=1_000, le=120_000)] = Field(
        default=30_000,  # 30 seconds
        description=(
            'How often the worker process writes the heartbeats of its RUNNING tasks in milliseconds (1s-2min); '
            'a task that holds the GIL in its child does not delay them'
        ),
    )

    claimer_heartbeat_interval_ms: Annotated[int, Field(ge=1_000, le=120_000)] = Field(
        default=30_000,  # 30 seconds
        description='How often worker sends heartbeats for CLAIMED tasks in milliseconds (1s-2min)',
    )

    # The upper bound is WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS. The
    # after-validator checks it, so the refusal can state why.
    worker_state_snapshot_interval_ms: Annotated[int, Field(ge=1_000)] = Field(
        default=30_000,  # 30 seconds
        description=(
            'How often each worker persists a worker-state snapshot (monitoring '
            'timeseries) in milliseconds '
            f'(1000-{WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS}); at most half the '
            f'{WORKER_STATE_LIVE_WINDOW_S} s live window, so a running worker '
            'stays live when a snapshot write is late by up to half the window; '
            'each snapshot is one row in '
            'horsies_worker_states, so shorter intervals grow the table faster'
        ),
    )

    @model_validator(mode='before')
    @classmethod
    def reject_moved_retention_fields(cls, data: object) -> object:
        """Fail closed on fields that moved to `AppConfig.retention`.

        These decide how long records live, which is data lifecycle
        rather than crash recovery. Pre-1.0 carries no shims: a config
        naming them here is corrected, never silently honoured on a
        model that no longer owns them. Every misplaced field is
        reported at once so one edit fixes the config.
        """
        if not isinstance(data, dict):
            return data
        fields = cast('dict[str, object]', data)
        report = ValidationReport('recovery')
        for name in MOVED_TO_RETENTION:
            if name in fields:
                report.add(
                    ConfigurationError(
                        message=f'{name} moved to AppConfig.retention',
                        # Deliberately a RECOVERY code, not
                        # CONFIG_INVALID_RETENTION, despite the retention
                        # wording: the user misconfigured RecoveryConfig
                        # and RecoveryConfig is refusing. A moved-field
                        # error belongs to the object that no longer has
                        # the field, which is the only object positioned
                        # to notice. The remedy living in retention does
                        # not make the refusal a retention refusal.
                        code=ErrorCode.CONFIG_INVALID_RECOVERY,
                        notes=[
                            'it governs how long records live, not how '
                            'stalled work is detected',
                        ],
                        help_text=f'set AppConfig.retention.{name}',
                    )
                )
        raise_collected(report)
        return fields

    @model_validator(mode='before')
    @classmethod
    def reject_removed_retention_knobs(cls, data: object) -> object:
        """Fail closed on knobs whose object no longer exists.

        Terminal task rows move to the task-history archive and age by
        RETENTION CLASS; heartbeat rows live in partitions that drop
        whole. A configuration naming the removed knobs is corrected,
        never silently ignored.
        """
        if isinstance(data, dict):
            fields = cast('dict[str, object]', data)
            removed = {
                'queue_terminal_record_retention_hours': (
                    'terminal task rows age by their retention class in '
                    'the task-history archive; map the queue in '
                    'AppConfig.retention.queue_retention instead, which '
                    'takes a duration and drops partitions rather than '
                    'deleting rows'
                ),
                'heartbeat_retention_hours': (
                    'heartbeat rows live in time-partitioned leaves that '
                    'drop whole; a row-delete window no longer exists'
                ),
            }
            for name, successor in removed.items():
                if name in fields:
                    raise ValueError(
                        f'{name} was removed in 0.5.0: {successor}'
                    )
            return fields
        return data

    def _running_threshold_errors(self) -> tuple[ConfigurationError, ...]:
        """The refusal of `running_stale_threshold_ms`, or none.

        With `auto_fail_stale_running` on, the floor is
        `fail_closed_threshold_floor_ms`: below it the worker stops while
        every runner heartbeat lands. With it off, the floor is two beat
        intervals.
        """
        threshold_ms = self.running_stale_threshold_ms
        interval_ms = self.runner_heartbeat_interval_ms
        threshold_note = (
            f'running_stale_threshold_ms={threshold_ms}ms ({threshold_ms / 1000:.1f}s)'
        )
        interval_note = (
            f'runner_heartbeat_interval_ms={interval_ms}ms ({interval_ms / 1000:.1f}s)'
        )
        match self.auto_fail_stale_running:
            case True:
                floor_ms = fail_closed_threshold_floor_ms(interval_ms)
                message = (
                    'running_stale_threshold_ms too low for auto_fail_stale_running'
                )
                rule_notes = [
                    'with auto_fail_stale_running on, the worker stops when no '
                    'runner heartbeat lands for half of running_stale_threshold_ms',
                    'that half must hold one runner_heartbeat_interval_ms plus '
                    f'{RUNNER_BEAT_LANDING_ALLOWANCE_MS}ms for a beat to land: '
                    'running_stale_threshold_ms >= 2 x (runner_heartbeat_interval_ms '
                    f'+ {RUNNER_BEAT_LANDING_ALLOWANCE_MS}ms)',
                ]
                remedy = ', or set auto_fail_stale_running=False'
            case False:
                floor_ms = interval_ms * 2
                message = 'running_stale_threshold_ms too low'
                rule_notes = ['threshold must be at least 2x heartbeat interval']
                remedy = ''
        if threshold_ms >= floor_ms:
            return ()
        return (
            ConfigurationError(
                message=message,
                code=ErrorCode.CONFIG_INVALID_RECOVERY,
                notes=[threshold_note, interval_note, *rule_notes],
                help_text=(
                    f'set running_stale_threshold_ms >= {floor_ms}ms '
                    f'({floor_ms / 1000:.1f}s){remedy}'
                ),
            ),
        )

    @model_validator(mode='after')
    def validate_heartbeat_thresholds(self) -> Self:
        """Refuse stale thresholds that a heartbeat cannot keep.

        `running_stale_threshold_ms`: see `_running_threshold_errors`. The
        finalizing and claimed thresholds are at least two intervals of
        their heartbeat. Collects every error and raises them together.
        """
        report = ValidationReport('recovery')
        min_running = self.runner_heartbeat_interval_ms * 2
        min_claimed = self.claimer_heartbeat_interval_ms * 2

        for error in self._running_threshold_errors():
            report.add(error)

        if self.finalizing_stale_threshold_ms < min_running:
            report.add(
                ConfigurationError(
                    message='finalizing_stale_threshold_ms too low',
                    code=ErrorCode.CONFIG_INVALID_RECOVERY,
                    notes=[
                        f'finalizing_stale_threshold_ms={self.finalizing_stale_threshold_ms}ms ({self.finalizing_stale_threshold_ms/1000:.1f}s)',
                        f'runner_heartbeat_interval_ms={self.runner_heartbeat_interval_ms}ms ({self.runner_heartbeat_interval_ms/1000:.1f}s)',
                        'threshold must be at least 2x runner heartbeat interval',
                    ],
                    help_text=f'set finalizing_stale_threshold_ms >= {min_running}ms ({min_running/1000:.1f}s)',
                )
            )

        # Validate claimer heartbeat vs claimed stale threshold
        if self.claimed_stale_threshold_ms < min_claimed:
            report.add(
                ConfigurationError(
                    message='claimed_stale_threshold_ms too low',
                    code=ErrorCode.CONFIG_INVALID_RECOVERY,
                    notes=[
                        f'claimed_stale_threshold_ms={self.claimed_stale_threshold_ms}ms ({self.claimed_stale_threshold_ms/1000:.1f}s)',
                        f'claimer_heartbeat_interval_ms={self.claimer_heartbeat_interval_ms}ms ({self.claimer_heartbeat_interval_ms/1000:.1f}s)',
                        'threshold must be at least 2x heartbeat interval',
                    ],
                    help_text=f'set claimed_stale_threshold_ms >= {min_claimed}ms ({min_claimed/1000:.1f}s)',
                )
            )

        raise_collected(report)
        return self

    @model_validator(mode='after')
    def validate_snapshot_interval_within_live_window(self) -> Self:
        """Refuse a snapshot interval above `WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS`.

        A worker reads as live only while its latest snapshot is inside
        the live window. A longer interval makes a live worker read as
        not live between its snapshots.
        """
        interval_ms = self.worker_state_snapshot_interval_ms
        if interval_ms > WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS:
            raise ConfigurationError(
                message=(
                    f'worker_state_snapshot_interval_ms={interval_ms}ms is above '
                    f'the cap of {WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS}ms'
                ),
                code=ErrorCode.CONFIG_INVALID_RECOVERY,
                notes=[
                    'the cap is half the live window '
                    f'(WORKER_STATE_LIVE_WINDOW_S={WORKER_STATE_LIVE_WINDOW_S}s)',
                    'a worker reads as live only while its latest snapshot is '
                    'inside the live window; the other half of the window is '
                    'room for a late snapshot write',
                ],
                help_text=(
                    'set worker_state_snapshot_interval_ms <= '
                    f'{WORKER_STATE_SNAPSHOT_INTERVAL_MAX_MS}ms'
                ),
            )
        return self
