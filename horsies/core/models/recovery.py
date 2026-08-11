# horsies/core/models/recovery.py
from __future__ import annotations
from typing import Annotated, Self, cast
from pydantic import BaseModel, Field, model_validator
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
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
    - check_interval_ms: How often the reaper checks for stale tasks
    - runner_heartbeat_interval_ms: How often RUNNING tasks send heartbeats from inside the task process
    - claimer_heartbeat_interval_ms: How often CLAIMED tasks send heartbeats
    - worker_state_snapshot_interval_ms: How often each worker persists a monitoring snapshot row
    - worker_state_retention_hours: Keep worker_state rows for this long (None disables cleanup)
    - terminal_record_retention_hours: Keep terminal WORKFLOW rows for this long (None disables cleanup);
      terminal task rows live in the task-history archive and age by retention class
    - retention_sweep_interval_s: Seconds between retention sweep passes
    - retention_delete_batch_size: Rows per retention DELETE batch
    """

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
        description='Automatically mark stale RUNNING tasks as FAILED (not safe to requeue)',
    )
    running_stale_threshold_ms: Annotated[int, Field(ge=1_000, le=7_200_000)] = Field(
        default=300_000,  # 5 minutes
        description='Milliseconds without runner heartbeat before RUNNING task is considered stale (1s-2hr)',
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
            'preserves the evidence (3-1000).'
        ),
    )

    check_interval_ms: Annotated[int, Field(ge=1_000, le=600_000)] = Field(
        default=30_000,  # 30 seconds
        description='How often the reaper checks for stale tasks in milliseconds (1s-10min)',
    )

    runner_heartbeat_interval_ms: Annotated[int, Field(ge=1_000, le=120_000)] = Field(
        default=30_000,  # 30 seconds
        description=(
            'How often RUNNING tasks send heartbeats from inside the task process in milliseconds (5s-2min); '
            'increase stale thresholds for CPU/GIL-heavy tasks to avoid false positives'
        ),
    )

    claimer_heartbeat_interval_ms: Annotated[int, Field(ge=1_000, le=120_000)] = Field(
        default=30_000,  # 30 seconds
        description='How often worker sends heartbeats for CLAIMED tasks in milliseconds (5s-2min)',
    )

    worker_state_snapshot_interval_ms: Annotated[int, Field(ge=1_000, le=300_000)] = Field(
        default=30_000,  # 30 seconds
        description=(
            'How often each worker persists a worker-state snapshot (monitoring '
            'timeseries) in milliseconds (1s-5min); each snapshot is one row in '
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

    @model_validator(mode='after')
    def validate_heartbeat_thresholds(self) -> Self:
        """Ensure stale thresholds are at least 2x heartbeat intervals for reliability.

        Collects both errors (if present) and raises them together.
        """
        report = ValidationReport('recovery')
        min_running = self.runner_heartbeat_interval_ms * 2
        min_claimed = self.claimer_heartbeat_interval_ms * 2

        # Validate runner heartbeat vs running stale threshold
        if self.running_stale_threshold_ms < min_running:
            report.add(
                ConfigurationError(
                    message='running_stale_threshold_ms too low',
                    code=ErrorCode.CONFIG_INVALID_RECOVERY,
                    notes=[
                        f'running_stale_threshold_ms={self.running_stale_threshold_ms}ms ({self.running_stale_threshold_ms/1000:.1f}s)',
                        f'runner_heartbeat_interval_ms={self.runner_heartbeat_interval_ms}ms ({self.runner_heartbeat_interval_ms/1000:.1f}s)',
                        'threshold must be at least 2x heartbeat interval',
                    ],
                    help_text=f'set running_stale_threshold_ms >= {min_running}ms ({min_running/1000:.1f}s)',
                )
            )

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
