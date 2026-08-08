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
                    'the task-history archive; per-queue task retention '
                    'windows no longer exist'
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
