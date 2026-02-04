# horsies/core/models/recovery.py
from __future__ import annotations
from typing import Annotated, Self
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
    - auto_fail_stale_running: If True, automatically mark stale RUNNING tasks as FAILED
    - running_stale_threshold_ms: Milliseconds without heartbeat before RUNNING task is stale
    - check_interval_ms: How often the reaper checks for stale tasks
    - runner_heartbeat_interval_ms: How often RUNNING tasks send heartbeats from inside the task process
    - claimer_heartbeat_interval_ms: How often CLAIMED tasks send heartbeats
    """

    auto_requeue_stale_claimed: bool = Field(
        default=True,
        description='Automatically requeue tasks stuck in CLAIMED (safe - user code never ran)',
    )
    claimed_stale_threshold_ms: Annotated[int, Field(ge=1_000, le=3_600_000)] = Field(
        default=120_000,  # 2 minutes
        description='Milliseconds without claimer heartbeat before CLAIMED task is considered stale (1s-1hr)',
    )

    auto_fail_stale_running: bool = Field(
        default=True,
        description='Automatically mark stale RUNNING tasks as FAILED (not safe to requeue)',
    )
    running_stale_threshold_ms: Annotated[int, Field(ge=1_000, le=7_200_000)] = Field(
        default=300_000,  # 5 minutes
        description='Milliseconds without runner heartbeat before RUNNING task is considered stale (1s-2hr)',
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
