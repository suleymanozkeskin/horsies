# app/core/models/app.py
from typing import List, Optional
from pydantic import BaseModel, model_validator, Field, ConfigDict
from horsies.core.models.queues import QueueMode, CustomQueueConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.models.schedule import ScheduleConfig, SchedulePattern
from horsies.core.exception_mapper import (
    ExceptionMapper,
    validate_exception_mapper,
    validate_error_code_string,
)
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
)
from horsies.core.utils.url import mask_database_url
import logging
import os


class AppConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    queue_mode: QueueMode = QueueMode.DEFAULT
    custom_queues: Optional[List[CustomQueueConfig]] = None
    broker: PostgresConfig
    # Cluster-wide cap on concurrently RUNNING tasks across all queues. None = unlimited.
    cluster_wide_cap: Optional[int] = None
    # Prefetch buffer: 0 = hard cap mode (count RUNNING + CLAIMED), >0 = soft cap with lease
    prefetch_buffer: int = 0
    # Claim lease duration in ms. Required when prefetch_buffer > 0.
    # Prefetched claims expire after this duration and can be reclaimed by other workers.
    claim_lease_ms: Optional[int] = None
    recovery: RecoveryConfig = Field(default_factory=RecoveryConfig)
    resilience: WorkerResilienceConfig = Field(
        default_factory=WorkerResilienceConfig
    )
    schedule: Optional[ScheduleConfig] = Field(
        default=None, description='Scheduler configuration'
    )
    exception_mapper: ExceptionMapper = Field(
        default_factory=lambda: ExceptionMapper(),
    )
    default_unhandled_error_code: str = 'UNHANDLED_EXCEPTION'

    @model_validator(mode='after')
    def validate_queue_configuration(self):
        """Validate queue configuration based on queue mode.

        Collects all independent errors and raises them together.
        """
        report = ValidationReport('config')

        if self.queue_mode == QueueMode.DEFAULT:
            if self.custom_queues is not None:
                report.add(
                    ConfigurationError(
                        message='custom_queues must be None in DEFAULT mode',
                        code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
                        notes=['queue_mode=DEFAULT but custom_queues was provided'],
                        help_text='either remove custom_queues or set queue_mode=CUSTOM',
                    )
                )
        elif self.queue_mode == QueueMode.CUSTOM:
            if self.custom_queues is None or len(self.custom_queues) == 0:
                report.add(
                    ConfigurationError(
                        message='custom_queues required in CUSTOM mode',
                        code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
                        notes=['queue_mode=CUSTOM but custom_queues is empty or None'],
                        help_text='provide at least one CustomQueueConfig in custom_queues',
                    )
                )
            else:
                # Validate unique queue names (only if queues exist)
                queue_names = [q.name for q in self.custom_queues]
                if len(queue_names) != len(set(queue_names)):
                    report.add(
                        ConfigurationError(
                            message='duplicate queue names in custom_queues',
                            code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
                            notes=[f'queue names: {queue_names}'],
                            help_text='each queue name must be unique',
                        )
                    )

        # Validate cluster_wide_cap if provided
        if self.cluster_wide_cap is not None and self.cluster_wide_cap <= 0:
            report.add(
                ConfigurationError(
                    message='cluster_wide_cap must be positive',
                    code=ErrorCode.CONFIG_INVALID_CLUSTER_CAP,
                    notes=[f'got cluster_wide_cap={self.cluster_wide_cap}'],
                    help_text='use a positive integer or None for unlimited',
                )
            )

        # Validate prefetch_buffer
        if self.prefetch_buffer < 0:
            report.add(
                ConfigurationError(
                    message='prefetch_buffer must be non-negative',
                    code=ErrorCode.CONFIG_INVALID_PREFETCH,
                    notes=[f'got prefetch_buffer={self.prefetch_buffer}'],
                    help_text='use 0 for hard cap mode or positive integer for soft cap',
                )
            )

        # Validate claim_lease_ms
        if self.prefetch_buffer > 0 and self.claim_lease_ms is None:
            report.add(
                ConfigurationError(
                    message='claim_lease_ms required when prefetch_buffer > 0',
                    code=ErrorCode.CONFIG_INVALID_PREFETCH,
                    notes=[
                        f'prefetch_buffer={self.prefetch_buffer} but claim_lease_ms is None',
                    ],
                    help_text='set claim_lease_ms (e.g., 30000 for 30 seconds)',
                )
            )
        if self.claim_lease_ms is not None and self.claim_lease_ms <= 0:
            report.add(
                ConfigurationError(
                    message='claim_lease_ms must be positive',
                    code=ErrorCode.CONFIG_INVALID_PREFETCH,
                    notes=[f'got claim_lease_ms={self.claim_lease_ms}'],
                    help_text='use a positive integer in milliseconds',
                )
            )

        # Forbid claim_lease_ms in hard cap mode
        if self.prefetch_buffer == 0 and self.claim_lease_ms is not None:
            report.add(
                ConfigurationError(
                    message='claim_lease_ms incompatible with hard cap mode',
                    code=ErrorCode.CONFIG_INVALID_PREFETCH,
                    notes=[
                        'prefetch_buffer=0 (hard cap mode)',
                        f'but claim_lease_ms={self.claim_lease_ms} was set',
                    ],
                    help_text='remove claim_lease_ms or set prefetch_buffer > 0',
                )
            )

        # Validate prefetch_buffer vs cluster_wide_cap conflict
        if self.cluster_wide_cap is not None and self.prefetch_buffer > 0:
            report.add(
                ConfigurationError(
                    message='cluster_wide_cap incompatible with prefetch mode',
                    code=ErrorCode.CONFIG_INVALID_CLUSTER_CAP,
                    notes=[
                        f'cluster_wide_cap={self.cluster_wide_cap}',
                        f'prefetch_buffer={self.prefetch_buffer}',
                        'cluster_wide_cap requires hard cap mode (prefetch_buffer=0)',
                    ],
                    help_text='set prefetch_buffer=0 when using cluster_wide_cap',
                )
            )

        # Validate exception_mapper entries
        mapper_errors = validate_exception_mapper(
            self.exception_mapper,
        )
        for msg in mapper_errors:
            report.add(
                ConfigurationError(
                    message=msg,
                    code=ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                    notes=['check exception_mapper keys and values'],
                    help_text='keys must be BaseException subclasses, values must be UPPER_SNAKE_CASE error codes',
                )
            )

        default_code_error = validate_error_code_string(
            self.default_unhandled_error_code,
            field_name='default_unhandled_error_code',
        )
        if default_code_error is not None:
            report.add(
                ConfigurationError(
                    message=default_code_error,
                    code=ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                    notes=['invalid default_unhandled_error_code in AppConfig'],
                    help_text='use UPPER_SNAKE_CASE error codes',
                )
            )

        raise_collected(report)
        return self

    def log_config(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Log the AppConfig in a human-readable format.
        Masks sensitive data like database passwords.

        Args:
            logger: Logger instance to use. If None, uses root logger.
        """
        if os.getenv('HORSIES_CHILD_PROCESS') == '1':
            return
        if logger is None:
            logger = logging.getLogger()

        formatted = self._format_for_logging()
        logger.info('AppConfig:\n%s', formatted)

    def _format_for_logging(self) -> str:
        """Internal helper to format the AppConfig for human-readable logging."""
        lines: list[str] = []

        # Queue mode and queues
        lines.append(f'  queue_mode: {self.queue_mode.value.upper()}')
        if self.queue_mode == QueueMode.CUSTOM and self.custom_queues:
            lines.append('  custom_queues:')
            for queue in self.custom_queues:
                lines.append(
                    f'    - {queue.name} (priority={queue.priority}, max_concurrency={queue.max_concurrency})'
                )

        # Cluster-wide cap and prefetch settings
        if self.cluster_wide_cap is not None:
            lines.append(f'  cluster_wide_cap: {self.cluster_wide_cap}')
        lines.append(f'  prefetch_buffer: {self.prefetch_buffer}')
        if self.claim_lease_ms is not None:
            lines.append(f'  claim_lease_ms: {self.claim_lease_ms}ms')

        # Broker config with masked password
        masked_url = mask_database_url(self.broker.database_url)
        lines.append(f'  broker:')
        lines.append(f'    database_url: {masked_url}')
        lines.append(f'    pool_size: {self.broker.pool_size}')
        lines.append(f'    max_overflow: {self.broker.max_overflow}')

        # Recovery config
        lines.append('  recovery:')
        lines.append(
            f'    auto_requeue_stale_claimed: {self.recovery.auto_requeue_stale_claimed}'
        )
        lines.append(
            f'    claimed_stale_threshold: {self.recovery.claimed_stale_threshold_ms}ms'
        )
        lines.append(
            f'    auto_fail_stale_running: {self.recovery.auto_fail_stale_running}'
        )
        lines.append(
            f'    running_stale_threshold: {self.recovery.running_stale_threshold_ms}ms'
        )
        lines.append(f'    check_interval: {self.recovery.check_interval_ms}ms')
        lines.append(
            f'    heartbeat_intervals: runner={self.recovery.runner_heartbeat_interval_ms}ms, claimer={self.recovery.claimer_heartbeat_interval_ms}ms'
        )

        # Resilience config
        lines.append('  resilience:')
        lines.append(
            f'    db_retry_initial_ms: {self.resilience.db_retry_initial_ms}ms'
        )
        lines.append(f'    db_retry_max_ms: {self.resilience.db_retry_max_ms}ms')
        lines.append(
            f'    db_retry_max_attempts: {self.resilience.db_retry_max_attempts}'
        )
        lines.append(
            f'    notify_poll_interval_ms: {self.resilience.notify_poll_interval_ms}ms'
        )

        # Exception mapper
        if self.exception_mapper:
            lines.append(f'  exception_mapper: {len(self.exception_mapper)} mapping(s)')
        if self.default_unhandled_error_code != 'UNHANDLED_EXCEPTION':
            lines.append(f'  default_unhandled_error_code: {self.default_unhandled_error_code}')

        # Schedule config
        if self.schedule is not None:
            lines.append('  schedule:')
            lines.append(f'    enabled: {self.schedule.enabled}')
            lines.append(f'    schedules: {len(self.schedule.schedules)} schedule(s)')
            if self.schedule.schedules:
                for sched in self.schedule.schedules:
                    pattern_desc = self._format_schedule_pattern(sched.pattern)
                    lines.append(
                        f'      - {sched.name}: {sched.task_name} {pattern_desc}'
                    )
            lines.append(f'    check_interval: {self.schedule.check_interval_seconds}s')

        return '\n'.join(lines)

    @staticmethod
    def _format_schedule_pattern(pattern: SchedulePattern) -> str:
        """Format schedule pattern for concise logging."""
        match pattern.type:
            case 'interval':
                parts: list[str] = []
                if pattern.days:
                    parts.append(f'{pattern.days}d')
                if pattern.hours:
                    parts.append(f'{pattern.hours}h')
                if pattern.minutes:
                    parts.append(f'{pattern.minutes}m')
                if pattern.seconds:
                    parts.append(f'{pattern.seconds}s')
                return f"every {' '.join(parts)}"
            case 'hourly':
                return f'hourly at :{pattern.minute:02d}:{pattern.second:02d}'
            case 'daily':
                return f"daily at {pattern.time.strftime('%H:%M:%S')}"
            case 'weekly':
                days = ', '.join(d.value for d in pattern.days)
                return f"weekly on {days} at {pattern.time.strftime('%H:%M:%S')}"
            case 'monthly':
                return f"monthly on day {pattern.day} at {pattern.time.strftime('%H:%M:%S')}"
