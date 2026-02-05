# horsies/core/models/resilience.py
from __future__ import annotations

from typing import Annotated, Self
from pydantic import BaseModel, Field, model_validator
from horsies.core.errors import ConfigurationError, ErrorCode, ValidationReport, raise_collected


class WorkerResilienceConfig(BaseModel):
    """
    Configuration for worker resilience behaviors.

    Controls retry/backoff for transient database failures and the fallback
    polling interval when NOTIFY is silent.
    """

    db_retry_initial_ms: Annotated[int, Field(ge=100, le=60_000)] = Field(
        default=500,
        description='Initial backoff for retrying transient DB errors (100ms-60s)',
    )
    db_retry_max_ms: Annotated[int, Field(ge=500, le=300_000)] = Field(
        default=30_000,
        description='Maximum backoff for retrying transient DB errors (500ms-5min)',
    )
    db_retry_max_attempts: Annotated[int, Field(ge=0, le=10_000)] = Field(
        default=0,
        description=(
            'Maximum retry attempts for transient DB errors; 0 means infinite'
        ),
    )
    notify_poll_interval_ms: Annotated[int, Field(ge=1_000, le=300_000)] = Field(
        default=5_000,
        description='Fallback poll interval when NOTIFY is silent (1s-5min)',
    )

    @model_validator(mode='after')
    def validate_backoff(self) -> Self:
        report = ValidationReport('resilience')
        if self.db_retry_max_ms < self.db_retry_initial_ms:
            report.add(
                ConfigurationError(
                    message='db_retry_max_ms must be >= db_retry_initial_ms',
                    code=ErrorCode.CONFIG_INVALID_RESILIENCE,
                    notes=[
                        f'db_retry_initial_ms={self.db_retry_initial_ms}ms',
                        f'db_retry_max_ms={self.db_retry_max_ms}ms',
                    ],
                    help_text='increase db_retry_max_ms or reduce db_retry_initial_ms',
                )
            )

        raise_collected(report)
        return self
