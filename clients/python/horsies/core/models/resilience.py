# clients/python/horsies/core/models/resilience.py
from __future__ import annotations

from typing import Annotated, ClassVar, Self
from pydantic import BaseModel, ConfigDict, Field, model_validator
from horsies.core.errors import ConfigurationError, HorsiesErrorCode, ValidationReport, raise_collected


class WorkerResilienceConfig(BaseModel):
    """
    Configuration for worker resilience behaviors.

    Controls retry/backoff for transient database failures and the fallback
    polling interval when NOTIFY is silent.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    database_retry_initial_ms: Annotated[int, Field(ge=100, le=60_000)] = Field(
        default=500,
        description='Initial backoff for retrying transient DB errors (100ms-60s)',
    )
    database_retry_max_ms: Annotated[int, Field(ge=500, le=300_000)] = Field(
        default=30_000,
        description='Maximum backoff for retrying transient DB errors (500ms-5min)',
    )
    database_retry_max_attempts: Annotated[int, Field(ge=0, le=10_000)] = Field(
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
        if self.database_retry_max_ms < self.database_retry_initial_ms:
            report.add(
                ConfigurationError(
                    message='database_retry_max_ms must be >= database_retry_initial_ms',
                    code=HorsiesErrorCode.CONFIG_INVALID_RESILIENCE,
                    notes=[
                        f'database_retry_initial_ms={self.database_retry_initial_ms}ms',
                        f'database_retry_max_ms={self.database_retry_max_ms}ms',
                    ],
                    help_text='increase database_retry_max_ms or reduce database_retry_initial_ms',
                )
            )

        raise_collected(report)
        return self
