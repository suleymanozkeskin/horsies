"""Unit tests for AppConfig validation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    MultipleValidationErrors,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.schedule import (
    DailySchedule,
    HourlySchedule,
    IntervalSchedule,
    MonthlySchedule,
    ScheduleConfig,
    TaskSchedule,
    Weekday,
    WeeklySchedule,
)
from horsies.core.utils.url import mask_database_url

from datetime import time as datetime_time


# Shared broker config for all tests
BROKER = PostgresConfig(
    database_url='postgresql+psycopg://user:pass@localhost/db',
    pool_size=5,
    max_overflow=5,
)

# Recovery config with short heartbeat for tests using small claim_lease_ms values.
# Satisfies the lease >= 2x heartbeat constraint (min_lease = 2000ms).
SHORT_HB_RECOVERY = RecoveryConfig(claimer_heartbeat_interval_ms=1_000)


@pytest.mark.unit
class TestPrefetchBufferValidation:
    """Tests for prefetch_buffer configuration validation."""

    def test_prefetch_buffer_zero_is_valid(self) -> None:
        """prefetch_buffer=0 (hard cap mode) should be valid."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=0,
        )
        assert config.prefetch_buffer == 0

    def test_prefetch_buffer_negative_raises(self) -> None:
        """prefetch_buffer < 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='prefetch_buffer must be non-negative'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=-1,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_prefetch_buffer_positive_requires_claim_lease(self) -> None:
        """prefetch_buffer > 0 without claim_lease_ms should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='claim_lease_ms required'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=4,
                claim_lease_ms=None,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_prefetch_buffer_positive_with_claim_lease_is_valid(self) -> None:
        """prefetch_buffer > 0 with valid claim_lease_ms should be valid."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=4,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.prefetch_buffer == 4
        assert config.claim_lease_ms == 5000


@pytest.mark.unit
class TestClaimLeaseMsValidation:
    """Tests for claim_lease_ms configuration validation."""

    def test_claim_lease_ms_none_is_valid_with_no_prefetch(self) -> None:
        """claim_lease_ms=None is valid when prefetch_buffer=0."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=0,
            claim_lease_ms=None,
        )
        assert config.claim_lease_ms is None

    def test_claim_lease_ms_with_no_prefetch_accepted(self) -> None:
        """claim_lease_ms is allowed in hard-cap mode (overrides default 60s lease)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=0,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.claim_lease_ms == 5000

    def test_claim_lease_ms_zero_raises(self) -> None:
        """claim_lease_ms=0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='claim_lease_ms must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=4,
                claim_lease_ms=0,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_claim_lease_ms_negative_raises(self) -> None:
        """claim_lease_ms < 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='claim_lease_ms must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=4,
                claim_lease_ms=-1000,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_claim_lease_ms_positive_is_valid(self) -> None:
        """claim_lease_ms > 0 should be valid."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            prefetch_buffer=4,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.claim_lease_ms == 5000


@pytest.mark.unit
class TestMaxClaimRenewAgeMsValidation:
    """Tests for max_claim_renew_age_ms configuration validation."""

    def test_default_value_is_valid(self) -> None:
        """Default max_claim_renew_age_ms (180_000) should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.max_claim_renew_age_ms == 180_000

    def test_zero_raises(self) -> None:
        """max_claim_renew_age_ms=0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='max_claim_renew_age_ms must be positive',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                max_claim_renew_age_ms=0,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_negative_raises(self) -> None:
        """max_claim_renew_age_ms < 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='max_claim_renew_age_ms must be positive',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                max_claim_renew_age_ms=-1000,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_less_than_effective_lease_raises(self) -> None:
        """max_claim_renew_age_ms < effective lease should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='max_claim_renew_age_ms must be >= effective claim lease',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                claim_lease_ms=10_000,
                max_claim_renew_age_ms=5_000,
                recovery=RecoveryConfig(claimer_heartbeat_interval_ms=2_000),
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_equal_to_effective_lease_is_valid(self) -> None:
        """max_claim_renew_age_ms == effective lease is the minimum valid value."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            claim_lease_ms=5_000,
            max_claim_renew_age_ms=5_000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.max_claim_renew_age_ms == 5_000

    def test_greater_than_effective_lease_is_valid(self) -> None:
        """max_claim_renew_age_ms > effective lease should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            claim_lease_ms=5_000,
            max_claim_renew_age_ms=60_000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.max_claim_renew_age_ms == 60_000

    def test_uses_default_lease_when_claim_lease_ms_none(self) -> None:
        """When claim_lease_ms is None, effective lease is DEFAULT_CLAIM_LEASE_MS (60s).

        max_claim_renew_age_ms must be >= 60_000 in that case.
        """
        with pytest.raises(
            ConfigurationError, match='max_claim_renew_age_ms must be >= effective claim lease',
        ):
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                claim_lease_ms=None,
                max_claim_renew_age_ms=30_000,
            )


@pytest.mark.unit
class TestClusterWideCapPrefetchConflict:
    """Tests for cluster_wide_cap and prefetch_buffer conflict validation."""

    def test_cluster_wide_cap_with_no_prefetch_is_valid(self) -> None:
        """cluster_wide_cap with prefetch_buffer=0 should be valid (hard cap mode)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            cluster_wide_cap=50,
            prefetch_buffer=0,
        )
        assert config.cluster_wide_cap == 50
        assert config.prefetch_buffer == 0

    def test_cluster_wide_cap_with_prefetch_raises(self) -> None:
        """cluster_wide_cap with prefetch_buffer > 0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap incompatible with prefetch mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=50,
                prefetch_buffer=4,
                claim_lease_ms=5000,
                recovery=SHORT_HB_RECOVERY,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_no_cluster_cap_with_prefetch_is_valid(self) -> None:
        """No cluster_wide_cap with prefetch_buffer > 0 should be valid (soft cap mode)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            cluster_wide_cap=None,
            prefetch_buffer=4,
            claim_lease_ms=5000,
            recovery=SHORT_HB_RECOVERY,
        )
        assert config.cluster_wide_cap is None
        assert config.prefetch_buffer == 4


@pytest.mark.unit
class TestClusterWideCapBoundaries:
    """Tests for cluster_wide_cap boundary values."""

    def test_cluster_wide_cap_zero_raises(self) -> None:
        """cluster_wide_cap=0 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=0,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_cluster_wide_cap_negative_raises(self) -> None:
        """cluster_wide_cap=-1 should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_cluster_wide_cap_minimum_valid(self) -> None:
        """cluster_wide_cap=1 should be the minimum valid value."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            cluster_wide_cap=1,
        )
        assert config.cluster_wide_cap == 1


@pytest.mark.unit
class TestQueueModeValidation:
    """Tests for queue_mode and custom_queues validation."""

    def test_default_mode_with_custom_queues_raises(self) -> None:
        """DEFAULT mode with custom_queues provided should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='custom_queues must be None in DEFAULT mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                custom_queues=[CustomQueueConfig(name='q1')],
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_custom_mode_with_none_queues_raises(self) -> None:
        """CUSTOM mode with custom_queues=None should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='custom_queues required in CUSTOM mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.CUSTOM,
                broker=BROKER,
                custom_queues=None,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_custom_mode_with_empty_queues_raises(self) -> None:
        """CUSTOM mode with empty custom_queues should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='custom_queues required in CUSTOM mode'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.CUSTOM,
                broker=BROKER,
                custom_queues=[],
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_custom_mode_with_duplicate_names_raises(self) -> None:
        """CUSTOM mode with duplicate queue names should raise ConfigurationError."""
        with pytest.raises(
            ConfigurationError, match='duplicate queue names in custom_queues'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.CUSTOM,
                broker=BROKER,
                custom_queues=[
                    CustomQueueConfig(name='q1'),
                    CustomQueueConfig(name='q1'),
                ],
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE

    def test_custom_mode_with_valid_queues(self) -> None:
        """CUSTOM mode with valid unique queues should succeed."""
        config = AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=BROKER,
            custom_queues=[
                CustomQueueConfig(name='high'),
                CustomQueueConfig(name='low'),
            ],
        )
        assert config.custom_queues is not None
        assert len(config.custom_queues) == 2

    def test_default_mode_without_custom_queues(self) -> None:
        """DEFAULT mode without custom_queues should succeed."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.queue_mode == QueueMode.DEFAULT
        assert config.custom_queues is None

    def test_custom_mode_queue_names_preserved(self) -> None:
        """Queue names should be preserved exactly as provided."""
        config = AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=BROKER,
            custom_queues=[
                CustomQueueConfig(name='priority'),
                CustomQueueConfig(name='background'),
            ],
        )
        assert config.custom_queues is not None
        names = [q.name for q in config.custom_queues]
        assert names == ['priority', 'background']


@pytest.mark.unit
class TestDefaultBehavior:
    """Tests for default configuration behavior."""

    def test_defaults_to_hard_cap_mode(self) -> None:
        """Default config should use hard cap mode (prefetch_buffer=0)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.prefetch_buffer == 0
        assert config.claim_lease_ms is None

    def test_cluster_wide_cap_defaults_to_none(self) -> None:
        """cluster_wide_cap should default to None (unlimited)."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        assert config.cluster_wide_cap is None

    def test_queue_mode_defaults_to_default(self) -> None:
        """queue_mode should default to DEFAULT."""
        config = AppConfig(broker=BROKER)
        assert config.queue_mode == QueueMode.DEFAULT

    def test_custom_queues_defaults_to_none(self) -> None:
        """custom_queues should default to None."""
        config = AppConfig(broker=BROKER)
        assert config.custom_queues is None

    def test_default_unhandled_error_code_default(self) -> None:
        """default_unhandled_error_code should default to 'UNHANDLED_EXCEPTION'."""
        config = AppConfig(broker=BROKER)
        assert config.default_unhandled_error_code == 'UNHANDLED_EXCEPTION'


@pytest.mark.unit
class TestMultiErrorCollection:
    """Tests for phase-gated error collection in AppConfig."""

    def test_multiple_independent_errors_collected(self) -> None:
        """Multiple independent config errors are collected together."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
            )
        errors = exc_info.value.report.errors
        assert len(errors) == 2
        messages = [e.message for e in errors]
        assert 'cluster_wide_cap must be positive' in messages
        assert 'prefetch_buffer must be non-negative' in messages

    def test_single_error_still_raises_original_type(self) -> None:
        """Single config error still raises ConfigurationError (backward compat)."""
        with pytest.raises(
            ConfigurationError, match='cluster_wide_cap must be positive'
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_CLUSTER_CAP

    def test_multi_error_is_horsies_error(self) -> None:
        """MultipleValidationErrors is catchable as HorsiesError."""
        with pytest.raises(HorsiesError):
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
            )

    def test_three_independent_errors_collected(self) -> None:
        """Three independent config errors are collected together."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
                default_unhandled_error_code='TimeoutError',
            )
        errors = exc_info.value.report.errors
        assert len(errors) == 3

    def test_multi_error_individual_codes(self) -> None:
        """Each error in a multi-error report has the correct error code."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                cluster_wide_cap=-1,
                prefetch_buffer=-5,
                default_unhandled_error_code='TimeoutError',
            )
        errors = exc_info.value.report.errors
        codes = {e.code for e in errors}
        assert ErrorCode.CONFIG_INVALID_CLUSTER_CAP in codes
        assert ErrorCode.CONFIG_INVALID_PREFETCH in codes
        assert ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER in codes


@pytest.mark.unit
class TestRetryCodeValidation:
    """Validation for retry/error-code safety settings."""

    def test_invalid_default_unhandled_error_code_raises(self) -> None:
        """Exception-like names are rejected as error codes."""
        with pytest.raises(
            ConfigurationError,
            match='default_unhandled_error_code',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                default_unhandled_error_code='TimeoutError',
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_invalid_mapper_value_format_raises(self) -> None:
        """Mapper values that look like exception names are rejected."""
        with pytest.raises(
            ConfigurationError,
            match='Mapper value',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                exception_mapper={ValueError: 'TimeoutError'},
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_valid_default_unhandled_error_code(self) -> None:
        """UPPER_SNAKE_CASE error codes should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            default_unhandled_error_code='CUSTOM_CODE',
        )
        assert config.default_unhandled_error_code == 'CUSTOM_CODE'

    def test_lowercase_error_code_raises(self) -> None:
        """Lowercase error codes should be rejected."""
        with pytest.raises(
            ConfigurationError,
            match='default_unhandled_error_code',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                default_unhandled_error_code='lowercase_code',
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_non_base_exception_mapper_key_raises(self) -> None:
        """Mapper keys that are not BaseException subclasses should be rejected.

        Pydantic validates the dict type annotation before the model_validator runs,
        so this raises pydantic ValidationError, not ConfigurationError.
        """
        with pytest.raises(ValidationError, match='is_subclass_of'):
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                exception_mapper={str: 'SOME_CODE'},  # type: ignore[dict-item]
            )

    def test_valid_exception_mapper_accepted(self) -> None:
        """Valid exception mapper entries should be accepted."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
            exception_mapper={ValueError: 'VALUE_ERROR'},
        )
        assert ValueError in config.exception_mapper
        assert config.exception_mapper[ValueError] == 'VALUE_ERROR'


@pytest.mark.unit
class TestAppConfigImmutability:
    """Tests for frozen model behavior."""

    def test_frozen_model_rejects_mutation(self) -> None:
        """AppConfig is frozen and should reject attribute assignment."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        with pytest.raises(ValidationError):
            config.prefetch_buffer = 1  # type: ignore[misc]


@pytest.mark.unit
class TestLogConfig:
    """Tests for log_config, _format_for_logging, and mask_database_url."""

    def test_mask_database_url_hides_password(self) -> None:
        """Password in database URL should be masked."""
        url = 'postgresql+psycopg://user:secret@localhost/db'
        masked = mask_database_url(url)
        assert 'secret' not in masked
        assert '***' in masked
        assert 'user' in masked
        assert 'localhost' in masked

    def test_mask_database_url_no_password(self) -> None:
        """URL without password should remain unchanged."""
        url = 'postgresql+psycopg://localhost/db'
        masked = mask_database_url(url)
        assert masked == url

    def test_mask_database_url_malformed_fallback(self) -> None:
        """Malformed URL with @ should use fallback masking."""
        # Force the except branch by providing something that urlparse
        # handles but has no parsed password, while still containing @
        url = 'not-a-real-scheme://user:secret@host/db'
        masked = mask_database_url(url)
        assert 'secret' not in masked

    def test_format_for_logging_contains_queue_mode(self) -> None:
        """Formatted output should contain queue_mode."""
        config = AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=BROKER,
        )
        formatted = config._format_for_logging()
        assert 'queue_mode:' in formatted
        assert 'DEFAULT' in formatted

    def test_format_for_logging_custom_queues_listed(self) -> None:
        """CUSTOM mode should list queue names in formatted output."""
        config = AppConfig(
            queue_mode=QueueMode.CUSTOM,
            broker=BROKER,
            custom_queues=[
                CustomQueueConfig(name='fast', priority=1, max_concurrency=10),
                CustomQueueConfig(name='slow', priority=2, max_concurrency=3),
            ],
        )
        formatted = config._format_for_logging()
        assert 'fast' in formatted
        assert 'slow' in formatted
        assert 'custom_queues:' in formatted

    def test_log_config_skips_in_child_process(self) -> None:
        """log_config should not log when HORSIES_CHILD_PROCESS=1."""
        config = AppConfig(broker=BROKER)
        logger = MagicMock()
        with patch.dict('os.environ', {'HORSIES_CHILD_PROCESS': '1'}):
            config.log_config(logger=logger)
        logger.info.assert_not_called()

    def test_log_config_uses_provided_logger(self) -> None:
        """log_config should call logger.info when provided."""
        config = AppConfig(broker=BROKER)
        logger = MagicMock()
        with patch.dict('os.environ', {}, clear=False):
            # Ensure HORSIES_CHILD_PROCESS is not set
            env = dict(**{k: v for k, v in __import__('os').environ.items() if k != 'HORSIES_CHILD_PROCESS'})
            with patch.dict('os.environ', env, clear=True):
                config.log_config(logger=logger)
        logger.info.assert_called_once()

    def test_log_config_none_logger_uses_root(self) -> None:
        """log_config with no logger should use root logger."""
        config = AppConfig(broker=BROKER)
        with patch.dict('os.environ', {}, clear=False):
            env = {k: v for k, v in __import__('os').environ.items() if k != 'HORSIES_CHILD_PROCESS'}
            with patch.dict('os.environ', env, clear=True):
                with patch('logging.getLogger') as mock_get_logger:
                    mock_logger = MagicMock()
                    mock_get_logger.return_value = mock_logger
                    config.log_config(logger=None)
                    mock_get_logger.assert_called_once_with()
                    mock_logger.info.assert_called_once()


@pytest.mark.unit
class TestFormatForLoggingExtended:
    """Tests for _format_for_logging covering cluster_wide_cap, claim_lease_ms, exception_mapper, schedule."""

    def test_cluster_wide_cap_displayed(self) -> None:
        """cluster_wide_cap should appear in formatted output when set."""
        config = AppConfig(broker=BROKER, cluster_wide_cap=50)
        formatted = config._format_for_logging()
        assert 'cluster_wide_cap: 50' in formatted

    def test_cluster_wide_cap_omitted_when_none(self) -> None:
        """cluster_wide_cap should not appear when None."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'cluster_wide_cap' not in formatted

    def test_claim_lease_ms_displayed(self) -> None:
        """claim_lease_ms should appear in formatted output when set."""
        config = AppConfig(
            broker=BROKER,
            prefetch_buffer=4,
            claim_lease_ms=30_000,
            recovery=RecoveryConfig(claimer_heartbeat_interval_ms=10_000),
        )
        formatted = config._format_for_logging()
        assert 'claim_lease_ms: 30000ms' in formatted

    def test_claim_lease_ms_omitted_when_none(self) -> None:
        """claim_lease_ms should not appear when None."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'claim_lease_ms' not in formatted

    def test_exception_mapper_displayed(self) -> None:
        """Non-empty exception mapper should show mapping count."""
        config = AppConfig(
            broker=BROKER,
            exception_mapper={ValueError: 'VALUE_ERROR', TypeError: 'TYPE_ERROR'},
        )
        formatted = config._format_for_logging()
        assert 'exception_mapper: 2 mapping(s)' in formatted

    def test_custom_default_error_code_displayed(self) -> None:
        """Non-default unhandled error code should appear in output."""
        config = AppConfig(
            broker=BROKER,
            default_unhandled_error_code='MY_ERROR',
        )
        formatted = config._format_for_logging()
        assert 'default_unhandled_error_code: MY_ERROR' in formatted

    def test_default_error_code_omitted_when_default(self) -> None:
        """Default unhandled error code should not appear in output."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'default_unhandled_error_code' not in formatted

    def test_retention_hours_displayed(self) -> None:
        """Retention hours should appear in formatted output."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'retention_hours:' in formatted
        assert 'heartbeats=24' in formatted
        assert 'worker_states=168' in formatted
        assert 'terminal_records=720' in formatted

    def test_schedule_displayed(self) -> None:
        """Schedule config should appear when set."""
        config = AppConfig(
            broker=BROKER,
            schedule=ScheduleConfig(
                enabled=True,
                schedules=[
                    TaskSchedule(
                        name='daily_cleanup',
                        task_name='cleanup',
                        pattern=DailySchedule(time=datetime_time(3, 0, 0)),
                    ),
                ],
                check_interval_seconds=5,
            ),
        )
        formatted = config._format_for_logging()
        assert 'schedule:' in formatted
        assert 'enabled: True' in formatted
        assert '1 schedule(s)' in formatted
        assert 'daily_cleanup' in formatted
        assert 'cleanup' in formatted
        assert 'check_interval: 5s' in formatted

    def test_schedule_omitted_when_none(self) -> None:
        """Schedule section should not appear when schedule is None."""
        config = AppConfig(broker=BROKER)
        formatted = config._format_for_logging()
        assert 'schedule:' not in formatted


@pytest.mark.unit
class TestFormatSchedulePattern:
    """Tests for AppConfig._format_schedule_pattern covering all match cases."""

    def test_interval_all_components(self) -> None:
        """Interval with days, hours, minutes, seconds should format all parts."""
        pattern = IntervalSchedule(days=1, hours=2, minutes=30, seconds=15)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'every 1d 2h 30m 15s'

    def test_interval_hours_only(self) -> None:
        """Interval with only hours should format just hours."""
        pattern = IntervalSchedule(hours=4)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'every 4h'

    def test_interval_minutes_and_seconds(self) -> None:
        """Interval with minutes and seconds only."""
        pattern = IntervalSchedule(minutes=10, seconds=30)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'every 10m 30s'

    def test_hourly(self) -> None:
        """Hourly pattern should format with zero-padded minute:second."""
        pattern = HourlySchedule(minute=5, second=30)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'hourly at :05:30'

    def test_hourly_defaults(self) -> None:
        """Hourly with default second should zero-pad correctly."""
        pattern = HourlySchedule(minute=15)
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'hourly at :15:00'

    def test_daily(self) -> None:
        """Daily pattern should format time as HH:MM:SS."""
        pattern = DailySchedule(time=datetime_time(14, 30, 0))
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'daily at 14:30:00'

    def test_weekly(self) -> None:
        """Weekly pattern should list day names and time."""
        pattern = WeeklySchedule(
            days=[Weekday.MONDAY, Weekday.FRIDAY],
            time=datetime_time(9, 0, 0),
        )
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'weekly on monday, friday at 09:00:00'

    def test_monthly(self) -> None:
        """Monthly pattern should show day number and time."""
        pattern = MonthlySchedule(day=15, time=datetime_time(3, 0, 0))
        result = AppConfig._format_schedule_pattern(pattern)
        assert result == 'monthly on day 15 at 03:00:00'
