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


# Shared broker config for all tests
BROKER = PostgresConfig(
    database_url='postgresql+psycopg://user:pass@localhost/db',
    pool_size=5,
    max_overflow=5,
)


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

    def test_claim_lease_ms_with_no_prefetch_raises(self) -> None:
        """claim_lease_ms must be None when prefetch_buffer=0."""
        with pytest.raises(
            ConfigurationError,
            match='claim_lease_ms incompatible with hard cap mode',
        ) as exc_info:
            AppConfig(
                queue_mode=QueueMode.DEFAULT,
                broker=BROKER,
                prefetch_buffer=0,
                claim_lease_ms=5000,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

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
        )
        assert config.claim_lease_ms == 5000


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
    """Tests for log_config, _format_for_logging, and _mask_database_url."""

    def test_mask_database_url_hides_password(self) -> None:
        """Password in database URL should be masked."""
        url = 'postgresql+psycopg://user:secret@localhost/db'
        masked = AppConfig._mask_database_url(url)
        assert 'secret' not in masked
        assert '***' in masked
        assert 'user' in masked
        assert 'localhost' in masked

    def test_mask_database_url_no_password(self) -> None:
        """URL without password should remain unchanged."""
        url = 'postgresql+psycopg://localhost/db'
        masked = AppConfig._mask_database_url(url)
        assert masked == url

    def test_mask_database_url_malformed_fallback(self) -> None:
        """Malformed URL with @ should use fallback masking."""
        # Force the except branch by providing something that urlparse
        # handles but has no parsed password, while still containing @
        url = 'not-a-real-scheme://user:secret@host/db'
        masked = AppConfig._mask_database_url(url)
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
