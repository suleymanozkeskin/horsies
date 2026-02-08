"""Unit tests for WorkerResilienceConfig validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.errors import ConfigurationError, ErrorCode


@pytest.mark.unit
class TestResilienceConfigDefaults:
    """Tests for default values."""

    def test_defaults(self) -> None:
        cfg = WorkerResilienceConfig()
        assert cfg.db_retry_initial_ms == 500
        assert cfg.db_retry_max_ms == 30_000
        assert cfg.db_retry_max_attempts == 0
        assert cfg.notify_poll_interval_ms == 5_000


@pytest.mark.unit
class TestResilienceConfigBoundaries:
    """Tests for field boundary constraints."""

    # --- db_retry_initial_ms [100, 60_000] ---

    def test_initial_ms_below_minimum_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_initial_ms"):
            WorkerResilienceConfig(db_retry_initial_ms=99)

    def test_initial_ms_at_minimum(self) -> None:
        cfg = WorkerResilienceConfig(db_retry_initial_ms=100)
        assert cfg.db_retry_initial_ms == 100

    def test_initial_ms_above_maximum_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_initial_ms"):
            WorkerResilienceConfig(db_retry_initial_ms=60_001)

    def test_initial_ms_at_maximum(self) -> None:
        cfg = WorkerResilienceConfig(db_retry_initial_ms=60_000, db_retry_max_ms=60_000)
        assert cfg.db_retry_initial_ms == 60_000

    # --- db_retry_max_ms [500, 300_000] ---

    def test_max_ms_below_minimum_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_max_ms"):
            WorkerResilienceConfig(db_retry_max_ms=499)

    def test_max_ms_at_minimum(self) -> None:
        cfg = WorkerResilienceConfig(db_retry_max_ms=500)
        assert cfg.db_retry_max_ms == 500

    def test_max_ms_at_maximum(self) -> None:
        cfg = WorkerResilienceConfig(db_retry_max_ms=300_000)
        assert cfg.db_retry_max_ms == 300_000

    def test_max_ms_above_maximum_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_max_ms"):
            WorkerResilienceConfig(db_retry_max_ms=300_001)

    # --- db_retry_max_attempts [0, 10_000] ---

    def test_max_attempts_zero_means_infinite(self) -> None:
        cfg = WorkerResilienceConfig(db_retry_max_attempts=0)
        assert cfg.db_retry_max_attempts == 0

    def test_max_attempts_negative_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_max_attempts"):
            WorkerResilienceConfig(db_retry_max_attempts=-1)

    def test_max_attempts_at_maximum(self) -> None:
        cfg = WorkerResilienceConfig(db_retry_max_attempts=10_000)
        assert cfg.db_retry_max_attempts == 10_000

    def test_max_attempts_above_maximum_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_max_attempts"):
            WorkerResilienceConfig(db_retry_max_attempts=10_001)

    # --- notify_poll_interval_ms [1_000, 300_000] ---

    def test_notify_poll_below_minimum_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"notify_poll_interval_ms"):
            WorkerResilienceConfig(notify_poll_interval_ms=999)

    def test_notify_poll_at_minimum(self) -> None:
        cfg = WorkerResilienceConfig(notify_poll_interval_ms=1_000)
        assert cfg.notify_poll_interval_ms == 1_000

    def test_notify_poll_at_maximum(self) -> None:
        cfg = WorkerResilienceConfig(notify_poll_interval_ms=300_000)
        assert cfg.notify_poll_interval_ms == 300_000

    def test_notify_poll_above_maximum_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"notify_poll_interval_ms"):
            WorkerResilienceConfig(notify_poll_interval_ms=300_001)


@pytest.mark.unit
class TestResilienceConfigCrossFieldValidation:
    """Tests for the model_validator ensuring max_ms >= initial_ms."""

    def test_max_ms_less_than_initial_ms_raises(self) -> None:
        with pytest.raises(
            ConfigurationError,
            match='db_retry_max_ms must be >= db_retry_initial_ms',
        ) as exc_info:
            WorkerResilienceConfig(
                db_retry_initial_ms=5_000,
                db_retry_max_ms=500,
            )
        error = exc_info.value
        assert error.code == ErrorCode.CONFIG_INVALID_RESILIENCE
        assert error.notes == [
            'db_retry_initial_ms=5000ms',
            'db_retry_max_ms=500ms',
        ]
        assert error.help_text == 'increase db_retry_max_ms or reduce db_retry_initial_ms'

    def test_max_ms_equal_to_initial_ms_is_valid(self) -> None:
        cfg = WorkerResilienceConfig(
            db_retry_initial_ms=500,
            db_retry_max_ms=500,
        )
        assert cfg.db_retry_max_ms == cfg.db_retry_initial_ms

    def test_max_ms_greater_than_initial_ms_is_valid(self) -> None:
        cfg = WorkerResilienceConfig(
            db_retry_initial_ms=500,
            db_retry_max_ms=10_000,
        )
        assert cfg.db_retry_max_ms > cfg.db_retry_initial_ms


@pytest.mark.unit
class TestResilienceConfigTypeCoercion:
    """Tests for type handling when config is loaded from untyped sources."""

    def test_none_for_required_field_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_initial_ms"):
            WorkerResilienceConfig(db_retry_initial_ms=None)  # type: ignore[arg-type]

    def test_non_numeric_string_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_initial_ms"):
            WorkerResilienceConfig(db_retry_initial_ms="fast")  # type: ignore[arg-type]

    def test_whole_float_coerced_to_int(self) -> None:
        cfg = WorkerResilienceConfig(db_retry_initial_ms=200.0)  # type: ignore[arg-type]
        assert cfg.db_retry_initial_ms == 200
        assert isinstance(cfg.db_retry_initial_ms, int)

    def test_fractional_float_raises(self) -> None:
        with pytest.raises(ValidationError, match=r"db_retry_initial_ms"):
            WorkerResilienceConfig(db_retry_initial_ms=200.5)  # type: ignore[arg-type]


@pytest.mark.unit
class TestResilienceConfigMultipleViolations:
    """Tests for multiple simultaneous constraint violations."""

    def test_multiple_field_violations_collected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            WorkerResilienceConfig(
                db_retry_initial_ms=99,
                db_retry_max_ms=499,
            )
        error_fields = {e['loc'][0] for e in exc_info.value.errors()}
        assert 'db_retry_initial_ms' in error_fields
        assert 'db_retry_max_ms' in error_fields

    def test_field_violation_prevents_cross_field_validator(self) -> None:
        """Field-level ge/le failures prevent model_validator(mode='after') from running.

        If the model_validator ran, it would raise ConfigurationError (not
        ValidationError), causing this test to fail.
        """
        with pytest.raises(ValidationError) as exc_info:
            WorkerResilienceConfig(
                db_retry_initial_ms=60_001,
                db_retry_max_ms=500,
            )
        error_fields = {e['loc'][0] for e in exc_info.value.errors()}
        assert 'db_retry_initial_ms' in error_fields


@pytest.mark.unit
class TestResilienceConfigSerialization:
    """Tests for dict round-trip via model_dump / model_validate."""

    def test_round_trip_with_custom_values(self) -> None:
        original = WorkerResilienceConfig(
            db_retry_initial_ms=200,
            db_retry_max_ms=10_000,
            db_retry_max_attempts=5,
            notify_poll_interval_ms=2_000,
        )
        data = original.model_dump()
        restored = WorkerResilienceConfig.model_validate(data)
        assert restored == original

    def test_round_trip_with_defaults(self) -> None:
        original = WorkerResilienceConfig()
        data = original.model_dump()
        restored = WorkerResilienceConfig.model_validate(data)
        assert restored == original

    def test_model_dump_contains_all_fields(self) -> None:
        data = WorkerResilienceConfig().model_dump()
        assert set(data.keys()) == {
            'db_retry_initial_ms',
            'db_retry_max_ms',
            'db_retry_max_attempts',
            'notify_poll_interval_ms',
        }


@pytest.mark.unit
class TestResilienceConfigModelBehavior:
    """Tests for model-level configuration behavior."""

    def test_fields_are_mutable_by_default(self) -> None:
        cfg = WorkerResilienceConfig()
        cfg.db_retry_initial_ms = 200
        assert cfg.db_retry_initial_ms == 200
