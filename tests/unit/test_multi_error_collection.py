"""Unit tests for phase-gated multi-error collection.

Tests the ValidationReport / MultipleValidationErrors / raise_collected
infrastructure and how it integrates with model validators.
Uses RecoveryConfig as the integration surface.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    MultipleValidationErrors,
)
from horsies.core.models.recovery import RecoveryConfig


pytestmark = pytest.mark.unit


class TestRecoveryConfigMultiError:
    """Tests for multi-error collection in RecoveryConfig."""

    # ── Happy paths ──────────────────────────────────────────────────────

    def test_valid_config_no_errors(self) -> None:
        """Explicitly valid thresholds should not raise."""
        config = RecoveryConfig(
            runner_heartbeat_interval_ms=30_000,
            running_stale_threshold_ms=120_000,
            claimer_heartbeat_interval_ms=30_000,
            claimed_stale_threshold_ms=120_000,
        )
        assert config.running_stale_threshold_ms == 120_000
        assert config.claimed_stale_threshold_ms == 120_000

    def test_default_values_are_internally_consistent(self) -> None:
        """All defaults must satisfy the 2x heartbeat rule."""
        config = RecoveryConfig()
        assert config.running_stale_threshold_ms >= config.runner_heartbeat_interval_ms * 2
        assert config.claimed_stale_threshold_ms >= config.claimer_heartbeat_interval_ms * 2

    # ── Multi-error collection (2 errors) ────────────────────────────────

    def test_both_thresholds_too_low_collected(self) -> None:
        """Both heartbeat thresholds too low should be reported together."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
                claimer_heartbeat_interval_ms=30_000,
                claimed_stale_threshold_ms=1_000,
            )
        errors = exc_info.value.report.errors
        assert len(errors) == 2
        messages = [e.message for e in errors]
        assert 'running_stale_threshold_ms too low' in messages
        assert 'claimed_stale_threshold_ms too low' in messages

    def test_multi_recovery_errors_are_horsies_error(self) -> None:
        """MultipleValidationErrors is catchable as HorsiesError."""
        with pytest.raises(HorsiesError):
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
                claimer_heartbeat_interval_ms=30_000,
                claimed_stale_threshold_ms=1_000,
            )

    def test_multi_error_format_contains_all_messages(self) -> None:
        """str() output includes both error messages and aborting summary."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
                claimer_heartbeat_interval_ms=30_000,
                claimed_stale_threshold_ms=1_000,
            )
        text = str(exc_info.value)
        assert 'running_stale_threshold_ms too low' in text
        assert 'claimed_stale_threshold_ms too low' in text
        assert 'aborting due to 2 previous errors' in text

    def test_report_phase_name(self) -> None:
        """Report's phase_name should be 'recovery'."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
                claimer_heartbeat_interval_ms=30_000,
                claimed_stale_threshold_ms=1_000,
            )
        assert exc_info.value.report.phase_name == 'recovery'

    # ── Single-error backward compatibility ──────────────────────────────

    def test_running_threshold_alone_preserves_type(self) -> None:
        """Single running threshold error preserves ConfigurationError type."""
        with pytest.raises(ConfigurationError, match='running_stale_threshold_ms too low'):
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
            )

    def test_claimed_threshold_alone_preserves_type(self) -> None:
        """Single claimed threshold error preserves ConfigurationError type."""
        with pytest.raises(ConfigurationError, match='claimed_stale_threshold_ms too low'):
            RecoveryConfig(
                claimer_heartbeat_interval_ms=30_000,
                claimed_stale_threshold_ms=1_000,
            )

    # ── Error codes and structured fields ────────────────────────────────

    def test_error_code_is_config_invalid_recovery(self) -> None:
        """Each collected error carries CONFIG_INVALID_RECOVERY code."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
                claimer_heartbeat_interval_ms=30_000,
                claimed_stale_threshold_ms=1_000,
            )
        for error in exc_info.value.report.errors:
            assert error.code == ErrorCode.CONFIG_INVALID_RECOVERY

    def test_single_error_code_is_config_invalid_recovery(self) -> None:
        """Single error also carries the correct error code."""
        with pytest.raises(ConfigurationError) as exc_info:
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_RECOVERY

    def test_errors_carry_notes_and_help_text(self) -> None:
        """Collected errors include structured notes and help_text."""
        with pytest.raises(MultipleValidationErrors) as exc_info:
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=1_000,
                claimer_heartbeat_interval_ms=30_000,
                claimed_stale_threshold_ms=1_000,
            )
        for error in exc_info.value.report.errors:
            assert len(error.notes) == 3
            assert 'threshold must be at least 2x heartbeat interval' in error.notes
            assert error.help_text is not None

    # ── Boundary values ──────────────────────────────────────────────────

    def test_exact_boundary_threshold_passes(self) -> None:
        """Threshold exactly 2x heartbeat should pass (not strict >)."""
        config = RecoveryConfig(
            runner_heartbeat_interval_ms=30_000,
            running_stale_threshold_ms=60_000,  # exactly 2x
            claimer_heartbeat_interval_ms=30_000,
            claimed_stale_threshold_ms=60_000,  # exactly 2x
        )
        assert config.running_stale_threshold_ms == 60_000
        assert config.claimed_stale_threshold_ms == 60_000

    def test_off_by_one_threshold_fails(self) -> None:
        """Threshold one ms below 2x heartbeat should raise."""
        with pytest.raises(ConfigurationError, match='running_stale_threshold_ms too low'):
            RecoveryConfig(
                runner_heartbeat_interval_ms=30_000,
                running_stale_threshold_ms=59_999,  # 2x - 1
            )

    # ── Pydantic field-constraint interaction ────────────────────────────

    def test_field_constraint_violation_raises_pydantic_error(self) -> None:
        """Field ge/le constraints fire before model_validator, raising Pydantic ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            RecoveryConfig(running_stale_threshold_ms=0)
        # Pydantic ValidationError, not ConfigurationError
        assert not isinstance(exc_info.value, HorsiesError)
