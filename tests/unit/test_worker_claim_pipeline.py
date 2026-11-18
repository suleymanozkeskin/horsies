"""Unit tests for claim pipeline hardening: lease safety, cap accounting,
config validation, crash recovery, and ownership gate behavior.

Tests cover:
- CLAIM_SQL RETURNING includes dispatch payload
- Lease always bounded (never NULL claim_expires_at)
- Cap-check SQL excludes expired CLAIMED tasks from in-flight counts
- Lease/heartbeat config validation boundaries
- Ownership gate behavioral tests (mock-based, not source inspection)
- Lease renewal SQL structure
- Unclaim SQL normalisation (claim_expires_at cleared on unclaim)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.worker.child_runner import (
    DeadlineBeforeStart,
    StartAllowed,
    StartCheck,
    StartWithin,
    _confirm_ownership_and_set_running,
)
from horsies.core.worker.run_return import CoreStoppedBeforeStart, StoppedBeforeRunning

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)

BROKER = PostgresConfig(
    database_url='postgresql://user:pass@localhost/db',
    pool_size=5,
    max_overflow=5,
)


# ---------------------------------------------------------------------------
# 4. Lease/heartbeat config validation boundaries
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLeaseHeartbeatValidation:
    """effective_claim_lease_ms must be >= 2x claimer_heartbeat_interval_ms."""

    def test_lease_below_2x_heartbeat_raises(self) -> None:
        """claim_lease_ms=5000 with heartbeat=10000 (min_lease=20000) must fail."""
        with pytest.raises(
            ConfigurationError,
            match='claim lease too short relative to claimer heartbeat',
        ) as exc_info:
            AppConfig(
                broker=BROKER,
                claim_lease_ms=5_000,
                recovery=RecoveryConfig(claimer_heartbeat_interval_ms=10_000),
            )
        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_lease_at_2x_heartbeat_passes(self) -> None:
        """claim_lease_ms=20000 with heartbeat=10000 (min_lease=20000) must pass."""
        config = AppConfig(
            broker=BROKER,
            claim_lease_ms=20_000,
            recovery=RecoveryConfig(claimer_heartbeat_interval_ms=10_000),
        )
        assert config.claim_lease_ms == 20_000

    def test_lease_above_2x_heartbeat_passes(self) -> None:
        """claim_lease_ms=60000 with heartbeat=10000 is valid."""
        config = AppConfig(
            broker=BROKER,
            claim_lease_ms=60_000,
            recovery=RecoveryConfig(claimer_heartbeat_interval_ms=10_000),
        )
        assert config.claim_lease_ms == 60_000

    def test_default_lease_below_2x_heartbeat_raises(self) -> None:
        """Default 60s lease with heartbeat=35000 (min_lease=70000) must fail."""
        with pytest.raises(
            ConfigurationError,
            match='claim lease too short relative to claimer heartbeat',
        ) as exc_info:
            AppConfig(
                broker=BROKER,
                # claim_lease_ms=None → default 60_000ms
                recovery=RecoveryConfig(claimer_heartbeat_interval_ms=35_000),
            )
        err = exc_info.value
        assert err.code == ErrorCode.CONFIG_INVALID_PREFETCH
        # Error message must mention the default
        assert 'default lease=60000ms' in str(err)

    def test_default_lease_with_default_heartbeat_passes(self) -> None:
        """Default 60s lease with default 30s heartbeat (min_lease=60000) passes."""
        config = AppConfig(broker=BROKER)
        assert config.claim_lease_ms is None  # None → worker applies 60s internally


# ---------------------------------------------------------------------------
# 5. Ownership gate: behavioral tests
# ---------------------------------------------------------------------------


TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``, which has no Python
    constructor: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.task_id = uuid.UUID(TASK_ID)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


class _FakeHandle:
    """A ``CoreChildHandle`` whose start answers ``start`` and keeps each
    call."""

    def __init__(self, start: Any) -> None:
        self._start = start
        self.calls: list[dict[str, object]] = []

    def start_attempt(self, task_id: uuid.UUID, **kwargs: object) -> Any:
        self.calls.append({'task_id': task_id, **kwargs})
        return self._start


def _start(handle: Any, start_check: Any = StartAllowed) -> Any:
    with patch(
        'horsies.core.worker.child_runner._get_child_core_handle',
        return_value=handle,
    ):
        return _confirm_ownership_and_set_running(
            TASK_ID,
            'worker-A',
            claimed_at=CLAIMED_AT,
            orphan_self_heal=True,
            start_check=start_check,
        )


@pytest.mark.unit
class TestOwnershipGateBehavioral:
    """Behavioral tests for _confirm_ownership_and_set_running.

    Exercises the paths by standing in the core handle of the child:
    1. Ownership confirmed (horsies_start_attempt answers STARTED)
    2. Ownership lost (it answers MISSED with NOT_HELD)
    3. Exception path (the core cannot run the call)
    """

    def test_ownership_confirmed_returns_none_after_one_call(self) -> None:
        """When the start answers STARTED, ownership is confirmed; the
        statement committed alone in autocommit."""
        handle = _FakeHandle(_record(core.AttemptStarted))

        result = _start(handle)

        assert result is None, 'Ownership confirmed should return None'
        assert len(handle.calls) == 1

    def test_ownership_lost_returns_claim_lost_with_no_other_call(self) -> None:
        """When the start answers NOT_HELD, returns CLAIM_LOST with no other
        statement."""
        handle = _FakeHandle(_record(core.AttemptMissed, reason='NOT_HELD'))

        result = _start(handle)

        assert result == StoppedBeforeRunning('CLAIM_LOST'), 'Ownership lost is CLAIM_LOST'
        assert len(handle.calls) == 1

    def test_exception_returns_ownership_unconfirmed(self) -> None:
        """When the core cannot run the call, returns OWNERSHIP_UNCONFIRMED."""
        handle = MagicMock()
        handle.start_attempt.side_effect = core.CoreCallTimedOut(
            'start_attempt', 40.0, 'outcome_unknown'
        )

        result = _start(handle)

        assert result == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')

    def test_a_stopped_core_is_a_core_stop_before_start(self) -> None:
        """A stopped core does not clear in the process: the child reports
        the release, then ends itself (``test_child_core_refusals``)."""
        stopped = core.CoreStopped('subject', 'cause')
        handle = MagicMock()
        handle.start_attempt.side_effect = stopped

        result = _start(handle)

        assert result == CoreStoppedBeforeStart(stopped)

    def test_a_deadline_before_start_runs_no_statement(self) -> None:
        """The start check runs right before the start. A deadline that fired
        first stops the run there: no RUNNING update, and the row stays
        CLAIMED."""
        handle = _FakeHandle(_record(core.AttemptStarted))
        checks: list[str] = []

        def deadline_fired() -> StartCheck:
            checks.append('checked')
            return DeadlineBeforeStart()

        result = _start(handle, start_check=deadline_fired)

        assert result == DeadlineBeforeStart()
        assert checks == ['checked']
        assert handle.calls == []

    def test_an_allowed_start_runs_the_update(self) -> None:
        handle = _FakeHandle(_record(core.AttemptStarted))

        result = _start(handle, start_check=StartAllowed)

        assert result is None
        assert len(handle.calls) == 1
        assert handle.calls[0]['deadline_in_ms'] is None

    def test_a_start_within_a_deadline_sends_the_time_left(self) -> None:
        """The core checks the time left once the pool gave the connection,
        before the statement."""
        handle = _FakeHandle(_record(core.AttemptStarted))

        result = _start(handle, start_check=lambda: StartWithin(milliseconds=1500))

        assert result is None
        assert handle.calls[0]['deadline_in_ms'] == 1500

    def test_a_deadline_that_passes_while_the_pool_waits_runs_no_statement(self) -> None:
        handle = _FakeHandle(_record(core.DeadlineBeforeStart))

        result = _start(handle, start_check=lambda: StartWithin(milliseconds=1))

        assert result == DeadlineBeforeStart()
