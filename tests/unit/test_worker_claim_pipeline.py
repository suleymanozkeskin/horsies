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
from datetime import datetime, timedelta, timezone
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
from horsies.core.worker.sql import (
    CLAIM_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_GLOBAL_IN_FLIGHT_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
)
from tests.baseline_sql import baseline_function

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)

BROKER = PostgresConfig(
    database_url='postgresql://user:pass@localhost/db',
    pool_size=5,
    max_overflow=5,
)


# ---------------------------------------------------------------------------
# 1. CLAIM_SQL structure: RETURNING includes dispatch payload
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestClaimSqlReturningPayload:
    """CLAIM_SQL RETURNING clause includes dispatch and lifecycle metadata."""

    def test_returning_clause_has_dispatch_columns(self) -> None:
        """The compiled RETURNING clause must include all dispatch-needed columns."""
        sql_text = CLAIM_SQL.text
        normalised = ' '.join(sql_text.split())
        assert (
            'RETURNING t.id, t.task_name, t.input, '
            't.queue_name, t.is_workflow_task, t.task_options, t.claimed_at;'
        ) in normalised

    def test_claim_sql_reclaims_expired_leases(self) -> None:
        """CLAIM_SQL WHERE includes reclaim branch for expired claim_expires_at."""
        sql_text = CLAIM_SQL.text
        assert 'claim_expires_at' in sql_text
        assert 'claim_expires_at < now()' in sql_text


# ---------------------------------------------------------------------------
# 2. Lease always bounded: no NULL claim_expires_at
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLeaseAlwaysBounded:
    """All claimed tasks receive a finite lease — never NULL claim_expires_at."""

    def test_hard_cap_claim_expires_at_is_datetime(self) -> None:
        """Hard-cap mode (claim_lease_ms=None) still produces a bounded lease."""
        from horsies.core.worker.worker import Worker, WorkerConfig

        cfg = WorkerConfig(
            dsn='postgresql://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            claim_lease_ms=None,
        )
        worker = Worker(
            session_factory=MagicMock(),
            listener=MagicMock(),
            cfg=cfg,
        )

        result = worker._compute_claim_expires_at()
        assert isinstance(result, datetime)
        delta = result - datetime.now(timezone.utc)
        assert timedelta(seconds=55) < delta < timedelta(seconds=65)

    def test_soft_cap_uses_explicit_lease(self) -> None:
        """Soft-cap mode uses user-configured claim_lease_ms."""
        from horsies.core.worker.worker import Worker, WorkerConfig

        cfg = WorkerConfig(
            dsn='postgresql://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            prefetch_buffer=2,
            claim_lease_ms=500,
        )
        worker = Worker(
            session_factory=MagicMock(),
            listener=MagicMock(),
            cfg=cfg,
        )

        result = worker._compute_claim_expires_at()
        delta = result - datetime.now(timezone.utc)
        assert timedelta(milliseconds=400) < delta < timedelta(milliseconds=600)


# ---------------------------------------------------------------------------
# 3. Cap-check SQL excludes expired CLAIMED tasks
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCapCheckExcludesExpiredClaims:
    """Hard-cap counting SQL must not count expired CLAIMED tasks as in-flight.

    Without this, expired CLAIMED tasks consume cap budget and block
    the reclaim branch in CLAIM_SQL from ever running.
    """

    def _assert_excludes_expired(self, sql_text: str) -> None:
        """Verify SQL counts only active claims (non-expired)."""
        normalised = ' '.join(sql_text.split())
        # Must count RUNNING always
        assert 'RUNNING' in normalised
        # Must NOT use plain IN ('RUNNING', 'CLAIMED') which includes expired
        assert "IN ('RUNNING', 'CLAIMED')" not in normalised
        # Must filter CLAIMED by active lease
        assert 'claim_expires_at IS NULL OR claim_expires_at > now()' in normalised

    def test_global_in_flight_excludes_expired(self) -> None:
        """COUNT_GLOBAL_IN_FLIGHT_SQL must exclude expired CLAIMED."""
        self._assert_excludes_expired(COUNT_GLOBAL_IN_FLIGHT_SQL.text)

    def test_queue_hard_cap_excludes_expired(self) -> None:
        """COUNT_QUEUE_IN_FLIGHT_HARD_SQL must exclude expired CLAIMED."""
        self._assert_excludes_expired(COUNT_QUEUE_IN_FLIGHT_HARD_SQL.text)

    def test_worker_in_flight_excludes_expired(self) -> None:
        """COUNT_IN_FLIGHT_FOR_WORKER_SQL must exclude expired CLAIMED."""
        self._assert_excludes_expired(COUNT_IN_FLIGHT_FOR_WORKER_SQL.text)

    def test_worker_claimed_count_excludes_expired(self) -> None:
        """COUNT_CLAIMED_FOR_WORKER_SQL must exclude expired CLAIMED."""
        sql_text = COUNT_CLAIMED_FOR_WORKER_SQL.text
        normalised = ' '.join(sql_text.split())
        assert 'claim_expires_at IS NULL OR claim_expires_at > now()' in normalised


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


# ---------------------------------------------------------------------------
# 6. Lease renewal SQL structure
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLeaseRenewalSql:
    """RENEW_CLAIM_LEASE_SQL extends claim_expires_at for owned CLAIMED tasks."""

    def test_renewal_sql_updates_claim_expires_at(self) -> None:
        from horsies.core.worker.sql import RENEW_CLAIM_LEASE_SQL

        sql_text = RENEW_CLAIM_LEASE_SQL.text
        assert 'claim_expires_at' in sql_text
        assert ':claim_lease_ms' in sql_text
        assert 'NOW() +' in sql_text
        assert "status = 'CLAIMED'" in sql_text
        assert 'claimed_by_worker_id' in sql_text

    def test_renewal_sql_scoped_to_worker(self) -> None:
        """Renewal must only affect tasks owned by the calling worker."""
        from horsies.core.worker.sql import RENEW_CLAIM_LEASE_SQL

        sql_text = RENEW_CLAIM_LEASE_SQL.text
        assert ':wid' in sql_text

    def test_renewal_sql_has_no_expiry_guard(self) -> None:
        """Renewal must NOT check claim_expires_at > NOW().

        This ensures renewal works even after a brief transient delay
        that might have let the lease lapse momentarily.
        """
        from horsies.core.worker.sql import RENEW_CLAIM_LEASE_SQL

        sql_text = RENEW_CLAIM_LEASE_SQL.text
        normalised = ' '.join(sql_text.split())
        # The WHERE clause should only have status + worker_id, not an expiry check
        assert 'claim_expires_at > now()' not in normalised
        assert 'claim_expires_at >' not in normalised


# ---------------------------------------------------------------------------
# 7. Unclaim SQL normalisation: claim_expires_at cleared
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUnclaimSqlClearsLeaseExpiry:
    """The paused-task cleanup must clear claim_expires_at, so no stale
    lease timestamp persists on a re-queued task.
    """

    def test_paused_task_cleanup_ends_the_claim(self) -> None:
        """Paused-task cleanup moves the claimed row to history as CANCELLED
        with TASK_CANCELLED and deletes the live row with its lease."""
        normalised = ' '.join(
            baseline_function('horsies_abandon_owned_nodes').split()
        )
        assert (
            "horsies_move_tasks_to_history( v_applied_ids, 'CANCELLED', "
            "'PAUSE_ABANDON_CLAIM_BATCH', v_terminal_at, NULL, 'TASK_CANCELLED',"
        ) in normalised
        stage = ' '.join(baseline_function('horsies_move_tasks_to_history').split())
        assert 'DELETE FROM horsies_tasks AS t WHERE t.id = ANY(p_task_ids)' in stage

    def test_paused_task_cleanup_matches_only_current_worker_claims(self) -> None:
        """Paused workflow cleanup must only cancel rows owned by this worker."""
        normalised = ' '.join(baseline_function('horsies_abandon_owned_nodes').split())
        assert "t.status = 'CLAIMED'" in normalised
        assert (
            't.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)'
            in normalised
        )

    def test_cancel_cancelled_workflow_tasks_matches_only_current_worker_claims(self) -> None:
        """Cancelled workflow cleanup must only cancel rows owned by this worker."""
        normalised = ' '.join(baseline_function('horsies_cancel_owned_nodes').split())
        assert "t.status = 'CLAIMED'" in normalised
        assert (
            't.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)'
            in normalised
        )

    def test_post_claim_guards_fence_on_the_claim_generation(self) -> None:
        """Both post-claim guards carry a pairwise claim-generation fence.

        The guard runs in its own transaction after the claim committed, so a
        lapsed-and-re-claimed row is CLAIMED by this worker again and the
        (status, worker) pair alone cannot reject it. Generations travel
        alongside ids because one batch can span claim transactions. A NULL
        generation is refused before any row is read, so the fence has no
        NULL arm.
        """
        for name in ('horsies_abandon_owned_nodes', 'horsies_cancel_owned_nodes'):
            normalised = ' '.join(baseline_function(name).split())
            assert 'unnest(' in normalised
            assert (
                'FROM unnest(p_ids, p_claimed_ats) '
                'AS input(task_id, expected_claimed_at)' in normalised
            )
            assert 'AND t.claimed_at = input.expected_claimed_at' in normalised
            assert 'expected_claimed_at IS NULL' not in normalised
            assert normalised.index('batch claim generations must be non-NULL') < (
                normalised.index('FOR UPDATE')
            )
