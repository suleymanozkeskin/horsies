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

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.worker.sql import (
    CLAIM_SQL,
    COUNT_CLAIMED_FOR_WORKER_SQL,
    COUNT_GLOBAL_IN_FLIGHT_SQL,
    COUNT_IN_FLIGHT_FOR_WORKER_SQL,
    COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
    UNCLAIM_CLAIMED_TASK_SQL,
    UNCLAIM_PAUSED_TASKS_SQL,
)

BROKER = PostgresConfig(
    database_url='postgresql+psycopg://user:pass@localhost/db',
    pool_size=5,
    max_overflow=5,
)


# ---------------------------------------------------------------------------
# 1. CLAIM_SQL structure: RETURNING includes dispatch payload
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestClaimSqlReturningPayload:
    """CLAIM_SQL RETURNING clause includes id, task_name, args, kwargs."""

    def test_returning_clause_has_dispatch_columns(self) -> None:
        """The compiled RETURNING clause must include all dispatch-needed columns."""
        sql_text = CLAIM_SQL.text
        normalised = ' '.join(sql_text.split())
        assert 'RETURNING t.id, t.task_name, t.args, t.kwargs;' in normalised

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
            dsn='postgresql+psycopg://u:p@localhost/db',
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
            dsn='postgresql+psycopg://u:p@localhost/db',
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


class _FakeCursor:
    """Minimal cursor mock supporting execute/fetchone for ownership tests."""

    def __init__(self, fetchone_return: Any = None) -> None:
        self._fetchone_return = fetchone_return
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.queries.append((sql, params))

    def fetchone(self) -> Any:
        return self._fetchone_return


class _FakeConn:
    """Minimal connection mock tracking commit/rollback calls."""

    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _FakePool:
    """Minimal pool mock that returns a _FakeConn via context manager."""

    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    @contextmanager
    def connection(self) -> Generator[_FakeConn, None, None]:
        yield self._conn


@pytest.mark.unit
class TestOwnershipGateBehavioral:
    """Behavioral tests for _confirm_ownership_and_set_running.

    Exercises three code paths by mocking _get_worker_pool:
    1. Ownership confirmed (UPDATE returns a row)
    2. Ownership lost (UPDATE returns None, no workflow status)
    3. Exception path (pool raises)
    """

    def test_ownership_confirmed_returns_none_and_commits(self) -> None:
        """When UPDATE RETURNING returns a row, ownership is confirmed."""
        cursor = _FakeCursor(fetchone_return=('task-1',))
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ), patch(
            'horsies.core.worker.child_runner._update_workflow_task_running_with_retry',
        ) as mock_wf_update:
            from horsies.core.worker.child_runner import (
                _confirm_ownership_and_set_running,
            )

            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result is None, 'Ownership confirmed should return None'
        assert conn.commits == 1, 'Should commit on ownership confirmation'
        assert conn.rollbacks == 0, 'No rollback on success'
        mock_wf_update.assert_called_once_with('task-1')

    def test_ownership_lost_returns_claim_lost_and_rollbacks(self) -> None:
        """When UPDATE RETURNING returns None and no workflow status, returns CLAIM_LOST."""
        # First fetchone (UPDATE RETURNING) → None (ownership lost)
        # Second fetchone (workflow status check) → None (not a workflow task)
        call_count = 0

        class _MultiReturnCursor(_FakeCursor):
            def fetchone(self) -> Any:
                nonlocal call_count
                call_count += 1
                return None  # Both UPDATE and workflow check return None

        cursor = _MultiReturnCursor()
        conn = _FakeConn(cursor)
        pool = _FakePool(conn)

        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            return_value=pool,
        ), patch(
            'horsies.core.worker.child_runner._update_workflow_task_running_with_retry',
        ) as mock_wf_update:
            from horsies.core.worker.child_runner import (
                _confirm_ownership_and_set_running,
            )

            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result is not None, 'Ownership lost should return a tuple'
        ok, _result_json, reason = result
        assert ok is False
        assert reason == 'CLAIM_LOST'
        assert conn.rollbacks == 1, 'Should rollback on ownership loss'
        assert conn.commits == 0, 'No commit on ownership loss'
        mock_wf_update.assert_not_called()

    def test_exception_returns_ownership_unconfirmed(self) -> None:
        """When pool raises an exception, returns OWNERSHIP_UNCONFIRMED."""
        with patch(
            'horsies.core.worker.child_runner._get_worker_pool',
            side_effect=RuntimeError('pool unavailable'),
        ), patch(
            'horsies.core.worker.child_runner._update_workflow_task_running_with_retry',
        ) as mock_wf_update:
            from horsies.core.worker.child_runner import (
                _confirm_ownership_and_set_running,
            )

            result = _confirm_ownership_and_set_running('task-1', 'worker-A')

        assert result is not None, 'Exception path should return a tuple'
        ok, _result_json, reason = result
        assert ok is False
        assert reason == 'OWNERSHIP_UNCONFIRMED'
        mock_wf_update.assert_not_called()


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
        assert ':new_expires_at' in sql_text
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
    """Both unclaim SQL variants must clear claim_expires_at to prevent
    stale lease timestamps from persisting on re-queued tasks.
    """

    def test_unclaim_paused_tasks_clears_claim_expires_at(self) -> None:
        """UNCLAIM_PAUSED_TASKS_SQL must SET claim_expires_at = NULL."""
        sql_text = UNCLAIM_PAUSED_TASKS_SQL.text
        normalised = ' '.join(sql_text.split())
        assert 'claim_expires_at = NULL' in normalised

    def test_unclaim_claimed_task_clears_claim_expires_at(self) -> None:
        """UNCLAIM_CLAIMED_TASK_SQL must SET claim_expires_at = NULL."""
        sql_text = UNCLAIM_CLAIMED_TASK_SQL.text
        normalised = ' '.join(sql_text.split())
        assert 'claim_expires_at = NULL' in normalised
