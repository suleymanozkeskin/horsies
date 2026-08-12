"""Worker startup distinguishes fatal coverage from contained reports."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from horsies.core.history.maintenance import coverage
from horsies.core.history.maintenance.coverage import (
    CoverageEnsureFailed,
    CoverageEnsured,
    StartupCoverageRefused,
    ensure_startup_coverage,
)

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]


async def test_history_failure_is_reported_when_heartbeat_is_covered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = CoverageEnsureFailed(
        stage='ensure_leaf_coverage',
        class_key='custom_7d',
        refusal='leaf creation refused',
        heartbeat_covered_now=True,
        absent_leaves=(),
    )
    monkeypatch.setattr(
        coverage,
        'ensure_partition_coverage',
        AsyncMock(return_value=failure),
    )

    outcome = await ensure_startup_coverage(
        AsyncMock(), history_horizon_days=1, heartbeat_horizon_hours=1
    )

    assert outcome is failure


async def test_failure_refuses_when_heartbeat_is_not_covered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = CoverageEnsureFailed(
        stage='ensure_heartbeat_coverage',
        class_key='__heartbeats__',
        refusal='current leaf unavailable',
        heartbeat_covered_now=False,
        absent_leaves=(),
    )
    monkeypatch.setattr(
        coverage,
        'ensure_partition_coverage',
        AsyncMock(return_value=failure),
    )

    outcome = await ensure_startup_coverage(
        AsyncMock(), history_horizon_days=1, heartbeat_horizon_hours=1
    )

    assert outcome == StartupCoverageRefused(outcome=failure)


async def test_complete_required_coverage_allows_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    ensured = CoverageEnsured(
        created_history_leaves=0,
        created_heartbeat_leaves=0,
        republished=False,
        heartbeat_covered_now=True,
        history_covered_through=now,
        heartbeats_covered_through=now,
        absent_leaves=(),
    )
    monkeypatch.setattr(
        coverage,
        'ensure_partition_coverage',
        AsyncMock(return_value=ensured),
    )

    outcome = await ensure_startup_coverage(
        AsyncMock(), history_horizon_days=1, heartbeat_horizon_hours=1
    )

    assert outcome is ensured


async def test_success_report_refuses_when_heartbeat_is_not_covered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    ensured = CoverageEnsured(
        created_history_leaves=0,
        created_heartbeat_leaves=0,
        republished=False,
        heartbeat_covered_now=False,
        history_covered_through=now,
        heartbeats_covered_through=now,
        absent_leaves=(),
    )
    monkeypatch.setattr(
        coverage,
        'ensure_partition_coverage',
        AsyncMock(return_value=ensured),
    )

    outcome = await ensure_startup_coverage(
        AsyncMock(), history_horizon_days=1, heartbeat_horizon_hours=1
    )

    assert outcome == StartupCoverageRefused(outcome=ensured)
