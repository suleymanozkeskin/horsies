# pyright: reportPrivateUsage=false
"""The coverage pass through the core: the request it sends and the outcome
it builds from each record of ``horsies._core``.

The records have no Python constructor, so each test stands in a record with
a ``MagicMock`` of its class: a class pattern matches it, and its fields are
set as attributes. ``CoverageRequest`` is replaced with a recorder of its
keyword arguments.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.history.maintenance import coverage as coverage_module
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    CoverageEnsureFailed,
    CoverageSerialization,
    CoveredForever,
    CoveredUntil,
    StartupCoverageRefused,
    ensure_startup_coverage_in_core,
    maintain_partition_coverage_in_core,
)
from horsies.core.models.retention import RetentionConfig
from horsies.core.worker.reaper import ReaperMixin

pytestmark = [pytest.mark.unit]

UNTIL = datetime(2026, 11, 20, tzinfo=timezone.utc)
HEARTBEATS_UNTIL = datetime(2026, 11, 17, 16, tzinfo=timezone.utc)
DECLARED = (('standard_7d', timedelta(days=7)), ('standard_365d', timedelta(days=365)))


def _record(record_class: type, **fields: object) -> MagicMock:
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _ensured_record(*, covered_now: bool = True, forever: bool = False) -> MagicMock:
    history = (
        _record(core.CoveredForeverRecord)
        if forever
        else _record(core.CoveredUntilRecord, at=UNTIL)
    )
    return _record(
        core.CoverageEnsuredRecord,
        created_history_leaves=3,
        created_heartbeat_leaves=2,
        republished=True,
        heartbeat_covered_now=covered_now,
        history_covered_through=history,
        heartbeats_covered_through=HEARTBEATS_UNTIL,
        absent_leaves=['gone_leaf'],
    )


def _handle(answer: object) -> MagicMock:
    handle = MagicMock()
    handle.ensure_partition_coverage_async = AsyncMock(return_value=answer)
    return handle


@pytest.fixture
def requests(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """The keyword arguments of each ``CoverageRequest`` the facade builds."""
    built: list[dict[str, Any]] = []

    def request(**kwargs: Any) -> MagicMock:
        built.append(kwargs)
        return MagicMock(spec=core.CoverageRequest)

    monkeypatch.setattr(coverage_module, 'CoverageRequest', request)
    return built


async def _maintain(handle: MagicMock) -> object:
    return await maintain_partition_coverage_in_core(
        handle,
        history_horizon_days=2,
        heartbeat_horizon_hours=3,
        declared_classes=DECLARED,
        serialization=CoverageSerialization.PER_LEAF_LOCKS,
    )


@pytest.mark.asyncio
async def test_the_request_carries_the_horizons_the_day_counts_and_the_serialization(
    requests: list[dict[str, Any]],
) -> None:
    handle = _handle(_ensured_record())

    _ = await _maintain(handle)

    assert requests == [
        {
            'history_horizon_days': 2,
            'heartbeat_horizon_hours': 3,
            'declared_period_days': [7, 365],
            'serialization': 'PER_LEAF_LOCKS',
        },
    ]
    handle.ensure_partition_coverage_async.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_ensured_record_is_coverage_ensured(requests: list[dict[str, Any]]) -> None:
    outcome = await _maintain(_handle(_ensured_record()))

    assert outcome == CoverageEnsured(
        created_history_leaves=3,
        created_heartbeat_leaves=2,
        republished=True,
        heartbeat_covered_now=True,
        history_covered_through=CoveredUntil(at=UNTIL),
        heartbeats_covered_through=HEARTBEATS_UNTIL,
        absent_leaves=('gone_leaf',),
    )
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_a_forever_record_is_covered_forever(requests: list[dict[str, Any]]) -> None:
    outcome = await _maintain(_handle(_ensured_record(forever=True)))

    assert isinstance(outcome, CoverageEnsured)
    assert outcome.history_covered_through == CoveredForever()
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_a_contained_refusal_is_coverage_ensure_failed(
    requests: list[dict[str, Any]],
) -> None:
    failed = _record(
        core.CoverageEnsureFailedRecord,
        stage='coverage_gate_busy',
        class_key='standard_30d',
        refusal='another worker owns the partition coverage gate',
        heartbeat_covered_now=False,
        absent_leaves=[],
    )

    outcome = await _maintain(_handle(failed))

    assert outcome == CoverageEnsureFailed(
        stage='coverage_gate_busy',
        class_key='standard_30d',
        refusal='another worker owns the partition coverage gate',
        heartbeat_covered_now=False,
        absent_leaves=(),
    )
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_a_pass_with_no_outcome_raises_the_record_exception(
    requests: list[dict[str, Any]],
) -> None:
    cause = core.CoreConnectionError('ensure_partition_coverage', 'connection refused', True)
    failed = _record(
        core.CoverageFailed, message='connection refused', retryable=True, exception=cause,
    )

    with pytest.raises(core.CoreConnectionError) as raised:
        _ = await _maintain(_handle(failed))

    assert raised.value is cause
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_a_declared_class_of_another_key_is_refused_before_the_core_call(
    requests: list[dict[str, Any]],
) -> None:
    handle = _handle(_ensured_record())

    with pytest.raises(ValueError, match='standard_8d'):
        _ = await maintain_partition_coverage_in_core(
            handle,
            history_horizon_days=2,
            heartbeat_horizon_hours=2,
            declared_classes=(('standard_8d', timedelta(days=7)),),
            serialization=CoverageSerialization.GATED,
        )

    assert requests == []
    handle.ensure_partition_coverage_async.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('covered_now', [True, False])
async def test_the_startup_pass_is_gated_and_refuses_an_uncovered_now(
    requests: list[dict[str, Any]],
    covered_now: bool,
) -> None:
    handle = _handle(_ensured_record(covered_now=covered_now))

    outcome = await ensure_startup_coverage_in_core(
        handle,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
        declared_classes=(),
    )

    assert requests[0]['serialization'] == 'GATED'
    match covered_now:
        case True:
            assert isinstance(outcome, CoverageEnsured)
        case False:
            assert isinstance(outcome, StartupCoverageRefused)
            assert isinstance(outcome.outcome, CoverageEnsured)


@pytest.mark.asyncio
async def test_the_reaper_pass_runs_ungated_on_the_handle_of_its_broker(
    requests: list[dict[str, Any]],
) -> None:
    handle = _handle(_ensured_record())
    broker = MagicMock()
    broker._core_handle = handle
    retention = RetentionConfig(
        history_leaf_horizon_days=4,
        heartbeat_leaf_horizon_hours=6,
        retention_periods=(timedelta(days=7),),
    )

    outcome = await ReaperMixin()._run_partition_coverage(broker, MagicMock(), retention)

    assert isinstance(outcome, CoverageEnsured)
    assert requests == [
        {
            'history_horizon_days': 4,
            'heartbeat_horizon_hours': 6,
            'declared_period_days': [7],
            'serialization': 'PER_LEAF_LOCKS',
        },
    ]
    handle.ensure_partition_coverage_async.assert_awaited_once()
