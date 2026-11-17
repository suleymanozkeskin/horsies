# pyright: reportPrivateUsage=false
"""The pruning pass through the core: the summary it builds from each record
of ``horsies._core``, and the reaper method that runs it.

The records have no Python constructor, so each test stands in a record with
a ``MagicMock`` of its class: a class pattern matches it, and its fields are
set as attributes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.history.maintenance.pruning import (
    PRUNING_SUBJECT,
    HistoryLeafSwept,
    PrunePass,
    PrunePassSummary,
    prune_expired_partitions_in_core,
    summary_of_pass,
)
from horsies.core.history.outcomes import (
    LeafAttachment,
    LeafDetached,
    LeafDropped,
    LeafPendingBlocked,
    PruneReach,
)
from horsies.core.types.core_failure import CoreOperationFailed
from horsies.core.worker.reaper import ReaperMixin

pytestmark = [pytest.mark.unit]

EXPIRES = datetime(2026, 10, 7, tzinfo=UTC)
FINALIZED = 'horsies_task_history_standard_30d_2026_10_06'
BLOCKED = 'horsies_task_history_standard_30d_2026_10_07'
DROPPED = 'horsies_task_history_standard_30d_2026_10_08'


def _record(record_class: type, **fields: object) -> MagicMock:
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _pass_record(*, reach: str = 'WITHIN_BOUNDS') -> MagicMock:
    return _record(
        core.PrunePassRecord,
        finalized_leaves=[FINALIZED],
        detached=2,
        dropped=1,
        refusals=[f'{BLOCKED}: LeafPendingBlocked'],
        errors=[f'{DROPPED}: database error'],
        reach=reach,
    )


def _handle(answer: object) -> MagicMock:
    handle = MagicMock()
    handle.prune_expired_partitions_async = AsyncMock(return_value=answer)
    return handle


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('reach_text', 'reach'),
    [('WITHIN_BOUNDS', PruneReach.WITHIN_BOUNDS), ('TRUNCATED', PruneReach.TRUNCATED)],
)
async def test_a_pass_record_is_the_summary_of_its_facts(
    reach_text: str, reach: PruneReach
) -> None:
    handle = _handle(_pass_record(reach=reach_text))

    summary = await prune_expired_partitions_in_core(handle)

    assert summary == PrunePassSummary(
        finalized_leaves=(FINALIZED,),
        detached_count=2,
        dropped_count=1,
        refusals=(f'{BLOCKED}: LeafPendingBlocked',),
        errors=(f'{DROPPED}: database error',),
        reach=reach,
    )
    assert summary.acted
    handle.prune_expired_partitions_async.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize('retryable', [True, False])
async def test_a_failed_pass_raises_with_its_subject_cause_and_retry_rule(
    retryable: bool,
) -> None:
    handle = _handle(
        _record(core.PruneFailed, message='candidates: connection refused', retryable=retryable)
    )

    with pytest.raises(CoreOperationFailed) as raised:
        _ = await prune_expired_partitions_in_core(handle)

    assert raised.value.subject == PRUNING_SUBJECT
    assert raised.value.message == 'candidates: connection refused'
    assert raised.value.retryable is retryable


def test_the_summary_of_a_python_pass_keeps_its_counts_and_reach() -> None:
    prune = PrunePass(
        finalized_leaves=(FINALIZED,),
        heartbeat_swept=(),
        history_swept=(
            HistoryLeafSwept(
                leaf_name=BLOCKED,
                class_key='standard_30d',
                detach=LeafPendingBlocked(
                    leaf_name=BLOCKED,
                    blocker_count=1,
                    expires_at=EXPIRES,
                    attachment=LeafAttachment.ATTACHED,
                ),
                drop=None,
            ),
            HistoryLeafSwept(
                leaf_name=DROPPED,
                class_key='standard_30d',
                detach=LeafDetached(leaf_name=DROPPED, expires_at=EXPIRES),
                drop=LeafDropped(leaf_name=DROPPED),
            ),
        ),
        refusals=(f'{BLOCKED}: LeafPendingBlocked',),
        errors=(),
        reach=PruneReach.TRUNCATED,
    )

    summary = summary_of_pass(prune)

    assert summary == PrunePassSummary(
        finalized_leaves=(FINALIZED,),
        detached_count=1,
        dropped_count=1,
        refusals=(f'{BLOCKED}: LeafPendingBlocked',),
        errors=(),
        reach=PruneReach.TRUNCATED,
    )
    assert summary.acted == prune.acted


def test_a_pass_that_found_nothing_did_not_act() -> None:
    summary = PrunePassSummary(
        finalized_leaves=(),
        detached_count=0,
        dropped_count=0,
        refusals=(),
        errors=(),
        reach=PruneReach.WITHIN_BOUNDS,
    )

    assert not summary.acted


@pytest.mark.asyncio
async def test_the_reaper_pass_prunes_on_the_handle_of_its_broker() -> None:
    handle = _handle(_pass_record())
    broker = MagicMock()
    broker._core_handle = handle

    summary = await ReaperMixin()._run_partition_pruning(broker, MagicMock())

    assert summary.dropped_count == 1
    handle.prune_expired_partitions_async.assert_awaited_once_with()
