"""The parent's release of a claim whose start the child did not confirm.

A child that ends with ``OWNERSHIP_UNCONFIRMED`` ran no task code. The
finalize releases the claim at once with the fenced release
(``_requeue_claimed_task``), for the generation of the dispatch. Each outcome
of that release ends the finalize with ``Ok(None)``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.types.result import Ok
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.runtime import _RequeueOutcome
from horsies.core.worker.worker import Worker
from tests.dispatch_claim_support import claim_at

pytestmark = [pytest.mark.unit]

_GENERATION = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


def _worker(release: _RequeueOutcome) -> tuple[Worker, AsyncMock]:
    worker = Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
        ),
    )
    worker.worker_instance_id = 'w1'
    requeue = AsyncMock(return_value=release)
    worker._requeue_claimed_task = requeue  # type: ignore[method-assign]
    return worker, requeue


async def _finalize_unconfirmed(worker: Worker) -> object:
    return await worker._persist_task_terminal_state(
        task_id='task-1',
        now=datetime.now(timezone.utc),
        ok=False,
        result_json_str='',
        failed_reason='OWNERSHIP_UNCONFIRMED',
        task_name='my_task',
        queue_name='default',
        is_workflow_task=False,
        claim=claim_at(_GENERATION),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'release',
    [_RequeueOutcome.REQUEUED, _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED],
    ids=['released', 'not-held'],
)
async def test_the_claim_of_the_dispatch_generation_is_released(
    release: _RequeueOutcome,
) -> None:
    """One fenced release of this generation; nothing is owed to phase 2."""
    worker, requeue = _worker(release)

    result = await _finalize_unconfirmed(worker)

    assert result == Ok(None)
    requeue.assert_awaited_once_with(
        'task-1',
        'the start of the attempt was not confirmed',
        claim=claim_at(_GENERATION),
    )
    worker.sf.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_a_failed_release_leaves_the_row_to_its_lease(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker, requeue = _worker(_RequeueOutcome.DB_ERROR)

    with caplog.at_level('ERROR'):
        result = await _finalize_unconfirmed(worker)

    assert result == Ok(None)
    requeue.assert_awaited_once()
    assert (
        'Task task-1: the release of an unconfirmed start failed; '
        'the claim lease returns the row'
    ) in caplog.text
