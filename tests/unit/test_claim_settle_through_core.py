# pyright: reportPrivateUsage=false
"""The claim-time settle through the core: the request it sends and the rows
it keeps for each record of ``horsies._core``.

The records have no Python constructor, so each test stands in a record with
a ``MagicMock`` of its class.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.worker.claiming import ClaimMixin

pytestmark = [pytest.mark.unit]

_WORKER = 'worker-settle-unit'
_CLAIMED_AT = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)


class _Probe(ClaimMixin):
    """The claim mixin with the attributes that the settle reads."""

    def __init__(self, handle: MagicMock) -> None:
        self.worker_instance_id = _WORKER
        self._handle = handle

    @property
    def core_handle(self) -> Any:
        return self._handle


def _record(record_class: type, **fields: object) -> MagicMock:
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _row(task_id: uuid.UUID, *, workflow: bool = True) -> dict[str, Any]:
    return {'id': str(task_id), 'is_workflow_task': workflow, 'claimed_at': _CLAIMED_AT}


@pytest.mark.asyncio
async def test_the_blocked_tasks_are_dropped_and_the_rest_keep_their_order() -> None:
    first, blocked, plain = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    handle = MagicMock()
    handle.settle_claimed_workflow_tasks_async = AsyncMock(
        return_value=_record(core.ClaimsSettled, blocked_task_ids=[blocked]),
    )
    rows = [_row(first), _row(blocked), _row(plain, workflow=False)]

    with patch('horsies._core.SettleRequest') as request_class:
        kept = await _Probe(handle)._filter_nonrunnable_workflow_tasks(rows)

    assert [row['id'] for row in kept] == [str(first), str(plain)]
    assert request_class.call_args.kwargs == {
        'worker_id': _WORKER,
        'claims': [(first, _CLAIMED_AT), (blocked, _CLAIMED_AT)],
    }


@pytest.mark.asyncio
async def test_rows_without_a_workflow_task_send_nothing() -> None:
    handle = MagicMock()
    handle.settle_claimed_workflow_tasks_async = AsyncMock()
    rows = [_row(uuid.uuid4(), workflow=False)]

    kept = await _Probe(handle)._filter_nonrunnable_workflow_tasks(rows)

    assert kept == rows
    handle.settle_claimed_workflow_tasks_async.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_settle_raises_with_its_retry_rule() -> None:
    cause = core.CoreConnectionError('settle_claimed_workflow_tasks', 'connection refused', True)
    handle = MagicMock()
    handle.settle_claimed_workflow_tasks_async = AsyncMock(
        return_value=_record(
            core.SettleFailed, message='connection refused', retryable=True, exception=cause,
        ),
    )

    with pytest.raises(core.CoreConnectionError) as raised:
        await _Probe(handle)._filter_nonrunnable_workflow_tasks([_row(uuid.uuid4())])

    assert raised.value is cause
