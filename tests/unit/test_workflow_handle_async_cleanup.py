"""Tests for WorkflowHandle async cleanup under cancellation pressure."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.types.result import Ok


def _make_handle() -> WorkflowHandle[Any]:
    broker = MagicMock()
    broker.listener = MagicMock()
    return WorkflowHandle(workflow_id='wf-1', broker=broker)


@pytest.mark.unit
class TestWorkflowHandleAsyncCleanup:
    """Cancellation-safe unsubscribe behavior in get_async()."""

    @pytest.mark.asyncio
    async def test_get_async_cancellation_still_completes_unsubscribe(self) -> None:
        """A second cancellation during finally should not interrupt unsubscribe cleanup."""
        handle = _make_handle()
        q: asyncio.Queue[object] = asyncio.Queue()
        handle.broker.listener.listen = AsyncMock(return_value=Ok(q))

        started = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def _unsubscribe(_channel: str, _queue: object) -> None:
            started.set()
            await release.wait()
            finished.set()

        handle.broker.listener.unsubscribe = AsyncMock(side_effect=_unsubscribe)
        handle.status_async = AsyncMock(return_value=Ok(WorkflowStatus.RUNNING))

        task = asyncio.create_task(handle.get_async(timeout_ms=60_000))

        for _ in range(50):
            if handle.broker.listener.listen.await_count > 0:
                break
            await asyncio.sleep(0)

        task.cancel()  # Enter finally path
        await asyncio.wait_for(started.wait(), timeout=1.0)
        task.cancel()  # Cancellation while unsubscribe is in progress
        await asyncio.sleep(0)

        assert finished.is_set() is False

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert finished.is_set() is True
        handle.broker.listener.unsubscribe.assert_awaited_once_with('workflow_done', q)
