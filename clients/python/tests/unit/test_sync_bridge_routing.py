"""Sync workflow facades call the core's blocking forms on the calling thread.

The blocking form runs the core operation on the calling thread, with the
GIL released; no facade calls the awaitable form. The outcome of each form
is tested in ``tests/unit/test_workflow_sync_blocking_forms.py``.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.outcome import ReadWaitTimeout
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import TaskNode
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.types.result import Err, Ok, Result, is_ok
from horsies.core.workflows.lifecycle import (
    pause_workflow,
    resume_workflow,
    start_workflow,
)

WORKFLOW_ID = '7a9c1e3f-5b7d-4f9a-8c2e-4f6a8c0e2a4c'
WORKFLOW_KEY = uuid.UUID(WORKFLOW_ID)


def _record(record_class: type) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = WORKFLOW_KEY
    return record


def _on_this_thread(threads: list[threading.Thread], answer: object) -> MagicMock:
    """A blocking core call that records the thread it ran on and answers
    ``answer``."""

    def call(*_args: object, **_kwargs: object) -> object:
        threads.append(threading.current_thread())
        return answer

    return MagicMock(side_effect=call)


@pytest.mark.unit
class TestSyncWorkflowFacadesCallTheBlockingForms:
    """Each sync workflow facade calls its blocking form of the core handle
    once, on the calling thread, and never the awaitable form."""

    def test_start_workflow(self) -> None:
        app = Horsies(AppConfig(
            queue_mode=QueueMode.DEFAULT,
            database=PostgresConfig(database_url=SecretStr('postgresql://u:p@127.0.0.1:1/db')),
        ))

        @app.task('routing.value')
        def value_task(*, value: int) -> Result[int, TaskError]:
            return Ok(value)

        node: TaskNode[int] = TaskNode(function=value_task, kwargs={'value': 1})
        spec = app.workflow('routing.flow', [node], output=node, definition_key='routing.flow.v1')
        broker = app.get_broker()
        threads: list[threading.Thread] = []
        handle = MagicMock()
        handle.start_workflow = _on_this_thread(threads, _record(core.WorkflowStarted))
        broker._core_handle = handle  # pyright: ignore[reportPrivateUsage]
        sent_at = datetime(2026, 10, 27, tzinfo=timezone.utc)

        with patch('horsies._core.WorkflowStartRequest') as request_class:
            result = start_workflow(
                spec, broker, WORKFLOW_ID, sent_at, resend_on_transient_error=True,
            )

        assert is_ok(result)
        handle.start_workflow.assert_called_once_with(request_class.return_value)
        handle.start_workflow_async.assert_not_called()
        assert threads == [threading.current_thread()]

    def test_pause_workflow_sync(self) -> None:
        threads: list[threading.Thread] = []
        broker = MagicMock()
        broker._core_handle.pause_workflow = _on_this_thread(
            threads, _record(core.WorkflowPaused),
        )

        result = pause_workflow(broker, WORKFLOW_KEY)

        assert is_ok(result)
        broker._core_handle.pause_workflow.assert_called_once_with(WORKFLOW_KEY)
        broker._core_handle.pause_workflow_async.assert_not_called()
        assert threads == [threading.current_thread()]

    def test_resume_workflow_sync(self) -> None:
        threads: list[threading.Thread] = []
        broker = MagicMock()
        broker._core_handle.resume_workflow = _on_this_thread(
            threads, _record(core.WorkflowResumed),
        )

        result = resume_workflow(broker, WORKFLOW_KEY)

        assert is_ok(result)
        broker._core_handle.resume_workflow.assert_called_once_with(WORKFLOW_KEY)
        broker._core_handle.resume_workflow_async.assert_not_called()
        assert threads == [threading.current_thread()]

    def test_handle_wrap_strategy(self) -> None:
        threads: list[threading.Thread] = []
        broker = MagicMock()
        read = MagicMock(spec=core.WorkflowStatusRead)
        read.status = 'RUNNING'
        broker._core_handle.workflow_status = _on_this_thread(threads, read)
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=WORKFLOW_KEY, broker=broker,
        )

        result = handle.status()

        assert is_ok(result)
        broker._core_handle.workflow_status.assert_called_once_with(WORKFLOW_KEY)
        broker._core_handle.workflow_status_async.assert_not_called()
        assert threads == [threading.current_thread()]

    def test_handle_read_strategy(self) -> None:
        threads: list[threading.Thread] = []
        broker = MagicMock()
        broker._core_handle.wait_workflow_result = _on_this_thread(
            threads, _record(core.WaitTimedOut),
        )
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=WORKFLOW_KEY, broker=broker,
        )

        result = handle.get(wait_ms=5)

        assert isinstance(result, Err)
        assert isinstance(result.err_value, ReadWaitTimeout)
        broker._core_handle.wait_workflow_result.assert_called_once_with(
            WORKFLOW_KEY, timeout_ms=5,
        )
        broker._core_handle.wait_workflow_result_async.assert_not_called()
        assert threads == [threading.current_thread()]

