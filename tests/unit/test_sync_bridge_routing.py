"""Sync workflow facades call the core's blocking forms on the calling thread.

None of them calls the broker's sync runner (``run_sync``): the blocking form
runs the core operation on the calling thread, with the GIL released. The
outcome of each form is tested in
``tests/unit/test_workflow_sync_blocking_forms.py``.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.types.result import is_ok
from horsies.core.utils.loop_runner import LoopRunnerError
from horsies.core.workflows.lifecycle import (
    pause_workflow_sync,
    resume_workflow_sync,
    start_workflow,
)

WORKFLOW_ID = '7a9c1e3f-5b7d-4f9a-8c2e-4f6a8c0e2a4c'
WORKFLOW_KEY = uuid.UUID(WORKFLOW_ID)


def _record(record_class: type) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = WORKFLOW_KEY
    return record


@pytest.mark.unit
class TestSyncWorkflowFacadesCallTheBlockingForms:
    """Each sync workflow facade calls its blocking form of the core handle
    once, and never ``broker.run_sync``."""

    def test_start_workflow(self) -> None:
        app = Horsies(AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr('postgresql://u:p@127.0.0.1:1/db')),
        ))

        @app.task('routing.value')
        def value_task(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        node: TaskNode[int] = TaskNode(fn=value_task, kwargs={'value': 1})
        spec = app.workflow('routing.flow', [node], output=node, definition_key='routing.flow.v1')
        broker = app.get_broker()
        handle = MagicMock()
        handle.start_workflow.return_value = _record(core.WorkflowStarted)
        broker._core_handle = handle  # pyright: ignore[reportPrivateUsage]
        sent_at = datetime(2026, 10, 27, tzinfo=timezone.utc)

        with (
            patch.object(broker, 'run_sync') as run_sync,
            patch('horsies._core.WorkflowStartRequest') as request_class,
        ):
            result = start_workflow(
                spec, broker, WORKFLOW_ID, sent_at, resend_on_transient_err=True,
            )

        assert is_ok(result)
        handle.start_workflow.assert_called_once_with(request_class.return_value)
        handle.start_workflow_async.assert_not_called()
        run_sync.assert_not_called()

    def test_pause_workflow_sync(self) -> None:
        broker = MagicMock()
        broker._core_handle.pause_workflow.return_value = _record(core.WorkflowPaused)

        result = pause_workflow_sync(broker, WORKFLOW_ID)

        assert is_ok(result)
        broker._core_handle.pause_workflow.assert_called_once_with(WORKFLOW_KEY)
        broker.run_sync.assert_not_called()

    def test_resume_workflow_sync(self) -> None:
        broker = MagicMock()
        broker._core_handle.resume_workflow.return_value = _record(core.WorkflowResumed)

        result = resume_workflow_sync(broker, WORKFLOW_ID)

        assert is_ok(result)
        broker._core_handle.resume_workflow.assert_called_once_with(WORKFLOW_KEY)
        broker.run_sync.assert_not_called()

    def test_handle_wrap_strategy(self) -> None:
        broker = MagicMock()
        read = MagicMock(spec=core.WorkflowStatusRead)
        read.status = 'RUNNING'
        broker._core_handle.workflow_status.return_value = read
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=WORKFLOW_ID, broker=broker,
        )

        result = handle.status()

        assert is_ok(result)
        broker._core_handle.workflow_status.assert_called_once_with(WORKFLOW_KEY)
        broker.run_sync.assert_not_called()

    def test_handle_fold_strategy(self) -> None:
        broker = MagicMock()
        broker._core_handle.wait_workflow_result.return_value = _record(core.WaitTimedOut)
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=WORKFLOW_ID, broker=broker,
        )

        result = handle.get(timeout_ms=5)

        assert result.is_err()
        broker._core_handle.wait_workflow_result.assert_called_once_with(
            WORKFLOW_KEY, timeout_ms=5,
        )
        broker.run_sync.assert_not_called()


async def _double(value: int) -> int:
    return value * 2


async def _raise_value_error(message: str) -> int:
    raise ValueError(message)


def _real_broker() -> PostgresBroker:
    return PostgresBroker(
        PostgresConfig(database_url=SecretStr('postgresql://u:p@localhost/db')),
    )


@pytest.mark.unit
class TestRunSync:
    """``PostgresBroker.run_sync`` runs a coroutine on the broker's runner."""

    def test_returns_the_coroutine_value(self) -> None:
        broker = _real_broker()
        try:
            assert broker.run_sync(_double, 21) == 42
        finally:
            closed = broker.close()
            assert is_ok(closed), closed

    def test_propagates_the_coroutine_exception(self) -> None:
        broker = _real_broker()
        try:
            with pytest.raises(ValueError, match='boom'):
                broker.run_sync(_raise_value_error, 'boom')
        finally:
            closed = broker.close()
            assert is_ok(closed), closed

    def test_raises_loop_runner_error_after_close(self) -> None:
        broker = _real_broker()
        closed = broker.close()
        assert is_ok(closed), closed

        with pytest.raises(LoopRunnerError, match='stopped'):
            broker.run_sync(_double, 1)
