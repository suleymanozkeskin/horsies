"""Sync workflow facades run on the broker's own sync runner.

Task sync calls already run on ``broker._loop_runner``. The workflow sync
calls must use the same runner: a second runner is a second event loop over
the same connection pool and listener.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.spec import WorkflowSpec
from horsies.core.types.result import Ok, is_ok
from horsies.core.workflows.lifecycle import (
    pause_workflow,
    pause_workflow_sync,
    resume_workflow,
    resume_workflow_sync,
    start_workflow,
    start_workflow_async,
)

WORKFLOW_ID = 'wf-routing'


@pytest.mark.unit
class TestSyncWorkflowFacadesUseBrokerRunner:
    """Each sync workflow facade calls ``broker._loop_runner.call`` once."""

    def test_start_workflow(self) -> None:
        spec = cast('WorkflowSpec[None]', MagicMock())
        broker = MagicMock()
        broker._loop_runner.call.return_value = Ok(MagicMock())
        sent_at = datetime(2026, 10, 27, tzinfo=timezone.utc)

        result = start_workflow(
            spec, broker, WORKFLOW_ID, sent_at, resend_on_transient_err=True,
        )

        assert is_ok(result)
        broker._loop_runner.call.assert_called_once_with(
            start_workflow_async,
            spec,
            broker,
            WORKFLOW_ID,
            sent_at,
            resend_on_transient_err=True,
        )

    def test_pause_workflow_sync(self) -> None:
        broker = MagicMock()
        broker._loop_runner.call.return_value = Ok(True)

        result = pause_workflow_sync(broker, WORKFLOW_ID)

        assert is_ok(result)
        broker._loop_runner.call.assert_called_once_with(
            pause_workflow, broker, WORKFLOW_ID,
        )

    def test_resume_workflow_sync(self) -> None:
        broker = MagicMock()
        broker._loop_runner.call.return_value = Ok(True)

        result = resume_workflow_sync(broker, WORKFLOW_ID)

        assert is_ok(result)
        broker._loop_runner.call.assert_called_once_with(
            resume_workflow, broker, WORKFLOW_ID,
        )

    def test_handle_wrap_strategy(self) -> None:
        broker = MagicMock()
        broker._loop_runner.call.return_value = Ok('RUNNING')
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=WORKFLOW_ID, broker=broker,
        )

        result = handle.status()

        assert is_ok(result)
        broker._loop_runner.call.assert_called_once_with(handle.status_async)

    def test_handle_fold_strategy(self) -> None:
        broker = MagicMock()
        expected = MagicMock()
        broker._loop_runner.call.return_value = expected
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=WORKFLOW_ID, broker=broker,
        )

        result = handle.get(timeout_ms=5)

        assert result is expected
        broker._loop_runner.call.assert_called_once_with(handle.get_async, 5)
