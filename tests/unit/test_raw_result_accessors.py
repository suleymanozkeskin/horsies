"""Unit tests for ``TaskHandle.raw_result`` and ``WorkflowHandle.raw_result``.

These accessors return the underlying JSON dict without going through
``rehydrate_value`` — the escape hatch for pure-consumer processes that
don't import the result-type Pydantic models / dataclasses and therefore
can't populate the serde class registry.  Concretely they let a
monitoring service that only has the broker URL read completed task
results as plain JSON.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err, is_ok


def _make_row(result_json: str | None) -> SimpleNamespace:
    return SimpleNamespace(result=result_json)


def _make_app_with_row(result_json: str | None) -> MagicMock:
    """Build a fake Horsies app whose broker returns the given row."""
    fetchone = MagicMock(return_value=_make_row(result_json))
    sql_result = MagicMock(fetchone=fetchone)
    session = AsyncMock()
    session.execute = AsyncMock(return_value=sql_result)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    broker = MagicMock(session_factory=MagicMock(return_value=session))
    app = MagicMock()
    app.get_broker = MagicMock(return_value=broker)
    return app


@pytest.mark.unit
@pytest.mark.asyncio
class TestTaskHandleRawResult:
    async def test_returns_raw_dict_without_rehydration(self) -> None:
        # ``__h_pydantic__`` inside ok would normally trigger registry
        # lookup; raw_result must pass it through unchanged.
        raw = {
            '__h_task_result__': True,
            'ok': {
                '__h_pydantic__': True,
                'module': 'pure.consumer.never.imported',
                'qualname': 'NotImported',
                'data': {'x': 1},
            },
            'err': None,
        }
        app = _make_app_with_row(json.dumps(raw))
        handle: TaskHandle[object] = TaskHandle(
            task_id='task-1', app=app, broker_mode=True,
        )

        result = await handle.raw_result_async()

        assert result == raw
        assert isinstance(result, dict)
        ok_payload = result['ok']
        assert isinstance(ok_payload, dict)
        assert ok_payload['__h_pydantic__'] is True
        assert ok_payload['module'] == 'pure.consumer.never.imported'

    async def test_returns_none_when_no_result_stored(self) -> None:
        app = _make_app_with_row(None)
        handle: TaskHandle[object] = TaskHandle(
            task_id='task-2', app=app, broker_mode=True,
        )
        assert await handle.raw_result_async() is None

    async def test_returns_none_on_missing_row(self) -> None:
        fetchone = MagicMock(return_value=None)
        sql_result = MagicMock(fetchone=fetchone)
        session = AsyncMock()
        session.execute = AsyncMock(return_value=sql_result)
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        broker = MagicMock(session_factory=MagicMock(return_value=session))
        app = MagicMock()
        app.get_broker = MagicMock(return_value=broker)
        handle: TaskHandle[object] = TaskHandle(
            task_id='nope', app=app, broker_mode=True,
        )
        assert await handle.raw_result_async() is None

    async def test_returns_none_on_corrupt_json(self) -> None:
        app = _make_app_with_row('not json {')
        handle: TaskHandle[object] = TaskHandle(
            task_id='task-3', app=app, broker_mode=True,
        )
        assert await handle.raw_result_async() is None

    async def test_returns_none_when_app_missing(self) -> None:
        handle: TaskHandle[object] = TaskHandle(
            task_id='task-4', app=None, broker_mode=True,
        )
        assert await handle.raw_result_async() is None


@pytest.mark.unit
class TestWorkflowHandleRawResultPlumbing:
    """Sanity that the workflow handle method exists and uses the same shape.

    Full async coverage is deferred to integration tests (workflow handles
    pull in the broker listener loop which is awkward to mock).
    """

    def test_workflow_handle_exposes_raw_result(self) -> None:
        from horsies.core.models.workflow.handle import WorkflowHandle

        assert hasattr(WorkflowHandle, 'raw_result')
        assert hasattr(WorkflowHandle, 'raw_result_async')
