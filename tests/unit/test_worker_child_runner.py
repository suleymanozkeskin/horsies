"""Unit tests for child-runner workflow-stop handling before task start."""

from __future__ import annotations

from typing import Any

import pytest

from horsies.core.worker.child_runner import _handle_workflow_stop_before_start


class _FakeCursor:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self.queries.append((sql, params))


class _FakeConn:
    def __init__(self) -> None:
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1


@pytest.mark.unit
def test_handle_workflow_stop_before_start_cancelled_marks_terminal() -> None:
    cursor = _FakeCursor()
    conn = _FakeConn()

    result = _handle_workflow_stop_before_start(
        cursor, conn, 'task-1', 'CANCELLED',
    )

    assert result == (False, '', 'WORKFLOW_STOPPED')
    assert conn.commits == 1
    sql_blob = '\n'.join(q[0] for q in cursor.queries)
    assert "SET status = 'SKIPPED'" in sql_blob
    assert "SET status = 'CANCELLED'" in sql_blob
    assert "SET status = 'COMPLETED'" not in sql_blob
    assert all(params == ('task-1',) for _, params in cursor.queries)


@pytest.mark.unit
def test_handle_workflow_stop_before_start_paused_requeues_non_terminal() -> None:
    cursor = _FakeCursor()
    conn = _FakeConn()

    result = _handle_workflow_stop_before_start(cursor, conn, 'task-2', 'PAUSED')

    assert result == (False, '', 'WORKFLOW_STOPPED')
    assert conn.commits == 1
    sql_blob = '\n'.join(q[0] for q in cursor.queries)
    assert "SET status = 'PENDING'" in sql_blob
    assert "SET status = 'READY'" in sql_blob
    assert 'task_id = NULL' in sql_blob
    assert "SET status = 'SKIPPED'" not in sql_blob
    assert "SET status = 'CANCELLED'" not in sql_blob
    assert all(params == ('task-2',) for _, params in cursor.queries)
