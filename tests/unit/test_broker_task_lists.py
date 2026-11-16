"""``get_stale_tasks`` and ``get_expired_tasks`` on a mocked session: the dicts built from the function rows.

Each list sends one statement in autocommit (``horsies_list_stale_tasks``,
``horsies_list_expired_tasks``) and maps its TASK rows to the dict keys of
the client statements it replaced; the LIST_END row ends the list.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.types.result import is_ok
from tests.unit.test_broker_postgres import _make_broker, _set_resources  # pyright: ignore[reportPrivateUsage]


@pytest.mark.unit
class TestMonitoringQueries:
    """Tests for get_stale_tasks, get_expired_tasks: the dicts built from the function rows."""

    def _setup_function_rows(self, broker: Any, rows: list[dict[str, Any]]) -> AsyncMock:
        """Answer the list function statement with ``rows``; return the connection double."""
        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = rows
        connection = AsyncMock()
        connection.execute = AsyncMock(return_value=mock_result)
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.connection = AsyncMock(return_value=connection)
        _set_resources(broker, session_factory=MagicMock(return_value=session))
        return connection

    @staticmethod
    def _list_end(state: str = 'COMPLETE') -> dict[str, Any]:
        return {'row_kind': 'LIST_END', 'list_state': state, 'row_bound': 10_000}

    @pytest.mark.asyncio
    async def test_get_stale_tasks_returns_list_of_dicts(self) -> None:
        """get_stale_tasks should return Ok(dicts) with the keys of the client statement."""
        broker = _make_broker()
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._setup_function_rows(
            broker,
            [
                {
                    'row_kind': 'TASK',
                    'task_id': 'task-1',
                    'worker_hostname': 'host-1',
                    'worker_pid': 1234,
                    'worker_process_name': 'child-1',
                    'last_heartbeat': None,
                    'started_at': started,
                    'task_name': 'slow_task',
                },
                self._list_end(),
            ],
        )

        result = await broker.get_stale_tasks(stale_threshold_minutes=5)

        assert is_ok(result)
        assert result.ok_value == [
            {
                'id': 'task-1',
                'worker_hostname': 'host-1',
                'worker_pid': 1234,
                'worker_process_name': 'child-1',
                'last_heartbeat': None,
                'started_at': started,
                'task_name': 'slow_task',
            },
        ]

    @pytest.mark.asyncio
    async def test_get_stale_tasks_empty(self) -> None:
        """get_stale_tasks with only the LIST_END row returns Ok([])."""
        broker = _make_broker()
        self._setup_function_rows(broker, [self._list_end()])

        result = await broker.get_stale_tasks()

        assert is_ok(result)
        assert result.ok_value == []

    @pytest.mark.asyncio
    async def test_get_expired_tasks_returns_list_of_dicts(self) -> None:
        """get_expired_tasks should return Ok(dicts) with the keys of the client statement."""
        broker = _make_broker()
        self._setup_function_rows(
            broker,
            [
                {
                    'row_kind': 'TASK',
                    'task_id': 'task-1',
                    'task_name': 'slow_task',
                    'queue_name': 'default',
                    'priority': 100,
                    'sent_at': None,
                    'enqueued_at': None,
                    'good_until': None,
                    'expired_for': timedelta(minutes=5),
                },
                self._list_end(),
            ],
        )

        result = await broker.get_expired_tasks()

        assert is_ok(result)
        assert len(result.ok_value) == 1
        assert list(result.ok_value[0]) == [
            'id',
            'task_name',
            'queue_name',
            'priority',
            'sent_at',
            'enqueued_at',
            'good_until',
            'expired_for',
        ]
        assert result.ok_value[0]['task_name'] == 'slow_task'
