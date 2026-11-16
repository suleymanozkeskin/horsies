"""``get_stale_tasks`` and ``get_expired_tasks`` on a mocked session.

Each list sends one statement in autocommit (``horsies_list_stale_tasks``,
``horsies_list_expired_tasks``). Its TASK rows become the dicts of the
client statements it replaced; its LIST_END row says whether the function
cut the list at ``TASK_LIST_ROW_BOUND`` rows. The stale threshold is
checked before any statement.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.postgres import MAX_STALE_THRESHOLD_MINUTES, TASK_LIST_ROW_BOUND
from horsies.core.brokers.result_types import BrokerInvalidInput
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import is_err, is_ok
from tests.unit.test_broker_postgres import _make_broker, _set_resources  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.unit

_STARTED = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _list_end(state: str) -> dict[str, Any]:
    return {'row_kind': 'LIST_END', 'list_state': state, 'row_bound': TASK_LIST_ROW_BOUND}


def _stale_row(task_id: str) -> dict[str, Any]:
    return {
        'row_kind': 'TASK',
        'task_id': task_id,
        'worker_hostname': 'host-1',
        'worker_pid': 1234,
        'worker_process_name': 'child-1',
        'last_heartbeat': None,
        'started_at': _STARTED,
        'task_name': 'slow_task',
    }


def _expired_row(task_id: str) -> dict[str, Any]:
    return {
        'row_kind': 'TASK',
        'task_id': task_id,
        'task_name': 'slow_task',
        'queue_name': 'default',
        'priority': 100,
        'sent_at': _STARTED,
        'enqueued_at': _STARTED,
        'good_until': _STARTED,
        'expired_for': timedelta(minutes=5),
    }


def _answer(broker: Any, rows: list[dict[str, Any]]) -> AsyncMock:
    """Answer the list statement with ``rows``; return the connection double."""
    result = MagicMock()
    result.mappings.return_value.all.return_value = rows
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=result)
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.connection = AsyncMock(return_value=connection)
    _set_resources(broker, session_factory=MagicMock(return_value=session))
    return connection


class TestStaleList:
    @pytest.mark.asyncio
    async def test_rows_become_the_dicts_of_the_client_statement(self) -> None:
        broker = _make_broker()
        _ = _answer(broker, [_stale_row('task-1'), _stale_row('task-2'), _list_end('COMPLETE')])

        result = await broker.get_stale_tasks(stale_threshold_minutes=5)

        assert is_ok(result)
        assert result.ok_value[0] == {
            'id': 'task-1',
            'worker_hostname': 'host-1',
            'worker_pid': 1234,
            'worker_process_name': 'child-1',
            'last_heartbeat': None,
            'started_at': _STARTED,
            'task_name': 'slow_task',
        }
        assert [row['id'] for row in result.ok_value] == ['task-1', 'task-2']

    @pytest.mark.asyncio
    async def test_only_the_list_end_is_an_empty_list(self) -> None:
        broker = _make_broker()
        _ = _answer(broker, [_list_end('COMPLETE')])

        result = await broker.get_stale_tasks()

        assert is_ok(result)
        assert result.ok_value == []

    @pytest.mark.asyncio
    async def test_the_threshold_goes_to_the_function(self) -> None:
        broker = _make_broker()
        connection = _answer(broker, [_list_end('COMPLETE')])

        _ = await broker.get_stale_tasks(stale_threshold_minutes=7)

        assert connection.execute.await_args.args[1] == {'threshold_minutes': 7}

    @pytest.mark.asyncio
    async def test_a_complete_list_logs_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        broker = _make_broker()
        _ = _answer(broker, [_stale_row('task-1'), _list_end('COMPLETE')])

        with caplog.at_level('WARNING'):
            result = await broker.get_stale_tasks(stale_threshold_minutes=5)

        assert is_ok(result)
        assert not [r for r in caplog.records if r.levelname == 'WARNING']

    @pytest.mark.asyncio
    async def test_a_truncated_list_logs_a_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        broker = _make_broker()
        _ = _answer(broker, [_stale_row('task-1'), _list_end('TRUNCATED')])

        with caplog.at_level('WARNING'):
            result = await broker.get_stale_tasks(stale_threshold_minutes=5)

        assert is_ok(result)
        assert [row['id'] for row in result.ok_value] == ['task-1']
        warnings = [r.getMessage() for r in caplog.records if r.levelname == 'WARNING']
        assert warnings == [
            f'get_stale_tasks returned the first {TASK_LIST_ROW_BOUND} rows; more rows match',
        ]

    @pytest.mark.asyncio
    async def test_the_threshold_bounds_are_accepted(self) -> None:
        assert MAX_STALE_THRESHOLD_MINUTES == 365 * 24 * 60
        for minutes in (0, MAX_STALE_THRESHOLD_MINUTES):
            broker = _make_broker()
            connection = _answer(broker, [_list_end('COMPLETE')])

            result = await broker.get_stale_tasks(stale_threshold_minutes=minutes)

            assert is_ok(result), minutes
            assert connection.execute.await_args.args[1] == {'threshold_minutes': minutes}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('minutes', 'cause'),
        [
            (-1, 'stale_threshold_minutes is below 0'),
            (525_601, 'stale_threshold_minutes is above 525600 (one year)'),
            (10 ** 5000, 'stale_threshold_minutes is above 525600 (one year)'),
            (True, 'stale_threshold_minutes is a bool, not an int'),
            (2.5, 'stale_threshold_minutes is a float, not an int'),
            ('5', 'stale_threshold_minutes is a str, not an int'),
        ],
        ids=['negative', 'above-one-year', 'huge-int', 'bool', 'float', 'str'],
    )
    async def test_a_threshold_outside_the_rule_is_invalid_input(
        self, minutes: object, cause: str,
    ) -> None:
        """A refused threshold is INVALID_INPUT before any database call."""
        broker = _make_broker()
        connection = _answer(broker, [_list_end('COMPLETE')])

        result = await broker.get_stale_tasks(stale_threshold_minutes=cast(int, minutes))

        assert is_err(result)
        match result.err_value:
            case BrokerInvalidInput(message=message, exception=NoCause()):
                assert message == f'{cause}; give whole minutes from 0 to 525600'
            case other:
                raise AssertionError(f'expected INVALID_INPUT, got {other!r}')
        connection.execute.assert_not_awaited()


class TestExpiredList:
    @pytest.mark.asyncio
    async def test_rows_become_the_dicts_of_the_client_statement(self) -> None:
        broker = _make_broker()
        connection = _answer(broker, [_expired_row('task-1'), _list_end('COMPLETE')])

        result = await broker.get_expired_tasks()

        assert is_ok(result)
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
        assert result.ok_value[0]['id'] == 'task-1'
        assert len(connection.execute.await_args.args) == 1

    @pytest.mark.asyncio
    async def test_a_truncated_list_logs_a_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        broker = _make_broker()
        _ = _answer(broker, [_expired_row('task-1'), _list_end('TRUNCATED')])

        with caplog.at_level('WARNING'):
            result = await broker.get_expired_tasks()

        assert is_ok(result)
        warnings = [r.getMessage() for r in caplog.records if r.levelname == 'WARNING']
        assert warnings == [
            f'get_expired_tasks returned the first {TASK_LIST_ROW_BOUND} rows; more rows match',
        ]
