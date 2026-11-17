"""The broker's monitoring reads before the monitoring switch: Python's bodies.

The dual-run reference of ``PostgresBroker.get_stale_tasks`` and
``PostgresBroker.get_expired_tasks``, which the core runs since the switch.
Each function takes the broker as its first argument, so the rebinding of
``tests/dualrun/operations/monitoring_and_web.py`` can put it in the place
of the method. The statements call ``horsies_list_stale_tasks`` and
``horsies_list_expired_tasks``; the rows become the dicts of the public API.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.engine import RowMapping

from horsies.core.brokers.postgres import (
    MAX_STALE_THRESHOLD_MINUTES,
    TASK_LIST_ROW_BOUND,
    PostgresBroker,
    _loop_limit_err,  # pyright: ignore[reportPrivateUsage]
    _operation_failed,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerResult,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import autocommit_connection, uuid_column_value

# (dict key, result column) of each list row, in the order of the dict: the
# keys of the client statements that the list functions replaced.
_STALE_TASK_KEYS: Final = (
    ('id', 'task_id'),
    ('worker_hostname', 'worker_hostname'),
    ('worker_pid', 'worker_pid'),
    ('worker_process_name', 'worker_process_name'),
    ('last_heartbeat', 'last_heartbeat'),
    ('started_at', 'started_at'),
    ('task_name', 'task_name'),
)
_EXPIRED_TASK_KEYS: Final = (
    ('id', 'task_id'),
    ('task_name', 'task_name'),
    ('queue_name', 'queue_name'),
    ('priority', 'priority'),
    ('sent_at', 'sent_at'),
    ('enqueued_at', 'enqueued_at'),
    ('good_until', 'good_until'),
    ('expired_for', 'expired_for'),
)

LIST_STALE_TASKS_SQL = text("""
    SELECT row_kind, task_id, worker_hostname, worker_pid, worker_process_name,
           last_heartbeat, started_at, task_name, list_state, row_bound
    FROM horsies_list_stale_tasks(CAST(:threshold_minutes AS integer))
    ORDER BY row_position NULLS LAST
""")

LIST_EXPIRED_TASKS_SQL = text("""
    SELECT row_kind, task_id, task_name, queue_name, priority, sent_at, enqueued_at,
           good_until, expired_for, list_state, row_bound
    FROM horsies_list_expired_tasks()
    ORDER BY row_position NULLS LAST
""")


@dataclass(frozen=True, slots=True)
class _CompleteTaskList:
    """Every matching row."""

    rows: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class _TruncatedTaskList:
    """The first ``TASK_LIST_ROW_BOUND`` matching rows; more rows match."""

    rows: list[dict[str, Any]]


type _BoundedTaskList = _CompleteTaskList | _TruncatedTaskList


def _task_list_dict(
    item: RowMapping, keys: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    """One TASK row of a list function as the dict of the public API. Pure.

    The ``id`` value is the ``uuid.UUID`` of the public API.
    """
    entry: dict[str, Any] = {key: item[column] for key, column in keys}
    entry['id'] = uuid_column_value(entry['id'], column='task_id')
    return entry


def _function_task_list(
    rows: Sequence[RowMapping],
    keys: tuple[tuple[str, str], ...],
) -> _BoundedTaskList:
    """The TASK rows of a list function as dicts, and whether it cut the list. Pure.

    ``keys`` maps each dict key to its result column, in dict order. The
    last row is the LIST_END row: ``TRUNCATED`` means the function cut the
    list at ``TASK_LIST_ROW_BOUND`` rows.
    """
    match rows:
        case [*items, end]:
            pass
        case _:
            raise AssertionError('the list function answered no LIST_END row')
    assert end['row_kind'] == 'LIST_END', end['row_kind']
    assert end['row_bound'] == TASK_LIST_ROW_BOUND, end['row_bound']
    assert all(item['row_kind'] == 'TASK' for item in items), 'a row before LIST_END is not a TASK row'
    dicts = [_task_list_dict(item, keys) for item in items]
    match end['list_state']:
        case 'COMPLETE':
            return _CompleteTaskList(rows=dicts)
        case 'TRUNCATED':
            return _TruncatedTaskList(rows=dicts)
        case other:
            raise AssertionError(f'list state {other!r} is not COMPLETE or TRUNCATED')


def _stale_threshold_minutes(value: object) -> Result[int, BrokerInvalidInput]:
    """Check the stale threshold: whole minutes from 0 to
    ``MAX_STALE_THRESHOLD_MINUTES``. Pure.

    The refusal text holds no value, because an int with more than 4300
    digits cannot be converted to a string.
    """
    match value:
        case bool():
            cause = 'is a bool, not an int'
        case int() if value < 0:
            cause = 'is below 0'
        case int() if value > MAX_STALE_THRESHOLD_MINUTES:
            cause = f'is above {MAX_STALE_THRESHOLD_MINUTES} (one year)'
        case int():
            return Ok(value)
        case _:
            cause = f'is a {type(value).__name__}, not an int'
    return Err(BrokerInvalidInput(
        message=(
            f'stale_threshold_minutes {cause}; give whole minutes from 0 '
            f'to {MAX_STALE_THRESHOLD_MINUTES}'
        ),
        exception=NoCause(),
    ))


def _rows_within_bound(
    broker: PostgresBroker,
    operation: str,
    bounded: _BoundedTaskList,
) -> list[dict[str, Any]]:
    """The rows of a task list read; a cut list logs a warning that names
    ``operation`` and the bound."""
    match bounded:
        case _CompleteTaskList(rows=complete):
            return complete
        case _TruncatedTaskList(rows=first_rows):
            broker.logger.warning(
                '%s returned the first %d rows; more rows match',
                operation,
                TASK_LIST_ROW_BOUND,
            )
            return first_rows


async def get_stale_tasks(
    self: PostgresBroker,
    stale_threshold_minutes: int = 2,
) -> BrokerResult[list[dict[str, Any]]]:
    """RUNNING tasks whose latest runner heartbeat, or their start when
    they have no heartbeat, is older than the threshold. Reads only.

    Rows come ordered by the latest heartbeat, rows with none first, then
    by id. At most ``TASK_LIST_ROW_BOUND`` rows come back; when more rows
    match, the first ``TASK_LIST_ROW_BOUND`` come back and a warning is
    logged.

    - ``Err(INVALID_INPUT)``: ``stale_threshold_minutes`` is not an int
      from 0 to ``MAX_STALE_THRESHOLD_MINUTES``. Nothing ran.
    - ``Err(MONITORING_QUERY_FAILED)``: the read failed.
    """
    match _stale_threshold_minutes(stale_threshold_minutes):
        case Err() as refused:
            return refused
        case Ok(ok_value=threshold):
            pass
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('get_stale_tasks', loop_limit)
        case Ok(resources):
            pass
    try:
        async with resources.session_factory() as session:
            connection = await autocommit_connection(session)
            result = await connection.execute(
                LIST_STALE_TASKS_SQL, {'threshold_minutes': threshold},
            )
            rows = result.mappings().all()
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.MONITORING_QUERY_FAILED,
            f'get_stale_tasks failed: {exc}',
            exc,
        )
    return Ok(_rows_within_bound(
        self, 'get_stale_tasks', _function_task_list(rows, _STALE_TASK_KEYS),
    ))


async def get_expired_tasks(self: PostgresBroker) -> BrokerResult[list[dict[str, Any]]]:
    """PENDING tasks whose ``good_until`` passed before a worker took
    them. Reads only.

    Rows come ordered by ``good_until``, oldest first, then by id. At
    most ``TASK_LIST_ROW_BOUND`` rows come back; when more rows match,
    the first ``TASK_LIST_ROW_BOUND`` come back and a warning is logged.

    - ``Err(MONITORING_QUERY_FAILED)``: the read failed.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('get_expired_tasks', loop_limit)
        case Ok(resources):
            pass
    try:
        async with resources.session_factory() as session:
            connection = await autocommit_connection(session)
            result = await connection.execute(LIST_EXPIRED_TASKS_SQL)
            rows = result.mappings().all()
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.MONITORING_QUERY_FAILED,
            f'get_expired_tasks failed: {exc}',
            exc,
        )
    return Ok(_rows_within_bound(
        self, 'get_expired_tasks', _function_task_list(rows, _EXPIRED_TASK_KEYS),
    ))
