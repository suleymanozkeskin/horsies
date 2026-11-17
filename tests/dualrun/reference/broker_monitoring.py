"""The broker's monitoring reads before the monitoring switch: Python's bodies.

The dual-run reference of ``PostgresBroker.get_stale_tasks``,
``get_expired_tasks``, ``list_worker_states_async``,
``get_worker_state_async``, ``get_worker_state_history_async``,
``get_class_coverage_async`` and ``get_class_report_async``, which the core
runs since the switch. Each function takes the broker as its first argument,
so the rebinding of ``tests/dualrun/operations/monitoring_and_web.py`` can
put it in the place of the method. The task list statements call
``horsies_list_stale_tasks`` and ``horsies_list_expired_tasks``; the rows
become the dicts of the public API. The worker state reads build the
snapshot dataclasses from the stored rows; the class reads run
``tests/dualrun/reference/class_coverage_reads.py``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.engine import RowMapping

from horsies.core.brokers.postgres import (
    _DRIVER_ERRORS,  # pyright: ignore[reportPrivateUsage]
    MAX_STALE_THRESHOLD_MINUTES,
    TASK_LIST_ROW_BOUND,
    PostgresBroker,
    _loop_limit_err,  # pyright: ignore[reportPrivateUsage]
    _operation_failed,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerOperationFailed,
    BrokerResult,
)
from horsies.core.models.class_coverage import (
    ClassCoverage,
    ClassReadRefused,
    ClassReport,
)
from horsies.core.models.health import (
    WorkerStateSnapshot,
    parse_class_capacity,
    parse_slot_health,
)
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import autocommit_connection, uuid_column_value
from tests.dualrun.reference.class_coverage_reads import (
    read_class_coverage,
    read_class_report,
)

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


# Latest snapshot per worker (includes idle workers). The retired
# get_worker_stats only saw workers with RUNNING tasks; this reads the
# worker-states timeseries instead. DISTINCT ON keeps the newest row per
# worker_id.
_WORKER_STATE_COLUMNS = """
        worker_id,
        snapshot_at,
        hostname,
        pid,
        processes,
        max_claim_batch,
        max_claim_per_worker,
        cluster_wide_cap,
        queues,
        queue_priorities,
        queue_max_concurrency,
        recovery_config,
        tasks_running,
        tasks_claimed,
        memory_usage_mb,
        memory_percent,
        cpu_percent,
        children_memory_mb,
        worker_started_at,
        slots_down,
        class_capacity
"""

# Recursive skip-scan: one (worker_id, snapshot_at DESC) index probe per
# worker instead of a DISTINCT ON pass over the whole snapshot timeseries.
# The timeseries holds days of per-worker rows (5s-30s cadence), so the
# DISTINCT ON form reads every retained row to return one per worker —
# measured at 10s on a 118k-row production table.
LIST_WORKER_STATES_SQL = text(f"""
    WITH RECURSIVE latest AS (
        (
            SELECT
{_WORKER_STATE_COLUMNS}
            FROM horsies_worker_states
            ORDER BY worker_id, snapshot_at DESC
            LIMIT 1
        )
        UNION ALL
        SELECT nxt.* FROM latest l
        CROSS JOIN LATERAL (
            SELECT
{_WORKER_STATE_COLUMNS}
            FROM horsies_worker_states w
            WHERE w.worker_id > l.worker_id
            ORDER BY w.worker_id, w.snapshot_at DESC
            LIMIT 1
        ) nxt
    )
    SELECT * FROM latest
""")

GET_WORKER_STATE_LATEST_SQL = text(f"""
    SELECT
{_WORKER_STATE_COLUMNS}
    FROM horsies_worker_states
    WHERE worker_id = :worker_id
    ORDER BY snapshot_at DESC
    LIMIT 1
""")

# History for a single worker, newest first. ``:limit`` of NULL returns all
# retained rows; callers pass an explicit cap to bound the fetch.
GET_WORKER_STATE_HISTORY_SQL = text(f"""
    SELECT
{_WORKER_STATE_COLUMNS}
    FROM horsies_worker_states
    WHERE worker_id = :worker_id
    ORDER BY snapshot_at DESC
    LIMIT :limit
""")


def _class_read_refused(
    operation: str,
    refused: ClassReadRefused,
) -> Err[BrokerOperationFailed]:
    """Stored data that a class read cannot decide on, or a bound it reached.

    Not retryable: the same rows give the same answer. Nothing changed.
    """
    message = f'{operation} refused: {refused.reason}'
    return _operation_failed(
        BrokerErrorCode.MONITORING_QUERY_FAILED,
        message,
        ValueError(message),
    )


def _row_to_worker_snapshot(row: Any) -> WorkerStateSnapshot:
    """Map a worker-states row to a typed snapshot.

    Raises:
        ValueError: the stored ``slots_down`` or ``class_capacity`` does
            not have the shape that the worker writes. The read methods
            catch it and return ``MONITORING_QUERY_FAILED``.
    """
    m = row._mapping
    match parse_slot_health(m['slots_down'], m['processes']):
        case Ok(slot_health):
            pass
        case Err(unreadable):
            raise ValueError(
                f'worker {m["worker_id"]} snapshot at {m["snapshot_at"]}: '
                f'slots_down is unreadable: {unreadable.reason}',
            )
    match parse_class_capacity(m['class_capacity']):
        case Ok(class_capacity):
            pass
        case Err(unreadable):
            raise ValueError(
                f'worker {m["worker_id"]} snapshot at {m["snapshot_at"]}: '
                f'class_capacity is unreadable: {unreadable.reason}',
            )
    return WorkerStateSnapshot(
        worker_id=m['worker_id'],
        snapshot_at=m['snapshot_at'],
        hostname=m['hostname'],
        pid=m['pid'],
        processes=m['processes'],
        max_claim_batch=m['max_claim_batch'],
        max_claim_per_worker=m['max_claim_per_worker'],
        cluster_wide_cap=m['cluster_wide_cap'],
        queues=list(m['queues']),
        queue_priorities=m['queue_priorities'],
        queue_max_concurrency=m['queue_max_concurrency'],
        recovery_config=m['recovery_config'],
        tasks_running=m['tasks_running'],
        tasks_claimed=m['tasks_claimed'],
        memory_usage_mb=m['memory_usage_mb'],
        memory_percent=m['memory_percent'],
        cpu_percent=m['cpu_percent'],
        children_memory_mb=m['children_memory_mb'],
        worker_started_at=m['worker_started_at'],
        slot_health=slot_health,
        class_capacity=class_capacity,
    )


async def list_worker_states_async(self: PostgresBroker) -> BrokerResult[list[WorkerStateSnapshot]]:
    """Latest state snapshot per worker, including idle workers.

    Unlike the retired ``get_worker_stats`` (RUNNING tasks only), this
    reads ``horsies_worker_states`` so every worker that has reported a
    snapshot appears, regardless of current load.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('list_worker_states', loop_limit)
        case Ok(resources):
            pass
    try:
        async with resources.session_factory() as session:
            result = await session.execute(LIST_WORKER_STATES_SQL)
            return Ok(
                [_row_to_worker_snapshot(row) for row in result.fetchall()]
            )
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.MONITORING_QUERY_FAILED,
            f'list_worker_states failed: {exc}',
            exc,
        )


async def get_worker_state_async(
    self: PostgresBroker,
    worker_id: str,
) -> BrokerResult[WorkerStateSnapshot | None]:
    """Latest state snapshot for one worker, or ``None`` if unknown."""
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('get_worker_state', loop_limit)
        case Ok(resources):
            pass
    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKER_STATE_LATEST_SQL,
                {'worker_id': worker_id},
            )
            row = result.fetchone()
            if row is None:
                return Ok(None)
            return Ok(_row_to_worker_snapshot(row))
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.MONITORING_QUERY_FAILED,
            f'get_worker_state failed: {exc}',
            exc,
        )


async def get_worker_state_history_async(
    self: PostgresBroker,
    worker_id: str,
    *,
    limit: int | None = None,
) -> BrokerResult[list[WorkerStateSnapshot]]:
    """Timeseries snapshots for one worker, newest first.

    ``limit`` of ``None`` returns all retained rows; pass an explicit cap
    to bound the fetch (the table grows ~1 row per worker per interval).
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('get_worker_state_history', loop_limit)
        case Ok(resources):
            pass
    if limit is not None and limit <= 0:
        return Err(
            BrokerInvalidInput(
                message=f'limit must be positive when set, got {limit}',
                exception=NoCause(),
            )
        )
    try:
        async with resources.session_factory() as session:
            result = await session.execute(
                GET_WORKER_STATE_HISTORY_SQL,
                {'worker_id': worker_id, 'limit': limit},
            )
            return Ok(
                [_row_to_worker_snapshot(row) for row in result.fetchall()]
            )
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.MONITORING_QUERY_FAILED,
            f'get_worker_state_history failed: {exc}',
            exc,
        )


async def get_class_coverage_async(self: PostgresBroker) -> BrokerResult[ClassCoverage]:
    """The fleet check: for each (queue, class) with PENDING rows, whether
    a live worker serves it.

    A worker is live while its latest snapshot is at most
    ``WORKER_STATE_LIVE_WINDOW_S`` old. It serves a (queue, class) when
    the queue is one of its queues and one of its budgets serves the
    class. Reads only.
    ``Err(MONITORING_QUERY_FAILED)``: a database or network error, a live
    snapshot whose ``class_capacity`` is unreadable, or a read that
    reached its bound (``retryable`` is ``False`` for the last two).
    ``Err(EVENT_LOOP_LIMIT)``: nothing ran. A code defect raises.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('get_class_coverage', loop_limit)
        case Ok(resources):
            pass
    try:
        async with resources.session_factory() as session:
            read = await read_class_coverage(session)
    except _DRIVER_ERRORS as exc:
        return _operation_failed(
            BrokerErrorCode.MONITORING_QUERY_FAILED,
            f'get_class_coverage failed: {exc}',
            exc,
        )
    match read:
        case Ok(coverage):
            return Ok(coverage)
        case Err(refused):
            return _class_read_refused('get_class_coverage', refused)


async def get_class_report_async(self: PostgresBroker) -> BrokerResult[ClassReport]:
    """Pending streams with the wait of their oldest due row and their
    coverage, and the budget load of each live worker.

    Reads only.
    ``Err(MONITORING_QUERY_FAILED)``: as ``get_class_coverage_async``, or
    a live worker holds rows of a class that its recorded budgets do not
    serve.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('get_class_report', loop_limit)
        case Ok(resources):
            pass
    try:
        async with resources.session_factory() as session:
            read = await read_class_report(session)
    except _DRIVER_ERRORS as exc:
        return _operation_failed(
            BrokerErrorCode.MONITORING_QUERY_FAILED,
            f'get_class_report failed: {exc}',
            exc,
        )
    match read:
        case Ok(report):
            return Ok(report)
        case Err(refused):
            return _class_read_refused('get_class_report', refused)
