"""Database reads for the execution classes of the fleet.

``read_class_coverage`` is the fleet check: for each (queue, class) with
``PENDING`` rows, whether a live worker serves it. ``read_class_report``
adds how long each stream's oldest due row has waited and the load of each
budget of each live worker.

Each read runs its statements in the caller's session, in one transaction,
so ``now()`` is one instant for all of them. Database errors propagate to
the caller. Every read is bounded by a named limit; a read that reaches its
limit returns ``Err(ClassReadRefused)`` instead of a partial answer.

Index use, measured on PostgreSQL 17 with 98,000 PENDING rows over 5
queues, 3 classes and 100 priorities, 2,000 in-flight rows and 50,080
snapshot rows of 520 workers:

- pending streams: a skip scan of ``idx_horsies_tasks_claim_pending_class``,
  one probe per (queue, class): 32 buffers, 0.2 ms;
- stream heads: the same skip scan per (queue, class, priority): 2,379
  buffers, 6.5 ms for 700 groups. A grouped scan of every pending row read
  6,624 buffers in 13 ms, and its cost grows with the backlog;
- live snapshots: a range of ``idx_horsies_worker_states_snapshot_at``: 9
  buffers;
- in-flight rows: ``idx_horsies_tasks_worker_status``: 83 buffers, 1 ms.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.models.class_coverage import (
    ClassCoverage,
    ClassReadRefused,
    ClassReport,
    LiveWorker,
    StreamHead,
    class_coverage,
    class_report,
    live_worker,
)
from horsies.core.models.health import WORKER_STATE_LIVE_WINDOW_S
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok, Result

# The most snapshot rows inside the live window that a read takes. A
# worker writes WORKER_STATE_LIVE_WINDOW_S / snapshot interval rows into the
# window: 4 at the default 30 s, 120 at the 1 s floor.
LIVE_SNAPSHOT_ROW_LIMIT: Final = 20_000

# The most (queue, class) streams with PENDING rows that a read takes: 1,000
# queues with all three classes.
PENDING_STREAM_LIMIT: Final = 3_000

# The most (queue, class, priority) groups of PENDING rows that the report
# reads. A queue priority is 1 to 100, so one stream has at most 100.
PENDING_HEAD_LIMIT: Final = 10_000

_CHECKED_AT_SQL = text('SELECT now() AS checked_at')

# The latest live snapshot of each worker. The inner range reads the window
# newest first on idx_horsies_worker_states_snapshot_at; recent_rows tells
# the caller whether the range reached its limit.
_LIVE_WORKERS_SQL = text("""
    WITH recent AS (
        SELECT w.worker_id, w.snapshot_at, w.queues, w.class_capacity
        FROM horsies_worker_states w
        WHERE w.snapshot_at >= now() - make_interval(
            secs => CAST(:live_window_s AS double precision))
        ORDER BY w.snapshot_at DESC
        LIMIT :row_limit
    )
    SELECT DISTINCT ON (r.worker_id)
        r.worker_id, r.snapshot_at, r.queues, r.class_capacity,
        (SELECT count(*) FROM recent) AS recent_rows
    FROM recent r
    ORDER BY r.worker_id, r.snapshot_at DESC
""")

# Each distinct (queue, class) of the PENDING rows: a skip scan with one
# probe of idx_horsies_tasks_claim_pending_class per stream. The outer LIMIT
# ends the recursion.
_PENDING_STREAMS_SQL = text("""
    WITH RECURSIVE streams AS (
        (
            SELECT t.queue_name, t.execution
            FROM horsies_tasks t
            WHERE t.status = 'PENDING'
            ORDER BY t.queue_name, t.execution
            LIMIT 1
        )
        UNION ALL
        SELECT nxt.queue_name, nxt.execution
        FROM streams s
        CROSS JOIN LATERAL (
            SELECT t.queue_name, t.execution
            FROM horsies_tasks t
            WHERE t.status = 'PENDING'
              AND (t.queue_name, t.execution) > (s.queue_name, s.execution)
            ORDER BY t.queue_name, t.execution
            LIMIT 1
        ) nxt
    )
    SELECT queue_name, execution FROM streams LIMIT :stream_limit
""")

# The head of each (queue, class, priority) of the PENDING rows: the first
# index entry of the group is its earliest enqueued_at. One probe of
# idx_horsies_tasks_claim_pending_class per group.
_PENDING_HEADS_SQL = text("""
    WITH RECURSIVE heads AS (
        (
            SELECT t.queue_name, t.execution, t.priority, t.enqueued_at
            FROM horsies_tasks t
            WHERE t.status = 'PENDING'
            ORDER BY t.queue_name, t.execution, t.priority, t.enqueued_at, t.id
            LIMIT 1
        )
        UNION ALL
        SELECT nxt.queue_name, nxt.execution, nxt.priority, nxt.enqueued_at
        FROM heads h
        CROSS JOIN LATERAL (
            SELECT t.queue_name, t.execution, t.priority, t.enqueued_at
            FROM horsies_tasks t
            WHERE t.status = 'PENDING'
              AND (t.queue_name, t.execution, t.priority)
                  > (h.queue_name, h.execution, h.priority)
            ORDER BY t.queue_name, t.execution, t.priority, t.enqueued_at, t.id
            LIMIT 1
        ) nxt
    )
    SELECT queue_name, execution, enqueued_at FROM heads LIMIT :head_limit
""")

# The rows each live worker holds, per class, as horsies_claim counts them:
# RUNNING, and CLAIMED with a live lease.
_IN_FLIGHT_SQL = text("""
    SELECT t.claimed_by_worker_id AS worker_id, t.execution, count(*) AS rows
    FROM horsies_tasks t
    WHERE t.claimed_by_worker_id = ANY(CAST(:worker_ids AS varchar[]))
      AND (t.status = 'RUNNING'
           OR (t.status = 'CLAIMED'
               AND (t.claim_expires_at IS NULL OR t.claim_expires_at > now())))
    GROUP BY t.claimed_by_worker_id, t.execution
""")


async def read_class_coverage(
    session: AsyncSession,
) -> Result[ClassCoverage, ClassReadRefused]:
    """The fleet check: the coverage of each (queue, class) with PENDING rows.

    Reads only. ``Err``: a live snapshot has an unreadable or missing
    ``class_capacity``, or a read reached its limit. Database errors
    propagate.
    """
    checked_at = await _checked_at(session)
    match await _live_workers(session):
        case Ok(workers):
            pass
        case Err(refused):
            return Err(refused)
    rows = (
        await session.execute(
            _PENDING_STREAMS_SQL,
            {'stream_limit': PENDING_STREAM_LIMIT + 1},
        )
    ).fetchall()
    if len(rows) > PENDING_STREAM_LIMIT:
        return Err(
            ClassReadRefused(
                f'more than {PENDING_STREAM_LIMIT} (queue, class) streams have '
                'PENDING rows',
            ),
        )
    streams = [(row.queue_name, _stored_execution(row.execution)) for row in rows]
    return Ok(class_coverage(checked_at, workers, streams))


async def read_class_report(
    session: AsyncSession,
) -> Result[ClassReport, ClassReadRefused]:
    """Pending streams with their oldest wait, and the budget load of each live worker.

    Reads only. ``Err``: as ``read_class_coverage``, or a live worker holds
    rows of a class that its budgets do not serve. Database errors
    propagate.
    """
    checked_at = await _checked_at(session)
    match await _live_workers(session):
        case Ok(workers):
            pass
        case Err(refused):
            return Err(refused)
    rows = (
        await session.execute(
            _PENDING_HEADS_SQL, {'head_limit': PENDING_HEAD_LIMIT + 1}
        )
    ).fetchall()
    if len(rows) > PENDING_HEAD_LIMIT:
        return Err(
            ClassReadRefused(
                f'more than {PENDING_HEAD_LIMIT} (queue, class, priority) groups '
                'have PENDING rows',
            ),
        )
    heads = [
        StreamHead(row.queue_name, _stored_execution(row.execution), row.enqueued_at)
        for row in rows
    ]
    in_flight = await _in_flight(session, workers)
    return class_report(checked_at, workers, heads, in_flight)


async def _checked_at(session: AsyncSession) -> datetime:
    value: Any = (await session.execute(_CHECKED_AT_SQL)).scalar_one()
    assert isinstance(value, datetime), f'now() returned {value!r}'
    return value


async def _live_workers(
    session: AsyncSession,
) -> Result[list[LiveWorker], ClassReadRefused]:
    rows = (
        await session.execute(
            _LIVE_WORKERS_SQL,
            {
                'live_window_s': WORKER_STATE_LIVE_WINDOW_S,
                'row_limit': LIVE_SNAPSHOT_ROW_LIMIT + 1,
            },
        )
    ).fetchall()
    if rows and rows[0].recent_rows > LIVE_SNAPSHOT_ROW_LIMIT:
        return Err(
            ClassReadRefused(
                f'more than {LIVE_SNAPSHOT_ROW_LIMIT} worker snapshots are inside '
                f'the live window of {WORKER_STATE_LIVE_WINDOW_S} s',
            ),
        )
    workers: list[LiveWorker] = []
    for row in rows:
        match live_worker(
            row.worker_id, row.snapshot_at, row.queues, row.class_capacity
        ):
            case Ok(worker):
                workers.append(worker)
            case Err(refused):
                return Err(refused)
    return Ok(workers)


async def _in_flight(
    session: AsyncSession,
    workers: Sequence[LiveWorker],
) -> dict[tuple[str, Execution], int]:
    if not workers:
        return {}
    rows = (
        await session.execute(
            _IN_FLIGHT_SQL,
            {'worker_ids': [worker.worker_id for worker in workers]},
        )
    ).fetchall()
    return {
        (row.worker_id, _stored_execution(row.execution)): int(row.rows) for row in rows
    }


def _stored_execution(value: object) -> Execution:
    """The ``Execution`` of an ``execution`` column value.

    The column has a CHECK on the ``Execution`` values, so another value is
    a defect of the schema (``AssertionError``).
    """
    for member in Execution:
        if value == member.value:
            return member
    raise AssertionError(f'horsies_tasks.execution holds {value!r}')
