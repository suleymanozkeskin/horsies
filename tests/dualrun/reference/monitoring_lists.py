"""The stale and expired lists as the broker read them before ``horsies_list_stale_tasks`` and ``horsies_list_expired_tasks``.

The client statements with the decided row bound: each reads at most
10,001 rows and the caller keeps the first 10,000. The threshold refusal
runs before any statement on both sides, so the reference takes only
accepted thresholds. Task ids come back as text, as the broker's engine
presents them.
"""

from __future__ import annotations

from typing import Any, Final, LiteralString

from tests.dualrun.capture import Connection

ROW_BOUND: Final = 10_000

STALE_TASKS_SQL: Final[LiteralString] = """
    SELECT
        t.id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        hb.last_heartbeat,
        t.started_at,
        t.task_name
    FROM horsies_tasks t
    LEFT JOIN LATERAL (
        SELECT sent_at AS last_heartbeat
        FROM horsies_heartbeats h
        WHERE h.task_id = t.id AND h.role = 'runner'
        ORDER BY sent_at DESC
        LIMIT 1
    ) hb ON TRUE
    WHERE t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - make_interval(mins => %(stale_threshold)s)
    ORDER BY hb.last_heartbeat NULLS FIRST
    LIMIT %(row_reach)s
"""

EXPIRED_TASKS_SQL: Final[LiteralString] = """
    SELECT
        id,
        task_name,
        queue_name,
        priority,
        sent_at,
        enqueued_at,
        good_until,
        NOW() - good_until as expired_for
    FROM horsies_tasks
    WHERE status = 'PENDING'
      AND good_until <= NOW()
    ORDER BY good_until ASC
    LIMIT %(row_reach)s
"""


def _rows(connection: Connection, statement: LiteralString, parameters: dict[str, object]) -> list[dict[str, Any]]:
    cursor = connection.execute(statement, parameters)
    assert cursor.description is not None, 'a list statement returns rows'
    columns = [column.name for column in cursor.description]
    rows = [dict(zip(columns, record, strict=True)) for record in cursor.fetchall()]
    for row in rows:
        row['id'] = str(row['id'])
    return rows[:ROW_BOUND]


def stale_tasks(connection: Connection, threshold_minutes: int) -> list[dict[str, Any]]:
    """The stale list of the client statement, cut at the bound."""
    return _rows(
        connection,
        STALE_TASKS_SQL,
        {'stale_threshold': threshold_minutes, 'row_reach': ROW_BOUND + 1},
    )


def expired_tasks(connection: Connection) -> list[dict[str, Any]]:
    """The expired list of the client statement, cut at the bound."""
    return _rows(connection, EXPIRED_TASKS_SQL, {'row_reach': ROW_BOUND + 1})
