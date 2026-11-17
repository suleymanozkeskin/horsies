"""Two-session cases of the lifecycle functions (0004_lifecycle_functions.sql).

Each case takes the connection string of a database that holds the baseline
migration and ``prepare`` has run on it. Cases use their own task ids, so they
can run one after another on one database. A case raises ``AssertionError``
with its name when a guarantee does not hold.

The lock cases check the lock order directly: a holder session takes the lock
that a function must take first, the function's session waits for it, and the
holder then takes the second lock with NOWAIT. The second lock is free only
when the waiting function took nothing else before its first lock.

This module imports only psycopg, so the cases also run outside pytest
against any database that holds the baseline.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, Final, LiteralString

import psycopg

Connection = psycopg.AsyncConnection[tuple[Any, ...]]

WAIT_POLL_INTERVAL_S: Final = 0.02
"""Time between two reads of the waiting session's state."""

WAIT_POLL_LIMIT: Final = 250
"""Reads before a case gives up on a session that never waits (5 s)."""

NOTIFY_SETTLE_S: Final = 0.3
"""Time the listener waits for the notifications of one committed write."""

STATEMENT_TIMEOUT_SQL: Final[LiteralString] = "SET statement_timeout = '10s'"
"""Bound of every statement of a case, so a lock wait that never ends fails the case."""

LISTEN_SQL: Final[tuple[LiteralString, ...]] = (
    'LISTEN task_queue_q_wake',
    'LISTEN task_new',
    'LISTEN task_done',
    'LISTEN horsies_task_status',
)
"""The channels that the wake case listens on: the queue of its tasks and the three event channels."""

PREPARE_SQL: Final[LiteralString] = """
CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid)
RETURNS TABLE (
    task_id uuid, status text, terminal_at timestamptz, terminalization_kind text, is_workflow_task boolean,
    command_fingerprint_version smallint, command_fingerprint bytea, sent_at timestamptz
)
LANGUAGE sql STABLE AS $$
    SELECT h.task_id, h.status::text, h.terminal_at, h.terminalization_kind::text, h.is_workflow_task,
        h.command_fingerprint_version, h.command_fingerprint, h.sent_at
    FROM horsies_task_history h
    WHERE h.task_id = p_task_id
    LIMIT 1
$$;
CREATE FUNCTION horsies_task_history_duplicate_staged(p_task_id uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM horsies_task_provenance_staged(p_task_id))
        OR EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions AS e WHERE e.task_id = p_task_id)
$$;
DO $$
BEGIN
    EXECUTE format(
        'CREATE TABLE horsies_heartbeats_lc_sessions PARTITION OF horsies_heartbeats FOR VALUES FROM (%L) TO (%L)',
        now() - interval '2 days', now() + interval '2 days'
    );
END
$$;
CREATE INDEX horsies_heartbeats_lc_sessions_probe ON horsies_heartbeats_lc_sessions (task_id, role, sent_at DESC);
"""
"""Fixtures that the published history readers and the partition manager provide at run time: a provenance
stand-in that reads the history parent, a duplicate guard stand-in on top of it, and one heartbeat leaf around
now."""

_INSERT_TASK_SQL: Final[LiteralString] = """
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
    created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
    claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
    worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
    is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
) VALUES (
    %(id)s, 'lc.task', %(queue)s, 100, '{"x":1}', NULL, 'ISOLATED', 'forever', now(),
    now(), 1, sha256('lc'::bytea), NULL, %(status)s::text, now() - interval '1 minute',
    CASE WHEN %(status)s::text = 'PENDING' THEN NULL ELSE now() - interval '1 second' END,
    CASE WHEN %(status)s::text = 'PENDING' THEN NULL ELSE 'w1' END,
    CASE WHEN %(status)s::text = 'CLAIMED' THEN now() + interval '1 minute' END,
    CASE WHEN %(status)s::text = 'RUNNING' THEN now() - interval '10 minutes' END,
    NULL, NULL,
    CASE WHEN %(status)s::text = 'RUNNING' THEN 7 END,
    CASE WHEN %(status)s::text = 'RUNNING' THEN 'host-a' END,
    CASE WHEN %(status)s::text = 'RUNNING' THEN 'worker-7' END,
    0, 3, NULL, NULL, FALSE, now(), TRUE, NULL, NULL
)
"""

_CLAIM_TIME_SQL: Final[LiteralString] = 'SELECT claimed_at FROM horsies_tasks WHERE id = %s'
_SENT_AT_SQL: Final[LiteralString] = 'SELECT sent_at FROM horsies_tasks WHERE id = %s'
_ADVISORY_LOCK_SQL: Final[LiteralString] = 'SELECT pg_advisory_xact_lock(hashtextextended(%s::text, 731))'
_ROW_LOCK_NOWAIT_SQL: Final[LiteralString] = 'SELECT 1 FROM horsies_tasks WHERE id = %s FOR UPDATE NOWAIT'
_WAIT_STATE_SQL: Final[LiteralString] = (
    'SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = %s'
)


async def connect(dsn: str) -> Connection:
    """An autocommit connection whose statements end after the bound of ``STATEMENT_TIMEOUT_SQL``."""
    connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await connection.execute(STATEMENT_TIMEOUT_SQL)
    return connection


async def connect_many(dsn: str, count: int) -> list[Connection]:
    """``count`` connections of ``connect``, opened one after another."""
    return [await connect(dsn) for _ in range(count)]


async def prepare(dsn: str) -> None:
    """Create the run-time fixtures (``PREPARE_SQL``) and commit them."""
    connection = await connect(dsn)
    try:
        await connection.execute(PREPARE_SQL)
    finally:
        await connection.close()


async def _task(
    connection: Connection,
    status: str,
    *,
    queue: str = 'q_sessions',
) -> uuid.UUID:
    """Insert one committed plain task of worker w1 and return its id."""
    task_id = uuid.uuid4()
    await connection.execute(_INSERT_TASK_SQL, {'id': task_id, 'queue': queue, 'status': status})
    return task_id


async def _scalar(connection: Connection, query: LiteralString, params: tuple[object, ...]) -> object:
    row = await (await connection.execute(query, params)).fetchone()
    assert row is not None, query
    return row[0]


async def _wait_for_lock(observer: Connection, pid: int, wait_event: str, case: str) -> None:
    """Return once session ``pid`` waits on a lock of kind ``wait_event``."""
    for _ in range(WAIT_POLL_LIMIT):
        row = await (await observer.execute(_WAIT_STATE_SQL, (pid,))).fetchone()
        if row == ('Lock', wait_event):
            return
        await asyncio.sleep(WAIT_POLL_INTERVAL_S)
    raise AssertionError(f'{case}: the session never waited on a lock of kind {wait_event}')


async def _first_row(
    connection: Connection,
    query: LiteralString,
    params: tuple[object, ...],
) -> tuple[Any, ...]:
    row = await (await connection.execute(query, params)).fetchone()
    assert row is not None, query
    return row


# ---------------------------------------------------------------------------
# Lock order: the task advisory lock before the row lock.
# ---------------------------------------------------------------------------

_LockedCall = Callable[[Connection, uuid.UUID, object], Coroutine[Any, Any, str]]


async def _cancel(connection: Connection, task_id: uuid.UUID, claimed_at: object) -> str:
    row = await _first_row(connection, 'SELECT outcome FROM horsies_cancel_task(%s, TRUE)', (task_id,))
    return str(row[0])


async def _complete(connection: Connection, task_id: uuid.UUID, claimed_at: object) -> str:
    row = await _first_row(
        connection,
        "SELECT outcome FROM horsies_complete_task_fused(%s, 'w1', %s, '{}', TRUE)",
        (task_id, claimed_at),
    )
    return str(row[0])


async def _fail(connection: Connection, task_id: uuid.UUID, claimed_at: object) -> str:
    row = await _first_row(
        connection,
        """SELECT outcome FROM horsies_finish_failed_attempt(
            %s, 'w1', %s, 'NONE', NULL, NULL, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)""",
        (task_id, claimed_at),
    )
    return str(row[0])


async def _fail_stale(connection: Connection, task_id: uuid.UUID, claimed_at: object) -> str:
    row = await _first_row(
        connection,
        """SELECT outcome FROM horsies_finish_stale_attempt(
            %s, 60000, 60000, 'NONE', NULL, NULL, '{}', 'WORKER_CRASHED', 'r', 'm', 'r')""",
        (task_id,),
    )
    return str(row[0])


async def _advisory_lock_comes_first(dsn: str, case: str, call: _LockedCall, status: str) -> None:
    setup, holder, caller, observer = await connect_many(dsn, 4)
    try:
        task_id = await _task(setup, status)
        claimed_at = await _scalar(setup, _CLAIM_TIME_SQL, (task_id,))
        async with holder.transaction():
            await holder.execute(_ADVISORY_LOCK_SQL, (task_id,))
            pending = asyncio.create_task(call(caller, task_id, claimed_at))
            await _wait_for_lock(observer, caller.info.backend_pid, 'advisory', case)
            # The waiting function holds no row lock yet: the holder takes it at once.
            await holder.execute(_ROW_LOCK_NOWAIT_SQL, (task_id,))
        outcome = await pending
        if outcome != 'APPLIED':
            raise AssertionError(f'{case}: expected APPLIED after the holder ended, got {outcome}')
    finally:
        for connection in (setup, holder, caller, observer):
            await connection.close()


async def case_cancel_takes_the_advisory_lock_first(dsn: str) -> None:
    await _advisory_lock_comes_first(dsn, 'cancel lock order', _cancel, 'PENDING')


async def case_success_takes_the_advisory_lock_first(dsn: str) -> None:
    await _advisory_lock_comes_first(dsn, 'success lock order', _complete, 'RUNNING')


async def case_failed_attempt_takes_the_advisory_lock_first(dsn: str) -> None:
    await _advisory_lock_comes_first(dsn, 'failed attempt lock order', _fail, 'RUNNING')


async def case_stale_attempt_takes_the_advisory_lock_first(dsn: str) -> None:
    await _advisory_lock_comes_first(dsn, 'stale attempt lock order', _fail_stale, 'RUNNING')


async def case_batch_success_locks_in_task_id_order(dsn: str) -> None:
    """The batch takes the low id first whatever the input order: while it waits for the low id's advisory
    lock it holds nothing of the high id."""
    case = 'batch lock order'
    setup, holder, caller, observer = await connect_many(dsn, 4)
    try:
        first, second = await _task(setup, 'RUNNING'), await _task(setup, 'RUNNING')
        low, high = sorted((first, second))
        low_claim = await _scalar(setup, _CLAIM_TIME_SQL, (low,))
        high_claim = await _scalar(setup, _CLAIM_TIME_SQL, (high,))
        async with holder.transaction():
            await holder.execute(_ADVISORY_LOCK_SQL, (low,))
            pending = asyncio.create_task(
                _first_row(
                    caller,
                    """SELECT string_agg(outcome, ',' ORDER BY ordinality) FROM horsies_complete_tasks_fused(
                        ARRAY[%s, %s]::uuid[], ARRAY[%s, %s]::timestamptz[], ARRAY['{}', '{}'], 'w1', TRUE)""",
                    (high, low, high_claim, low_claim),
                )
            )
            await _wait_for_lock(observer, caller.info.backend_pid, 'advisory', case)
            await holder.execute(_ROW_LOCK_NOWAIT_SQL, (high,))
        outcomes = (await pending)[0]
        if outcomes != 'APPLIED,APPLIED':
            raise AssertionError(f'{case}: expected APPLIED,APPLIED, got {outcomes}')
    finally:
        for connection in (setup, holder, caller, observer):
            await connection.close()


# ---------------------------------------------------------------------------
# A row that moves while another function waits for it.
# ---------------------------------------------------------------------------


async def case_start_of_a_row_that_moves_is_not_held(dsn: str) -> None:
    """A start that waits on a row which a move deletes misses, and writes no beat."""
    case = 'start during a move'
    setup, holder, caller, observer = await connect_many(dsn, 4)
    try:
        task_id = await _task(setup, 'CLAIMED')
        claimed_at = await _scalar(setup, _CLAIM_TIME_SQL, (task_id,))
        async with holder.transaction():
            await holder.execute('SELECT * FROM horsies_cancel_task(%s, TRUE)', (task_id,))
            pending = asyncio.create_task(
                _first_row(
                    caller,
                    "SELECT outcome, miss_reason FROM horsies_start_attempt(%s, 'w1', %s, 7, 'h', 'p', 's')",
                    (task_id, claimed_at),
                )
            )
            await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid', case)
        outcome = await pending
        beats = await _scalar(setup, 'SELECT count(*) FROM horsies_heartbeats WHERE task_id = %s', (task_id,))
        if outcome != ('MISSED', 'NOT_HELD') or beats != 0:
            raise AssertionError(f'{case}: expected MISSED NOT_HELD with no beat, got {outcome}, {beats} beats')
    finally:
        for connection in (setup, holder, caller, observer):
            await connection.close()


async def case_keyed_send_on_a_row_that_moves_is_unverifiable(dsn: str) -> None:
    """The conflict read of a keyed send waits on the row; the move deletes it; the send answers
    CONFLICT_UNVERIFIABLE and leaves no reservation and no second live row."""
    case = 'keyed send during a move'
    setup, holder, caller, observer = await connect_many(dsn, 4)
    try:
        task_id = await _task(setup, 'PENDING')
        sent_at = await _scalar(setup, _SENT_AT_SQL, (task_id,))
        async with holder.transaction():
            await holder.execute(_ADVISORY_LOCK_SQL, (task_id,))
            await holder.execute('SELECT 1 FROM horsies_tasks WHERE id = %s FOR UPDATE', (task_id,))
            pending = asyncio.create_task(
                _first_row(
                    caller,
                    """SELECT outcome FROM horsies_enqueue_task(
                        %s, 'lc.task', 'q_sessions', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
                        %s, 'NOW', NULL, NULL, 1::smallint, sha256('lc'::bytea), 'FIRST_RUN', NULL, NULL,
                        'KEYED', sha256('session-key'::bytea), 1::smallint, interval '1 hour', 'FIRST')""",
                    (task_id, sent_at),
                )
            )
            await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid', case)
            await holder.execute('SELECT * FROM horsies_cancel_task(%s, TRUE)', (task_id,))
        outcome = (await pending)[0]
        reservations = await _scalar(
            setup,
            "SELECT count(*) FROM horsies_key_reservations WHERE idempotency_key_digest = sha256('session-key'::bytea)",
            (),
        )
        live = await _scalar(setup, 'SELECT count(*) FROM horsies_tasks WHERE id = %s', (task_id,))
        if (outcome, reservations, live) != ('CONFLICT_UNVERIFIABLE', 0, 0):
            raise AssertionError(
                f'{case}: expected CONFLICT_UNVERIFIABLE, no reservation, no live row; '
                f'got {outcome}, {reservations}, {live}'
            )
    finally:
        for connection in (setup, holder, caller, observer):
            await connection.close()


async def case_repeat_send_during_a_move_never_runs_twice(dsn: str) -> None:
    """A repeat send waits on the task advisory lock of a move in progress, then finds the task in history:
    SAME_COMMAND, and no second live row."""
    case = 'repeat send during a move'
    setup, holder, caller, observer = await connect_many(dsn, 4)
    try:
        task_id = await _task(setup, 'PENDING')
        sent_at = await _scalar(setup, _SENT_AT_SQL, (task_id,))
        async with holder.transaction():
            await holder.execute('SELECT * FROM horsies_cancel_task(%s, TRUE)', (task_id,))
            pending = asyncio.create_task(
                _first_row(
                    caller,
                    """SELECT outcome FROM horsies_enqueue_task(
                        %s, 'lc.task', 'q_sessions', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
                        %s, 'NOW', NULL, NULL, 1::smallint, sha256('lc'::bytea), 'FIRST_RUN', NULL, NULL,
                        'UNKEYED', NULL, NULL, NULL, 'REPEAT')""",
                    (task_id, sent_at),
                )
            )
            await _wait_for_lock(observer, caller.info.backend_pid, 'advisory', case)
        outcome = (await pending)[0]
        live = await _scalar(setup, 'SELECT count(*) FROM horsies_tasks WHERE id = %s', (task_id,))
        if (outcome, live) != ('SAME_COMMAND', 0):
            raise AssertionError(f'{case}: expected SAME_COMMAND and no live row, got {outcome}, {live}')
    finally:
        for connection in (setup, holder, caller, observer):
            await connection.close()


async def case_claim_skips_a_locked_row(dsn: str) -> None:
    """The claim never waits on a row: a locked due row is skipped and the pass reports SHORT."""
    case = 'claim skips locked rows'
    setup, holder, caller = await connect_many(dsn, 3)
    try:
        task_id = await _task(setup, 'PENDING', queue='q_skip')
        async with holder.transaction():
            await holder.execute('SELECT 1 FROM horsies_tasks WHERE id = %s FOR UPDATE', (task_id,))
            rows = await (
                await caller.execute(
                    """SELECT row_kind, short_by FROM horsies_claim(
                        'w1', '["q_skip"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 1}'::jsonb,
                        '{"ISOLATED": "ISOLATED"}'::jsonb, 0, 1, 1, NULL, 60000, '[]'::jsonb)"""
                )
            ).fetchall()
        if rows != [('SHORT', 1)]:
            raise AssertionError(f'{case}: expected one SHORT row of 1, got {rows}')
    finally:
        for connection in (setup, holder, caller):
            await connection.close()


# ---------------------------------------------------------------------------
# Wakes and status events.
# ---------------------------------------------------------------------------


async def _drain(listener: Connection) -> list[tuple[str, str]]:
    """The notifications that arrived within ``NOTIFY_SETTLE_S``."""
    received: list[tuple[str, str]] = []
    async for notify in listener.notifies(timeout=NOTIFY_SETTLE_S):
        received.append((notify.channel, notify.payload))
    return received


async def case_wakes_and_events(dsn: str) -> None:
    case = 'wakes and events'
    setup, listener = await connect(dsn), await connect(dsn)
    try:
        # The fixture rows exist before the listener starts, so their insert events are not received.
        first, second = await _task(setup, 'RUNNING', queue='q_wake'), await _task(setup, 'RUNNING', queue='q_wake')
        silent = await _task(setup, 'RUNNING', queue='q_wake')
        failing = await _task(setup, 'RUNNING', queue='q_wake')
        retrying = await _task(setup, 'RUNNING', queue='q_wake')
        released = [await _task(setup, 'CLAIMED', queue='q_wake') for _ in range(3)]
        starting = await _task(setup, 'CLAIMED', queue='q_wake')
        for listen in LISTEN_SQL:
            await listener.execute(listen)

        def check(step: str, received: list[tuple[str, str]], expected: list[tuple[str, str]]) -> None:
            if sorted(received) != sorted(expected):
                raise AssertionError(f'{case}, {step}: expected {sorted(expected)}, got {sorted(received)}')

        sent = uuid.uuid4()
        await setup.execute(
            """SELECT * FROM horsies_enqueue_task(
                %s, 'lc.task', 'q_wake', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(), 'NOW', NULL,
                NULL, 1::smallint, sha256('wake'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
                'FIRST')""",
            (sent,),
        )
        check(
            'enqueue',
            await _drain(listener),
            [('task_new', str(sent)), ('task_queue_q_wake', 'new'), ('horsies_task_status', str(sent))],
        )

        claims = {t: await _scalar(setup, _CLAIM_TIME_SQL, (t,)) for t in (first, second, silent, failing, retrying)}
        await setup.execute(
            """SELECT * FROM horsies_complete_tasks_fused(
                ARRAY[%s, %s]::uuid[], ARRAY[%s, %s]::timestamptz[], ARRAY['{}', '{}'], 'w1', TRUE)""",
            (first, second, claims[first], claims[second]),
        )
        check(
            'batch success: one capacity wake for the queue',
            await _drain(listener),
            [
                ('task_queue_q_wake', 'capacity'),
                ('task_done', str(first)), ('horsies_task_status', str(first)),
                ('task_done', str(second)), ('horsies_task_status', str(second)),
            ],
        )
        await setup.execute(
            "SELECT * FROM horsies_complete_task_fused(%s, 'w1', %s, '{}', FALSE)", (silent, claims[silent]),
        )
        check(
            'success with wake off',
            await _drain(listener),
            [('task_done', str(silent)), ('horsies_task_status', str(silent))],
        )
        await setup.execute(
            """SELECT * FROM horsies_finish_failed_attempt(
                %s, 'w1', %s, 'NONE', NULL, NULL, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)""",
            (failing, claims[failing]),
        )
        check(
            'terminal failure of a plain task',
            await _drain(listener),
            [('task_queue_q_wake', 'capacity'), ('task_done', str(failing)), ('horsies_task_status', str(failing))],
        )
        await setup.execute(
            """SELECT * FROM horsies_finish_failed_attempt(
                %s, 'w1', %s, 'RETRY', 0, 30.0, 'SAME_CLASS', '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)""",
            (retrying, claims[retrying]),
        )
        check('retry: no wake, one status event', await _drain(listener), [('horsies_task_status', str(retrying))])

        claimed_at = [await _scalar(setup, _CLAIM_TIME_SQL, (t,)) for t in released]
        await setup.execute(
            """SELECT * FROM horsies_release_claims(
                ARRAY[%s, %s]::uuid[], ARRAY[%s, %s]::timestamptz[], 'w1', 'ISOLATED')""",
            (released[0], released[1], claimed_at[0], claimed_at[1]),
        )
        check(
            'release as ISOLATED: one class wake',
            await _drain(listener),
            [
                ('task_queue_q_wake', 'class'),
                ('horsies_task_status', str(released[0])), ('horsies_task_status', str(released[1])),
            ],
        )
        await setup.execute(
            "SELECT * FROM horsies_release_claims(ARRAY[%s]::uuid[], ARRAY[%s]::timestamptz[], 'w1', 'SAME_CLASS')",
            (released[2], claimed_at[2]),
        )
        check('release in the same class: no wake', await _drain(listener), [('horsies_task_status', str(released[2]))])

        start_claim = await _scalar(setup, _CLAIM_TIME_SQL, (starting,))
        await setup.execute(
            "SELECT * FROM horsies_start_attempt(%s, 'w1', %s, 7, 'h', 'p', 's')", (starting, start_claim),
        )
        check('start: no event', await _drain(listener), [])
        await setup.execute('SELECT * FROM horsies_cancel_task(%s, TRUE)', (starting,))
        check(
            'cancel: done and status, no wake',
            await _drain(listener),
            [('task_done', str(starting)), ('horsies_task_status', str(starting))],
        )
    finally:
        await setup.close()
        await listener.close()


CASES: Final[tuple[Callable[[str], Awaitable[None]], ...]] = (
    case_cancel_takes_the_advisory_lock_first,
    case_success_takes_the_advisory_lock_first,
    case_failed_attempt_takes_the_advisory_lock_first,
    case_stale_attempt_takes_the_advisory_lock_first,
    case_batch_success_locks_in_task_id_order,
    case_start_of_a_row_that_moves_is_not_held,
    case_keyed_send_on_a_row_that_moves_is_unverifiable,
    case_repeat_send_during_a_move_never_runs_twice,
    case_claim_skips_a_locked_row,
    case_wakes_and_events,
)
"""Every two-session case, in the order the runner calls them."""
