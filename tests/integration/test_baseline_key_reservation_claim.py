"""horsies_key_reservation_claim (0003_interim_functions.sql) under concurrent claims of one key.

Each test builds its own database (``isolated_test_database``) that holds the
baseline migration.

- The two-session tests order the sessions by lock waits: a holder session
  keeps a transaction open, the caller's claim waits on it (read from
  ``pg_stat_activity``), and the holder then commits or rolls back.
- The send tests run ``CONCURRENT_SENDS`` calls of ``horsies_enqueue_task``
  under one key. A table lock holds all of them before the key claim and
  releases them together.
"""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from collections.abc import AsyncGenerator
from typing import Any, Final, LiteralString

import psycopg
import pytest
import pytest_asyncio

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

Connection = psycopg.AsyncConnection[tuple[Any, ...]]

CONCURRENT_SENDS: Final = 64
"""Sends of one key in the send tests, as in the identity prototype tests."""

CLAIM_ROUNDS: Final = 3
"""``c_claim_rounds`` of the claim: read, delete and insert rounds before it raises 40001."""

STATEMENT_TIMEOUT_SQL: Final[LiteralString] = "SET statement_timeout = '10s'"
"""Bound of every statement, so a lock wait that never ends fails the test."""

WAIT_POLL_INTERVAL_S: Final = 0.02
"""Time between two reads of the waiting sessions' state."""

WAIT_POLL_LIMIT: Final = 250
"""Reads before a test gives up on sessions that never wait (5 s)."""

PAUSE_LOCK_KEY: Final = 937_001
"""Advisory lock that the pause trigger (``_PAUSE_TRIGGER_SQL``) of the round test waits on."""

KEY: Final = hashlib.sha256(b'key-claim-test').digest()
COMMAND_A: Final = hashlib.sha256(b'command-a').digest()
COMMAND_B: Final = hashlib.sha256(b'command-b').digest()

_CLAIM_SQL: Final[LiteralString] = """
    SELECT outcome, task_id FROM horsies_key_reservation_claim(
        %s, 1::smallint, interval '1 hour', 1::smallint, %s, %s)
"""
_TERMINALIZE_IN_THE_PAST_SQL: Final[LiteralString] = (
    "SELECT horsies_key_reservation_terminalize(%s, %s, now() - interval '2 hours')"
)
"""Terminal two hours ago with a one-hour window: the reservation is expired."""
_CLEANUP_SQL: Final[LiteralString] = 'SELECT horsies_key_reservation_cleanup(10)'
_RESERVATIONS_SQL: Final[LiteralString] = """
    SELECT task_id, disposition FROM horsies_key_reservations
    WHERE idempotency_key_digest = %s
"""
_LOCK_RESERVATION_SQL: Final[LiteralString] = (
    'SELECT 1 FROM horsies_key_reservations WHERE idempotency_key_digest = %s FOR UPDATE'
)
_WAIT_STATE_SQL: Final[LiteralString] = (
    'SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = %s'
)
_WAITING_COUNT_SQL: Final[LiteralString] = """
    SELECT count(*) FROM pg_stat_activity
    WHERE pid = ANY(%s) AND wait_event_type = 'Lock' AND wait_event = 'relation'
"""
_PAUSE_TRIGGER_SQL: Final[LiteralString] = """
CREATE FUNCTION key_claim_test_pause() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_lock_key text := nullif(current_setting('horsies_test.pause_lock_key', TRUE), '');
BEGIN
    IF v_lock_key IS NOT NULL THEN
        PERFORM pg_advisory_lock(v_lock_key::bigint);
        PERFORM pg_advisory_unlock(v_lock_key::bigint);
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER key_claim_test_pause BEFORE INSERT ON horsies_key_reservations
    FOR EACH ROW EXECUTE FUNCTION key_claim_test_pause();
"""
"""A BEFORE INSERT trigger fires before the conflict check of INSERT ... ON CONFLICT. In a session that sets
horsies_test.pause_lock_key it waits on that advisory lock before every insert of a reservation, so the test can
commit rows between the claim's read and its insert."""
_SEND_SQL: Final[LiteralString] = """
    SELECT outcome, task_id FROM horsies_enqueue_task(
        %s, 'key.claim', 'q_key_claim', 100, '{"order_id":"order-1"}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
        now(), 'NOW', NULL, NULL, 1::smallint, %s, 'FIRST_RUN', NULL, NULL,
        'KEYED', %s, 1::smallint, 3600, 'FIRST')
"""


def _migration() -> BaselineMigration:
    match load_baseline_migration():
        case Ok(loaded):
            return loaded
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


@pytest_asyncio.fixture
async def baseline_dsn() -> AsyncGenerator[str]:
    """The connection string of a fresh database that holds the baseline migration."""
    migration = _migration()
    async with isolated_test_database(DB_URL) as url:
        dsn = to_psycopg_url(url)
        connection = await psycopg.AsyncConnection.connect(dsn)
        try:
            # No parameters: psycopg sends the text with the simple query
            # protocol, which runs every statement of it.
            async with connection.transaction():
                await connection.execute(migration.sql.encode('utf-8'))
        finally:
            await connection.close()
        yield dsn


async def _connect_many(dsn: str, count: int) -> list[Connection]:
    """``count`` autocommit connections whose statements end after the bound of ``STATEMENT_TIMEOUT_SQL``."""
    connections: list[Connection] = []
    for _ in range(count):
        connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
        connections.append(connection)
        await connection.execute(STATEMENT_TIMEOUT_SQL)
    return connections


async def _close_all(connections: list[Connection]) -> None:
    for connection in connections:
        await connection.close()


async def _row(connection: Connection, query: LiteralString, params: tuple[object, ...]) -> tuple[Any, ...]:
    row = await (await connection.execute(query, params)).fetchone()
    assert row is not None, query
    return row


async def _claim(connection: Connection, command: bytes, task_id: uuid.UUID) -> tuple[str, uuid.UUID]:
    outcome, owner = await _row(connection, _CLAIM_SQL, (KEY, command, task_id))
    return str(outcome), owner


async def _reservations(connection: Connection) -> list[tuple[uuid.UUID, str]]:
    rows = await (await connection.execute(_RESERVATIONS_SQL, (KEY,))).fetchall()
    return [(row[0], str(row[1])) for row in rows]


async def _wait_for_lock(observer: Connection, pid: int, wait_event: str) -> None:
    """Return once session ``pid`` waits on a lock of kind ``wait_event``."""
    for _ in range(WAIT_POLL_LIMIT):
        row = await (await observer.execute(_WAIT_STATE_SQL, (pid,))).fetchone()
        if row == ('Lock', wait_event):
            return
        await asyncio.sleep(WAIT_POLL_INTERVAL_S)
    pytest.fail(f'session {pid} never waited on a lock of kind {wait_event}')


async def _expired_reservation(connection: Connection) -> uuid.UUID:
    """Commit an expired TERMINAL reservation of ``KEY`` and return its task id."""
    owner = uuid.uuid4()
    assert await _claim(connection, COMMAND_A, owner) == ('APPLIED', owner)
    assert await _row(connection, _TERMINALIZE_IN_THE_PAST_SQL, (KEY, owner)) == (True,)
    return owner


# ---------------------------------------------------------------------------
# Two sessions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize(
    ('command', 'expected'),
    [(COMMAND_A, 'REPLAY'), (COMMAND_B, 'CONFLICT')],
    ids=['same command', 'other command'],
)
async def test_a_claim_that_waits_on_a_committed_first_claim_is_classified_against_it(
    baseline_dsn: str,
    command: bytes,
    expected: str,
) -> None:
    """The caller's insert waits on the holder's uncommitted reservation; after the commit the caller reads it."""
    holder, caller, observer = await _connect_many(baseline_dsn, 3)
    try:
        first, second = uuid.uuid4(), uuid.uuid4()
        async with holder.transaction():
            assert await _claim(holder, COMMAND_A, first) == ('APPLIED', first)
            pending = asyncio.create_task(_claim(caller, command, second))
            await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid')
        assert await pending == (expected, first)
        assert await _reservations(observer) == [(first, 'LIVE')]
    finally:
        await _close_all([holder, caller, observer])


@pytest.mark.asyncio(loop_scope='function')
async def test_a_claim_that_waits_on_a_rolled_back_first_claim_applies(baseline_dsn: str) -> None:
    holder, caller, observer = await _connect_many(baseline_dsn, 3)
    try:
        first, second = uuid.uuid4(), uuid.uuid4()
        await holder.execute('BEGIN')
        assert await _claim(holder, COMMAND_A, first) == ('APPLIED', first)
        pending = asyncio.create_task(_claim(caller, COMMAND_A, second))
        await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid')
        await holder.execute('ROLLBACK')
        assert await pending == ('APPLIED', second)
        assert await _reservations(observer) == [(second, 'LIVE')]
    finally:
        await _close_all([holder, caller, observer])


@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize('holder_end', ['COMMIT', 'ROLLBACK'])
async def test_a_claim_that_waits_on_the_cleanup_of_an_expired_reservation_applies(
    baseline_dsn: str,
    holder_end: LiteralString,
) -> None:
    """The cleanup holds the expired reservation; the caller's delete of it waits. Whether the cleanup's delete
    commits or rolls back, the caller takes the key."""
    holder, caller, observer = await _connect_many(baseline_dsn, 3)
    try:
        await _expired_reservation(observer)
        second = uuid.uuid4()
        await holder.execute('BEGIN')
        assert await _row(holder, _CLEANUP_SQL, ()) == (1,)
        pending = asyncio.create_task(_claim(caller, COMMAND_A, second))
        await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid')
        await holder.execute(holder_end)
        assert await pending == ('APPLIED', second)
        assert await _reservations(observer) == [(second, 'LIVE')]
    finally:
        await _close_all([holder, caller, observer])


@pytest.mark.asyncio(loop_scope='function')
async def test_two_claims_of_a_key_with_an_expired_reservation_apply_once(baseline_dsn: str) -> None:
    """The holder's claim deletes the expired reservation and inserts its own; the caller's delete waits on it.
    After the commit the caller's insert does nothing and the caller replays the holder's reservation."""
    holder, caller, observer = await _connect_many(baseline_dsn, 3)
    try:
        await _expired_reservation(observer)
        first, second = uuid.uuid4(), uuid.uuid4()
        async with holder.transaction():
            assert await _claim(holder, COMMAND_A, first) == ('APPLIED', first)
            pending = asyncio.create_task(_claim(caller, COMMAND_A, second))
            await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid')
        assert await pending == ('REPLAY', first)
        assert await _reservations(observer) == [(first, 'LIVE')]
    finally:
        await _close_all([holder, caller, observer])


@pytest.mark.asyncio(loop_scope='function')
async def test_a_reservation_that_is_expired_at_the_second_read_is_reused(baseline_dsn: str) -> None:
    """The reservation that stopped the caller's insert is expired when the caller reads again: the next round
    deletes it and inserts the caller's reservation."""
    holder, caller, observer = await _connect_many(baseline_dsn, 3)
    try:
        first, second = uuid.uuid4(), uuid.uuid4()
        async with holder.transaction():
            assert await _claim(holder, COMMAND_A, first) == ('APPLIED', first)
            assert await _row(holder, _TERMINALIZE_IN_THE_PAST_SQL, (KEY, first)) == (True,)
            pending = asyncio.create_task(_claim(caller, COMMAND_B, second))
            await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid')
        assert await pending == ('APPLIED', second)
        assert await _reservations(observer) == [(second, 'LIVE')]
    finally:
        await _close_all([holder, caller, observer])


@pytest.mark.asyncio(loop_scope='function')
async def test_a_claim_whose_reservation_changes_in_every_round_raises_40001(baseline_dsn: str) -> None:
    """In each round a reservation commits between the caller's read and its insert, and is gone before the next
    read. After ``CLAIM_ROUNDS`` rounds the claim raises serialization_failure and writes nothing."""
    setup, gate, locker, caller, observer = await _connect_many(baseline_dsn, 5)
    try:
        await setup.execute(_PAUSE_TRIGGER_SQL)
        await gate.execute('SELECT pg_advisory_lock(%s)', (PAUSE_LOCK_KEY,))
        await caller.execute("SELECT set_config('horsies_test.pause_lock_key', %s, FALSE)", (str(PAUSE_LOCK_KEY),))
        pending = asyncio.create_task(_claim(caller, COMMAND_A, uuid.uuid4()))
        owners: list[uuid.UUID] = []
        for round_number in range(1, CLAIM_ROUNDS + 1):
            await _wait_for_lock(observer, caller.info.backend_pid, 'advisory')
            owner = uuid.uuid4()
            owners.append(owner)
            assert await _claim(setup, COMMAND_A, owner) == ('APPLIED', owner)
            await locker.execute('BEGIN')
            await _row(locker, _LOCK_RESERVATION_SQL, (KEY,))
            await gate.execute('SELECT pg_advisory_unlock(%s)', (PAUSE_LOCK_KEY,))
            if round_number == CLAIM_ROUNDS:
                break
            # The insert did nothing; the next round's read waits on the locker's row lock.
            await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid')
            await gate.execute('SELECT pg_advisory_lock(%s)', (PAUSE_LOCK_KEY,))
            assert await _row(locker, _TERMINALIZE_IN_THE_PAST_SQL, (KEY, owner)) == (True,)
            assert await _row(locker, _CLEANUP_SQL, ()) == (1,)
            await locker.execute('COMMIT')
        with pytest.raises(psycopg.errors.SerializationFailure):
            await pending
        await locker.execute('ROLLBACK')
        assert await _reservations(observer) == [(owners[-1], 'LIVE')]
    finally:
        await _close_all([setup, gate, locker, caller, observer])


@pytest.mark.asyncio(loop_scope='function')
async def test_under_repeatable_read_a_claim_that_waits_on_a_first_claim_raises_40001(
    baseline_dsn: str,
) -> None:
    """The reservation committed after the caller's snapshot: PostgreSQL's ON CONFLICT rule raises
    serialization_failure under REPEATABLE READ, not unique_violation."""
    holder, caller, observer = await _connect_many(baseline_dsn, 3)
    try:
        first = uuid.uuid4()
        await caller.execute('BEGIN ISOLATION LEVEL REPEATABLE READ')
        async with holder.transaction():
            assert await _claim(holder, COMMAND_A, first) == ('APPLIED', first)
            pending = asyncio.create_task(_claim(caller, COMMAND_A, uuid.uuid4()))
            await _wait_for_lock(observer, caller.info.backend_pid, 'transactionid')
        with pytest.raises(psycopg.errors.SerializationFailure):
            await pending
        await caller.execute('ROLLBACK')
        assert await _reservations(observer) == [(first, 'LIVE')]
    finally:
        await _close_all([holder, caller, observer])


# ---------------------------------------------------------------------------
# Concurrent sends (identity prototype tests: one applied, the rest classified against it)
# ---------------------------------------------------------------------------


async def _concurrent_sends(dsn: str, commands: list[bytes]) -> list[tuple[uuid.UUID, str, uuid.UUID]]:
    """Send ``commands[i]`` under ``KEY`` with a new task id each, all at once; return (sent id, outcome, id)."""
    gate, observer, *senders = await _connect_many(dsn, 2 + len(commands))
    try:
        task_ids = [uuid.uuid4() for _ in commands]
        async with gate.transaction():
            await gate.execute('LOCK TABLE horsies_key_reservations IN EXCLUSIVE MODE')
            pending = [
                asyncio.create_task(_row(sender, _SEND_SQL, (task_id, command, KEY)))
                for sender, task_id, command in zip(senders, task_ids, commands, strict=True)
            ]
            pids = [sender.info.backend_pid for sender in senders]
            for _ in range(WAIT_POLL_LIMIT):
                if await _row(observer, _WAITING_COUNT_SQL, (pids,)) == (len(senders),):
                    break
                await asyncio.sleep(WAIT_POLL_INTERVAL_S)
            else:
                pytest.fail('the sends never all waited on the reservation table lock')
        rows = await asyncio.gather(*pending)
        return [(task_id, str(row[0]), row[1]) for task_id, row in zip(task_ids, rows, strict=True)]
    finally:
        await _close_all([gate, observer, *senders])


def _the_applied_send(outcomes: list[tuple[uuid.UUID, str, uuid.UUID]]) -> int:
    applied = [index for index, (_, outcome, _) in enumerate(outcomes) if outcome == 'INSERTED']
    assert len(applied) == 1, outcomes
    return applied[0]


async def _live_rows(dsn: str) -> tuple[int, list[tuple[uuid.UUID, str]]]:
    (connection,) = await _connect_many(dsn, 1)
    try:
        tasks = await _row(connection, 'SELECT count(*) FROM horsies_tasks', ())
        return int(tasks[0]), await _reservations(connection)
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_concurrent_sends_of_one_command_under_one_key_apply_once(baseline_dsn: str) -> None:
    outcomes = await _concurrent_sends(baseline_dsn, [COMMAND_A] * CONCURRENT_SENDS)

    winner = outcomes[_the_applied_send(outcomes)][0]
    replays = [(outcome, owner) for task_id, outcome, owner in outcomes if task_id != winner]
    assert replays == [('KEY_REPLAY', winner)] * (CONCURRENT_SENDS - 1)
    assert await _live_rows(baseline_dsn) == (1, [(winner, 'LIVE')])


@pytest.mark.asyncio(loop_scope='function')
async def test_concurrent_sends_of_two_commands_are_classified_against_the_applied_one(
    baseline_dsn: str,
) -> None:
    commands = [COMMAND_A if index % 2 == 0 else COMMAND_B for index in range(CONCURRENT_SENDS)]
    outcomes = await _concurrent_sends(baseline_dsn, commands)

    applied = _the_applied_send(outcomes)
    winner = outcomes[applied][0]
    for index, (task_id, outcome, owner) in enumerate(outcomes):
        match index == applied, commands[index] == commands[applied]:
            case True, _:
                assert (outcome, owner) == ('INSERTED', task_id)
            case False, True:
                assert (outcome, owner) == ('KEY_REPLAY', winner), index
            case False, False:
                assert (outcome, owner) == ('KEY_CONFLICT', winner), index
    assert await _live_rows(baseline_dsn) == (1, [(winner, 'LIVE')])
