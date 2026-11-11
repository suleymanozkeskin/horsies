"""Round trips of the one-statement writes, counted at the wire.

Each test drives the real entry point through a ``RoundTripProxy`` and
counts client turns in the steady state of psycopg's prepared statements
(``WARM_RUNS``). The worker's engine is the one the CLI builds
(``worker_process_config``): ``worker_pool_pre_ping`` is off unless a test
turns it on. The child pools are the pools a child opens: autocommit, with
the checkout check off.

A one-statement write is one round trip: no pre-ping, no BEGIN, no COMMIT.
A write that needs several statements to commit together (the RUNNING
transition of a workflow task) keeps its transaction.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timezone
from typing import Any, Final
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Ok
from horsies.core.worker.child_pool import (
    _cleanup_worker_pool,
    _initialize_worker_pool,
    open_shared_async_pool,
)
from horsies.core.worker.child_runner import (
    StartAllowed,
    StartCheck,
    StatementsBound,
    StatementsLimit,
    StatementsUnbounded,
    _confirm_until_not_resumed,
    _mark_task_finalizing,
)
from horsies.core.worker.claiming import ClaimPass
from horsies.core.worker.shared_lifecycle import (
    confirm_until_not_resumed,
    mark_task_finalizing,
)
from tests.integration.autocommit_support import (
    QUEUE,
    TASK_NAME,
    WARM_RUNS,
    WORKER_ID,
    ProxiedWorker,
    insert_task,
    ok_result_json,
    open_proxied_worker,
    proxied_url,
    psycopg_url,
    recorded_turns,
    steady_state_turns,
    upstream_of,
)
from tests.integration.round_trip_proxy import RoundTripProxy, Turn
from tests.integration.test_workflow_node_handoff import (
    WORKER_ID as NODE_WORKER_ID,
    _seed_claimed_workflow_task,
    _seed_running_workflow_with_node,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

ONE_STATEMENT: Final = 1
STATEMENT_AFTER_PING: Final = 2
WORKFLOW_START: Final = ('BEGIN', 'COMMIT')

type Case = tuple[str, datetime]


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


@pytest_asyncio.fixture(loop_scope='function')
async def proxied(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001 - the schema and history leaves
) -> AsyncGenerator[ProxiedWorker]:
    opened = await open_proxied_worker(proxied_url(db_url, proxy))
    yield opened
    _ = await opened.broker.close_async()


@pytest_asyncio.fixture(loop_scope='function')
async def async_child_pool(
    proxy: RoundTripProxy,
    db_url: str,
) -> AsyncGenerator[AsyncConnectionPool[AsyncConnection[Any]]]:
    pool = await open_shared_async_pool(
        psycopg_url(proxied_url(db_url, proxy)),
        connect_kwargs={},
        min_size=0,
        max_size=2,
        check_on_checkout=False,
    )
    yield pool
    await pool.close(timeout=5.0)


@pytest.fixture
def sync_child_pool(proxy: RoundTripProxy, db_url: str) -> Generator[None]:
    """The per-process sync pool of an isolated or shared thread child."""
    _cleanup_worker_pool()
    _initialize_worker_pool(psycopg_url(proxied_url(db_url, proxy)))
    yield
    _cleanup_worker_pool()


def _allowed() -> StartCheck:
    return StartAllowed()


SYNC_CHILD_KINDS: Final = pytest.mark.parametrize(
    'statements',
    [StatementsUnbounded(), StatementsBound(seconds=5.0)],
    ids=['isolated', 'shared_thread_refusal'],
)


async def _row(session: AsyncSession, task_id: str) -> Any:
    row = (
        await session.execute(
            text("""
                SELECT status, finalizing_at FROM horsies_tasks
                WHERE id = CAST(:id AS uuid)
            """),
            {'id': task_id},
        )
    ).one_or_none()
    await session.commit()
    return row


async def _history_status(session: AsyncSession, task_id: str) -> str:
    status = await session.scalar(
        text(
            'SELECT status FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)'
        ),
        {'id': task_id},
    )
    await session.commit()
    assert isinstance(status, str)
    return status


async def _history_rows(session: AsyncSession, task_id: str) -> int:
    count = await session.scalar(
        text(
            'SELECT count(*) FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)'
        ),
        {'id': task_id},
    )
    await session.commit()
    assert isinstance(count, int)
    return count


def _simple_queries(turns: tuple[Turn, ...]) -> list[str]:
    return [
        message.removeprefix('Q:')
        for turn in turns
        for message in turn.messages
        if message.startswith('Q:')
    ]


# ---------------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('pre_ping', 'round_trips'),
    [(False, ONE_STATEMENT), (True, STATEMENT_AFTER_PING)],
    ids=['pre_ping_off', 'pre_ping_on'],
)
async def test_a_plain_success_finalize_is_one_statement(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    pre_ping: bool,
    round_trips: int,
) -> None:
    """The fused completion is its own transaction; the pre-ping is the only
    other round trip, and only when ``worker_pool_pre_ping`` is on."""
    opened = await open_proxied_worker(proxied_url(db_url, proxy), pre_ping=pre_ping)

    async def arrange() -> Case:
        return await insert_task(session, status='RUNNING')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        result = await opened.worker._finalize_task_result(
            task_id,
            True,
            ok_result_json(),
            None,
            queue_name=QUEUE,
            is_workflow_task=False,
            task_name=TASK_NAME,
            claimed_at=claimed_at,
        )
        assert result == Ok(None)

    try:
        (task_id, _), turns = await steady_state_turns(proxy, arrange, act)
    finally:
        _ = await opened.broker.close_async()

    assert len(turns) == round_trips, turns
    assert await _row(session, task_id) is None
    assert await _history_rows(session, task_id) == 1


async def test_an_orphan_cancel_is_one_statement(
    proxy: RoundTripProxy,
    proxied: ProxiedWorker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A claimed workflow task with no node, reported WORKFLOW_CHECK_FAILED,
    is cancelled by one function call in autocommit."""

    async def arrange() -> Case:
        return await insert_task(session, status='CLAIMED', is_workflow_task=True)

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        result = await proxied.worker._finalize_task_result(
            task_id,
            False,
            '',
            'WORKFLOW_CHECK_FAILED',
            queue_name=QUEUE,
            is_workflow_task=True,
            task_name=TASK_NAME,
            claimed_at=claimed_at,
        )
        assert result == Ok(None)

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    assert await _row(session, task_id) is None
    assert await _history_status(session, task_id) == 'CANCELLED'


async def test_a_claim_pass_is_one_statement(
    proxy: RoundTripProxy,
    proxied: ProxiedWorker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """``horsies_claim`` runs as its own transaction: claim and commit in one
    round trip."""
    worker = proxied.worker
    # Each run leaves its row CLAIMED, and the claim counts it against the
    # budget: one slot per run.
    worker._claim_slot_count = lambda: WARM_RUNS  # type: ignore[method-assign]
    worker._registered_classes = {TASK_NAME: Execution.ISOLATED}
    dispatch = AsyncMock()
    worker._dispatch_one = dispatch  # type: ignore[method-assign]

    async def arrange() -> Case:
        return await insert_task(session, status='PENDING')

    async def act(case: Case) -> None:
        del case
        claim_pass = await worker._claim_and_dispatch_all()
        assert claim_pass == ClaimPass(claimed=1, dropped=0, short_of_plan=0)

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    assert dispatch.await_args is not None
    assert dispatch.await_args.args[0] == task_id
    row = await _row(session, task_id)
    assert row is not None and row.status == 'CLAIMED'


async def test_the_delayed_retry_wake_is_one_statement(
    proxy: RoundTripProxy,
    proxied: ProxiedWorker,
) -> None:
    """The NOTIFY that wakes the queue when a retry comes due is sent in
    autocommit."""

    async def arrange() -> str:
        return str(uuid.uuid4())

    async def act(task_id: str) -> None:
        await proxied.worker._schedule_delayed_notification(
            0.0,
            f'task_queue_{QUEUE}',
            f'retry:{task_id}',
        )

    _, turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns


# ---------------------------------------------------------------------------
# Shared async child
# ---------------------------------------------------------------------------


async def test_a_shared_async_running_update_is_one_statement(
    proxy: RoundTripProxy,
    async_child_pool: AsyncConnectionPool[AsyncConnection[Any]],
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A plain task's RUNNING update, with its first beat, is one round trip
    on the autocommit pool."""

    async def arrange() -> Case:
        return await insert_task(session, status='CLAIMED')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        started = await confirm_until_not_resumed(
            async_child_pool,
            task_id,
            WORKER_ID,
            False,
            claimed_at,
        )
        assert started is None

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'


async def test_a_shared_async_finalizing_marker_is_one_statement(
    proxy: RoundTripProxy,
    async_child_pool: AsyncConnectionPool[AsyncConnection[Any]],
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    async def arrange() -> Case:
        return await insert_task(session, status='RUNNING')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        await mark_task_finalizing(async_child_pool, task_id, WORKER_ID, claimed_at)

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.finalizing_at is not None


async def test_a_shared_async_workflow_start_keeps_its_transaction(
    proxy: RoundTripProxy,
    async_child_pool: AsyncConnectionPool[AsyncConnection[Any]],
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The RUNNING update of a workflow task and its node start commit
    together: BEGIN first, COMMIT last."""
    claimed_at = datetime.now(timezone.utc)
    task_id = await _seed_claimed_workflow_task(session, claimed_at)
    await _seed_running_workflow_with_node(session, task_id, 'ENQUEUED')
    await session.commit()

    async def act(case: Case) -> None:
        started = await confirm_until_not_resumed(
            async_child_pool,
            case[0],
            NODE_WORKER_ID,
            True,
            case[1],
        )
        assert started is None

    turns = await recorded_turns(proxy, act, (task_id, claimed_at))

    queries = _simple_queries(turns)
    assert (queries[0], queries[-1]) == WORKFLOW_START, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'


# ---------------------------------------------------------------------------
# Isolated and shared thread children (the sync pool)
# ---------------------------------------------------------------------------


@SYNC_CHILD_KINDS
async def test_a_sync_running_update_is_one_statement(
    proxy: RoundTripProxy,
    sync_child_pool: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    statements: StatementsLimit,
) -> None:
    async def arrange() -> Case:
        return await insert_task(session, status='CLAIMED')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        started = _confirm_until_not_resumed(
            task_id,
            WORKER_ID,
            False,
            claimed_at,
            start_check=_allowed,
            statements=statements,
        )
        assert started is None

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'


@SYNC_CHILD_KINDS
async def test_a_sync_finalizing_marker_is_one_statement(
    proxy: RoundTripProxy,
    sync_child_pool: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    statements: StatementsLimit,
) -> None:
    async def arrange() -> Case:
        return await insert_task(session, status='RUNNING')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        _mark_task_finalizing(task_id, WORKER_ID, claimed_at, statements=statements)

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.finalizing_at is not None


async def test_a_sync_workflow_start_keeps_its_transaction(
    proxy: RoundTripProxy,
    sync_child_pool: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    claimed_at = datetime.now(timezone.utc)
    task_id = await _seed_claimed_workflow_task(session, claimed_at)
    await _seed_running_workflow_with_node(session, task_id, 'ENQUEUED')
    await session.commit()

    async def act(case: Case) -> None:
        started = _confirm_until_not_resumed(case[0], NODE_WORKER_ID, True, case[1])
        assert started is None

    turns = await recorded_turns(proxy, act, (task_id, claimed_at))

    queries = _simple_queries(turns)
    assert (queries[0], queries[-1]) == WORKFLOW_START, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'
