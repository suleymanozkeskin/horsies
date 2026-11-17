"""Round trips of the one-statement writes, counted at the wire.

Each test drives the real entry point through a ``RoundTripProxy`` and
counts client turns in the steady state of psycopg's prepared statements
(``WARM_RUNS``). The worker's engine is the one the CLI builds
(``worker_process_config``): ``worker_pool_pre_ping`` is off unless a test
turns it on.

A one-statement write is one round trip: no pre-ping, no BEGIN, no COMMIT.
Every write of a child goes through the core handle of the child
(``proxied_child_core``): one statement for the start, one for the path of a
miss, and one for the finalizing marker.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Callable, Generator
from datetime import datetime, timezone
from typing import Any, Final
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Ok
from horsies.core.worker.child_runner import (
    StartAllowed,
    StartCheck,
    StartWithin,
    _confirm_until_not_resumed,
    mark_run_finalizing,
)
from horsies.core.worker.claiming import ClaimPass
from horsies.core.worker.run_return import StoppedBeforeRunning
from horsies.core.worker.shared_lifecycle import (
    confirm_until_not_resumed,
    mark_task_finalizing,
)
from tests.dispatch_claim_support import claim_at
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
    steady_state_turns,
    upstream_of,
)
from tests.fixtures.child_core import child_core_handle_for
from tests.integration.round_trip_proxy import RoundTripProxy
from tests.integration.test_workflow_node_handoff import (
    WORKER_ID as NODE_WORKER_ID,
    _seed_claimed_workflow_task,
    _seed_running_workflow_with_node,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

ONE_STATEMENT: Final = 1
HELD_FOR_THE_TEST_S: Final = 600.0
"""The idle time of the held connections in a test that measures them held."""

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
    await opened.close()


@pytest.fixture
def proxied_child_core(proxy: RoundTripProxy, db_url: str) -> Generator[None]:
    """The core handle of a child, on the proxy. The handle of the suite
    comes back after the test."""
    with child_core_handle_for(proxied_url(db_url, proxy)):
        yield


def _allowed() -> StartCheck:
    return StartAllowed()


def _within_a_minute() -> StartCheck:
    return StartWithin(milliseconds=60_000)


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


async def _claimed_workflow_task(session: AsyncSession) -> Case:
    """A CLAIMED workflow task of NODE_WORKER_ID whose node waits in a
    RUNNING workflow, committed."""
    claimed_at = datetime.now(timezone.utc)
    task_id = await _seed_claimed_workflow_task(session, claimed_at)
    await _seed_running_workflow_with_node(session, task_id, 'ENQUEUED')
    await session.commit()
    return task_id, claimed_at


# ---------------------------------------------------------------------------
# Worker process
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('pre_ping', [False, True], ids=['pre_ping_off', 'pre_ping_on'])
async def test_a_plain_success_finalize_is_one_statement(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    pre_ping: bool,
) -> None:
    """The fused completion is its own transaction, sent by the core on its
    pool through the worker's core handle. The core pings a pooled
    connection only after it was idle for ``DEFAULT_IDLE_PING_AFTER`` (with
    ``worker_pool_pre_ping`` on), so in steady state the completion is one
    round trip, also with pre-ping on. The claim loop's held connection
    keeps its connection for the whole test."""
    opened = await open_proxied_worker(proxied_url(db_url, proxy), pre_ping=pre_ping)
    for held in opened.worker._held_connections:  # pyright: ignore[reportPrivateUsage]
        held._idle_release_s = HELD_FOR_THE_TEST_S  # pyright: ignore[reportPrivateUsage]

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
            claim=claim_at(claimed_at),
        )
        assert result == Ok(None)

    try:
        (task_id, _), turns = await steady_state_turns(proxy, arrange, act)
    finally:
        await opened.close()

    assert len(turns) == ONE_STATEMENT, turns
    assert await _row(session, task_id) is None
    assert await _history_rows(session, task_id) == 1


async def test_a_workflow_check_failure_finalize_sends_nothing(
    proxy: RoundTripProxy,
    proxied: ProxiedWorker,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A claimed workflow task with no node, reported WORKFLOW_CHECK_FAILED
    (the orphan self-heal of the run was off), is left CLAIMED: the worker
    sends no statement. With the self-heal on, the child's stop before start
    cancels the orphan
    (``test_a_stop_before_start_is_one_statement_after_the_start``)."""

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
            claim=claim_at(claimed_at),
        )
        assert result == Ok(None)

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert turns == (), turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'CLAIMED'


async def test_a_stop_before_start_is_one_statement_after_the_start(
    proxy: RoundTripProxy,
    proxied_child_core: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A claimed workflow task with no node: the start misses with
    NODE_NOT_LINKED, and the stop before start cancels the orphan
    (``horsies_workflow_task_stop_before_start``, ORPHAN_CANCELLED): two
    statements in all, each its own transaction."""

    async def arrange() -> Case:
        return await insert_task(session, status='CLAIMED', is_workflow_task=True)

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        stopped = _confirm_until_not_resumed(
            task_id,
            WORKER_ID,
            claimed_at,
            orphan_self_heal=True,
        )
        assert stopped == StoppedBeforeRunning('ORPHAN_CANCELLED')

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == 2 * ONE_STATEMENT, turns
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
    proxied_child_core: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """A plain task's RUNNING update, with its first beat, is one round trip
    through the core handle of the child."""

    async def arrange() -> Case:
        return await insert_task(session, status='CLAIMED')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        started = await confirm_until_not_resumed(
            task_id,
            WORKER_ID,
            claimed_at,
            orphan_self_heal=True,
        )
        assert started is None

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'


async def test_a_shared_async_finalizing_marker_is_one_statement(
    proxy: RoundTripProxy,
    proxied_child_core: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The marker of a seat is one round trip through the awaitable method
    of the core handle of the child."""

    async def arrange() -> Case:
        return await insert_task(session, status='RUNNING')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        await mark_task_finalizing(uuid.UUID(task_id), WORKER_ID, claimed_at)

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.finalizing_at is not None


async def test_a_shared_async_workflow_start_is_one_statement(
    proxy: RoundTripProxy,
    proxied_child_core: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The RUNNING write of a workflow task and its node write are one
    horsies_start_attempt statement through the core handle of the child,
    once the handle's connection is set up (``steady_state_turns``)."""

    async def arrange() -> Case:
        return await _claimed_workflow_task(session)

    async def act(case: Case) -> None:
        started = await confirm_until_not_resumed(
            case[0],
            NODE_WORKER_ID,
            case[1],
            orphan_self_heal=True,
        )
        assert started is None

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'


# ---------------------------------------------------------------------------
# Isolated and shared thread children (the sync calls of the core handle)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'start_check',
    [_allowed, _within_a_minute],
    ids=['isolated', 'shared_thread'],
)
async def test_a_sync_running_update_is_one_statement(
    proxy: RoundTripProxy,
    proxied_child_core: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    start_check: Callable[[], StartCheck],
) -> None:
    async def arrange() -> Case:
        return await insert_task(session, status='CLAIMED')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        started = _confirm_until_not_resumed(
            task_id,
            WORKER_ID,
            claimed_at,
            orphan_self_heal=True,
            start_check=start_check,
        )
        assert started is None

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'


async def test_a_sync_finalizing_marker_is_one_statement(
    proxy: RoundTripProxy,
    proxied_child_core: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The marker of an isolated run or of a thread seat, task or refusal
    alike, is one round trip through the core handle of the child."""

    async def arrange() -> Case:
        return await insert_task(session, status='RUNNING')

    async def act(case: Case) -> None:
        task_id, claimed_at = case
        mark_run_finalizing(uuid.UUID(task_id), WORKER_ID, claimed_at)

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.finalizing_at is not None


async def test_a_sync_workflow_start_is_one_statement(
    proxy: RoundTripProxy,
    proxied_child_core: None,  # noqa: ARG001
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """The sync form of the start of a workflow task: one statement once
    the handle's connection is set up (``steady_state_turns``)."""

    async def arrange() -> Case:
        return await _claimed_workflow_task(session)

    async def act(case: Case) -> None:
        started = _confirm_until_not_resumed(
            case[0],
            NODE_WORKER_ID,
            case[1],
            orphan_self_heal=True,
        )
        assert started is None

    (task_id, _), turns = await steady_state_turns(proxy, arrange, act)

    assert len(turns) == ONE_STATEMENT, turns
    row = await _row(session, task_id)
    assert row is not None and row.status == 'RUNNING'
