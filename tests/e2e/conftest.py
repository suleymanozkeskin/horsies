"""E2E test fixtures and shared helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Generator
import pytest
import pytest_asyncio

from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.app import Horsies

from tests.e2e.helpers.db import cleanup_tables
from tests.e2e.helpers.worker import kill_stale_workers
from tests.e2e.tasks import instance as default_instance
from tests.e2e.tasks import instance_custom
from tests.e2e.tasks import instance_cluster_cap
from tests.e2e.tasks import instance_recovery
from tests.e2e.tasks import instance_softcap
from tests.e2e.tasks import instance_requeue_guard
from tests.e2e.tasks import instance_scheduler
from tests.e2e.tasks import instance_retry_precedence
from tests.e2e.tasks import instance_priority_binding


from tests.e2e.helpers.env import e2e_database_url

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')


async def _initialize_session_broker(
    brk: PostgresBroker,
    *,
    seed_history: bool = False,
) -> None:
    """Initialize and bind listener ownership to the session loop.

    Sync facades subsequently fall back to polling instead of claiming the
    shared listener from their LoopRunner. Session-loop listener tests then
    receive the same uncontaminated owner regardless of collection order.
    """
    schema = await brk.ensure_schema_initialized()
    assert schema.is_ok(), schema
    listener = await brk.listener.start()
    assert listener.is_ok(), listener
    if seed_history:
        from tests.integration.history_seeding import ensure_history_seedable

        async with brk.async_engine.begin() as connection:
            await ensure_history_seedable(connection)


@pytest_asyncio.fixture(scope='session', loop_scope='session', autouse=True)
async def broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker used by e2e tasks, bound before any sync test can claim it."""
    brk = default_instance.broker
    await _initialize_session_broker(brk, seed_history=True)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        # May fail if broker was used from sync context (LoopRunner)
        # which creates tasks in a different event loop
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def custom_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Custom-queue broker instance for e2e tasks."""
    brk = instance_custom.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        # May fail if broker was used from sync context (LoopRunner)
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def priority_binding_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker for the subworkflow priority-binding claim-order e2e."""
    brk = instance_priority_binding.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def cluster_cap_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with cluster_wide_cap for e2e tasks."""
    brk = instance_cluster_cap.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        # May fail if broker was used from sync context (LoopRunner)
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def recovery_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with fast recovery thresholds for crash-detection tests."""
    brk = instance_recovery.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def softcap_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with soft cap (prefetch_buffer > 0) for lease contention tests."""
    brk = instance_softcap.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def requeue_guard_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker for requeue-guard age-guard e2e tests."""
    brk = instance_requeue_guard.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def scheduler_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance for scheduler e2e tests."""
    brk = instance_scheduler.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def retry_precedence_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance for retry-precedence e2e tests."""
    brk = instance_retry_precedence.broker
    await _initialize_session_broker(brk)
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session', autouse=True)
async def bind_all_session_brokers(
    broker: PostgresBroker,
    custom_broker: PostgresBroker,
    priority_binding_broker: PostgresBroker,
    cluster_cap_broker: PostgresBroker,
    recovery_broker: PostgresBroker,
    softcap_broker: PostgresBroker,
    requeue_guard_broker: PostgresBroker,
    scheduler_broker: PostgresBroker,
    retry_precedence_broker: PostgresBroker,
) -> AsyncGenerator[None, None]:
    """Bind every singleton broker before any sync facade can claim it."""
    _ = (
        broker,
        custom_broker,
        priority_binding_broker,
        cluster_cap_broker,
        recovery_broker,
        softcap_broker,
        requeue_guard_broker,
        scheduler_broker,
        retry_precedence_broker,
    )
    yield


@pytest_asyncio.fixture(loop_scope='session')
async def session(broker: PostgresBroker) -> AsyncGenerator[AsyncSession, None]:
    """Database session for e2e assertions."""
    async with broker.session_factory() as sess:
        yield sess


@pytest.fixture
def app() -> Horsies:
    """App instance used by e2e tasks."""
    return default_instance.app


@pytest_asyncio.fixture(autouse=True, loop_scope='session')
async def clean_db(request: pytest.FixtureRequest) -> AsyncGenerator[None, None]:
    """Auto-cleanup before and after each e2e test."""
    node: Any = getattr(request, 'node')
    if node.get_closest_marker('pgbouncer') is not None:
        yield
        return

    schema = await default_instance.broker.ensure_schema_initialized()
    assert schema.is_ok(), schema
    async with default_instance.broker.session_factory() as session:
        await cleanup_tables(session)
    try:
        yield
    finally:
        async with default_instance.broker.session_factory() as session:
            await cleanup_tables(session)


@pytest.fixture(scope='session', autouse=True)
def clean_stale_workers_session() -> Generator[None, None, None]:
    """Reap leftover worker processes at session boundaries."""
    kill_stale_workers()
    try:
        yield
    finally:
        kill_stale_workers()


# =============================================================================
# Enqueue SHA helper
# =============================================================================


def compute_test_enqueue_sha(
    task_name: str,
    queue_name: str = 'default',
    priority: int = 100,
    args_json: str = '[]',
    kwargs_json: str = '{}',
    sent_at: datetime | None = None,
    good_until: datetime | None = None,
    task_options: str | None = None,
) -> tuple[datetime, str]:
    """Compute (sent_at, enqueue_sha) pair for test task insertion.

    Returns a consistent pair: the sent_at used in the hash matches
    the one to be stored in the row.
    """
    from horsies.core.utils.fingerprint import enqueue_fingerprint

    if sent_at is None:
        sent_at = datetime.now(timezone.utc)
    sha = enqueue_fingerprint(
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        args_json=args_json,
        kwargs_json=kwargs_json,
        sent_at=sent_at,
        good_until=good_until,
        enqueue_delay_seconds=None,
        task_options=task_options,
    )
    return sent_at, sha
