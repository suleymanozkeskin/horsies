"""E2E test fixtures and shared helpers."""

from __future__ import annotations

from typing import AsyncGenerator, Generator
import os
import pytest
import pytest_asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.app import Horsies

from tests.e2e.helpers.worker import kill_stale_workers
from tests.e2e.tasks import instance as default_instance
from tests.e2e.tasks import instance_custom
from tests.e2e.tasks import instance_cluster_cap
from tests.e2e.tasks import instance_recovery
from tests.e2e.tasks import instance_softcap
from tests.e2e.tasks import instance_requeue_guard
from tests.e2e.tasks import instance_scheduler
from tests.e2e.tasks import instance_retry_precedence


DB_URL = os.environ.get(
    'HORSES_E2E_DB_URL',
    f'postgresql+psycopg://postgres:{os.environ["DB_PASSWORD"]}@localhost:5432/horsies',
)


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance used by e2e tasks."""
    brk = default_instance.broker
    await brk.ensure_schema_initialized()
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
    await brk.ensure_schema_initialized()
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        # May fail if broker was used from sync context (LoopRunner)
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def cluster_cap_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with cluster_wide_cap for e2e tasks."""
    brk = instance_cluster_cap.broker
    await brk.ensure_schema_initialized()
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
    await brk.ensure_schema_initialized()
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def softcap_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with soft cap (prefetch_buffer > 0) for lease contention tests."""
    brk = instance_softcap.broker
    await brk.ensure_schema_initialized()
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def requeue_guard_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker for requeue-guard age-guard e2e tests."""
    brk = instance_requeue_guard.broker
    await brk.ensure_schema_initialized()
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def scheduler_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance for scheduler e2e tests."""
    brk = instance_scheduler.broker
    await brk.ensure_schema_initialized()
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def retry_precedence_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance for retry-precedence e2e tests."""
    brk = instance_retry_precedence.broker
    await brk.ensure_schema_initialized()
    yield brk
    try:
        await brk.close_async()
    except RuntimeError:
        pass


@pytest_asyncio.fixture(loop_scope='session')
async def session(broker: PostgresBroker) -> AsyncGenerator[AsyncSession, None]:
    """Database session for e2e assertions."""
    async with broker.session_factory() as sess:
        yield sess


@pytest.fixture
def app() -> Horsies:
    """App instance used by e2e tasks."""
    return default_instance.app


async def _cleanup_tables(session: AsyncSession) -> None:
    """Truncate tables between tests."""
    await session.execute(
        text("""
            TRUNCATE horsies_tasks, horsies_workflow_tasks, horsies_workflows, horsies_schedule_state, horsies_heartbeats CASCADE
        """),
    )
    await session.commit()


@pytest_asyncio.fixture(autouse=True, loop_scope='session')
async def clean_db(session: AsyncSession) -> AsyncGenerator[None, None]:
    """Auto-cleanup before and after each e2e test."""
    await _cleanup_tables(session)
    try:
        yield
    finally:
        await _cleanup_tables(session)


@pytest.fixture(scope='session', autouse=True)
def clean_stale_workers_session() -> Generator[None, None, None]:
    """Reap leftover worker processes at session boundaries."""
    kill_stale_workers()
    try:
        yield
    finally:
        kill_stale_workers()
