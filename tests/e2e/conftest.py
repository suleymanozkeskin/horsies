"""E2E test fixtures and shared helpers."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Generator

import pytest
import pytest_asyncio

from tests.session_database import suite_database_url

DB_URL = suite_database_url()
"""The one database of this e2e process and of every subprocess it starts.

Bound here, before the task modules below are imported. A task module reads
``HORSES_E2E_DB_URL`` at import and builds its broker from it, and every
scheduler or worker subprocess inherits the variable. ``suite_database_url``
creates the session database and writes its URL to that variable, so the
in-process brokers and the subprocesses get the same URL. pytest imports this
conftest before ``pytest_configure`` when the run names a path under
``tests/e2e``, so the database cannot wait for that hook.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.app import Horsies
from horsies.core.types.result import is_ok

from tests.e2e.helpers.db import cleanup_tables
from tests.e2e.helpers.env import (
    TASK_MODULE_URL_ENV_NAME,
    database_url_mismatches,
    e2e_database_url_in,
)
from tests.e2e.helpers.session_broker import initialize_session_broker
from tests.e2e.helpers.worker import kill_stale_workers, scheduler_env, worker_env
from tests.e2e.tasks import instance as default_instance
from tests.e2e.tasks import instance_child_isolation
from tests.e2e.tasks import instance_core_stopped_child
from tests.e2e.tasks import instance_child_isolation_prefetch
from tests.e2e.tasks import instance_custom
from tests.e2e.tasks import instance_cluster_cap
from tests.e2e.tasks import instance_recovery
from tests.e2e.tasks import instance_left_work_marker
from tests.e2e.tasks import instance_softcap
from tests.e2e.tasks import instance_requeue_guard
from tests.e2e.tasks import instance_scheduler
from tests.e2e.tasks import instance_retry_precedence
from tests.e2e.tasks import instance_priority_binding


from tests.broker_resources_support import current_resources

PGBOUNCER_TOPOLOGY_MODULES = frozenset({'tests.e2e.tasks.instance_pgbouncer'})
"""Task modules whose URLs come from the PgBouncer fixture variables, not
from the session database."""


def _observed_database_urls() -> dict[str, str]:
    """The URL of each source that must use ``DB_URL``: every loaded e2e
    module with a module-level ``DB_URL``, and the URL that a scheduler or a
    worker subprocess resolves from the environment these helpers give it."""
    observed = {
        'scheduler subprocess': e2e_database_url_in(scheduler_env(), TASK_MODULE_URL_ENV_NAME),
        'worker subprocess': e2e_database_url_in(worker_env(), TASK_MODULE_URL_ENV_NAME),
    }
    for name, module in tuple(sys.modules.items()):
        if not name.startswith('tests.e2e.') or name in PGBOUNCER_TOPOLOGY_MODULES:
            continue
        url = getattr(module, 'DB_URL', None)
        if isinstance(url, str):
            observed[name] = url
    return observed


@pytest.fixture(scope='session', autouse=True)
def one_database_for_test_process_and_subprocesses() -> None:
    """Fail the session when a source would use another database than
    ``DB_URL``."""
    mismatches = database_url_mismatches(DB_URL, _observed_database_urls())
    assert mismatches == [], (
        'e2e sources resolve another database than the session database:\n'
        + '\n'.join(mismatches)
    )


@pytest_asyncio.fixture(scope='session', loop_scope='session', autouse=True)
async def broker(
    one_database_for_test_process_and_subprocesses: None,  # noqa: ARG001 - checked before any broker connects
) -> AsyncGenerator[PostgresBroker, None]:
    """Broker used by e2e tasks, bound before any sync test can claim it."""
    brk = default_instance.broker
    await initialize_session_broker(brk, seed_history=True)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def custom_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Custom-queue broker instance for e2e tasks."""
    brk = instance_custom.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def priority_binding_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker for the subworkflow priority-binding claim-order e2e."""
    brk = instance_priority_binding.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def cluster_cap_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with cluster_wide_cap for e2e tasks."""
    brk = instance_cluster_cap.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def recovery_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with fast recovery thresholds for crash-detection tests."""
    brk = instance_recovery.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def left_work_marker_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with small recovery thresholds for the finalizing
    marker of an isolated async task that leaves work."""
    brk = instance_left_work_marker.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def core_stopped_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance whose tasks stop the core of their isolated child."""
    brk = instance_core_stopped_child.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def softcap_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance with soft cap (prefetch_buffer > 0) for lease contention tests."""
    brk = instance_softcap.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def isolation_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker for child isolation e2e tests."""
    brk = instance_child_isolation.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def isolation_prefetch_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker for child isolation e2e tests with a prefetch buffer."""
    brk = instance_child_isolation_prefetch.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def requeue_guard_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker for requeue-guard age-guard e2e tests."""
    brk = instance_requeue_guard.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def scheduler_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance for scheduler e2e tests."""
    brk = instance_scheduler.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def retry_precedence_broker() -> AsyncGenerator[PostgresBroker, None]:
    """Broker instance for retry-precedence e2e tests."""
    brk = instance_retry_precedence.broker
    await initialize_session_broker(brk)
    yield brk
    closed = await brk.close_async()
    assert is_ok(closed), closed


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
    async with current_resources(broker).session_factory() as sess:
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
    async with current_resources(default_instance.broker).session_factory() as session:
        await cleanup_tables(session)
    try:
        yield
    finally:
        async with current_resources(default_instance.broker).session_factory() as session:
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
# Send identity helper
# =============================================================================


def compute_test_command_fingerprint(
    task_name: str,
    queue_name: str = 'default',
    priority: int = 100,
    input_json: str = '{}',
    sent_at: datetime | None = None,
    good_until: datetime | None = None,
    task_options: str | None = None,
    retention_class_key: str = 'standard_30d',
    retain_rerun_input: bool = False,
) -> tuple[datetime, bytes]:
    """Compute (sent_at, command_fingerprint) for test task insertion.

    The fingerprint is the version-1 command fingerprint that a send of
    these values stores, with the stored ``input`` text in its kwargs
    element. The returned sent_at is the one to store in the row.
    """
    from horsies.core.history.identity.fingerprint import EnqueueCommandV1

    if sent_at is None:
        sent_at = datetime.now(timezone.utc)
    fingerprint = EnqueueCommandV1(
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        positional_args_json=None,
        input_json=input_json,
        good_until=good_until,
        enqueue_delay_seconds=None,
        task_options_json=task_options,
        retention_class_key=retention_class_key,
        retain_rerun_input=retain_rerun_input,
        rerun_of_task_id=None,
        rerun_root_task_id=None,
    ).fingerprint
    return sent_at, fingerprint
