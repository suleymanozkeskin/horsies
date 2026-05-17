'''PgBouncer e2e smoke tests.'''

from __future__ import annotations

import importlib
import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import pytest

from tests.e2e.helpers.assertions import assert_ok, start_ok_sync, unwrap_send
from tests.e2e.helpers.worker import run_worker
from tests.pgbouncer_utils import isolated_pgbouncer_database, skip_if_pgbouncer_disabled


skip_if_pgbouncer_disabled()

pytestmark = [pytest.mark.e2e, pytest.mark.pgbouncer]

PGB_INSTANCE = "tests.e2e.tasks.instance_pgbouncer:app"


@contextmanager
def _pgbouncer_env(
    direct_url: str,
    transaction_url: str,
) -> Generator[None, None, None]:
    old_direct = os.environ.get("HORSIES_TEST_DATABASE_URL_DIRECT")
    old_tx = os.environ.get("HORSIES_TEST_DATABASE_URL_TRANSACTION")
    os.environ["HORSIES_TEST_DATABASE_URL_DIRECT"] = direct_url
    os.environ["HORSIES_TEST_DATABASE_URL_TRANSACTION"] = transaction_url
    try:
        yield
    finally:
        if old_direct is None:
            os.environ.pop("HORSIES_TEST_DATABASE_URL_DIRECT", None)
        else:
            os.environ["HORSIES_TEST_DATABASE_URL_DIRECT"] = old_direct
        if old_tx is None:
            os.environ.pop("HORSIES_TEST_DATABASE_URL_TRANSACTION", None)
        else:
            os.environ["HORSIES_TEST_DATABASE_URL_TRANSACTION"] = old_tx


def _reload_pgbouncer_modules() -> tuple[Any, Any, Any]:
    import tests.e2e.tasks.instance_pgbouncer as instance_pgbouncer
    import tests.e2e.tasks.pgbouncer_tasks as pgbouncer_tasks
    import tests.e2e.tasks.pgbouncer_workflows as pgbouncer_workflows

    instance_pgbouncer = importlib.reload(instance_pgbouncer)
    pgbouncer_tasks = importlib.reload(pgbouncer_tasks)
    pgbouncer_workflows = importlib.reload(pgbouncer_workflows)
    return instance_pgbouncer, pgbouncer_tasks, pgbouncer_workflows


@pytest.mark.asyncio(loop_scope="function")
async def test_worker_processes_task_and_workflow_through_pgbouncer_split_urls() -> None:
    with isolated_pgbouncer_database("horsies_pgbouncer_e2e") as urls:
        with _pgbouncer_env(urls.direct, urls.transaction):
            instance_pgbouncer, pgbouncer_tasks, pgbouncer_workflows = (
                _reload_pgbouncer_modules()
            )
            schema_r = await instance_pgbouncer.broker.ensure_schema_initialized()
            assert schema_r.is_ok()

            try:
                with run_worker(PGB_INSTANCE):
                    task_handle = unwrap_send(pgbouncer_tasks.double.send(21))
                    assert_ok(await task_handle.get_async(timeout_ms=15000), 42)

                    workflow_handle = start_ok_sync(pgbouncer_workflows.spec_smoke)
                    assert_ok(await workflow_handle.get_async(timeout_ms=15000), 42)
            finally:
                await instance_pgbouncer.broker.close_async()
