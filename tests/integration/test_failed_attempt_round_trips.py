"""Round trips of a failed attempt, counted at the wire.

A failure finalize, a timeout write and their retries are one unlocked read
of the retry facts and one call of ``horsies_finish_failed_attempt``, both
in autocommit on one checkout. A worker-level failure is never retried, so
it skips the read. Each test drives the real entry point through a
``RoundTripProxy`` in the steady state of psycopg's prepared statements, as
``test_round_trips`` does, with the worker pre-ping off and on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from collections.abc import Coroutine, Generator
from datetime import datetime
from typing import Any, Final
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.tasks import OutcomeCode, TaskError, TaskResult
from horsies.core.types.result import Ok
from horsies.core.worker.runtime import _TimeoutSettled
from horsies.core.worker.shared_stop import RetryPlacement
from tests.integration.autocommit_support import (
    QUEUE,
    TASK_NAME,
    insert_task,
    open_proxied_worker,
    proxied_url,
    steady_state_turns,
    upstream_of,
)
from tests.integration.round_trip_proxy import RoundTripProxy

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

PING: Final = 1
"""The pool pre-ping of a checkout, when ``worker_pool_pre_ping`` is on."""
ERROR_CODE: Final = 'FLAKY'
TIMEOUT_MS: Final = 1_000

type Case = tuple[str, datetime]

PRE_PING = pytest.mark.parametrize(
    'pre_ping', [False, True], ids=['pre_ping_off', 'pre_ping_on']
)
RETRY = pytest.mark.parametrize('retries', [False, True], ids=['fails', 'retries'])


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


def _error_json() -> str:
    envelope = encode_task_result(
        TaskResult(err=TaskError(error_code=ERROR_CODE, message='flaky')),
        JsonValue,
    )
    return dumps_json(envelope).unwrap()


def _close_unstarted(coroutine: Coroutine[Any, Any, Any], **_: object) -> None:
    """Drop the delayed retry wake: the count covers the write only."""
    coroutine.close()


async def _running(session: AsyncSession, *, retry_code: str | None) -> Case:
    """A RUNNING task of the proxied worker; with ``retry_code`` its policy
    retries that code."""
    task_id, claimed_at = await insert_task(session, status='RUNNING')
    if retry_code is not None:
        await session.execute(
            text("""
                UPDATE horsies_tasks
                SET max_retries = 3, task_options = :options
                WHERE id = CAST(:id AS uuid)
            """),
            {
                'id': task_id,
                'options': json.dumps(
                    {
                        'retry_policy': {
                            'auto_retry_for': [retry_code],
                            'intervals': [600],
                            'backoff_strategy': 'fixed',
                            'jitter': False,
                        },
                    }
                ),
            },
        )
        await session.commit()
    return task_id, claimed_at


async def _live_status(session: AsyncSession, task_id: str) -> str | None:
    status = await session.scalar(
        text('SELECT status FROM horsies_tasks WHERE id = CAST(:id AS uuid)'),
        {'id': task_id},
    )
    await session.commit()
    return None if status is None else str(status)


async def _measure(
    proxy: RoundTripProxy,
    db_url: str,
    session: AsyncSession,
    *,
    pre_ping: bool,
    retry_code: str | None,
    act_on: Any,
) -> tuple[str, int, Any]:
    opened = await open_proxied_worker(proxied_url(db_url, proxy), pre_ping=pre_ping)
    opened.worker._spawn_background = MagicMock(side_effect=_close_unstarted)  # type: ignore[method-assign]

    async def arrange() -> Case:
        return await _running(session, retry_code=retry_code)

    async def act(case: Case) -> None:
        await act_on(opened.worker, case)

    try:
        (task_id, _), turns = await steady_state_turns(proxy, arrange, act)
    finally:
        _ = await opened.broker.close_async()
    return task_id, len(turns), turns


@PRE_PING
async def test_a_worker_level_failure_is_one_statement(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    pre_ping: bool,
) -> None:
    async def act(worker: Any, case: Case) -> None:
        task_id, claimed_at = case
        result = await worker._finalize_task_result(
            task_id,
            False,
            '',
            'the child lost its pipe',
            queue_name=QUEUE,
            is_workflow_task=False,
            task_name=TASK_NAME,
            claimed_at=claimed_at,
        )
        assert result == Ok(None)

    task_id, round_trips, turns = await _measure(
        proxy,
        db_url,
        session,
        pre_ping=pre_ping,
        retry_code=None,
        act_on=act,
    )

    assert round_trips == 1 + (PING if pre_ping else 0), turns
    assert await _live_status(session, task_id) is None


@PRE_PING
@RETRY
async def test_a_task_error_is_one_read_and_one_statement(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    pre_ping: bool,
    retries: bool,
) -> None:
    async def act(worker: Any, case: Case) -> None:
        task_id, claimed_at = case
        result = await worker._finalize_task_result(
            task_id,
            True,
            _error_json(),
            None,
            queue_name=QUEUE,
            is_workflow_task=False,
            task_name=TASK_NAME,
            claimed_at=claimed_at,
        )
        assert result == Ok(None)

    task_id, round_trips, turns = await _measure(
        proxy,
        db_url,
        session,
        pre_ping=pre_ping,
        retry_code=ERROR_CODE if retries else None,
        act_on=act,
    )

    assert round_trips == 2 + (PING if pre_ping else 0), turns
    assert await _live_status(session, task_id) == ('PENDING' if retries else None)


@PRE_PING
@RETRY
async def test_a_timeout_is_one_read_and_one_statement(
    proxy: RoundTripProxy,
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
    pre_ping: bool,
    retries: bool,
) -> None:
    async def act(worker: Any, case: Case) -> None:
        task_id, claimed_at = case
        settled = await worker._record_task_timeout(
            task_id,
            TIMEOUT_MS,
            claimed_at,
            retry=RetryPlacement.SAME_CLASS,
        )
        assert settled == _TimeoutSettled()

    task_id, round_trips, turns = await _measure(
        proxy,
        db_url,
        session,
        pre_ping=pre_ping,
        retry_code=OutcomeCode.TASK_TIMEOUT.value if retries else None,
        act_on=act,
    )

    assert round_trips == 2 + (PING if pre_ping else 0), turns
    assert await _live_status(session, task_id) == ('PENDING' if retries else None)
