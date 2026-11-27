"""``rerun_task`` through the core's rerun steps: the read it sends on the
caller's connection, and the preconditions it raises for.

The connection is a mock whose source read returns a row in the core's
column order; the core functions of ``horsies._core`` run as shipped.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import timedelta
from enum import Enum
from typing import Final
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from horsies.core.app import Horsies
from horsies.core.history.rerun.operations import (
    RerunEnqueuePolicy,
    RerunSourceLive,
    RerunTask,
    rerun_task,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import RetryPolicy, TaskError
from horsies.core.types.result import Ok, Result
from horsies._core import CorePreconditionFailed
from tests.attempt_snapshot_support import encode_attempt_snapshot

pytestmark = [pytest.mark.unit]

SOURCE_ID: Final = UUID('0192f0a0-0000-7000-8000-000000000001')
_READ_COLUMNS: Final = (
    'class_registered', 'client_family', 'location', 'absence_reason', 'task_id',
    'task_name', 'queue_name', 'priority', 'max_retries', 'status', 'is_workflow_task',
    'rerun_root_task_id', 'attempt_archive_version', 'attempt_snapshot',
    'attempt_snapshot_digest', 'rerun_input_disposition', 'rerun_input_version',
    'rerun_input_digest', 'rerun_input_inline',
)


def _connection(**values: object) -> AsyncMock:
    """A connection whose source read row holds ``values`` by column name
    (the Python family unless given), every other column ``None``."""
    named = {'client_family': 'PYTHON', **values}
    result = MagicMock()
    result.one.return_value = tuple(named.get(column) for column in _READ_COLUMNS)
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=result)
    return connection


def _policy() -> RerunEnqueuePolicy:
    return RerunEnqueuePolicy(
        retention=timedelta(days=30),
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    )


async def _rerun(connection: AsyncMock) -> object:
    return await rerun_task(
        connection,
        RerunTask(source_task_id=SOURCE_ID, deadline=None),
        _policy(),
        registered_task_codes={},
    )


@pytest.mark.asyncio
async def test_the_read_binds_the_source_and_the_class_on_the_callers_connection() -> None:
    connection = _connection(class_registered=True, location='LIVE')

    outcome = await _rerun(connection)

    assert outcome == RerunSourceLive(task_id=SOURCE_ID)
    assert connection.execute.await_count == 1
    statement, parameters = connection.execute.await_args.args
    assert 'horsies_rerun_source_read(CAST(:p1 AS uuid), CAST(:p2 AS text))' in str(statement)
    assert parameters == {'p1': SOURCE_ID, 'p2': 'standard_30d'}


@pytest.mark.asyncio
async def test_an_unregistered_class_raises_before_any_write() -> None:
    connection = _connection(class_registered=False, location='LIVE')

    with pytest.raises(ValueError, match="retention class 'standard_30d' is not registered"):
        _ = await _rerun(connection)

    assert connection.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('stored', ['RUST', 'TYPESCRIPT', None])
async def test_a_database_of_another_family_raises_before_any_write(stored: str | None) -> None:
    connection = _connection(class_registered=True, client_family=stored, location='LIVE')

    with pytest.raises(CorePreconditionFailed) as raised:
        _ = await _rerun(connection)

    assert raised.value.subject == 'rerun_task'
    assert raised.value.retryable is False
    assert 'PYTHON' in raised.value.message
    assert connection.execute.await_count == 1


TASK_NAME: Final = 'charge'
"""The task name of the history source of the retry code tests."""


class WriteSent(Exception):
    """Raised by the mock connection at the write: the rerun passed every
    check before the write."""


def _history_row(auto_retry_for: list[str]) -> tuple[object, ...]:
    """The source read row of a FAILED history source of ``TASK_NAME`` whose
    stored options retry on the code texts ``auto_retry_for``."""
    options = {
        'retry_policy': {
            'max_retries': 1,
            'intervals': [1],
            'backoff_strategy': 'fixed',
            'jitter': True,
            'auto_retry_for': auto_retry_for,
        },
    }
    options_text = json.dumps(options, separators=(',', ':'))
    payload = f'{{"input":{{"k":"v"}},"options":{options_text}}}'.encode()
    snapshot = encode_attempt_snapshot(())
    named: dict[str, object] = {
        'class_registered': True,
        'client_family': 'PYTHON',
        'location': 'HISTORY',
        'task_id': SOURCE_ID,
        'task_name': TASK_NAME,
        'queue_name': 'default',
        'priority': 50,
        'max_retries': 1,
        'status': 'FAILED',
        'is_workflow_task': False,
        'attempt_archive_version': snapshot.version,
        'attempt_snapshot': snapshot.payload,
        'attempt_snapshot_digest': snapshot.digest,
        'rerun_input_disposition': 'INLINE',
        'rerun_input_version': 1,
        'rerun_input_digest': hashlib.sha256(payload).digest(),
        'rerun_input_inline': payload,
    }
    return tuple(named.get(column) for column in _READ_COLUMNS)


def _history_connection(auto_retry_for: list[str]) -> AsyncMock:
    """A connection whose source read returns ``_history_row`` and whose
    write raises ``WriteSent``."""
    result = MagicMock()
    result.one.return_value = _history_row(auto_retry_for)
    connection = AsyncMock()
    connection.execute = AsyncMock(side_effect=[result, WriteSent()])
    return connection


async def _rerun_with(
    connection: AsyncMock,
    registered_task_codes: Mapping[str, tuple[str, ...]],
) -> object:
    return await rerun_task(
        connection,
        RerunTask(source_task_id=SOURCE_ID, deadline=None),
        _policy(),
        registered_task_codes=registered_task_codes,
    )


@pytest.mark.asyncio
async def test_a_stored_custom_retry_code_that_the_task_declares_reaches_the_write() -> None:
    connection = _history_connection(['TRANSIENT'])

    with pytest.raises(WriteSent):
        _ = await _rerun_with(connection, {TASK_NAME: ('TRANSIENT',)})

    assert connection.execute.await_count == 2


@pytest.mark.asyncio
async def test_a_stored_custom_retry_code_that_the_task_does_not_declare_raises() -> None:
    connection = _history_connection(['WORKER_CRASHED', 'TRANSIENT'])

    with pytest.raises(CorePreconditionFailed) as raised:
        _ = await _rerun_with(connection, {TASK_NAME: ('RATE_LIMITED',)})

    assert raised.value.subject == 'rerun_task'
    assert raised.value.retryable is False
    assert "'charge'" in raised.value.message
    assert "'TRANSIENT'" in raised.value.message
    assert connection.execute.await_count == 1


@pytest.mark.asyncio
async def test_a_stored_custom_retry_code_of_an_unregistered_task_raises() -> None:
    connection = _history_connection(['TRANSIENT'])

    with pytest.raises(CorePreconditionFailed) as raised:
        _ = await _rerun_with(connection, {'another_task': ('TRANSIENT',)})

    assert raised.value.retryable is False
    assert "does not register task 'charge'" in raised.value.message
    assert connection.execute.await_count == 1


@pytest.mark.asyncio
async def test_stored_built_in_retry_codes_reach_the_write_with_no_registered_task() -> None:
    connection = _history_connection(['WORKER_CRASHED', 'TASK_TIMEOUT'])

    with pytest.raises(WriteSent):
        _ = await _rerun_with(connection, {})

    assert connection.execute.await_count == 2


@pytest.mark.asyncio
async def test_a_registered_code_set_that_the_core_refuses_raises_value_error() -> None:
    connection = _history_connection(['TRANSIENT'])

    with pytest.raises(ValueError, match="registered_task_codes of task 'charge'"):
        _ = await _rerun_with(connection, {TASK_NAME: ('DATABASE_ERROR',)})

    assert connection.execute.await_count == 1


class _ChargeCode(str, Enum):
    TRANSIENT = 'TRANSIENT'


def _app_with_charge(*, declares_transient: bool) -> Horsies:
    """An app that registers ``TASK_NAME``, with or without ``TRANSIENT``
    among its declared codes."""
    app = Horsies(
        config=AppConfig(
            broker=PostgresConfig(database_url='postgresql://user:pass@localhost/db'),
        ),
    )
    error_codes: type[Enum] | tuple[str, ...] = _ChargeCode if declares_transient else ()

    @app.task(
        TASK_NAME,
        error_codes=error_codes,
        retry_policy=RetryPolicy.fixed([1], auto_retry_for=[_ChargeCode.TRANSIENT])
        if declares_transient
        else None,
    )
    def charge() -> Result[str, TaskError]:
        return Ok('charged')

    @app.task('refund')
    def refund() -> Result[str, TaskError]:
        return Ok('refunded')

    _ = (charge, refund)
    return app


def test_the_app_gives_the_declared_codes_of_every_registered_task() -> None:
    app = _app_with_charge(declares_transient=True)

    assert app.registered_task_codes() == {TASK_NAME: ('TRANSIENT',), 'refund': ()}


@pytest.mark.asyncio
async def test_the_app_rerun_checks_the_stored_retry_list_against_its_registry() -> None:
    declaring = _history_connection(['TRANSIENT'])
    with pytest.raises(WriteSent):
        _ = await _app_with_charge(declares_transient=True).rerun_task_async(
            declaring,
            RerunTask(source_task_id=SOURCE_ID, deadline=None),
            _policy(),
        )

    not_declaring = _history_connection(['TRANSIENT'])
    with pytest.raises(CorePreconditionFailed, match="names the code 'TRANSIENT'"):
        _ = await _app_with_charge(declares_transient=False).rerun_task_async(
            not_declaring,
            RerunTask(source_task_id=SOURCE_ID, deadline=None),
            _policy(),
        )
    assert not_declaring.execute.await_count == 1
