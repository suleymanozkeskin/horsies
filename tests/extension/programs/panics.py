"""Cases of the two panic rules of a producer process.

- A panic in an operation raises ``PanicException`` from that call, and the
  core keeps running. The panic comes from the child spec source of the
  handle: it raises ``PanicException``, which is a defect of the client, so
  the operation that asked panics.
- A panic in the log forwarder stops the core: the call in flight and every
  later call raise ``CoreStopped(subject, cause)``. The panic comes from a
  logging handler that raises ``PanicException`` on a core record.

The handles use the ``ASSUME_INITIALIZED`` schema mode: the database has the
schema (``conftest.extension_database``), and no call installs it.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from collections.abc import Callable
from typing import Any, Final

from horsies._core import (
    CoreHandle,
    CoreStopped,
    PanicException,
    core_state,
    mint_task_id,
)
from tests.extension.programs.report import JsonValue, Report, run_case
from tests.extension.programs.support import (
    RECORD_WAIT_BOUND_S,
    TasksTableLock,
    collect_records,
    database_url,
    manifest_json,
    minted_task_id,
    wait_for_new_waiter_async,
    wait_until_async,
    waiter_report,
)
from tests.extension.programs.workflow_spec import (
    PanickingChildSpecSource,
    subworkflow_start_request,
)

SOURCE_PANIC_TEXT: Final = 'planned panic in the child spec source'
FORWARDER_PANIC_TEXT: Final = 'planned panic in a log handler'

PANIC_STATEMENT: Final = 'health_check'
"""The statement whose record makes the logging handler panic: the ping's."""

IN_FLIGHT_BOUND_S: Final = 10.0
"""The longest wait for a call in flight to end after the forwarder panicked."""


async def _panic_in_an_operation() -> Report:
    bridge = collect_records('horsies.bridge', logging.WARNING)
    handle = CoreHandle(
        manifest_json(database_url(), schema='ASSUME_INITIALIZED', echo=False),
        pool_profile='PRODUCER',
    )
    source = PanickingChildSpecSource(SOURCE_PANIC_TEXT)
    handle.register_child_spec_source(source)
    workflow_id = uuid.uuid4()
    request = subworkflow_start_request(workflow_id)
    caught_by = 'nothing'
    text = ''
    try:
        try:
            outcome = await handle.start_workflow_async(request)
            caught_by = f'returned {type(outcome).__name__}'
        except Exception as caught:
            caught_by = f'Exception: {type(caught).__name__}'
            text = str(caught)
    except PanicException as panic:
        caught_by = 'PanicException'
        text = str(panic)
    state_after = core_state()
    ping_after = await handle.ping_database_async()
    status_after = await handle.workflow_status_async(workflow_id)
    _ = await wait_until_async(
        lambda: any(record.level == 'ERROR' for record in bridge.records()),
        RECORD_WAIT_BOUND_S,
    )
    error_records: list[JsonValue] = [
        record.message for record in bridge.records() if record.level == 'ERROR'
    ]
    return {
        'caught_by': caught_by,
        'text': text,
        'source_calls': source.calls,
        'core_state_after': state_after,
        'ping_after': type(ping_after).__name__,
        'status_after': type(status_after).__name__,
        'bridge_errors': error_records,
    }


def panic_in_an_operation() -> Report:
    """A workflow start whose child spec source raises ``PanicException``."""
    return asyncio.run(_panic_in_an_operation())


class PanicOnStatement(logging.Handler):
    """Raises ``PanicException`` on each record of the statement ``statement``."""

    def __init__(self, statement: str) -> None:
        super().__init__(level=logging.NOTSET)
        self._statement = statement

    def emit(self, record: logging.LogRecord) -> None:
        statement: object = getattr(record, 'statement', None)
        if statement == self._statement:
            raise PanicException(FORWARDER_PANIC_TEXT)


def _stopped_report(stopped: CoreStopped) -> Report:
    return {
        'ended': 'raised',
        'class': 'CoreStopped',
        'arguments': [str(argument) for argument in stopped.args],
    }


async def _awaited(future: asyncio.Future[Any]) -> Report:
    """How an awaited call ended within ``IN_FLIGHT_BOUND_S``."""
    try:
        outcome = await asyncio.wait_for(future, IN_FLIGHT_BOUND_S)
        return {'ended': 'returned', 'class': type(outcome).__name__}
    except CoreStopped as stopped:
        return _stopped_report(stopped)
    except TimeoutError:
        return {'ended': 'timed_out', 'bound_s': IN_FLIGHT_BOUND_S}


def _called(call: Callable[[], object]) -> Report:
    """How a call ended: its outcome class, or ``CoreStopped`` with its arguments."""
    try:
        outcome = call()
        return {'ended': 'returned', 'class': type(outcome).__name__}
    except CoreStopped as stopped:
        return _stopped_report(stopped)


def _task_info_async(handle: CoreHandle) -> asyncio.Future[Any]:
    return handle.task_info_async(
        minted_task_id(),
        include_result=False,
        include_failed_reason=False,
        include_attempts=False,
    )


async def _panic_in_the_log_forwarder() -> Report:
    url = database_url()
    sql_logger = logging.getLogger('horsies.sql')
    sql_logger.setLevel(logging.DEBUG)
    sql_logger.addHandler(PanicOnStatement(PANIC_STATEMENT))
    handle = CoreHandle(
        manifest_json(url, schema='ASSUME_INITIALIZED', echo=True),
        pool_profile='PRODUCER',
    )
    warm_up = await _awaited(_task_info_async(handle))
    lock = TasksTableLock(url)
    known = lock.waiting_backends()
    lock.take()
    in_flight_future = _task_info_async(handle)
    waiter = await wait_for_new_waiter_async(lock, known)
    trigger = await _awaited(handle.ping_database_async())
    in_flight = await _awaited(in_flight_future)
    later_async = _called(handle.ping_database_async)
    later_blocking = _called(handle.ping_database)
    later_pure = _called(mint_task_id)
    lock.release()
    lock.close()
    return {
        'pid': os.getpid(),
        'warm_up': warm_up,
        'waiter': waiter_report(waiter),
        'trigger': trigger,
        'in_flight': in_flight,
        'later_async': later_async,
        'later_blocking': later_blocking,
        'later_pure': later_pure,
        'core_state_after': core_state(),
    }


def panic_in_the_log_forwarder() -> Report:
    """A logging handler on ``horsies.sql`` that raises ``PanicException`` on
    the ping's statement record, while a read waits for the held lock."""
    return asyncio.run(_panic_in_the_log_forwarder())


if __name__ == '__main__':
    run_case(
        {
            'panic_in_an_operation': panic_in_an_operation,
            'panic_in_the_log_forwarder': panic_in_the_log_forwarder,
        },
    )
