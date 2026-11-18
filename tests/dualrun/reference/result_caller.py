# pyright: reportPrivateUsage=false
"""The raw result read and wait, before it called the core.

``PostgresBroker.get_raw_result_record_async`` and ``get_raw_result_record``
(``python/horsies/core/brokers/postgres.py``) probed the result through the
running event loop's SQLAlchemy resource set (``_probe_result_row``: the
staged result function, or the live probe and the staged detail read before
it is published), then waited on that set's ``PostgresListener`` for a
``task_done`` notification with the task id, probing again every 5 s, or
every 0.2 s with no listener. The probe methods stay on the class. The sync
form ran the async one on the broker's sync runner. The functions below are
the two methods' bodies. Each takes the broker as its first parameter, so the
dual-run rebinding can put it in the place of the method; the sync form runs
the async reference.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional, cast
from uuid import UUID

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.postgres import (
    _STILL_WAITING,
    PostgresBroker,
    _async_context_err,
    _loop_limit_err,
    _operation_failed,
    _wait_inside_task_err,
)
from horsies.core.brokers.resources import ListenerDisabled, ListenerEnabled
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerResult,
    RawResultRecord,
)
from horsies.core.process_role import ProcessRole, process_role
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok
from horsies.core.utils.loop_runner import LoopRunnerError, in_running_event_loop
from horsies.core.utils.wait_timeout import wait_timeout_seconds
from tests.dualrun.reference.enqueue_caller import _loop_runner_err
from tests.dualrun.reference.install_caller import ensure_initialized


async def get_raw_result_record_async(
    self: PostgresBroker,
    task_id: UUID,
    timeout_ms: Optional[int] = None,
) -> BrokerResult[RawResultRecord | None]:
    """Raw broker fetch of a task's stored result envelope.

    Strict-serde phase 6a primitive. The broker performs no typed
    decoding; callers at the handle/app layer derive ``ok_type``
    from ``record.task_name`` and call
    ``decode_task_result(record.raw_result, ok_type)``.

    Semantics:

    - ``Ok(None)``: row truly absent (no ``TaskModel`` row for
      ``task_id``).
    - ``Ok(RawResultRecord(status=COMPLETED|FAILED|EXPIRED, raw_result=<dict>))``:
      terminal row with a stored payload.
    - ``Ok(RawResultRecord(status=CANCELLED, raw_result=None))``:
      task was cancelled before completion.
    - ``Ok(RawResultRecord(status=<non-terminal>, raw_result=None))``:
      timeout fired before the row reached a terminal state.
    - ``Err(BrokerOperationError(INVALID_JSON_PAYLOAD, ...))``:
      stored JSON failed strict parse (malformed, non-finite
      float, non-object envelope).
    - ``Err(BrokerWaitInsideTask)``: the call runs in a process that
      runs task bodies (a worker child). This is the first check: it
      comes before the argument checks. Nothing ran.
    - ``Err(BrokerInvalidInput)``: ``timeout_ms`` is not None or an int
      from 0 to ``MAX_TASK_TIMEOUT_MS``. Nothing ran.
    - ``Err(...)`` with other codes for DB / listener failures.
    """
    if process_role() is ProcessRole.RUNS_TASK_BODIES:
        return _wait_inside_task_err(
            'PostgresBroker.get_raw_result_record_async()', task_id,
        )
    match wait_timeout_seconds(timeout_ms):
        case Err(err_value=refused):
            return Err(BrokerInvalidInput(
                message=f'raw result fetch of task {task_id}: {refused.message}',
                exception=NoCause(),
            ))
        case Ok(ok_value=timeout_seconds):
            pass
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('raw result fetch', loop_limit)
        case Ok(resources):
            pass
    try:
        await ensure_initialized(self)

        start_time = asyncio.get_event_loop().time()

        # Quick path: row may already be terminal.
        async with resources.session_factory() as session:
            outcome = await self._probe_result_row(session, task_id)
            if outcome is not _STILL_WAITING:
                return cast(
                    'BrokerResult[RawResultRecord | None]', outcome,
                )

        # Listen + poll loop. A broker without a listener polls.
        q: asyncio.Queue[Any] | None = None
        subscription: tuple[PostgresListener, asyncio.Queue[Any]] | None = None
        match resources.listener:
            case ListenerEnabled(listener=listener):
                listen_r = await listener.listen_payload('task_done', str(task_id))
                match listen_r:
                    case Ok(queue):
                        q = queue
                        subscription = (listener, queue)
                    case Err(listen_err):
                        self.logger.debug(
                            'LISTEN unavailable; falling back to polling '
                            'for task_done. Original error: %s',
                            listen_err.message,
                        )
            case ListenerDisabled():
                pass

        try:
            poll_interval = 5.0 if q is not None else 0.2
            while True:
                remaining_timeout: float | None = None
                if timeout_seconds is not None:
                    elapsed = (
                        asyncio.get_event_loop().time() - start_time
                    )
                    remaining_timeout = timeout_seconds - elapsed
                    if remaining_timeout <= 0:
                        # Timeout. Surface current row state so the
                        # caller can map to WAIT_TIMEOUT / outcome
                        # codes per their UX. The row may have moved
                        # to terminal between checks; one more read
                        # to capture the latest snapshot.
                        async with resources.session_factory() as session:
                            outcome = await self._probe_result_row(
                                session,
                                task_id,
                                non_terminal_snapshot=True,
                            )
                            return cast(
                                'BrokerResult[RawResultRecord | None]',
                                outcome,
                            )

                wait_time = (
                    min(poll_interval, remaining_timeout)
                    if remaining_timeout is not None
                    else poll_interval
                )

                if q is not None:
                    try:

                        async def _wait_for_task() -> None:
                            while True:
                                note = await q.get()
                                if note.payload == str(task_id):
                                    return

                        await asyncio.wait_for(
                            _wait_for_task(), timeout=wait_time,
                        )
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(wait_time)

                async with resources.session_factory() as session:
                    outcome = await self._probe_result_row(
                        session, task_id,
                    )
                    if outcome is not _STILL_WAITING:
                        return cast(
                            'BrokerResult[RawResultRecord | None]',
                            outcome,
                        )
        finally:
            match subscription:
                case (listener, queue):
                    await self._unsubscribe_task_done_safely(
                        listener, task_id, queue,
                    )
                case None:
                    pass
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        self.logger.exception(
            'Broker error while retrieving raw task result record',
        )
        return _operation_failed(
            BrokerErrorCode.TASK_INFO_QUERY_FAILED,
            f'Broker error while retrieving task {task_id}: {exc}',
            exc,
        )

def get_raw_result_record(
    self: PostgresBroker,
    task_id: UUID,
    timeout_ms: Optional[int] = None,
) -> BrokerResult[RawResultRecord | None]:
    """Synchronous wrapper around ``get_raw_result_record_async``.

    Runs the async path on the broker's background event loop via
    ``LoopRunner``. In a process that runs task bodies it returns
    ``Err(BrokerWaitInsideTask)`` first. On a thread that runs an event
    loop it returns ``Err(ASYNC_CONTEXT)``. Neither does I/O.
    """
    if process_role() is ProcessRole.RUNS_TASK_BODIES:
        return _wait_inside_task_err(
            'PostgresBroker.get_raw_result_record()', task_id,
        )
    if in_running_event_loop():
        return _async_context_err(
            'PostgresBroker.get_raw_result_record()',
            'broker.get_raw_result_record_async(...)',
        )
    try:
        return self.run_sync(
            get_raw_result_record_async, self, task_id, timeout_ms,
        )
    except LoopRunnerError as exc:
        return _loop_runner_err(
            f'PostgresBroker.get_raw_result_record({task_id!r})', exc,
        )
