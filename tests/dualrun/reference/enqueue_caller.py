# pyright: reportPrivateUsage=false
"""The send callers, before they called the core.

``PostgresBroker.enqueue_async`` and ``enqueue``
(``python/horsies/core/brokers/postgres.py``) checked the send
(``_prepare_enqueue``: the timing, the input shape, the options, the version-1
command fingerprint and the scoped key), installed the schema once per broker
(``_ensure_initialized``), and sent one ``horsies_enqueue_task`` statement in
autocommit: the async form on the running loop's SQLAlchemy engine
(``ENQUEUE_TASK_SQL``), the sync form on the calling thread through the sync
send engine (``ENQUEUE_TASK_DRIVER_SQL``, ``send_route`` ``CALLER_THREAD``) or
on the sync runner through the async form. ``_prepare_enqueue``,
``send_route``, ``_enqueue_on_caller_thread`` and the statements stay in
``postgres.py``. The functions below are the two methods' bodies. Each takes
the broker as its first parameter, so the dual-run rebinding can put it in
the place of the method; the sync form runs the async reference.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from horsies.core.brokers.enqueue_task import (
    ENQUEUE_TASK_SQL,
    SendKind,
    enqueue_result,
    enqueue_task_params,
    read_enqueue_outcome,
)
from horsies.core.brokers.postgres import (
    _DRIVER_ERRORS,
    PostgresBroker,
    SyncSendRoute,
    _async_context_err,
    _enqueue_failed,
    _loop_limit_err,
    _loop_runner_err,
)
from horsies.core.brokers.result_types import BrokerEnqueueError, BrokerSyncCallError
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
    resolve_retention_class_key,
)
from horsies.core.history.periods import RetentionClassKey
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.utils.loop_runner import LoopRunnerError, in_running_event_loop


async def enqueue_async(
    self: PostgresBroker,
    task_name: str,
    queue_name: str = 'default',
    *,
    task_id: UUID,
    send_kind: SendKind,
    input_json: str | None = None,
    priority: int = 100,
    sent_at: datetime,
    enqueued_at: Optional[datetime] = None,
    enqueue_delay_seconds: Optional[int] = None,
    good_until: Optional[datetime] = None,
    task_options: Optional[str] = None,
    retention_class_key: RetentionClassKey | None = DEFAULT_RETENTION_CLASS_KEY,
    retain_rerun_input: Optional[bool] = None,
    idempotency_key: Optional[str] = None,
) -> Result[UUID, BrokerEnqueueError]:
    """Write one PENDING task row, or answer the send that wrote it.

    One ``horsies_enqueue_task`` statement in autocommit. The row stores
    ``input_json`` as its ``input`` (``{}`` for none or JSON null), the
    version-1 command fingerprint of the send and ``sent_at``, the call
    time of the send. A send that repeats a task id is the same send only
    when the stored fingerprint and ``sent_at`` are equal; a resend
    passes the ``sent_at`` of the first send. ``send_kind`` is
    ``SendKind.REPEAT`` for a send retry: a task that finished then
    answers with its id and does not run again.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('enqueue', loop_limit)
        case Ok(resources):
            pass
    # The ratified retention resolution: absent → the immutable
    # 30-day default class; forever only by EXPLICIT None.
    resolved_retention_class_key = resolve_retention_class_key(
        retention_class_key
    )
    # The ratified retention posture: an app-level default with a
    # per-task override, resolved here and snapshotted at enqueue.
    # None means inherit the deployment's standing policy.
    if retain_rerun_input is None:
        retain_rerun_input = self.config.retain_rerun_input_default
    prepared_r = self._prepare_enqueue(
        task_id=task_id,
        task_name=task_name,
        queue_name=queue_name,
        priority=priority,
        input_json=input_json,
        sent_at=sent_at,
        enqueued_at=enqueued_at,
        enqueue_delay_seconds=enqueue_delay_seconds,
        good_until=good_until,
        task_options=task_options,
        retention_class_key=resolved_retention_class_key,
        retain_rerun_input=retain_rerun_input,
        idempotency_key=idempotency_key,
        key_window=self.idempotency_reservation_window,
    )
    if is_err(prepared_r):
        return prepared_r
    prepared = prepared_r.ok_value

    # Schema init keeps a broad catch, as ensure_schema_initialized does:
    # it raises RuntimeError for operator states such as an incomplete
    # cutover, and each of those is a failed enqueue, not a crash.
    try:
        await self._ensure_initialized()
    except Exception as exc:
        return _enqueue_failed(
            f'Failed to enqueue task {task_name}: {exc}',
            exc,
        )

    try:
        async with resources.engine.connect() as conn:
            autocommit_conn = await conn.execution_options(
                isolation_level='AUTOCOMMIT',
            )
            result = await autocommit_conn.execute(
                ENQUEUE_TASK_SQL,
                enqueue_task_params(prepared.row, send_kind),
            )
            rows = result.fetchall()
    except _DRIVER_ERRORS as exc:
        return _enqueue_failed(
            f'Failed to enqueue task {task_name}: {exc}',
            exc,
        )
    return enqueue_result(
        read_enqueue_outcome(rows),
        task_id=task_id,
        task_name=task_name,
    )


def enqueue(
    self: PostgresBroker,
    task_name: str,
    queue_name: str = 'default',
    *,
    task_id: UUID,
    send_kind: SendKind,
    input_json: str | None = None,
    priority: int = 100,
    sent_at: datetime,
    enqueued_at: Optional[datetime] = None,
    enqueue_delay_seconds: Optional[int] = None,
    good_until: Optional[datetime] = None,
    task_options: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    retention_class_key: RetentionClassKey | None = DEFAULT_RETENTION_CLASS_KEY,
    retain_rerun_input: Optional[bool] = None,
) -> Result[UUID, BrokerEnqueueError | BrokerSyncCallError]:
    """Synchronous task submission, with the outcomes of ``enqueue_async``.

    ``send_route`` decides where the row is written: on the calling
    thread, or on the sync runner's loop through ``enqueue_async``. On a
    thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)`` and
    writes nothing.
    """
    if in_running_event_loop():
        return _async_context_err(
            'PostgresBroker.enqueue()', 'broker.enqueue_async(...)',
        )
    match self.send_route():
        case SyncSendRoute.CALLER_THREAD:
            return self._enqueue_on_caller_thread(
                task_name,
                queue_name,
                task_id=task_id,
                send_kind=send_kind,
                input_json=input_json,
                priority=priority,
                sent_at=sent_at,
                enqueued_at=enqueued_at,
                enqueue_delay_seconds=enqueue_delay_seconds,
                good_until=good_until,
                task_options=task_options,
                retention_class_key=retention_class_key,
                retain_rerun_input=retain_rerun_input,
                idempotency_key=idempotency_key,
            )
        case SyncSendRoute.LOOP_RUNNER:
            pass
    try:
        return self.run_sync(
            enqueue_async,
            self,
            task_name,
            queue_name,
            task_id=task_id,
            send_kind=send_kind,
            input_json=input_json,
            priority=priority,
            sent_at=sent_at,
            enqueued_at=enqueued_at,
            enqueue_delay_seconds=enqueue_delay_seconds,
            good_until=good_until,
            task_options=task_options,
            idempotency_key=idempotency_key,
            retention_class_key=retention_class_key,
            retain_rerun_input=retain_rerun_input,
        )
    except LoopRunnerError as exc:
        return _loop_runner_err(f'PostgresBroker.enqueue({task_name!r})', exc)
