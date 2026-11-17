# pyright: reportPrivateUsage=false
"""The task info callers, before they called the core.

``PostgresBroker.get_task_info_async`` and ``get_task_info``
(``python/horsies/core/brokers/postgres.py``) read the live row through a
session of the running event loop's SQLAlchemy resource set, with the
attempts read; a live miss fell through to the staged detail reader
(``PostgresBroker._history_task_info``, which stays on the class). The sync
form ran the async one on the broker's sync runner. The functions below are
the two methods' bodies. Each takes the broker as its first parameter, so the
dual-run rebinding can put it in the place of the method; the sync form runs
the async reference.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import text

from horsies.core.brokers.postgres import (
    SELECT_TASK_ATTEMPTS_BY_TASK_ID_SQL,
    PostgresBroker,
    _async_context_err,
    _loop_limit_err,
    _loop_runner_err,
    _operation_failed,
)
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidJsonPayload,
    BrokerResult,
)
from horsies.core.codec.json_io import loads_json
from horsies.core.models.tasks import Execution, TaskAttemptInfo, TaskInfo
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.types.status import TaskAttemptOutcome, TaskStatus
from horsies.core.utils.loop_runner import LoopRunnerError, in_running_event_loop
from tests.dualrun.reference.install_caller import ensure_initialized


async def get_task_info_async(
    self: PostgresBroker,
    task_id: UUID,
    *,
    include_result: bool = False,
    include_failed_reason: bool = False,
    include_attempts: bool = False,
) -> BrokerResult[TaskInfo | None]:
    """Fetch metadata for a task by ID.

    Returns Ok(TaskInfo) if found, Ok(None) if not found,
    Err(BrokerOperationError) on infrastructure failure.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('get_task_info', loop_limit)
        case Ok(resources):
            pass
    try:
        await ensure_initialized(self)

        async with resources.session_factory() as session:
            base_columns = [
                'task_name',
                'status',
                'queue_name',
                'priority',
                'retry_count',
                'max_retries',
                'next_retry_at',
                'sent_at',
                'enqueued_at',
                'claimed_at',
                'started_at',
                # A live row stores no terminal times, error code,
                # result or failed reason: those live in history.
                'NULL::timestamptz AS completed_at',
                'NULL::timestamptz AS failed_at',
                'worker_hostname',
                'worker_pid',
                'worker_process_name',
                'NULL::text AS error_code',
                'execution',
            ]
            if include_result:
                base_columns.append('NULL::text AS result')
            if include_failed_reason:
                base_columns.append('NULL::text AS failed_reason')

            query = text(
                f"""
                SELECT {', '.join(base_columns)}
                FROM horsies_tasks
                WHERE id = :id
            """
            )
            result = await session.execute(query, {'id': str(task_id)})
            row = result.fetchone()
            if row is None:
                # Terminalization moves finished rows to history, so
                # a live miss is the normal terminal case: fall
                # through to the staged detail read, exactly like
                # the result surface. Ok(None) only on true absence.
                return await self._history_task_info(
                    session,
                    task_id,
                    include_result=include_result,
                    include_failed_reason=include_failed_reason,
                    include_attempts=include_attempts,
                )

            raw_result_value: dict[str, Any] | None = None
            failed_reason = None

            idx = 0
            task_name = row[idx]
            idx += 1
            status = TaskStatus(row[idx])
            idx += 1
            queue_name = row[idx]
            idx += 1
            priority = row[idx]
            idx += 1
            retry_count = row[idx] or 0
            idx += 1
            max_retries = row[idx] or 0
            idx += 1
            next_retry_at = row[idx]
            idx += 1
            sent_at = row[idx]
            idx += 1
            enqueued_at = row[idx]
            idx += 1
            claimed_at = row[idx]
            idx += 1
            started_at = row[idx]
            idx += 1
            completed_at = row[idx]
            idx += 1
            failed_at = row[idx]
            idx += 1
            worker_hostname = row[idx]
            idx += 1
            worker_pid = row[idx]
            idx += 1
            worker_process_name = row[idx]
            idx += 1
            error_code_value = row[idx]
            idx += 1
            # The CHECK on the column holds the domain. A value outside
            # it raises here, and the read fails as a query failure.
            execution = Execution(row[idx])
            idx += 1

            if include_result:
                raw_result_text = row[idx]
                idx += 1
                if raw_result_text:
                    _lr = loads_json(raw_result_text)
                    if is_err(_lr):
                        return Err(BrokerInvalidJsonPayload(
                            message=(
                                f'Result JSON parse failed for task '
                                f'{task_id}: {_lr.err_value}'
                            ),
                            exception=_lr.err_value,
                        ))
                    loaded = _lr.ok_value
                    if loaded is not None and not isinstance(loaded, dict):
                        return Err(BrokerInvalidJsonPayload(
                            message=(
                                f'Result for task {task_id} is not a '
                                f'JSON object; got '
                                f'{type(loaded).__name__}'
                            ),
                            exception=NoCause(),
                        ))
                    # Broker stays infrastructure: no typed decode
                    # here. The app-level `get_task_info` does the
                    # `decode_task_result` step using the local task
                    # catalog. (Strict-serde design §6.)
                    raw_result_value = loaded
                else:
                    raw_result_value = None
            else:
                raw_result_value = None

            if include_failed_reason:
                failed_reason = row[idx]

            # Load attempt history if requested
            attempts: list[TaskAttemptInfo] | None = None
            if include_attempts:
                att_result = await session.execute(
                    SELECT_TASK_ATTEMPTS_BY_TASK_ID_SQL,
                    {'task_id': str(task_id)},
                )
                attempts = [
                    TaskAttemptInfo(
                        task_id=task_id,
                        attempt=att_row[1],
                        outcome=TaskAttemptOutcome(att_row[2]),
                        will_retry=att_row[3],
                        started_at=att_row[4],
                        finished_at=att_row[5],
                        error_code=att_row[6],
                        error_message=att_row[7],
                        failed_reason=att_row[8],
                        worker_id=att_row[9],
                        worker_hostname=att_row[10],
                        worker_pid=att_row[11],
                        worker_process_name=att_row[12],
                    )
                    for att_row in att_result.fetchall()
                ]

            return Ok(
                TaskInfo(
                    task_id=task_id,
                    task_name=task_name,
                    status=status,
                    queue_name=queue_name,
                    priority=priority,
                    retry_count=retry_count,
                    max_retries=max_retries,
                    next_retry_at=next_retry_at,
                    sent_at=sent_at,
                    enqueued_at=enqueued_at,
                    claimed_at=claimed_at,
                    started_at=started_at,
                    completed_at=completed_at,
                    failed_at=failed_at,
                    worker_hostname=worker_hostname,
                    worker_pid=worker_pid,
                    worker_process_name=worker_process_name,
                    error_code=error_code_value,
                    raw_result=raw_result_value,
                    decoded_result=None,
                    result_decoded=False,
                    failed_reason=failed_reason,
                    attempts=attempts,
                    execution=execution,
                )
            )
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.TASK_INFO_QUERY_FAILED,
            f'get_task_info failed for {task_id}: {exc}',
            exc,
        )

def get_task_info(
    self: PostgresBroker,
    task_id: UUID,
    *,
    include_result: bool = False,
    include_failed_reason: bool = False,
    include_attempts: bool = False,
) -> BrokerResult[TaskInfo | None]:
    """Synchronous wrapper for get_task_info_async().

    On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
    and does no I/O.
    """
    if in_running_event_loop():
        return _async_context_err(
            'PostgresBroker.get_task_info()',
            'broker.get_task_info_async(...)',
        )
    try:
        return self.run_sync(
            get_task_info_async,
            self,
            task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
            include_attempts=include_attempts,
        )
    except LoopRunnerError as exc:
        return _loop_runner_err(f'PostgresBroker.get_task_info({task_id!r})', exc)
