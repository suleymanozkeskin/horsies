"""Child-process helpers: import resolution, heartbeat, task execution."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import os
import signal
import socket
import threading
import time
from collections.abc import Sequence
from importlib import import_module
from typing import Any, Optional, Tuple, cast

from psycopg import Connection, Cursor, InterfaceError, OperationalError
from psycopg.errors import DeadlockDetected, SerializationFailure

from horsies.core.app import Horsies
from horsies.core.codec.serde import (
    loads_json,
    json_to_args,
    json_to_kwargs,
    dumps_json,
    serialize_error_payload,
    SerializationError,
    task_result_from_json,
)
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.types.result import is_err


def _serialization_error_response(
    task_name: str,
    error: SerializationError,
) -> tuple[bool, str, str | None]:
    """Build a WORKER_SERIALIZATION_ERROR response for a serde failure."""
    logger.error(f'Serialization error for task {task_name}: {error}')
    tr: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=LibraryErrorCode.WORKER_SERIALIZATION_ERROR,
            message=str(error),
            data={'task_name': task_name},
        ),
    )
    return (True, serialize_error_payload(tr), f'SerializationError: {error}')
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.logging import get_logger
from horsies.core.worker.current import get_current_app, set_current_app
from horsies.core.worker.child_pool import (
    _get_worker_pool,
    _initialize_worker_pool,
)
from horsies.core.utils.imports import import_file_path
import sys

logger = get_logger('worker')


def _dedupe_paths(paths: Sequence[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for path in paths:
        if not path:
            continue
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def _debug_imports_enabled() -> bool:
    return os.getenv('HORSIES_DEBUG_IMPORTS', '').strip() == '1'


def _debug_imports_log(message: str) -> None:
    if _debug_imports_enabled():
        logger.debug(message)


def _derive_sys_path_roots_from_file(file_path: str) -> list[str]:
    """Derive sys.path roots from a file path.

    Simple and explicit: just the file's parent directory.
    No traversal for pyproject.toml to avoid monorepo collisions.
    sys_path_roots from CLI are the authoritative source.
    """
    abs_path = os.path.realpath(file_path)
    parent_dir = os.path.dirname(abs_path)
    return [parent_dir] if parent_dir else []


def _build_sys_path_roots(
    app_locator: str,
    imports: Sequence[str],
    extra_roots: Sequence[str],
) -> list[str]:
    roots: list[str] = []
    for root in extra_roots:
        if root:
            roots.append(os.path.abspath(root))
    if app_locator and ':' in app_locator:
        mod_path = app_locator.split(':', 1)[0]
        if mod_path.endswith('.py') or os.path.sep in mod_path:
            roots.extend(_derive_sys_path_roots_from_file(mod_path))
    for mod in imports:
        if mod.endswith('.py') or os.path.sep in mod:
            roots.extend(_derive_sys_path_roots_from_file(mod))
    return _dedupe_paths(roots)


def import_by_path(path: str, module_name: str | None = None) -> Any:
    """Import module from file path."""
    return import_file_path(path, module_name)


# ---------- helpers for child processes ----------
def _locate_app(app_locator: str) -> Horsies:
    """
    app_locator examples:
      - 'package.module:app'         -> import module, take variable attr
      - '/abs/path/to/file.py:app'   -> load from file path
    """
    logger.info(f'Locating app from {app_locator}')
    if not app_locator or ':' not in app_locator:
        raise ConfigurationError(
            message='invalid app locator format',
            code=ErrorCode.WORKER_INVALID_LOCATOR,
            notes=[f'got: {app_locator!r}'],
            help_text="use 'module.path:app' or '/path/to/file.py:app'",
        )
    mod_path, _, attr = app_locator.partition(':')
    if mod_path.endswith('.py') or os.path.sep in mod_path:
        mod = import_by_path(mod_path)
    else:
        mod = import_module(mod_path)
    obj = getattr(mod, attr)
    if not isinstance(obj, Horsies):
        raise ConfigurationError(
            message='app locator did not resolve to Horsies instance',
            code=ErrorCode.WORKER_INVALID_LOCATOR,
            notes=[
                f'locator: {app_locator!r}',
                f'resolved to: {type(obj).__name__}',
            ],
            help_text='ensure the locator points to a Horsies app instance',
        )
    return obj


def _child_initializer(
    app_locator: str,
    imports: Sequence[str],
    sys_path_roots: Sequence[str],
    loglevel: int,
    database_url: str,
) -> None:
    # Ignore SIGINT in child processes - let the parent handle shutdown gracefully
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Set log level for this child process before any logging
    from horsies.core.logging import set_default_level

    set_default_level(loglevel)

    # Mark child process to adjust logging behavior during module import
    os.environ['HORSIES_CHILD_PROCESS'] = '1'
    app_mod_path = app_locator.split(':', 1)[0]
    app_mod_abs = (
        os.path.abspath(app_mod_path) if app_mod_path.endswith('.py') else None
    )
    sys_path_roots_resolved = _build_sys_path_roots(
        app_locator, imports, sys_path_roots
    )
    _debug_imports_log(
        f'[child {os.getpid()}] app_locator={app_locator!r} sys_path_roots={sys_path_roots_resolved}'
    )
    for root in sys_path_roots_resolved:
        if root not in sys.path:
            sys.path.insert(0, root)

    app = _locate_app(app_locator)  # uses import_by_path -> loads as 'instance'
    set_current_app(app)

    # Suppress accidental sends while importing modules for discovery
    try:
        app.suppress_sends(True)
    except Exception:
        pass

    try:
        combined_imports = list(imports)
        try:
            combined_imports.extend(app.get_discovered_task_modules())
        except Exception:
            pass
        combined_imports = _dedupe_paths(combined_imports)
        _debug_imports_log(f'[child {os.getpid()}] import_modules={combined_imports}')
        for m in combined_imports:
            if m.endswith('.py') or os.path.sep in m:
                m_abs = os.path.abspath(m)
                if app_mod_abs and os.path.samefile(m_abs, app_mod_abs):
                    continue  # don't import the app file again
                import_by_path(m_abs)
            else:
                import_module(m)
    finally:
        try:
            app.suppress_sends(False)
        except Exception:
            pass

    # optional: sanity log
    try:
        keys = app.tasks.keys_list()
    except Exception:
        keys = list(app.tasks.keys())
    _debug_imports_log(f'[child {os.getpid()}] registered_tasks={keys}')

    # Initialize per-process connection pool (after all imports complete)
    _initialize_worker_pool(database_url)
    logger.debug(f'[child {os.getpid()}] Connection pool initialized')


# ---------- Child-process execution ----------
def _heartbeat_worker(
    task_id: str,
    database_url: str,
    stop_event: threading.Event,
    sender_worker_id: str,
    heartbeat_interval_ms: int = 30_000,
) -> None:
    """
    Runs in a separate thread within the task process.
    Sends heartbeats at configured interval until stopped or process dies.

    Uses the per-process connection pool for efficient connection reuse.

    Args:
        task_id: The task ID
        database_url: Database connection string (unused, kept for compatibility)
        stop_event: Event to signal thread termination
        sender_worker_id: Worker instance ID
        heartbeat_interval_ms: Milliseconds between heartbeats (default: 30000 = 30s)
    """
    # Convert to seconds only for threading.Event.wait()
    heartbeat_interval_seconds = heartbeat_interval_ms / 1000.0

    def send_heartbeat() -> bool:
        try:
            pool = _get_worker_pool()
            with pool.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
                    VALUES (%s, %s, 'runner', NOW(), %s, %s)
                    """,
                    (
                        task_id,
                        sender_worker_id + f':{os.getpid()}',
                        socket.gethostname(),
                        os.getpid(),
                    ),
                )
                conn.commit()
                cursor.close()
            return True
        except Exception as e:
            logger.error(f'Heartbeat failed for task {task_id}: {e}')
            return False

    # Send an immediate heartbeat so freshly RUNNING tasks aren't considered stale
    send_heartbeat()

    while not stop_event.is_set():
        # Wait for interval, but check stop_event periodically
        if stop_event.wait(timeout=heartbeat_interval_seconds):
            break  # stop_event was set

        # Use pooled connection for heartbeat
        if not send_heartbeat():
            break


def _is_retryable_db_error(exc: BaseException) -> bool:
    match exc:
        case (
            OperationalError()
            | InterfaceError()
            | SerializationFailure()
            | DeadlockDetected()
        ):
            return True
        case _:
            return False


def _get_workflow_status_for_task(cursor: Cursor[Any], task_id: str) -> str | None:
    cursor.execute(
        """
        SELECT w.status FROM horsies_workflows w
        JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
        WHERE wt.task_id = %s
        """,
        (task_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    status = row[0]
    if isinstance(status, str):
        return status
    return None


def _handle_workflow_stop_before_start(
    cursor: Cursor[Any],
    conn: Connection[Any],
    task_id: str,
    workflow_status: str,
) -> Tuple[bool, str, Optional[str]]:
    logger.info(
        f'Blocking task {task_id} execution before start - workflow is {workflow_status}'
    )
    match workflow_status:
        case 'PAUSED':
            # Pause is resumable: put task back to claimable state.
            cursor.execute(
                """
                UPDATE horsies_tasks
                SET status = 'PENDING',
                    claimed = FALSE,
                    claimed_at = NULL,
                    claimed_by_worker_id = NULL,
                    claim_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'CLAIMED'
                """,
                (task_id,),
            )
            cursor.execute(
                """
                UPDATE horsies_workflow_tasks
                SET status = 'READY',
                    task_id = NULL,
                    started_at = NULL
                WHERE task_id = %s
                  AND status = 'ENQUEUED'
                """,
                (task_id,),
            )
            conn.commit()
            return (False, '', 'WORKFLOW_STOPPED')
        case 'CANCELLED':
            # Cancellation is terminal: mark both task and workflow_task terminal.
            cursor.execute(
                """
                UPDATE horsies_workflow_tasks
                SET status = 'SKIPPED',
                    completed_at = NOW()
                WHERE task_id = %s
                  AND status IN ('ENQUEUED', 'READY')
                """,
                (task_id,),
            )
            cursor.execute(
                """
                UPDATE horsies_tasks
                SET status = 'CANCELLED',
                    claimed = FALSE,
                    claimed_at = NULL,
                    claimed_by_worker_id = NULL,
                    claim_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = %s
                  AND status IN ('CLAIMED', 'PENDING')
                """,
                (task_id,),
            )
            conn.commit()
            return (False, '', 'WORKFLOW_STOPPED')
        case _:
            return (False, '', 'WORKFLOW_CHECK_FAILED')


def _update_workflow_task_running_with_retry(task_id: str) -> None:
    backoff_seconds = (0.0, 0.25, 0.75)
    total_attempts = len(backoff_seconds)
    for attempt_index, delay in enumerate(backoff_seconds):
        if delay > 0:
            time.sleep(delay)
        try:
            pool = _get_worker_pool()
            with pool.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE horsies_workflow_tasks wt
                    SET status = 'RUNNING', started_at = NOW()
                    FROM horsies_workflows w
                    WHERE wt.task_id = %s
                      AND wt.status = 'ENQUEUED'
                      AND wt.workflow_id = w.id
                      AND w.status = 'RUNNING'
                    """,
                    (task_id,),
                )
                conn.commit()
            return
        except Exception as exc:
            retryable = _is_retryable_db_error(exc)
            is_last_attempt = attempt_index == total_attempts - 1
            match (retryable, is_last_attempt):
                case (True, True):
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}: {exc}'
                    )
                    return
                case (True, False):
                    logger.warning(
                        f'Retrying workflow_tasks RUNNING update for task {task_id}: {exc}'
                    )
                    continue
                case (False, _):
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}: {exc}'
                    )
                    return
                case _:
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}: {exc}'
                    )
                    return


def _preflight_workflow_check(
    task_id: str,
) -> Optional[Tuple[bool, str, Optional[str]]]:
    try:
        pool = _get_worker_pool()
        with pool.connection() as conn:
            cursor = conn.cursor()
            workflow_status = _get_workflow_status_for_task(cursor, task_id)
            match workflow_status:
                case 'PAUSED' | 'CANCELLED':
                    return _handle_workflow_stop_before_start(
                        cursor,
                        conn,
                        task_id,
                        workflow_status,
                    )
                case _:
                    return None
    except Exception as e:
        logger.error(f'Failed to check workflow status for task {task_id}: {e}')
        return (False, '', 'WORKFLOW_CHECK_FAILED')


def _confirm_ownership_and_set_running(
    task_id: str,
    worker_id: str,
) -> Optional[Tuple[bool, str, Optional[str]]]:
    try:
        pool = _get_worker_pool()
        with pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE horsies_tasks
                SET status = 'RUNNING',
                    claimed = FALSE,
                    claim_expires_at = NULL,
                    started_at = NOW(),
                    worker_pid = %s,
                    worker_hostname = %s,
                    worker_process_name = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'CLAIMED'
                  AND claimed_by_worker_id = %s
                  AND (claim_expires_at IS NULL OR claim_expires_at > now())
                  AND NOT EXISTS (
                      SELECT 1
                      FROM horsies_workflow_tasks wt
                      JOIN horsies_workflows w ON w.id = wt.workflow_id
                      WHERE wt.task_id = %s
                        AND w.status IN ('PAUSED', 'CANCELLED')
                  )
                RETURNING id
                """,
                (
                    os.getpid(),
                    socket.gethostname(),
                    f'worker-{os.getpid()}',
                    task_id,
                    worker_id,
                    task_id,
                ),
            )
            updated_row = cursor.fetchone()
            if updated_row is None:
                workflow_status = _get_workflow_status_for_task(cursor, task_id)
                match workflow_status:
                    case 'PAUSED' | 'CANCELLED':
                        return _handle_workflow_stop_before_start(
                            cursor,
                            conn,
                            task_id,
                            workflow_status,
                        )
                    case _:
                        conn.rollback()
                        logger.warning(
                            f'Task {task_id} ownership lost - claim was reclaimed or status changed. '
                            f'Aborting execution to prevent double-execution.'
                        )
                        return (False, '', 'CLAIM_LOST')

            conn.commit()

        _update_workflow_task_running_with_retry(task_id)
        return None
    except Exception as e:
        logger.error(f'Failed to transition task {task_id} to RUNNING: {e}')
        return (False, '', 'OWNERSHIP_UNCONFIRMED')


def _start_heartbeat_thread(
    task_id: str,
    database_url: str,
    heartbeat_stop_event: threading.Event,
    worker_id: str,
    runner_heartbeat_interval_ms: int,
) -> threading.Thread:
    heartbeat_thread = threading.Thread(
        target=_heartbeat_worker,
        args=(
            task_id,
            database_url,
            heartbeat_stop_event,
            worker_id,
            runner_heartbeat_interval_ms,
        ),
        daemon=True,
        name=f'heartbeat-{task_id[:8]}',
    )
    heartbeat_thread.start()
    return heartbeat_thread


def _run_task_entry(
    task_name: str,
    args_json: Optional[str],
    kwargs_json: Optional[str],
    task_id: str,
    database_url: str,
    master_worker_id: str,
    runner_heartbeat_interval_ms: int = 30_000,
) -> Tuple[bool, str, Optional[str]]:
    """
    Child-process entry.
    Returns:
      (ok, serialized_task_result_json, worker_failure_reason)

    ok=True: we produced a valid TaskResult JSON (success *or* failure).
    ok=False: worker couldn't even produce a TaskResult (infra error).

    Do not mistake the 'ok' here as the task's own success/failure.
    It's only a signal that the worker was able to produce a valid JSON.
    The task's own success/failure is determined by the TaskResult's ok/err fields.
    """
    logger.info(f'Starting task execution: {task_name}')

    # Pre-execute guard: check if workflow is PAUSED or CANCELLED
    # If so, skip execution and mark task as SKIPPED
    preflight_result = _preflight_workflow_check(task_id)
    if preflight_result is not None:
        return preflight_result

    # Mark as RUNNING in DB at the actual start of execution (child process)
    # CRITICAL: Include ownership check to prevent double-execution when claim lease expires
    ownership_result = _confirm_ownership_and_set_running(task_id, master_worker_id)
    if ownership_result is not None:
        return ownership_result

    # Start heartbeat monitoring in separate thread
    heartbeat_stop_event = threading.Event()
    _start_heartbeat_thread(
        task_id,
        database_url,
        heartbeat_stop_event,
        master_worker_id,
        runner_heartbeat_interval_ms,
    )

    try:
        # Resolve from TaskRegistry
        try:
            app = get_current_app()
            task = app.tasks[task_name]  # may raise NotRegistered
        except Exception as e:
            logger.error(f'Failed to resolve task {task_name}: {e}')
            tr: TaskResult[Any, TaskError] = TaskResult(
                err=TaskError(
                    error_code=LibraryErrorCode.WORKER_RESOLUTION_ERROR,
                    message=f'Failed to resolve {task_name}: {type(e).__name__}: {e}',
                    data={'task_name': task_name},
                )
            )
            # Stop heartbeat thread since task failed to resolve
            heartbeat_stop_event.set()
            logger.debug(
                f'Heartbeat stop signal sent for task {task_id} (resolution failed)'
            )
            return (True, serialize_error_payload(tr), f'{type(e).__name__}: {e}')

        # Deserialize task arguments
        args_json_result = loads_json(args_json)
        if is_err(args_json_result):
            return _serialization_error_response(task_name, args_json_result.err_value)
        args_result = json_to_args(args_json_result.ok_value)
        if is_err(args_result):
            return _serialization_error_response(task_name, args_result.err_value)
        args = args_result.ok_value

        kwargs_json_result = loads_json(kwargs_json)
        if is_err(kwargs_json_result):
            return _serialization_error_response(task_name, kwargs_json_result.err_value)
        kwargs_result = json_to_kwargs(kwargs_json_result.ok_value)
        if is_err(kwargs_result):
            return _serialization_error_response(task_name, kwargs_result.err_value)
        kwargs = kwargs_result.ok_value

        # Deserialize injected TaskResults from workflow args_from
        for key, value in list(kwargs.items()):
            if (
                isinstance(value, dict)
                and '__horsies_taskresult__' in value
                and value['__horsies_taskresult__']
            ):
                task_result_dict = cast(dict[str, Any], value)
                data_str = task_result_dict.get('data')
                if isinstance(data_str, str):
                    data_json_result = loads_json(data_str)
                    if is_err(data_json_result):
                        return _serialization_error_response(task_name, data_json_result.err_value)
                    tr_result = task_result_from_json(data_json_result.ok_value)
                    if is_err(tr_result):
                        return _serialization_error_response(task_name, tr_result.err_value)
                    kwargs[key] = tr_result.ok_value

        # Handle workflow injection payloads (workflow_ctx / workflow_meta).
        workflow_ctx_data = kwargs.pop('__horsies_workflow_ctx__', None)
        workflow_meta_data = kwargs.pop('__horsies_workflow_meta__', None)
        if workflow_ctx_data is not None or workflow_meta_data is not None:
            import inspect

            underlying_fn = getattr(task, '_original_fn', getattr(task, '_fn', task))
            sig = inspect.signature(underlying_fn)
            if workflow_ctx_data is not None and 'workflow_ctx' in sig.parameters:
                from horsies.core.models.workflow import (
                    WorkflowContext,
                    SubWorkflowSummary,
                )

                results_by_id_raw = workflow_ctx_data.get('results_by_id', {})
                results_by_id: dict[str, TaskResult[Any, TaskError]] = {}
                for node_id, result_json in results_by_id_raw.items():
                    if isinstance(result_json, str):
                        rj = loads_json(result_json)
                        if is_err(rj):
                            return _serialization_error_response(task_name, rj.err_value)
                        tr_r = task_result_from_json(rj.ok_value)
                        if is_err(tr_r):
                            return _serialization_error_response(task_name, tr_r.err_value)
                        results_by_id[node_id] = tr_r.ok_value

                summaries_by_id_raw = workflow_ctx_data.get('summaries_by_id', {})
                summaries_by_id: dict[str, SubWorkflowSummary[Any]] = {}
                for node_id, summary_json in summaries_by_id_raw.items():
                    if isinstance(summary_json, str):
                        sj = loads_json(summary_json)
                        if is_err(sj):
                            return _serialization_error_response(task_name, sj.err_value)
                        parsed = sj.ok_value
                        if isinstance(parsed, dict):
                            summaries_by_id[node_id] = SubWorkflowSummary.from_json(
                                parsed,
                            )

                kwargs['workflow_ctx'] = WorkflowContext.from_serialized(
                    workflow_id=workflow_ctx_data.get('workflow_id', ''),
                    task_index=workflow_ctx_data.get('task_index', 0),
                    task_name=workflow_ctx_data.get('task_name', ''),
                    results_by_id=results_by_id,
                    summaries_by_id=summaries_by_id,
                )
            # If task doesn't declare workflow_ctx, silently skip injection.

            if workflow_meta_data is not None and 'workflow_meta' in sig.parameters:
                from horsies.core.models.workflow import WorkflowMeta

                if isinstance(workflow_meta_data, dict):
                    meta_dict = cast(dict[str, Any], workflow_meta_data)
                    task_index_raw: Any = meta_dict.get('task_index', 0)
                    try:
                        task_index_value = int(task_index_raw)
                    except (TypeError, ValueError):
                        task_index_value = 0

                    kwargs['workflow_meta'] = WorkflowMeta(
                        workflow_id=str(meta_dict.get('workflow_id', '')),
                        task_index=task_index_value,
                        task_name=str(meta_dict.get('task_name', '')),
                    )
            # If task doesn't declare workflow_meta, silently skip injection.

        out = task(*args, **kwargs)  # __call__ returns TaskResult
        logger.info(f'Task execution completed: {[task_id]} : {[task_name]}')

        if isinstance(out, TaskResult):
            ser = dumps_json(out)
            if is_err(ser):
                return _serialization_error_response(task_name, ser.err_value)
            return (True, ser.ok_value, None)

        if out is None:
            tr: TaskResult[Any, TaskError] = TaskResult(
                err=TaskError(
                    error_code=LibraryErrorCode.TASK_EXCEPTION,
                    message=f'Task {task_name} returned None instead of TaskResult or value',
                    data={'task_name': task_name},
                ),
            )
            return (True, serialize_error_payload(tr), 'Task returned None')

        # Plain value → wrap into success
        ser = dumps_json(TaskResult(ok=out))
        if is_err(ser):
            return _serialization_error_response(task_name, ser.err_value)
        return (True, ser.ok_value, None)

    except BaseException as e:
        logger.error(f'Task exception: {task_name}: {e}')
        # If the task raised an exception, we wrap it in a TaskError
        tr = TaskResult(
            err=TaskError(
                error_code=LibraryErrorCode.TASK_EXCEPTION,
                message=f'{type(e).__name__}: {e}',
                data={'task_name': task_name},
                exception=e,  # pydantic will accept and we flatten on serialization if needed elsewhere
            )
        )
        return (True, serialize_error_payload(tr), None)

    finally:
        # Always stop the heartbeat thread when task completes (success/failure/error)
        heartbeat_stop_event.set()
        logger.debug(f'Heartbeat stop signal sent for task {task_id}')


_CHILD_RUNNER_ENTRYPOINTS = (_child_initializer, _run_task_entry)
