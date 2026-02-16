# app/core/worker.py
from __future__ import annotations
import asyncio
import uuid
import os
import random
import signal
import socket
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from importlib import import_module
from typing import Any, Optional, Sequence, Tuple, cast
import atexit
import hashlib
from psycopg import Connection, Cursor, InterfaceError, OperationalError
from psycopg.errors import DeadlockDetected, SerializationFailure
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from horsies.core.app import Horsies
from horsies.core.brokers.listener import PostgresListener
from horsies.core.codec.serde import (
    loads_json,
    json_to_args,
    json_to_kwargs,
    dumps_json,
    SerializationError,
    task_result_from_json,
)
from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.logging import get_logger
from horsies.core.worker.current import get_current_app, set_current_app
import sys
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.imports import import_file_path

logger = get_logger('worker')

# ---------- Per-process connection pool (initialized in child processes) ----------
_worker_pool: ConnectionPool | None = None


def _get_worker_pool() -> ConnectionPool:
    """Get the per-process connection pool. Raises if not initialized."""
    if _worker_pool is None:
        raise RuntimeError(
            'Worker connection pool not initialized. '
            'This function must be called from a child worker process.'
        )
    return _worker_pool


def _cleanup_worker_pool() -> None:
    """Clean up the connection pool on process exit."""
    global _worker_pool
    if _worker_pool is not None:
        try:
            _worker_pool.close()
        except Exception:
            pass
        _worker_pool = None


def _initialize_worker_pool(database_url: str) -> None:
    """
    Initialize the per-process connection pool.

    In production: Called by _child_initializer in spawned worker processes.
    In tests: Can be called directly to set up the pool for direct _run_task_entry calls.
    """
    global _worker_pool
    if _worker_pool is not None:
        return  # Already initialized
    _worker_pool = ConnectionPool(
        database_url,
        min_size=1,
        max_size=5,
        max_lifetime=300.0,
        check=ConnectionPool.check_connection,
        open=True,
    )
    atexit.register(_cleanup_worker_pool)


def _default_str_list() -> list[str]:
    return []


def _default_str_int_dict() -> dict[str, int]:
    return {}


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
    global _worker_pool

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


@dataclass
class _RetryBackoff:
    initial_ms: int
    max_ms: int
    max_attempts: int
    attempts: int = 0

    def reset(self) -> None:
        self.attempts = 0

    def can_retry(self) -> bool:
        match self.max_attempts:
            case 0:
                return True
            case _:
                return self.attempts < self.max_attempts

    def next_delay_seconds(self) -> float:
        self.attempts += 1
        exponent = max(0, self.attempts - 1)
        base_ms = min(self.max_ms, int(self.initial_ms * (2**exponent)))
        jitter_range = base_ms * 0.25
        delay_ms = base_ms + random.uniform(-jitter_range, jitter_range)
        return max(0.1, delay_ms / 1000.0)


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


def _mark_task_skipped_for_workflow_stop(
    cursor: Cursor[Any],
    conn: Connection[Any],
    task_id: str,
    workflow_status: str,
) -> Tuple[bool, str, Optional[str]]:
    logger.info(f'Skipping task {task_id} - workflow is {workflow_status}')
    cursor.execute(
        """
        UPDATE horsies_workflow_tasks
        SET status = 'SKIPPED'
        WHERE task_id = %s AND status IN ('ENQUEUED', 'READY')
        """,
        (task_id,),
    )
    result: TaskResult[Any, TaskError] = TaskResult(
        err=TaskError(
            error_code='WORKFLOW_STOPPED',
            message=f'Task skipped - workflow is {workflow_status}',
        )
    )
    result_json = dumps_json(result)
    cursor.execute(
        """
        UPDATE horsies_tasks
        SET status = 'COMPLETED',
            result = %s,
            completed_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
        """,
        (result_json, task_id),
    )
    conn.commit()
    return (True, result_json, f'Workflow {workflow_status}')


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
                    return _mark_task_skipped_for_workflow_stop(
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
                        return _mark_task_skipped_for_workflow_stop(
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
            task = app.tasks[task_name]  # may raise NotRegistered(KeyError)
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
            return (True, dumps_json(tr), f'{type(e).__name__}: {e}')

        args = json_to_args(loads_json(args_json))
        kwargs = json_to_kwargs(loads_json(kwargs_json))

        # Deserialize injected TaskResults from workflow args_from
        for key, value in list(kwargs.items()):
            if (
                isinstance(value, dict)
                and '__horsies_taskresult__' in value
                and value['__horsies_taskresult__']
            ):
                # value is a dict with "data" key containing serialized TaskResult
                task_result_dict = cast(dict[str, Any], value)
                data_str = task_result_dict.get('data')
                if isinstance(data_str, str):
                    kwargs[key] = task_result_from_json(loads_json(data_str))

        # Handle workflow injection payloads (workflow_ctx / workflow_meta).
        workflow_ctx_data = kwargs.pop('__horsies_workflow_ctx__', None)
        workflow_meta_data = kwargs.pop('__horsies_workflow_meta__', None)
        if workflow_ctx_data is not None or workflow_meta_data is not None:
            import inspect

            # Access the underlying function: TaskFunctionImpl stores it in _original_fn
            underlying_fn = getattr(task, '_original_fn', getattr(task, '_fn', task))
            sig = inspect.signature(underlying_fn)
            if workflow_ctx_data is not None and 'workflow_ctx' in sig.parameters:
                from horsies.core.models.workflow import (
                    WorkflowContext,
                    SubWorkflowSummary,
                )

                # Reconstruct TaskResults from serialized data (node_id-based)
                results_by_id_raw = workflow_ctx_data.get('results_by_id', {})

                results_by_id: dict[str, TaskResult[Any, TaskError]] = {}
                for node_id, result_json in results_by_id_raw.items():
                    if isinstance(result_json, str):
                        results_by_id[node_id] = task_result_from_json(
                            loads_json(result_json)
                        )

                # Reconstruct SubWorkflowSummaries from serialized data (node_id-based)
                summaries_by_id_raw = workflow_ctx_data.get('summaries_by_id', {})
                summaries_by_id: dict[str, SubWorkflowSummary[Any]] = {}
                for node_id, summary_json in summaries_by_id_raw.items():
                    if isinstance(summary_json, str):
                        parsed = loads_json(summary_json)
                        if isinstance(parsed, dict):
                            summaries_by_id[node_id] = SubWorkflowSummary.from_json(
                                parsed
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
            return (True, dumps_json(out), None)

        if out is None:
            tr: TaskResult[Any, TaskError] = TaskResult(
                err=TaskError(
                    error_code=LibraryErrorCode.TASK_EXCEPTION,
                    message=f'Task {task_name} returned None instead of TaskResult or value',
                    data={'task_name': task_name},
                )
            )
            return (True, dumps_json(tr), 'Task returned None')

        # Plain value → wrap into success
        return (True, dumps_json(TaskResult(ok=out)), None)

    except SerializationError as se:
        logger.error(f'Serialization error for task {task_name}: {se}')
        tr = TaskResult(
            err=TaskError(
                error_code=LibraryErrorCode.WORKER_SERIALIZATION_ERROR,
                message=str(se),
                data={'task_name': task_name},
            )
        )
        return (True, dumps_json(tr), f'SerializationError: {se}')

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
        return (True, dumps_json(tr), None)

    finally:
        # Always stop the heartbeat thread when task completes (success/failure/error)
        heartbeat_stop_event.set()
        logger.debug(f'Heartbeat stop signal sent for task {task_id}')


# ---------- Claim SQL (priority + sent_at) ----------
# Supports both hard cap mode (claim_expires_at = NULL) and soft cap mode with lease.
# In soft cap mode, also reclaims tasks with expired claim leases.

CLAIM_SQL = text("""
WITH next AS (
  SELECT id
  FROM horsies_tasks
  WHERE queue_name = :queue
    AND (
      -- Fresh pending tasks
      status = 'PENDING'
      -- OR expired claims (soft cap mode: lease expired, reclaim for this worker)
      OR (status = 'CLAIMED' AND claim_expires_at IS NOT NULL AND claim_expires_at < now())
    )
    AND sent_at <= now()
    AND (next_retry_at IS NULL OR next_retry_at <= now())
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, sent_at ASC, id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT :lim
)
UPDATE horsies_tasks t
SET status = 'CLAIMED',
    claimed = TRUE,
    claimed_at = now(),
    claimed_by_worker_id = :worker_id,
    claim_expires_at = :claim_expires_at,
    updated_at = now()
FROM next
WHERE t.id = next.id
RETURNING t.id;
""")

# Fetch rows for a list of ids (to get func_path/args/etc.)
LOAD_ROWS_SQL = text("""
SELECT id, task_name, args, kwargs, retry_count, max_retries, task_options
FROM horsies_tasks
WHERE id = ANY(:ids)
""")


# ---------- WorkerConfig ----------
@dataclass
class WorkerConfig:
    dsn: str  # SQLAlchemy async URL (e.g. postgresql+psycopg://...)
    psycopg_dsn: str  # plain psycopg URL for listener
    queues: list[str]  # which queues to serve
    processes: int = os.cpu_count() or 2
    # Claiming knobs
    # max_claim_batch: Top-level fairness limiter to prevent worker starvation in multi-worker setups.
    # Limits claims per queue per pass, regardless of available capacity. Increase for high-concurrency workloads.
    max_claim_batch: int = 2
    # max_claim_per_worker: Per-worker limit on total CLAIMED tasks to prevent over-claiming.
    # 0 = auto (defaults to processes). Increase for deeper prefetch if tasks start very quickly.
    max_claim_per_worker: int = 0
    coalesce_notifies: int = 100  # drain up to N notes after wake
    app_locator: str = ''  # NEW (see _locate_app)
    sys_path_roots: list[str] = field(default_factory=_default_str_list)
    imports: list[str] = field(
        default_factory=_default_str_list
    )  # modules that contain @app.task defs
    # When in CUSTOM mode, provide per-queue settings {name: {priority, max_concurrency}}
    queue_priorities: dict[str, int] = field(default_factory=_default_str_int_dict)
    queue_max_concurrency: dict[str, int] = field(default_factory=_default_str_int_dict)
    cluster_wide_cap: Optional[int] = None
    # Prefetch buffer: 0 = hard cap mode (count RUNNING + CLAIMED), >0 = soft cap with lease
    prefetch_buffer: int = 0
    # Claim lease duration in ms. Required when prefetch_buffer > 0.
    claim_lease_ms: Optional[int] = None
    # Recovery configuration from AppConfig
    recovery_config: Optional['RecoveryConfig'] = (
        None  # RecoveryConfig, avoid circular import
    )
    resilience_config: Optional['WorkerResilienceConfig'] = (
        None  # WorkerResilienceConfig, allow override
    )
    # Log level for worker processes (default: INFO)
    loglevel: int = 20  # logging.INFO


# ---------- Worker SQL constants ----------

CLAIM_ADVISORY_LOCK_SQL = text("""
    SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))
""")

COUNT_GLOBAL_IN_FLIGHT_SQL = text("""
    SELECT COUNT(*) FROM horsies_tasks WHERE status IN ('RUNNING', 'CLAIMED')
""")

COUNT_QUEUE_IN_FLIGHT_HARD_SQL = text("""
    SELECT COUNT(*) FROM horsies_tasks WHERE status IN ('RUNNING', 'CLAIMED') AND queue_name = :q
""")

COUNT_QUEUE_IN_FLIGHT_SOFT_SQL = text("""
    SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = :q
""")

COUNT_CLAIMED_FOR_WORKER_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status = 'CLAIMED'
""")

COUNT_RUNNING_FOR_WORKER_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status = 'RUNNING'
""")

COUNT_IN_FLIGHT_FOR_WORKER_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status IN ('RUNNING', 'CLAIMED')
""")

COUNT_RUNNING_IN_QUEUE_SQL = text("""
    SELECT COUNT(*)
    FROM horsies_tasks
    WHERE status = 'RUNNING'
      AND queue_name = :q
""")

GET_PAUSED_WORKFLOW_TASK_IDS_SQL = text("""
    SELECT t.id
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE t.id = ANY(:ids)
      AND w.status = 'PAUSED'
""")

UNCLAIM_PAUSED_TASKS_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        updated_at = NOW()
    WHERE id = ANY(:ids)
""")

UNCLAIM_CLAIMED_TASK_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        updated_at = NOW()
    WHERE id = :id
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
""")

RESET_PAUSED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY', task_id = NULL, started_at = NULL
    WHERE task_id = ANY(:ids)
""")

MARK_TASK_FAILED_WORKER_SQL = text("""
    UPDATE horsies_tasks
    SET status='FAILED',
        failed_at = :now,
        failed_reason = :reason,
        updated_at = :now
    WHERE id = :id
      AND status = 'RUNNING'
    RETURNING id
""")

MARK_TASK_FAILED_SQL = text("""
    UPDATE horsies_tasks
    SET status='FAILED',
        failed_at = :now,
        result = :result_json,
        updated_at = :now
    WHERE id = :id
      AND status = 'RUNNING'
    RETURNING id
""")

MARK_TASK_COMPLETED_SQL = text("""
    UPDATE horsies_tasks
    SET status='COMPLETED',
        completed_at = :now,
        result = :result_json,
        updated_at = :now
    WHERE id = :id
      AND status = 'RUNNING'
    RETURNING id
""")

GET_TASK_QUEUE_NAME_SQL = text("""
    SELECT queue_name FROM horsies_tasks WHERE id = :id
""")

NOTIFY_TASK_NEW_SQL = text("""
    SELECT pg_notify(:c1, :p)
""")

NOTIFY_TASK_QUEUE_SQL = text("""
    SELECT pg_notify(:c2, :p)
""")

CHECK_WORKFLOW_TASK_EXISTS_SQL = text("""
    SELECT 1 FROM horsies_workflow_tasks WHERE task_id = :tid LIMIT 1
""")

GET_TASK_RETRY_INFO_SQL = text("""
    SELECT retry_count, max_retries, task_options FROM horsies_tasks WHERE id = :id
""")

GET_TASK_RETRY_CONFIG_SQL = text("""
    SELECT retry_count, task_options FROM horsies_tasks WHERE id = :id
""")

SCHEDULE_TASK_RETRY_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        retry_count = :retry_count,
        next_retry_at = :next_retry_at,
        sent_at = :next_retry_at,
        updated_at = now()
    WHERE id = :id
      AND status = 'RUNNING'
    RETURNING id
""")

NOTIFY_DELAYED_SQL = text("""
    SELECT pg_notify(:channel, :payload)
""")

INSERT_CLAIMER_HEARTBEAT_SQL = text("""
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT id, CAST(:wid AS VARCHAR), 'claimer', NOW(), :host, :pid
    FROM horsies_tasks
    WHERE status = 'CLAIMED' AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
""")

INSERT_WORKER_STATE_SQL = text("""
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid,
        processes, max_claim_batch, max_claim_per_worker,
        cluster_wide_cap, queues, queue_priorities, queue_max_concurrency,
        recovery_config, tasks_running, tasks_claimed,
        memory_usage_mb, memory_percent, cpu_percent,
        worker_started_at
    )
    VALUES (
        :wid, NOW(), :host, :pid, :procs, :mcb, :mcpw, :cwc,
        :queues, :qp, :qmc, :recovery, :running, :claimed,
        :mem_mb, :mem_pct, :cpu_pct, :started
    )
""")

DELETE_EXPIRED_HEARTBEATS_SQL = text("""
    DELETE FROM horsies_heartbeats
    WHERE sent_at < NOW() - INTERVAL '24 hours'
""")

_HEARTBEAT_RETENTION_CLEANUP_INTERVAL_S = 3600.0


_FINALIZER_DRAIN_TIMEOUT_S: float = 30.0


def _collect_psutil_metrics() -> tuple[float, float, float]:
    """Collect process metrics. Blocking — must run in a thread."""
    import psutil

    process = psutil.Process()
    memory_info = process.memory_info()
    return (
        memory_info.rss / 1024 / 1024,
        process.memory_percent(),
        process.cpu_percent(interval=0.1),
    )


class Worker:
    """
    Async master that:
      - Subscribes to queue channels
      - Claims tasks (priority + sent_at) with SKIP LOCKED
      - Executes in a process pool
      - On completion, writes result/failed, COMMITs, and NOTIFY task_done
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        listener: PostgresListener,
        cfg: WorkerConfig,
    ):
        self.sf = session_factory
        self.listener = listener
        self.cfg = cfg
        self.worker_instance_id = str(uuid.uuid4())
        self._started_at = datetime.now(timezone.utc)
        self._app: Horsies | None = None
        self._resilience = self.cfg.resilience_config or WorkerResilienceConfig()
        # Delay creation of the process pool until after preloading modules so that
        # any import/validation errors surface in the main process at startup.
        self._executor: Optional[ProcessPoolExecutor] = None
        self._stop = asyncio.Event()
        self._service_tasks: set[asyncio.Task[Any]] = set()
        self._finalizer_tasks: set[asyncio.Task[Any]] = set()

    def request_stop(self) -> None:
        """Request worker to stop gracefully."""
        self._stop.set()

    def _spawn_background(
        self,
        coro: Any,
        *,
        name: str,
        finalizer: bool = False,
    ) -> asyncio.Task[Any]:
        """Create a tracked background task with automatic cleanup."""
        task_group = self._finalizer_tasks if finalizer else self._service_tasks
        task = asyncio.create_task(coro, name=name)
        task_group.add(task)

        def _on_done(t: asyncio.Task[Any]) -> None:
            task_group.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                logger.error(f'Background task {t.get_name()!r} failed: {exc}')

        task.add_done_callback(_on_done)
        return task

    def _create_executor(self) -> ProcessPoolExecutor:
        child_database_url = self.cfg.dsn.replace('+asyncpg', '').replace(
            '+psycopg', ''
        )
        return ProcessPoolExecutor(
            max_workers=self.cfg.processes,
            initializer=_child_initializer,
            initargs=(
                self.cfg.app_locator,
                self.cfg.imports,
                self.cfg.sys_path_roots,
                self.cfg.loglevel,
                child_database_url,
            ),
        )

    async def _restart_executor(self, reason: str) -> None:
        if self._stop.is_set():
            return
        if self._executor is None:
            self._executor = self._create_executor()
            logger.warning(f'Executor created after restart request: {reason}')
            return

        loop = asyncio.get_running_loop()
        executor = self._executor
        self._executor = None
        logger.error(f'Restarting worker executor: {reason}')
        try:
            await loop.run_in_executor(
                None, lambda: executor.shutdown(wait=True, cancel_futures=True)
            )
        except Exception as e:
            logger.error(f'Error shutting down broken executor: {e}')
        self._executor = self._create_executor()

    def _make_retry_backoff(self) -> _RetryBackoff:
        return _RetryBackoff(
            initial_ms=self._resilience.db_retry_initial_ms,
            max_ms=self._resilience.db_retry_max_ms,
            max_attempts=self._resilience.db_retry_max_attempts,
        )

    async def _sleep_with_stop(self, delay_seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay_seconds)
        except asyncio.TimeoutError:
            return

    async def _cleanup_after_failed_start(self) -> None:
        try:
            await self.listener.close()
        except Exception as e:
            logger.error(f'Error closing listener after failed start: {e}')

        if self._executor:
            loop = asyncio.get_running_loop()
            executor = self._executor
            self._executor = None
            try:
                await loop.run_in_executor(
                    None, lambda: executor.shutdown(wait=True, cancel_futures=True)
                )
            except Exception as e:
                logger.error(f'Error shutting down executor after failed start: {e}')

    async def _handle_retryable_start_error(
        self,
        exc: BaseException,
        backoff: _RetryBackoff,
    ) -> None:
        if not backoff.can_retry():
            logger.error(
                f'Worker start failed after {backoff.attempts} attempts: {exc}'
            )
            raise

        await self._cleanup_after_failed_start()
        delay = backoff.next_delay_seconds()
        logger.error(
            f'Worker start failed: {exc}. Retrying in {delay:.1f}s '
            f'(attempt {backoff.attempts}/{backoff.max_attempts or "inf"})'
        )
        await self._sleep_with_stop(delay)

    async def _start_with_resilience(self) -> None:
        backoff = self._make_retry_backoff()
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self.start(), timeout=30.0)
                return
            except asyncio.TimeoutError as exc:
                await self._handle_retryable_start_error(exc, backoff)
                continue
            except Exception as exc:
                if is_retryable_connection_error(exc):
                    await self._handle_retryable_start_error(exc, backoff)
                    continue
                raise

    # ----- lifecycle -----

    async def start(self) -> None:
        logger.debug('Starting worker')
        # Preload the app and task modules in the main process to fail fast
        self._preload_modules_main()

        # Create the process pool AFTER successful preload so initializer runs in children only
        self._executor = self._create_executor()
        await self.listener.start()
        # Surface concurrency configuration clearly for operators
        max_claimed_effective = (
            self.cfg.max_claim_per_worker
            if self.cfg.max_claim_per_worker > 0
            else self.cfg.processes
        )
        logger.info(
            'Concurrency config: processes=%s, cluster_wide_cap=%s, max_claim_per_worker=%s, max_claim_batch=%s',
            self.cfg.processes,
            (
                self.cfg.cluster_wide_cap
                if self.cfg.cluster_wide_cap is not None
                else 'unlimited'
            ),
            max_claimed_effective,
            self.cfg.max_claim_batch,
        )

        # Subscribe to each queue channel (and a global) in one batch
        all_channels = [f'task_queue_{q}' for q in self.cfg.queues] + ['task_new']
        all_queues = await self.listener.listen_many(all_channels)
        self._queues = all_queues[:-1]
        self._global = all_queues[-1]
        logger.info(f'Subscribed to queues: {self.cfg.queues} + global')
        # Start claimer heartbeat loop (CLAIMED coverage)
        self._spawn_background(
            self._claimer_heartbeat_loop(), name='claimer-heartbeat',
        )
        # Start worker state heartbeat loop for monitoring
        self._spawn_background(
            self._worker_state_heartbeat_loop(), name='worker-state-heartbeat',
        )
        logger.info('Worker state heartbeat loop started for monitoring')
        # Start reaper loop for automatic stale task handling
        if self.cfg.recovery_config:
            self._spawn_background(self._reaper_loop(), name='reaper')
            logger.info('Reaper loop started for automatic stale task recovery')

    async def stop(
        self,
        *,
        force: bool = False,
        finalizer_timeout_s: float = _FINALIZER_DRAIN_TIMEOUT_S,
    ) -> None:
        self._stop.set()
        # Service loops are safe to cancel.
        if self._service_tasks:
            service_tasks = tuple(self._service_tasks)
            for task in service_tasks:
                task.cancel()
            await asyncio.gather(*service_tasks, return_exceptions=True)
            self._service_tasks.clear()

        # Finalizers persist task outcomes and should be drained gracefully.
        if self._finalizer_tasks:
            finalizer_tasks = tuple(self._finalizer_tasks)
            if force:
                for task in finalizer_tasks:
                    task.cancel()
                await asyncio.gather(*finalizer_tasks, return_exceptions=True)
            else:
                done, pending = await asyncio.wait(
                    finalizer_tasks, timeout=max(0.0, finalizer_timeout_s)
                )
                if pending:
                    logger.warning(
                        'Worker stop timed out with %s finalize task(s) still running; cancelling pending finalizers',
                        len(pending),
                    )
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                if done:
                    await asyncio.gather(*done, return_exceptions=True)
            self._finalizer_tasks.clear()
        # Close the Postgres listener early to avoid UNLISTEN races on dispatcher connection
        try:
            await self.listener.close()
            logger.info('Postgres listener closed')
        except Exception as e:
            logger.error(f'Error closing Postgres listener: {e}')
        # Shutdown executor
        if self._executor:
            # Offload blocking shutdown to a thread to avoid freezing the event loop
            loop = asyncio.get_running_loop()
            executor = self._executor
            self._executor = None
            try:
                await loop.run_in_executor(
                    None, lambda: executor.shutdown(wait=True, cancel_futures=True)
                )  # TODO:inspect this behaviour more in depth!
            except Exception as e:
                logger.error(f'Error shutting down executor: {e}')
            logger.info('Worker executor shutdown')
        # Release per-process registries/caches on shutdown.
        try:
            from horsies.core.workflows.registry import clear_workflow_registry
            from horsies.core.codec.serde import clear_serde_caches

            clear_workflow_registry()
            clear_serde_caches()
        except Exception as e:
            logger.error(f'Error clearing workflow/serde registries: {e}')
        logger.info('Worker stopped')

    def _preload_modules_main(self) -> None:
        """Import the app and all task modules in the main process.

        This ensures Pydantic validations and module-level side effects run once
        and any configuration errors surface during startup rather than inside
        the child process initializer.
        """
        try:
            sys_path_roots_resolved = _build_sys_path_roots(
                self.cfg.app_locator, self.cfg.imports, self.cfg.sys_path_roots
            )
            _debug_imports_log(
                f'[preload] app_locator={self.cfg.app_locator!r} sys_path_roots={sys_path_roots_resolved}'
            )
            for root in sys_path_roots_resolved:
                if root not in sys.path:
                    sys.path.insert(0, root)

            # Load app object (variable or factory)
            app = _locate_app(self.cfg.app_locator)
            # Optionally set as current for consistency in main process
            set_current_app(app)
            self._app = app

            # Suppress accidental sends while importing modules for discovery
            try:
                app.suppress_sends(True)
            except Exception:
                pass

            # Import declared modules that contain task definitions
            combined_imports = list(self.cfg.imports)
            try:
                combined_imports.extend(app.get_discovered_task_modules())
            except Exception:
                pass
            combined_imports = _dedupe_paths(combined_imports)
            _debug_imports_log(f'[preload] import_modules={combined_imports}')
            for m in combined_imports:
                if m.endswith('.py') or os.path.sep in m:
                    import_by_path(os.path.abspath(m))
                else:
                    import_module(m)

            # Re-enable sends after import completes
            try:
                app.suppress_sends(False)
            except Exception:
                pass
            _debug_imports_log(f'[preload] registered_tasks={app.list_tasks()}')
        except Exception as e:
            # Surface the error clearly and re-raise to stop startup
            logger.error(f'Failed during preload of task modules: {e}')
            raise

    # ----- main loop -----

    async def run_forever(self) -> None:
        """Main orchestrator loop."""
        await self._start_with_resilience()
        if self._stop.is_set():
            return
        logger.info('Worker started')
        try:
            backoff = self._make_retry_backoff()
            while not self._stop.is_set():
                try:
                    # Single budgeted claim pass, then wait for new NOTIFY
                    await self._claim_and_dispatch_all()

                    # Wait for a NOTIFY from any queue (coalesce bursts).
                    await self._wait_for_any_notify(
                        poll_interval_ms=self._resilience.notify_poll_interval_ms
                    )
                    await self._claim_and_dispatch_all()
                    backoff.reset()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if is_retryable_connection_error(exc):
                        if not backoff.can_retry():
                            logger.error(
                                f'Worker loop failed after {backoff.attempts} attempts: {exc}'
                            )
                            raise
                        delay = backoff.next_delay_seconds()
                        logger.error(
                            f'Worker loop error: {exc}. Retrying in {delay:.1f}s '
                            f'(attempt {backoff.attempts}/{backoff.max_attempts or "inf"})'
                        )
                        await self._sleep_with_stop(delay)
                        continue
                    raise
        finally:
            await self.stop()

    async def _wait_for_any_notify(self, poll_interval_ms: int) -> None:
        """Wait on any subscribed queue channel; coalesce a burst."""
        import contextlib

        queue_tasks = [
            asyncio.create_task(q.get()) for q in (self._queues + [self._global])
        ]
        # Add only the stop event as an additional wait condition (no periodic polling)
        stop_task = asyncio.create_task(self._stop.wait())
        all_tasks = queue_tasks + [stop_task]
        timeout_seconds = max(0.0, poll_interval_ms / 1000.0)
        done, pending = await asyncio.wait(
            all_tasks,
            return_when=asyncio.FIRST_COMPLETED,
            timeout=timeout_seconds,
        )

        # Check if stop was signaled
        if self._stop.is_set():
            # Cancel all pending tasks and await them to avoid warnings
            for p in pending:
                p.cancel()
            for p in pending:
                with contextlib.suppress(asyncio.CancelledError):
                    await p
            return

        # Timeout: fall back to polling
        if not done:
            for p in pending:
                p.cancel()
            for p in pending:
                with contextlib.suppress(asyncio.CancelledError):
                    await p
            return

        # cancel the rest to avoid background tasks piling up and await them
        for p in pending:
            p.cancel()
        for p in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await p

        # drain a burst
        drained = 0
        for q in self._queues + [self._global]:
            while drained < self.cfg.coalesce_notifies and not q.empty():
                try:
                    q.get_nowait()
                    drained += 1
                except asyncio.QueueEmpty:
                    break

    # ----- claim & dispatch -----

    async def _claim_and_dispatch_all(self) -> bool:
        """
        Claim tasks subject to:
          - max_claim_per_worker guard (prevents over-claiming)
          - queue priorities (CUSTOM mode)
          - per-queue max_concurrency (CUSTOM mode)
          - worker global concurrency (processes)
        Returns True if anything was claimed.
        """
        # Guard: Check if we've already claimed too many tasks
        # Default depends on mode:
        # - Hard cap (prefetch_buffer=0): default to processes
        # - Soft cap (prefetch_buffer>0): default to processes + prefetch_buffer
        if self.cfg.max_claim_per_worker > 0:
            # User explicitly set a limit - use it
            max_claimed = self.cfg.max_claim_per_worker
        elif self.cfg.prefetch_buffer > 0:
            # Soft cap mode: allow claiming up to processes + prefetch_buffer
            max_claimed = self.cfg.processes + self.cfg.prefetch_buffer
        else:
            # Hard cap mode: limit to processes
            max_claimed = self.cfg.processes
        claimed_count = await self._count_claimed_for_worker()
        if claimed_count >= max_claimed:
            return False

        # Cluster-wide, lock-guarded claim to avoid races. One short transaction.
        # Use local capacity + small prefetch to size claims fairly across workers.
        claimed_ids: list[str] = []

        # Queue order: if custom priorities provided, sort by priority; otherwise keep given order
        if self.cfg.queue_priorities:
            ordered_queues = sorted(
                [q for q in self.cfg.queues if q in self.cfg.queue_priorities],
                key=lambda q: self.cfg.queue_priorities.get(q, 100),
            )
        else:
            ordered_queues = list(self.cfg.queues)

        # Open one transaction, take a global advisory xact lock
        async with self.sf() as s:
            # Take a cluster-wide transaction-scoped advisory lock to serialize claiming
            await s.execute(
                CLAIM_ADVISORY_LOCK_SQL,
                {'key': self._advisory_key_global()},
            )

            # Compute local budget and optional global remaining
            # Hard cap mode (prefetch_buffer=0): count RUNNING + CLAIMED for strict enforcement
            # Soft cap mode (prefetch_buffer>0): count only RUNNING, allow prefetch with lease
            hard_cap_mode = self.cfg.prefetch_buffer == 0

            if hard_cap_mode:
                # Hard cap: count both RUNNING and CLAIMED for this worker
                local_in_flight = await self._count_in_flight_for_worker()
                max_local_capacity = self.cfg.processes
            else:
                # Soft cap: count only RUNNING to allow prefetch beyond processes
                local_in_flight = await self._count_only_running_for_worker()
                max_local_capacity = self.cfg.processes + self.cfg.prefetch_buffer
            local_available = max(0, int(max_local_capacity) - int(local_in_flight))
            budget_remaining = local_available

            global_remaining: Optional[int] = None
            if self.cfg.cluster_wide_cap is not None:
                # Hard cap mode: count RUNNING + CLAIMED globally
                # (Note: prefetch_buffer must be 0 when cluster_wide_cap is set, enforced by config validation)
                res = await s.execute(COUNT_GLOBAL_IN_FLIGHT_SQL)
                row = res.fetchone()
                if row:
                    in_flight_global = int(row[0])
                else:
                    in_flight_global = 0
                global_remaining = max(
                    0, int(self.cfg.cluster_wide_cap) - in_flight_global
                )

            # Total claim budget for this pass: local budget capped by global remaining (if any)
            total_remaining = (
                budget_remaining
                if global_remaining is None
                else min(budget_remaining, global_remaining)
            )
            if total_remaining <= 0:
                # Nothing to claim globally or locally
                await s.commit()
                return False

            for qname in ordered_queues:
                if total_remaining <= 0:
                    break

                # Compute queue remaining in cluster (only if custom-configured)
                q_remaining: Optional[int] = None
                if (
                    self.cfg.queue_priorities
                    and qname in self.cfg.queue_max_concurrency
                ):
                    # Hard cap mode: count RUNNING + CLAIMED for this queue
                    # Soft cap mode: count only RUNNING
                    if hard_cap_mode:
                        resq = await s.execute(
                            COUNT_QUEUE_IN_FLIGHT_HARD_SQL,
                            {'q': qname},
                        )
                    else:
                        resq = await s.execute(
                            COUNT_QUEUE_IN_FLIGHT_SOFT_SQL,
                            {'q': qname},
                        )
                    row = resq.fetchone()
                    if row:
                        in_flight_q = int(row[0])
                    else:
                        in_flight_q = 0
                    max_q = int(self.cfg.queue_max_concurrency.get(qname, 0))
                    q_remaining = max(0, max_q - in_flight_q)

                # Determine how many we may claim from this queue
                # Hierarchy: max_claim_batch (fairness) -> q_remaining (queue cap) -> total_remaining (worker budget)

                if self.cfg.queue_priorities:
                    # Strict priority mode: try to fill remaining budget from this queue
                    # Ignore max_claim_batch (which forces round-robin fairness)
                    per_queue_cap = total_remaining
                else:
                    # Default mode: use max_claim_batch to ensure fairness across queues
                    per_queue_cap = self.cfg.max_claim_batch

                if q_remaining is not None:
                    per_queue_cap = min(per_queue_cap, q_remaining)
                to_claim = min(total_remaining, per_queue_cap)
                if to_claim <= 0:
                    continue

                ids = await self._claim_batch_locked(s, qname, to_claim)
                if not ids:
                    continue
                claimed_ids.extend(ids)
                total_remaining -= len(ids)

            await s.commit()

        if not claimed_ids:
            return False

        rows = await self._load_rows(claimed_ids)

        # PAUSE guard: filter out tasks belonging to PAUSED workflows and unclaim them
        rows = await self._filter_paused_workflow_tasks(rows)

        for row in rows:
            await self._dispatch_one(
                row['id'], row['task_name'], row['args'], row['kwargs']
            )
        return len(rows) > 0

    async def _filter_paused_workflow_tasks(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Filter out tasks belonging to PAUSED workflows and unclaim them.

        Post-claim guard: If a task belongs to a workflow that is PAUSED,
        we unclaim it (set back to PENDING) so it can be processed on resume.

        Returns the filtered list of rows that should be dispatched.
        """
        if not rows:
            return rows

        task_ids = [row['id'] for row in rows]

        # Find which tasks belong to PAUSED workflows
        async with self.sf() as s:
            res = await s.execute(
                GET_PAUSED_WORKFLOW_TASK_IDS_SQL,
                {'ids': task_ids},
            )
            paused_task_ids = {row[0] for row in res.fetchall()}

            if paused_task_ids:
                # Unclaim these tasks: set back to PENDING so they can be picked up on resume
                await s.execute(
                    UNCLAIM_PAUSED_TASKS_SQL,
                    {'ids': list(paused_task_ids)},
                )
                # Also reset workflow_tasks back to READY for consistency
                # (they were ENQUEUED, but the task is now unclaimed)
                await s.execute(
                    RESET_PAUSED_WORKFLOW_TASKS_SQL,
                    {'ids': list(paused_task_ids)},
                )
                await s.commit()

        # Return only tasks not belonging to PAUSED workflows
        return [row for row in rows if row['id'] not in paused_task_ids]

    def _advisory_key_global(self) -> int:
        """Compute a stable 64-bit advisory lock key for this cluster."""
        basis = (self.cfg.psycopg_dsn or self.cfg.dsn or 'horsies').encode(
            'utf-8', errors='ignore'
        )
        h = hashlib.sha256(b'horsies-global:' + basis).digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    # Stale detection is handled via heartbeat policy for RUNNING tasks.

    def _compute_claim_expires_at(self) -> Optional[datetime]:
        """Compute claim expiration timestamp for soft cap mode, or None for hard cap mode."""
        if self.cfg.claim_lease_ms is None:
            return None
        return datetime.now(timezone.utc) + timedelta(
            milliseconds=self.cfg.claim_lease_ms
        )

    async def _claim_batch_locked(
        self, s: AsyncSession, queue: str, limit: int
    ) -> list[str]:
        """Claim up to limit tasks for a given queue within an open transaction/lock."""
        res = await s.execute(
            CLAIM_SQL,
            {
                'queue': queue,
                'lim': limit,
                'worker_id': self.worker_instance_id,
                'claim_expires_at': self._compute_claim_expires_at(),
            },
        )
        return [r[0] for r in res.fetchall()]

    async def _count_claimed_for_worker(self) -> int:
        """Count only CLAIMED tasks for this worker (not yet RUNNING)."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_CLAIMED_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _count_only_running_for_worker(self) -> int:
        """Count only RUNNING tasks for this worker (excludes CLAIMED)."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _count_in_flight_for_worker(self) -> int:
        """Count RUNNING + CLAIMED tasks for this worker (hard cap mode)."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_IN_FLIGHT_FOR_WORKER_SQL,
                {'wid': self.worker_instance_id},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _count_running_in_queue(self, queue_name: str) -> int:
        """Count RUNNING tasks in a given queue across the cluster."""
        async with self.sf() as s:
            res = await s.execute(
                COUNT_RUNNING_IN_QUEUE_SQL,
                {'q': queue_name},
            )
            row = res.fetchone()
            return int(row[0]) if row else 0

    async def _claim_batch(self, queue: str, limit: int) -> list[str]:
        async with self.sf() as s:
            res = await s.execute(
                CLAIM_SQL,
                {
                    'queue': queue,
                    'lim': limit,
                    'worker_id': self.worker_instance_id,
                    'claim_expires_at': self._compute_claim_expires_at(),
                },
            )
            ids = [r[0] for r in res.fetchall()]
            # Make the CLAIMED state visible and release the row locks
            await s.commit()
            return ids

    async def _load_rows(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        async with self.sf() as s:
            res = await s.execute(LOAD_ROWS_SQL, {'ids': list(ids)})
            cols = res.keys()
            return [dict(zip(cols, row)) for row in res.fetchall()]

    async def _requeue_claimed_task(self, task_id: str, reason: str) -> bool:
        async with self.sf() as s:
            res = await s.execute(
                UNCLAIM_CLAIMED_TASK_SQL,
                {'id': task_id, 'wid': self.worker_instance_id},
            )
            await s.commit()
            rowcount = getattr(res, 'rowcount', 0) or 0
            requeued = rowcount > 0

        if requeued:
            logger.warning(f'Requeued CLAIMED task {task_id}: {reason}')
        else:
            logger.warning(
                f'Failed to requeue task {task_id} (not CLAIMED or owner mismatch): {reason}'
            )
        return requeued

    async def _handle_broken_pool(self, task_id: str, exc: BaseException) -> None:
        await self._requeue_claimed_task(task_id, f'Broken process pool: {exc}')
        await self._restart_executor(f'Broken process pool: {exc}')

    async def _dispatch_one(
        self,
        task_id: str,
        task_name: str,
        args_json: Optional[str],
        kwargs_json: Optional[str],
    ) -> None:
        """Submit to process pool; attach completion handler."""
        if self._executor is None:
            await self._restart_executor('Executor missing before dispatch')
            if self._executor is None:
                await self._requeue_claimed_task(
                    task_id, 'Executor unavailable after restart attempt'
                )
                return
        loop = asyncio.get_running_loop()

        # Get heartbeat interval from recovery config (milliseconds)
        runner_heartbeat_interval_ms = 30_000  # default: 30 seconds
        if self.cfg.recovery_config:
            runner_heartbeat_interval_ms = (
                self.cfg.recovery_config.runner_heartbeat_interval_ms
            )

        # Pass task_id and database_url to task process for self-heartbeat
        database_url = self.cfg.dsn.replace('+asyncpg', '').replace('+psycopg', '')
        try:
            fut = loop.run_in_executor(
                self._executor,
                _run_task_entry,
                task_name,
                args_json,
                kwargs_json,
                task_id,
                database_url,
                self.worker_instance_id,
                runner_heartbeat_interval_ms,
            )
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(task_id, exc)
            return
        except Exception as exc:
            await self._requeue_claimed_task(
                task_id, f'Failed to dispatch task to executor: {exc}'
            )
            return

        # When done, record the outcome
        self._spawn_background(
            self._finalize_after(fut, task_id),
            name=f'finalize-{task_id}',
            finalizer=True,
        )

    # ----- finalize (write back to DB + notify) -----

    async def _finalize_after(
        self, fut: 'asyncio.Future[tuple[bool, str, Optional[str]]]', task_id: str
    ) -> None:
        try:
            ok, result_json_str, failed_reason = await fut
        except asyncio.CancelledError:
            raise
        except BrokenProcessPool as exc:
            await self._handle_broken_pool(task_id, exc)
            return
        except Exception as exc:
            await self._requeue_claimed_task(
                task_id, f'Worker future failed before result: {exc}'
            )
            return
        now = datetime.now(timezone.utc)

        # Note: Heartbeat thread in task process automatically dies when process completes

        async with self.sf() as s:
            if not ok:
                # CLAIM_LOST: Another worker reclaimed this task - do nothing
                # The task is not failed; it belongs to another worker now
                match failed_reason:
                    case (
                        'CLAIM_LOST'
                        | 'OWNERSHIP_UNCONFIRMED'
                        | 'WORKFLOW_CHECK_FAILED'
                        | 'WORKFLOW_STOPPED'
                    ):
                        logger.debug(
                            f'Task {task_id} aborted with reason={failed_reason}, skipping finalization'
                        )
                        return
                    case _:
                        pass

                # worker-level failure (rare): mark FAILED with reason
                res = await s.execute(
                    MARK_TASK_FAILED_WORKER_SQL,
                    {
                        'now': now,
                        'reason': failed_reason or 'Worker failure',
                        'id': task_id,
                    },
                )
                if res.fetchone() is None:
                    logger.warning(
                        f'Task {task_id} finalize aborted: status is no longer RUNNING '
                        f'(reaper likely reclaimed). Skipping to prevent double-execution.'
                    )
                    return
                # Trigger automatically sends NOTIFY on UPDATE
                await s.commit()
                return

            # Parse the TaskResult we produced
            tr = task_result_from_json(loads_json(result_json_str))
            if tr.is_err():
                # Check if this task should be retried
                task_error = tr.unwrap_err()
                match task_error.error_code if task_error else None:
                    case 'WORKFLOW_STOPPED':
                        logger.debug(
                            f'Task {task_id} skipped due to workflow stop, skipping finalization'
                        )
                        return
                    case _:
                        pass
                should_retry = await self._should_retry_task(task_id, task_error, s)
                if should_retry:
                    retry_ok = await self._schedule_retry(task_id, s)
                    if not retry_ok:
                        logger.warning(
                            f'Task {task_id} retry aborted during finalize: '
                            f'task no longer RUNNING (reaper reclaimed).'
                        )
                    await s.commit()
                    return

                # Mark as failed if no retry
                fail_res = await s.execute(
                    MARK_TASK_FAILED_SQL,
                    {'now': now, 'result_json': result_json_str, 'id': task_id},
                )
                if fail_res.fetchone() is None:
                    logger.warning(
                        f'Task {task_id} finalize-fail aborted: status is no longer RUNNING '
                        f'(reaper likely reclaimed). Skipping to prevent double-execution.'
                    )
                    return
            else:
                comp_res = await s.execute(
                    MARK_TASK_COMPLETED_SQL,
                    {'now': now, 'result_json': result_json_str, 'id': task_id},
                )
                if comp_res.fetchone() is None:
                    logger.warning(
                        f'Task {task_id} finalize-complete aborted: status is no longer RUNNING '
                        f'(reaper likely reclaimed). Skipping to prevent double-execution.'
                    )
                    return

            # Handle workflow task completion (if this task is part of a workflow)
            await self._handle_workflow_task_if_needed(s, task_id, tr)

            # Proactively wake workers to re-check capacity/backlog.
            try:
                # Notify workers globally and on the specific queue to wake claims
                # Fetch queue name for this task
                resq = await s.execute(
                    GET_TASK_QUEUE_NAME_SQL,
                    {'id': task_id},
                )
                rowq = resq.fetchone()
                qname = str(rowq[0]) if rowq and rowq[0] else 'default'
                payload = f'capacity:{task_id}'
                await s.execute(NOTIFY_TASK_NEW_SQL, {'c1': 'task_new', 'p': payload})
                await s.execute(
                    NOTIFY_TASK_QUEUE_SQL,
                    {'c2': f'task_queue_{qname}', 'p': payload},
                )
            except Exception:
                # Non-fatal if NOTIFY fails; continue
                pass

            # Trigger automatically sends NOTIFY on UPDATE; commit to flush NOTIFYs
            await s.commit()

    async def _handle_workflow_task_if_needed(
        self,
        session: 'AsyncSession',
        task_id: str,
        result: 'TaskResult[Any, TaskError]',
    ) -> None:
        """
        Check if task is part of a workflow and handle accordingly.

        This method is called after a task completes (success or failure).
        It updates the workflow_task record and triggers dependency resolution.
        """
        from horsies.core.workflows.engine import on_workflow_task_complete

        # Quick check: is this task linked to a workflow?
        check = await session.execute(
            CHECK_WORKFLOW_TASK_EXISTS_SQL,
            {'tid': task_id},
        )

        if check.fetchone() is None:
            return  # Not a workflow task

        broker = self._app.get_broker() if self._app is not None else None
        # Handle workflow task completion
        await on_workflow_task_complete(session, task_id, result, broker)

    async def _should_retry_task(
        self, task_id: str, error: TaskError, session: AsyncSession,
    ) -> bool:
        """Check if a task should be retried based on its configuration and current retry count."""
        result = await session.execute(
            GET_TASK_RETRY_INFO_SQL,
            {'id': task_id},
        )
        row = result.fetchone()

        if not row:
            return False

        retry_count = row.retry_count or 0
        max_retries = row.max_retries or 0

        if retry_count >= max_retries or max_retries == 0:
            return False

        # Parse task options to check auto_retry_for (nested in retry_policy)
        try:
            task_options_data = loads_json(row.task_options) if row.task_options else {}
            if not isinstance(task_options_data, dict):
                return False
            retry_policy_raw = task_options_data.get('retry_policy', {})
            if not isinstance(retry_policy_raw, dict):
                return False
            auto_retry_for = retry_policy_raw.get('auto_retry_for')
        except Exception:
            return False

        if not isinstance(auto_retry_for, list) or not auto_retry_for:
            return False

        # Match error code against auto_retry_for (enum value or string)
        code = (
            error.error_code.value
            if isinstance(error.error_code, LibraryErrorCode)
            else error.error_code
        )
        if code and code in auto_retry_for:
            return True

        return False

    async def _schedule_retry(self, task_id: str, session: AsyncSession) -> bool:
        """Schedule a task for retry by updating its status and next retry time.

        Returns True if the retry was scheduled, False if the task was already
        reclaimed by the reaper (status no longer RUNNING).
        """
        # Get current retry configuration
        result = await session.execute(
            GET_TASK_RETRY_CONFIG_SQL,
            {'id': task_id},
        )
        row = result.fetchone()

        if not row:
            return False

        retry_count = (row.retry_count or 0) + 1

        # Parse retry policy from task options
        try:
            task_options_data = loads_json(row.task_options) if row.task_options else {}
            if not isinstance(task_options_data, dict):
                retry_policy_data = {}
            else:
                retry_policy_data = task_options_data.get('retry_policy', {})
                if not isinstance(retry_policy_data, dict):
                    retry_policy_data = {}
        except Exception:
            retry_policy_data = {}

        # Calculate retry delay
        delay_seconds = self._calculate_retry_delay(retry_count, retry_policy_data)
        next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        # Update task for retry — guarded by AND status = 'RUNNING'
        res = await session.execute(
            SCHEDULE_TASK_RETRY_SQL,
            {'id': task_id, 'retry_count': retry_count, 'next_retry_at': next_retry_at},
        )
        if res.fetchone() is None:
            logger.warning(
                f'Task {task_id} retry aborted: status is no longer RUNNING '
                f'(reaper likely reclaimed). Skipping to prevent double-execution.'
            )
            return False

        # Schedule a delayed notification using asyncio for the task's actual queue
        queue_name = await self._get_task_queue_name(task_id)
        self._spawn_background(
            self._schedule_delayed_notification(
                delay_seconds, f'task_queue_{queue_name}', f'retry:{task_id}',
            ),
            name=f'delayed-notify-{task_id}',
        )

        logger.info(
            f'Scheduled task {task_id} for retry #{retry_count} at {next_retry_at}'
        )
        return True

    def _calculate_retry_delay(
        self, retry_attempt: int, retry_policy_data: dict[str, Any]
    ) -> float:
        """Calculate the delay in seconds for a retry attempt."""
        # Default retry policy values (these match RetryPolicy defaults)
        intervals = retry_policy_data.get(
            'intervals', [60, 300, 900]
        )  # 1min, 5min, 15min
        backoff_strategy = retry_policy_data.get('backoff_strategy', 'fixed')
        jitter = retry_policy_data.get('jitter', True)

        # Calculate base delay based on strategy
        if backoff_strategy == 'fixed':
            # Fixed strategy: use intervals directly, clamped to last interval
            # if retry_attempt exceeds length (possible after DB deserialization, old data)
            clamped_index = min(retry_attempt - 1, len(intervals) - 1)
            base_delay = intervals[clamped_index]

        elif backoff_strategy == 'exponential':
            # Exponential strategy: use intervals[0] as base and apply exponential multiplier
            base_interval = intervals[0] if intervals else 60
            base_delay = base_interval * (2 ** (retry_attempt - 1))

        else:
            # Fallback (shouldn't happen due to Literal type validation)
            base_delay = intervals[0] if intervals else 60

        # Apply jitter (±25% randomization)
        if jitter:
            jitter_range = base_delay * 0.25
            base_delay += random.uniform(-jitter_range, jitter_range)

        return float(max(1.0, base_delay))

    async def _schedule_delayed_notification(
        self, delay_seconds: float, channel: str, payload: str
    ) -> None:
        """Schedule a delayed notification to wake up the worker for retry."""
        try:
            await asyncio.sleep(delay_seconds)

            # Send notification to trigger retry processing
            async with self.sf() as session:
                await session.execute(
                    NOTIFY_DELAYED_SQL,
                    {'channel': channel, 'payload': payload},
                )
                await session.commit()

            logger.debug(f'Sent delayed notification for retry: {payload}')
        except asyncio.CancelledError:
            logger.debug(f'Delayed notification cancelled for: {payload}')
        except Exception as e:
            logger.error(f'Error sending delayed notification for {payload}: {e}')

    async def _get_task_queue_name(self, task_id: str) -> str:
        """Fetch the queue_name for a given task id."""
        async with self.sf() as session:
            res = await session.execute(
                GET_TASK_QUEUE_NAME_SQL,
                {'id': task_id},
            )
            row = res.fetchone()
            return str(row[0]) if row and row[0] else 'default'

    async def _claimer_heartbeat_loop(self) -> None:
        """Emit claimer heartbeats for tasks we've claimed but not yet started."""
        # Get heartbeat interval from recovery config (milliseconds)
        claimer_heartbeat_interval_ms = 30_000  # default: 30 seconds
        if self.cfg.recovery_config:
            claimer_heartbeat_interval_ms = (
                self.cfg.recovery_config.claimer_heartbeat_interval_ms
            )

        try:
            while not self._stop.is_set():
                try:
                    async with self.sf() as s:
                        await s.execute(
                            INSERT_CLAIMER_HEARTBEAT_SQL,
                            {
                                'wid': self.worker_instance_id,
                                'host': socket.gethostname(),
                                'pid': os.getpid(),
                            },
                        )
                        await s.commit()
                except Exception as e:
                    logger.error(f'Claimer heartbeat error: {e}')
                # Convert to seconds only for asyncio.sleep
                await asyncio.sleep(claimer_heartbeat_interval_ms / 1000.0)
        except asyncio.CancelledError:
            return

    async def _update_worker_state(self) -> None:
        """Update worker state snapshot in database for monitoring."""
        try:
            rss_mb, mem_pct, cpu_pct = await asyncio.to_thread(
                _collect_psutil_metrics,
            )

            # Get current task counts
            running = await self._count_only_running_for_worker()
            claimed = await self._count_claimed_for_worker()

            # Serialize recovery config
            recovery_dict = None
            if self.cfg.recovery_config:
                recovery_dict = {
                    'auto_requeue_stale_claimed': self.cfg.recovery_config.auto_requeue_stale_claimed,
                    'claimed_stale_threshold_ms': self.cfg.recovery_config.claimed_stale_threshold_ms,
                    'auto_fail_stale_running': self.cfg.recovery_config.auto_fail_stale_running,
                    'running_stale_threshold_ms': self.cfg.recovery_config.running_stale_threshold_ms,
                    'check_interval_ms': self.cfg.recovery_config.check_interval_ms,
                    'runner_heartbeat_interval_ms': self.cfg.recovery_config.runner_heartbeat_interval_ms,
                    'claimer_heartbeat_interval_ms': self.cfg.recovery_config.claimer_heartbeat_interval_ms,
                }

            async with self.sf() as s:
                await s.execute(
                    INSERT_WORKER_STATE_SQL,
                    {
                        'wid': self.worker_instance_id,
                        'host': socket.gethostname(),
                        'pid': os.getpid(),
                        'procs': self.cfg.processes,
                        'mcb': self.cfg.max_claim_batch,
                        'mcpw': self.cfg.max_claim_per_worker
                        if self.cfg.max_claim_per_worker > 0
                        else self.cfg.processes,
                        'cwc': self.cfg.cluster_wide_cap,
                        'queues': self.cfg.queues,
                        'qp': Jsonb(self.cfg.queue_priorities)
                        if self.cfg.queue_priorities
                        else None,
                        'qmc': Jsonb(self.cfg.queue_max_concurrency)
                        if self.cfg.queue_max_concurrency
                        else None,
                        'recovery': Jsonb(recovery_dict) if recovery_dict else None,
                        'running': running,
                        'claimed': claimed,
                        'mem_mb': rss_mb,
                        'mem_pct': mem_pct,
                        'cpu_pct': cpu_pct,
                        'started': self._started_at,
                    },
                )
                await s.commit()
        except Exception as e:
            logger.error(f'Failed to update worker state: {e}')

    async def _worker_state_heartbeat_loop(self) -> None:
        """Periodically update worker state for monitoring (every 5 seconds)."""
        worker_state_interval_ms = 5_000  # 5 seconds

        try:
            while not self._stop.is_set():
                try:
                    await self._update_worker_state()
                except Exception as e:
                    logger.error(f'Worker state heartbeat error: {e}')

                # Wait for interval or stop signal
                await asyncio.sleep(worker_state_interval_ms / 1000.0)
        except asyncio.CancelledError:
            return

    async def _reaper_loop(self) -> None:
        """Automatic stale task handling loop.

        Periodically checks for and recovers stale tasks based on RecoveryConfig:
        - Requeues tasks stuck in CLAIMED (safe - user code never ran)
        - Marks stale RUNNING tasks as FAILED (not safe to requeue)
        """
        from horsies.core.brokers.postgres import PostgresBroker

        if not self.cfg.recovery_config:
            return

        recovery_cfg = self.cfg.recovery_config
        check_interval_ms = recovery_cfg.check_interval_ms
        temp_broker = None
        next_heartbeat_cleanup_at = time.monotonic()

        logger.info(
            f'Reaper configuration: auto_requeue_claimed={recovery_cfg.auto_requeue_stale_claimed}, '
            f'auto_fail_running={recovery_cfg.auto_fail_stale_running}, '
            f'check_interval={check_interval_ms}ms ({check_interval_ms/1000:.1f}s)'
        )

        try:
            from horsies.core.models.broker import PostgresConfig

            temp_broker_config = PostgresConfig(database_url=self.cfg.dsn)
            temp_broker = PostgresBroker(temp_broker_config)
            if self._app is not None:
                temp_broker.app = self._app

            while not self._stop.is_set():
                try:
                    # Auto-requeue stale CLAIMED tasks
                    if recovery_cfg.auto_requeue_stale_claimed:
                        requeued = await temp_broker.requeue_stale_claimed(
                            stale_threshold_ms=recovery_cfg.claimed_stale_threshold_ms
                        )
                        if requeued > 0:
                            logger.info(
                                f'Reaper requeued {requeued} stale CLAIMED task(s)'
                            )

                    # Auto-fail stale RUNNING tasks
                    if recovery_cfg.auto_fail_stale_running:
                        failed = await temp_broker.mark_stale_tasks_as_failed(
                            stale_threshold_ms=recovery_cfg.running_stale_threshold_ms
                        )
                        if failed > 0:
                            logger.warning(
                                f'Reaper marked {failed} stale RUNNING task(s) as FAILED'
                            )

                    # Recover stuck workflows
                    try:
                        from horsies.core.workflows.recovery import (
                            recover_stuck_workflows,
                        )

                        async with temp_broker.session_factory() as s:
                            recovered = await recover_stuck_workflows(s, temp_broker)
                            if recovered > 0:
                                logger.info(
                                    f'Reaper recovered {recovered} stuck workflow task(s)'
                                )
                            await s.commit()
                    except Exception as wf_err:
                        logger.error(f'Workflow recovery error: {wf_err}')

                    now_monotonic = time.monotonic()
                    if now_monotonic >= next_heartbeat_cleanup_at:
                        try:
                            async with temp_broker.session_factory() as s:
                                result = await s.execute(DELETE_EXPIRED_HEARTBEATS_SQL)
                                await s.commit()
                            deleted = int(result.rowcount or 0)
                            if deleted > 0:
                                logger.info(
                                    f'Reaper pruned {deleted} stale heartbeat row(s)'
                                )
                        except Exception as hb_err:
                            logger.error(f'Heartbeat retention cleanup error: {hb_err}')
                        finally:
                            next_heartbeat_cleanup_at = (
                                now_monotonic
                                + _HEARTBEAT_RETENTION_CLEANUP_INTERVAL_S
                            )

                except Exception as e:
                    logger.error(f'Reaper loop error: {e}')

                # Wait for check interval or stop signal (convert ms to seconds for asyncio)
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=check_interval_ms / 1000.0
                    )
                    break  # Stop signal received
                except asyncio.TimeoutError:
                    continue  # Continue to next iteration

        except asyncio.CancelledError:
            logger.info('Reaper loop cancelled')
            return
        finally:
            if temp_broker is not None:
                try:
                    await temp_broker.close_async()
                except Exception as e:
                    logger.error(f'Error closing reaper broker: {e}')


"""
horsies examples/instance.py worker --loglevel=info --processes=8 
"""
