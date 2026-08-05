"""Child-process helpers: import resolution, heartbeat, task execution."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import os
import signal
import socket
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import datetime
from importlib import import_module
from typing import Any, Optional, Tuple, assert_never, cast

from psycopg import Connection, Cursor, InterfaceError, OperationalError
from psycopg.rows import namedtuple_row
from psycopg.errors import DeadlockDetected, SerializationFailure

from pydantic import ValidationError
from pydantic_core import PydanticSerializationError

from horsies.core.app import Horsies
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import decode_kwargs, underlying_task_fn
from horsies.core.codec.json_io import (
    loads_json,
    dumps_json,
    SerializationError,
)
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.typed import (
    decode_task_result,
    encode_task_result,
)


# Engine transport keys (strict-serde §8). Pinned here so the worker
# consumer stays in lockstep with the engine emitters in
# ``horsies/core/workflows/engine.py``.
_ENGINE_KEY_WORKFLOW_CTX = '__h_workflow_ctx__'
_ENGINE_KEY_WORKFLOW_META = '__h_workflow_meta__'
_ENGINE_KEY_TASKRESULT_ENVELOPE = '__h_taskresult_envelope__'
from horsies.core.models.tasks import (
    TaskResult,
    TaskError,
    OperationalErrorCode,
    OutcomeCode,
)
from horsies.core.lifecycle.commands import ExpireOwnedClaim
from horsies.core.lifecycle.fences import WorkerOwned
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_sync
from horsies.core.types.result import is_err


def _serialization_error_response(
    task_name: str,
    error: SerializationError,
) -> tuple[bool, str, str | None]:
    """Build a WORKER_SERIALIZATION_ERROR response for a serde failure."""
    logger.error(f'Serialization error for task {task_name}: {error}')
    tr: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
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
    """Import module from file path.

    Raises:
        Exception: import/filesystem errors propagate to the boot path
            (parent preload fails fast at start; child-initializer failure
            kills the child).
    """
    return import_file_path(path, module_name)


# ---------- helpers for child processes ----------
def _locate_app(app_locator: str) -> Horsies:
    """
    app_locator examples:
      - 'package.module:app'         -> import module, take variable attr
      - '/abs/path/to/file.py:app'   -> load from file path

    Raises:
        Exception: import/attribute/validation failures propagate to the
            boot path (parent preload fails fast at start; child-initializer
            failure kills the child).
    """
    # Child initializer re-runs this on every recycle (max_tasks_per_child),
    # having set HORSIES_CHILD_PROCESS first; keep it at DEBUG there. The
    # parent preload path (env unset) logs once at INFO.
    if os.getenv('HORSIES_CHILD_PROCESS') == '1':
        logger.debug(f'Locating app from {app_locator}')
    else:
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


def _discard_inherited_broker(app: Horsies) -> None:
    """Drop any broker object inherited from a forked parent process."""
    broker = getattr(app, '_broker', None)
    if broker is None:
        return
    async_engine = getattr(broker, 'async_engine', None)
    sync_engine = getattr(async_engine, 'sync_engine', None)
    dispose = getattr(sync_engine, 'dispose', None)
    if callable(dispose):
        try:
            dispose(close=False)
        except TypeError:
            dispose()
        except Exception as exc:
            logger.warning('Failed to discard inherited broker pool in child: %s', exc)
    try:
        setattr(app, '_broker', None)
    except Exception as exc:
        logger.warning('Failed to reset inherited broker in child: %s', exc)


# Exit code for a failed/hung on_child_process_start hook. Distinct from
# generic child crashes so the parent stops the worker instead of
# restart-looping on an unfixable child boot.
CHILD_HOOK_FAILURE_EXIT_CODE = 173

# Per-hook execution budget (cf. Celery's 4s worker_process_init limit).
CHILD_HOOK_TIMEOUT_SECONDS = 10.0


def _exit_for_hung_hook(hook_name: str) -> None:
    """Watchdog target: a hook exceeded its budget; terminate the child."""
    logger.critical(
        "on_child_process_start hook '%s' exceeded %.0fs in child %s; "
        'terminating child',
        hook_name,
        CHILD_HOOK_TIMEOUT_SECONDS,
        os.getpid(),
    )
    os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)


def _run_child_start_hooks(app: Horsies) -> None:
    """Run app-registered child-start hooks; any failure is terminal.

    Fail-closed by design: a failed pool-policy install must not silently
    proceed to the state the hook exists to prevent. Failure or timeout
    logs the hook name and exits with CHILD_HOOK_FAILURE_EXIT_CODE, which
    the parent recognizes and stops the worker rather than restart-looping.
    Best-effort work belongs inside the hook's own try/except.
    """
    for hook in app.get_child_process_start_hooks():
        hook_name = getattr(hook, '__qualname__', repr(hook))
        watchdog = threading.Timer(
            CHILD_HOOK_TIMEOUT_SECONDS,
            _exit_for_hung_hook,
            args=(hook_name,),
        )
        watchdog.daemon = True
        watchdog.start()
        try:
            hook()
        except BaseException as exc:
            watchdog.cancel()
            logger.critical(
                "on_child_process_start hook '%s' failed in child %s: %s",
                hook_name,
                os.getpid(),
                exc,
            )
            os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)
        watchdog.cancel()
        logger.debug(
            "[child %s] on_child_process_start hook '%s' completed",
            os.getpid(),
            hook_name,
        )


def _child_initializer(
    app_locator: str,
    imports: Sequence[str],
    sys_path_roots: Sequence[str],
    loglevel: int,
    database_url: str,
    connect_kwargs: Mapping[str, object] | None = None,
    child_pool_min_size: int = 0,
    child_pool_max_size: int = 2,
    child_pool_check: bool = True,
) -> None:
    """Child-process bootstrap: locate the app, import tasks, open the pool.

    Raises:
        Exception: any bootstrap failure propagates out of the initializer,
            which kills this child; the parent recovers via warmup failure /
            BrokenProcessPool. A failing on_child_process_start hook does
            not raise — _run_child_start_hooks exits the child with
            CHILD_HOOK_FAILURE_EXIT_CODE (fail-closed).
    """
    # Ignore SIGINT in child processes - let the parent handle shutdown gracefully
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Configure logging for this child process before any logging
    from horsies.core.logging import configure_logging

    configure_logging(loglevel)

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
    _discard_inherited_broker(app)
    app.set_role('worker')
    set_current_app(app)

    # Suppress accidental sends while importing modules for discovery
    app.suppress_sends(True)

    try:
        combined_imports = list(imports)
        combined_imports.extend(app.get_discovered_task_modules())
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
        app.suppress_sends(False)

    keys = app.tasks.keys_list()
    _debug_imports_log(f'[child {os.getpid()}] registered_tasks={keys}')

    # App-owned per-child setup (engine disposal / pool policy) runs after
    # task imports so app engines exist, and before horsies opens its own
    # child pool so a failing hook aborts cleanly.
    _run_child_start_hooks(app)

    # Initialize per-process connection pool (after all imports complete)
    _initialize_worker_pool(
        database_url,
        connect_kwargs=connect_kwargs,
        min_size=child_pool_min_size,
        max_size=child_pool_max_size,
        check_on_checkout=child_pool_check,
    )
    logger.debug(f'[child {os.getpid()}] Connection pool initialized')


# ---------- Child-process execution ----------
def _heartbeat_worker(
    task_id: str,
    database_url: str,
    stop_event: threading.Event,
    sender_worker_id: str,
    heartbeat_interval_ms: int = 30_000,
    timeout_ms: int | None = None,
) -> None:
    """
    Runs in a separate thread within the task process.
    Sends heartbeats at configured interval until stopped or process dies.

    Uses the per-process connection pool for efficient connection reuse.

    Total per beat: send_heartbeat contains DB errors and returns False.
    The loop keeps beating until stopped or the timeout deadline —
    consecutive failures are tracked for logging only, deliberately: a
    recovered DB resumes reaper protection, and a stopped thread could not.

    Args:
        task_id: The task ID
        database_url: Database connection string (unused, kept for compatibility)
        stop_event: Event to signal thread termination
        sender_worker_id: Worker instance ID
        heartbeat_interval_ms: Milliseconds between heartbeats (default: 30000 = 30s)
    """
    # Convert to seconds only for threading.Event.wait()
    heartbeat_interval_seconds = heartbeat_interval_ms / 1000.0
    consecutive_failures = 0
    # Reaper backstop for timed-out tasks: stop beating past the deadline
    # (plus one interval of grace) so the staleness machinery reclaims the
    # task even if the parent worker died before it could kill this child.
    # The thread cannot kill the process itself — that needs the GIL, which
    # hung user code may never release.
    deadline_monotonic: float | None = None
    if timeout_ms is not None:
        deadline_monotonic = (
            time.monotonic() + timeout_ms / 1000.0 + heartbeat_interval_seconds
        )

    def send_heartbeat() -> bool:
        nonlocal consecutive_failures
        try:
            pool = _get_worker_pool()
            with pool.connection() as conn:
                cursor = conn.cursor(row_factory=namedtuple_row)
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
            if consecutive_failures > 0:
                logger.info(
                    'Runner heartbeat recovered for task %s after %d failed attempt(s)',
                    task_id,
                    consecutive_failures,
                )
                consecutive_failures = 0
            return True
        except Exception as e:
            consecutive_failures += 1
            logger.error(
                'Runner heartbeat failed for task %s (attempt %d): %s',
                task_id,
                consecutive_failures,
                e,
            )
            return False

    # No immediate beat here: _confirm_ownership_and_set_running inserts the
    # first heartbeat inside the RUNNING transaction, so coverage starts
    # atomically with the state transition.
    while not stop_event.is_set():
        # Wait for interval, but check stop_event periodically
        if stop_event.wait(timeout=heartbeat_interval_seconds):
            break  # stop_event was set

        if (
            deadline_monotonic is not None
            and time.monotonic() >= deadline_monotonic
        ):
            logger.warning(
                'Task %s exceeded timeout_ms=%s; stopping runner heartbeats '
                'so the reaper can reclaim it',
                task_id,
                timeout_ms,
            )
            break

        # Use pooled connection for heartbeat
        send_heartbeat()


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
    """Read the owning workflow's status for a task (None if not in one).

    Raises:
        Exception: DB errors propagate to the wrapped caller,
            _confirm_ownership_and_set_running, which folds them into
            OWNERSHIP_UNCONFIRMED.
    """
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
    status = row.status
    if isinstance(status, str):
        return status
    return None


def _handle_workflow_stop_before_start(
    cursor: Cursor[Any],
    conn: Connection[Any],
    task_id: str,
    workflow_status: str,
    worker_id: str,
    claimed_at: Optional[datetime] = None,
) -> Tuple[bool, str, Optional[str]]:
    """Skip a task whose workflow is PAUSED/CANCELLED; return the wire tuple.

    Both branches carry the ClaimedOwnerFence pair (worker_id, claimed_at) —
    the same fence _confirm_ownership_and_set_running applies to the
    CLAIMED->RUNNING transition. These are claimed pre-start operations, so a
    row requeued and re-claimed since this child was handed its dispatch is a
    different claim generation and must not be terminalized here. A NULL
    claimed_at disables the generation half, matching the ownership confirm.

    Raises:
        Exception: DB errors propagate to the wrapped caller,
            _confirm_ownership_and_set_running, which folds them into
            OWNERSHIP_UNCONFIRMED.
    """
    logger.info(
        f'Blocking task {task_id} execution before start - workflow is {workflow_status}'
    )
    match workflow_status:
        case 'PAUSED':
            # Pause is resumable at the workflow node level. The already
            # claimed task row is abandoned so resume can enqueue a fresh row
            # without leaving an orphan claim that can run outside the workflow.
            cursor.execute(
                """
                UPDATE horsies_tasks
                SET status = 'CANCELLED',
                    claimed = FALSE,
                    claimed_at = NULL,
                    claimed_by_worker_id = NULL,
                    claim_expires_at = NULL,
                    finalizing_at = NULL,
                    finalizing_by_worker_id = NULL,
                    error_code = 'TASK_CANCELLED',
                    failed_reason = 'Workflow paused before task start',
                    terminal_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'CLAIMED'
                  AND claimed_by_worker_id = %s
                  AND (%s::timestamptz IS NULL OR claimed_at = %s::timestamptz)
                """,
                (task_id, worker_id, claimed_at, claimed_at),
            )
            cursor.execute(
                """
                UPDATE horsies_workflow_tasks
                SET status = 'READY',
                    task_id = NULL,
                    started_at = NULL
                WHERE task_id = %s
                  AND status IN ('ENQUEUED', 'RUNNING')
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
                    finalizing_at = NULL,
                    finalizing_by_worker_id = NULL,
                    terminal_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                  AND (
                      -- Claimed: fence to this child's claim generation.
                      (status = 'CLAIMED'
                       AND claimed_by_worker_id = %s
                       AND (%s::timestamptz IS NULL
                            OR claimed_at = %s::timestamptz))
                      -- Requeued to PENDING: the claim is already gone, so
                      -- there is no generation to fence. The workflow is
                      -- CANCELLED either way, which is the operative guard.
                      OR status = 'PENDING'
                  )
                """,
                (task_id, worker_id, claimed_at, claimed_at),
            )
            conn.commit()
            return (False, '', 'WORKFLOW_STOPPED')
        case _:
            return (False, '', 'WORKFLOW_CHECK_FAILED')


def _update_workflow_task_running_with_retry(task_id: str) -> bool:
    backoff_seconds = (0.0, 0.25, 0.75)
    total_attempts = len(backoff_seconds)
    for attempt_index, delay in enumerate(backoff_seconds):
        if delay > 0:
            time.sleep(delay)
        try:
            pool = _get_worker_pool()
            with pool.connection() as conn:
                cursor = conn.cursor(row_factory=namedtuple_row)
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
            return True
        except Exception as exc:
            retryable = _is_retryable_db_error(exc)
            is_last_attempt = attempt_index == total_attempts - 1
            match (retryable, is_last_attempt):
                case (True, True):
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}: {exc}'
                    )
                    return False
                case (True, False):
                    logger.warning(
                        f'Retrying workflow_tasks RUNNING update for task {task_id}: {exc}'
                    )
                    continue
                case (False, _):
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}: {exc}'
                    )
                    return False
                case _:
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}: {exc}'
                    )
                    return False
    return False


_EXPIRE_CLAIMED_TASK_BEFORE_START_SQL = """
        UPDATE horsies_tasks
        SET status = 'EXPIRED',
            claimed = FALSE,
            claim_expires_at = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            failed_at = NOW(),
            result = %s,
            error_code = %s,
            terminal_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
          AND status = 'CLAIMED'
          AND claimed_by_worker_id = %s
          AND good_until IS NOT NULL
          AND good_until <= NOW()
        RETURNING id
"""


def _expire_claimed_task_before_start(
    cursor: Cursor[Any],
    conn: Connection[Any],
    task_id: str,
    worker_id: str,
) -> Optional[Tuple[bool, str, Optional[str]]]:
    task_result: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=OutcomeCode.TASK_EXPIRED,
            message='Task expired: good_until deadline passed before execution started',
            data={'task_id': task_id, 'worker_id': worker_id},
        ),
    )
    result_json = serialize_error_payload(task_result)
    outcome = apply_sync(
        cursor,
        ExpireOwnedClaim(
            task_id=task_id,
            fence=WorkerOwned(worker_id=worker_id),
            result_json=result_json,
            error_code=OutcomeCode.TASK_EXPIRED.value,
        ),
    )
    match outcome:
        case Applied() | AlreadyApplied():
            conn.commit()
            logger.info(
                f'Task {task_id} expired before actual execution start; marked EXPIRED.',
            )
            return (False, '', OutcomeCode.TASK_EXPIRED.value)
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            return None
        case _ as unreachable:
            assert_never(unreachable)


def _confirm_ownership_and_set_running(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool = True,
    claimed_at: Optional[datetime] = None,
) -> Optional[Tuple[bool, str, Optional[str]]]:
    try:
        pool = _get_worker_pool()
        with pool.connection() as conn:
            cursor = conn.cursor(row_factory=namedtuple_row)
            workflow_guard_sql = """
                  AND NOT EXISTS (
                      SELECT 1
                      FROM horsies_workflow_tasks wt
                      JOIN horsies_workflows w ON w.id = wt.workflow_id
                      WHERE wt.task_id = %s
                        AND w.status IN ('PAUSED', 'CANCELLED')
                  )
            """
            sql = (
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
                  AND (%s::timestamptz IS NULL OR claimed_at = %s::timestamptz)
                  AND (claim_expires_at IS NULL OR claim_expires_at > now())
                  AND (good_until IS NULL OR good_until > now())
                """
                + (workflow_guard_sql if is_workflow_task else '')
                + """
                RETURNING id
                """
            )
            # The claimed_at pair is the claim-generation fence (C10): the
            # dispatch that submitted this child carries the claim's own
            # claimed_at; a row requeued and re-claimed since then (same
            # worker or not) no longer matches, so the stale child aborts
            # with CLAIM_LOST instead of double-running the task.
            params = [
                os.getpid(),
                socket.gethostname(),
                f'worker-{os.getpid()}',
                task_id,
                worker_id,
                claimed_at,
                claimed_at,
            ]
            if is_workflow_task:
                params.append(task_id)
            cursor.execute(
                sql,
                tuple(params),
            )
            updated_row = cursor.fetchone()
            if updated_row is None:
                expired_result = _expire_claimed_task_before_start(
                    cursor,
                    conn,
                    task_id,
                    worker_id,
                )
                if expired_result is not None:
                    return expired_result

                if is_workflow_task:
                    workflow_status = _get_workflow_status_for_task(cursor, task_id)
                    match workflow_status:
                        case 'PAUSED' | 'CANCELLED':
                            return _handle_workflow_stop_before_start(
                                cursor,
                                conn,
                                task_id,
                                workflow_status,
                                worker_id,
                                claimed_at,
                            )
                        case _:
                            pass
                conn.rollback()
                logger.warning(
                    f'Task {task_id} ownership lost - claim was reclaimed or status changed. '
                    f'Aborting execution to prevent double-execution.'
                )
                return (False, '', 'CLAIM_LOST')

            if is_workflow_task:
                cursor.execute(
                    """
                    UPDATE horsies_workflow_tasks
                    SET status = 'RUNNING',
                        started_at = NOW()
                    WHERE task_id = %s
                      AND status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                    RETURNING task_id
                    """,
                    (task_id,),
                )
                if cursor.fetchone() is None:
                    conn.rollback()
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}'
                    )
                    return (False, '', 'WORKFLOW_CHECK_FAILED')

            # First runner heartbeat rides the RUNNING transition: the row
            # can never be observed RUNNING without heartbeat coverage, and
            # the heartbeat thread skips its immediate beat (one fewer
            # round trip per task on remote links).
            cursor.execute(
                """
                INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
                VALUES (%s, %s, 'runner', NOW(), %s, %s)
                """,
                (
                    task_id,
                    worker_id + f':{os.getpid()}',
                    socket.gethostname(),
                    os.getpid(),
                ),
            )

            conn.commit()
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
    timeout_ms: int | None = None,
) -> threading.Thread:
    """Start the daemon heartbeat thread for a running task.

    Raises:
        RuntimeError: thread start failed (resource exhaustion). Runs before
            _run_task_entry's containment, so it escapes the child as a
            future failure; the parent recovers via
            _recover_worker_future_failure (the row is RUNNING by then, so
            retry policy applies).
    """
    heartbeat_thread = threading.Thread(
        target=_heartbeat_worker,
        args=(
            task_id,
            database_url,
            heartbeat_stop_event,
            worker_id,
            runner_heartbeat_interval_ms,
            timeout_ms,
        ),
        daemon=True,
        name=f'heartbeat-{task_id[:8]}',
    )
    heartbeat_thread.start()
    return heartbeat_thread


def _mark_task_finalizing(task_id: str, worker_id: str) -> None:
    """Mark a completed child result as waiting for parent finalization."""
    try:
        pool = _get_worker_pool()
        with pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE horsies_tasks
                SET finalizing_at = NOW(),
                    finalizing_by_worker_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'RUNNING'
                  AND claimed_by_worker_id = %s
                """,
                (worker_id, task_id, worker_id),
            )
            conn.commit()
    except Exception as exc:
        logger.warning('Failed to mark task %s finalizing: %s', task_id, exc)


def _run_task_entry(
    task_name: str,
    args_json: Optional[str],
    kwargs_json: Optional[str],
    task_id: str,
    database_url: str,
    master_worker_id: str,
    runner_heartbeat_interval_ms: int = 30_000,
    is_workflow_task: bool = True,
    timeout_ms: Optional[int] = None,
    claimed_at: Optional[datetime] = None,
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

    This is the child's containment seam: the execution body is wrapped in
    ``except BaseException`` and folds everything into the wire tuple. The
    pre-try steps (preflight, ownership confirm) return wire tuples
    themselves; only ``_start_heartbeat_thread`` can escape (see its
    docstring) and is recovered by the parent as a future failure.
    """
    logger.info(f'Starting task execution: {task_name} ({task_id})')

    # Mark as RUNNING in DB at the actual start of execution (child process).
    # CRITICAL: Include ownership check to prevent double-execution when claim
    # lease expires. The statement's guards also cover expiry (good_until) and
    # paused/cancelled workflows; its miss path diagnoses which one fired —
    # there is deliberately no separate pre-flight round trip.
    ownership_result = _confirm_ownership_and_set_running(
        task_id,
        master_worker_id,
        is_workflow_task,
        claimed_at,
    )
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
        timeout_ms,
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
                    error_code=OperationalErrorCode.WORKER_RESOLUTION_ERROR,
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

        # Deserialize task arguments. Strict-serde: positional args are
        # rejected at every producer; the wire shape for args is always
        # `null` or `[]`. Inspect the raw loaded JSON to enforce this
        # before any typed decode runs.
        args_json_result = loads_json(args_json)
        if is_err(args_json_result):
            return _serialization_error_response(task_name, args_json_result.err_value)
        raw_args = args_json_result.ok_value
        if raw_args is not None and raw_args != []:
            return _serialization_error_response(
                task_name,
                SerializationError(
                    f'Task {task_name!r} row carries positional args; '
                    f'strict-serde rejects positional args. '
                    f'Drop in-flight pre-strict rows before upgrading.',
                ),
            )
        args: tuple[Any, ...] = ()

        if kwargs_json is None:
            raw_kwargs: dict[str, Any] = {}
        else:
            kwargs_json_result = loads_json(kwargs_json)
            if is_err(kwargs_json_result):
                return _serialization_error_response(
                    task_name, kwargs_json_result.err_value
                )
            decoded_kwargs = kwargs_json_result.ok_value
            if not isinstance(decoded_kwargs, dict):
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'kwargs must be a JSON object, got '
                        f'{type(decoded_kwargs).__name__}',
                    ),
                )
            raw_kwargs = decoded_kwargs

        # Strict-serde phase 3+4: decode user kwargs using the receiver's
        # declared parameter types. Engine-private transport keys
        # (`__h_workflow_ctx__`, `__h_workflow_meta__`,
        # `__h_taskresult_envelope__`) pass through ``decode_kwargs``
        # unchanged because their leading prefix is in the transport
        # allowlist; per-key unpacking happens immediately below.
        underlying_fn = underlying_task_fn(task)
        try:
            kwargs = decode_kwargs(underlying_fn, raw_kwargs)
        except (StrictJsonError, ValidationError) as exc:
            return _serialization_error_response(
                task_name,
                SerializationError(
                    f'Failed to decode kwargs for {task_name}: {exc}',
                ),
            )

        # Deserialize injected TaskResults from workflow args_from.
        # Strict-serde envelope shape (engine.py emit sites):
        #   {
        #     '__h_taskresult_envelope__': True,
        #     'source_task_name': '<upstream task name>',
        #     'inner': <encode_task_result(tr, source_ok_type)>,
        #   }
        # The worker derives the upstream OkT from `app.tasks[source_task_name]`
        # and decodes via `decode_task_result(inner, ok_type)`.
        for key, value in list(kwargs.items()):
            if not (
                isinstance(value, dict)
                and value.get(_ENGINE_KEY_TASKRESULT_ENVELOPE) is True
            ):
                continue
            envelope = cast('dict[str, Any]', value)
            source_task_name = envelope.get('source_task_name')
            if not isinstance(source_task_name, str):
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'args_from envelope for kwarg {key!r} is missing '
                        f'source_task_name'
                    ),
                )
            inner = envelope.get('inner')
            source_definition_key = envelope.get('source_definition_key')
            from horsies.core.models.workflow.typing_utils import (
                resolve_source_ok_type,
            )

            source_ok_type = resolve_source_ok_type(
                app, source_task_name,
                sub_definition_key=(
                    source_definition_key
                    if isinstance(source_definition_key, str)
                    else None
                ),
            )
            if source_ok_type is None:
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'args_from source {source_task_name!r} is not '
                        f'registered (no task or workflow definition with '
                        f'this name); cannot decode kwarg {key!r}'
                    ),
                )
            try:
                kwargs[key] = decode_task_result(inner, source_ok_type)
            except (StrictJsonError, ValidationError) as exc:
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'decode_task_result failed for kwarg {key!r} '
                        f'(source task {source_task_name!r}): {exc}'
                    ),
                )

        # Handle workflow injection payloads (workflow_ctx / workflow_meta).
        workflow_ctx_data = kwargs.pop(_ENGINE_KEY_WORKFLOW_CTX, None)
        workflow_meta_data = kwargs.pop(_ENGINE_KEY_WORKFLOW_META, None)
        if workflow_ctx_data is not None or workflow_meta_data is not None:
            import inspect

            underlying_fn = underlying_task_fn(task)
            sig = inspect.signature(underlying_fn)
            if workflow_ctx_data is not None and 'workflow_ctx' in sig.parameters:
                from horsies.core.models.workflow import (
                    WorkflowContext,
                    SubWorkflowSummary,
                )

                results_by_id_raw = workflow_ctx_data.get('results_by_id', {})
                task_name_by_id = workflow_ctx_data.get('task_name_by_id', {})
                definition_key_by_id = workflow_ctx_data.get(
                    'definition_key_by_id', {},
                )
                results_by_id: dict[str, TaskResult[Any, TaskError]] = {}
                for node_id, envelope in results_by_id_raw.items():
                    if envelope is None:
                        # Engine couldn't encode this entry (no
                        # registered task or no ok_type); surface as a
                        # sentinel TaskError so the context still has a
                        # typed value.
                        results_by_id[node_id] = TaskResult(
                            err=TaskError(
                                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=(
                                    f'workflow_ctx result for node '
                                    f'{node_id!r} unavailable (source '
                                    f'task not registered at encode '
                                    f'time)'
                                ),
                                data={'node_id': node_id},
                            ),
                        )
                        continue
                    source_task_name = task_name_by_id.get(node_id)
                    if not isinstance(source_task_name, str):
                        results_by_id[node_id] = TaskResult(
                            err=TaskError(
                                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=(
                                    f'workflow_ctx missing task_name for '
                                    f'node {node_id!r}; cannot decode'
                                ),
                                data={'node_id': node_id},
                            ),
                        )
                        continue
                    from horsies.core.models.workflow.typing_utils import (
                        resolve_source_ok_type as _resolve_src_ok,
                    )

                    _sub_def_key = definition_key_by_id.get(node_id)
                    source_ok_type = _resolve_src_ok(
                        app, source_task_name,
                        sub_definition_key=(
                            _sub_def_key
                            if isinstance(_sub_def_key, str)
                            else None
                        ),
                    )
                    if source_ok_type is None:
                        results_by_id[node_id] = TaskResult(
                            err=TaskError(
                                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=(
                                    f'workflow_ctx source '
                                    f'{source_task_name!r} not '
                                    f'registered in this process (no task '
                                    f'or workflow definition with this name)'
                                ),
                                data={
                                    'node_id': node_id,
                                    'source_task_name': source_task_name,
                                },
                            ),
                        )
                        continue
                    try:
                        results_by_id[node_id] = decode_task_result(
                            envelope, source_ok_type,
                        )
                    except (StrictJsonError, ValidationError) as exc:
                        results_by_id[node_id] = TaskResult(
                            err=TaskError(
                                error_code=OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
                                message=(
                                    f'decode_task_result failed for '
                                    f'workflow_ctx node {node_id!r}: '
                                    f'{exc}'
                                ),
                                data={
                                    'node_id': node_id,
                                    'source_task_name': source_task_name,
                                },
                            ),
                        )

                summaries_by_id_raw = workflow_ctx_data.get('summaries_by_id', {})
                summaries_by_id: dict[str, SubWorkflowSummary[Any]] = {}
                for node_id, summary_json in summaries_by_id_raw.items():
                    if isinstance(summary_json, str):
                        sj = loads_json(summary_json)
                        if is_err(sj):
                            return _serialization_error_response(
                                task_name, sj.err_value
                            )
                        try:
                            summaries_by_id[node_id] = SubWorkflowSummary.from_json(
                                sj.ok_value,
                            )
                        except ValueError as summary_exc:
                            return _serialization_error_response(
                                task_name, SerializationError(str(summary_exc))
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
        logger.info(f'Task execution completed: {task_name} ({task_id})')

        # Strict-serde phase 5: result encoding routes through
        # ``encode_task_result(result, ok_type)`` so the wire envelope
        # uses ``__h_task_result__`` and the ok-slot is encoded against
        # the task's declared ``OkT`` (path-aware reserved-key scan and
        # JsonValue fence inherited from ``encode_value``).
        ok_type = getattr(task, 'task_ok_type', None)
        if ok_type is None:
            return _serialization_error_response(
                task_name,
                SerializationError(
                    f'Task {task_name!r} has no `task_ok_type`; '
                    f'cannot encode return value'
                ),
            )

        if isinstance(out, TaskResult):
            try:
                envelope = encode_task_result(out, ok_type)
            except (
                StrictJsonError,
                ValidationError,
                PydanticSerializationError,
            ) as exc:
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'encode_task_result failed for {task_name!r}: '
                        f'{exc}'
                    ),
                )
            ser = dumps_json(envelope)
            if is_err(ser):
                return _serialization_error_response(task_name, ser.err_value)
            return (True, ser.ok_value, None)

        if out is None:
            tr: TaskResult[Any, TaskError] = TaskResult(
                err=TaskError(
                    error_code=OperationalErrorCode.TASK_EXCEPTION,
                    message=f'Task {task_name} returned None instead of TaskResult or value',
                    data={'task_name': task_name},
                ),
            )
            return (True, serialize_error_payload(tr), 'Task returned None')

        # Plain value → wrap into success
        wrapped: TaskResult[Any, TaskError] = TaskResult(ok=out)
        try:
            envelope = encode_task_result(wrapped, ok_type)
        except (
            StrictJsonError,
            ValidationError,
            PydanticSerializationError,
        ) as exc:
            return _serialization_error_response(
                task_name,
                SerializationError(
                    f'encode_task_result failed for {task_name!r} '
                    f'(plain-value wrap): {exc}'
                ),
            )
        ser = dumps_json(envelope)
        if is_err(ser):
            return _serialization_error_response(task_name, ser.err_value)
        return (True, ser.ok_value, None)

    except BaseException as e:
        logger.error(f'Task exception: {task_name}: {e}')
        # If the task raised an exception, we wrap it in a TaskError
        tr = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.TASK_EXCEPTION,
                message=f'{type(e).__name__}: {e}',
                data={'task_name': task_name},
                exception=e,  # pydantic will accept and we flatten on serialization if needed elsewhere
            )
        )
        return (True, serialize_error_payload(tr), None)

    finally:
        # Mark finalizing before stopping heartbeat so the reaper does not
        # mistake a completed child for a crashed child while the parent writes
        # the durable terminal state.
        _mark_task_finalizing(task_id, master_worker_id)
        # Always stop the heartbeat thread when task completes (success/failure/error)
        heartbeat_stop_event.set()
        logger.debug(f'Heartbeat stop signal sent for task {task_id}')


_CHILD_RUNNER_ENTRYPOINTS = (_child_initializer, _run_task_entry)
