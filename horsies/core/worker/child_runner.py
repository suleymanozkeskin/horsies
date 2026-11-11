"""Child-process helpers: import resolution and task execution."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import os
import socket
import threading
from collections.abc import Callable, Coroutine, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from typing import Any, Final, LiteralString, Optional, Tuple, assert_never, cast

from psycopg import Connection, Cursor
from psycopg.rows import namedtuple_row


from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import (
    declared_taskresult_ok_type,
    decode_kwargs,
    fill_omitted_defaults,
    underlying_task_fn,
)
from horsies.core.codec.json_io import (
    loads_json,
    dumps_json,
    SerializationError,
)
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.typed import (
    conform_to_target,
    decode_task_result,
    encode_returned_result,
)
from horsies.core.codec.validation_errors import ValueValidationError, exception_text


# Engine transport keys (strict-serde §8). Pinned here so the worker
# consumer stays in lockstep with the engine emitters in
# ``horsies/core/workflows/engine.py``.
_ENGINE_KEY_WORKFLOW_CTX = '__h_workflow_ctx__'
_ENGINE_KEY_WORKFLOW_META = '__h_workflow_meta__'
_ENGINE_KEY_TASKRESULT_ENVELOPE = '__h_taskresult_envelope__'
from horsies.core.models.boot_failure import ThreadStartRefusedError
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MAX_TASK_TIMEOUT_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
    TaskResult,
    TaskError,
    OperationalErrorCode,
    OutcomeCode,
)
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutRefusal,
    TimeoutValueInvalid,
)
from horsies.core.worker.child_protocol import WireResult
from horsies.core.worker.run_return import (
    IsolatedMarkerWindow,
    MarkerAfterLeftWork,
    MarkerAtReturn,
    RunReturn,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.task_loop import (
    AsyncTaskPresence,
    LoopClosed,
    LoopNotCreated,
    LoopOpen,
    TaskLoop,
)
from horsies.core.lifecycle.commands import (
    AbandonOwnedNode,
    CancelOwnedNode,
    ExpireOwnedClaim,
)
from horsies.core.lifecycle.fences import OwnedClaim, WorkerOwned
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedWorkflowState,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_sync
from horsies.core.task_decorator import TaskBodyKind, await_task_call_in_worker
from horsies.core.types.result import Err, Ok, is_err


@dataclass(frozen=True, slots=True)
class _WorkflowResumedBeforeStart:
    """The pre-start abandon refused: the workflow is no longer PAUSED.

    A resume committed after this child read PAUSED. The claim is still this
    child's and the task is runnable again, so the RUNNING transition is
    attempted again instead of stopping the task.
    """

    workflow_status: str


@dataclass(frozen=True, slots=True)
class StartAllowed:
    """The run may set its row RUNNING."""


@dataclass(frozen=True, slots=True)
class DeadlineBeforeStart:
    """The deadline of the run fired before its RUNNING update, while it
    waited for a pooled connection. The run does not start: its row stays
    CLAIMED, and the timeout of the worker returns it to the queue."""


type StartCheck = StartAllowed | DeadlineBeforeStart
"""Whether a run may still start, read after it got its pooled connection."""


@dataclass(frozen=True, slots=True)
class StatementsBound:
    """The statements of one write, once it holds its connection, run at most
    ``seconds``. A timer then cancels the statement that runs; the
    transaction aborts, so every later statement of the write fails at
    once, and the write ends with its error path."""

    seconds: float


@dataclass(frozen=True, slots=True)
class StatementsUnbounded:
    """The statements of a write have no bound of their own."""


type StatementsLimit = StatementsBound | StatementsUnbounded

CANCEL_REQUEST_TIMEOUT_S: Final = 5.0
"""The longest wait for the server to take a cancel request of a bounded
write (``StatementsBound``)."""

LEFT_WORK_MARKER_CONNECTION_WAIT_S: Final = 5.0
"""The longest wait for a pooled connection of the finalizing marker of an
isolated run whose task left work on its task loop (``MarkerAfterLeftWork``).
A marker that gets no connection in time is skipped: the runner heartbeats of
the worker cover the row until it reads the end of the run."""

LEFT_WORK_MARKER_STATEMENTS_S: Final = 5.0
"""The longest time that the statement of that marker takes once it holds
its connection."""

LEFT_WORK_MARKER_BOUND_S: Final = (
    LEFT_WORK_MARKER_CONNECTION_WAIT_S
    + LEFT_WORK_MARKER_STATEMENTS_S
    + CANCEL_REQUEST_TIMEOUT_S
)
"""The longest time that the marker after left work takes: the wait for a
connection, the statement, and the cancel request of a statement past its
bound. The backstop of the parent after the return of such a run
(``slots.ISOLATED_LEFTOVER_BACKSTOP_S``) counts it."""


def _cancel_running_statement(conn: Connection[Any]) -> None:
    """Timer target: cancel the statement that runs on ``conn``. A failed
    cancel is logged: the statement then ends by itself."""
    try:
        conn.cancel_safe(timeout=CANCEL_REQUEST_TIMEOUT_S)
    except Exception as exc:
        logger.warning('Failed to cancel a statement past its bound: %s', exc)


@contextmanager
def _statements_within(
    conn: Connection[Any],
    limit: StatementsLimit,
) -> Generator[None]:
    """Run the body with the bound of ``limit`` on the statements of ``conn``.

    ``StatementsBound`` starts a timer thread that cancels the running
    statement after its seconds. On exit the timer is cancelled and joined,
    so no cancel reaches the connection after it returns to the pool.

    Raises:
        ThreadStartRefusedError: the OS refused the timer thread; no
            statement ran.
    """
    match limit:
        case StatementsUnbounded():
            yield
            return
        case StatementsBound(seconds=seconds):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    timer = threading.Timer(seconds, _cancel_running_statement, args=(conn,))
    timer.daemon = True
    try:
        timer.start()
    except RuntimeError as exc:
        raise ThreadStartRefusedError('the bound of a write', exc) from exc
    try:
        yield
    finally:
        timer.cancel()
        timer.join()


def start_always_allowed() -> StartCheck:
    """The start check of a run with no deadline in its child, as in an
    isolated child: the parent holds that deadline."""
    return StartAllowed()


# One attempt plus one after a resume. A workflow paused and resumed again
# between the two ends as OWNERSHIP_UNCONFIRMED; the claim lease then
# returns the task to the queue.
_PRE_START_CONFIRM_ATTEMPTS = 2


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


def _stored_timeout_refusal_response(
    task_name: str,
    refusal: TimeoutRefusal,
) -> tuple[bool, str, str | None]:
    """Build a WORKER_SERIALIZATION_ERROR response for a stored timeout.

    The row is RUNNING when this runs, so the normal persist path fails the
    task. `data` names the stored field that the worker cannot use. An
    isolated child and a seat of a shared async child build the same response.
    """
    match refusal:
        case OptionsUnreadable(reason=reason):
            data: dict[str, str | int] = {'task_name': task_name, 'field': 'task_options'}
        case TimeoutValueInvalid(reason=reason):
            data = {
                'task_name': task_name,
                'field': 'timeout_ms',
                'min_ms': MIN_TASK_TIMEOUT_MS,
                'max_ms': MAX_TASK_TIMEOUT_MS,
            }
        case CancelGraceInvalid(reason=reason):
            data = {
                'task_name': task_name,
                'field': 'cancel_grace_ms',
                'min_ms': MIN_CANCEL_GRACE_MS,
                'max_ms': MAX_CANCEL_GRACE_MS,
            }
        case _ as unreachable:
            assert_never(unreachable)
    logger.error(f'Stored timeout refused for task {task_name}: {reason}')
    tr: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.WORKER_SERIALIZATION_ERROR,
            message=reason,
            data=data,
        ),
    )
    return (True, serialize_error_payload(tr), f'StoredTimeoutRefused: {reason}')


from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.logging import get_logger
from horsies.core.process_role import mark_process_runs_task_bodies
from horsies.core.worker.current import get_current_app, set_current_app
from horsies.core.worker.child_pool import (
    CHILD_POOL_CONNECTION_WAIT_S,
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
    """Drop any broker that importing the app built in this child.

    ``app.get_broker()`` then builds a new one, after the worker role is set.
    """
    match getattr(app, '_broker', None):
        case PostgresBroker() as broker:
            try:
                broker.discard_inherited_resources()
            except Exception as exc:
                logger.warning('Failed to discard the import-time broker in child: %s', exc)
        case _:
            # Importing the app built no broker.
            pass
    try:
        setattr(app, '_broker', None)
    except Exception as exc:
        logger.warning('Failed to reset the import-time broker in child: %s', exc)


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


def start_hook_watchdog(hook_name: str) -> threading.Timer:
    """Start the thread that ends the process when the start hook
    ``hook_name`` runs longer than ``CHILD_HOOK_TIMEOUT_SECONDS``.

    The caller cancels the returned timer when the hook returns.

    Raises:
        ThreadStartRefusedError: the OS refused the thread. The hook has
            not run then.
    """
    watchdog = threading.Timer(
        CHILD_HOOK_TIMEOUT_SECONDS,
        _exit_for_hung_hook,
        args=(hook_name,),
    )
    watchdog.daemon = True
    try:
        watchdog.start()
    except RuntimeError as exc:
        raise ThreadStartRefusedError(
            f'the watchdog of start hook {hook_name!r}',
            exc,
        ) from exc
    return watchdog


def _run_child_start_hooks(app: Horsies) -> None:
    """Run app-registered child-start hooks; any failure is terminal.

    Fail-closed by design: a failed pool-policy install must not silently
    proceed to the state the hook exists to prevent. Failure or timeout
    logs the hook name and exits with CHILD_HOOK_FAILURE_EXIT_CODE, which
    the parent recognizes and stops the worker rather than restart-looping.
    Best-effort work belongs inside the hook's own try/except.

    Raises:
        ThreadStartRefusedError: the OS refused the watchdog of a hook,
            before that hook ran. The boot names the cause
            ``ThreadStartRefused``.
    """
    for hook in app.get_child_process_start_hooks():
        hook_name = getattr(hook, '__qualname__', repr(hook))
        watchdog = start_hook_watchdog(hook_name)
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
            which ends this child before it reports ready; the parent counts
            that as a failed boot. A failing on_child_process_start hook does
            not raise — _run_child_start_hooks exits the child with
            CHILD_HOOK_FAILURE_EXIT_CODE (fail-closed).
    """
    app = import_app_for_child(app_locator, imports, sys_path_roots, loglevel)
    finish_child_start(
        app,
        database_url,
        connect_kwargs,
        child_pool_min_size,
        child_pool_max_size,
        child_pool_check,
    )


def import_app_for_child(
    app_locator: str,
    imports: Sequence[str],
    sys_path_roots: Sequence[str],
    loglevel: int,
) -> Horsies:
    """Locate the app and import its task modules. Opens no connection.

    A preload parent runs this before it forks, so children share the result.

    Raises:
        Exception: any import failure propagates to the caller.
    """
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
    return app


def finish_child_start(
    app: Horsies,
    database_url: str,
    connect_kwargs: Mapping[str, object] | None,
    child_pool_min_size: int,
    child_pool_max_size: int,
    child_pool_check: bool,
) -> None:
    """Run the per-child steps. Each child runs these itself, after any fork.

    Raises:
        Exception: a pool setup failure propagates to the caller. A failing
            hook exits the child with CHILD_HOOK_FAILURE_EXIT_CODE.
    """
    # Every thread and event loop of this process serves task bodies from
    # here on, so result waits fail closed in it, also in the start hooks.
    mark_process_runs_task_bodies()
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


def finish_shared_child_start(app: Horsies) -> None:
    """Run the per-child steps of a shared async child that need no loop.

    The same steps as ``finish_child_start`` up to the pool: the process
    runs task bodies from here on, and the ``on_child_process_start`` hooks
    run. A shared async child opens no sync pool: it opens an async pool on
    its event loop afterwards (``shared_runner``). A failing hook exits the
    child with CHILD_HOOK_FAILURE_EXIT_CODE.
    """
    mark_process_runs_task_bodies()
    _run_child_start_hooks(app)


# ---------- Child-process execution ----------
# The statements of the writes around one attempt. An isolated child runs
# them on a sync connection here; a shared async child runs the same text on
# an async connection (``horsies.core.worker.shared_lifecycle``).

WORKFLOW_STATUS_OF_TASK_SQL: Final[LiteralString] = """
        SELECT w.status FROM horsies_workflows w
        JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
        WHERE wt.task_id = %s
        """

CLAIM_STILL_STARTABLE_SQL: Final[LiteralString] = """
        SELECT EXISTS (
            SELECT 1 FROM horsies_tasks
            WHERE id = %s
              AND status = 'CLAIMED'
              AND claimed_by_worker_id = %s
              AND (%s::timestamptz IS NULL OR claimed_at = %s::timestamptz)
              AND (claim_expires_at IS NULL OR claim_expires_at > now())
              AND (good_until IS NULL OR good_until > now())
        ) AS startable
        """

RESET_PAUSED_NODE_SQL: Final[LiteralString] = """
                        UPDATE horsies_workflow_tasks
                        SET status = 'READY',
                            task_id = NULL,
                            started_at = NULL
                        WHERE task_id = %s
                          AND status IN ('ENQUEUED', 'RUNNING')
                        """

SKIP_CANCELLED_NODE_SQL: Final[LiteralString] = """
                        UPDATE horsies_workflow_tasks
                        SET status = 'SKIPPED',
                            completed_at = NOW()
                        WHERE task_id = %s
                          AND status IN ('ENQUEUED', 'READY')
                        """

_SET_RUNNING_HEAD_SQL: Final[LiteralString] = """
                WITH started AS (
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

_SET_RUNNING_WORKFLOW_GUARD_SQL: Final[LiteralString] = """
                  AND NOT EXISTS (
                      SELECT 1
                      FROM horsies_workflow_tasks wt
                      JOIN horsies_workflows w ON w.id = wt.workflow_id
                      WHERE wt.task_id = %s
                        AND w.status IN ('PAUSED', 'CANCELLED')
                  )
            """

_SET_RUNNING_TAIL_SQL: Final[LiteralString] = """
                    RETURNING id
                ),
                first_beat AS (
                    INSERT INTO horsies_heartbeats (
                        task_id, sender_id, role, sent_at, hostname, pid
                    )
                    SELECT id, %s, 'runner', NOW(), %s, %s
                    FROM started
                )
                SELECT id FROM started
                """

START_WORKFLOW_NODE_SQL: Final[LiteralString] = """
                    WITH linked AS MATERIALIZED (
                        SELECT id, status
                        FROM horsies_workflow_tasks
                        WHERE task_id = %s
                          AND status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                        FOR UPDATE
                    ),
                    changed AS (
                        UPDATE horsies_workflow_tasks node
                        SET status = 'RUNNING',
                            started_at = NOW()
                        FROM linked
                        WHERE node.id = linked.id
                          AND linked.status <> 'RUNNING'
                        RETURNING node.id
                    )
                    SELECT EXISTS (SELECT 1 FROM linked) AS node_linked
                    """

MARK_FINALIZING_SQL: Final[LiteralString] = """
                UPDATE horsies_tasks
                SET finalizing_at = NOW(),
                    finalizing_by_worker_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND status = 'RUNNING'
                  AND claimed_by_worker_id = %s
                  AND (%s::timestamptz IS NULL OR claimed_at = %s::timestamptz)
                """


def set_running_sql(is_workflow_task: bool) -> LiteralString:
    """The RUNNING transition with its first runner heartbeat. A workflow task
    adds the guard against a paused or cancelled workflow."""
    match is_workflow_task:
        case True:
            return (
                _SET_RUNNING_HEAD_SQL
                + _SET_RUNNING_WORKFLOW_GUARD_SQL
                + _SET_RUNNING_TAIL_SQL
            )
        case False:
            return _SET_RUNNING_HEAD_SQL + _SET_RUNNING_TAIL_SQL


def set_running_params(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: Optional[datetime],
) -> tuple[object, ...]:
    """The parameters of ``set_running_sql(is_workflow_task)``, in order.

    The row and the first beat name this process: its pid, its host, and the
    sender ``<worker_id>:<pid>``. Reads the pid and the host name; no I/O.
    """
    pid = os.getpid()
    hostname = socket.gethostname()
    params: list[object] = [
        pid,
        hostname,
        f'worker-{pid}',
        task_id,
        worker_id,
        claimed_at,
        claimed_at,
    ]
    if is_workflow_task:
        params.append(task_id)
    params += [f'{worker_id}:{pid}', hostname, pid]
    return tuple(params)


def _get_workflow_status_for_task(cursor: Cursor[Any], task_id: str) -> str | None:
    """Read the owning workflow's status for a task (None if not in one).

    Raises:
        Exception: DB errors propagate to the wrapped caller,
            _confirm_ownership_and_set_running, which folds them into
            OWNERSHIP_UNCONFIRMED.
    """
    cursor.execute(WORKFLOW_STATUS_OF_TASK_SQL, (task_id,))
    row = cursor.fetchone()
    if row is None:
        return None
    status = row.status
    if isinstance(status, str):
        return status
    return None


def _claim_still_startable(
    cursor: Cursor[Any],
    task_id: str,
    worker_id: str,
    claimed_at: Optional[datetime],
) -> bool:
    """Whether every guard of the RUNNING transition but the workflow holds.

    The same claim, lease, and deadline predicates as the transition; a NULL
    claimed_at disables the generation half, as there.
    """
    cursor.execute(
        CLAIM_STILL_STARTABLE_SQL,
        (task_id, worker_id, claimed_at, claimed_at),
    )
    row = cursor.fetchone()
    assert row is not None, 'SELECT EXISTS returns one row'
    return bool(row[0])


def _handle_workflow_stop_before_start(
    cursor: Cursor[Any],
    conn: Connection[Any],
    task_id: str,
    workflow_status: str,
    worker_id: str,
    claimed_at: Optional[datetime] = None,
) -> Tuple[bool, str, Optional[str]] | _WorkflowResumedBeforeStart:
    """Skip a task whose workflow is PAUSED/CANCELLED; return the wire tuple.

    Both branches carry the ClaimedOwnerFence pair (worker_id, claimed_at) —
    the same fence _confirm_ownership_and_set_running applies to the
    CLAIMED->RUNNING transition. These are claimed pre-start operations, so a
    row requeued and re-claimed since this child was handed its dispatch is a
    different claim generation and must not be terminalized here. A NULL
    claimed_at disables the generation half, matching the ownership confirm.

    PAUSED: the abandon re-checks the pause under the workflow lock. A refusal
    because the workflow was resumed returns ``_WorkflowResumedBeforeStart``
    with nothing written. The node is reset to READY only when the abandon
    applied (or was already applied): a lost claim, another state conflict,
    or an absent row leaves the node to the claim generation that owns it.

    CANCELLED: the node is marked SKIPPED only when the cancel applied (or
    was already applied); any other outcome leaves the node to the writer of
    the row's actual transition.

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
            outcome = apply_sync(
                cursor,
                AbandonOwnedNode(
                    task_id=task_id,
                    fence=OwnedClaim(
                        worker_id=worker_id,
                        claimed_at=claimed_at,
                    ),
                ),
            )
            match outcome:
                case SourceStateConflict(
                    evidence=ObservedWorkflowState(
                        workflow_id=workflow_id,
                        workflow_status=resumed_status,
                    ),
                ):
                    conn.rollback()
                    logger.info(
                        f'Task {task_id} abandon refused: workflow {workflow_id} '
                        f'is {resumed_status}, no longer PAUSED'
                    )
                    return _WorkflowResumedBeforeStart(
                        workflow_status=resumed_status,
                    )
                case Applied() | AlreadyApplied():
                    cursor.execute(RESET_PAUSED_NODE_SQL, (task_id,))
                case LostClaim() | SourceStateConflict() | TaskAbsent():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            conn.commit()
            return (False, '', 'WORKFLOW_STOPPED')
        case 'CANCELLED':
            # Cancellation is terminal: cancel the task row, then mark the
            # node SKIPPED in the same transaction, only when the cancel
            # applied (or was already applied).
            outcome = apply_sync(
                cursor,
                CancelOwnedNode(
                    task_id=task_id,
                    fence=OwnedClaim(
                        worker_id=worker_id,
                        claimed_at=claimed_at,
                    ),
                    accepts_requeued_pending=True,
                ),
            )
            match outcome:
                case Applied() | AlreadyApplied():
                    cursor.execute(SKIP_CANCELLED_NODE_SQL, (task_id,))
                case LostClaim() | SourceStateConflict() | TaskAbsent():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            conn.commit()
            return (False, '', 'WORKFLOW_STOPPED')
        case _:
            return (False, '', 'WORKFLOW_CHECK_FAILED')


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


def _confirm_until_not_resumed(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: Optional[datetime],
    *,
    start_check: Callable[[], StartCheck] = start_always_allowed,
    statements: StatementsLimit = StatementsUnbounded(),
) -> Optional[Tuple[bool, str, Optional[str]]] | DeadlineBeforeStart:
    """Run the RUNNING transition again when a resume refused the abandon.

    Returns None to start the task, or the wire tuple that stops it. A
    workflow resumed on every one of ``_PRE_START_CONFIRM_ATTEMPTS`` tries
    returns OWNERSHIP_UNCONFIRMED; the task stays claimed until its lease
    returns it to the queue. ``DeadlineBeforeStart``: ``start_check`` found
    the deadline of the run fired (``_confirm_ownership_and_set_running``).
    ``statements`` bounds the statements of each attempt.
    """
    for _ in range(_PRE_START_CONFIRM_ATTEMPTS):
        result = _confirm_ownership_and_set_running(
            task_id,
            worker_id,
            is_workflow_task,
            claimed_at,
            start_check=start_check,
            statements=statements,
        )
        match result:
            case _WorkflowResumedBeforeStart():
                continue
            case None | tuple() | DeadlineBeforeStart():
                return result
            case _ as unreachable:
                assert_never(unreachable)
    logger.warning(
        f'Task {task_id} workflow was resumed on each of '
        f'{_PRE_START_CONFIRM_ATTEMPTS} start attempts; leaving the claim '
        f'to its lease'
    )
    return (False, '', 'OWNERSHIP_UNCONFIRMED')


def _confirm_ownership_and_set_running(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool = True,
    claimed_at: Optional[datetime] = None,
    *,
    start_check: Callable[[], StartCheck] = start_always_allowed,
    statements: StatementsLimit = StatementsUnbounded(),
) -> (
    Optional[Tuple[bool, str, Optional[str]]]
    | _WorkflowResumedBeforeStart
    | DeadlineBeforeStart
):
    """Set the row RUNNING with its first runner heartbeat. None: started.

    ``start_check`` runs once the pool gave a connection, before the
    update: a shared thread seat waits for that connection, and its
    deadline can fire meanwhile. ``DeadlineBeforeStart`` then returns with
    no statement run, so the row stays CLAIMED. ``statements`` bounds the
    statements after the wait for the connection; a cancelled statement
    gives OWNERSHIP_UNCONFIRMED, as a database error does.
    """
    try:
        pool = _get_worker_pool()
        with pool.connection() as conn, _statements_within(conn, statements):
            match start_check():
                case StartAllowed():
                    pass
                case DeadlineBeforeStart() as passed:
                    return passed
                case _ as unreachable:
                    assert_never(unreachable)
            cursor = conn.cursor(row_factory=namedtuple_row)
            # The first runner heartbeat is part of the transition: the row can
            # never be observed RUNNING without heartbeat coverage, and a
            # transition that does not apply inserts no beat. The worker
            # process writes the later beats while this child holds the task.
            # The claimed_at pair is the claim-generation fence (C10): the
            # dispatch that submitted this child carries the claim's own
            # claimed_at; a row requeued and re-claimed since then (same
            # worker or not) no longer matches, so the stale child aborts
            # with CLAIM_LOST instead of double-running the task.
            cursor.execute(
                set_running_sql(is_workflow_task),
                set_running_params(task_id, worker_id, is_workflow_task, claimed_at),
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
                        case 'RUNNING' if _claim_still_startable(
                            cursor,
                            task_id,
                            worker_id,
                            claimed_at,
                        ):
                            # The transition missed on the paused guard, and
                            # a resume committed before this read.
                            conn.rollback()
                            return _WorkflowResumedBeforeStart(
                                workflow_status=workflow_status,
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
                # A retried attempt finds its node already RUNNING: the node
                # keeps its tuple and its first started_at. Only a node that
                # is not RUNNING yet is written. A node in none of the four
                # statuses (or no node) fails the workflow check.
                cursor.execute(START_WORKFLOW_NODE_SQL, (task_id,))
                handoff_row = cursor.fetchone()
                assert handoff_row is not None, 'SELECT EXISTS returns one row'
                node_linked = bool(handoff_row[0])
                if not node_linked:
                    conn.rollback()
                    logger.error(
                        f'Failed to update workflow_tasks to RUNNING for task {task_id}'
                    )
                    return (False, '', 'WORKFLOW_CHECK_FAILED')

            conn.commit()
        return None
    except Exception as e:
        logger.error(f'Failed to transition task {task_id} to RUNNING: {e}')
        return (False, '', 'OWNERSHIP_UNCONFIRMED')


def _mark_task_finalizing(
    task_id: str,
    worker_id: str,
    claimed_at: Optional[datetime],
    *,
    connection_wait_s: float = CHILD_POOL_CONNECTION_WAIT_S,
    statements: StatementsLimit = StatementsUnbounded(),
) -> None:
    """Mark a completed child result as waiting for parent finalization.

    The marker is fenced on the claim generation (worker_id, claimed_at): a
    stale attempt cannot refresh the marker of a later claim of the same
    task, which would delay that claim's stale-RUNNING recovery. A NULL
    claimed_at disables the generation half, matching the ownership confirm.
    The worker's runner heartbeat skips a row that carries the marker, so
    the beats of the attempt end here, also while the worker's event loop
    has not read the result. The marker is best effort: a skipped or failed
    write is logged only. The wait for a pooled connection is bounded by
    ``connection_wait_s``; a shared thread seat passes the time left before
    the deadline of its run. ``statements`` bounds the statement after that
    wait.
    """
    try:
        pool = _get_worker_pool()
        with (
            pool.connection(timeout=connection_wait_s) as conn,
            _statements_within(conn, statements),
        ):
            cursor = conn.cursor()
            cursor.execute(
                MARK_FINALIZING_SQL,
                (worker_id, task_id, worker_id, claimed_at, claimed_at),
            )
            marked_rows = cursor.rowcount
            conn.commit()
    except Exception as exc:
        logger.warning('Failed to mark task %s finalizing: %s', task_id, exc)
        return
    if marked_rows == 0:
        logger.debug(
            'Finalizing marker for task %s skipped: the claim changed',
            task_id,
        )


def mark_isolated_run_finalizing(
    task_id: str,
    worker_id: str,
    claimed_at: Optional[datetime],
    window: IsolatedMarkerWindow,
) -> None:
    """Write the finalizing marker of an isolated run that set its row
    RUNNING and has ended (``_mark_task_finalizing``).

    ``MarkerAtReturn``: the bounds of the pool, as the marker at the return
    always had. ``MarkerAfterLeftWork``: the wait for a connection is
    ``LEFT_WORK_MARKER_CONNECTION_WAIT_S``, and the statement
    ``LEFT_WORK_MARKER_STATEMENTS_S``. A failed, late or skipped write is
    logged only: nothing escapes.
    """
    match window:
        case MarkerAtReturn():
            _mark_task_finalizing(task_id, worker_id, claimed_at)
        case MarkerAfterLeftWork():
            _mark_task_finalizing(
                task_id,
                worker_id,
                claimed_at,
                connection_wait_s=LEFT_WORK_MARKER_CONNECTION_WAIT_S,
                statements=StatementsBound(LEFT_WORK_MARKER_STATEMENTS_S),
            )
        case _ as unreachable:
            assert_never(unreachable)


def async_task_presence(app: Horsies) -> AsyncTaskPresence:
    """Whether the registry of ``app`` holds an ``async def`` task function.

    Reads the in-memory registry only, and runs no code of the entries: it
    reads ``task_body_kind`` from the class of each entry, where the task
    decorator sets it. An entry with no such class attribute is not a task
    function, and the run step reports it as ``WORKER_RESOLUTION_ERROR``.
    """
    for task in app.tasks.values():
        match getattr(type(task), 'task_body_kind', None):
            case TaskBodyKind.ASYNC_DEF:
                return AsyncTaskPresence.HAS_ASYNC_TASKS
            case _:
                continue
    return AsyncTaskPresence.NO_ASYNC_TASK


def _resolution_failure(
    task_name: str,
    message: str,
    reason: str,
) -> Tuple[bool, str, Optional[str]]:
    """The wire tuple of a task the worker cannot resolve: WORKER_RESOLUTION_ERROR."""
    tr: TaskResult[Any, TaskError] = TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.WORKER_RESOLUTION_ERROR,
            message=message,
            data={'task_name': task_name},
        )
    )
    return (True, serialize_error_payload(tr), reason)


@dataclass(frozen=True, slots=True)
class OnIsolatedChild:
    """The task runs alone in an isolated child: a ``def`` task on its
    thread, an ``async def`` task on ``task_loop``."""

    task_loop: TaskLoop


@dataclass(frozen=True, slots=True)
class OnSharedSeat:
    """The task runs on a seat of a shared async child, as a coroutine on
    the running loop of that child, beside other tasks."""


@dataclass(frozen=True, slots=True)
class OnSharedThreadSeat:
    """The task runs on a seat of a shared thread child, on a thread of its
    own, beside the tasks of the other seats."""


type RunPlace = OnIsolatedChild | OnSharedSeat | OnSharedThreadSeat


@dataclass(frozen=True, slots=True)
class _PreparedCall:
    """A resolved task, the app that holds it, and the arguments of its call."""

    task: Any
    app: Horsies
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


def _why_not_runnable(body_kind: object, place: RunPlace) -> str | None:
    """Why ``place`` cannot run a registry entry, or None when it can.

    Only the task decorator makes a task function, with its
    ``task_body_kind``. An ``async def`` task in an isolated child needs the
    task loop, which the child creates at boot only when its app had an
    ``async def`` task then: a task that was registered later finds no loop.
    A run after the loop closed is a defect in the caller: ``AssertionError``.
    A seat of a shared async child runs only ``async def`` tasks: a ``def``
    body would block the loop of every task in the child. A seat of a shared
    thread child runs only ``def`` tasks: it has no event loop.
    """
    match body_kind, place:
        case TaskBodyKind.DEF, OnIsolatedChild():
            return None
        case TaskBodyKind.ASYNC_DEF, OnIsolatedChild(task_loop=task_loop):
            match task_loop.state:
                case LoopOpen():
                    return None
                case LoopNotCreated():
                    return (
                        'the task is async def, and this child has no task loop: '
                        'no async def task was registered when the child booted'
                    )
                case LoopClosed():
                    raise AssertionError('a task ran after the task loop closed')
                case _ as unreachable:
                    assert_never(unreachable)
        case TaskBodyKind.ASYNC_DEF, OnSharedSeat():
            return None
        case TaskBodyKind.DEF, OnSharedSeat():
            return 'the task is def, and a shared async child runs only async def tasks'
        case TaskBodyKind.DEF, OnSharedThreadSeat():
            return None
        case TaskBodyKind.ASYNC_DEF, OnSharedThreadSeat():
            return (
                'the task is async def, and a shared thread child runs only '
                'def tasks'
            )
        case _:
            return (
                'the registry entry is not a task function: its '
                f'task_body_kind is {body_kind!r}'
            )


def _task_call_result(
    task_name: str,
    called: TaskResult[Any, TaskError] | Coroutine[Any, Any, TaskResult[Any, TaskError]],
    task_loop: TaskLoop,
    map_exception: Callable[[BaseException], TaskResult[Any, TaskError]],
) -> TaskResult[Any, TaskError]:
    """The result of one call of a task wrapper.

    The call of a ``def`` task returns its result. The call of an
    ``async def`` task returns a coroutine, which runs on ``task_loop``; the
    child opened that loop at boot. A ``CancelledError`` or a
    ``GeneratorExit`` that leaves that coroutine goes through
    ``map_exception``, the mapping of the task
    (``await_task_call_in_worker``). Other exceptions from that run
    propagate. The wrapper returns one of the two, so another value is a
    defect in the library: ``AssertionError``.
    """
    match called:
        case TaskResult():
            return called
        case Coroutine():
            return task_loop.run(await_task_call_in_worker(called, map_exception))
        case _:
            raise AssertionError(
                f'task wrapper of {task_name!r} returned {type(called).__name__}',
            )


def _prepare_task_call(
    task_name: str,
    args_json: Optional[str],
    kwargs_json: Optional[str],
    place: RunPlace,
) -> _PreparedCall | Tuple[bool, str, Optional[str]]:
    """Resolve a task from the registry and decode the arguments of its call.

    Returns the task and its arguments, or the wire tuple that ends the run
    before user code: a task that cannot be resolved or cannot run in
    ``place`` (``WORKER_RESOLUTION_ERROR``), or arguments that do not
    decode (``WORKER_SERIALIZATION_ERROR``). Runs no user code but the
    pydantic validators of the declared types. An exception propagates to
    the caller, whose containment seam maps it. No I/O.
    """
    # Resolve from TaskRegistry
    try:
        app = get_current_app()
        task = app.tasks[task_name]  # may raise NotRegistered
        body_kind = getattr(task, 'task_body_kind', None)
    except Exception as e:
        logger.error(f'Failed to resolve task {task_name}: {e}')
        return _resolution_failure(
            task_name,
            f'Failed to resolve {task_name}: {type(e).__name__}: {e}',
            f'{type(e).__name__}: {e}',
        )
    match _why_not_runnable(body_kind, place):
        case None:
            pass
        case not_runnable:
            logger.error(f'Failed to resolve task {task_name}: {not_runnable}')
            return _resolution_failure(
                task_name,
                f'Failed to resolve {task_name}: {not_runnable}',
                not_runnable,
            )

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
    except (StrictJsonError, ValueValidationError) as exc:
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
            decoded_result = decode_task_result(inner, source_ok_type)
        except (StrictJsonError, ValueValidationError) as exc:
            return _serialization_error_response(
                task_name,
                SerializationError(
                    f'decode_task_result failed for kwarg {key!r} '
                    f'(source task {source_task_name!r}): {exc}'
                ),
            )
        target_ok_type = declared_taskresult_ok_type(underlying_fn, key)
        if target_ok_type is None or target_ok_type == source_ok_type:
            kwargs[key] = decoded_result
            continue
        match conform_to_target(decoded_result, target_ok_type):
            case Ok(ok_value=conformed):
                kwargs[key] = conformed
            case Err(err_value=refusal):
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'kwarg {key!r} does not fit its declared type '
                        f'(source task {source_task_name!r}): {refusal}'
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

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
                except (StrictJsonError, ValueValidationError) as exc:
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

    try:
        kwargs = fill_omitted_defaults(underlying_fn, kwargs)
    except (StrictJsonError, ValueValidationError) as exc:
        return _serialization_error_response(
            task_name,
            SerializationError(
                f'A default of {task_name!r} does not fit its declared type: '
                f'{exc}',
            ),
        )

    return _PreparedCall(task=task, app=app, args=args, kwargs=kwargs)


def _task_exception_response(
    task_name: str,
    e: BaseException,
) -> Tuple[bool, str, Optional[str]]:
    """The wire tuple of a task that raised: ``TASK_EXCEPTION``. Logs it."""
    logger.error(f'Task exception: {task_name}: {exception_text(e)}')
    # If the task raised an exception, we wrap it in a TaskError
    tr = TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.TASK_EXCEPTION,
            message=f'{type(e).__name__}: {exception_text(e)}',
            data={'task_name': task_name},
            exception=e,  # pydantic will accept and we flatten on serialization if needed elsewhere
        )
    )
    return (True, serialize_error_payload(tr), None)


def _encode_task_outcome(
    task_name: str,
    task: Any,
    out: TaskResult[Any, TaskError],
) -> Tuple[bool, str, Optional[str]]:
    """Encode the result of a task call for the wire, against the declared
    ok type of the task. A value that does not fit ends the run with
    ``WORKER_SERIALIZATION_ERROR``. An exception propagates. No I/O."""
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

    try:
        envelope = encode_returned_result(out, ok_type)
    except (StrictJsonError, ValueValidationError) as exc:
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


def _run_task_entry(
    task_name: str,
    args_json: Optional[str],
    kwargs_json: Optional[str],
    task_id: str,
    master_worker_id: str,
    is_workflow_task: bool = True,
    timeout: StoredTimeout = NoTimeout(),
    claimed_at: Optional[datetime] = None,
    *,
    task_loop: TaskLoop,
) -> RunReturn:
    """Run one task in an isolated child, up to its wire tuple.

    Returns the wire tuple ``(ok, serialized_task_result_json,
    worker_failure_reason)`` in ``SetRunning`` when the run set its row
    RUNNING, and in ``StoppedBeforeRunning`` when the ownership confirm
    ended it first (a lost claim, an expiry, a stopped workflow, a failed
    RUNNING update).

    An ``async def`` task runs on ``task_loop``, the one loop of this child.
    A ``def`` task runs on this thread with no running event loop.

    ok=True: we produced a valid TaskResult JSON (success *or* failure).
    ok=False: worker couldn't even produce a TaskResult (infra error).

    Do not mistake the 'ok' here as the task's own success/failure.
    It's only a signal that the worker was able to produce a valid JSON.
    The task's own success/failure is determined by the TaskResult's ok/err fields.

    This is the child's containment seam: the execution body is wrapped in
    ``except BaseException`` and folds everything into the wire tuple. The
    pre-try steps (preflight, ownership confirm) return wire tuples
    themselves.

    Writes no finalizing marker: the child writes it when the run has
    ended, after the work that the task left on its task loop
    (``child_main.mark_child_task_finalizing``).

    The child writes the first runner heartbeat, in the RUNNING transition.
    The worker process writes the later ones while this child holds the
    task.
    """
    logger.info(f'Starting task execution: {task_name} ({task_id})')

    # Mark as RUNNING in DB at the actual start of execution (child process).
    # CRITICAL: Include ownership check to prevent double-execution when claim
    # lease expires. The statement's guards also cover expiry (good_until) and
    # paused/cancelled workflows; its miss path diagnoses which one fired —
    # there is deliberately no separate pre-flight round trip.
    ownership_result = _confirm_until_not_resumed(
        task_id,
        master_worker_id,
        is_workflow_task,
        claimed_at,
    )
    match ownership_result:
        case None:
            pass
        case tuple():
            return StoppedBeforeRunning(ownership_result)
        case DeadlineBeforeStart():
            raise AssertionError(
                'an isolated child checks no deadline before its RUNNING update',
            )
        case _ as unreachable:
            assert_never(unreachable)
    return SetRunning(
        _run_isolated_after_running(
            task_name,
            args_json,
            kwargs_json,
            task_id,
            timeout,
            task_loop,
        ),
    )


def _run_isolated_after_running(
    task_name: str,
    args_json: Optional[str],
    kwargs_json: Optional[str],
    task_id: str,
    timeout: StoredTimeout,
    task_loop: TaskLoop,
) -> WireResult:
    """The steps of an isolated run after its RUNNING update: the stored
    timeout check, the resolve, the call and the result encode. Any
    exception becomes ``TASK_EXCEPTION``."""
    try:
        # A stored timeout that the worker cannot use fails the task before
        # it resolves the task, so no user code runs.
        match timeout:
            case OptionsUnreadable() | TimeoutValueInvalid() | CancelGraceInvalid():
                return _stored_timeout_refusal_response(task_name, timeout)
            case NoTimeout() | TaskTimeout():
                pass
            case _ as unreachable:
                assert_never(unreachable)

        prepared = _prepare_task_call(
            task_name,
            args_json,
            kwargs_json,
            OnIsolatedChild(task_loop),
        )
        match prepared:
            case _PreparedCall(task=task, args=args, kwargs=kwargs):
                pass
            case tuple():
                return prepared
        out = _task_call_result(
            task_name,
            task(*args, **kwargs),
            task_loop,
            task._map_exception,
        )
        logger.info(f'Task execution completed: {task_name} ({task_id})')
        return _encode_task_outcome(task_name, task, out)

    except BaseException as e:
        return _task_exception_response(task_name, e)


_CHILD_RUNNER_ENTRYPOINTS = (_child_initializer, _run_task_entry)
