"""Child-process helpers: import resolution and task execution.

A child runs from its version-2 init frame (``core.InitFrame``): the start
of each run and its miss path go through the core handle that the child
builds from that frame, and every run ends with a ``RunReturn``
(``run_return``): its result envelope after the RUNNING update, or the
reason of a stop before it.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import logging
import math
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from typing import Any, Final, Literal, assert_never, cast

import horsies._core as core
from horsies._core import (
    CoreChildHandle,
    CoreError,
    CoreFailure,
    CoreStopped,
    ForkedAfterCoreStart,
)
from horsies.core.app import Horsies
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import (
    NoRuntimeParameter,
    TakesRuntime,
    declared_outcome_code_type,
    declared_outcome_ok_type,
    decode_kwargs,
    fill_omitted_defaults,
    task_runtime_parameter,
    underlying_task_fn,
)
from horsies.core.task_runtime import (
    FencingToken,
    RunCancelCell,
    TaskRuntime,
    run_task_runtime,
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
# ``clients/python/horsies/core/workflows/engine.py``.
_ENGINE_KEY_WORKFLOW_CTX = '__h_workflow_ctx__'
_ENGINE_KEY_WORKFLOW_META = '__h_workflow_meta__'
_ENGINE_KEY_TASKRESULT_ENVELOPE = '__h_taskresult_envelope__'
from horsies.core.models.boot_failure import ThreadStartRefusedError
from horsies.core.models.schema_mode import AppRole
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MAX_TASK_TIMEOUT_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
    ContractCode,
    Execution,
    OutcomeCode,
    RetrievalCode,
    CustomCodeType,
    TaskError,
    OperationalCode,
    custom_code_text,
)
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    OptionsUnreadable,
    TimeoutRefusal,
    TimeoutValueInvalid,
)
from horsies.core.worker.child_frames import (
    PROTOCOL_NUMBERS,
    RunTrace,
    run_trace_of,
    stored_refusal_of,
)
from horsies.core.worker.child_main import end_child_after_core_refusal
from horsies.core.worker.run_return import (
    BodyInterrupted,
    CoreStoppedBeforeStart,
    EncodedResult,
    MarkerDone,
    MarkerEnd,
    MarkerMetStoppedCore,
    NothingRaised,
    RaisedInRun,
    ResultEnvelope,
    ResultFailed,
    ResultOutcome,
    ResultSucceeded,
    RunCancelled,
    RunReturn,
    RunWorkflow,
    SetRunning,
    StartedRunning,
    StoppedBeforeRunning,
    run_workflow_of,
)
from horsies.core.worker.run_cancel import (
    EndCancelled,
    EndNotCancelled,
    RunCancel,
    SkipTheBody,
    StartTheBody,
)
from horsies.core.telemetry.attempt_span import (
    AttemptSubject,
    BodyEndOrder,
    OperatorCancelFirst,
    attempt_spans_of,
    isolated_body_end,
    run_in_attempt_span,
    use_attempt_spans,
)
from horsies.core.worker.task_loop import (
    AsyncTaskPresence,
    LoopClosed,
    LoopNotCreated,
    LoopOpen,
    TaskLoop,
)
from horsies.core.lifecycle.core_records import task_row_end_of
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
    TerminalizationOutcome,
)
from horsies.core.task_decorator import TaskBodyKind, await_task_call_in_worker
from horsies.core.models.outcome import Failed, TaskOutcome
from horsies.core.task_read import typed_outcome
from horsies.core.types.result import Err, Ok, Result, is_err


@dataclass(frozen=True, slots=True)
class _WorkflowResumedBeforeStart:
    """The stop before start found a resume that came first.

    A resume committed after the start read PAUSED: the claim is still this
    child's and still startable, and its node is linked live in a RUNNING
    workflow. Nothing was written, so the RUNNING transition is attempted
    again instead of stopping the task.
    """


@dataclass(frozen=True, slots=True)
class StartAllowed:
    """The run has no deadline in this child: it may set its row RUNNING
    whenever the pool gives a connection."""


@dataclass(frozen=True, slots=True)
class StartWithin:
    """The run may set its row RUNNING only before its deadline:
    ``milliseconds`` is the time left before it at the check, 1 or more. The
    core checks it again after the pool gave the connection, before the
    statement."""

    milliseconds: int


@dataclass(frozen=True, slots=True)
class DeadlineBeforeStart:
    """The deadline of the run passed before its RUNNING update, while it
    waited for a pooled connection. The run does not start: its row stays
    CLAIMED, and the timeout of the worker returns it to the queue."""


type StartCheck = StartAllowed | StartWithin | DeadlineBeforeStart
"""Whether a run may still start, and until when, read right before the
start."""


FINALIZING_MARKER_TIMEOUT_S: Final = 10.0
"""The bound of the finalizing marker in the core (its
``FINALIZING_MARKER_BOUND``): the wait for a connection and the statement.
A marker that the core cancels at the bound is not written later."""


def start_always_allowed() -> StartCheck:
    """The start check of a run with no deadline in its child, as in an
    isolated child: the parent holds that deadline."""
    return StartAllowed()


# One attempt plus one after a resume. A workflow paused and resumed again
# between the two ends as OWNERSHIP_UNCONFIRMED; the worker then releases the
# claim (ReleaseUnconfirmedClaim), and the task goes back to the queue.
_PRE_START_CONFIRM_ATTEMPTS = 2


def attempt_elapsed_ms(attempt_began: float) -> int:
    """The time since a start attempt began at ``attempt_began`` (a
    ``time.monotonic()`` reading), in whole milliseconds, rounded up. The
    miss path of the attempt sends it to the core, which ends the path by
    the end of the attempt (``START_ATTEMPT_BOUND``). Reads the clock."""
    return max(0, math.ceil((time.monotonic() - attempt_began) * 1000.0))


@dataclass(frozen=True, slots=True)
class RunTheTask:
    """The run frame asks for the task: its stored options are usable."""


type IsolatedWork = RunTheTask | TimeoutRefusal
"""What an isolated run does after its RUNNING update: run the task, or fail
it for stored options that the worker cannot use."""


def isolated_work_of(work: Literal['RUN'] | core.StoredOptionsRefused) -> IsolatedWork:
    """The work of an isolated run frame. Pure."""
    match work:
        case 'RUN':
            return RunTheTask()
        case core.StoredOptionsRefused():
            return stored_refusal_of(work)
        case _ as unreachable:
            assert_never(unreachable)


def error_code_text(code: object) -> str:
    """The text of an error code: the value of a built-in code, or the stored
    text of a custom code (``custom_code_text``: the ``str``, or the value of
    the member). Pure."""
    match code:
        case OperationalCode() | ContractCode() | RetrievalCode() | OutcomeCode():
            return code.value
        case _:
            return custom_code_text(code)


def result_outcome_of(out: Result[Any, TaskError]) -> ResultOutcome:
    """What the envelope of ``out`` holds: its value, or its task error with
    the code text and the live exception that the error keeps. Pure."""
    match out:
        case Ok():
            return ResultSucceeded()
        case Err(err_value=error):
            match error.exception:
                case BaseException() as exception:
                    return ResultFailed(error_code_text(error.code), RaisedInRun(exception))
                case _:
                    return ResultFailed(error_code_text(error.code), NothingRaised())


def _encoded_error(error: TaskError) -> EncodedResult:
    """The envelope of an error that the run built itself, and its outcome."""
    return EncodedResult(
        ResultEnvelope(serialize_error_payload(error)),
        result_outcome_of(Err(error)),
    )


def _serialization_error_response(
    task_name: str,
    error: SerializationError,
) -> EncodedResult:
    """Build a WORKER_SERIALIZATION_ERROR response for a serde failure."""
    logger.error(f'Serialization error for task {task_name}: {error}')
    task_error = TaskError(
        code=OperationalCode.WORKER_SERIALIZATION_ERROR,
        message=str(error),
        data={'task_name': task_name},
    )
    return _encoded_error(task_error)


def _stored_timeout_refusal_response(
    task_name: str,
    refusal: TimeoutRefusal,
) -> EncodedResult:
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
    task_error = TaskError(
        code=OperationalCode.WORKER_SERIALIZATION_ERROR,
        message=reason,
        data=data,
    )
    return _encoded_error(task_error)


from horsies.core.errors import ConfigurationError, HorsiesErrorCode
from horsies.core.logging import get_logger
from horsies.core.process_role import mark_process_runs_task_bodies
from horsies.core.worker.current import get_current_app, set_current_app
from horsies.core.worker.child_pool import (
    _get_child_core_handle,
    _initialize_child_core_handle,
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
            code=HorsiesErrorCode.WORKER_INVALID_LOCATOR,
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
            code=HorsiesErrorCode.WORKER_INVALID_LOCATOR,
            notes=[
                f'locator: {app_locator!r}',
                f'resolved to: {type(obj).__name__}',
            ],
            help_text='ensure the locator points to a Horsies app instance',
        )
    return obj


def _discard_inherited_broker(app: Horsies) -> None:
    """Drop any broker that importing the app built in this child, without
    closing it.

    ``app.get_broker()`` then builds a new one, after the worker role is set.
    """
    try:
        setattr(app, '_broker', None)
    except Exception as exc:
        logger.warning('Failed to reset the import-time broker in child: %s', exc)


# Exit code for a failed/hung on_child_start hook. Distinct from
# generic child crashes so the parent stops the worker instead of
# restart-looping on an unfixable child boot.
CHILD_HOOK_FAILURE_EXIT_CODE: Final = PROTOCOL_NUMBERS.child_hook_failed_exit_code

# Per-hook execution budget (cf. Celery's 4s worker_process_init limit).
CHILD_HOOK_TIMEOUT_SECONDS = 10.0


def _exit_for_hung_hook(hook_name: str) -> None:
    """Watchdog target: a hook exceeded its budget; terminate the child."""
    logger.critical(
        "on_child_start hook '%s' exceeded %.0fs in child %s; "
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
    for hook in app.get_child_start_hooks():
        hook_name = getattr(hook, '__qualname__', repr(hook))
        watchdog = start_hook_watchdog(hook_name)
        try:
            hook()
        except BaseException as exc:
            watchdog.cancel()
            logger.critical(
                "on_child_start hook '%s' failed in child %s: %s",
                hook_name,
                os.getpid(),
                exc,
            )
            os._exit(CHILD_HOOK_FAILURE_EXIT_CODE)
        watchdog.cancel()
        logger.debug(
            "[child %s] on_child_start hook '%s' completed",
            os.getpid(),
            hook_name,
        )


_LOG_LEVELS: Final[Mapping[str, int]] = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}


def log_level_of(
    level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
) -> int:
    """The ``logging`` level of the log level of an init frame. Pure."""
    return _LOG_LEVELS[level]


def import_app_for_child(
    app_locator: str,
    imports: Sequence[str],
    sys_path_roots: Sequence[str],
    log_level: int,
    task_send_pool: int,
) -> Horsies:
    """Locate the app and import its task modules. Opens no connection.

    Before the imports, the app takes the worker role and the task-send
    pool of the kind of the child (``task_send_pool`` connections, 1 or
    more), so a broker that an import or a task builds through
    ``app.get_broker()`` opens that pool. Once the app is located, the
    horsies log records of the process take its ``AppConfig.log_format``.

    A preload parent runs this before it forks, so children share the result.

    Raises:
        Exception: any import failure propagates to the caller.
    """
    # Configure logging for this child process before any logging
    from horsies.core.logging import configure_logging

    configure_logging(log_level)

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
    # The records from here on take the app's format.
    configure_logging(log_level, log_format=app.config.log_format)
    _discard_inherited_broker(app)
    app.set_role(AppRole.WORKER)
    app.set_task_send_pool(task_send_pool)
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


def finish_child_start(app: Horsies, init: core.InitFrame) -> None:
    """Run the per-child start steps of every kind of child. Each child runs
    these itself, after any fork.

    The process runs task bodies from here on, the
    ``on_child_start`` hooks run, the child chooses its attempt
    spans from ``AppConfig.tracing`` (``use_attempt_spans``), and the core
    handle of the child is built from ``init``
    (``_initialize_child_core_handle``). Every write of the child goes
    through that handle; building it starts nothing. A shared async child
    runs its async start hooks afterwards, on its event loop
    (``shared_runner``).

    Raises:
        Exception: a failure of a step propagates to the caller. A failing
            hook exits the child with CHILD_HOOK_FAILURE_EXIT_CODE.
    """
    # Every thread and event loop of this process serves task bodies from
    # here on, so result waits fail closed in it, also in the start hooks.
    mark_process_runs_task_bodies()
    # App-owned per-child setup (engine disposal / pool policy) runs after
    # task imports so app engines exist, and before the core handle of the
    # child is built, so a failing hook aborts cleanly.
    _run_child_start_hooks(app)
    # After the hooks, where the user sets the OpenTelemetry SDK of the
    # child; the tracer of the API follows a provider set later too.
    use_attempt_spans(attempt_spans_of(app.config.tracing))
    _initialize_child_core_handle(init)
    logger.debug(f'[child {os.getpid()}] Core handle of the child built')


# ---------- Child-process execution ----------
# The start of a run, its miss path and the finalizing marker go through the
# core handle of the child (``CoreChildHandle``, ``child_pool``); a shared
# async child calls the awaitable forms of the same methods
# (``horsies.core.worker.shared_lifecycle``).

type StartMissReason = Literal[
    'DEADLINE_PASSED',
    'NOT_HELD',
    'LEASE_EXPIRED',
    'WORKFLOW_PAUSED',
    'WORKFLOW_CANCELLED',
    'NODE_NOT_LINKED',
]
"""Why ``horsies_start_attempt`` did not start the row: the first guard that
failed, in the function's order (``AttemptMissed.reason``).

- ``DEADLINE_PASSED``: CLAIMED by this worker at any claim generation, and
  ``good_until`` passed: the expiry applies.
- ``NOT_HELD``: absent, or the status, owner or claim generation differs.
- ``LEASE_EXPIRED``: the claim lease passed.
- ``WORKFLOW_PAUSED``, ``WORKFLOW_CANCELLED``: the workflow of the task.
- ``NODE_NOT_LINKED``: a workflow task whose node is absent or not in a
  runnable status.
"""

type StopRecord = (
    core.StoppedBeforeStart | core.ResumedBeforeStart | core.StopBeforeStartFailed
)
"""The records of ``CoreChildHandle.stop_workflow_task_before_start``."""

type FinalizingRecord = (
    core.FinalizingMarked | core.FinalizingNotHeld | core.MarkFinalizingFailed
)
"""The records of ``CoreChildHandle.mark_finalizing``."""


def log_finalizing_record(record: FinalizingRecord) -> None:
    """Log the end of a finalizing marker that is not written: DEBUG when
    the claim changed, WARNING when the write failed. A written marker logs
    nothing."""
    match record:
        case core.FinalizingMarked():
            pass
        case core.FinalizingNotHeld(task_id=task_id):
            logger.debug(
                'Finalizing marker for task %s skipped: the claim changed',
                task_id,
            )
        case core.MarkFinalizingFailed(task_id=task_id, message=message):
            logger.warning('Failed to mark task %s finalizing: %s', task_id, message)
        case _ as unreachable:
            assert_never(unreachable)


def _claim_lost(task_id: str) -> StoppedBeforeRunning:
    """The stop of a start whose claim is gone. Logs a warning."""
    logger.warning(
        f'Task {task_id} ownership lost - claim was reclaimed or status changed. '
        f'Aborting execution to prevent double-execution.'
    )
    return StoppedBeforeRunning('CLAIM_LOST')


def _start_unconfirmed(task_id: str, cause: object) -> StoppedBeforeRunning:
    """The stop of a start whose write failed: OWNERSHIP_UNCONFIRMED. The
    worker then releases the claim. Logs an error with ``cause``."""
    logger.error(f'Failed to transition task {task_id} to RUNNING: {cause}')
    return StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


def start_ended_by(
    task_id: str,
    raised: CoreError | CoreFailure,
) -> StoppedBeforeRunning | CoreStoppedBeforeStart:
    """The end of a start (the start statement or its miss path) that raised
    ``raised``. No task code ran.

    - ``CoreStopped``: ``CoreStoppedBeforeStart``. The child reports it as
      ``OWNERSHIP_UNCONFIRMED``, so the worker releases the claim with no
      attempt charged, and then ends itself.
    - ``ForkedAfterCoreStart``: this process is a fork of the child. It ends
      at once with ``CORE_STOPPED_EXIT_CODE`` and writes no frame: a fork
      must not write on the channel of the child it was forked from. Does
      not return.
    - Any other error: OWNERSHIP_UNCONFIRMED (``_start_unconfirmed``).
    """
    match raised:
        case CoreStopped():
            return CoreStoppedBeforeStart(raised)
        case ForkedAfterCoreStart():
            end_child_after_core_refusal(raised, f'At the start of task {task_id}')
        case _:
            return _start_unconfirmed(task_id, raised)


def expired_before_start_of(
    task_id: str,
    outcome: TerminalizationOutcome,
) -> StoppedBeforeRunning:
    """The stop of the expiry of a claim whose deadline passed before its
    start (``horsies_expire_owned_claim``): ``Applied`` and
    ``AlreadyApplied`` mean the task is EXPIRED; a refusal means the claim
    is gone. Logs the end. Pure but the log."""
    match outcome:
        case Applied() | AlreadyApplied():
            logger.info(
                f'Task {task_id} expired before actual execution start; marked EXPIRED.',
            )
            return StoppedBeforeRunning('TASK_EXPIRED')
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            return _claim_lost(task_id)
        case _ as unreachable:
            assert_never(unreachable)


def stopped_before_start_of(
    task_id: str,
    record: StopRecord,
) -> StoppedBeforeRunning | _WorkflowResumedBeforeStart:
    """The stop of a workflow task whose start missed
    (``horsies_workflow_task_stop_before_start``), by the step that ran:

    - ``EXPIRED``: TASK_EXPIRED; phase 2 of the node is owed.
    - ``ABANDONED`` (PAUSED workflow), ``CANCELLED`` (CANCELLED workflow):
      WORKFLOW_STOPPED, whatever the outcome of the move.
    - ``ORPHAN_CANCELLED``: ORPHAN_CANCELLED; nothing is owed.
    - ``NODE_NOT_LINKED`` (the orphan self-heal is off): NODE_NOT_LINKED;
      the row stays CLAIMED.
    - ``NOT_HELD``: CLAIM_LOST.
    - ``ResumedBeforeStart``: a resume came first; the start runs again.
    - ``StopBeforeStartFailed``: OWNERSHIP_UNCONFIRMED.

    Logs the end. Pure but the log.
    """
    match record:
        case core.StoppedBeforeStart(outcome=outcome, move_outcome=move_outcome):
            pass
        case core.ResumedBeforeStart():
            logger.info(
                f'Task {task_id} stop refused: its workflow was resumed and the '
                f'claim is still startable'
            )
            return _WorkflowResumedBeforeStart()
        case core.StopBeforeStartFailed(message=message):
            return _start_unconfirmed(task_id, message)
        case _ as unreachable:
            assert_never(unreachable)
    match outcome:
        case 'EXPIRED':
            logger.info(
                f'Task {task_id} expired before actual execution start; marked EXPIRED.',
            )
            return StoppedBeforeRunning('TASK_EXPIRED')
        case 'ABANDONED' | 'CANCELLED':
            return StoppedBeforeRunning('WORKFLOW_STOPPED')
        case 'ORPHAN_CANCELLED':
            logger.info(
                f'Task {task_id} orphaned workflow task (no live workflow_task '
                f'linkage): the cancel ended {move_outcome}'
            )
            return StoppedBeforeRunning('ORPHAN_CANCELLED')
        case 'NODE_NOT_LINKED':
            logger.error(
                f'Failed to update workflow_tasks to RUNNING for task {task_id}'
            )
            return StoppedBeforeRunning('NODE_NOT_LINKED')
        case 'NOT_HELD':
            return _claim_lost(task_id)
        case _ as unreachable:
            assert_never(unreachable)


def _confirm_until_not_resumed(
    task_id: str,
    worker_id: str,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
    start_check: Callable[[], StartCheck] = start_always_allowed,
) -> StartedRunning | StoppedBeforeRunning | DeadlineBeforeStart | CoreStoppedBeforeStart:
    """Run the RUNNING transition again when a resume came before the stop.

    Returns ``StartedRunning`` with the time of the start write to start the
    task, or the stop that ends it
    (``CoreStoppedBeforeStart``: the core of the child stopped). A
    workflow resumed on every one of ``_PRE_START_CONFIRM_ATTEMPTS`` tries
    returns OWNERSHIP_UNCONFIRMED; the worker then releases the claim
    (``ReleaseUnconfirmedClaim``). ``DeadlineBeforeStart``: the deadline of
    the run passed before the start (``_confirm_ownership_and_set_running``).
    """
    for _ in range(_PRE_START_CONFIRM_ATTEMPTS):
        result = _confirm_ownership_and_set_running(
            task_id,
            worker_id,
            claimed_at=claimed_at,
            orphan_self_heal=orphan_self_heal,
            start_check=start_check,
        )
        match result:
            case _WorkflowResumedBeforeStart():
                continue
            case (
                StartedRunning()
                | StoppedBeforeRunning()
                | DeadlineBeforeStart()
                | CoreStoppedBeforeStart()
            ):
                return result
            case _ as unreachable:
                assert_never(unreachable)
    logger.warning(
        f'Task {task_id} workflow was resumed on each of '
        f'{_PRE_START_CONFIRM_ATTEMPTS} start attempts; the worker releases '
        f'the claim'
    )
    return StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


def _confirm_ownership_and_set_running(
    task_id: str,
    worker_id: str,
    *,
    claimed_at: datetime,
    orphan_self_heal: bool,
    start_check: Callable[[], StartCheck] = start_always_allowed,
) -> (
    StartedRunning
    | StoppedBeforeRunning
    | _WorkflowResumedBeforeStart
    | DeadlineBeforeStart
    | CoreStoppedBeforeStart
):
    """Set the row RUNNING. ``StartedRunning``: started, at the time that
    the start write returned.

    One ``horsies_start_attempt`` statement through the core handle of the
    child (``CoreChildHandle.start_attempt``), fenced to ``worker_id`` and the
    claim generation ``claimed_at``: a row requeued and claimed again since
    the dispatch of this child no longer matches, so the stale child stops
    with CLAIM_LOST instead of running the task twice. The function reads
    whether the task is a workflow task from the locked row. A
    miss runs its path (``_after_missed_start``) after the statement, with no
    lock held.

    ``start_check`` runs right before the start. ``DeadlineBeforeStart``
    returns with nothing sent. ``StartWithin`` sends the time left before the
    deadline of the run: when it has passed by the time the pool gives the
    connection, the core runs no statement and ``DeadlineBeforeStart``
    returns; the row stays CLAIMED.

    A failure record of the core, or a ``CoreError`` of the core boundary,
    gives OWNERSHIP_UNCONFIRMED, except the two refusals that no later call
    clears (``start_ended_by``): ``CoreStopped`` gives
    ``CoreStoppedBeforeStart``, and ``ForkedAfterCoreStart`` ends this
    process at once. The start and its miss path are one start
    attempt with the bound of the core (``START_ATTEMPT_BOUND``, 40 s): the
    wait for a connection is at most 30 s, and a statement gets a cancel
    request after 10 s or at the end of the attempt. A start that waits on a
    row lock past its bound ends with a failure record, and it does not
    commit after the bound.
    """
    handle = _get_child_core_handle()
    attempt_began = time.monotonic()
    match start_check():
        case StartAllowed():
            deadline_in_ms: int | None = None
        case StartWithin(milliseconds=milliseconds):
            deadline_in_ms = milliseconds
        case DeadlineBeforeStart() as passed:
            return passed
        case _ as unreachable:
            assert_never(unreachable)
    try:
        record = handle.start_attempt(
            uuid.UUID(task_id),
            worker_id=worker_id,
            claimed_at=claimed_at,
            worker_pid=os.getpid(),
            worker_hostname=socket.gethostname(),
            deadline_in_ms=deadline_in_ms,
        )
        match record:
            case core.AttemptStarted(started_at=started_at, workflow=workflow):
                return StartedRunning(started_at, run_workflow_of(workflow))
            case core.AttemptMissed(reason=reason):
                return _after_missed_start(
                    handle,
                    task_id,
                    worker_id,
                    reason,
                    claimed_at,
                    orphan_self_heal=orphan_self_heal,
                    attempt_began=attempt_began,
                )
            case core.DeadlineBeforeStart():
                return DeadlineBeforeStart()
            case core.StartFailed(message=message):
                return _start_unconfirmed(task_id, message)
            case _ as unreachable:
                assert_never(unreachable)
    except (CoreError, CoreFailure) as exc:
        return start_ended_by(task_id, exc)


def _after_missed_start(
    handle: CoreChildHandle,
    task_id: str,
    worker_id: str,
    reason: StartMissReason,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
    attempt_began: float,
) -> StoppedBeforeRunning | _WorkflowResumedBeforeStart:
    """The path of a start that missed, by its reason. One statement at most,
    which ends by the end of the start attempt that began at
    ``attempt_began`` (``attempt_elapsed_ms``).

    - ``WORKFLOW_PAUSED``, ``WORKFLOW_CANCELLED``, ``NODE_NOT_LINKED``: the
      miss of a workflow task. One ``horsies_workflow_task_stop_before_start``
      (``CoreChildHandle.stop_workflow_task_before_start``), fenced to the
      claim generation, which judges the task again under its own locks:
      the expiry, the abandon of a PAUSED workflow, the cancel of a
      CANCELLED one, a resume that came first, and with ``orphan_self_heal``
      the cancel of a task that no node links live
      (``stopped_before_start_of``).
    - ``DEADLINE_PASSED``: the expiry of the claim
      (``horsies_expire_owned_claim``); a refused expiry means the claim is
      gone (``expired_before_start_of``).
    - ``NOT_HELD``, ``LEASE_EXPIRED``: the claim is lost; nothing is sent.

    Raises:
        CoreError: the core of this process cannot run the call.
        CoreFailure: the expiry failed (the core's class of the cause): it
            wrote nothing, or it committed and its reply was lost.
        OutcomeDecodeError: a record outside the Python catalogs.

        ``_confirm_ownership_and_set_running`` ends the first two through
        ``start_ended_by``.
    """
    match reason:
        case 'WORKFLOW_PAUSED' | 'WORKFLOW_CANCELLED' | 'NODE_NOT_LINKED':
            logger.info(
                f'Blocking task {task_id} execution before start - the start '
                f'missed with {reason}'
            )
            return stopped_before_start_of(
                task_id,
                handle.stop_workflow_task_before_start(
                    uuid.UUID(task_id),
                    worker_id=worker_id,
                    claimed_at=claimed_at,
                    orphan_self_heal=orphan_self_heal,
                    attempt_elapsed_ms=attempt_elapsed_ms(attempt_began),
                ),
            )
        case 'DEADLINE_PASSED':
            return expired_before_start_of(
                task_id,
                task_row_end_of(
                    handle.expire_owned_claim(
                        uuid.UUID(task_id),
                        worker_id=worker_id,
                        attempt_elapsed_ms=attempt_elapsed_ms(attempt_began),
                    ),
                ),
            )
        case 'NOT_HELD' | 'LEASE_EXPIRED':
            return _claim_lost(task_id)
        case _ as unreachable:
            assert_never(unreachable)


def mark_run_finalizing(
    task_id: uuid.UUID,
    worker_id: str,
    claimed_at: datetime,
) -> MarkerEnd:
    """Write the finalizing marker of a run that set its row RUNNING and has
    ended, through the core handle of the child
    (``CoreChildHandle.mark_finalizing``), on the calling thread.

    One ``horsies_mark_finalizing`` statement, fenced on the claim
    generation (``worker_id``, ``claimed_at``): a stale attempt cannot
    refresh the marker of a later claim of the same task, which would delay
    that claim's stale-RUNNING recovery. The worker's runner heartbeat skips
    a row that carries the marker, so the beats of the attempt end here. The
    core bounds the write by ``FINALIZING_MARKER_TIMEOUT_S``, and the call
    returns within ``LEFT_WORK_MARKER_BOUND_S``. The marker is best effort:
    a skipped or failed write is logged only (``log_finalizing_record``),
    and so is a ``CoreError`` of the core boundary; each returns
    ``MarkerDone``. Two refusals are not:

    - ``CoreStopped`` returns ``MarkerMetStoppedCore``. The body finished,
      so the child reports its result, and then ends itself.
    - ``ForkedAfterCoreStart``: this process is a fork of the child. It
      ends at once (``end_child_after_core_refusal``) and writes no frame.
      Does not return.

    Raises:
        RuntimeError: the process built no core handle; a defect of the
            caller (``_get_child_core_handle``).
    """
    handle = _get_child_core_handle()
    try:
        record = handle.mark_finalizing(
            task_id,
            worker_id=worker_id,
            claimed_at=claimed_at,
        )
    except CoreError as exc:
        return marker_refused(task_id, exc)
    log_finalizing_record(record)
    return MarkerDone()


def marker_refused(task_id: uuid.UUID, refused: CoreError) -> MarkerEnd:
    """The end of a finalizing marker that raised ``refused``: a body
    finished before it. ``CoreStopped`` gives ``MarkerMetStoppedCore``; a
    fork of the child (``ForkedAfterCoreStart``) ends at once and does not
    return; any other error is logged and gives ``MarkerDone``."""
    match refused:
        case CoreStopped():
            return MarkerMetStoppedCore(refused)
        case ForkedAfterCoreStart():
            end_child_after_core_refusal(refused, f'After the body of task {task_id}')
        case _:
            logger.warning('Failed to mark task %s finalizing: %s', task_id, refused)
            return MarkerDone()


def encoded_body_end(
    task_name: str,
    task: Any,
    out: Result[Any, TaskError],
) -> EncodedResult | BodyInterrupted:
    """The end of a body whose task call returned ``out``.

    ``BodyInterrupted``: a refusal of the core of this child ended the
    body; its task error holds ``CoreStopped`` or ``ForkedAfterCoreStart``
    as its exception. Any other outcome is encoded for the frame
    (``_encode_task_outcome``), and an exception of the encode propagates.
    """
    match out:
        case Err(err_value=TaskError(exception=CoreStopped() | ForkedAfterCoreStart() as refusal)):
            return BodyInterrupted(refusal)
        case _:
            return _encode_task_outcome(task_name, task, out)


def set_running_or_end_child(
    task_id: str,
    started: StartedRunning,
    ran: ResultEnvelope | BodyInterrupted,
) -> SetRunning:
    """The return of a run whose body ran after its RUNNING update at
    ``started``.

    ``BodyInterrupted`` ends the child at once with no report
    (``end_child_after_core_refusal``): the body has no result, so the
    worker takes the crashed-child path (``WORKER_CRASHED``) and the retry
    policy of the task applies. Does not return then."""
    match ran:
        case ResultEnvelope():
            return SetRunning(ran, started.started_at)
        case BodyInterrupted(refusal=refusal):
            end_child_after_core_refusal(refusal, f'During the body of task {task_id}')
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


def _resolution_failure(task_name: str, message: str) -> EncodedResult:
    """The result of a task the worker cannot resolve: WORKER_RESOLUTION_ERROR."""
    task_error = TaskError(
        code=OperationalCode.WORKER_RESOLUTION_ERROR,
        message=message,
        data={'task_name': task_name},
    )
    return _encoded_error(task_error)


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
    called: Result[Any, TaskError] | Coroutine[Any, Any, Result[Any, TaskError]],
    task_loop: TaskLoop,
    map_exception: Callable[[BaseException], Result[Any, TaskError]],
    cancel: RunCancel,
) -> Result[Any, TaskError]:
    """The result of one call of a task wrapper.

    The call of a ``def`` task returns its result. The call of an
    ``async def`` task returns a coroutine, which runs on ``task_loop``; the
    child opened that loop at boot. Its asyncio task goes to ``cancel``
    first, so an operator cancel from the control thread reaches the body
    at its next ``await`` (``RunCancel.watch_body_task``). A
    ``CancelledError`` or a ``GeneratorExit`` that leaves that coroutine
    goes through ``map_exception``, the mapping of the task
    (``await_task_call_in_worker``). Other exceptions from that run
    propagate. The wrapper returns one of the two, so another value is a
    defect in the library: ``AssertionError``.
    """
    match called:
        case Ok() | Err():
            return called
        case Coroutine():
            return task_loop.run_watched(
                await_task_call_in_worker(called, map_exception),
                cancel,
            )
        case _:
            raise AssertionError(
                f'task wrapper of {task_name!r} returned {type(called).__name__}',
            )


def run_runtime(task_id: uuid.UUID, attempt: int, cancel: RunCancelCell) -> TaskRuntime:
    """The ``TaskRuntime`` of attempt ``attempt`` of task ``task_id``, from
    the run frame, whose ``cancel_state`` reads ``cancel``: the cell of the
    run, which the child requests at an operator cancel or a deadline. The
    core holds the attempt of a frame to 1 to ``MAX_ATTEMPT``. Pure."""
    return run_task_runtime(FencingToken(task_id=task_id, value=attempt), cancel)


def _prepare_task_call(
    task_name: str,
    input_json: str,
    place: RunPlace,
    runtime: TaskRuntime,
) -> _PreparedCall | EncodedResult:
    """Resolve a task from the registry and decode the arguments of its call.

    ``input_json`` is the stored input of the row, the body of the run
    frame: the keyword arguments as one JSON object. A task whose first
    parameter takes the ``TaskRuntime`` gets ``runtime`` as its one
    positional argument. Returns the task and
    its arguments, or the result that ends the run before user code: a task
    that cannot be resolved or cannot run in ``place``
    (``WORKER_RESOLUTION_ERROR``), or an input that does not decode
    (``WORKER_SERIALIZATION_ERROR``). Runs no user code but the pydantic
    validators of the declared types. An exception propagates to the
    caller, whose containment seam maps it. No I/O.
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
        )
    match _why_not_runnable(body_kind, place):
        case None:
            pass
        case not_runnable:
            logger.error(f'Failed to resolve task {task_name}: {not_runnable}')
            return _resolution_failure(
                task_name,
                f'Failed to resolve {task_name}: {not_runnable}',
            )

    # Deserialize the task input: the keyword arguments as one JSON object.
    # Strict-serde: the input holds no positional arguments; the one
    # positional argument is the runtime, which the worker passes.
    input_result = loads_json(input_json)
    if is_err(input_result):
        return _serialization_error_response(task_name, input_result.err_value)
    decoded_kwargs = input_result.ok_value
    if not isinstance(decoded_kwargs, dict):
        return _serialization_error_response(
            task_name,
            SerializationError(
                f'kwargs must be a JSON object, got '
                f'{type(decoded_kwargs).__name__}',
            ),
        )
    raw_kwargs: dict[str, Any] = decoded_kwargs

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

    # Deserialize injected outcomes from workflow args_from.
    # Strict-serde envelope shape (engine.py emit sites):
    #   {
    #     '__h_taskresult_envelope__': True,
    #     'source_task_name': '<upstream task name>',
    #     'inner': <encode_task_result(tr, source_ok_type)>,
    #   }
    # The worker derives the upstream OkT from `app.tasks[source_task_name]`
    # and decodes via `decode_task_result(inner, ok_type)`.
    for key, value in list(kwargs.items()):
        if not isinstance(value, dict):
            continue
        envelope = cast('dict[str, Any]', value)
        if envelope.get(_ENGINE_KEY_TASKRESULT_ENVELOPE) is not True:
            continue
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
            resolve_source_code_type,
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
        # The stored error of the upstream outcome holds a custom code that
        # the upstream task declares, or the input does not decode.
        match typed_outcome(
            decoded_result,
            resolve_source_code_type(
                app, source_task_name,
                sub_definition_key=(
                    source_definition_key
                    if isinstance(source_definition_key, str)
                    else None
                ),
            ),
        ):
            case Ok():
                pass
            case Err(err_value=undeclared):
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'kwarg {key!r}: the upstream outcome of '
                        f'{source_task_name!r} holds a custom code that '
                        f'the task does not declare: {undeclared.message}'
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)
        target_ok_type = declared_outcome_ok_type(underlying_fn, key)
        if target_ok_type is not None and target_ok_type != source_ok_type:
            match conform_to_target(decoded_result, target_ok_type):
                case Ok(ok_value=conformed):
                    decoded_result = conformed
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
        match typed_outcome(decoded_result, declared_outcome_code_type(underlying_fn, key)):
            case Ok(ok_value=typed):
                kwargs[key] = typed
            case Err(err_value=unknown):
                return _serialization_error_response(
                    task_name,
                    SerializationError(
                        f'kwarg {key!r} does not fit its declared type '
                        f'(source task {source_task_name!r}): {unknown.message}'
                    ),
                )
            case _ as unreachable:
                assert_never(unreachable)

    # Handle workflow injection payloads (workflow_context / workflow_meta).
    workflow_ctx_data = kwargs.pop(_ENGINE_KEY_WORKFLOW_CTX, None)
    workflow_meta_data = kwargs.pop(_ENGINE_KEY_WORKFLOW_META, None)
    if workflow_ctx_data is not None or workflow_meta_data is not None:
        import inspect

        underlying_fn = underlying_task_fn(task)
        sig = inspect.signature(underlying_fn)
        if workflow_ctx_data is not None and 'workflow_context' in sig.parameters:
            from horsies.core.models.workflow import (
                WorkflowContext,
                SubWorkflowSummary,
            )

            results_by_id_raw = workflow_ctx_data.get('results_by_id', {})
            task_name_by_id = workflow_ctx_data.get('task_name_by_id', {})
            definition_key_by_id = workflow_ctx_data.get(
                'definition_key_by_id', {},
            )
            results_by_id: dict[str, TaskOutcome[Any, TaskError[Any]]] = {}
            for node_id, envelope in results_by_id_raw.items():
                if envelope is None:
                    # Engine couldn't encode this entry (no
                    # registered task or no ok_type); surface as a
                    # sentinel TaskError so the context still has a
                    # typed value.
                    results_by_id[node_id] = Failed(
                        TaskError(
                            code=OperationalCode.RESULT_DESERIALIZATION_ERROR,
                            message=(
                                f'workflow_context result for node '
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
                    results_by_id[node_id] = Failed(
                        TaskError(
                            code=OperationalCode.RESULT_DESERIALIZATION_ERROR,
                            message=(
                                f'workflow_context missing task_name for '
                                f'node {node_id!r}; cannot decode'
                            ),
                            data={'node_id': node_id},
                        ),
                    )
                    continue
                from horsies.core.models.workflow.typing_utils import (
                    resolve_source_code_type,
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
                    results_by_id[node_id] = Failed(
                        TaskError(
                            code=OperationalCode.RESULT_DESERIALIZATION_ERROR,
                            message=(
                                f'workflow_context source '
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
                    results_by_id[node_id] = _context_entry(
                        node_id,
                        decode_task_result(envelope, source_ok_type),
                        resolve_source_code_type(
                            app, source_task_name,
                            sub_definition_key=(
                                _sub_def_key
                                if isinstance(_sub_def_key, str)
                                else None
                            ),
                        ),
                    )
                except (StrictJsonError, ValueValidationError) as exc:
                    results_by_id[node_id] = Failed(
                        TaskError(
                            code=OperationalCode.RESULT_DESERIALIZATION_ERROR,
                            message=(
                                f'decode_task_result failed for '
                                f'workflow_context node {node_id!r}: '
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

            # The engine payload keeps its stored keys (`task_index`,
            # `results_by_id`, `summaries_by_id`): SQL writes them. They map
            # here to the API names of the context.
            kwargs['workflow_context'] = WorkflowContext.from_serialized(
                workflow_id=workflow_ctx_data.get('workflow_id', ''),
                node_index=workflow_ctx_data.get('task_index', 0),
                task_name=workflow_ctx_data.get('task_name', ''),
                results_by_node_id=results_by_id,
                summaries_by_node_id=summaries_by_id,
            )
        # If task doesn't declare workflow_context, silently skip injection.

        if workflow_meta_data is not None and 'workflow_meta' in sig.parameters:
            from horsies.core.models.workflow import WorkflowMetadata

            if isinstance(workflow_meta_data, dict):
                meta_dict = cast(dict[str, Any], workflow_meta_data)
                task_index_raw: Any = meta_dict.get('task_index', 0)
                try:
                    task_index_value = int(task_index_raw)
                except (TypeError, ValueError):
                    task_index_value = 0

                # The stored key `task_index` (written by SQL) maps to the
                # API name `node_index`.
                kwargs['workflow_meta'] = WorkflowMetadata(
                    workflow_id=str(meta_dict.get('workflow_id', '')),
                    node_index=task_index_value,
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

    args: tuple[TaskRuntime, ...]
    match task_runtime_parameter(underlying_fn):
        case TakesRuntime():
            args = (runtime,)
        case NoRuntimeParameter():
            args = ()
        case _ as unreachable:
            assert_never(unreachable)
    return _PreparedCall(task=task, app=app, args=args, kwargs=kwargs)


def _context_entry(
    node_id: str,
    outcome: TaskOutcome[Any, TaskError],
    code_type: CustomCodeType,
) -> TaskOutcome[Any, TaskError[Any]]:
    """The entry of one node in a workflow context: its outcome with the
    custom codes of its task (``typed_outcome``). A stored code that the
    task's ``Enum`` does not name is a failed outcome with
    ``RESULT_DESERIALIZATION_ERROR``, as a node whose stored value does not
    read. Pure."""
    match typed_outcome(outcome, code_type):
        case Ok(ok_value=typed):
            return typed
        case Err(err_value=unknown):
            return Failed(TaskError(
                code=OperationalCode.RESULT_DESERIALIZATION_ERROR,
                message=f'workflow_context node {node_id!r}: {unknown.message}',
                data={'node_id': node_id, 'custom_code': unknown.custom_code},
            ))


def _task_exception_response(task_name: str, e: BaseException) -> EncodedResult:
    """The result of a task that raised: ``TASK_EXCEPTION``. Logs it."""
    logger.error(f'Task exception: {task_name}: {exception_text(e)}')
    # If the task raised an exception, we wrap it in a TaskError
    task_error = TaskError(
        code=OperationalCode.TASK_EXCEPTION,
        message=f'{type(e).__name__}: {exception_text(e)}',
        data={'task_name': task_name},
        exception=e,  # pydantic will accept and we flatten on serialization if needed elsewhere
    )
    return _encoded_error(task_error)


def _encode_task_outcome(
    task_name: str,
    task: Any,
    out: Result[Any, TaskError],
) -> EncodedResult:
    """Encode the result of a task call for the frame, against the declared
    ok type of the task, and read the ok value back with that type
    (``encode_returned_result``): the worker stores the envelope as it is. A
    value that does not fit, or that does not read back, ends the run with
    ``WORKER_SERIALIZATION_ERROR``. The outcome is that of ``out``, or of
    that failure. An exception propagates. No I/O."""
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
    return EncodedResult(ResultEnvelope(ser.ok_value), result_outcome_of(out))


def shared_attempt_subject(
    frame: core.SharedRunFrame,
    execution: Execution,
    workflow: RunWorkflow,
) -> AttemptSubject:
    """The attempt of the run of ``frame`` on a seat of a shared child of
    the class ``execution``, whose start gave the workflow node
    ``workflow``. Pure."""
    return AttemptSubject(
        task_id=str(frame.task_id),
        task_name=frame.task_name,
        queue_name=frame.queue_name,
        attempt=frame.attempt,
        worker_id=frame.worker_id,
        execution=execution,
        workflow=workflow,
        trace=run_trace_of(frame.trace),
    )


def _run_task_entry(  # pyright: ignore[reportUnusedFunction]
    task_name: str,
    input_json: str,
    task_id: uuid.UUID,
    master_worker_id: str,
    work: IsolatedWork,
    *,
    claimed_at: datetime,
    task_loop: TaskLoop,
    orphan_self_heal: bool,
    queue_name: str,
    attempt: int,
    trace: RunTrace,
) -> RunReturn:
    """``run_isolated_task`` with a cancel that no operator cancel reaches:
    a run in this process outside the frame loop of a child (the tests and
    the integration callers). It never returns ``RunCancelled``."""
    return run_isolated_task(
        task_name,
        input_json,
        task_id,
        master_worker_id,
        work,
        claimed_at=claimed_at,
        task_loop=task_loop,
        orphan_self_heal=orphan_self_heal,
        queue_name=queue_name,
        attempt=attempt,
        trace=trace,
        cancel=RunCancel(),
    )


def run_isolated_task(
    task_name: str,
    input_json: str,
    task_id: uuid.UUID,
    master_worker_id: str,
    work: IsolatedWork,
    *,
    claimed_at: datetime,
    task_loop: TaskLoop,
    orphan_self_heal: bool,
    queue_name: str,
    attempt: int,
    trace: RunTrace,
    cancel: RunCancel,
) -> RunReturn:
    """Run one task in an isolated child, up to its result.

    Returns ``SetRunning`` with the result envelope when the run set its row
    RUNNING, and ``StoppedBeforeRunning`` with its reason when the ownership
    confirm ended it first (a lost claim, an expiry, a stopped workflow, a
    failed RUNNING update). A body that a refusal of the core of the child
    ended does not return: the child ends at once
    (``set_running_or_end_child``).

    An ``async def`` task runs on ``task_loop``, the one loop of this child.
    A ``def`` task runs on this thread with no running event loop.

    The steps after the RUNNING update run inside the attempt span of the
    run, whose parent the send's carrier ``trace`` gives
    (``_run_isolated_after_running``). ``queue_name`` and ``attempt`` come
    with the run from the worker, and the workflow node from the start, for
    the span and the log records of the body.

    ``orphan_self_heal`` comes with the run from the worker (the app's
    ``RecoveryConfig.terminate_orphaned_workflow_tasks``): with it, the
    stop of a workflow task whose node is not linked cancels the task
    (ORPHAN_CANCELLED).

    A task whose first parameter takes the ``TaskRuntime`` gets the runtime
    of ``task_id`` and ``attempt`` (``run_runtime``), whose cancel state is
    the cell of ``cancel``.

    ``cancel`` is the operator cancel of the run, which the control thread
    of the child requests (``child_main``):

    - Before the body (the start write may still run): the body does not
      start, and the run returns ``RunCancelled`` with ``NOT_STARTED``. A
      start that ends the run another way returns that end.
    - While the body runs: a ``def`` body reads ``cancel_state``; an
      ``async def`` body gets ``CancelledError`` at its next ``await``.
      Whatever the body then returns is dropped: the run returns
      ``RunCancelled`` with ``RAN``.

    The envelope holds the task's success or its task error; the task's own
    outcome is in its ``ok`` and ``err`` members.

    This is the child's containment seam: the execution body is wrapped in
    ``except BaseException`` and folds everything into the result envelope.
    The ownership confirm returns its stop itself.

    Writes no finalizing marker: the child writes it when the run has
    ended, after the work that the task left on its task loop
    (``child_main.mark_child_task_finalizing``, ``mark_run_finalizing``).

    The child writes no runner heartbeat. The worker process writes them
    while this child holds the task; until the first one, the stale RUNNING
    rule judges the attempt from its ``started_at``.
    """
    task_id_text = str(task_id)
    logger.info(f'Starting task execution: {task_name} ({task_id_text})')

    # Mark as RUNNING in DB at the actual start of execution (child process).
    # CRITICAL: Include ownership check to prevent double-execution when claim
    # lease expires. The statement's guards also cover expiry (good_until) and
    # paused/cancelled workflows; its miss path diagnoses which one fired —
    # there is deliberately no separate pre-flight round trip.
    ownership_result = _confirm_until_not_resumed(
        task_id_text,
        master_worker_id,
        claimed_at,
        orphan_self_heal=orphan_self_heal,
    )
    match ownership_result:
        case StartedRunning() as started:
            pass
        case StoppedBeforeRunning() | CoreStoppedBeforeStart():
            # A cancel kept before the start is dropped: the start ended the
            # run, and that end is reported.
            _ = cancel.fix_end()
            return ownership_result
        case DeadlineBeforeStart():
            raise AssertionError(
                'an isolated child checks no deadline before its RUNNING update',
            )
        case _ as unreachable:
            assert_never(unreachable)
    match cancel.begin_body():
        case SkipTheBody():
            logger.info(
                f'Task {task_name} ({task_id_text}) was cancelled before its body '
                'started; the body does not run',
            )
            return _cancelled_end(cancel, started.started_at)
        case StartTheBody():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    ran = _run_isolated_after_running(
        input_json,
        work,
        task_loop,
        AttemptSubject(
            task_id=task_id_text,
            task_name=task_name,
            queue_name=queue_name,
            attempt=attempt,
            worker_id=master_worker_id,
            execution=Execution.ISOLATED,
            workflow=started.workflow,
            trace=trace,
        ),
        run_runtime(task_id, attempt, cancel.cell),
        cancel,
    )
    cancel.end_body()
    returned = set_running_or_end_child(task_id_text, started, ran)
    match cancel.fix_end():
        case EndCancelled(body=body):
            logger.info(
                f'Task {task_name} ({task_id_text}) was cancelled by an operator; '
                'what its body returned is dropped',
            )
            return RunCancelled(body=body, started_at=returned.started_at)
        case EndNotCancelled():
            return returned
        case _ as unreachable:
            assert_never(unreachable)


def _cancelled_end(cancel: RunCancel, started_at: datetime) -> RunCancelled:
    """The return of a run whose body an operator cancel skipped. The end is
    fixed here, so a later cancel is ignored."""
    match cancel.fix_end():
        case EndCancelled(body=body):
            return RunCancelled(body=body, started_at=started_at)
        case EndNotCancelled():
            raise AssertionError('a skipped body ended as not cancelled')
        case _ as unreachable:
            assert_never(unreachable)


def _run_isolated_after_running(
    input_json: str,
    work: IsolatedWork,
    task_loop: TaskLoop,
    attempt: AttemptSubject,
    runtime: TaskRuntime,
    cancel: RunCancel,
) -> ResultEnvelope | BodyInterrupted:
    """The steps of an isolated run after its RUNNING update, inside the
    span and the log context of ``attempt`` (``run_in_attempt_span``). The
    span is current on this thread, so a ``def`` body and the coroutine of
    an ``async def`` body, which the task loop runs in a copy of this
    context, see it. The deadline of an isolated run is the worker's
    (``isolated_body_end``): the span ends with what the steps returned, or
    with the cancel when an operator cancelled the run while the steps ran
    (``_isolated_order``)."""
    return run_in_attempt_span(
        attempt,
        lambda: _isolated_steps(
            attempt.task_name,
            input_json,
            attempt.task_id,
            work,
            task_loop,
            runtime,
            cancel,
        ),
        lambda: _isolated_order(cancel),
    )


def _isolated_order(cancel: RunCancel) -> BodyEndOrder:
    """The order of an isolated run when its steps return: an operator
    cancel came first, or the steps did (``isolated_body_end``)."""
    match cancel.operator_requested():
        case True:
            return OperatorCancelFirst()
        case False:
            return isolated_body_end()


def _isolated_steps(
    task_name: str,
    input_json: str,
    task_id: str,
    work: IsolatedWork,
    task_loop: TaskLoop,
    runtime: TaskRuntime,
    cancel: RunCancel,
) -> EncodedResult | BodyInterrupted:
    """The steps of an isolated run after its RUNNING update: the stored
    options refusal, the resolve, the call and the result encode. Any
    exception becomes ``TASK_EXCEPTION``. A body that a refusal of the core
    ended gives ``BodyInterrupted`` (``encoded_body_end``). A task that takes
    its ``TaskRuntime`` gets ``runtime``."""
    try:
        # Stored options that the worker cannot use fail the task before it
        # resolves the task, so no user code runs.
        match work:
            case OptionsUnreadable() | TimeoutValueInvalid() | CancelGraceInvalid():
                return _stored_timeout_refusal_response(task_name, work)
            case RunTheTask():
                pass
            case _ as unreachable:
                assert_never(unreachable)

        prepared = _prepare_task_call(
            task_name,
            input_json,
            OnIsolatedChild(task_loop),
            runtime,
        )
        match prepared:
            case _PreparedCall(task=task, args=args, kwargs=kwargs):
                pass
            case EncodedResult():
                return prepared
        out = _task_call_result(
            task_name,
            task(*args, **kwargs),
            task_loop,
            task._map_exception,
            cancel,
        )
        logger.info(f'Task execution completed: {task_name} ({task_id})')
        return encoded_body_end(task_name, task, out)

    except BaseException as e:
        return _task_exception_response(task_name, e)


