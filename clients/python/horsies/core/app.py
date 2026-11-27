# app/core/app.py
from collections.abc import Awaitable, Coroutine
from typing import (
    Literal,
    Optional,
    Callable,
    Concatenate,
    TypeVar,
    overload,
    TYPE_CHECKING,
    ParamSpec,
    Any,
    Union,
    Final,
    assert_never,
    cast,
    get_origin,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.health_reads import (
    BoundedTaskList,
    ExpiredTaskRow,
    MinResponses,
    MinResponsesFullWindow,
    PingTarget,
    PingTargetEveryWorker,
    StaleTaskRow,
    WorkerHistoryLimit,
    WorkerHistoryLimitAllRetained,
)
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import (
    Execution,
    TaskError,
    TaskOptions,
    RetryPolicy,
    OperationalCode,
)
from horsies.core.task_decorator import (
    create_task_wrapper,
    resolve_node_queue_and_priority,
)
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowSpec,
    WorkflowTerminalResults,
    OnError,
    SuccessPolicy,
    WorkflowHandle,
    WorkflowHandleInvalidWorkflowId,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import get_logger
from horsies.core.registry.tasks import TaskRegistry
from horsies.core.exception_mapper import (
    ExceptionMapper,
    validate_exception_mapper,
    validate_error_code_string,
)
from horsies.core.telemetry.send_telemetry import (
    SendTelemetry,
    current_context_trace,
    send_telemetry_of,
)
from horsies.core.history.rerun.operations import (
    RerunCommand,
    RerunEnqueuePolicy,
    RerunOutcome,
    rerun_with_trace,
    rerun_with_trace_async,
)
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncConnection
from horsies.core.declared_error_codes import (
    NO_ERROR_CODES,
    CodeRefusal,
    CodeSetFormRefused,
    CodesRefused,
    CodeTextRefused,
    RepeatedCodeRefused,
    ReservedNameRefused,
    RetryEntryNotAMember,
    RetryEntryRefusal,
    RetryMemberNotDeclared,
    check_retry_codes,
    parse_declared_error_codes,
    with_mapper_codes,
)
from horsies.core.errors import (
    ConfigurationError,
    HorsiesError,
    MultipleValidationErrors,
    TaskDefinitionError,
    HorsiesErrorCode,
    SourceLocation,
    ValidationReport,
    raise_collected,
    WorkflowValidationError,
)
import inspect
import logging
import os
import threading
import types
import importlib
import glob
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatch
from uuid import UUID
from horsies.core.utils.imports import import_by_path
from horsies.core.utils.loop_runner import (
    async_context_message,
    in_running_event_loop,
)
from horsies.core.process_role import (
    ProcessRole,
    process_role,
    wait_inside_task_message,
)
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import (
    decode_subworkflow_kwargs,
    fill_build_with_defaults,
)
from horsies.core.models.workflow.nesting_check import (
    NestingPath,
    NestingStep,
    StopWithFinding,
    StopWithoutFinding,
    WalkChild,
    child_start,
    decide_nesting,
)
from horsies.core.models.workflow.nodes import stored_kwargs
from horsies.core.codec.validation_errors import ValueValidationError, exception_text
from horsies.core.types.core_failure import call_refusal_or_raise
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schema_mode import AppRole, SchemaMode
from horsies._catalog.manifest import dump_producer_manifest
from horsies._core import (
    ConnectivityPassed,
    ConnectivityRefused,
    CoreError,
    CoreHandle,
    WorkerConnectionError,
)
from horsies.core.history.identity.keys import IDEMPOTENCY_WINDOW_DEFAULT
from horsies.core.manifest import to_producer_manifest
from horsies.core.worker_manifest import dump_worker_profiles
from horsies._catalog.events import AnyEvent
from horsies.core.event_handlers import (
    EventHandler,
    EventHandlerDecorator,
    HandlerChildStartHook,
    KnownEventType,
    check_event_handler,
    event_type_refusal_error,
    handler_refusal_error,
    known_event_type,
)

if TYPE_CHECKING:
    from horsies.core.task_decorator import (
        AsyncRuntimeTaskFunction,
        AsyncTaskFunction,
        CancelGraceRefusedDecorator,
        RuntimeTaskFunction,
        SharedAsyncTaskDecorator,
        SharedThreadTaskDecorator,
        TaskDecorator,
        TaskFunction,
    )
    from horsies.core.task_runtime import TaskRuntime
    from horsies.core.models.tasks import TaskInfo, TaskError
    from horsies.core.models.outcome import ReadResult
    from horsies.core.brokers.result_types import DatabaseResult, StoredResult
    from horsies.core.models.class_coverage import ExecutionClassCoverage, ExecutionClassReport
    from horsies.core.models.health import (
        DatabasePing,
        WorkerPong,
        WorkerStateSnapshot,
    )

P = ParamSpec('P')
T = TypeVar('T')
C = TypeVar('C', default=str)
"""The type of the custom codes of a task (`TaskError[C]`)."""
OutT = TypeVar('OutT')

CHECK_PHASE: Final = 'check'
"""The phase name of the report of ``Horsies.check()``."""

_E = TypeVar('_E', bound=HorsiesError)
_F = TypeVar('_F', bound=Callable[..., Any])

# Sync, zero-argument callable run once per worker child process.
ChildStartHook = Callable[[], None]

# Zero-argument coroutine function run once per shared async child, on the
# event loop of that child, before its first task.
SharedAsyncChildStartHook = Callable[[], Coroutine[Any, Any, None]]


@dataclass(frozen=True, slots=True)
class _HookReturnsCoroutine:
    """The call of the hook returns a coroutine: an ``async def`` function,
    an object whose ``__call__`` is ``async def``, or a function whose return
    annotation is an awaitable type."""


@dataclass(frozen=True, slots=True)
class _HookIsLambda:
    """A lambda, which can be neither ``async def`` nor annotated. The child
    awaits its value, and a value that is not awaitable fails the hook when
    the child starts."""


@dataclass(frozen=True, slots=True)
class _HookIsSync:
    """The call of the hook runs its body with no event loop."""


type _SharedAsyncHookKind = _HookReturnsCoroutine | _HookIsLambda | _HookIsSync


def _returns_awaitable(fn: Callable[..., object]) -> bool:
    """Whether the return annotation of ``fn`` is an awaitable type.

    An annotation that is missing or does not resolve counts as not
    awaitable. Does not call ``fn``.
    """
    try:
        annotation = inspect.signature(fn, eval_str=True).return_annotation
    except (NameError, TypeError, ValueError, SyntaxError, AttributeError):
        return False
    origin = get_origin(annotation) or annotation
    return isinstance(origin, type) and issubclass(origin, Awaitable)


def _shared_async_hook_kind(fn: object) -> _SharedAsyncHookKind:
    """Whether the call of ``fn`` returns a coroutine, read without a call.

    A call at registration could run a sync body in the process that imports
    the app, so only the function, its ``__call__`` and its return
    annotation are read.
    """
    if inspect.iscoroutinefunction(fn):
        return _HookReturnsCoroutine()
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        return _HookIsSync()
    match fn:
        case types.FunctionType() if fn.__name__ == '<lambda>':
            return _HookIsLambda()
        case types.FunctionType() | types.MethodType():
            target: Callable[..., object] = fn
        case _:
            target = type(fn).__call__
            if inspect.iscoroutinefunction(target):
                return _HookReturnsCoroutine()
    if _returns_awaitable(target):
        return _HookReturnsCoroutine()
    return _HookIsSync()


# Sentinel attribute name stamped on functions decorated with @app.check_workflow_builder
_BUILDER_ATTR = '__horsies_workflow_builder__'

# Attribute name for opt-out of undecorated builder detection
_NO_CHECK_ATTR = '__horsies_no_check__'


@dataclass(frozen=True, slots=True)
class _ChildBuilt:
    """A child spec that ``horsies check`` built from static kwargs."""

    spec: WorkflowSpec[Any]


@dataclass(frozen=True, slots=True)
class _ChildNotBuilt:
    """A child spec that ``horsies check`` did not build. The cause is in the
    check findings, in the log, or left to the run time."""


@dataclass(frozen=True, slots=True)
class _ConfigBrokerPool:
    """``get_broker()`` opens the pool that ``PostgresConfig`` sets:
    ``pool_size`` plus ``max_overflow``."""


@dataclass(frozen=True, slots=True)
class _TaskSendBrokerPool:
    """A worker child: ``get_broker()`` opens up to ``connections``
    connections, with no overflow: the ``task_send_pool`` of the kind of the
    child."""

    connections: int


@dataclass
class _WorkflowBuilderMeta:
    """Internal metadata for a registered workflow builder."""

    function: Callable[..., Any]
    cases: list[dict[str, Any]]
    location: SourceLocation | None


def _no_location(error: _E) -> _E:
    """Strip the auto-detected source location from a programmatic error.

    Used for errors created inside horsies internals where the auto-detected
    frame (e.g., CLI entry point) is misleading. The error message itself
    contains the relevant context (e.g., module path).
    """
    error.location = None
    return error


_ERROR_CODES_FORM_HELP: Final = (
    'pass an Enum class whose member values are the codes, or a tuple of '
    "UPPER_SNAKE_CASE strings, for example error_codes=('RATE_LIMITED',)"
)
"""The help of an `error_codes` value that is not an `Enum` class or a tuple."""


def _declared_error_codes_or_raise(
    value: object,
    *,
    task_label: str,
    location: SourceLocation | None,
) -> tuple[str, ...]:
    """The codes that the `error_codes` value `value` of a task declares,
    in the order of the declaration (`parse_declared_error_codes`).

    Raises:
        TaskDefinitionError: HRS-102 when `value` is neither an `Enum` class
            nor a tuple; one error per refused code otherwise (HRS-112,
            HRS-113, HRS-212), several in a `MultipleValidationErrors`
            (`raise_collected`).
    """
    match parse_declared_error_codes(value):
        case Ok(codes):
            return codes
        case Err(refused):
            pass
    match refused:
        case CodeSetFormRefused(given=given):
            raise TaskDefinitionError(
                message='invalid error_codes',
                code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                location=location,
                notes=[
                    f"task '{task_label}'",
                    f'error_codes is a {given}, not an Enum class or a tuple',
                ],
                help_text=_ERROR_CODES_FORM_HELP,
            )
        case CodesRefused(refusals=refusals):
            report = ValidationReport('task definition')
            for refusal in refusals:
                report.add(
                    _declared_code_error(
                        refusal,
                        task_label=task_label,
                        location=location,
                    ),
                )
            raise_collected(report)
            raise AssertionError(
                f"task '{task_label}': a refused error_codes value has no refusal",
            )
        case _ as unreachable:
            assert_never(unreachable)


def _declared_code_error(
    refusal: CodeRefusal,
    *,
    task_label: str,
    location: SourceLocation | None,
) -> TaskDefinitionError:
    """The definition error of one refused entry of `error_codes`."""
    match refusal:
        case CodeTextRefused(detail=detail):
            code = HorsiesErrorCode.TASK_INVALID_ERROR_CODE
            message = detail
            help_text = 'use UPPER_SNAKE_CASE error codes, for example RATE_LIMITED'
        case ReservedNameRefused(detail=detail):
            code = HorsiesErrorCode.CHECK_RESERVED_CODE_COLLISION
            message = detail
            help_text = (
                'use a different UPPER_SNAKE_CASE code that does not match a '
                'built-in code'
            )
        case RepeatedCodeRefused(code=repeated):
            code = HorsiesErrorCode.TASK_DUPLICATE_ERROR_CODE
            message = f"error_codes value '{repeated}' is given more than once"
            help_text = 'list each code once'
        case _ as unreachable:
            assert_never(unreachable)
    return TaskDefinitionError(
        message=message,
        code=code,
        location=location,
        notes=[f"task '{task_label}'"],
        help_text=help_text,
    )


_RETRY_CODES_HELP: Final = (
    'pass code members: a built-in code, for example '
    'OperationalCode.WORKER_CRASHED, or a member of the Enum class that '
    'the task declares with error_codes='
)
"""The help of a refused `retry_for` entry."""


def _retry_codes_or_raise(
    policy: RetryPolicy,
    code_enum: type[Enum] | None,
    *,
    task_name: str,
    location: SourceLocation | None,
) -> None:
    """Refuse each `retry_for` entry of `policy` that is not a code
    member of the task (`check_retry_codes`). `code_enum` is the `Enum`
    class of the task's `error_codes=`, or None for a tuple or no
    declaration.

    Raises:
        TaskDefinitionError: `TASK_INVALID_OPTIONS`, one per refused entry,
            several in a `MultipleValidationErrors` (`raise_collected`).
            Nothing is registered.
    """
    match check_retry_codes(policy.retry_for, code_enum):
        case Ok():
            return
        case Err(refused):
            pass
    report = ValidationReport('task definition')
    for refusal in refused.refusals:
        report.add(
            _retry_entry_error(
                refusal,
                code_enum,
                task_name=task_name,
                location=location,
            ),
        )
    raise_collected(report)
    raise AssertionError(
        f"task '{task_name}': a refused retry_for has no refusal",
    )


def _retry_entry_error(
    refusal: RetryEntryRefusal,
    code_enum: type[Enum] | None,
    *,
    task_name: str,
    location: SourceLocation | None,
) -> TaskDefinitionError:
    """The definition error of one refused `retry_for` entry: the task,
    the entry and the code class that the task declares. Pure."""
    match refusal:
        case RetryEntryNotAMember(index=index, entry=entry, given=given):
            message = f'retry_for[{index}] is not a code member'
            entry_note = f'retry_for[{index}]={entry} is a {given}, not a code member'
        case RetryMemberNotDeclared(index=index, member=member):
            message = f'retry_for[{index}] is not a code of the task'
            entry_note = (
                f'retry_for[{index}]={member} is neither a built-in code '
                'nor a member of the declared Enum class'
            )
        case _ as unreachable:
            assert_never(unreachable)
    match code_enum:
        case None:
            declared_note = 'the task declares no Enum class with error_codes='
        case declared:
            declared_note = f'the task declares error_codes={declared.__name__}'
    return TaskDefinitionError(
        message=message,
        code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
        location=location,
        notes=[f"task '{task_name}'", entry_note, declared_note],
        help_text=_RETRY_CODES_HELP,
    )


def _connectivity_failed(cause: str) -> ConfigurationError:
    """Python's refusal of a connectivity step other than the LISTEN probe,
    with its cause as the note. Pure."""
    return _no_location(
        ConfigurationError(
            message='broker connectivity check failed',
            code=HorsiesErrorCode.DATABASE_INVALID_URL,
            notes=[cause],
            help_text='check database_url in PostgresConfig',
        )
    )


_LIMIT_UNREADABLE_HELP: Final = (
    'the worker profiles were not compared with max_connections; check that '
    "the role can run current_setting('max_connections')"
)
"""The help of a worker connection check that the core could not run."""


def _worker_connection_error(refusal: WorkerConnectionError) -> ConfigurationError:
    """Python's error of one refusal of the core's worker connection check:
    its message, notes and help, and the Python code of its core code
    (``HRS-219``, ``CHECK_WORKER_CONNECTIONS``). Pure.

    Raises:
        ValueError: a core code that ``HorsiesErrorCode`` does not have, a defect of
            the pairing of the core and this package.
    """
    code = None if refusal.code is None else HorsiesErrorCode(refusal.code)
    return _no_location(
        ConfigurationError(
            message=refusal.message,
            code=code,
            notes=list(refusal.notes),
            help_text=refusal.help_text,
        )
    )


def _worker_connections_unchecked(cause: str) -> ConfigurationError:
    """Python's error of a worker connection check that the core could not
    run, with its cause as the note. Pure."""
    return _no_location(
        ConfigurationError(
            message='the server connection limit could not be read',
            code=HorsiesErrorCode.CHECK_WORKER_CONNECTIONS,
            notes=[cause],
            help_text=_LIMIT_UNREADABLE_HELP,
        )
    )


class Horsies:
    """
    Configuration-driven task management app.
    Requires an AppConfig instance for proper validation and type safety.
    """

    def __init__(
        self,
        config: AppConfig,
        *,
        schema_mode: SchemaMode = SchemaMode.MIGRATE_AND_VALIDATE,
    ):
        self.config = config
        # Propagated to the broker this app constructs. See
        # ``PostgresBroker.schema_mode``.
        self.schema_mode: SchemaMode = schema_mode
        self._broker: Optional['PostgresBroker'] = None
        self._broker_init_lock = threading.Lock()
        # The spans and producer metrics of the sends and workflow starts,
        # chosen once from config.tracing. NoTracing imports no
        # opentelemetry module.
        self._send_telemetry: SendTelemetry = send_telemetry_of(config.tracing)
        # The pool that get_broker() opens. A worker child sets its
        # task-send pool before it imports task modules.
        self._broker_pool: _ConfigBrokerPool | _TaskSendBrokerPool = _ConfigBrokerPool()
        self.tasks: 'TaskRegistry[TaskFunction[Any, Any, Any] | AsyncTaskFunction[Any, Any, Any] | RuntimeTaskFunction[Any, Any, Any] | AsyncRuntimeTaskFunction[Any, Any, Any]]' = (
            TaskRegistry()
        )
        self.logger: logging.Logger = get_logger('app')
        self._discovered_task_modules: list[str] = []
        self._workflow_builders: list[_WorkflowBuilderMeta] = []
        # When True, task sends/schedules are suppressed (used during import/discovery)
        self._suppress_sends: bool = False
        # What this process does: the CLI sets WORKER or SCHEDULER.
        self._role: AppRole = AppRole.PRODUCER
        # Hooks run once per worker child process, after task imports and
        # before the child opens its own connection pool.
        self._child_start_hooks: list[ChildStartHook] = []
        # Hooks run once per shared async child, on its event loop, before it
        # takes its first task.
        self._shared_async_child_start_hooks: list[SharedAsyncChildStartHook] = []
        # The event handlers, in registration order: the worker runs them in
        # its handler child.
        self._event_handlers: list[EventHandler] = []
        # Hooks run once in the handler child, before its first event.
        self._handler_child_start_hooks: list[HandlerChildStartHook] = []

        if os.getenv('HORSIES_CHILD_PROCESS') == '1':
            # Child processes re-run this on every recycle
            # (max_tasks_per_child); keep per-child init at DEBUG so a
            # recycling worker does not flood logs.
            self.logger.debug(
                f'horsies subprocess initialized with {config.queue_mode.name} mode (pid={os.getpid()})'
            )
        else:
            # Don't assert a role here — at init the process role is unknown
            # (the CLI calls set_role() after discovery for workers/schedulers).
            self.logger.info(
                f'horsies initialized with {config.queue_mode.name} mode'
            )

    def set_role(self, role: AppRole) -> None:
        """Set the role and log it. Called by CLI after discovery."""
        self._role = role
        # Worker children call set_role(AppRole.WORKER) on every recycle; keep
        # that at DEBUG. Parent/producer/scheduler set their role once -> INFO.
        if os.getenv('HORSIES_CHILD_PROCESS') == '1':
            self.logger.debug(f'horsies running as {role.value}')
        else:
            self.logger.info(f'horsies running as {role.value}')

    def set_task_send_pool(self, connections: int) -> None:
        """Make ``get_broker()`` open a pool of up to ``connections``
        connections with no overflow, in place of the pool that
        ``PostgresConfig`` sets.

        A worker child calls this at start, before it imports task modules,
        with the ``task_send_pool`` of its kind, so task code that sends
        opens that pool. ``connections`` is from 1 to
        ``MAX_CHILD_POOL_CONNECTIONS``: the child decoded it within that
        bound. A broker that ``get_broker()`` built before the call keeps
        its pool. Below 1 is a defect: ``AssertionError``.
        """
        assert connections >= 1, f'connections must be >= 1; got {connections}'
        self._broker_pool = _TaskSendBrokerPool(connections=connections)

    def on_child_start(
        self,
        hook: ChildStartHook,
    ) -> ChildStartHook:
        """Register a hook that runs once in every worker child process.

        Hooks run after task-module imports and before the child opens its
        own connection pool, in registration order. They fire on every child
        start, including replacement children, so bodies must be idempotent.
        A hook that raises or hangs terminates the child and stops the
        worker (fail-closed); catch inside the hook for best-effort work.

        The canonical body closes connections opened during import
        (`engine.dispose()`) and rebinds app-owned engines to a
        worker-appropriate pool.
        """
        if not callable(hook):
            raise ConfigurationError(
                message='on_child_start requires a callable',
                code=HorsiesErrorCode.CONFIG_INVALID_CHILD_HOOK,
                notes=[f'got {type(hook).__name__!r}'],
                help_text='register a zero-argument sync function',
            )
        if inspect.iscoroutinefunction(hook):
            raise ConfigurationError(
                message='on_child_start hooks must be sync',
                code=HorsiesErrorCode.CONFIG_INVALID_CHILD_HOOK,
                notes=[
                    f"hook '{getattr(hook, '__name__', repr(hook))}' is an async function",
                    'hooks run during child-process boot, before any event loop exists',
                ],
                help_text=(
                    'use a sync function; for async engines dispose via '
                    'async_engine.sync_engine.dispose(close=False)'
                ),
            )
        if all(existing is not hook for existing in self._child_start_hooks):
            self._child_start_hooks.append(hook)
        return hook

    def get_child_start_hooks(self) -> list[ChildStartHook]:
        """Snapshot of registered child start hooks, in order."""
        return list(self._child_start_hooks)

    def on_shared_async_child_start(
        self,
        hook: SharedAsyncChildStartHook,
    ) -> SharedAsyncChildStartHook:
        """Register a hook that runs once in every shared async child.

        The hook is an ``async def`` function with no arguments. It runs on
        the event loop of the child, before the child takes its first task,
        so a client that it builds (an HTTP client, an async database pool)
        is bound to the loop that every task of the child shares. Keep such
        a client in module state, and use it from the tasks.

        Hooks run after the ``on_child_start`` hooks and after the
        child opens its connection pool, in registration order, each for at
        most 10 seconds. A hook that raises or runs longer ends the child and
        stops the worker, as an ``on_child_start`` hook does. They
        run in every shared async child, also in a replacement child, so
        bodies must be idempotent. Work that a hook starts and leaves
        running, such as a background task of a client, belongs to no task.
        Isolated children do not run these hooks.

        Accepted, read without a call: an ``async def`` function, an object
        whose ``__call__`` is ``async def``, a function or an object whose
        ``__call__`` has an awaitable return annotation, and a lambda. A
        lambda cannot be checked here; when its value is not awaitable, the
        hook fails when the child starts. Any other callable is refused with
        ``HRS-214``.
        """
        if not callable(hook):
            raise ConfigurationError(
                message='on_shared_async_child_start requires a callable',
                code=HorsiesErrorCode.CONFIG_INVALID_CHILD_HOOK,
                notes=[f'got {type(hook).__name__!r}'],
                help_text='register a zero-argument async def function',
            )
        match _shared_async_hook_kind(hook):
            case _HookReturnsCoroutine() | _HookIsLambda():
                pass
            case _HookIsSync():
                raise ConfigurationError(
                    message='on_shared_async_child_start hooks must be async def',
                    code=HorsiesErrorCode.CONFIG_INVALID_CHILD_HOOK,
                    notes=[
                        f"hook '{getattr(hook, '__name__', repr(hook))}' is not an "
                        'async function, and its return annotation is not '
                        'awaitable',
                        'the hook runs on the event loop of the shared async child',
                    ],
                    help_text=(
                        'use an async def function; for setup with no event '
                        'loop, use on_child_start'
                    ),
                )
        if all(existing is not hook for existing in self._shared_async_child_start_hooks):
            self._shared_async_child_start_hooks.append(hook)
        return hook

    def get_shared_async_child_start_hooks(self) -> list[SharedAsyncChildStartHook]:
        """Snapshot of registered shared async child start hooks, in order."""
        return list(self._shared_async_child_start_hooks)

    def on_event[E: AnyEvent](self, event_type: type[E]) -> EventHandlerDecorator[E]:
        """Register a handler of the events of `event_type`, one event type
        of `horsies.telemetry`.

        The decorated function is a `def` or an `async def` function that
        takes one event of `event_type`. A worker of the app runs it in its
        handler child, a process apart from the worker and from the task
        children, on a thread and a bounded queue of its own; an `async def`
        handler runs on the event loop of that thread. A handler that raises
        is logged and counted, and a handler whose queue is full misses the
        events that do not fit; neither touches the tasks of the worker.
        The name of the handler is `module.qualname` of its function.

        Raises:
            ConfigurationError: `HRS-222` here, when `event_type` is not an
                event type. When the decorator is applied: `HRS-223` for a
                handler that cannot take the events, `HRS-224` for a name
                that an earlier handler has, `HRS-225` for a name with no
                module or over its bound, `HRS-226` past the most handlers
                (`horsies.core.event_handlers`). Nothing is registered then.
        """
        match known_event_type(event_type):
            case Ok(known):
                return EventHandlerDecorator[E](known, self._register_event_handler)
            case Err(refused):
                raise event_type_refusal_error(refused)

    def _register_event_handler(self, event_type: KnownEventType, handler: object) -> None:
        """Register `handler` for `event_type` after the checks of
        `check_event_handler`.

        Raises:
            ConfigurationError: the refusal (`handler_refusal_error`).
        """
        match check_event_handler(event_type, handler, self._event_handlers):
            case Ok(registered):
                self._event_handlers.append(registered)
            case Err(refused):
                raise handler_refusal_error(refused, handler=handler, event_type=event_type)

    def get_event_handlers(self) -> tuple[EventHandler, ...]:
        """The registered event handlers, in registration order."""
        return tuple(self._event_handlers)

    def on_handler_child_start(
        self,
        hook: HandlerChildStartHook,
    ) -> HandlerChildStartHook:
        """Register a hook that runs once in the handler child of a worker,
        after the app imports and before the child takes its first event.

        The hook is a sync function with no arguments. Hooks run in
        registration order, each for at most 10 seconds, and again in each
        new handler child, so bodies must be idempotent. Set the
        OpenTelemetry SDK of the handler child here, or build a client that
        the handlers share.

        A hook that raises or runs longer ends the handler child: it stays
        down until the worker restarts, and the worker runs its tasks on. The
        `on_child_start` hooks do not run in the handler child.

        Raises:
            ConfigurationError: `HRS-214` for a value that is not callable
                or for an `async def` function.
        """
        if not callable(hook):
            raise ConfigurationError(
                message='on_handler_child_start requires a callable',
                code=HorsiesErrorCode.CONFIG_INVALID_CHILD_HOOK,
                notes=[f'got {type(hook).__name__!r}'],
                help_text='register a zero-argument sync function',
            )
        if inspect.iscoroutinefunction(hook):
            raise ConfigurationError(
                message='on_handler_child_start hooks must be sync',
                code=HorsiesErrorCode.CONFIG_INVALID_CHILD_HOOK,
                notes=[
                    f"hook '{getattr(hook, '__name__', repr(hook))}' is an async function",
                    'the hook runs at the boot of the handler child, before any '
                    'event loop exists',
                ],
                help_text='use a sync function',
            )
        if all(existing is not hook for existing in self._handler_child_start_hooks):
            self._handler_child_start_hooks.append(hook)
        return hook

    def get_handler_child_start_hooks(self) -> list[HandlerChildStartHook]:
        """Snapshot of registered handler child start hooks, in order."""
        return list(self._handler_child_start_hooks)

    def get_valid_queue_names(self) -> list[str]:
        """Get list of valid queue names based on configuration"""
        if self.config.queue_mode == QueueMode.DEFAULT:
            return ['default']
        else:  # CUSTOM mode
            return [queue.name for queue in (self.config.custom_queues or [])]

    def validate_queue(self, queue: Optional[str]) -> str:
        """The queue of a task or a send, checked against the queue mode."""
        if self.config.queue_mode == QueueMode.DEFAULT:
            if queue is not None:
                raise ConfigurationError(
                    message='cannot specify queue_name in DEFAULT mode',
                    code=HorsiesErrorCode.CONFIG_INVALID_QUEUE_MODE,
                    notes=[
                        f"queue_name='{queue}' was specified",
                        'but app is configured with QueueMode.DEFAULT',
                    ],
                    help_text='either remove queue_name or switch to QueueMode.CUSTOM',
                )
            return 'default'
        else:  # CUSTOM mode
            if queue is None:
                raise ConfigurationError(
                    message='queue_name is required in CUSTOM mode',
                    code=HorsiesErrorCode.CONFIG_INVALID_QUEUE_MODE,
                    notes=['app is configured with QueueMode.CUSTOM'],
                    help_text='specify queue_name from configured queues',
                )
            valid_queues = self.get_valid_queue_names()
            if queue not in valid_queues:
                raise ConfigurationError(
                    message=f"invalid queue_name '{queue}'",
                    code=HorsiesErrorCode.TASK_INVALID_QUEUE,
                    notes=[f'valid queues: {valid_queues}'],
                    help_text='use one of the configured queue names',
                )
            return queue

    # The runtime forms come first: a function whose first parameter takes a
    # TaskRuntime also fits the plain forms, with the runtime in P.
    @overload
    def task(  # pyright: ignore[reportOverlappingOverload]
        self,
        name: str,
        function: Callable[
            Concatenate['TaskRuntime', P],
            Coroutine[Any, Any, 'Result[T, TaskError[C]]'],
        ],
    ) -> 'AsyncRuntimeTaskFunction[P, T, C]': ...

    @overload
    def task(  # pyright: ignore[reportOverlappingOverload]
        self,
        name: str,
        function: Callable[Concatenate['TaskRuntime', P], 'Result[T, TaskError[C]]'],
    ) -> 'RuntimeTaskFunction[P, T, C]': ...

    @overload
    def task(
        self,
        name: str,
        function: Callable[P, Coroutine[Any, Any, 'Result[T, TaskError[C]]']],
    ) -> 'AsyncTaskFunction[P, T, C]': ...

    @overload
    def task(
        self, name: str, function: Callable[P, 'Result[T, TaskError[C]]']
    ) -> 'TaskFunction[P, T, C]': ...

    # The last overload also accepts a literal SHARED_ASYNC with
    # cancel_grace_ms, and its return type accepts more function kinds than
    # this one. A type checker picks this overload first for the literal.
    @overload
    def task(  # pyright: ignore[reportOverlappingOverload]
        self,
        name: str,
        *,
        execution: Literal[Execution.SHARED_ASYNC],
        timeout_ms: int,
        cancel_grace_ms: Optional[int] = None,
        queue: Optional[str] = None,
        retry_policy: Optional['RetryPolicy'] = None,
        exception_mapper: Optional['ExceptionMapper'] = None,
        default_unhandled_error_code: Optional[str] = None,
        error_codes: type[Enum] | tuple[str, ...] = (),
    ) -> 'SharedAsyncTaskDecorator': ...

    # The last overload also accepts a literal SHARED_THREAD, and its return
    # type accepts more function kinds than this one. A type checker picks
    # this overload first for the literal, so the overlap only reaches a
    # class known at run time, which the decorator checks at run time.
    @overload
    def task(  # pyright: ignore[reportOverlappingOverload]
        self,
        name: str,
        *,
        execution: Literal[Execution.SHARED_THREAD],
        timeout_ms: int,
        queue: Optional[str] = None,
        retry_policy: Optional['RetryPolicy'] = None,
        exception_mapper: Optional['ExceptionMapper'] = None,
        default_unhandled_error_code: Optional[str] = None,
        error_codes: type[Enum] | tuple[str, ...] = (),
    ) -> 'SharedThreadTaskDecorator': ...

    # cancel_grace_ms with a literal SHARED_THREAD: the returned decorator
    # takes no function, so the decoration is refused. The last overload also
    # accepts these arguments, for a class known at run time; a type checker
    # picks this overload first for the literal.
    @overload
    def task(  # pyright: ignore[reportOverlappingOverload]
        self,
        name: str,
        *,
        execution: Literal[Execution.SHARED_THREAD],
        timeout_ms: int,
        cancel_grace_ms: int,
        queue: Optional[str] = None,
        retry_policy: Optional['RetryPolicy'] = None,
        exception_mapper: Optional['ExceptionMapper'] = None,
        default_unhandled_error_code: Optional[str] = None,
        error_codes: type[Enum] | tuple[str, ...] = (),
    ) -> 'CancelGraceRefusedDecorator': ...

    # ISOLATED, the default class. cancel_grace_ms is the hard stop after an
    # operator cancel: the worker kills the child that long after the cancel
    # when the run has not ended. Without it, nothing is killed.
    @overload
    def task(
        self,
        name: str,
        *,
        queue: Optional[str] = None,
        retry_policy: Optional['RetryPolicy'] = None,
        exception_mapper: Optional['ExceptionMapper'] = None,
        default_unhandled_error_code: Optional[str] = None,
        error_codes: type[Enum] | tuple[str, ...] = (),
        timeout_ms: Optional[int] = None,
        cancel_grace_ms: Optional[int] = None,
        execution: Literal[Execution.ISOLATED] = Execution.ISOLATED,
    ) -> 'TaskDecorator': ...

    # A class known only at run time: timeout_ms is required, because a
    # shared class needs it. cancel_grace_ms is accepted, because the class
    # may be SHARED_ASYNC or ISOLATED. The decorator checks the rules at run
    # time.
    @overload
    def task(
        self,
        name: str,
        *,
        execution: Execution,
        timeout_ms: int,
        cancel_grace_ms: Optional[int] = None,
        queue: Optional[str] = None,
        retry_policy: Optional['RetryPolicy'] = None,
        exception_mapper: Optional['ExceptionMapper'] = None,
        default_unhandled_error_code: Optional[str] = None,
        error_codes: type[Enum] | tuple[str, ...] = (),
    ) -> 'TaskDecorator': ...

    def task(
        self,
        name: str,
        function: Optional[
            Callable[..., 'Result[Any, TaskError[Any]]']
            | Callable[..., Coroutine[Any, Any, 'Result[Any, TaskError[Any]]']]
        ] = None,
        **task_options_kwargs: Any,
    ) -> Union[
        'TaskFunction[Any, Any, Any]',
        'AsyncTaskFunction[Any, Any, Any]',
        'RuntimeTaskFunction[Any, Any, Any]',
        'AsyncRuntimeTaskFunction[Any, Any, Any]',
        'TaskDecorator',
        'SharedAsyncTaskDecorator',
        'SharedThreadTaskDecorator',
        'CancelGraceRefusedDecorator',
    ]:
        """
        Decorator to register a task with this app.
        Task options are validated against TaskOptions model and app configuration.

        A `def` function gives a `TaskFunction`. An `async def` function
        gives an `AsyncTaskFunction`, whose direct call returns a coroutine.
        A function whose first parameter is `runtime: TaskRuntime` gives a
        `RuntimeTaskFunction` or an `AsyncRuntimeTaskFunction`: the worker
        passes the runtime of each run, and the sends do not take it.

        `execution` (an `Execution`, default `ISOLATED`) declares how a
        worker runs the task. `SHARED_ASYNC` needs an `async def` function
        and `timeout_ms`; `SHARED_THREAD` needs a `def` function and
        `timeout_ms`. `cancel_grace_ms` is for `SHARED_ASYNC` (the time to
        stop after a cancel, default 1000 ms) and `ISOLATED` (the hard stop
        after an operator cancel: the worker kills the child when the run has
        not ended that long after the cancel; with none, nothing is killed).
        `SHARED_THREAD` takes none: a thread cannot be stopped.

        `error_codes` (an `Enum` class, whose member values are the codes, or
        a tuple of code texts; default `()`) declares the task's own error
        codes: the values of its `error_code` metric label besides the
        built-in codes and `undeclared` (`horsies.core.declared_error_codes`).
        The codes that the task's `exception_mapper` and
        `AppConfig.exception_mapper` map to are declared with no second
        declaration (`with_mapper_codes`); the app's mapper is read when the
        task is defined.

        Each `retry_for` entry of `retry_policy` is a code member: a
        member of a built-in code enum, or a member of the `Enum` class of
        `error_codes=` (`check_retry_codes`). A plain `str`, or a member of
        another `Enum` class, is `TASK_INVALID_OPTIONS`.

        Raises:
            TaskDefinitionError: empty task name, invalid options, mapper,
                or error code; or an execution declaration that breaks a
                rule (HRS-107 to HRS-111, one code per rule); or an
                `error_codes` value that is not an `Enum` class or a tuple
                (HRS-102), one error per refused code (HRS-112, HRS-113,
                HRS-212), several in a `MultipleValidationErrors`.
            ConfigurationError: queue name not in app config.
            RegistryError: duplicate task name from a different source.
                All three fire at import/definition time by design — a task
                that cannot be registered must crash the import, not surface
                at first send.
        """

        @overload
        def decorator(  # pyright: ignore[reportOverlappingOverload]
            function: Callable[
                Concatenate['TaskRuntime', P],
                Coroutine[Any, Any, 'Result[T, TaskError[C]]'],
            ],
        ) -> 'AsyncRuntimeTaskFunction[P, T, C]': ...

        @overload
        def decorator(  # pyright: ignore[reportOverlappingOverload]
            function: Callable[Concatenate['TaskRuntime', P], 'Result[T, TaskError[C]]'],
        ) -> 'RuntimeTaskFunction[P, T, C]': ...

        @overload
        def decorator(
            function: Callable[P, Coroutine[Any, Any, 'Result[T, TaskError[C]]']],
        ) -> 'AsyncTaskFunction[P, T, C]': ...

        @overload
        def decorator(
            function: Callable[P, 'Result[T, TaskError[C]]'],
        ) -> 'TaskFunction[P, T, C]': ...

        def decorator(
            function: Callable[P, 'Result[T, TaskError[C]]']
            | Callable[P, Coroutine[Any, Any, 'Result[T, TaskError[C]]']],
        ) -> 'TaskFunction[P, T, C] | AsyncTaskFunction[P, T, C] | RuntimeTaskFunction[Any, T, C] | AsyncRuntimeTaskFunction[Any, T, C]':
            function_location = SourceLocation.from_function(function)

            if name == '':
                raise TaskDefinitionError(
                    message='task name is empty',
                    code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                    location=function_location,
                    notes=[
                        f"task '{function.__name__}'",
                        "app.task('') received an empty task name",
                    ],
                    help_text='pass a task name that is not empty, for example the function name',
                )

            if 'retry_for' in task_options_kwargs:
                raise TaskDefinitionError(
                    message='invalid task options',
                    code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                    location=function_location,
                    notes=[
                        f"task '{function.__name__}'",
                        "top-level 'retry_for' is not a task option; the codes are a field of RetryPolicy",
                    ],
                    help_text=(
                        'move retry triggers into RetryPolicy, e.g. '
                        'RetryPolicy.fixed([...], retry_for=[...])'
                    ),
                )

            # Pop mapper-related kwargs (not part of TaskOptions, not serialized)
            exception_mapper: ExceptionMapper | None = task_options_kwargs.pop(
                'exception_mapper',
                None,
            )
            default_unhandled_error_code: str | None = task_options_kwargs.pop(
                'default_unhandled_error_code',
                None,
            )

            error_codes_value: object = task_options_kwargs.pop('error_codes', NO_ERROR_CODES)
            declared_error_codes = _declared_error_codes_or_raise(
                error_codes_value,
                task_label=function.__name__,
                location=function_location,
            )
            code_enum: type[Enum] | None = (
                error_codes_value
                if isinstance(error_codes_value, type) and issubclass(error_codes_value, Enum)
                else None
            )

            good_until_value = task_options_kwargs.pop('good_until', None)
            if good_until_value is not None:
                raise TaskDefinitionError(
                    message='good_until must be set when sending a task',
                    code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                    location=function_location,
                    notes=[
                        f"task '{function.__name__}'",
                        '@app.task(..., good_until=...) is evaluated when the task is defined',
                        'relative deadlines declared there become stale and can expire future sends',
                    ],
                    help_text=(
                        'use task.with_options(good_until=deadline).send(...) '
                        'or task.node(good_until=deadline)(...) for workflow nodes'
                    ),
                )

            # Validate per-task mapper if provided
            if exception_mapper is not None:
                mapper_errors = validate_exception_mapper(
                    exception_mapper,
                )
                if mapper_errors:
                    raise TaskDefinitionError(
                        message='invalid exception_mapper',
                        code=HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        location=function_location,
                        notes=[f"task '{function.__name__}'", *mapper_errors],
                        help_text='keys must be BaseException subclasses, values must be UPPER_SNAKE_CASE error codes',
                    )

            # Validate per-task default_unhandled_error_code if provided
            if default_unhandled_error_code is not None:
                code_error = validate_error_code_string(
                    default_unhandled_error_code,
                    field_name='default_unhandled_error_code',
                )
                if code_error is not None:
                    raise TaskDefinitionError(
                        message='invalid default_unhandled_error_code',
                        code=HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        location=function_location,
                        notes=[f"task '{function.__name__}'", code_error],
                        help_text='use UPPER_SNAKE_CASE error codes',
                    )

            # Validate and create TaskOptions - this enforces pydantic validation
            try:
                task_options = TaskOptions(task_name=name, **task_options_kwargs)
            except Exception as e:
                raise TaskDefinitionError(
                    message=f'invalid task options',
                    code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                    location=function_location,
                    notes=[f"task '{function.__name__}'", exception_text(e)],
                    help_text='check task decorator arguments',
                )

            # The retry codes are checked against the codes of the task, which
            # only the decorator knows.
            match task_options.retry_policy:
                case None:
                    pass
                case retry_policy:
                    _retry_codes_or_raise(
                        retry_policy,
                        code_enum,
                        task_name=name,
                        location=function_location,
                    )

            # VALIDATION AT DEFINITION TIME
            # Validate the queue against app configuration
            try:
                self.validate_queue(task_options.queue)
            except ConfigurationError:
                raise  # Re-raise with original formatting

            # Create wrapper that uses this app's configuration
            task_function = create_task_wrapper(
                function,
                self,
                name,
                task_options,
                exception_mapper=exception_mapper,
                default_unhandled_error_code=default_unhandled_error_code,
                error_codes=with_mapper_codes(
                    declared_error_codes,
                    exception_mapper,
                    self.config.exception_mapper,
                ),
                code_enum=code_enum,
            )

            # Register task with this app, passing source for duplicate detection
            # Normalize path with realpath to handle symlinks and relative paths
            source_str = (
                f'{os.path.realpath(function_location.file)}:{function_location.line}'
                if function_location
                else None
            )
            self.tasks.register(task_function, name=name, source=source_str)

            return task_function

        if function is None:
            # Called with arguments: @app.task("name", queue="custom")
            return decorator
        else:
            # Called without arguments: @app.task
            return decorator(function)

    def check(self, *, live: bool = False) -> ValidationReport:
        """Orchestrate phased validation and return the report of the phases.

        Phase 1: Config — already validated at construction (implicit pass).
        Phase 2: Task module imports — import each module, collect errors.
        Phase 3: Workflow validation — happens during imports (WorkflowSpec construction).
        Phase 3.1: WorkflowDefinition class validation — imported definitions must
            declare definition_key.
        Phase 3.2: Workflow builder execution — run registered builders under send suppression.
        Phase 3.3: Undecorated builder detection — fail-closed on missing decorators.
        Phase 3.5: Policy safety checks — runtime retry/mapping validation.
        Phase 3.6: Schedule validation — skipped for the worker role, which
            never acts on schedules. Validates timezone, task registration,
            queue, and the kwargs wire contract for each enabled schedule.
        Phase 4 (if live): Broker connectivity — the core's connectivity
            check.
        Phase 4.1 (if live): Worker connections — the core's check of each
            worker profile against the server connection limit. Nothing is
            read with no profile or through a transaction pooler.
        Both live phases run on one core handle of their own.

        Args:
            live: If True, also run the live phases (Phase 4 and 4.1).

        Returns:
            A ``ValidationReport`` of the phase ``check``: its ``refusals``
            are the errors of the first phase that refused. An empty report
            means all validations passed.

        Raises:
            Exception: each phase contains its expected failures into the
                returned list (import errors, builder raises, bad cases).
                An escape — a horsies bug, or exotic module-level
                machinery the collectors don't guard (e.g. a module
                __getattr__ raising during member scans) — propagates to
                the CLI command (traceback, non-zero exit) rather than
                being masked as a validation error.
            ForkedAfterCoreStart, CoreStopped: a live phase called the
                core in a process that was forked without exec after the
                core started, or whose core stopped
                (``call_refusal_or_raise``).
        """
        report = ValidationReport(CHECK_PHASE)
        for error in self._check_phases(live=live):
            report.add(error)
        return report

    def _check_phases(self, *, live: bool) -> list[HorsiesError]:
        """The errors of the first phase of ``check`` that refused, or none."""
        all_errors: list[HorsiesError] = []

        # Phase 2: task module imports (also triggers Phase 3 workflow validation)
        all_errors.extend(self._check_task_imports())
        if all_errors:
            return all_errors

        # Phase 3.1: validate imported WorkflowDefinition subclasses
        all_errors.extend(self._check_workflow_definitions())
        if all_errors:
            return all_errors

        # Phase 3.2: execute registered workflow builders under send suppression
        all_errors.extend(self._check_workflow_builders())
        if all_errors:
            return all_errors

        # Phase 3.3: detect undecorated top-level WorkflowSpec-returning functions
        all_errors.extend(self._check_undecorated_builders())
        if all_errors:
            return all_errors

        # Phase 3.5: policy safety checks that require imported task metadata.
        all_errors.extend(self._check_runtime_policy_safety())
        if all_errors:
            return all_errors

        # Phase 3.6: validate configured schedules (skipped for workers).
        all_errors.extend(self._check_schedules())
        if all_errors:
            return all_errors

        # Phase 4 and 4.1 (optional): connectivity, then the worker profiles
        # against the server connection limit
        if live:
            all_errors.extend(self._check_live())

        return all_errors

    def _check_schedules(self) -> list[HorsiesError]:
        """Validate configured schedules, unless running as a worker.

        Workers never enqueue scheduled tasks, so schedule validation would be
        wasted compute; the scheduler boot path and the standalone ``check``
        command (producer role) still validate. Shares
        ``collect_schedule_errors`` with ``Scheduler._validate_schedules``.
        """
        match self._role:
            case AppRole.WORKER:
                return []
            case AppRole.PRODUCER | AppRole.SCHEDULER:
                pass
        if self.config.schedule is None:
            return []
        from horsies.core.scheduler.validation import collect_schedule_errors

        return collect_schedule_errors(self, self.config.schedule)

    def _check_task_imports(self) -> list[HorsiesError]:
        """Import task modules and collect any errors."""
        errors: list[HorsiesError] = []
        modules = self._discovered_task_modules
        prev_suppress = self._suppress_sends
        self.suppress_sends(True)
        try:
            for module_path in modules:
                try:
                    if module_path.endswith('.py') or os.path.sep in module_path:
                        abs_path = os.path.realpath(module_path)
                        if not os.path.exists(abs_path):
                            errors.append(
                                _no_location(
                                    ConfigurationError(
                                        message=f'task module not found: {module_path}',
                                        code=HorsiesErrorCode.CLI_INVALID_ARGS,
                                        notes=[f'resolved path: {abs_path}'],
                                        help_text=(
                                            'remove it from app.discover_tasks([...]) or fix the path; \n'
                                            'if using globs, run app.expand_module_globs([...]) first'
                                        ),
                                    )
                                )
                            )
                            continue
                        import_by_path(abs_path)
                    else:
                        importlib.import_module(module_path)
                except MultipleValidationErrors as exc:
                    errors.extend(exc.report.refusals)
                except HorsiesError as exc:
                    errors.append(exc)
                except (ModuleNotFoundError, ImportError) as exc:
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=f'failed to import module: {module_path}',
                                code=HorsiesErrorCode.CLI_INVALID_ARGS,
                                notes=[str(exc)],
                                help_text=(
                                    'ensure the module is importable; '
                                    'for file paths include .py and a valid path, '
                                    'for dotted paths verify PYTHONPATH or run from the project root'
                                ),
                            )
                        )
                    )
                except Exception as exc:
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=f'error while importing module: {module_path}',
                                code=HorsiesErrorCode.MODULE_IMPORT_ERROR,
                                notes=[f'{type(exc).__name__}: {exc}'],
                                help_text=(
                                    'the module was found but raised an error during import;\n'
                                    'check the module-level code for bugs'
                                ),
                            )
                        )
                    )
        finally:
            self.suppress_sends(prev_suppress)
        return errors

    def _check_workflow_definitions(self) -> list[HorsiesError]:
        """Validate imported WorkflowDefinition subclasses.

        The actual class lookup happens inside
        ``_collect_module_workflow_definitions``; this method only walks
        the discovered modules and forwards validation calls.
        """
        errors: list[HorsiesError] = []
        seen: set[type[Any]] = set()

        for module_path in self._discovered_task_modules:
            module = self._resolve_imported_module(module_path)
            if module is None:
                continue

            modules_to_check = [module]
            if hasattr(module, '__path__'):
                prefix = module.__name__ + '.'
                for name, mod in sys.modules.items():
                    if name.startswith(prefix) and mod is not None:
                        modules_to_check.append(mod)

            for mod in modules_to_check:
                for workflow_def in self._collect_module_workflow_definitions(mod):
                    if workflow_def in seen:
                        continue
                    seen.add(workflow_def)
                    try:
                        workflow_def._require_definition_key()
                    except HorsiesError as exc:
                        errors.append(exc)
                    errors.extend(self._check_subworkflow_targets(workflow_def))

        return errors

    @staticmethod
    def _check_subworkflow_targets(workflow_def: type[Any]) -> list[HorsiesError]:
        """Hold each `SubWorkflowNode` of a definition to the task signature rules.

        A definition builds its spec when it starts, so this runs the check at
        boot, before a run can reach it.
        """
        from horsies.core.codec.signature_check import (
            SignatureValidationError,
            check_build_with_signature,
        )
        from horsies.core.models.workflow.nodes import SubWorkflowNode

        errors: list[HorsiesError] = []
        for name, value in vars(workflow_def).items():
            if not isinstance(value, SubWorkflowNode):
                continue
            try:
                check_build_with_signature(value.workflow_definition, workflow_name=name)
            except SignatureValidationError as refusal:
                errors.append(
                    WorkflowValidationError(
                        message='subworkflow build_with cannot cross the wire',
                        code=HorsiesErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE,
                        notes=[
                            f"node '{name}'",
                            refusal.detail.headline,
                            refusal.detail.reason,
                        ],
                        help_text=refusal.detail.help_text,
                    ),
                )
        return errors

    def _check_runtime_policy_safety(self) -> list[HorsiesError]:
        """Validate runtime retry/mapping policies after task imports."""
        from horsies.core.models.tasks import BUILTIN_CODE_REGISTRY

        errors: list[HorsiesError] = []

        # Re-validate global settings so post-construction mutations fail startup.
        mapper_errors = validate_exception_mapper(self.config.exception_mapper)
        for msg in mapper_errors:
            errors.append(
                _no_location(
                    ConfigurationError(
                        message=msg,
                        code=HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        notes=['app config'],
                        help_text='exception_mapper must be a mapping with exception-class keys and UPPER_SNAKE_CASE code values',
                    )
                )
            )

        # Check exception_mapper values for reserved built-in code collisions.
        if isinstance(self.config.exception_mapper, Mapping):
            for exc_cls, code_str in self.config.exception_mapper.items():
                if isinstance(code_str, str) and code_str in BUILTIN_CODE_REGISTRY:
                    builtin = BUILTIN_CODE_REGISTRY[code_str]
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=(
                                    f"exception_mapper value '{code_str}' collides with "
                                    f'reserved built-in {type(builtin).__name__}.{builtin.name}'
                                ),
                                code=HorsiesErrorCode.CHECK_RESERVED_CODE_COLLISION,
                                notes=['app config', f'key: {exc_cls.__name__}'],
                                help_text='use a different UPPER_SNAKE_CASE code that does not match a built-in code',
                            )
                        )
                    )

        global_default_error = validate_error_code_string(
            self.config.default_unhandled_error_code,
            field_name='default_unhandled_error_code',
        )
        if global_default_error is not None:
            errors.append(
                _no_location(
                    ConfigurationError(
                        message=global_default_error,
                        code=HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        notes=['app config'],
                        help_text='use UPPER_SNAKE_CASE error codes',
                    )
                )
            )

        # Check global default_unhandled_error_code for reserved collision.
        # The library default 'UNHANDLED_EXCEPTION' is intentionally a built-in code
        # and should not be flagged as a collision.
        global_default = self.config.default_unhandled_error_code
        if (
            isinstance(global_default, str)
            and global_default in BUILTIN_CODE_REGISTRY
            and global_default != OperationalCode.UNHANDLED_EXCEPTION.value
        ):
            builtin = BUILTIN_CODE_REGISTRY[global_default]
            errors.append(
                _no_location(
                    ConfigurationError(
                        message=(
                            f"default_unhandled_error_code '{global_default}' collides with "
                            f'reserved built-in {type(builtin).__name__}.{builtin.name}'
                        ),
                        code=HorsiesErrorCode.CHECK_RESERVED_CODE_COLLISION,
                        notes=['app config'],
                        help_text='use a different UPPER_SNAKE_CASE code that does not match a built-in code',
                    )
                )
            )

        for task_name, task in self.tasks.items():
            task_default = getattr(task, 'default_unhandled_error_code', None)
            if task_default is not None:
                code_error = validate_error_code_string(
                    task_default,
                    field_name='default_unhandled_error_code',
                )
                if code_error is not None:
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=code_error,
                                code=HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                                notes=[f"task '{task_name}'"],
                                help_text='use UPPER_SNAKE_CASE error codes',
                            )
                        )
                    )
                elif (
                    isinstance(task_default, str)
                    and task_default in BUILTIN_CODE_REGISTRY
                    and task_default != OperationalCode.UNHANDLED_EXCEPTION.value
                ):
                    builtin = BUILTIN_CODE_REGISTRY[task_default]
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=(
                                    f"default_unhandled_error_code '{task_default}' collides with "
                                    f'reserved built-in {type(builtin).__name__}.{builtin.name}'
                                ),
                                code=HorsiesErrorCode.CHECK_RESERVED_CODE_COLLISION,
                                notes=[f"task '{task_name}'"],
                                help_text='use a different UPPER_SNAKE_CASE code that does not match a built-in code',
                            )
                        )
                    )

            # Check per-task exception_mapper for reserved collisions. Stored as
            # MappingProxyType on the task wrapper, so match Mapping, not dict.
            task_mapper = getattr(task, 'exception_mapper', None)
            if isinstance(task_mapper, Mapping):
                for exc_cls, code_str in task_mapper.items():
                    if isinstance(code_str, str) and code_str in BUILTIN_CODE_REGISTRY:
                        builtin = BUILTIN_CODE_REGISTRY[code_str]
                        errors.append(
                            _no_location(
                                ConfigurationError(
                                    message=(
                                        f"exception_mapper value '{code_str}' collides with "
                                        f'reserved built-in {type(builtin).__name__}.{builtin.name}'
                                    ),
                                    code=HorsiesErrorCode.CHECK_RESERVED_CODE_COLLISION,
                                    notes=[
                                        f"task '{task_name}'",
                                        f'key: {exc_cls.__name__}'
                                        if isinstance(exc_cls, type)
                                        else f'key: {exc_cls!r}',
                                    ],
                                    help_text='use a different UPPER_SNAKE_CASE code that does not match a built-in code',
                                )
                            )
                        )

            task_options_json = task.task_options_json
            if task_options_json is None:
                continue

            options_result = loads_json(task_options_json)
            if is_err(options_result):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='invalid task_options_json',
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[
                                f"task '{task_name}'",
                                str(options_result.err_value),
                            ],
                            help_text='task options metadata must be valid JSON',
                        )
                    )
                )
                continue
            options = options_result.ok_value
            if not isinstance(options, dict):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='task_options_json must be an object',
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[
                                f"task '{task_name}'",
                                f'task_options_json={options!r}',
                            ],
                            help_text='task options metadata must be a JSON object',
                        )
                    )
                )
                continue
            retry_policy_raw = options.get('retry_policy')
            if retry_policy_raw is None:
                continue
            if not isinstance(retry_policy_raw, dict):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='retry_policy must be an object',
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[
                                f"task '{task_name}'",
                                f'retry_policy={retry_policy_raw!r}',
                            ],
                            help_text='retry policy metadata must be a JSON object',
                        )
                    )
                )
                continue
            retry_for = retry_policy_raw.get('retry_for')
            if retry_for is None:
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='retry_policy.retry_for is required',
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'"],
                            help_text='configure retry triggers in RetryPolicy(retry_for=[...])',
                        )
                    )
                )
                continue
            if not isinstance(retry_for, list):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='retry_for must be a list',
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[
                                f"task '{task_name}'",
                                f'retry_for={retry_for!r}',
                            ],
                            help_text='use a list of UPPER_SNAKE_CASE error codes',
                        )
                    )
                )
                continue
            if len(retry_for) == 0:
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='retry_for must not be empty',
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'"],
                            help_text='provide at least one UPPER_SNAKE_CASE error code',
                        )
                    )
                )
                continue

            has_entry_errors = False
            for idx, entry in enumerate(retry_for):
                if not isinstance(entry, str):
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message='retry_for entries must be strings',
                                code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                                notes=[
                                    f"task '{task_name}'",
                                    f'retry_for[{idx}]={entry!r}',
                                ],
                                help_text='use explicit UPPER_SNAKE_CASE error codes',
                            )
                        )
                    )
                    has_entry_errors = True
                    continue
                code_error = validate_error_code_string(
                    entry,
                    field_name='retry_for',
                )
                if code_error is None:
                    continue
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message=code_error,
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'"],
                            help_text='replace exception names with explicit error codes',
                        )
                    )
                )
                has_entry_errors = True
            if has_entry_errors:
                continue

            try:
                RetryPolicy.model_validate(retry_policy_raw)
            except Exception as exc:
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='invalid retry_policy metadata',
                            code=HorsiesErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'", exception_text(exc)],
                            help_text='ensure retry_policy matches RetryPolicy schema',
                        )
                    )
                )

        return errors

    def _check_live(self) -> list[HorsiesError]:
        """Run the live phases on one core handle of their own: the
        connectivity check, then, when it passed, the worker connections.

        The handle is built from ``config.database`` with the schema mode
        ``OBSERVE_ONLY`` (no schema step) and closed at the end; the app's
        broker is not built. On a thread that runs an event loop the
        blocking core calls cannot run, so the check reports
        ``CHECK_LIVE_ASYNC_CONTEXT`` and connects to nothing.

        Returns the errors of the first phase that refused, or none. Nothing
        is written but the LISTEN probe's one NOTIFY.
        """
        if in_running_event_loop():
            return [
                _no_location(
                    ConfigurationError(
                        message='live broker check cannot run inside a running event loop',
                        code=HorsiesErrorCode.CHECK_LIVE_ASYNC_CONTEXT,
                        notes=[
                            'check(live=True) was called on a thread that runs an '
                            'event loop; the connectivity check did not run',
                        ],
                        help_text=(
                            'call check(live=True) from sync code, or from async '
                            'code use: await asyncio.to_thread(app.check, live=True)'
                        ),
                    )
                )
            ]
        handle = CoreHandle(
            dump_producer_manifest(
                to_producer_manifest(
                    self.config.database,
                    schema='OBSERVE_ONLY',
                    idempotency_reservation_window=IDEMPOTENCY_WINDOW_DEFAULT,
                ),
            ),
            pool_profile='PRODUCER',
        )
        try:
            connectivity_errors = self._check_broker_connectivity(handle)
            if connectivity_errors:
                return connectivity_errors
            return self._check_worker_connections(handle)
        finally:
            self._close_check_handle(handle)

    def _check_broker_connectivity(self, handle: CoreHandle) -> list[HorsiesError]:
        """Check the database paths with the core's connectivity check, the
        rule of both clients (``CoreHandle.check_connectivity``).

        In order, stopping at the first failure: ``SELECT 1`` on
        ``database_url``; ``SELECT 1`` on the session database URL when it
        differs; in PgBouncer transaction mode, a LISTEN probe on the session
        URL that must receive a NOTIFY within 2 s. Runs on ``handle``, the
        live check's own handle.

        Returns at most one error, ``DATABASE_INVALID_URL``: "broker listener
        capability check failed" for the probe, "broker connectivity check
        failed" for every other step and for a core that cannot run the
        call. Nothing is written but the probe's one NOTIFY. Raises
        ``ForkedAfterCoreStart`` and ``CoreStopped``.
        """
        try:
            outcome = handle.check_connectivity()
        except CoreError as exc:
            return [_connectivity_failed(str(call_refusal_or_raise(exc)))]
        match outcome:
            case ConnectivityPassed():
                return []
            case ConnectivityRefused(step='LISTENER', message=message):
                return [
                    _no_location(
                        ConfigurationError(
                            message='broker listener capability check failed',
                            code=HorsiesErrorCode.DATABASE_INVALID_URL,
                            notes=[
                                'the session_database URL appears to be transaction-pooled; '
                                'LISTEN notification was not delivered',
                                message,
                            ],
                            help_text=(
                                'set session_database to a separate '
                                'direct/session-capable Postgres URL'
                            ),
                        )
                    )
                ]
            case ConnectivityRefused(message=message):
                return [_connectivity_failed(message)]
            case _ as unreachable:
                assert_never(unreachable)

    def _close_check_handle(self, handle: CoreHandle) -> None:
        """Close the core handle of a live check. A core that cannot run the
        close is logged: the check's answer stands, and the process holds the
        handle's pools until it ends. Raises ``ForkedAfterCoreStart`` and
        ``CoreStopped``."""
        try:
            _ = handle.close()
        except CoreError as exc:
            self.logger.warning(
                'the core handle of the live check did not close: %s',
                call_refusal_or_raise(exc),
            )

    def _check_worker_connections(self, handle: CoreHandle) -> list[HorsiesError]:
        """Compare each worker profile with the server connection limit, with
        the core's check, the rule of both clients
        (``CoreHandle.check_worker_connections``).

        One worker of a profile opens its parent pool, its session pool when
        the session URL differs, its listener, its runner heartbeat
        connection and the pools of its children. A profile whose worker
        opens more than the server has usable slots is an error here; each
        error also states what task code that sends adds. The worker itself
        only warns at boot. With no profiles, or through a transaction
        pooler, the core reads nothing. Runs on ``handle``, after the
        connectivity phase passed on it.

        Returns one ``CHECK_WORKER_CONNECTIONS`` error per profile that does
        not fit, in name order, or one when the limit was not read or the
        core could not run the call. Writes nothing. Raises
        ``ForkedAfterCoreStart`` and ``CoreStopped``.
        """
        profiles_json = dump_worker_profiles(self.config.worker_profiles)
        try:
            refusals = handle.check_worker_connections(profiles_json)
        except CoreError as exc:
            return [_worker_connections_unchecked(str(call_refusal_or_raise(exc)))]
        return [_worker_connection_error(refusal) for refusal in refusals]

    def registered_task_names(self) -> list[str]:
        """The names of the tasks registered with this app. Pure."""
        return list(self.tasks.keys_list())

    def registered_task_codes(self) -> dict[str, tuple[str, ...]]:
        """The error codes that each registered task declares
        (`error_codes`), by task name, for every task of the app: `()`
        for a task that declares none. A rerun checks the stored retry list
        of its source against them (`horsies.rerun_task`). Pure."""
        return {
            name: self.tasks[name].error_codes
            for name in self.tasks.keys_list()
        }

    def declared_error_codes(self) -> dict[str, tuple[str, ...]]:
        """The error codes that each task declares (`error_codes=`), by task
        name in name order, for each task that declares at least one. Pure."""
        return {
            name: self.tasks[name].error_codes
            for name in sorted(self.tasks.keys_list())
            if self.tasks[name].error_codes
        }

    def send_telemetry(self) -> SendTelemetry:
        """The telemetry of the task sends and workflow starts of this app,
        chosen from `config.tracing` when the app was built:
        `NoSendTelemetry`, or `OtelSendTelemetry` with its recorder
        (`horsies.core.telemetry.send_telemetry`). Pure."""
        return self._send_telemetry

    async def rerun_task_in_async(
        self,
        connection: AsyncConnection,
        command: RerunCommand,
        policy: RerunEnqueuePolicy,
    ) -> RerunOutcome:
        """``horsies.rerun_task_in_async`` with the tracing setting of this
        app: the same statements on ``connection``, in its transaction, with
        the same outcomes and errors. It does not commit. The stored retry list of the
        source is checked against ``registered_task_codes()``.

        With ``OtelTracing`` the new task stores the carrier that the
        configured propagator injects for the caller's current context, with
        the trace id of its span, so the attempts of the new task are
        children of the caller's span. The rerun opens no span and records no
        metric. With no valid span in the current context, or with
        ``NoTracing``, the new task stores no carrier; ``NoTracing`` imports
        no ``opentelemetry`` module. A failure of the user's OpenTelemetry
        SDK never changes the outcome of the rerun.
        """
        return await rerun_with_trace_async(
            connection,
            command,
            policy,
            current_context_trace(self._send_telemetry),
            registered_task_codes=self.registered_task_codes(),
        )

    def rerun_task_in(
        self,
        connection: Connection,
        command: RerunCommand,
        policy: RerunEnqueuePolicy,
    ) -> RerunOutcome:
        """``rerun_task_in_async`` on a blocking SQLAlchemy ``Connection`` of the
        caller: the same statements in its transaction, the same trace
        carrier, outcomes and errors. It does not commit."""
        return rerun_with_trace(
            connection,
            command,
            policy,
            current_context_trace(self._send_telemetry),
            registered_task_codes=self.registered_task_codes(),
        )

    def get_broker(self) -> 'PostgresBroker':
        """Get the configured PostgreSQL broker for this app.

        In a worker child the broker opens the task-send pool of the kind of
        the child (``set_task_send_pool``). In every other process, the
        worker process included, it opens the pool that ``PostgresConfig``
        sets. The worker process opens its own pool, ``WorkerProfile.pool``,
        on a broker of its own. The broker's schema install ends with the
        startup coverage pass of ``config.retention``: the history leaves of
        every class that the app's sends can stamp.

        Raises:
            HorsiesError: broker construction failed (DATABASE_INIT_FAILED;
                non-HorsiesError causes are wrapped). Construction happens
                once per process — callers treat this as fail-fast
                configuration error, not a retryable condition.
        """
        try:
            if self._broker is None:
                # Serialized: two threads racing the None check would each
                # construct a PostgresBroker (one engine/pool leaks).
                with self._broker_init_lock:
                    if self._broker is None:
                        broker_config = self._broker_config()
                        broker = PostgresBroker(
                            broker_config,
                            schema_mode=self.schema_mode,
                            retention=self.config.retention,
                        )
                        broker.app = self  # app ref for subworkflow support
                        self._broker = broker
            return self._broker
        except HorsiesError:
            raise
        except Exception as e:
            raise HorsiesError(
                message=f'Failed to initialize broker: {e}',
                code=HorsiesErrorCode.DATABASE_INIT_FAILED,
                notes=[str(e)],
                help_text='check database_url in PostgresConfig; the note holds the cause',
            ) from e

    def workflow_handle(
        self, workflow_id: str,
    ) -> Result[WorkflowHandle[Any], WorkflowHandleInvalidWorkflowId]:
        """The handle of the workflow whose id is ``workflow_id``, read
        through this app's broker (``get_broker``), or why ``workflow_id`` is
        not a workflow id. The handle reads nothing until one of its methods
        runs, and a workflow that does not exist is ``WORKFLOW_NOT_FOUND`` of
        that method.

        ``workflow_id`` is any spelling of a UUID that ``uuid.UUID`` accepts
        (upper case, braces, ``urn:uuid:``, no hyphens); the handle holds the
        UUID, whose text is the canonical form. Text that ``uuid.UUID``
        refuses is ``Err(WorkflowHandleInvalidWorkflowId)`` with its ``ValueError``;
        the same text gives the same error, and no broker is built. The
        handle has no output type (``output_type`` is ``None``).

        Raises:
            HorsiesError: broker construction failed (``get_broker``,
                DATABASE_INIT_FAILED).
        """
        try:
            workflow_key = UUID(workflow_id)
        except ValueError as exc:
            return Err(WorkflowHandleInvalidWorkflowId(
                message=f'workflow id {workflow_id!r} is not a UUID: {exc}',
                workflow_id_text=workflow_id,
                cause=exc,
            ))
        handle: WorkflowHandle[Any] = WorkflowHandle(
            workflow_id=workflow_key, broker=self.get_broker(),
        )
        return Ok(handle)

    def _broker_config(self) -> PostgresConfig:
        """The config of the broker that ``get_broker()`` builds."""
        match self._broker_pool:
            case _ConfigBrokerPool():
                return self.config.database
            case _TaskSendBrokerPool(connections=connections):
                return self.config.database.with_exact_pool(connections)
            case _ as unreachable:
                assert_never(unreachable)

    # ─── Strict-serde phase 6b: app-level result APIs ──────────────

    async def get_raw_result_async(
        self,
        task_id: UUID,
        wait_ms: int | None = None,
    ) -> 'DatabaseResult[dict[str, Any] | None]':
        """Fetch the raw stored result envelope without typed decode.

        Thin wrapper around ``broker.get_raw_result_record_async``.
        Returns the raw envelope dict (or ``None`` for absent/non-
        terminal rows). Cross-process monitoring / debugging callers
        use this to inspect payloads without importing user task
        modules.

        The first check is the wait limit: a wait_ms that is not None or
        an int from 0 to MAX_TASK_TIMEOUT_MS returns
        ``Err(DatabaseInvalidInput)``. Then, in a process that runs task
        bodies, it returns ``Err(DatabaseWaitInsideTask)``. Neither does I/O.
        """
        from horsies.core.types.result import Err, Ok, is_err

        refused_wait = self._raw_wait_refused('get_raw_result_async', task_id, wait_ms)
        if refused_wait is not None:
            return refused_wait
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return self._wait_inside_task_err('get_raw_result_async', task_id)
        broker = self.get_broker()
        record_result = await broker.get_raw_result_record_async(
            task_id, wait_ms,
        )
        if is_err(record_result):
            return Err(record_result.err_value)
        record = record_result.ok_value
        if record is None:
            return Ok(None)
        return Ok(record.raw_result)

    def get_raw_result(
        self,
        task_id: UUID,
        wait_ms: int | None = None,
    ) -> 'DatabaseResult[dict[str, Any] | None]':
        """Synchronous version of ``get_raw_result_async``.

        The checks before any I/O come in this order: the wait limit
        (``Err(DatabaseInvalidInput)``), a process that runs task bodies
        (``Err(DatabaseWaitInsideTask)``), and a thread that runs an event loop
        (``Err(ASYNC_CONTEXT)``).
        """
        from horsies.core.types.result import Err, Ok, is_err

        refused_wait = self._raw_wait_refused('get_raw_result', task_id, wait_ms)
        if refused_wait is not None:
            return refused_wait
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return self._wait_inside_task_err('get_raw_result', task_id)
        if in_running_event_loop():
            return self._async_context_err('get_raw_result')
        broker = self.get_broker()
        record_result = broker.get_raw_result_record(task_id, wait_ms)
        if is_err(record_result):
            return Err(record_result.err_value)
        record = record_result.ok_value
        if record is None:
            return Ok(None)
        return Ok(record.raw_result)

    async def get_result_async(
        self,
        task_id: UUID,
        wait_ms: int | None = None,
    ) -> 'ReadResult[Any, TaskError[Any]]':
        """Fetch and typed-decode a task's stored result.

        Returns ``Ok(Succeeded(value))`` or ``Ok(Failed(task_error))`` for a
        task that ended: a success materializes the typed value using the
        local task catalog's ``task_ok_type``; a failed task decodes without
        ``ok_type`` (TaskError has a fixed schema), so a cross-process caller
        inspecting failed tasks can decode without importing user code.

        Returns ``Err(read_error)`` when the read gives no outcome:
        ``ReadNoTypeAvailable`` when the ok slot is populated and ``task_name``
        isn't in the local registry; ``ReadTaskNotFound``, ``ReadWaitTimeout``,
        ``ReadResultNotAvailable``, ``ReadResultDeserializationError`` and
        ``ReadDatabaseError`` as ``TaskHandle.get``.

        The first check is the wait limit: a wait_ms that is not None or
        an int from 0 to MAX_TASK_TIMEOUT_MS returns ``Err(ReadInvalidInput)``.
        Then, in a process that runs task bodies, it returns
        ``Err(ReadWaitInsideTask)``. Neither does I/O.
        """
        refused_wait = self._read_wait_refused('get_result_async', task_id, wait_ms)
        if refused_wait is not None:
            return refused_wait
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return self._wait_inside_task_read('get_result_async', task_id)
        record_result = await self.get_broker().get_raw_result_record_async(
            task_id, wait_ms,
        )
        return self._task_result_of_record(task_id, wait_ms, record_result)

    def _task_result_of_record(
        self,
        task_id: UUID,
        wait_ms: int | None,
        record_result: 'DatabaseResult[StoredResult | None]',
    ) -> 'ReadResult[Any, TaskError[Any]]':
        """The answer of ``get_result`` and ``get_result_async`` for the
        broker's result read (``read_of_record``). Pure apart from the lookup
        of the task's ``task_ok_type`` in the registry.
        """
        from horsies.core.models.tasks import CustomCodeType, TextCodes
        from horsies.core.task_read import read_of_record

        ok_type: object | None = None
        code_type: CustomCodeType = TextCodes()
        match record_result:
            case Ok(ok_value=record) if record is not None:
                task = self.tasks.get(record.task_name)
                if task is not None:
                    ok_type = getattr(task, 'task_ok_type', None)
                    code_type = task.task_code_type
            case _:
                pass
        return read_of_record(task_id, record_result, ok_type, wait_ms, code_type)

    def get_result(
        self,
        task_id: UUID,
        wait_ms: int | None = None,
    ) -> 'ReadResult[Any, TaskError[Any]]':
        """Synchronous version of ``get_result_async``: the broker's blocking
        result read (``PostgresBroker.get_raw_result_record``) on the calling
        thread, with the GIL released and signals checked every 100 ms.

        The checks before any I/O come in this order: the wait limit
        (``Err(ReadInvalidInput)``), a process that runs task bodies
        (``Err(ReadWaitInsideTask)``), and a thread that runs an event loop
        (``Err(ReadAsyncContext)``). A raised signal handler (Ctrl+C) ends the
        wait with its exception; nothing changed.
        """
        from horsies.core.models.outcome import ReadAsyncContext

        refused_wait = self._read_wait_refused('get_result', task_id, wait_ms)
        if refused_wait is not None:
            return refused_wait
        if process_role() is ProcessRole.RUNS_TASK_BODIES:
            return self._wait_inside_task_read('get_result', task_id)
        if in_running_event_loop():
            return Err(ReadAsyncContext(message=async_context_message(
                'app.get_result()', 'app.get_result_async(...)',
            )))
        record_result = self.get_broker().get_raw_result_record(task_id, wait_ms)
        return self._task_result_of_record(task_id, wait_ms, record_result)

    async def get_task_info_async(
        self,
        task_id: UUID,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> 'DatabaseResult[TaskInfo | None]':
        """Fetch task metadata, including typed-decoded result when possible.

        Wraps ``broker.get_task_info_async`` and adds the
        ``decoded_result`` / ``result_decoded`` fields when the task is
        registered locally and decode succeeds.
        """
        info_result = await self.get_broker().get_task_info_async(
            task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
            include_attempts=include_attempts,
        )
        return self._decoded_task_info(task_id, include_result, info_result)

    def _decoded_task_info(
        self,
        task_id: UUID,
        include_result: bool,
        info_result: 'DatabaseResult[TaskInfo | None]',
    ) -> 'DatabaseResult[TaskInfo | None]':
        """The answer of ``get_task_info`` and ``get_task_info_async`` for the
        broker's task info read: the info with its result decoded when
        ``include_result`` asked for it. Sets ``decoded_result`` and
        ``result_decoded`` on the broker's new ``TaskInfo``; otherwise pure
        apart from the lookup of the task's ``task_ok_type`` in the registry.
        """
        from horsies.core.brokers.result_types import (
            DatabaseInvalidJsonPayload,
        )
        from horsies.core.codec.json_value import StrictJsonError
        from horsies.core.codec.typed import (
            decode_task_error,
            decode_task_result,
            validate_task_result_envelope,
        )
        from horsies.core.types.result import Err, Ok, is_err

        if is_err(info_result):
            return Err(info_result.err_value)
        info = info_result.ok_value
        if info is None:
            return Ok(None)
        if not include_result or info.raw_result is None:
            return Ok(info)

        # Validate envelope shape before either fast-path or full
        # decode — mirrors ``get_result_async``. A malformed envelope
        # (missing marker, both slots populated, extras) must fail
        # closed rather than being silently returned undecoded.
        try:
            envelope = validate_task_result_envelope(info.raw_result)
        except StrictJsonError as exc:
            return Err(DatabaseInvalidJsonPayload(
                message=(
                    f'TaskResult envelope invalid for {task_id}: {exc}'
                ),
                cause=exc,
            ))
        err_slot = envelope.get('err')
        # Err-fast-path: TaskError is fixed-schema; decodable without
        # ok_type, so unregistered tasks still surface their real err
        # via ``decoded_result`` instead of being downgraded to
        # ``decoded_result=None``.
        if err_slot is not None:
            try:
                err_value = decode_task_error(err_slot)
            except (StrictJsonError, ValueValidationError) as exc:
                return Err(DatabaseInvalidJsonPayload(
                    message=(
                        f'TaskError decode failed for {task_id}: {exc}'
                    ),
                    cause=exc,
                ))
            from horsies.core.models.outcome import Failed
            info.decoded_result = Failed(err_value)
            info.result_decoded = True
            return Ok(info)

        # Ok slot — needs ok_type from local registry.
        task = self.tasks.get(info.task_name)
        ok_type = getattr(task, 'task_ok_type', None) if task is not None else None
        if ok_type is None:
            # Task not registered locally — leave decoded_result=None,
            # result_decoded=False (already the broker's default).
            return Ok(info)
        try:
            decoded = decode_task_result(info.raw_result, ok_type)
        except (StrictJsonError, ValueValidationError) as exc:
            return Err(DatabaseInvalidJsonPayload(
                message=(
                    f'get_task_info decode failed for {task_id}: {exc}'
                ),
                cause=exc,
            ))
        info.decoded_result = decoded
        info.result_decoded = True
        return Ok(info)

    def get_task_info(
        self,
        task_id: UUID,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> 'DatabaseResult[TaskInfo | None]':
        """Synchronous version of ``get_task_info_async``: the broker's
        blocking read (``PostgresBroker.get_task_info``) on the calling
        thread, with the GIL released.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('get_task_info')
        info_result = self.get_broker().get_task_info(
            task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
            include_attempts=include_attempts,
        )
        return self._decoded_task_info(task_id, include_result, info_result)

    # ------------- Health / liveness API -------------

    def _read_wait_refused(
        self, label: str, task_id: UUID, wait_ms: object,
    ) -> 'ReadResult[Any, TaskError[Any]] | None':
        """``Err(ReadInvalidInput)`` for a wait limit that no wait can use, or
        None to go on. The first check of a typed result read. Pure."""
        from horsies.core.models.outcome import ReadInvalidInput
        from horsies.core.utils.wait_timeout import wait_timeout_seconds

        match wait_timeout_seconds(wait_ms):
            case Err(err_value=refused):
                return Err(ReadInvalidInput(
                    message=f'app.{label}() for task {task_id}: {refused.message}',
                ))
            case Ok():
                return None

    def _raw_wait_refused(
        self, label: str, task_id: UUID, wait_ms: object,
    ) -> 'DatabaseResult[Any] | None':
        """``Err(DatabaseInvalidInput)`` for a wait limit that no wait can use,
        or None to go on. The first check of a raw result read. Pure."""
        from horsies.core.brokers.result_types import DatabaseInvalidInput
        from horsies.core.types.error_detail import NoCause
        from horsies.core.utils.wait_timeout import wait_timeout_seconds

        match wait_timeout_seconds(wait_ms):
            case Err(err_value=refused):
                return Err(DatabaseInvalidInput(
                    message=f'app.{label}() for task {task_id}: {refused.message}',
                    cause=NoCause(),
                ))
            case Ok():
                return None

    def _wait_inside_task_read(
        self, label: str, task_id: UUID,
    ) -> 'ReadResult[Any, TaskError[Any]]':
        """Refuse a result wait in a process that runs task bodies. Nothing ran."""
        from horsies.core.models.outcome import ReadWaitInsideTask

        return Err(ReadWaitInsideTask(
            message=wait_inside_task_message(f'app.{label}() for task {task_id}'),
        ))

    def _wait_inside_task_err(self, label: str, task_id: UUID) -> 'DatabaseResult[Any]':
        """Refuse a raw result wait in a process that runs task bodies. Nothing ran."""
        from horsies.core.brokers.result_types import DatabaseWaitInsideTask
        from horsies.core.types.result import Err

        return Err(DatabaseWaitInsideTask(
            message=wait_inside_task_message(f'app.{label}() for task {task_id}'),
        ))

    def _async_context_err(self, label: str) -> 'DatabaseResult[Any]':
        """Refuse a sync facade called inside a running event loop. Nothing ran."""
        from horsies.core.brokers.result_types import (
            DatabaseAsyncContext,
        )
        from horsies.core.types.result import Err

        return Err(DatabaseAsyncContext(
            message=async_context_message(
                f'app.{label}()', f'app.{label}_async(...)',
            ),
        ))

    async def ping_database_async(self) -> 'DatabaseResult[DatabasePing]':
        """Probe Postgres reachability + latency through the live pool."""
        return await self.get_broker().ping_database_async()

    def ping_database(self) -> 'DatabaseResult[DatabasePing]':
        """Synchronous version of ``ping_database_async``: the core's blocking
        ping on the calling thread, with the GIL released.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('ping_database')
        return self.get_broker()._ping_database_blocking()  # pyright: ignore[reportPrivateUsage]

    async def ping_workers_async(
        self,
        *,
        target: PingTarget = PingTargetEveryWorker(),
        wait_ms: int = 2_000,
        min_responses: MinResponses = MinResponsesFullWindow(),
    ) -> 'DatabaseResult[list[WorkerPong]]':
        """Active ping-pong: collect live workers' replies within ``wait_ms``
        milliseconds.

        Pass ``min_responses=MinResponsesAtLeast(1)`` for a fast fail-open
        liveness gate — returns on the first reply rather than waiting the
        full ``wait_ms``.
        """
        return await self.get_broker().ping_workers_async(
            target=target,
            wait_ms=wait_ms,
            min_responses=min_responses,
        )

    def ping_workers(
        self,
        *,
        target: PingTarget = PingTargetEveryWorker(),
        wait_ms: int = 2_000,
        min_responses: MinResponses = MinResponsesFullWindow(),
    ) -> 'DatabaseResult[list[WorkerPong]]':
        """Synchronous version of ``ping_workers_async``: the core's blocking
        ping on the calling thread, with the GIL released, and the same
        outcomes. The call is bounded by ``wait_ms`` plus the core's bound of
        a blocking call.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('ping_workers')
        return self.get_broker()._ping_workers_blocking(  # pyright: ignore[reportPrivateUsage]
            target=target,
            wait_ms=wait_ms,
            min_responses=min_responses,
        )

    async def list_worker_states_async(
        self,
    ) -> 'DatabaseResult[list[WorkerStateSnapshot]]':
        """Latest state snapshot per worker (including idle workers)."""
        return await self.get_broker().list_worker_states_async()

    def list_worker_states(self) -> 'DatabaseResult[list[WorkerStateSnapshot]]':
        """Synchronous version of ``list_worker_states_async``: the core's blocking
        read on the calling thread, with the GIL released, and the same
        outcomes.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('list_worker_states')
        return self.get_broker()._list_worker_states_blocking()  # pyright: ignore[reportPrivateUsage]

    async def get_worker_state_async(
        self,
        worker_id: str,
    ) -> 'DatabaseResult[WorkerStateSnapshot | None]':
        """Latest state snapshot for one worker, or ``None`` if unknown."""
        return await self.get_broker().get_worker_state_async(worker_id)

    def get_worker_state(
        self,
        worker_id: str,
    ) -> 'DatabaseResult[WorkerStateSnapshot | None]':
        """Synchronous version of ``get_worker_state_async``: the core's blocking
        read on the calling thread, with the GIL released, and the same
        outcomes.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('get_worker_state')
        return self.get_broker()._get_worker_state_blocking(worker_id)  # pyright: ignore[reportPrivateUsage]

    async def get_worker_state_history_async(
        self,
        worker_id: str,
        *,
        limit: WorkerHistoryLimit = WorkerHistoryLimitAllRetained(),
    ) -> 'DatabaseResult[list[WorkerStateSnapshot]]':
        """Timeseries snapshots for one worker, newest first: every retained
        snapshot, or the newest ``count`` of ``WorkerHistoryLimitNewest``."""
        return await self.get_broker().get_worker_state_history_async(
            worker_id, limit=limit,
        )

    def get_worker_state_history(
        self,
        worker_id: str,
        *,
        limit: WorkerHistoryLimit = WorkerHistoryLimitAllRetained(),
    ) -> 'DatabaseResult[list[WorkerStateSnapshot]]':
        """Synchronous version of ``get_worker_state_history_async``: the core's blocking
        read on the calling thread, with the GIL released, and the same
        outcomes.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('get_worker_state_history')
        return self.get_broker()._get_worker_state_history_blocking(  # pyright: ignore[reportPrivateUsage]
            worker_id, limit=limit,
        )

    async def get_stale_tasks_async(
        self,
        threshold_minutes: int,
    ) -> 'DatabaseResult[BoundedTaskList[StaleTaskRow]]':
        """RUNNING tasks whose latest runner heartbeat, or their start when
        they have no heartbeat, is older than ``threshold_minutes``. Reads
        only: the reaper recovers stale tasks.

        At most ``TASK_LIST_ROW_BOUND`` rows: ``BoundedTaskListComplete``, or
        ``BoundedTaskListTruncated`` when more rows match.

        - ``Err(INVALID_INPUT)``: ``threshold_minutes`` is not an int from 0
          to ``MAX_STALE_THRESHOLD_MINUTES`` (525,600, one year). Nothing ran.
        - ``Err(MONITORING_QUERY_FAILED)``: the read failed.
        """
        return await self.get_broker().get_stale_tasks(threshold_minutes)

    def get_stale_tasks(
        self,
        threshold_minutes: int,
    ) -> 'DatabaseResult[BoundedTaskList[StaleTaskRow]]':
        """Synchronous version of ``get_stale_tasks_async``: the core's
        blocking read on the calling thread, with the GIL released, and the
        same outcomes.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('get_stale_tasks')
        return self.get_broker()._get_stale_tasks_blocking(  # pyright: ignore[reportPrivateUsage]
            threshold_minutes,
        )

    async def get_expired_tasks_async(
        self,
    ) -> 'DatabaseResult[BoundedTaskList[ExpiredTaskRow]]':
        """PENDING tasks past their ``good_until``, oldest deadline first; no
        worker claims them. ``expired_for`` is on the database clock.

        At most ``TASK_LIST_ROW_BOUND`` rows: ``BoundedTaskListComplete``, or
        ``BoundedTaskListTruncated`` when more rows match.

        ``Err(MONITORING_QUERY_FAILED)``: the read failed.
        """
        return await self.get_broker().get_expired_tasks()

    def get_expired_tasks(self) -> 'DatabaseResult[BoundedTaskList[ExpiredTaskRow]]':
        """Synchronous version of ``get_expired_tasks_async``: the core's
        blocking read on the calling thread, with the GIL released, and the
        same outcomes.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('get_expired_tasks')
        return self.get_broker()._get_expired_tasks_blocking()  # pyright: ignore[reportPrivateUsage]

    async def get_execution_class_coverage_async(self) -> 'DatabaseResult[ExecutionClassCoverage]':
        """For each (queue, class) with PENDING rows, whether a live worker serves it.

        See ``PostgresBroker.get_execution_class_coverage_async``.
        """
        return await self.get_broker().get_execution_class_coverage_async()

    def get_execution_class_coverage(self) -> 'DatabaseResult[ExecutionClassCoverage]':
        """Synchronous version of ``get_execution_class_coverage_async``: the core's blocking
        read on the calling thread, with the GIL released, and the same
        outcomes.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('get_execution_class_coverage')
        return self.get_broker()._get_execution_class_coverage_blocking()  # pyright: ignore[reportPrivateUsage]

    async def get_execution_class_report_async(self) -> 'DatabaseResult[ExecutionClassReport]':
        """Pending streams with their oldest wait and coverage, and the budget
        load of each live worker.

        See ``PostgresBroker.get_execution_class_report_async``.
        """
        return await self.get_broker().get_execution_class_report_async()

    def get_execution_class_report(self) -> 'DatabaseResult[ExecutionClassReport]':
        """Synchronous version of ``get_execution_class_report_async``: the core's blocking
        read on the calling thread, with the GIL released, and the same
        outcomes.

        On a thread that runs an event loop it returns ``Err(ASYNC_CONTEXT)``
        and does no I/O.
        """
        if in_running_event_loop():
            return self._async_context_err('get_execution_class_report')
        return self.get_broker()._get_execution_class_report_blocking()  # pyright: ignore[reportPrivateUsage]

    def discover_tasks(
        self,
        modules: list[str],
    ) -> None:
        """
        Register task modules for later import.

        This method only records module paths — no file I/O happens here.
        Actual imports occur when import_task_modules() is called (typically by worker).

        Args:
            modules: List of dotted module paths (e.g., ['myapp.tasks', 'myapp.jobs.tasks'])
                     or file paths (e.g., ['tasks.py', 'src/worker_tasks.py'])

        Examples:
            app.discover_tasks(['myapp.tasks'])  # dotted module path
            app.discover_tasks(['tasks.py'])     # file path

            # For glob patterns, use expand_module_globs() first:
            paths = app.expand_module_globs(['src/**/*_tasks.py'])
            app.discover_tasks(paths)
        """
        if self._discovered_task_modules:
            self.logger.warning(
                f'discover_tasks() called again — replacing {len(self._discovered_task_modules)} '
                f'previously registered module(s) with {len(modules)} new module(s)'
            )
        self._discovered_task_modules = list(modules)

        is_child_process = os.getenv('HORSIES_CHILD_PROCESS') == '1'
        child_logs_enabled = os.getenv('HORSIES_CHILD_DISCOVERY_LOGS') == '1'
        should_log = not is_child_process or child_logs_enabled

        if should_log and len(modules) > 0:
            self.logger.info(f'Registered {len(modules)} task module(s) for discovery')

    def expand_module_globs(
        self,
        patterns: list[str],
        exclude: list[str] | None = None,
    ) -> list[str]:
        """
        Expand glob patterns to file paths.

        Use this explicitly when you need glob-based discovery.
        This is separated from discover_tasks() to make the I/O cost explicit.

        Args:
            patterns: Glob patterns like ['src/**/*_tasks.py'] or file paths
            exclude: Glob patterns to exclude (default: test files)

        Returns:
            List of absolute file paths

        Examples:
            paths = app.expand_module_globs(['src/**/*_tasks.py'])
            app.discover_tasks(paths)

        Raises:
            OSError: filesystem failure during glob/realpath expansion.
                Called at app-definition time — fail-fast where the config
                is declared.
        """
        exclude_patterns = exclude or ['*_test.py', 'test_*.py', 'conftest.py']
        results: list[str] = []

        def _is_excluded(path: str) -> bool:
            basename = os.path.basename(path)
            for pattern in exclude_patterns:
                if fnmatch(path, pattern) or fnmatch(basename, pattern):
                    return True
            return False

        for pattern in patterns:
            has_glob = any(ch in pattern for ch in ['*', '?', '[', ']'])
            if has_glob:
                for match in glob.glob(pattern, recursive=True):
                    abs_path = os.path.realpath(match)
                    if not os.path.exists(abs_path):
                        continue
                    if os.path.isdir(abs_path):
                        continue
                    if not abs_path.endswith('.py'):
                        continue
                    if _is_excluded(abs_path):
                        continue
                    if abs_path not in results:
                        results.append(abs_path)
            elif pattern.endswith('.py') or os.path.sep in pattern:
                # Direct file path
                abs_path = os.path.realpath(pattern)
                if os.path.exists(abs_path) and abs_path.endswith('.py'):
                    if not _is_excluded(abs_path) and abs_path not in results:
                        results.append(abs_path)
            else:
                # Dotted module path - pass through as-is
                if pattern not in results:
                    results.append(pattern)

        return results

    def get_discovered_task_modules(self) -> list[str]:
        """Get the list of discovered task modules"""
        return self._discovered_task_modules.copy()

    def import_task_modules(
        self,
        modules: Optional[list[str]] = None,
    ) -> list[str]:
        """Import task modules to eagerly register tasks.

        If modules is None, imports the modules discovered by discover_tasks().
        Returns the list of module identifiers that were imported.

        Raises:
            Exception: import errors (ImportError, module-level raises,
                registration errors) propagate — fail-fast at the import
                site. ``check()`` is the error-collecting variant
                (``_check_task_imports``); use it to report instead of
                crash. Missing module FILES are warn-and-skip here because
                discovery globs may legitimately go stale; bad module CODE
                must not be skipped.
        """
        modules_to_import = (
            self._discovered_task_modules if modules is None else modules
        )
        imported: list[str] = []
        for module in modules_to_import:
            if module.endswith('.py') or os.path.sep in module:
                abs_path = os.path.realpath(module)
                if not os.path.exists(abs_path):
                    self.logger.warning(f'Task module not found: {module}')
                    continue
                import_by_path(abs_path)
                imported.append(abs_path)
            else:
                importlib.import_module(module)
                imported.append(module)
        return imported

    # -------- side-effect control (import/discovery) --------
    def suppress_sends(self, value: bool = True) -> None:
        """Internal: enable/disable suppression of task sends/schedules.

        Do not call from user code. This is set automatically by the worker,
        scheduler, and check phases during module imports so that top-level
        ``.send()`` calls do not enqueue tasks as an import side effect.
        """
        self._suppress_sends = value

    def are_sends_suppressed(self) -> bool:
        """Internal: return True if sends/schedules should be no-ops.

        True when ``suppress_sends(True)`` was called (worker/scheduler import
        phase, ``check()``, builder execution) or when the environment variable
        ``HORSIES_SUPPRESS_SENDS=1`` is set.

        This is process-local state — it has no effect on other producers or
        workers in a multi-process cluster.
        """
        env_flag = os.getenv('HORSIES_SUPPRESS_SENDS', '').strip()
        return self._suppress_sends or env_flag == '1'

    # -------- workflow factory --------
    @overload
    def workflow(
        self,
        name: str,
        nodes: list[TaskNode[Any, Any] | SubWorkflowNode[Any]],
        on_error: OnError,
        output: TaskNode[OutT, Any] | SubWorkflowNode[OutT],
        success_policy: SuccessPolicy | None = None,
        *,
        definition_key: str | None = None,
        workflow_definition: type[Any] | None = None,
    ) -> WorkflowSpec[OutT]: ...

    @overload
    def workflow(
        self,
        name: str,
        nodes: list[TaskNode[Any, Any] | SubWorkflowNode[Any]],
        on_error: OnError = OnError.FAIL,
        *,
        output: TaskNode[OutT, Any] | SubWorkflowNode[OutT],
        success_policy: SuccessPolicy | None = None,
        definition_key: str | None = None,
        workflow_definition: type[Any] | None = None,
    ) -> WorkflowSpec[OutT]: ...

    @overload
    def workflow(
        self,
        name: str,
        nodes: list[TaskNode[Any, Any] | SubWorkflowNode[Any]],
        on_error: OnError = OnError.FAIL,
        output: None = None,
        success_policy: SuccessPolicy | None = None,
        *,
        definition_key: str | None = None,
        workflow_definition: type[Any] | None = None,
    ) -> WorkflowSpec[WorkflowTerminalResults]: ...

    def workflow(
        self,
        name: str,
        nodes: list[TaskNode[Any, Any] | SubWorkflowNode[Any]],
        on_error: OnError = OnError.FAIL,
        output: TaskNode[OutT, Any] | SubWorkflowNode[OutT] | None = None,
        success_policy: SuccessPolicy | None = None,
        *,
        definition_key: str | None = None,
        workflow_definition: type[Any] | None = None,
    ) -> WorkflowSpec[OutT] | WorkflowSpec[WorkflowTerminalResults]:
        """
        Create a validated WorkflowSpec with proper queue and priority resolution.

        Validates queues against app config and resolves priorities using
        effective_priority() to match non-workflow task behavior.

        Args:
            name: Human-readable workflow name
            nodes: List of TaskNode/SubWorkflowNode instances
            on_error: Error handling policy (FAIL or PAUSE)
            output: Explicit output task (optional)
            success_policy: Custom success policy for workflow completion (optional)

        Returns:
            WorkflowSpec ready to start

        Raises:
            MultipleValidationErrors: queue/mode violations collected here,
                or graph violations collected by WorkflowSpec construction.
            HorsiesError: broker initialization failure (``get_broker``),
                spec validation (WorkflowValidationError), or a missing
                ``definition_key`` on a definition-backed spec.
                Definition-time fail-fast by design: a spec that cannot be
                built must crash where it is declared, not at send time.
        """
        report = ValidationReport('workflow')
        # WorkflowSpec shallow-copies each node, so the resolved queue/priority
        # is captured from the original at copy time. We set it on the original
        # here and restore it after construction (see the try/finally below) so
        # app.workflow() never leaves a persistent side effect on the caller's
        # TaskNode — reusing one node across configs must be order-independent.
        # Caveat: the save/restore is not thread-safe; do not build specs
        # sharing the same node objects from concurrent threads.
        saved_node_config: list[tuple[TaskNode[Any, Any], str | None, int | None]] = []
        for node in nodes:
            # SubWorkflowNode doesn't have queue/priority - handled at execution time
            if isinstance(node, SubWorkflowNode):
                continue

            # TaskNode: resolve queue and priority
            task = node

            # Bind queue/priority through the shared resolver (same path the
            # subworkflow enqueue and check use). Run this for EVERY node —
            # including frozen nodes reused from a prior spec — so an invalid
            # concrete queue is still rejected (the resolver raises on a queue
            # not in this app's config). Collect rather than raise so
            # app.workflow() reports every offending node.
            try:
                resolved_queue, resolved_priority = resolve_node_queue_and_priority(
                    self, task,
                )
            except ConfigurationError as exc:
                report.add(exc)
                continue

            # Frozen nodes are immutable and already carry concrete
            # queue/priority; validated above, but not re-bound.
            if getattr(task, '_frozen', False):
                continue

            # Stash the caller's values so we can restore them after the spec
            # has copied the node.
            saved_node_config.append((task, task.queue, task.priority))
            task.queue = resolved_queue
            task.priority = resolved_priority

        try:
            raise_collected(report)

            # Create spec with validated tasks and broker. The spec
            # shallow-copies each node, capturing the resolved queue/priority
            # set above.
            spec = WorkflowSpec(
                name=name,
                nodes=nodes,
                on_error=on_error,
                output=output,
                success_policy=success_policy,
                broker=self.get_broker(),
                definition_key=definition_key,
                workflow_definition=workflow_definition,
                resend_on_transient_error=self.config.resend_on_transient_error,
            )
            spec._require_definition_key()
        finally:
            # Restore the caller's nodes — the spec now owns independent copies.
            for original, saved_queue, saved_priority in saved_node_config:
                original.queue = saved_queue
                original.priority = saved_priority

        if output is None:
            return cast('WorkflowSpec[WorkflowTerminalResults]', spec)
        return spec

    # -------- workflow builder contract --------
    def check_workflow_builder(
        self,
        *,
        cases: list[dict[str, Any]] | None = None,
    ) -> Callable[[_F], _F]:
        """Decorator registering a workflow builder for check-phase validation.

        Builders are functions that return WorkflowSpec. During `horsies check`,
        registered builders are executed under send suppression to validate the
        workflows they produce.

        Args:
            cases: For parameterized builders (with required params), a list of
                   kwarg dicts to invoke the builder with. Required when the
                   builder has parameters without defaults.
        """

        def decorator(function: _F) -> _F:
            location = SourceLocation.from_function(function)
            setattr(function, _BUILDER_ATTR, True)
            self._workflow_builders.append(
                _WorkflowBuilderMeta(
                    function=function,
                    cases=cases or [],
                    location=location,
                ),
            )
            return function

        return decorator

    def get_check_workflow_builders(self) -> list[_WorkflowBuilderMeta]:
        """Return registered builders (for tests and introspection)."""
        return list(self._workflow_builders)

    def _check_workflow_builders(self) -> list[HorsiesError]:
        """Execute registered builders under send suppression, collect errors."""
        errors: list[HorsiesError] = []
        prev_suppress = self._suppress_sends
        self.suppress_sends(True)
        try:
            for meta in self._workflow_builders:
                errors.extend(self._run_single_builder(meta))
        finally:
            self.suppress_sends(prev_suppress)
        return errors

    def _run_single_builder(
        self,
        meta: _WorkflowBuilderMeta,
    ) -> list[HorsiesError]:
        """Run a single builder (all cases or auto-invoke) and collect errors.

        Total: introspection and invocation failures fold into the returned
        error list (check-phase containment), never raise.
        """
        errors: list[HorsiesError] = []
        function = meta.function
        function_name = getattr(function, '__qualname__', function.__name__)
        try:
            sig = inspect.signature(function)
        except (ValueError, TypeError) as exc:
            errors.append(
                ConfigurationError(
                    message=f"builder '{function_name}' signature is not introspectable",
                    code=HorsiesErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
                    location=meta.location,
                    notes=[f'inspect.signature failed: {exc}'],
                    help_text='register a plain function (or functools.partial with a resolvable signature) as the builder',
                ),
            )
            return errors

        # Determine if builder has required params
        required_params = [
            p
            for p in sig.parameters.values()
            if p.default is inspect.Parameter.empty
            and p.kind
            not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        ]
        has_required = len(required_params) > 0

        if not has_required:
            # Zero-arg or all-defaults builder: auto-run once
            errors.extend(self._invoke_builder(function, function_name, {}, meta.location))
            return errors

        # Parameterized builder: must have cases
        if not meta.cases:
            errors.append(
                ConfigurationError(
                    message=f"parameterized builder '{function_name}' missing cases",
                    code=HorsiesErrorCode.WORKFLOW_CHECK_CASES_REQUIRED,
                    location=meta.location,
                    notes=[
                        f'required params: {[p.name for p in required_params]}',
                    ],
                    help_text=(
                        'add cases= to @app.check_workflow_builder() with at least one '
                        'dict of kwargs covering each required parameter'
                    ),
                ),
            )
            return errors

        # Validate and run each case
        param_names = set(sig.parameters.keys())
        for idx, case in enumerate(meta.cases):
            if not isinstance(case, dict):  # type: ignore[reportUnnecessaryIsInstance]
                errors.append(
                    ConfigurationError(
                        message=f"builder '{function_name}' case[{idx}] is not a dict",
                        code=HorsiesErrorCode.WORKFLOW_CHECK_CASE_INVALID,
                        location=meta.location,
                        notes=[f'got {type(case).__name__}'],
                        help_text='each case must be a dict of keyword arguments',
                    ),
                )
                continue

            # Check for unknown keys
            unknown_keys = set(case.keys()) - param_names
            if unknown_keys:
                errors.append(
                    ConfigurationError(
                        message=f"builder '{function_name}' case[{idx}] has unknown keys",
                        code=HorsiesErrorCode.WORKFLOW_CHECK_CASE_INVALID,
                        location=meta.location,
                        notes=[
                            f'unknown: {sorted(unknown_keys)}',
                            f'valid params: {sorted(param_names)}',
                        ],
                        help_text='case keys must match builder parameter names',
                    ),
                )
                continue

            # Check for missing required keys
            missing_keys = {p.name for p in required_params} - set(case.keys())
            if missing_keys:
                errors.append(
                    ConfigurationError(
                        message=f"builder '{function_name}' case[{idx}] missing required keys",
                        code=HorsiesErrorCode.WORKFLOW_CHECK_CASE_INVALID,
                        location=meta.location,
                        notes=[
                            f'missing: {sorted(missing_keys)}',
                        ],
                        help_text='each case must provide all required parameters',
                    ),
                )
                continue

            errors.extend(self._invoke_builder(function, function_name, case, meta.location))

        return errors

    def _invoke_builder(
        self,
        function: Callable[..., Any],
        function_name: str,
        kwargs: dict[str, Any],
        location: SourceLocation | None,
    ) -> list[HorsiesError]:
        """Call a builder with kwargs and collect any errors it produces."""
        errors: list[HorsiesError] = []
        try:
            result = function(**kwargs)
            if not self._is_workflow_spec_like(result):
                case_summary = f' with {kwargs}' if kwargs else ''
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message=f"builder '{function_name}' returned non-WorkflowSpec{case_summary}",
                            code=HorsiesErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
                            location=location,
                            notes=[f'got {type(result).__name__}'],
                            help_text='workflow builders must return a WorkflowSpec instance',
                        ),
                    ),
                )
            else:
                result._require_definition_key()
                self._validate_spec_bindings(result, errors, ())
        except MultipleValidationErrors as exc:
            errors.extend(exc.report.refusals)
        except HorsiesError as exc:
            errors.append(exc)
        except Exception as exc:
            case_summary = f' with {kwargs}' if kwargs else ''
            errors.append(
                _no_location(
                    ConfigurationError(
                        message=f"builder '{function_name}' raised an unexpected exception{case_summary}",
                        code=HorsiesErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
                        location=location,
                        notes=[f'{type(exc).__name__}: {exc}'],
                        help_text='workflow builders must not raise non-horsies exceptions during check',
                    ),
                ),
            )
        return errors

    def _validate_spec_bindings(
        self,
        spec: Any,
        errors: list[HorsiesError],
        path: NestingPath,
    ) -> None:
        """Check the nodes of ``spec`` and append each finding to ``errors``.

        A TaskNode is bound through the resolver that the runtime persist
        paths use, so an invalid queue fails check instead of surfacing at
        enqueue time. A SubWorkflowNode goes to ``_check_subworkflow_node``.
        ``path`` holds the SubWorkflowNodes from the root spec to ``spec``.
        Raises nothing for a finding.
        """
        for node in getattr(spec, 'nodes', ()):
            if isinstance(node, SubWorkflowNode):
                self._check_subworkflow_node(node, errors, path)
                continue
            try:
                resolve_node_queue_and_priority(self, node)
            except ConfigurationError as exc:
                errors.append(exc)

    def _check_subworkflow_node(
        self,
        node: SubWorkflowNode[Any],
        errors: list[HorsiesError],
        path: NestingPath,
    ) -> None:
        """Walk the child spec of ``node`` when check can build it.

        A node with ``args_from`` takes kwargs from results at run time, so
        its child is not built; register its build_with as a builder for
        coverage. ``decide_nesting`` stops the walk at a repeated definition
        and at the depth limit, and gives a finding only for a nesting that
        fails for certain. Each finding goes to ``errors``.
        """
        # Only expand when build_with's inputs are fully static. args_from
        # and from_node() markers are runtime injections (dependency
        # results) the engine merges in at enqueue, absent at check.
        if node.args_from:
            return
        try:
            def_key = node.workflow_definition._require_definition_key()
        except HorsiesError:
            return  # missing definition_key reported elsewhere
        # Key a repeat on (definition_key, kwargs signature), not
        # definition_key alone: a definition that includes itself with
        # DIFFERENT kwargs produces a different child spec whose queue
        # binding must still be validated. The kwargs signature is a stable
        # repr (workflow kwargs are serializable).
        wire = stored_kwargs(node)
        step = NestingStep(
            definition_key=def_key,
            kwargs_signature=repr(sorted(wire.items())),
            start=child_start(node),
        )
        match decide_nesting(path, node.workflow_name, step):
            case StopWithoutFinding():
                return
            case StopWithFinding(finding=finding):
                errors.append(finding)
            case WalkChild(path=child_path):
                match self._build_child_spec_for_check(node, wire, errors):
                    case _ChildNotBuilt():
                        return
                    case _ChildBuilt(spec=child_spec):
                        self._validate_spec_bindings(child_spec, errors, child_path)

    def _build_child_spec_for_check(
        self,
        node: SubWorkflowNode[Any],
        wire: dict[str, Any],
        errors: list[HorsiesError],
    ) -> _ChildBuilt | _ChildNotBuilt:
        """Build the child spec of ``node`` the way the engine does.

        Building the child runs its full validation (queue/mode AND
        structural: empty tasks, cycles, node ids). Each finding goes to
        ``errors``, and the result is then ``_ChildNotBuilt``. A non-horsies
        exception gives no verdict on the workflow: it is logged, or left to
        the run time, and the result is ``_ChildNotBuilt``.
        """
        try:
            # The engine's read of the same row: real containers and
            # filled defaults, not the frozen snapshot on the node.
            child_kwargs = fill_build_with_defaults(
                node.workflow_definition,
                decode_subworkflow_kwargs(node.workflow_definition, wire),
            )
        except (StrictJsonError, ValueValidationError) as exc:
            # At run time this fails the parent node, so it is a finding.
            errors.append(WorkflowValidationError(
                message='subworkflow kwargs do not fit build_with',
                code=HorsiesErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE,
                notes=[f"node '{node.workflow_name}': {exc}"],
                help_text='fix the kwarg, or the default of the parameter',
            ))
            return _ChildNotBuilt()
        except Exception as exc:
            # No verdict on the value: a validator can need a service that
            # is down at boot, so this must not stop the process.
            self.logger.warning(
                f"check could not fill the defaults of node '{node.workflow_name}': "
                f'{type(exc).__name__}: {exception_text(exc)}',
            )
            return _ChildNotBuilt()
        try:
            child_spec = node.workflow_definition.build_with(self, **child_kwargs)
        except MultipleValidationErrors as exc:
            errors.extend(exc.report.refusals)
            return _ChildNotBuilt()
        except HorsiesError as exc:
            # Any horsies validation error building the child is a real check
            # finding for this static subworkflow. Surface it so single- and
            # multi-error failures are treated alike.
            errors.append(exc)
            return _ChildNotBuilt()
        except Exception:
            # A non-horsies exception from build_with is out of scope (a
            # user-code or environmental failure, not workflow validation);
            # it surfaces via the registered-builder path or at runtime.
            return _ChildNotBuilt()
        return _ChildBuilt(child_spec)

    def _check_undecorated_builders(self) -> list[HorsiesError]:
        """Detect top-level functions returning WorkflowSpec without decorator.

        For package modules (those with __path__), also scans already-loaded
        sub-modules from sys.modules so that builders defined in sub-modules
        are not silently missed.
        """
        errors: list[HorsiesError] = []

        for module_path in self._discovered_task_modules:
            module = self._resolve_imported_module(module_path)
            if module is None:
                continue

            modules_to_check = [module]
            if hasattr(module, '__path__'):
                prefix = module.__name__ + '.'
                for name, mod in sys.modules.items():
                    if name.startswith(prefix) and mod is not None:
                        modules_to_check.append(mod)

            for mod in modules_to_check:
                errors.extend(self._check_module_for_undecorated_builders(mod))

        return errors

    def _check_module_for_undecorated_builders(
        self,
        module: Any,
    ) -> list[HorsiesError]:
        """Check a single module for undecorated WorkflowSpec-returning functions."""
        errors: list[HorsiesError] = []

        try:
            hints_by_name = self._collect_module_function_return_hints(module)
        except Exception:
            # If we can't resolve type hints (forward refs, etc.), skip module
            return errors

        for fn_name, return_hint in hints_by_name.items():
            fn_obj = getattr(module, fn_name)

            # Skip if already decorated
            if getattr(fn_obj, _BUILDER_ATTR, False):
                continue

            # Skip if explicitly opted out
            if getattr(fn_obj, _NO_CHECK_ATTR, False):
                continue

            if self._hint_is_workflow_spec(return_hint):
                location = SourceLocation.from_function(fn_obj)
                errors.append(
                    ConfigurationError(
                        message=f"undecorated workflow builder '{fn_name}'",
                        code=HorsiesErrorCode.WORKFLOW_CHECK_UNDECORATED_BUILDER,
                        location=location,
                        notes=[
                            f"function '{fn_name}' returns WorkflowSpec but is not decorated with @app.check_workflow_builder",
                        ],
                        help_text=(
                            'add @app.check_workflow_builder() to register this builder for check validation;\n'
                            f'or set {fn_name}.__horsies_no_check__ = True to suppress this warning'
                        ),
                    ),
                )

        return errors

    @staticmethod
    def _collect_module_workflow_definitions(
        module: Any,
    ) -> list[type[Any]]:
        """Collect WorkflowDefinition subclasses defined in a module/package."""
        from horsies.core.models.workflow.definition import WorkflowDefinition

        definitions: list[type[Any]] = []
        is_package = hasattr(module, '__path__')
        for _name, obj in inspect.getmembers(module, inspect.isclass):
            if obj is WorkflowDefinition:
                continue
            try:
                if not issubclass(obj, WorkflowDefinition):
                    continue
            except TypeError:
                continue

            obj_module = getattr(obj, '__module__', None)
            if obj_module != module.__name__:
                if not (
                    is_package
                    and obj_module is not None
                    and obj_module.startswith(module.__name__ + '.')
                ):
                    continue
            definitions.append(obj)
        return definitions

    @staticmethod
    def _resolve_imported_module(module_path: str) -> Any | None:
        """Look up an already-imported module by path or dotted name."""
        if module_path.endswith('.py') or os.path.sep in module_path:
            abs_path = os.path.realpath(module_path)
            # Find module in sys.modules by filename
            for mod in sys.modules.values():
                mod_file = getattr(mod, '__file__', None)
                if mod_file is not None and os.path.realpath(mod_file) == abs_path:
                    return mod
            return None
        return sys.modules.get(module_path)

    @staticmethod
    def _collect_module_function_return_hints(
        module: Any,
    ) -> dict[str, Any]:
        """Get return type hints for top-level functions in a module."""
        result: dict[str, Any] = {}
        is_package = hasattr(module, '__path__')
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            # Only functions defined in this module (or its sub-modules for packages)
            obj_module = getattr(obj, '__module__', None)
            if obj_module != module.__name__:
                if not (
                    is_package
                    and obj_module is not None
                    and obj_module.startswith(module.__name__ + '.')
                ):
                    continue
            try:
                hints = inspect.get_annotations(obj, eval_str=True)
            except Exception:
                continue
            return_hint = hints.get('return')
            if return_hint is not None:
                result[name] = return_hint
        return result

    @staticmethod
    def _hint_is_workflow_spec(hint: Any) -> bool:
        """Check if a type hint resolves to WorkflowSpec[...] or bare WorkflowSpec."""
        from typing import get_origin

        if hint is WorkflowSpec:
            return True
        origin = get_origin(hint)
        if origin is WorkflowSpec:
            return True
        return False

    @staticmethod
    def _is_workflow_spec_like(value: Any) -> bool:
        """Return True for WorkflowSpec instances and test doubles with spec shape."""
        if isinstance(value, WorkflowSpec):
            return True
        required_attrs = ('name', 'nodes', 'on_error', 'broker')
        return all(hasattr(value, attr) for attr in required_attrs)
