# app/core/task_decorator.py
from __future__ import annotations
import asyncio
import dataclasses
import inspect
import time
from dataclasses import dataclass
from enum import Enum
from typing import (
    Callable,
    Concatenate,
    Final,
    get_type_hints,
    Literal,
    ParamSpec,
    Sequence,
    TypeVar,
    Generic,
    Protocol,
    TYPE_CHECKING,
    Optional,
    Any,
    Never,
    assert_never,
    cast,
    overload,
)
from abc import abstractmethod
from collections.abc import Coroutine, Mapping
from types import MappingProxyType
from uuid import UUID
from datetime import datetime, timedelta, timezone
from pydantic import TypeAdapter, ValidationError
from horsies.core.codec.json_value import StrictJsonError, escape_lone_surrogates
from horsies.core.codec.kwargs import (
    encode_kwargs_read_back,
    fill_omitted_defaults,
    missing_required_kwargs,
)
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.payload_guard import (
    PayloadOverLimit,
    PayloadWithinLimit,
    check_payload_size,
    warn_if_payload_large,
)
from horsies.core.codec.task_options import serialize_task_options
from horsies.core.task_runtime import TaskRuntime
from horsies.core.process_role import (
    ProcessRole,
    process_role,
    wait_inside_task_message,
)
from horsies.core.codec.typed import (
    Json,
    TypeAnnotation,
    lone_surrogate_text,
)
from horsies.core.codec.signature_check import (
    SignatureValidationError,
    check_task_signature,
)
from horsies.core.codec.validation_errors import (
    ValueValidationError,
    exception_text,
    validation_error_entries,
    validation_summary,
    without_validation_input,
)
from horsies.core.history.periods import (
    RetentionClassKey,
    RetentionDays,
    parse_retention_period,
    period_class_key,
)
from horsies.core.models.retention import (
    RetentionConfig,
    resolve_queue_retention_class,
)
from horsies.core.utils.loop_runner import (
    async_context_message,
    in_running_event_loop,
)
from horsies._core import IdMintRefused, mint_task_id as core_mint_task_id
from horsies.core.utils.wait_timeout import wait_timeout_seconds

from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import (
    Err,
    NotAResultAnnotation,
    Ok,
    Result,
    ResultTypeArguments,
    is_err,
    result_type_arguments,
)
from horsies.core.models.outcome import (
    Failed,
    ReadAsyncContext,
    ReadInvalidInput,
    ReadResult,
    ReadResultNotAvailable,
    ReadWaitInsideTask,
    Succeeded,
)
from horsies.core.task_read import is_final_read, read_of_record
from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.result_types import (
    DatabaseAsyncContext,
    DatabaseEnqueueError,
    DatabaseEnqueueFailed,
    DatabaseIdempotencyKeyConflict,
    DatabaseInvalidInput,
    DatabaseInvalidTaskOptions,
    DatabaseNotAttached,
    DatabasePayloadMismatch,
    DatabaseResult,
    DatabaseSyncCallError,
)
from horsies.core.models.task_send_types import (
    TaskSendAsyncContext,
    TaskSendEnqueueFailed,
    TaskSendIdempotencyKeyConflict,
    TaskSendPayload,
    TaskSendPayloadMismatch,
    TaskSendPayloadTooLarge,
    TaskSendResendable,
    TaskSendResult,
    TaskSendSuppressed,
    TaskSendValidationFailed,
)

if TYPE_CHECKING:
    from horsies.core.app import Horsies
    from horsies.core.models.tasks import TaskOptions
    from horsies.core.models.tasks import TaskError
    from horsies.core.models.tasks import TaskInfo
    from horsies.core.models.workflow import (
        TaskNode,
        SubWorkflowNode,
    )

from horsies.core.models.tasks import (
    DEFAULT_CANCEL_GRACE_MS,
    MAX_CANCEL_GRACE_MS,
    MIN_CANCEL_GRACE_MS,
    BUILTIN_FAMILIES,
    CustomCodeType,
    EnumCodes,
    Execution,
    NotATaskErrorType,
    TaskError,
    TaskOptions,
    TextCodes,
    BuiltInTaskCode,
    ContractCode,
    custom_code_type_of,
)
from horsies.core.models.workflow import WorkflowContextMissingId
from horsies.core.telemetry.send_telemetry import (
    NO_TRACE_CARRIER,
    NoSendTelemetry,
    OtelSendTelemetry,
    TaskSendSubject,
    TraceArgument,
    task_send_end,
    traced,
    traced_async,
)
from horsies.core.exception_mapper import (
    ExceptionMapper,
    resolve_exception_error_code,
)
from horsies.core.errors import (
    TaskDefinitionError,
    ConfigurationError,
    WorkflowValidationError,
    HorsiesErrorCode,
    SourceLocation,
)

P = ParamSpec('P')
T = TypeVar('T')
C = TypeVar('C', default=str)
"""The type of the custom codes of a task: `str`, or the `Enum` class that
the task declares with `error_codes=` and names in `TaskError[...]`."""
_UNSET: Final[object] = object()


class _UnsetRetention:
    """No retention choice was made at this send call.

    Retention needs three states where a default value only carries
    two. Thirty days passed explicitly and the parameter omitted would be
    the same value, but they must resolve differently: a queue mapping
    loses to the explicit choice and wins over the omission. Absence is
    therefore its own value.

    Typed, unlike the untyped `_UNSET` above it, because these three
    states are the whole semantics and a checker should reject anything
    outside them.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return '<no retention choice>'


_UNSET_RETENTION: Final[_UnsetRetention] = _UnsetRetention()

# A retention argument as callers may express it: a finite period, explicit
# None for forever, or absence.
type SendRetention = timedelta | None | _UnsetRetention


@dataclass(frozen=True, slots=True)
class _FromQueueMapping:
    """No explicit choice: the queue's mapping, or the default, decides."""


@dataclass(frozen=True, slots=True)
class _ExplicitClass:
    """The send chose its class. ``key`` None is the explicit forever."""

    key: RetentionClassKey | None


type _RetentionStamp = _FromQueueMapping | _ExplicitClass

# Internal retry constants for resend_on_transient_error.
# 1 initial attempt + _SEND_RETRY_COUNT retries = 4 total broker calls.
_SEND_RETRY_COUNT: Final[int] = 3
_SEND_RETRY_INITIAL_MS: Final[int] = 200
_SEND_RETRY_MAX_MS: Final[int] = 2000


def _mint_send_task_id() -> UUID:
    """The task id of a new send: a UUIDv7 from the core's process-monotonic
    source (``horsies._core.mint_task_id``).

    Raises ``ValueError`` with the source's cause when it refuses (the
    millisecond clock is outside the 48-bit range), as the Python generator
    raised. Raises ``ForkedAfterCoreStart`` and ``CoreStopped``, the
    refusals of every core call in this process.
    """
    match core_mint_task_id():
        case UUID() as task_id:
            return task_id
        case IdMintRefused(cause=cause):
            raise ValueError(cause)


def _send_kind_of_attempt(first: SendKind, attempt: int) -> SendKind:
    """The send kind of broker call ``attempt`` (0 is the first) of one send.

    ``first`` is the kind of the first call: ``FIRST`` for a new send,
    ``REPEAT`` for a resend method. Every later call of the resend loop sends
    the same task id and ``sent_at`` again, so it is a ``REPEAT``. Pure.
    """
    match attempt:
        case 0:
            return first
        case _:
            return SendKind.REPEAT

# The send variants that a broker error maps to.
type _BrokerSendError = (
    TaskSendValidationFailed
    | TaskSendEnqueueFailed
    | TaskSendPayloadMismatch
    | TaskSendIdempotencyKeyConflict
    | TaskSendAsyncContext
)


class FromNodeMarker:
    """Marker for injecting an upstream node's result into a `.node()()` kwarg.

    Created via `from_node(upstream_node)`. When passed as a kwarg value to
    `NodeFactory.__call__()`, the factory converts it to an `args_from` entry
    and auto-wires the dependency.

    Not intended for direct construction — use `from_node()` instead.
    """

    __slots__ = ('_node',)

    def __init__(self, node: 'TaskNode[Any, Any] | SubWorkflowNode[Any]') -> None:
        self._node = node

    @property
    def node(self) -> 'TaskNode[Any, Any] | SubWorkflowNode[Any]':
        """The upstream node whose result will be injected."""
        return self._node

    def __repr__(self) -> str:
        return f'FromNodeMarker(node={self._node!r})'


def from_node(upstream: 'TaskNode[Any, Any] | SubWorkflowNode[Any]') -> Any:
    """Mark a `.node()()` kwarg as injected from an upstream node's result.

    When used as a kwarg value in the second call of the two-stage
    `.node()()` API, the NodeFactory will:
    1. Add the upstream node to `args_from` under this kwarg's key.
    2. Auto-add the upstream node to `waits_for` if not already present.

    Returns Any so pyright accepts it in place of any expected kwarg type.

    Example::

        producer_node = produce.node()(value=42)
        consumer_node = consume.node()(data=from_node(producer_node))
        # Equivalent to:
        # TaskNode(function=consume, args_from={"data": producer_node}, waits_for=[producer_node])
    """
    from horsies.core.models.workflow import TaskNode, SubWorkflowNode

    if not isinstance(upstream, (TaskNode, SubWorkflowNode)):
        raise WorkflowValidationError(
            message='from_node() expects a TaskNode or SubWorkflowNode',
            code=HorsiesErrorCode.WORKFLOW_INVALID_ARGS_FROM,
            notes=[f'got {type(upstream).__name__}'],
            help_text='pass a node instance created by TaskNode(...) / SubWorkflowNode(...) or .node()()',
        )

    return FromNodeMarker(upstream)


def effective_priority(
    app: 'Horsies',
    queue_name: str,
) -> int:
    """Resolve the effective priority for a queue.

    Raises:
        ConfigurationError: queue_name not in custom_queues (CUSTOM mode).
            The send path folds this into Err(VALIDATION_FAILED) in
            ``_prepare_send``; ``app.workflow()`` and the scheduler
            pre-validate the queue before calling.
    """
    if app.config.queue_mode.name == 'DEFAULT':
        return 100  # default priority, least important
    config = next(
        (q for q in (app.config.custom_queues or []) if q.name == queue_name),
        None,
    )
    if config is None:
        valid = [q.name for q in (app.config.custom_queues or [])]
        raise ConfigurationError(
            message=f"queue '{queue_name}' not found in custom_queues",
            code=HorsiesErrorCode.TASK_INVALID_QUEUE,
            notes=[f'valid queues: {valid}'],
            help_text='use one of the configured queue names',
        )
    return config.priority


def resolve_node_queue(node: 'TaskNode[Any, Any]') -> str:
    """Return the queue name of a workflow ``TaskNode``.

    Rule: ``node.queue``, else the task's declared ``queue``, else
    ``'default'``. An empty string counts as not set at each step, so the
    result is never ``''``. It does not check the name against the app's
    queue config: ``resolve_node_queue_and_priority`` does that.
    """
    return node.queue or node.function.queue or 'default'


def resolve_node_queue_and_priority(
    app: 'Horsies',
    node: 'TaskNode[Any, Any]',
) -> tuple[str, int]:
    """Bind a workflow ``TaskNode``'s queue and priority against app config.

    Single source of truth for resolving an authored ``TaskNode`` (whose
    ``queue``/``priority`` may be ``None``, meaning "inherit from the queue
    config") to concrete values before persistence. ``app.workflow()``, the
    subworkflow enqueue path, and ``app.check()`` all route through this so the
    three cannot diverge — the divergence that let subworkflow tasks persist at
    a hardcoded ``100`` instead of their queue priority.

    Resolution:
        queue    = resolve_node_queue(node)
        priority = node.priority if set, else effective_priority(app, queue)

    Raises:
        ConfigurationError: queue invalid for the app's queue mode — CUSTOM
            mode and queue not in custom_queues (TASK_INVALID_QUEUE), or
            DEFAULT mode and queue is not 'default' (CONFIG_INVALID_QUEUE_MODE).
    """
    queue = resolve_node_queue(node)
    if app.config.queue_mode.name == 'CUSTOM':
        valid_queues = app.get_valid_queue_names()
        if queue not in valid_queues:
            raise ConfigurationError(
                message='TaskNode queue not in app config',
                code=HorsiesErrorCode.TASK_INVALID_QUEUE,
                notes=[
                    f"TaskNode '{node.name}' has queue '{queue}'",
                    f'valid queues: {valid_queues}',
                ],
                help_text='use one of the configured queue names or add this queue to app config',
            )
    elif queue != 'default':
        raise ConfigurationError(
            message='TaskNode has non-default queue in DEFAULT mode',
            code=HorsiesErrorCode.CONFIG_INVALID_QUEUE_MODE,
            notes=[
                f"TaskNode '{node.name}' has queue '{queue}'",
                "app is in DEFAULT mode (only 'default' queue allowed)",
            ],
            help_text='either remove queue override or switch to QueueMode.CUSTOM',
        )
    priority = (
        node.priority if node.priority is not None
        else effective_priority(app, queue)
    )
    return queue, priority


@dataclass(frozen=True)
class BoundTaskNode:
    """A workflow ``TaskNode`` with queue/priority resolved against app config.

    Produced only by ``bind_task_node``. Persistence reads ``queue``/``priority``
    from here — both non-optional — so no enqueue path can fall back to a
    guessed default. ``node`` carries the remaining authored fields unchanged.
    """

    node: 'TaskNode[Any, Any]'
    queue: str
    priority: int


def bind_task_node(
    app: 'Horsies',
    node: 'TaskNode[Any, Any]',
) -> BoundTaskNode:
    """Resolve a ``TaskNode`` to a ``BoundTaskNode`` (queue/priority concrete).

    Raises:
        ConfigurationError: from ``resolve_node_queue_and_priority`` when the
            node's queue is invalid for the app's queue mode.
    """
    queue, priority = resolve_node_queue_and_priority(app, node)
    return BoundTaskNode(node=node, queue=queue, priority=priority)


class TaskHandle(Generic[T, C]):
    """
    Handle for a submitted task, used to read its result.

    ``get()`` and ``get_async()`` return ``ReadResult[T, TaskError[C]]``,
    where ``C`` is the type of the task's custom codes (``str``, or the
    ``Enum`` class that it declares):

    - ``Ok(Succeeded(value))``: the task returned ``value``.
    - ``Ok(Failed(task_error))``: the task returned this error, or the
      library recorded it (``TASK_CANCELLED``, ``TASK_TIMEOUT``, ...).
    - ``Err(read_error)``: the read gave no outcome (``ReadWaitTimeout``,
      ``ReadTaskNotFound``, ``ReadDatabaseError``, ...). A read failure is never a
      ``TaskError``.

    Neither method raises for a task error or a read failure. The handle
    keeps the first final read (``is_final_read``): an outcome,
    ``ReadResultNotAvailable``, ``ReadResultDeserializationError``,
    ``ReadUnknownCustomCode`` and ``ReadNoTypeAvailable``.
    """

    def __init__(
        self,
        task_id: UUID,
        app: Optional['Horsies'] = None,
        broker_mode: bool = False,
        ok_type: TypeAnnotation | None = None,
        code_type: CustomCodeType = TextCodes(),
    ):
        self.task_id = task_id
        self._app = app
        self._broker_mode = broker_mode
        # Declared OkT for typed decode. ``.send()`` / ``.send_async()``
        # populate this from the wrapper's ``task_ok_type``. By-id
        # reconstruction without an app or registered task leaves this
        # ``None``; ``.get()`` then returns ``ReadNoTypeAvailable`` when an ok
        # payload is present.
        self.ok_type: TypeAnnotation | None = ok_type
        # The type of the custom codes of the task: a read turns a stored
        # custom code into the member of the declared Enum (`EnumCodes`).
        self.code_type: CustomCodeType = code_type
        self._cached_result: Optional[ReadResult[T, TaskError[C]]] = None

    def _kept(self, read: ReadResult[Any, TaskError[Any]]) -> ReadResult[T, TaskError[C]]:
        """Keep ``read`` when it is final, and return it as this handle's read."""
        typed = cast('ReadResult[T, TaskError[C]]', read)
        if is_final_read(read):
            self._cached_result = typed
        return typed

    def _waits_are_refused(self) -> bool:
        """Return whether a result wait on this handle is refused here.

        A broker-backed handle waits on the broker. In a process that runs
        task bodies, that wait is refused.
        """
        return (
            self._broker_mode
            and self._app is not None
            and process_role() is ProcessRole.RUNS_TASK_BODIES
        )

    def _wait_inside_task_result(self, operation: str) -> ReadResult[T, TaskError[C]]:
        """Return ``ReadWaitInsideTask`` for a refused wait. Not kept; nothing ran."""
        return Err(ReadWaitInsideTask(message=wait_inside_task_message(
            f'TaskHandle.{operation}() for task {self.task_id}',
        )))

    def _before_read(
        self, operation: str, wait_ms: Optional[int],
    ) -> ReadResult[T, TaskError[C]] | None:
        """The answer that comes before any broker read, or None to read.

        In order: ``ReadInvalidInput`` for a ``wait_ms`` that no wait can use,
        ``ReadWaitInsideTask`` in a process that runs task bodies, the kept read,
        and ``ReadResultNotAvailable`` (kept) for a handle with no broker. The
        wait limit is the first check in every client, so one input gives
        one code.
        """
        match wait_timeout_seconds(wait_ms):
            case Err(err_value=refused):
                return Err(ReadInvalidInput(
                    message=f'TaskHandle.{operation}() for task {self.task_id}: {refused.message}',
                ))
            case Ok():
                pass
        if self._waits_are_refused():
            return self._wait_inside_task_result(operation)
        if self._cached_result is not None:
            return self._cached_result
        if self._broker_mode and self._app:
            return None
        return self._kept(Err(ReadResultNotAvailable(
            message=f'Task {self.task_id}: result not available; the task may not have run',
        )))

    def get(
        self,
        wait_ms: Optional[int] = None,
    ) -> ReadResult[T, TaskError[C]]:
        """
        Read the task result (blocking).

        The first check is the wait limit: a wait_ms that is not None or
        an int from 0 to MAX_TASK_TIMEOUT_MS returns ``Err(ReadInvalidInput)``
        (not kept) before any other check. Then a broker-backed handle
        returns ``Err(ReadWaitInsideTask)`` (not kept) in a process that runs
        task bodies; compose tasks with a workflow there. It returns
        ``Err(ReadAsyncContext)`` (not kept) on a thread that runs an event loop;
        use ``get_async()`` there.

        Args:
            wait_ms: Maximum time to wait for result (milliseconds)

        Returns:
            ReadResult[T, TaskError[C]]: ``Ok(Succeeded(value))``,
            ``Ok(Failed(task_error))`` or ``Err(read_error)``. Never raises
            for a task error or a read failure.
        """
        before = self._before_read('get', wait_ms)
        if before is not None:
            return before
        assert self._app is not None, 'a broker-backed handle reads through its app'
        if in_running_event_loop():
            return Err(ReadAsyncContext(message=async_context_message(
                f'TaskHandle.get() for task {self.task_id}',
                'handle.get_async(...)',
            )))
        broker_result = self._app.get_broker().get_raw_result_record(
            self.task_id, wait_ms,
        )
        return self._kept(
            read_of_record(
                self.task_id, broker_result, self.ok_type, wait_ms, self.code_type,
            ),
        )

    async def get_async(
        self,
        wait_ms: Optional[int] = None,
    ) -> ReadResult[T, TaskError[C]]:
        """
        Read the task result asynchronously.

        The first check is the wait limit, as in ``get()``: a wait_ms
        that is not None or an int from 0 to MAX_TASK_TIMEOUT_MS returns
        ``Err(ReadInvalidInput)`` (not kept). Then a broker-backed handle returns
        ``Err(ReadWaitInsideTask)`` (not kept) in a process that runs task
        bodies; compose tasks with a workflow there.

        Args:
            wait_ms: Maximum time to wait for result (milliseconds)

        Returns:
            ReadResult[T, TaskError[C]], as ``get()``.
        """
        before = self._before_read('get_async', wait_ms)
        if before is not None:
            return before
        assert self._app is not None, 'a broker-backed handle reads through its app'
        broker_result = await self._app.get_broker().get_raw_result_record_async(
            self.task_id, wait_ms,
        )
        return self._kept(
            read_of_record(
                self.task_id, broker_result, self.ok_type, wait_ms, self.code_type,
            ),
        )

    def info(
        self,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> 'DatabaseResult[TaskInfo | None]':
        """Fetch metadata for this task from the broker.

        Returns Ok(TaskInfo) if found, Ok(None) if not found,
        Err(DatabaseOperationError) on infrastructure or precondition failure,
        and Err(ASYNC_CONTEXT) on a thread that runs an event loop.
        """
        if not self._broker_mode or not self._app:
            return Err(DatabaseNotAttached(
                message=(
                    'TaskHandle.info() requires a broker-backed task handle '
                    '(use .send() or .send_async())'
                ),
            ))
        if in_running_event_loop():
            return Err(DatabaseAsyncContext(
                message=async_context_message(
                    f'TaskHandle.info() for task {self.task_id}',
                    'handle.info_async(...)',
                ),
            ))

        # Strict-serde phase 6: route through the app-level typed
        # decode so ``include_result=True`` populates
        # ``decoded_result`` / ``result_decoded`` whenever the task is
        # locally registered. The broker-level ``get_task_info`` returns
        # raw_result only.
        return self._app.get_task_info(
            self.task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
            include_attempts=include_attempts,
        )

    async def info_async(
        self,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> 'DatabaseResult[TaskInfo | None]':
        """Async version of info().

        Returns Ok(TaskInfo) if found, Ok(None) if not found,
        Err(DatabaseOperationError) on infrastructure or precondition failure.
        """
        if not self._broker_mode or not self._app:
            return Err(DatabaseNotAttached(
                message=(
                    'TaskHandle.info_async() requires a broker-backed task handle '
                    '(use .send() or .send_async())'
                ),
            ))

        # Strict-serde phase 6: same routing as the sync variant.
        return await self._app.get_task_info_async(
            self.task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
            include_attempts=include_attempts,
        )

    def set_immediate_result(
        self,
        result: Result[T, TaskError[C]],
    ) -> None:
        """Internal method to set the outcome of a synchronous execution."""
        match result:
            case Ok(ok_value=value):
                self._cached_result = Ok(Succeeded(value))
            case Err(err_value=error):
                self._cached_result = Ok(Failed(error))


class NodeFactory(Generic[P, T, C]):
    """
    Factory for creating TaskNode instances with typed arguments.

    Returned by TaskFunction.node(). Call with the task's arguments
    to create a TaskNode with full static type checking.

    Example:
        node = my_task.node(waits_for=[dep])(value='test')
        # Type checker validates 'value' against my_task's signature
    """

    _function: 'TaskFunction[P, T, C] | AsyncTaskFunction[P, T, C] | RuntimeTaskFunction[P, T, C] | AsyncRuntimeTaskFunction[P, T, C]'
    _waits_for: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None
    _workflow_context_from: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None
    _args_from: dict[str, 'TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None
    _queue: str | None
    _priority: int | None
    _allow_failed_dependencies: bool
    _join: Literal['all', 'any', 'quorum']
    _min_success: int | None
    _good_until: datetime | None
    _node_id: str | None

    def __init__(
        self,
        function: 'TaskFunction[P, T, C] | AsyncTaskFunction[P, T, C] | RuntimeTaskFunction[P, T, C] | AsyncRuntimeTaskFunction[P, T, C]',
        *,
        waits_for: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None,
        workflow_context_from: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None,
        args_from: dict[str, 'TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None,
        queue: str | None,
        priority: int | None,
        allow_failed_dependencies: bool,
        join: Literal['all', 'any', 'quorum'],
        min_success: int | None,
        good_until: datetime | None,
        node_id: str | None,
    ) -> None:
        self._function = function
        self._waits_for = waits_for
        self._workflow_context_from = workflow_context_from
        self._args_from = args_from
        self._queue = queue
        self._priority = priority
        self._allow_failed_dependencies = allow_failed_dependencies
        self._join = join
        self._min_success = min_success
        self._good_until = good_until
        self._node_id = node_id

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> 'TaskNode[T, C]':
        from horsies.core.models.workflow import TaskNode

        # D.1: Reject positional args — workflow nodes are kwargs-only
        if args:
            raise WorkflowValidationError(
                message='positional arguments not allowed in .node()() calls',
                code=HorsiesErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED,
                notes=[
                    f"task '{self._function.task_name}' received {len(args)} positional arg(s)",
                    'workflow nodes use kwargs-only; pass all arguments by keyword',
                ],
                help_text='change .node()(val1, val2) to .node()(param1=val1, param2=val2)',
            )

        # C.1: Scan kwargs for FromNodeMarker instances
        marker_args_from: dict[str, 'TaskNode[Any, Any] | SubWorkflowNode[Any]'] = {}
        static_kwargs: dict[str, Any] = {}

        for key, value in kwargs.items():
            if isinstance(value, FromNodeMarker):
                marker_args_from[key] = value.node
            else:
                static_kwargs[key] = value

        # Merge explicit args_from with marker-derived args_from
        explicit_args_from = dict(self._args_from) if self._args_from else {}

        # Detect overlap: marker key vs explicit args_from key
        marker_explicit_overlap = set(marker_args_from.keys()) & set(explicit_args_from.keys())
        if marker_explicit_overlap:
            raise WorkflowValidationError(
                message='from_node() marker conflicts with explicit args_from',
                code=HorsiesErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP,
                notes=[
                    f"task '{self._function.task_name}' has key(s) in both from_node() "
                    f"and args_from: {sorted(marker_explicit_overlap)}",
                    'each injected key must come from exactly one source',
                ],
                help_text='remove the key from either from_node() kwargs or the explicit args_from dict',
            )

        merged_args_from = {**explicit_args_from, **marker_args_from}

        # Auto-wire waits_for from markers
        waits_for = list(self._waits_for) if self._waits_for else []
        existing_deps = set(id(dep) for dep in waits_for)
        for upstream in marker_args_from.values():
            if id(upstream) not in existing_deps:
                waits_for.append(upstream)
                existing_deps.add(id(upstream))

        return TaskNode(
            function=self._function,
            kwargs=static_kwargs,
            waits_for=waits_for,
            workflow_context_from=list(self._workflow_context_from)
            if self._workflow_context_from
            else None,
            args_from=merged_args_from,
            queue=self._queue,
            priority=self._priority,
            allow_failed_dependencies=self._allow_failed_dependencies,
            join=self._join,
            min_success=self._min_success,
            good_until=self._good_until,
            node_id=self._node_id,
        )


class _TaskFunctionMembers(Protocol[P, T, C]):
    """The members that a `def` task and an `async def` task share.

    `TaskFunction` and `AsyncTaskFunction` add the direct call. The generic
    parameter T represents the success type in Result[T, TaskError[C]].

    The sends and the two resend methods raise
    `ForkedAfterCoreStart` (this process was forked without `exec` after
    the core started) and `CoreStopped` (a core background task panicked).
    These are states of the process: nothing was sent, and no send result
    names them.
    """

    task_name: str
    queue: str | None
    """The queue that the task declares (`queue=`), or None for the default
    queue."""
    task_options_json: str | None
    error_codes: tuple[str, ...]
    """The custom error codes that the task declares (`error_codes=`), in the
    order of the declaration; `()` when it declares none."""
    task_code_type: CustomCodeType
    """The type of the custom codes: `EnumCodes(E)` for a task that returns
    `Result[T, TaskError[E]]`, else `TextCodes()`."""

    @abstractmethod
    def send(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult['TaskHandle[T, C]']: ...

    @abstractmethod
    async def send_async(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult['TaskHandle[T, C]']: ...

    @abstractmethod
    def with_options(
        self,
        *,
        good_until: datetime | None = None,
        idempotency_key: str | None = None,
        retention: SendRetention = _UNSET_RETENTION,
        delay_seconds: int | None = None,
    ) -> 'TaskFunctionWithOptions[P, T, C]': ...

    @abstractmethod
    def resend(
        self,
        error: TaskSendResendable,
    ) -> TaskSendResult['TaskHandle[T, C]']: ...

    @abstractmethod
    async def resend_async(
        self,
        error: TaskSendResendable,
    ) -> TaskSendResult['TaskHandle[T, C]']: ...

    @abstractmethod
    def node(
        self,
        *,
        waits_for: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None = None,
        workflow_context_from: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None = None,
        args_from: dict[str, 'TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None = None,
        queue: str | None = None,
        priority: int | None = None,
        allow_failed_dependencies: bool = False,
        join: Literal['all', 'any', 'quorum'] = 'all',
        min_success: int | None = None,
        good_until: datetime | None = None,
        node_id: str | None = None,
    ) -> 'NodeFactory[P, T, C]': ...


class TaskFunction(_TaskFunctionMembers[P, T, C], Protocol[P, T, C]):
    """
    A TaskFunction is a `def` function that gets a @task decorator applied to it.
    Protocol extends the simple function signature to include the send and send_async methods,
    thus being able to be called with or without the @task decorator.

    The generic parameter T represents the success type in Result[T, TaskError[C]].
    """

    @abstractmethod
    def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Result[T, TaskError[C]]: ...


class AsyncTaskFunction(_TaskFunctionMembers[P, T, C], Protocol[P, T, C]):
    """An `async def` function that gets a @task decorator applied to it.

    A direct call returns a coroutine. Await it to get the `Result`.
    The send methods and `node()` are the same as on `TaskFunction`.
    """

    @abstractmethod
    def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Coroutine[Any, Any, Result[T, TaskError[C]]]: ...


class RuntimeTaskFunction(_TaskFunctionMembers[P, T, C], Protocol[P, T, C]):
    """A `def` task function whose first parameter takes its `TaskRuntime`.

    `P` is the parameters after the runtime: the sends and `node()` take
    these, and the worker passes the runtime on each run. A
    direct call takes the runtime first, for example
    `charge(TaskRuntime.for_test(task_id=...), order_id='o-1')`.
    """

    @abstractmethod
    def __call__(
        self,
        runtime: TaskRuntime,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Result[T, TaskError[C]]: ...


class AsyncRuntimeTaskFunction(_TaskFunctionMembers[P, T, C], Protocol[P, T, C]):
    """An `async def` task function whose first parameter takes its
    `TaskRuntime`.

    As `RuntimeTaskFunction`; a direct call returns a coroutine.
    """

    @abstractmethod
    def __call__(
        self,
        runtime: TaskRuntime,
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Coroutine[Any, Any, Result[T, TaskError[C]]]: ...


# The runtime overloads come first. A function whose first parameter takes a
# `TaskRuntime` also fits the overloads after them, with the runtime in `P`;
# the first match wins, so the sends of such a task do not take the runtime.
# A type checker reports that overlap; it is the intended order.


class TaskDecorator(Protocol):
    """What `app.task(task_name, **options)` returns.

    It registers one task function: a `def` function gives a
    `TaskFunction`, and an `async def` function gives an `AsyncTaskFunction`.
    A function whose first parameter is `runtime: TaskRuntime` gives a
    `RuntimeTaskFunction` or an `AsyncRuntimeTaskFunction`.
    """

    @overload
    def __call__(  # pyright: ignore[reportOverlappingOverload]
        self,
        function: Callable[Concatenate[TaskRuntime, P], Coroutine[Any, Any, Result[T, TaskError[C]]]],
        /,
    ) -> AsyncRuntimeTaskFunction[P, T, C]: ...

    @overload
    def __call__(  # pyright: ignore[reportOverlappingOverload]
        self,
        function: Callable[Concatenate[TaskRuntime, P], Result[T, TaskError[C]]],
        /,
    ) -> RuntimeTaskFunction[P, T, C]: ...

    @overload
    def __call__(
        self,
        function: Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]],
        /,
    ) -> AsyncTaskFunction[P, T, C]: ...

    @overload
    def __call__(
        self,
        function: Callable[P, Result[T, TaskError[C]]],
        /,
    ) -> TaskFunction[P, T, C]: ...


class SharedAsyncTaskDecorator(Protocol):
    """What `app.task(task_name, execution=Execution.SHARED_ASYNC, ...)` returns.

    It accepts only an `async def` function, so a type checker refuses a
    `def` function. The decorator refuses it at run time too (HRS-107).
    """

    @overload
    def __call__(  # pyright: ignore[reportOverlappingOverload]
        self,
        function: Callable[Concatenate[TaskRuntime, P], Coroutine[Any, Any, Result[T, TaskError[C]]]],
        /,
    ) -> AsyncRuntimeTaskFunction[P, T, C]: ...

    @overload
    def __call__(
        self,
        function: Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]],
        /,
    ) -> AsyncTaskFunction[P, T, C]: ...


class SharedThreadTaskDecorator(Protocol):
    """What `app.task(task_name, execution=Execution.SHARED_THREAD, ...)` returns.

    It accepts only a `def` function, so a type checker refuses an
    `async def` function. The decorator refuses it at run time too (HRS-108).
    """

    @overload
    def __call__(  # pyright: ignore[reportOverlappingOverload]
        self,
        function: Callable[Concatenate[TaskRuntime, P], Result[T, TaskError[C]]],
        /,
    ) -> RuntimeTaskFunction[P, T, C]: ...

    @overload
    def __call__(
        self,
        function: Callable[P, Result[T, TaskError[C]]],
        /,
    ) -> TaskFunction[P, T, C]: ...


class CancelGraceRefusedDecorator(Protocol):
    """What `app.task(task_name, cancel_grace_ms=..., ...)` returns for the
    literal class `SHARED_THREAD`.

    It accepts no function, so a type checker refuses the decoration. The
    decorator refuses it at run time too (HRS-110).
    """

    def __call__(self, function: Never, /) -> Never: ...


class TaskFunctionWithOptions(Protocol[P, T, C]):
    """The sends of one task with per-send options, from `with_options()`.

    A send with a delay is due after `delay_seconds`; a send without one
    is due now. For workflow tasks, use .node(good_until=...)(...) instead.
    """

    @abstractmethod
    def send(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult['TaskHandle[T, C]']: ...

    @abstractmethod
    async def send_async(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult['TaskHandle[T, C]']: ...


# ---- Broker-to-TaskSendError mapping ----

def _map_broker_err(
    broker_err: DatabaseEnqueueError | DatabaseSyncCallError,
    task_id: UUID,
    payload: TaskSendPayload,
) -> _BrokerSendError:
    """Map a broker error variant to its send variant."""
    match broker_err:
        case DatabasePayloadMismatch(message=message):
            return TaskSendPayloadMismatch(
                task_id=task_id,
                payload=payload,
                message=message,
            )
        case DatabaseIdempotencyKeyConflict(
            message=message,
            owner_task_id=owner_task_id,
        ):
            return TaskSendIdempotencyKeyConflict(
                task_id=task_id,
                payload=payload,
                message=message,
                owner_task_id=owner_task_id,
            )
        case DatabaseAsyncContext(message=message):
            return TaskSendAsyncContext(
                task_id=task_id,
                payload=payload,
                message=message,
            )
        case (
            DatabaseInvalidTaskOptions(message=message, cause=exception)
            | DatabaseInvalidInput(message=message, cause=exception)
        ):
            # Refused before the write: resending the same payload is
            # refused again, so the resend methods must not accept it.
            return TaskSendValidationFailed(
                message=message,
                cause=exception,
            )
        case DatabaseEnqueueFailed(
            message=message,
            retryable=retryable,
            cause=exception,
        ):
            return TaskSendEnqueueFailed(
                task_id=task_id,
                payload=payload,
                message=message,
                retryable=retryable,
                cause=exception,
            )
        case _ as unreachable:
            assert_never(unreachable)


def _returned_type_name(returned: object) -> str:
    """The type of a wrong return, for the refusal text.

    A copy of `Ok` or `Err` from a second import path gets its module, so that
    the text does not read 'returned Ok, expected Result'.
    """
    if returned is None:
        return 'None'
    returned_type = type(returned)
    if returned_type.__name__ in (Ok.__name__, Err.__name__):
        return f'{returned_type.__module__}.{returned_type.__qualname__}'
    return returned_type.__name__


class TaskBodyKind(Enum):
    """How a task function was defined, which decides how a worker runs it.

    The decorator sets it as ``task_body_kind`` on the class of the task
    function. A worker child reads it from the registry at boot, and creates
    its event loop only when the app has an ``async def`` task.
    """

    DEF = 'def'
    ASYNC_DEF = 'async_def'


@dataclass(frozen=True, slots=True)
class _SyncTaskBody(Generic[P, T, C]):
    """A task function defined with `def`."""

    function: Callable[P, Result[T, TaskError[C]]]


@dataclass(frozen=True, slots=True)
class _AsyncTaskBody(Generic[P, T, C]):
    """A task function defined with `async def`."""

    function: Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]]


def _task_body(
    function: Callable[P, Result[T, TaskError[C]]]
    | Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]],
) -> _SyncTaskBody[P, T, C] | _AsyncTaskBody[P, T, C]:
    """Sort a task function by the way it is called.

    `create_task_wrapper` refuses generator and async generator functions
    before this, so a function that is not a coroutine function is a `def`
    function that returns its result.
    """
    if inspect.iscoroutinefunction(function):
        return _AsyncTaskBody(
            cast('Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]]', function),
        )
    return _SyncTaskBody(cast('Callable[P, Result[T, TaskError[C]]]', function))


def _task_body_kind(
    function: Callable[P, Result[T, TaskError[C]]]
    | Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]],
) -> TaskBodyKind:
    """The kind of a task function, as `_task_body` sorts it."""
    match _task_body(function):
        case _SyncTaskBody():
            return TaskBodyKind.DEF
        case _AsyncTaskBody():
            return TaskBodyKind.ASYNC_DEF
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class SharedAsyncOnDef:
    """`execution=SHARED_ASYNC` on a function defined with `def`."""


@dataclass(frozen=True, slots=True)
class SharedThreadOnAsyncDef:
    """`execution=SHARED_THREAD` on a function defined with `async def`."""


@dataclass(frozen=True, slots=True)
class SharedWithoutTimeout:
    """A shared class without `timeout_ms`."""

    execution: Execution


@dataclass(frozen=True, slots=True)
class CancelGraceOnSharedThread:
    """`cancel_grace_ms` with `SHARED_THREAD`: a thread cannot be stopped,
    so the class has no cancel grace."""


@dataclass(frozen=True, slots=True)
class CancelGraceOutOfRange:
    """`cancel_grace_ms` outside MIN_CANCEL_GRACE_MS to MAX_CANCEL_GRACE_MS,
    for `execution` (`SHARED_ASYNC` or `ISOLATED`)."""

    execution: Literal[Execution.SHARED_ASYNC, Execution.ISOLATED]


type ExecutionRuleViolation = (
    SharedAsyncOnDef
    | SharedThreadOnAsyncDef
    | SharedWithoutTimeout
    | CancelGraceOnSharedThread
    | CancelGraceOutOfRange
)
"""A task declaration that breaks one of the execution rules."""


def check_execution_rules(
    kind: TaskBodyKind,
    options: TaskOptions,
) -> Result[None, ExecutionRuleViolation]:
    """Check the execution declaration of one task against its function.

    The rules, checked in this order, give the first violation:
    `SHARED_ASYNC` needs an `async def` function and `SHARED_THREAD` a `def`
    function; a shared class needs `timeout_ms`; `SHARED_THREAD` takes no
    `cancel_grace_ms`; and a `cancel_grace_ms` of `SHARED_ASYNC` or
    `ISOLATED` is from MIN_CANCEL_GRACE_MS to MAX_CANCEL_GRACE_MS.
    `ISOLATED` takes both kinds, no timeout, and a grace or none (the hard
    stop after an operator cancel). Pure: it does not read or change any
    state.
    """
    match options.execution:
        case Execution.ISOLATED:
            pass
        case Execution.SHARED_ASYNC | Execution.SHARED_THREAD:
            kind_r = _check_function_kind(options.execution, kind)
            if is_err(kind_r):
                return kind_r
            if options.timeout_ms is None:
                return Err(SharedWithoutTimeout(execution=options.execution))
        case _ as unreachable:
            assert_never(unreachable)
    return _check_cancel_grace(options.execution, options.cancel_grace_ms)


def _check_function_kind(
    execution: Literal[Execution.SHARED_ASYNC, Execution.SHARED_THREAD],
    kind: TaskBodyKind,
) -> Result[None, SharedAsyncOnDef | SharedThreadOnAsyncDef]:
    """A shared class runs only the function kind that it is for."""
    match execution, kind:
        case Execution.SHARED_ASYNC, TaskBodyKind.DEF:
            return Err(SharedAsyncOnDef())
        case Execution.SHARED_THREAD, TaskBodyKind.ASYNC_DEF:
            return Err(SharedThreadOnAsyncDef())
        case _:
            return Ok(None)


def _check_cancel_grace(
    execution: Execution,
    cancel_grace_ms: int | None,
) -> Result[None, CancelGraceOnSharedThread | CancelGraceOutOfRange]:
    """`SHARED_THREAD` takes no grace; the grace of `SHARED_ASYNC` and of
    `ISOLATED` is in its bounds."""
    match cancel_grace_ms:
        case None:
            return Ok(None)
        case int():
            pass
    match execution:
        case Execution.SHARED_ASYNC | Execution.ISOLATED:
            if MIN_CANCEL_GRACE_MS <= cancel_grace_ms <= MAX_CANCEL_GRACE_MS:
                return Ok(None)
            return Err(CancelGraceOutOfRange(execution=execution))
        case Execution.SHARED_THREAD:
            return Err(CancelGraceOnSharedThread())
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class _RuleText:
    """The code and the text of one execution rule error."""

    code: HorsiesErrorCode
    message: str
    notes: tuple[str, ...]
    help_text: str


def execution_rule_error(
    violation: ExecutionRuleViolation,
    *,
    task_name: str,
    location: SourceLocation | None,
) -> TaskDefinitionError:
    """The definition error, with its own code, for one rule violation.

    The text names the rule and the fix, never the declared grace: an int
    with more than 4300 digits cannot be converted to a string.
    """
    match violation:
        case SharedAsyncOnDef() | SharedThreadOnAsyncDef() | SharedWithoutTimeout():
            rule = _function_rule_text(violation)
        case CancelGraceOnSharedThread() | CancelGraceOutOfRange():
            rule = _grace_rule_text(violation)
        case _ as unreachable:
            assert_never(unreachable)
    return TaskDefinitionError(
        message=rule.message,
        code=rule.code,
        location=location,
        notes=[f"task '{task_name}'", *rule.notes],
        help_text=rule.help_text,
    )


def _function_rule_text(
    violation: SharedAsyncOnDef | SharedThreadOnAsyncDef | SharedWithoutTimeout,
) -> _RuleText:
    """The text of a rule about the function kind or the timeout."""
    match violation:
        case SharedAsyncOnDef():
            return _RuleText(
                code=HorsiesErrorCode.TASK_SHARED_ASYNC_REQUIRES_ASYNC_DEF,
                message='execution=SHARED_ASYNC requires an `async def` function',
                notes=(
                    'the function is defined with `def`',
                    'SHARED_ASYNC runs many tasks as coroutines in one child process',
                ),
                help_text=(
                    'define the function with `async def`, or use '
                    'Execution.SHARED_THREAD for a `def` function'
                ),
            )
        case SharedThreadOnAsyncDef():
            return _RuleText(
                code=HorsiesErrorCode.TASK_SHARED_THREAD_REQUIRES_DEF,
                message='execution=SHARED_THREAD requires a `def` function',
                notes=(
                    'the function is defined with `async def`',
                    'SHARED_THREAD runs many tasks on threads in one child process',
                ),
                help_text=(
                    'define the function with `def`, or use '
                    'Execution.SHARED_ASYNC for an `async def` function'
                ),
            )
        case SharedWithoutTimeout(execution=execution):
            return _RuleText(
                code=HorsiesErrorCode.TASK_SHARED_EXECUTION_REQUIRES_TIMEOUT,
                message=f'execution={execution.value} requires timeout_ms',
                notes=('a shared task runs in one child process with other tasks',),
                help_text=(
                    'set timeout_ms, for example @app.task(..., '
                    f'execution=Execution.{execution.value}, timeout_ms=30_000)'
                ),
            )
        case _ as unreachable:
            assert_never(unreachable)


def _grace_rule_text(
    violation: CancelGraceOnSharedThread | CancelGraceOutOfRange,
) -> _RuleText:
    """The text of a rule about `cancel_grace_ms`."""
    match violation:
        case CancelGraceOnSharedThread():
            return _RuleText(
                code=HorsiesErrorCode.TASK_CANCEL_GRACE_REQUIRES_SHARED_ASYNC,
                message='cancel_grace_ms is not valid with execution=SHARED_THREAD',
                notes=(
                    'execution is SHARED_THREAD',
                    'a thread cannot be cancelled or stopped, so a SHARED_THREAD '
                    'task has no cancel grace',
                ),
                help_text=(
                    'remove cancel_grace_ms, or use Execution.SHARED_ASYNC with an '
                    '`async def` function, or Execution.ISOLATED'
                ),
            )
        case CancelGraceOutOfRange(execution=execution):
            return _RuleText(
                code=HorsiesErrorCode.TASK_CANCEL_GRACE_OUT_OF_RANGE,
                message='cancel_grace_ms is out of range',
                notes=(
                    f'cancel_grace_ms must be from {MIN_CANCEL_GRACE_MS} to '
                    f'{MAX_CANCEL_GRACE_MS} ms',
                    f'execution is {execution.value}',
                ),
                help_text=_grace_range_help(execution),
            )
        case _ as unreachable:
            assert_never(unreachable)


def _grace_range_help(
    execution: Literal[Execution.SHARED_ASYNC, Execution.ISOLATED],
) -> str:
    """The fix of a grace out of range: the default of the class when the
    grace is removed."""
    match execution:
        case Execution.SHARED_ASYNC:
            return (
                'set a value in the range, or remove cancel_grace_ms to '
                f'use the default of {DEFAULT_CANCEL_GRACE_MS} ms'
            )
        case Execution.ISOLATED:
            return (
                'set a value in the range, or remove cancel_grace_ms: an '
                'ISOLATED task then has no hard stop after an operator cancel'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _sync_task_call(
    body: Callable[P, Result[T, TaskError[C]]],
    checked_result: Callable[[object], Result[T, TaskError[C]]],
    exception_result: Callable[[BaseException], Result[T, TaskError[C]]],
) -> Callable[P, Result[T, TaskError[C]]]:
    """The direct call of a `def` task: run the body, then check its return.

    An exception from the body or from the check becomes an error result.
    `KeyboardInterrupt` propagates, for worker shutdown.
    """

    def call(*args: P.args, **kwargs: P.kwargs) -> Result[T, TaskError[C]]:
        try:
            return checked_result(body(*args, **kwargs))
        except KeyboardInterrupt:
            raise
        except BaseException as exc:
            return exception_result(exc)

    return call


def _async_task_call(
    body: Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]],
    checked_result: Callable[[object], Result[T, TaskError[C]]],
    exception_result: Callable[[BaseException], Result[T, TaskError[C]]],
) -> Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]]:
    """The direct call of an `async def` task: a coroutine that awaits the
    body, then checks its return.

    Exceptions become error results as in `_sync_task_call`. A
    `CancelledError` propagates when the asyncio task that awaits this
    coroutine has a cancel request, so a timeout or a task group around the
    call works. A `CancelledError` with no cancel request, for example from
    an inner task that was cancelled, becomes an error result.
    `GeneratorExit` propagates, so `close()` on a suspended call ends it.
    """

    async def call(*args: P.args, **kwargs: P.kwargs) -> Result[T, TaskError[C]]:
        try:
            return checked_result(await body(*args, **kwargs))
        except (KeyboardInterrupt, GeneratorExit):
            raise
        except asyncio.CancelledError as exc:
            if _cancel_requested():
                raise
            return exception_result(exc)
        except BaseException as exc:
            return exception_result(exc)

    return call


async def await_task_call_in_worker(
    called: Coroutine[Any, Any, Result[T, TaskError[C]]],
    map_exception: Callable[[BaseException], Result[T, TaskError[C]]],
) -> Result[T, TaskError[C]]:
    """Await the direct call of an `async def` task inside a worker child.

    `called` is the coroutine of the direct call, and `map_exception` the
    exception mapping of the same task (`_map_exception` of the task
    function). The direct call maps every exception of the body, but lets a
    `CancelledError` pass while the awaiting asyncio task has a cancel
    request. In a worker child that request can come from the body itself:
    a body that cancels its own task. So the worker maps that
    `CancelledError` through the exception mappers as well. A cancel that
    the worker sends at a deadline is mapped the same way here; the worker
    then drops the result and records `TASK_TIMEOUT`.

    A `GeneratorExit` that leaves the body goes through the exception
    mappers too, as it does for a `def` body. The direct call lets it pass,
    so that `close()` on a suspended call ends it. A `close()` of this
    coroutine still ends it: the mapped result returns into `close()`,
    which drops it, and no code runs after it. `KeyboardInterrupt`
    propagates. The direct call outside a worker does not change.
    """
    try:
        return await called
    except (asyncio.CancelledError, GeneratorExit) as exc:
        return map_exception(exc)


def _cancel_requested() -> bool:
    """True when the current asyncio task has a cancel request."""
    match asyncio.current_task():
        case None:
            return False
        case task:
            return task.cancelling() > 0


def _declared_code_type(
    err_type: object,
    code_enum: type[Enum] | None,
    *,
    mapper_codes: tuple[str, ...],
    task_name: str,
    location: SourceLocation | None,
) -> CustomCodeType:
    """The custom code type of a task, from the error type of its return
    annotation and the `Enum` class of its `error_codes=`.

    `TaskError` gives `TextCodes()`, with any declaration. `TaskError[E]`
    gives `EnumCodes(E)` and needs `error_codes=E`, and every custom code
    of its exception mappers and defaults (`mapper_codes`) must be the value
    of a member of `E`.

    Raises:
        TaskDefinitionError: `HRS-105` for `TaskError[E]` without
            `error_codes=E`; `HRS-209` for a mapper code that `E` does not
            name. `check_task_signature` refused any other error type before.
    """
    match custom_code_type_of(err_type):
        case NotATaskErrorType():
            raise AssertionError(
                f'task {task_name!r}: check_task_signature accepted the error type {err_type!r}',
            )
        case TextCodes() as text_codes:
            return text_codes
        case EnumCodes(code_type=enum_class) as enum_codes:
            pass
    if code_enum is not enum_class:
        raise TaskDefinitionError(
            message=f'TaskError[{enum_class.__name__}] needs error_codes={enum_class.__name__}',
            code=HorsiesErrorCode.TASK_STRICT_SIGNATURE_VIOLATION,
            location=location,
            notes=[
                f"task '{task_name}' returns Result[..., TaskError[{enum_class.__name__}]]",
                'the custom codes of the error are the members of the Enum that the task declares',
            ],
            help_text=f'declare the codes: @app.task(..., error_codes={enum_class.__name__})',
        )
    values = {member.value for member in enum_class}
    outside = [code for code in mapper_codes if code not in values]
    if outside:
        raise TaskDefinitionError(
            message=f'an exception mapper code is not a member of {enum_class.__name__}',
            code=HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
            location=location,
            notes=[
                f"task '{task_name}' returns Result[..., TaskError[{enum_class.__name__}]]",
                f'codes that {enum_class.__name__} does not name: {sorted(outside)}',
            ],
            help_text=(
                f'add the codes to {enum_class.__name__}, or map the exceptions '
                'to codes that it names'
            ),
        )
    return enum_codes


def _mapper_code_texts(
    task_mapper: ExceptionMapper | None,
    app_mapper: ExceptionMapper,
    task_default: str | None,
    app_default: str | None,
) -> tuple[str, ...]:
    """The custom codes that the exception mapping of a task can give: the
    values of both mappers and both defaults that are not built-in codes.
    Pure."""
    from horsies.core.models.tasks import BUILTIN_CODE_REGISTRY

    task_values = tuple(task_mapper.values()) if task_mapper is not None else ()
    candidates = (*task_values, *app_mapper.values(), task_default, app_default)
    return tuple(
        code for code in candidates
        if code is not None and code not in BUILTIN_CODE_REGISTRY
    )


def custom_code_refusal(error: object, code_type: CustomCodeType) -> str | None:
    """Why the custom code of a returned error does not fit the task's code
    type, or None when it fits. Pure.

    With `EnumCodes(E)`, the code is a built-in code or a member of `E`; a
    text, also one with the value of a member, does not fit. With
    `TextCodes()`, every code that `TaskError` accepts fits.
    """
    match error, code_type:
        case TaskError(), TextCodes():
            return None
        case TaskError(code=code), EnumCodes(code_type=enum_class):
            if isinstance(code, BUILTIN_FAMILIES) or isinstance(code, enum_class):
                return None
            return (
                f'code must be a member of {enum_class.__name__} or a '
                f'built-in code; got {type(code).__name__}'
            )
        case _:
            return None


def mapped_code(
    code: BuiltInTaskCode | str, code_type: CustomCodeType,
) -> BuiltInTaskCode | str | Enum:
    """The code of a mapped exception in the task's code type: a built-in
    code as it is; a custom text as it is with `TextCodes()`, and as its
    member with `EnumCodes(E)`. Pure.

    Raises:
        AssertionError: the text is not the value of a member of `E`;
            `_declared_code_type` refused such a mapper when the task was
            defined.
    """
    match code, code_type:
        case str(), EnumCodes(code_type=enum_class) if not isinstance(code, BUILTIN_FAMILIES):
            members = {member.value: member for member in enum_class}
            member = members.get(code)
            assert member is not None, (
                f'the mapped code {code!r} is not a member of {enum_class.__name__}'
            )
            return member
        case _:
            return code


@overload
def create_task_wrapper(  # pyright: ignore[reportOverlappingOverload]
    function: Callable[Concatenate[TaskRuntime, P], Coroutine[Any, Any, Result[T, TaskError[C]]]],
    app: 'Horsies',
    task_name: str,
    task_options: Optional['TaskOptions'] = None,
    *,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str | None = None,
    error_codes: tuple[str, ...] = (),
    code_enum: type[Enum] | None = None,
) -> 'AsyncRuntimeTaskFunction[P, T, C]': ...


@overload
def create_task_wrapper(  # pyright: ignore[reportOverlappingOverload]
    function: Callable[Concatenate[TaskRuntime, P], Result[T, TaskError[C]]],
    app: 'Horsies',
    task_name: str,
    task_options: Optional['TaskOptions'] = None,
    *,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str | None = None,
    error_codes: tuple[str, ...] = (),
    code_enum: type[Enum] | None = None,
) -> 'RuntimeTaskFunction[P, T, C]': ...


@overload
def create_task_wrapper(
    function: Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]],
    app: 'Horsies',
    task_name: str,
    task_options: Optional['TaskOptions'] = None,
    *,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str | None = None,
    error_codes: tuple[str, ...] = (),
    code_enum: type[Enum] | None = None,
) -> 'AsyncTaskFunction[P, T, C]': ...


@overload
def create_task_wrapper(
    function: Callable[P, Result[T, TaskError[C]]],
    app: 'Horsies',
    task_name: str,
    task_options: Optional['TaskOptions'] = None,
    *,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str | None = None,
    error_codes: tuple[str, ...] = (),
    code_enum: type[Enum] | None = None,
) -> 'TaskFunction[P, T, C]': ...


def create_task_wrapper(
    function: Callable[P, Result[T, TaskError[C]]]
    | Callable[P, Coroutine[Any, Any, Result[T, TaskError[C]]]],
    app: 'Horsies',
    task_name: str,
    task_options: Optional['TaskOptions'] = None,
    *,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str | None = None,
    error_codes: tuple[str, ...] = (),
    code_enum: type[Enum] | None = None,
) -> 'TaskFunction[P, T, C] | AsyncTaskFunction[P, T, C] | RuntimeTaskFunction[Any, T, C] | AsyncRuntimeTaskFunction[Any, T, C]':
    """
    Create a task wrapper for a specific app instance.
    Called by app.task() decorator.

    A `def` function gives a `TaskFunction`. An `async def` function gives
    an `AsyncTaskFunction`, whose direct call returns a coroutine. A
    function whose first parameter takes the `TaskRuntime` gives the runtime
    form of each (`RuntimeTaskFunction`, `AsyncRuntimeTaskFunction`): the
    same wrapper, whose sends do not take the runtime.

    `error_codes` are the codes that the task declares, which the decorator
    checked (`parse_declared_error_codes`), with the codes of its exception
    mappers (`with_mapper_codes`); the wrapper keeps them as
    `error_codes`. `code_enum` is the `Enum` class of `error_codes=`,
    or None for a tuple or no declaration. A return annotation
    `Result[T, TaskError[E]]` needs `code_enum` to be `E`; the wrapper keeps
    the code type as `task_code_type`.
    """

    function_location = SourceLocation.from_function(function)

    # Strict contract: @app.task() must decorate the raw task function directly.
    # Reject pre-decorated wrappers to keep signature/type contract deterministic.
    unwrapped = inspect.unwrap(function)
    if unwrapped is not function:
        raise TaskDefinitionError(
            message='task function must not be pre-decorated',
            code=HorsiesErrorCode.TASK_PREDECORATED_NOT_SUPPORTED,
            location=function_location,
            notes=[
                f"task '{task_name}' received a callable with __wrapped__ chain",
                'inner decorators are not supported for @app.task()',
            ],
            help_text='remove inner decorators and apply @app.task() directly to the raw function',
        )

    # An async generator function returns an async generator, so the body
    # never runs and every run would fail.
    if inspect.isasyncgenfunction(function):
        raise TaskDefinitionError(
            message='task function must not be an async generator',
            code=HorsiesErrorCode.TASK_INVALID_RETURN_TYPE,
            location=function_location,
            notes=[
                f"function '{function.__name__}' is defined with `async def` and uses `yield`",
                'a task returns one Result',
            ],
            help_text='collect the items and return them in one Ok(...)',
        )

    # A generator function returns a generator, so the body never runs and
    # every run would fail.
    if inspect.isgeneratorfunction(function):
        raise TaskDefinitionError(
            message='task function must not be a generator',
            code=HorsiesErrorCode.TASK_INVALID_RETURN_TYPE,
            location=function_location,
            notes=[
                f"function '{function.__name__}' uses `yield`",
                'a task returns one Result',
            ],
            help_text='collect the items and return them in one Ok(...)',
        )

    # The execution class must fit the function kind, which is known only
    # now that generator functions are refused.
    match task_options:
        case None:
            pass
        case declared_options:
            rules_r = check_execution_rules(_task_body_kind(function), declared_options)
            if is_err(rules_r):
                raise execution_rule_error(
                    rules_r.err_value,
                    task_name=task_name,
                    location=function_location,
                )

    hints = get_type_hints(function, include_extras=True)

    # Validate that return type is Result[*, TaskError]
    return_hint = hints.get('return')
    if return_hint is None:
        raise TaskDefinitionError(
            message='task function must declare an explicit return type',
            code=HorsiesErrorCode.TASK_NO_RETURN_TYPE,
            location=function_location,
            notes=[f"function '{function.__name__}' has no return type annotation"],
            help_text='add return type annotation: `-> Result[YourType, TaskError]`',
        )

    # Extract T and E from Result[T, E] (the union Ok[T] | Err[E]) for
    # runtime validation
    match result_type_arguments(return_hint):
        case NotAResultAnnotation():
            raise TaskDefinitionError(
                message='task function must return Result',
                code=HorsiesErrorCode.TASK_INVALID_RETURN_TYPE,
                location=function_location,
                notes=[
                    f"function '{function.__name__}' returns `{return_hint}`",
                    'tasks must return Result[T, TaskError[C]]',
                ],
                help_text='change return type to `-> Result[YourType, TaskError]`',
            )
        case ResultTypeArguments(ok_type=ok_type, err_type=err_type):
            pass
        case _ as unreachable:
            assert_never(unreachable)

    # Strict signature validation per design-doc §2: every param / return
    # annotation must use a type from the closed allow list. Runs AFTER the
    # existing return-shape checks so HRS-100 / HRS-101 fire for missing /
    # malformed return types; the strict validator catches banned content
    # (Any, bare containers, set, bytes, Path, TypedDict, TypeVar, ...) and
    # enforces ErrT is TaskError. Failure here is the documented breaking
    # cut at app construction time.
    try:
        check_task_signature(function, task_name=task_name)
    except SignatureValidationError as exc:
        raise TaskDefinitionError(
            message=exc.detail.headline,
            code=HorsiesErrorCode.TASK_STRICT_SIGNATURE_VIOLATION,
            location=function_location,
            notes=[exc.detail.reason],
            help_text=exc.detail.help_text,
        ) from exc

    code_type = _declared_code_type(
        err_type,
        code_enum,
        mapper_codes=_mapper_code_texts(
            exception_mapper,
            app.config.exception_mapper,
            default_unhandled_error_code,
            app.config.default_unhandled_error_code,
        ),
        task_name=task_name,
        location=function_location,
    )

    ok_type_adapter: TypeAdapter[Any] = TypeAdapter(ok_type)
    # A parametrized `TaskError[E]` is a subclass that pydantic builds; a body
    # builds the plain class. The error is validated as a `TaskError`, and
    # its custom code against `code_type` (`custom_code_refusal`).
    err_type_adapter: TypeAdapter[Any] = TypeAdapter(TaskError)

    def return_type_mismatch(
        variant: Literal['ok', 'err'],
        returned: object,
        summary: str,
        entries: list[Json],
        entry_count: int,
    ) -> Result[T, TaskError[C]]:
        """The refusal of a returned value that does not validate.

        The text and the data name types and failing fields, never the value.
        """
        expected_type = ok_type if variant == 'ok' else err_type
        actual_type = type(returned).__name__
        wrapper = 'Ok' if variant == 'ok' else 'Err'
        return Err(TaskError(
            code=ContractCode.RETURN_TYPE_MISMATCH,
            message=(
                f'Task {function.__name__} returned {wrapper}(<{actual_type}>) '
                f'but expected type {expected_type}: {summary}'
            ),
            data={
                'variant': variant,
                'expected_type': str(expected_type),
                'actual_type': actual_type,
                'validation_errors': entries,
                'validation_error_count': entry_count,
            },
        ))

    def not_a_result(returned: object) -> Result[T, TaskError[C]]:
        """The refusal of a return that is not an `Ok` or an `Err`.

        The text and the data name types, never the value. A returned
        `TaskError` also names its code, because the reader otherwise sees
        only the type. `variant` keeps its stored value `not_a_task_result`,
        which the other clients write too.
        """
        actual_type = _returned_type_name(returned)
        code_text = (
            f' with code {escape_lone_surrogates(str(returned.code))!r}'
            if isinstance(returned, TaskError)
            else ''
        )
        return Err(TaskError(
            code=ContractCode.RETURN_TYPE_MISMATCH,
            message=(
                f'Task {function.__name__} returned {actual_type}{code_text} '
                f'but must return Result[{ok_type}, {err_type}]'
            ),
            data={
                'variant': 'not_a_task_result',
                'expected_type': f'Result[{ok_type}, {err_type}]',
                'actual_type': actual_type,
                'validation_errors': [],
                'validation_error_count': 0,
            },
        ))

    def validated_return(
        variant: Literal['ok', 'err'],
        value: object,
        returned: Ok[object] | Err[object],
    ) -> Result[T, TaskError[C]]:
        """Validate one returned value against its declared type.

        The ok value is validated against T. A validator can transform it, so
        a transformed value is returned in a new `Ok`: an in-process caller
        then sees the value that a wire consumer decodes. Otherwise, and for
        an err value validated against E, `returned` comes back as it is.
        """
        try:
            try:
                match variant:
                    case 'ok':
                        validated_ok = ok_type_adapter.validate_python(
                            value, strict=True, extra='forbid',
                        )
                        if validated_ok is not value:
                            return Ok(cast('T', validated_ok))
                        return cast('Result[T, TaskError[C]]', returned)
                    case 'err':
                        _ = err_type_adapter.validate_python(value)
                        match custom_code_refusal(value, code_type):
                            case None:
                                return cast('Result[T, TaskError[C]]', returned)
                            case refusal:
                                return return_type_mismatch(
                                    'err',
                                    value,
                                    refusal,
                                    [{'loc': ['code'], 'msg': refusal, 'type': 'custom_code'}],
                                    1,
                                )
            except ValidationError as ve:
                # Type validation failed - return error result.
                return return_type_mismatch(
                    variant,
                    value,
                    validation_summary(ve),
                    validation_error_entries(ve),
                    ve.error_count(),
                )
        except UnicodeEncodeError:
            # A validator wrote a lone surrogate in its error text, so
            # pydantic cannot build that text: validate_python raises
            # this for a ValueError, and ValidationError.errors() for the
            # context of a PydanticCustomError. It is the same refusal.
            # The task body is outside this try, so its own
            # UnicodeEncodeError is an unhandled exception.
            text = lone_surrogate_text('a validation error', ())
            return return_type_mismatch(
                variant,
                value,
                text,
                [{'loc': [], 'msg': text, 'type': 'lone_surrogate'}],
                1,
            )

    def checked_result(returned: object) -> Result[T, TaskError[C]]:
        """Check one return of the task body against the declared types.

        A return that is not an `Ok` or an `Err`, or that does not fit the
        types, becomes a RETURN_TYPE_MISMATCH error. A `def` task and an
        `async def` task share this check.
        """
        match returned:
            case Ok(ok_value=value):
                return validated_return('ok', value, cast('Ok[object]', returned))
            case Err(err_value=error):
                return validated_return('err', error, cast('Err[object]', returned))
            case _:
                return not_a_result(returned)

    def exception_result(exc: BaseException) -> Result[T, TaskError[C]]:
        """The error result of an exception from the task body or its check.

        A `def` task and an `async def` task share this mapping: the
        exception mappers decide the code, as `resolve_exception_error_code`
        states.
        """
        match exc:
            case WorkflowContextMissingId():
                return Err(TaskError(
                    exception=exc,
                    code=ContractCode.WORKFLOW_CONTEXT_MISSING_ID,
                    message=str(exc),
                    data={'task_name': task_name},
                ))
            case _:
                error_code = resolve_exception_error_code(
                    exc=exc,
                    task_mapper=exception_mapper,
                    global_mapper=app.config.exception_mapper,
                    task_default=default_unhandled_error_code,
                    global_default=app.config.default_unhandled_error_code,
                )
                from horsies.core.models.tasks import BUILTIN_CODE_REGISTRY

                resolved_code: BuiltInTaskCode | str = BUILTIN_CODE_REGISTRY.get(
                    error_code, error_code,
                )
                return Err(TaskError(
                    exception=exc,
                    code=cast('C | BuiltInTaskCode', mapped_code(resolved_code, code_type)),
                    message=f'Unhandled exception in task {function.__name__}: {type(exc).__name__}: {exception_text(exc)}',
                    data={'exception_type': type(exc).__name__},
                ))

    # Pre-serialize task_options once so the send/send_async closures
    # below can reuse the JSON string instead of re-serializing on every call.
    _pre_serialized_options: str | None = None
    if task_options is not None:
        _opts_result = serialize_task_options(task_options)
        if is_err(_opts_result):
            raise ValueError(
                f'Failed to serialize task_options for {task_name}: {_opts_result.err_value}',
            )
        _pre_serialized_options = _opts_result.ok_value

    def _serialize_options_for_send(
        good_until_override: datetime | None | object,
    ) -> TaskSendResult[tuple[datetime | None, str | None]]:
        """Resolve good_until and task_options JSON for this concrete send."""
        if good_until_override is _UNSET:
            # app.task() rejects definition-time good_until, but direct wrapper
            # construction and legacy serialized options can still carry it.
            return Ok((
                task_options.good_until if task_options else None,
                _pre_serialized_options,
            ))

        good_until = cast(datetime | None, good_until_override)
        if good_until is None and task_options is None:
            return Ok((None, None))

        try:
            send_options = TaskOptions(
                task_name=task_options.task_name if task_options else task_name,
                queue=task_options.queue if task_options else None,
                good_until=good_until,
                retry_policy=task_options.retry_policy if task_options else None,
                timeout_ms=task_options.timeout_ms if task_options else None,
                execution=(
                    task_options.execution if task_options else Execution.ISOLATED
                ),
                cancel_grace_ms=(
                    task_options.cancel_grace_ms if task_options else None
                ),
            )
        except Exception as exc:
            cause = without_validation_input(exc)
            return Err(TaskSendValidationFailed(
                message=f'Invalid send options for {task_name}: {cause}',
                cause=cause,
            ))

        opts_result = serialize_task_options(send_options)
        if is_err(opts_result):
            err = opts_result.err_value
            return Err(TaskSendValidationFailed(
                message=f'Failed to serialize send options for {task_name}: {err}',
                cause=err,
            ))
        return Ok((good_until, opts_result.ok_value))

    # ---- Core send helpers ----

    def _attempt_send(
        task_id: UUID,
        payload: TaskSendPayload,
        first_kind: SendKind,
        trace: TraceArgument,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """The broker calls of one sync send: the first, and the resends of a
        transient failure when ``resend_on_transient_error`` is on. Each call
        passes ``trace``.

        ``first_kind`` is the send kind of the first broker call
        (``_send_kind_of_attempt``).
        """
        if in_running_event_loop():
            return Err(TaskSendAsyncContext(
                task_id=task_id,
                payload=payload,
                message=(
                    f'sync send for task {task_name!r} was called inside a '
                    f'running event loop; its blocking enqueue would stall '
                    f'the loop. Use `await {task_name}.send_async(...)` '
                    f'(or resend_async for resends).'
                ),
            ))
        broker = app.get_broker()

        last_err: TaskSendEnqueueFailed | None = None
        max_attempts = 1 + (_SEND_RETRY_COUNT if app.config.resend_on_transient_error else 0)

        for attempt in range(max_attempts):
            if attempt > 0 and last_err is not None:
                delay_ms = min(
                    _SEND_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                    _SEND_RETRY_MAX_MS,
                )
                time.sleep(delay_ms / 1000.0)

            result = broker.enqueue(
                task_name,
                payload.queue_name,
                task_id=task_id,
                send_kind=_send_kind_of_attempt(first_kind, attempt),
                input_json=payload.input_json,
                priority=payload.priority,
                good_until=payload.good_until,
                sent_at=payload.sent_at,
                delay_seconds=payload.enqueue_delay_seconds,
                task_options=payload.task_options,
                idempotency_key=payload.idempotency_key,
                retention_class_key=payload.retention_class_key,
                trace=trace,
            )

            if is_err(result):
                send_err = _map_broker_err(result.err_value, task_id, payload)
                match send_err:
                    case TaskSendEnqueueFailed(retryable=True) if (
                        attempt < max_attempts - 1
                    ):
                        last_err = send_err
                        continue
                    case _:
                        return Err(send_err)

            return Ok(TaskHandle(
                result.ok_value, app, broker_mode=True, ok_type=ok_type, code_type=code_type,
            ))

        # Unreachable: max_attempts >= 1, and the last attempt returns.
        assert last_err is not None, (
            'enqueue attempt loop ended without a failed attempt'
        )
        return Err(last_err)

    async def _attempt_send_async(
        task_id: UUID,
        payload: TaskSendPayload,
        first_kind: SendKind,
        trace: TraceArgument,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """The broker calls of one async send, as ``_attempt_send``.

        ``first_kind`` is the send kind of the first broker call
        (``_send_kind_of_attempt``).
        """
        broker = app.get_broker()

        last_err: TaskSendEnqueueFailed | None = None
        max_attempts = 1 + (_SEND_RETRY_COUNT if app.config.resend_on_transient_error else 0)

        for attempt in range(max_attempts):
            if attempt > 0 and last_err is not None:
                delay_ms = min(
                    _SEND_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                    _SEND_RETRY_MAX_MS,
                )
                await asyncio.sleep(delay_ms / 1000.0)

            result = await broker.enqueue_async(
                task_name,
                payload.queue_name,
                task_id=task_id,
                send_kind=_send_kind_of_attempt(first_kind, attempt),
                input_json=payload.input_json,
                priority=payload.priority,
                good_until=payload.good_until,
                sent_at=payload.sent_at,
                delay_seconds=payload.enqueue_delay_seconds,
                task_options=payload.task_options,
                idempotency_key=payload.idempotency_key,
                retention_class_key=payload.retention_class_key,
                trace=trace,
            )

            if is_err(result):
                send_err = _map_broker_err(result.err_value, task_id, payload)
                match send_err:
                    case TaskSendEnqueueFailed(retryable=True) if (
                        attempt < max_attempts - 1
                    ):
                        last_err = send_err
                        continue
                    case _:
                        return Err(send_err)

            return Ok(TaskHandle(
                result.ok_value, app, broker_mode=True, ok_type=ok_type, code_type=code_type,
            ))

        # Unreachable: max_attempts >= 1, and the last attempt returns.
        assert last_err is not None, (
            'enqueue attempt loop ended without a failed attempt'
        )
        return Err(last_err)

    def _send_subject(task_id: UUID, payload: TaskSendPayload) -> TaskSendSubject:
        return TaskSendSubject(
            task_name=task_name,
            queue=payload.queue_name,
            task_id=task_id,
        )

    def _do_send(
        task_id: UUID,
        payload: TaskSendPayload,
        first_kind: SendKind,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """One sync send, in the span of the app's send telemetry. Used by
        send() and resend()."""
        match app.send_telemetry():
            case NoSendTelemetry():
                return _attempt_send(task_id, payload, first_kind, NO_TRACE_CARRIER)
            case OtelSendTelemetry(recorder=recorder):
                return traced(
                    lambda: recorder.start_task_send(_send_subject(task_id, payload)),
                    lambda trace: _attempt_send(task_id, payload, first_kind, trace),
                    task_send_end,
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _do_send_async(
        task_id: UUID,
        payload: TaskSendPayload,
        first_kind: SendKind,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """One async send, in the span of the app's send telemetry. Used by
        send_async() and resend_async()."""
        match app.send_telemetry():
            case NoSendTelemetry():
                return await _attempt_send_async(
                    task_id, payload, first_kind, NO_TRACE_CARRIER,
                )
            case OtelSendTelemetry(recorder=recorder):
                return await traced_async(
                    lambda: recorder.start_task_send(_send_subject(task_id, payload)),
                    lambda trace: _attempt_send_async(task_id, payload, first_kind, trace),
                    task_send_end,
                )
            case _ as unreachable:
                assert_never(unreachable)

    # ---- Pre-send preparation ----

    def _prepare_send(
        args: tuple[Any, ...],
        kwargs_dict: dict[str, Any],
        enqueue_delay_seconds: int | None = None,
        good_until_override: datetime | None | object = _UNSET,
    ) -> TaskSendResult[tuple[UUID, TaskSendPayload]]:
        """Validate, serialize, and build a (task_id, TaskSendPayload) tuple.

        Returns Err(TaskSendError) on validation or serialization failure.
        """
        queue_name = task_options.queue if task_options else None
        try:
            validated_queue_name = app.validate_queue(queue_name)
            priority = effective_priority(app, validated_queue_name)
        except Exception as exc:
            # Exception, not BaseException: a KeyboardInterrupt/SystemExit
            # arriving mid-send must propagate as a shutdown signal, not be
            # swallowed into Err(VALIDATION_FAILED).
            return Err(TaskSendValidationFailed(
                message=f'Queue validation failed for {task_name}: {exc}',
                cause=exc,
            ))

        # Process-monotonic UUIDv7 from the core: the embedded birth time
        # prunes the staged history lookup. Schedule-derived tasks keep
        # their deterministic slot identity, which is the schedule's
        # idempotency.
        task_id = _mint_send_task_id()
        sent_at = datetime.now(timezone.utc)
        options_result = _serialize_options_for_send(good_until_override)
        if is_err(options_result):
            return options_result
        good_until, task_options_json = options_result.ok_value

        # Strict-serde: positional args have no typed wire representation
        # and are rejected at every producer entry point. The receiver
        # always binds against the kwargs signature; positional values
        # would defeat that binding.
        if args:
            return Err(TaskSendValidationFailed(
                message=(
                    f"Task {task_name!r} received {len(args)} positional "
                    f"arg(s); strict-serde rejects positional args. "
                    f"Pass every argument by keyword."
                ),
                cause=NoCause(),
            ))
        missing = missing_required_kwargs(function, kwargs_dict)
        if missing.workflow_only:
            return Err(TaskSendValidationFailed(
                message=(
                    f'Task {task_name!r} requires {list(missing.workflow_only)}, '
                    f'which only a workflow can fill. Run it as a workflow node, '
                    f'or give these parameters a default.'
                ),
                cause=NoCause(),
            ))
        if missing.producer:
            return Err(TaskSendValidationFailed(
                message=(
                    f'Task {task_name!r} is missing required kwargs '
                    f'{list(missing.producer)}; nothing was enqueued.'
                ),
                cause=NoCause(),
            ))
        try:
            _ = fill_omitted_defaults(function, kwargs_dict)
        except (StrictJsonError, ValueValidationError) as exc:
            return Err(TaskSendValidationFailed(
                message=f'A default of {task_name!r} does not fit its type: {exc}',
                cause=exc,
            ))

        # Serialize kwargs against the receiver's declared parameter types.
        input_json: str | None = None
        if kwargs_dict:
            try:
                encoded_kwargs = encode_kwargs_read_back(function, kwargs_dict)
            except (StrictJsonError, ValueValidationError) as exc:
                return Err(TaskSendValidationFailed(
                    message=f'Failed to encode kwargs for {task_name}: {exc}',
                    cause=exc,
                ))
            kwargs_r = dumps_json(encoded_kwargs)
            if is_err(kwargs_r):
                return Err(TaskSendValidationFailed(
                    message=f'Failed to serialize encoded kwargs for {task_name}: {kwargs_r.err_value}',
                    cause=kwargs_r.err_value,
                ))
            input_json = kwargs_r.ok_value

            warn_if_payload_large(
                app.config.payload,
                task_name=task_name,
                kind='kwargs',
                payload_text=input_json,
            )
            match check_payload_size(
                app.config.payload,
                payload_text=input_json,
            ):
                case PayloadWithinLimit():
                    pass
                case PayloadOverLimit(
                    size_bytes=size_bytes,
                    limit_bytes=limit_bytes,
                ):
                    return Err(TaskSendPayloadTooLarge(
                        message=(
                            f'kwargs payload for {task_name} is {size_bytes} bytes, '
                            f'exceeding payload.reject_bytes='
                            f'{limit_bytes}; nothing was '
                            f'enqueued. Pass a reference to external storage or '
                            f'raise the limit.'
                        ),
                        size_bytes=size_bytes,
                        limit_bytes=limit_bytes,
                    ))
                case _ as unreachable:
                    assert_never(unreachable)

        payload = TaskSendPayload(
            task_name=task_name,
            queue_name=validated_queue_name,
            priority=priority,
            input_json=input_json,
            sent_at=sent_at,
            good_until=good_until,
            enqueue_delay_seconds=enqueue_delay_seconds,
            task_options=task_options_json,
        )
        return Ok((task_id, payload))

    # ---- Public send methods ----

    def _send_with_options(
        args: tuple[Any, ...],
        kwargs_dict: dict[str, Any],
        good_until_override: datetime | None | object,
        *,
        idempotency_key: str | None = None,
        retention: SendRetention = _UNSET_RETENTION,
        delay_seconds: int | None = None,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        if app.are_sends_suppressed():
            return Err(TaskSendSuppressed(
                message=f'Task send suppressed for {task_name} (import/check phase or HORSIES_SUPPRESS_SENDS=1)',
            ))
        delay_check = _validate_delay(delay_seconds)
        if is_err(delay_check):
            return delay_check
        checked_delay = delay_check.ok_value

        # Only an explicit key is validated here. A key the mapping
        # derives is generated from configuration that was already
        # checked, and validating it against the declared set would
        # refuse the very classes the mapping exists to create.
        stamp_parse = _parse_retention_choice(retention)
        if is_err(stamp_parse):
            return stamp_parse
        stamp = stamp_parse.ok_value

        prep = _prepare_send(
            args,
            kwargs_dict,
            enqueue_delay_seconds=checked_delay,
            good_until_override=good_until_override,
        )
        if is_err(prep):
            return prep
        task_id, payload = prep.ok_value
        if idempotency_key is not None:
            payload = dataclasses.replace(
                payload, idempotency_key=idempotency_key
            )
        payload = dataclasses.replace(
            payload,
            retention_class_key=_stamped_class(stamp, payload.queue_name),
        )
        return _do_send(task_id, payload, SendKind.FIRST)

    async def _send_async_with_options(
        args: tuple[Any, ...],
        kwargs_dict: dict[str, Any],
        good_until_override: datetime | None | object,
        *,
        idempotency_key: str | None = None,
        retention: SendRetention = _UNSET_RETENTION,
        delay_seconds: int | None = None,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        if app.are_sends_suppressed():
            return Err(TaskSendSuppressed(
                message=f'Task send suppressed for {task_name} (import/check phase or HORSIES_SUPPRESS_SENDS=1)',
            ))
        delay_check = _validate_delay(delay_seconds)
        if is_err(delay_check):
            return delay_check
        checked_delay = delay_check.ok_value

        # Only an explicit key is validated here. A key the mapping
        # derives is generated from configuration that was already
        # checked, and validating it against the declared set would
        # refuse the very classes the mapping exists to create.
        stamp_parse = _parse_retention_choice(retention)
        if is_err(stamp_parse):
            return stamp_parse
        stamp = stamp_parse.ok_value

        prep = _prepare_send(
            args,
            kwargs_dict,
            enqueue_delay_seconds=checked_delay,
            good_until_override=good_until_override,
        )
        if is_err(prep):
            return prep
        task_id, payload = prep.ok_value
        if idempotency_key is not None:
            payload = dataclasses.replace(
                payload, idempotency_key=idempotency_key
            )
        payload = dataclasses.replace(
            payload,
            retention_class_key=_stamped_class(stamp, payload.queue_name),
        )
        return await _do_send_async(task_id, payload, SendKind.FIRST)

    def _stamped_class(
        stamp: _RetentionStamp, queue_name: str
    ) -> RetentionClassKey | None:
        """The class key to stamp, given the parsed choice.

        Precedence, highest first: an explicit argument at the send call
        (including an explicit `None` for forever and an explicit 30
        days), then this queue's `queue_retention` mapping, then the
        immutable 30-day default.

        Resolution happens once, here, and the result is written into
        the payload. The payload is the replay envelope, so a resend
        carries the class the original send chose — editing the mapping
        governs later sends and never restates the promise already made
        to a task in flight.
        """
        match stamp:
            case _FromQueueMapping():
                return _queue_retention_class(queue_name)
            case _ExplicitClass(key=key):
                return key

    def _queue_retention_class(queue_name: str) -> RetentionClassKey | None:
        """This queue's mapped class, read from the app at send time.

        Read through `app` rather than captured at decoration, because a
        task may be defined before the app's config is finalised. The
        rule itself lives in one place so every enqueue path shares it.
        """
        return resolve_queue_retention_class(
            getattr(app.config, 'retention', None), queue_name
        )

    def _parse_retention_choice(
        choice: object,
    ) -> Result[_RetentionStamp, TaskSendValidationFailed]:
        """Parse the send's retention argument into the class it stamps.

        Accepted: the parameter omitted (the queue mapping or the 30-day
        default decides), explicit ``None`` (forever), or a period of
        ``AppConfig.retention.sendable_periods()``: the default, a mapped
        period, or one in ``retention_periods``. Any other value is
        refused, including the class-key strings that 0.6.0 accepted.

        Typed as ``object``: this is the send boundary, and a caller can
        pass any value at run time. The set of periods is process-static,
        so the check costs no database round trip. It is a per-process
        contract on purpose: the maintenance owner registers the classes
        of its own config's periods, so a period outside that set would
        have no partition when the task finishes.
        """
        match choice:
            case _UnsetRetention():
                return Ok(_FromQueueMapping())
            case None:
                return Ok(_ExplicitClass(key=None))
            case timedelta() as period:
                return _parse_explicit_period(period)
            case other:
                return Err(TaskSendValidationFailed(
                    message=(
                        f'Invalid retention for {task_name}: retention takes '
                        f'a timedelta of whole days or None, not '
                        f'{type(other).__name__}. The class keys of 0.6.0 '
                        f'were removed: pass the period, for example '
                        f'timedelta(days=7).'
                    ),
                    cause=NoCause(),
                ))

    def _parse_explicit_period(
        period: timedelta,
    ) -> Result[_RetentionStamp, TaskSendValidationFailed]:
        """The class of an explicit finite period, if this process registers it."""
        match parse_retention_period(period):
            case Err(err_value=refused):
                return Err(TaskSendValidationFailed(
                    message=f'Invalid retention for {task_name}: {refused.reason}.',
                    cause=NoCause(),
                ))
            case Ok(ok_value=days):
                pass
        allowed = _sendable_periods()
        match days in allowed:
            case True:
                return Ok(_ExplicitClass(key=period_class_key(days)))
            case False:
                listed = ', '.join(
                    f'{allowed_days} days' for allowed_days in sorted(allowed)
                )
                return Err(TaskSendValidationFailed(
                    message=(
                        f'Retention of {days} days for {task_name} is not a '
                        f'period this deployment registers. Valid values: '
                        f'omit it for the queue mapping or the 30-day default, '
                        f'pass None to keep the record forever, or choose one '
                        f'of: {listed}. Add the period to '
                        f'AppConfig.retention.retention_periods to send with it.'
                    ),
                    cause=NoCause(),
                ))

    def _sendable_periods() -> frozenset[RetentionDays]:
        """The finite periods this process may stamp, read at send time.

        Read through ``app`` rather than captured at decoration, because
        a task may be defined before the app's config is finalised.
        """
        match getattr(app.config, 'retention', None):
            case RetentionConfig() as retention:
                return retention.sendable_periods()
            case _:
                return RetentionConfig().sendable_periods()

    def _validate_delay(
        delay_seconds: object,
    ) -> Result[int | None, TaskSendValidationFailed]:
        """The delay of a send: ``None`` for a send now, else a non-negative
        int number of seconds.

        Typed as ``object``: this is the send boundary, and a caller can
        pass any value at run time. bool is an int subclass, and
        ``delay_seconds=False`` that means 0 is a footgun, so it is refused
        with the other non-int types. A refused value returns Err; nothing
        raises.
        """
        match delay_seconds:
            case None:
                return Ok(None)
            case bool() as refused:
                return Err(_delay_type_refused(refused))
            case int() as delay if delay >= 0:
                return Ok(delay)
            case int() as delay:
                return Err(TaskSendValidationFailed(
                    message=(
                        f'delay_seconds must not be negative, got {delay}; '
                        f'use delay_seconds=0, or omit it, to send {task_name} now'
                    ),
                    cause=NoCause(),
                ))
            case refused:
                return Err(_delay_type_refused(refused))

    def _delay_type_refused(refused: object) -> TaskSendValidationFailed:
        """The refusal of a delay that is not an int number of seconds."""
        return TaskSendValidationFailed(
            message=(
                f'delay_seconds must be an int number of seconds or None, '
                f'got {type(refused).__name__}'
            ),
            cause=NoCause(),
        )

    def send(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """Execute task asynchronously via app's broker."""
        return _send_with_options(args, kwargs, _UNSET)

    async def send_async(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """Async variant for frameworks like FastAPI."""
        return await _send_async_with_options(args, kwargs, _UNSET)

    class _TaskFunctionWithOptionsImpl:
        def __init__(
            self,
            good_until: datetime | None,
            idempotency_key: str | None,
            retention: SendRetention,
            delay_seconds: int | None,
        ) -> None:
            self.good_until = good_until
            self.idempotency_key = idempotency_key
            # Carried unresolved: `with_options(...)` may be built before
            # the send that knows which queue it goes to, and absence
            # must still read as absence when it gets there.
            self.retention = retention
            # Checked at each send, which returns the refusal.
            self.delay_seconds = delay_seconds

        def send(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskSendResult[TaskHandle[T, C]]:
            return _send_with_options(
                args, kwargs, self.good_until,
                idempotency_key=self.idempotency_key,
                retention=self.retention,
                delay_seconds=self.delay_seconds,
            )

        async def send_async(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskSendResult[TaskHandle[T, C]]:
            return await _send_async_with_options(
                args, kwargs, self.good_until,
                idempotency_key=self.idempotency_key,
                retention=self.retention,
                delay_seconds=self.delay_seconds,
            )

    def with_options(
        *,
        good_until: datetime | None = None,
        idempotency_key: str | None = None,
        retention: SendRetention = _UNSET_RETENTION,
        delay_seconds: int | None = None,
    ) -> TaskFunctionWithOptions[P, T, C]:
        """Set options for one ad-hoc send without changing the task definition.

        ``retention``: omit for the queue's mapping or the 30-day default;
        pass ``None`` to keep the terminal record forever; or pass a period
        in whole days that ``AppConfig.retention.sendable_periods()``
        contains. Any other period is refused before the enqueue writes
        anything.

        ``delay_seconds``: omit or pass ``None`` to make the task due now;
        pass a whole number of seconds, 0 or more, to make it due after that
        delay. Any other value is refused before the enqueue writes anything.
        """
        return _TaskFunctionWithOptionsImpl(
            good_until, idempotency_key, retention, delay_seconds,
        )

    # ---- Resend methods ----

    def _validate_resend(
        error: TaskSendResendable,
    ) -> Result[tuple[UUID, TaskSendPayload], TaskSendValidationFailed]:
        """Check that a resendable error's payload belongs to this task.

        The parameter type admits ``TaskSendEnqueueFailed`` and
        ``TaskSendAsyncContext``; each carries its task id and payload. Another variant, passed by a caller
        without a type checker, fails an assertion that names it. Refused: a
        payload of another task. A payload with a delay is sent again with
        its delay. Returns ``Ok((task_id, payload))``.
        """
        match error:
            case TaskSendEnqueueFailed() | TaskSendAsyncContext():
                pass
            case _ as refused:
                assert_never(refused)
        if error.payload.task_name != task_name:
            return Err(TaskSendValidationFailed(
                message=(
                    f'cross-task resend: error is for {error.payload.task_name!r} '
                    f'but this task is {task_name!r}'
                ),
                cause=NoCause(),
            ))
        return Ok((error.task_id, error.payload))

    def resend(
        error: TaskSendResendable,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """Send the stored payload of a failed send again: the same task id,
        input, ``sent_at`` and delay.

        Each broker call is a ``SendKind.REPEAT``: a task that an earlier
        call wrote answers with its id, also after it finished.
        """
        v = _validate_resend(error)
        if is_err(v):
            return v
        task_id, payload = v.ok_value
        return _do_send(task_id, payload, SendKind.REPEAT)

    async def resend_async(
        error: TaskSendResendable,
    ) -> TaskSendResult[TaskHandle[T, C]]:
        """Async variant of resend() for callers on an event loop.

        Each broker call is a ``SendKind.REPEAT``: a task that an earlier
        call wrote answers with its id, also after it finished.
        """
        v = _validate_resend(error)
        if is_err(v):
            return v
        task_id, payload = v.ok_value
        return await _do_send_async(task_id, payload, SendKind.REPEAT)

    class TaskFunctionImpl:
        """The members that both kinds share. A subclass adds `__call__`."""

        def __init__(self, call: Callable[..., object]) -> None:
            # `__getattr__` reads `_call`, so it is set first.
            self._call = call
            self.__name__ = function.__name__
            self.__doc__ = function.__doc__
            self.__annotations__ = function.__annotations__
            self.task_name = task_name
            # Persist the declared queue (validated at definition time) so other components
            # (e.g., scheduler) can infer a task's home queue in CUSTOM mode.
            self.queue = task_options.queue if task_options else None
            # Keep a reference to the original function for introspection (signature checks).
            self._original_function = function
            # Expose the original function as the wrapped target so
            # inspect.signature(task_func) resolves to the real parameters
            # instead of TaskFunctionImpl.__call__'s (*args, **kwargs). function is
            # guaranteed unwrapped (pre-decorated callables are rejected above).
            self.__wrapped__ = function
            # Expose the declared ok type for workflow args_from type checks.
            self.task_ok_type = ok_type
            # The type of the custom codes, for the reads of this task.
            self.task_code_type: CustomCodeType = code_type
            # Reuse the value already computed in the outer scope
            self.task_options_json: str | None = _pre_serialized_options
            # Expose mapper config for app.check() safety validation.
            # Frozen to prevent post-definition mutation.
            self.exception_mapper: Mapping[type[BaseException], str] | None = (
                MappingProxyType(exception_mapper) if exception_mapper is not None else None
            )
            self.default_unhandled_error_code: str | None = default_unhandled_error_code
            self.error_codes: tuple[str, ...] = error_codes
            # The exception mapping of this task, for the worker's await of
            # an async call (`await_task_call_in_worker`).
            self._map_exception = exception_result

        def send(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskSendResult[TaskHandle[T, C]]:
            return send(*args, **kwargs)

        async def send_async(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskSendResult[TaskHandle[T, C]]:
            return await send_async(*args, **kwargs)

        def with_options(
            self,
            *,
            good_until: datetime | None = None,
            idempotency_key: str | None = None,
            retention: SendRetention = _UNSET_RETENTION,
            delay_seconds: int | None = None,
        ) -> TaskFunctionWithOptions[P, T, C]:
            return with_options(
                good_until=good_until,
                idempotency_key=idempotency_key,
                retention=retention,
                delay_seconds=delay_seconds,
            )

        def resend(
            self,
            error: TaskSendResendable,
        ) -> TaskSendResult[TaskHandle[T, C]]:
            return resend(error)

        async def resend_async(
            self,
            error: TaskSendResendable,
        ) -> TaskSendResult[TaskHandle[T, C]]:
            return await resend_async(error)

        def node(
            self,
            *,
            waits_for: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None = None,
            workflow_context_from: Sequence['TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None = None,
            args_from: dict[str, 'TaskNode[Any, Any] | SubWorkflowNode[Any]'] | None = None,
            queue: str | None = None,
            priority: int | None = None,
            allow_failed_dependencies: bool = False,
            join: Literal['all', 'any', 'quorum'] = 'all',
            min_success: int | None = None,
            good_until: datetime | None = None,
            node_id: str | None = None,
        ) -> NodeFactory[P, T, C]:
            return NodeFactory(
                function=self,  # type: ignore[arg-type]
                waits_for=waits_for,
                workflow_context_from=workflow_context_from,
                args_from=args_from,
                queue=queue,
                priority=priority,
                allow_failed_dependencies=allow_failed_dependencies,
                join=join,
                min_success=min_success,
                good_until=good_until,
                node_id=node_id,
            )

        def __getattr__(self, name: str) -> Any:
            """Read other attributes from the call function.

            An instance that `copy` builds has no `_call` until its state is
            set. `self._call` would then call this method again, so `_call`
            is read from the instance dict.
            """
            instance_fields = vars(self)
            if '_call' not in instance_fields:
                raise AttributeError(
                    f'{type(self).__name__!r} object has no attribute {name!r}',
                )
            return getattr(instance_fields['_call'], name)

    match _task_body(function):
        case _SyncTaskBody(function=sync_body):
            sync_call = _sync_task_call(sync_body, checked_result, exception_result)

            class SyncTaskFunctionImpl(TaskFunctionImpl):
                task_body_kind = TaskBodyKind.DEF

                def __call__(
                    self,
                    *args: P.args,
                    **kwargs: P.kwargs,
                ) -> Result[T, TaskError[C]]:
                    return sync_call(*args, **kwargs)

            return SyncTaskFunctionImpl(sync_call)
        case _AsyncTaskBody(function=async_body):
            async_call = _async_task_call(async_body, checked_result, exception_result)

            class AsyncTaskFunctionImpl(TaskFunctionImpl):
                task_body_kind = TaskBodyKind.ASYNC_DEF

                def __call__(
                    self,
                    *args: P.args,
                    **kwargs: P.kwargs,
                ) -> Coroutine[Any, Any, Result[T, TaskError[C]]]:
                    return async_call(*args, **kwargs)

            return AsyncTaskFunctionImpl(async_call)
        case _ as unreachable:
            assert_never(unreachable)
