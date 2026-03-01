# app/core/task_decorator.py
from __future__ import annotations
import asyncio
import time
import uuid
from typing import (
    Callable,
    Final,
    get_origin,
    get_type_hints,
    get_args,
    Literal,
    ParamSpec,
    Sequence,
    TypeVar,
    Generic,
    Protocol,
    TYPE_CHECKING,
    Optional,
    Any,
)
from abc import abstractmethod
from collections.abc import Mapping
from types import MappingProxyType
from datetime import datetime, timezone
from pydantic import TypeAdapter, ValidationError
from horsies.core.codec.serde import (
    serialize_task_options,
    args_to_json,
    kwargs_to_json,
)
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.fingerprint import enqueue_fingerprint

from horsies.core.types.result import is_err, Ok, Err
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationError, BrokerResult
from horsies.core.models.task_send_types import (
    TaskSendErrorCode,
    TaskSendPayload,
    TaskSendError,
    TaskSendResult,
)

if TYPE_CHECKING:
    from horsies.core.app import Horsies
    from horsies.core.models.tasks import TaskOptions
    from horsies.core.models.tasks import TaskError, TaskResult
    from horsies.core.models.tasks import TaskInfo
    from horsies.core.models.workflow import (
        TaskNode,
        SubWorkflowNode,
        WorkflowContext,
    )

from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.models.workflow import WorkflowContextMissingIdError
from horsies.core.exception_mapper import (
    ExceptionMapper,
    resolve_exception_error_code,
)
from horsies.core.errors import (
    TaskDefinitionError,
    ConfigurationError,
    WorkflowValidationError,
    ErrorCode,
    SourceLocation,
)

P = ParamSpec('P')
T = TypeVar('T')
E = TypeVar('E')

# Internal retry constants for resend_on_transient_err.
# 1 initial attempt + _SEND_RETRY_COUNT retries = 4 total broker calls.
_SEND_RETRY_COUNT: Final[int] = 3
_SEND_RETRY_INITIAL_MS: Final[int] = 200
_SEND_RETRY_MAX_MS: Final[int] = 2000


class FromNodeMarker:
    """Marker for injecting an upstream node's result into a `.node()()` kwarg.

    Created via `from_node(upstream_node)`. When passed as a kwarg value to
    `NodeFactory.__call__()`, the factory converts it to an `args_from` entry
    and auto-wires the dependency.

    Not intended for direct construction — use `from_node()` instead.
    """

    __slots__ = ('_node',)

    def __init__(self, node: 'TaskNode[Any] | SubWorkflowNode[Any]') -> None:
        self._node = node

    @property
    def node(self) -> 'TaskNode[Any] | SubWorkflowNode[Any]':
        """The upstream node whose result will be injected."""
        return self._node

    def __repr__(self) -> str:
        return f'FromNodeMarker(node={self._node!r})'


def from_node(upstream: 'TaskNode[Any] | SubWorkflowNode[Any]') -> Any:
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
        # TaskNode(fn=consume, args_from={"data": producer_node}, waits_for=[producer_node])
    """
    from horsies.core.models.workflow import TaskNode, SubWorkflowNode

    if not isinstance(upstream, (TaskNode, SubWorkflowNode)):
        raise WorkflowValidationError(
            message='from_node() expects a TaskNode or SubWorkflowNode',
            code=ErrorCode.WORKFLOW_INVALID_ARGS_FROM,
            notes=[f'got {type(upstream).__name__}'],
            help_text='pass a node instance created by TaskNode(...) / SubWorkflowNode(...) or .node()()',
        )

    return FromNodeMarker(upstream)


def effective_priority(
    app: 'Horsies',
    queue_name: str,
) -> int:
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
            code=ErrorCode.TASK_INVALID_QUEUE,
            notes=[f'valid queues: {valid}'],
            help_text='use one of the configured queue names',
        )
    return config.priority


class TaskHandle(Generic[T]):
    """
    Handle for a submitted task, used to retrieve results.

    The get() and get_async() methods always return TaskResult[T, TaskError]:
    - On success: TaskResult(ok=value) where value is of type T
    - On task error: TaskResult(err=TaskError) from task execution
    - On retrieval error: TaskResult(err=TaskError) with WAIT_TIMEOUT, TASK_NOT_FOUND, etc.
    - On broker error: TaskResult(err=TaskError) with BROKER_ERROR

    This unified return type enables consistent error handling without try/except.
    """

    def __init__(
        self, task_id: str, app: Optional['Horsies'] = None, broker_mode: bool = False
    ):
        self.task_id = task_id
        self._app = app
        self._broker_mode = broker_mode
        self._cached_result: Optional[TaskResult[T, TaskError]] = None
        self._result_fetched = False

    def _error_result(
        self,
        *,
        error_code: LibraryErrorCode,
        message: str,
        data: dict[str, Any],
        exception: BaseException | None = None,
    ) -> TaskResult[T, TaskError]:
        error_result: TaskResult[T, TaskError] = TaskResult(
            err=TaskError(
                error_code=error_code,
                message=message,
                data=data,
                exception=exception,
            )
        )
        self._cached_result = error_result
        self._result_fetched = True
        return error_result

    def get(
        self,
        timeout_ms: Optional[int] = None,
    ) -> TaskResult[T, TaskError]:
        """
        Get the task result (blocking).

        Args:
            timeout_ms: Maximum time to wait for result (milliseconds)

        Returns:
            TaskResult[T, TaskError] - always returns TaskResult, never raises for task/retrieval errors.
            Check result.is_err() and result.err.error_code for error handling.
        """
        if self._result_fetched:
            match self._cached_result:
                case None:
                    return self._error_result(
                        error_code=LibraryErrorCode.RESULT_NOT_AVAILABLE,
                        message='Result cache is empty after fetch',
                        data={'task_id': self.task_id},
                    )
                case result:
                    return result

        if self._broker_mode and self._app:
            # Fetch from app's broker - broker now returns TaskResult for all cases
            broker = self._app.get_broker()
            try:
                result = broker.get_result(self.task_id, timeout_ms)
                # WAIT_TIMEOUT is transient ("not done yet"), not terminal.
                # Caching it would prevent subsequent get() calls from ever
                # seeing the real result once the task completes.
                err_value = result.err
                is_wait_timeout = (
                    result.is_err()
                    and err_value is not None
                    and err_value.error_code == LibraryErrorCode.WAIT_TIMEOUT
                )
                if not is_wait_timeout:
                    self._cached_result = result
                    self._result_fetched = True
                return result
            except Exception as exc:
                return self._error_result(
                    error_code=LibraryErrorCode.BROKER_ERROR,
                    message='Broker error while retrieving task result',
                    data={'task_id': self.task_id},
                    exception=exc,
                )
        else:
            # For synchronous/immediate execution, result should already be set
            match self._cached_result:
                case None:
                    return self._error_result(
                        error_code=LibraryErrorCode.RESULT_NOT_AVAILABLE,
                        message='Result not available - task may not have been executed',
                        data={'task_id': self.task_id},
                    )
                case result:
                    return result

    async def get_async(
        self,
        timeout_ms: Optional[int] = None,
    ) -> TaskResult[T, TaskError]:
        """
        Get the task result asynchronously.

        Args:
            timeout_ms: Maximum time to wait for result (milliseconds)

        Returns:
            TaskResult[T, TaskError] - always returns TaskResult, never raises for task/retrieval errors.
            Check result.is_err() and result.err.error_code for error handling.
        """
        if self._result_fetched:
            match self._cached_result:
                case None:
                    return self._error_result(
                        error_code=LibraryErrorCode.RESULT_NOT_AVAILABLE,
                        message='Result cache is empty after fetch',
                        data={'task_id': self.task_id},
                    )
                case result:
                    return result

        if self._broker_mode and self._app:
            # Fetch from app's broker - broker now returns TaskResult for all cases
            broker = self._app.get_broker()
            try:
                result = await broker.get_result_async(self.task_id, timeout_ms)
                # WAIT_TIMEOUT is transient ("not done yet"), not terminal.
                # Caching it would prevent subsequent get_async() calls from
                # ever seeing the real result once the task completes.
                err_value = result.err
                is_wait_timeout = (
                    result.is_err()
                    and err_value is not None
                    and err_value.error_code == LibraryErrorCode.WAIT_TIMEOUT
                )
                if not is_wait_timeout:
                    self._cached_result = result
                    self._result_fetched = True
                return result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return self._error_result(
                    error_code=LibraryErrorCode.BROKER_ERROR,
                    message='Broker error while retrieving task result',
                    data={'task_id': self.task_id},
                    exception=exc,
                )
        else:
            # For synchronous/immediate execution, result should already be set
            match self._cached_result:
                case None:
                    return self._error_result(
                        error_code=LibraryErrorCode.RESULT_NOT_AVAILABLE,
                        message='Result not available - task may not have been executed',
                        data={'task_id': self.task_id},
                    )
                case result:
                    return result

    def info(
        self,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
    ) -> 'BrokerResult[TaskInfo | None]':
        """Fetch metadata for this task from the broker.

        Returns Ok(TaskInfo) if found, Ok(None) if not found,
        Err(BrokerOperationError) on infrastructure or precondition failure.
        """
        if not self._broker_mode or not self._app:
            return Err(BrokerOperationError(
                code=BrokerErrorCode.NO_BROKER,
                message=(
                    'TaskHandle.info() requires a broker-backed task handle '
                    '(use .send() or .send_async())'
                ),
                retryable=False,
            ))

        broker = self._app.get_broker()
        return broker.get_task_info(
            self.task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
        )

    async def info_async(
        self,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
    ) -> 'BrokerResult[TaskInfo | None]':
        """Async version of info().

        Returns Ok(TaskInfo) if found, Ok(None) if not found,
        Err(BrokerOperationError) on infrastructure or precondition failure.
        """
        if not self._broker_mode or not self._app:
            return Err(BrokerOperationError(
                code=BrokerErrorCode.NO_BROKER,
                message=(
                    'TaskHandle.info_async() requires a broker-backed task handle '
                    '(use .send() or .send_async())'
                ),
                retryable=False,
            ))

        broker = self._app.get_broker()
        return await broker.get_task_info_async(
            self.task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
        )

    def set_immediate_result(
        self,
        result: TaskResult[T, TaskError],
    ) -> None:
        """Internal method to set result for synchronous execution"""
        self._cached_result = result
        self._result_fetched = True


class NodeFactory(Generic[P, T]):
    """
    Factory for creating TaskNode instances with typed arguments.

    Returned by TaskFunction.node(). Call with the task's arguments
    to create a TaskNode with full static type checking.

    Example:
        node = my_task.node(waits_for=[dep])(value='test')
        # Type checker validates 'value' against my_task's signature
    """

    _fn: 'TaskFunction[P, T]'
    _waits_for: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None
    _workflow_ctx_from: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None
    _args_from: dict[str, 'TaskNode[Any] | SubWorkflowNode[Any]'] | None
    _queue: str | None
    _priority: int | None
    _allow_failed_deps: bool
    _run_when: Callable[['WorkflowContext'], bool] | None
    _skip_when: Callable[['WorkflowContext'], bool] | None
    _join: Literal['all', 'any', 'quorum']
    _min_success: int | None
    _good_until: datetime | None
    _node_id: str | None

    def __init__(
        self,
        fn: 'TaskFunction[P, T]',
        *,
        waits_for: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None,
        workflow_ctx_from: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None,
        args_from: dict[str, 'TaskNode[Any] | SubWorkflowNode[Any]'] | None,
        queue: str | None,
        priority: int | None,
        allow_failed_deps: bool,
        run_when: Callable[['WorkflowContext'], bool] | None,
        skip_when: Callable[['WorkflowContext'], bool] | None,
        join: Literal['all', 'any', 'quorum'],
        min_success: int | None,
        good_until: datetime | None,
        node_id: str | None,
    ) -> None:
        self._fn = fn
        self._waits_for = waits_for
        self._workflow_ctx_from = workflow_ctx_from
        self._args_from = args_from
        self._queue = queue
        self._priority = priority
        self._allow_failed_deps = allow_failed_deps
        self._run_when = run_when
        self._skip_when = skip_when
        self._join = join
        self._min_success = min_success
        self._good_until = good_until
        self._node_id = node_id

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> 'TaskNode[T]':
        from horsies.core.models.workflow import TaskNode

        # D.1: Reject positional args — workflow nodes are kwargs-only
        if args:
            raise WorkflowValidationError(
                message='positional arguments not allowed in .node()() calls',
                code=ErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED,
                notes=[
                    f"task '{self._fn.task_name}' received {len(args)} positional arg(s)",
                    'workflow nodes use kwargs-only; pass all arguments by keyword',
                ],
                help_text='change .node()(val1, val2) to .node()(param1=val1, param2=val2)',
            )

        # C.1: Scan kwargs for FromNodeMarker instances
        marker_args_from: dict[str, 'TaskNode[Any] | SubWorkflowNode[Any]'] = {}
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
                code=ErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP,
                notes=[
                    f"task '{self._fn.task_name}' has key(s) in both from_node() "
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
            fn=self._fn,
            kwargs=static_kwargs,
            waits_for=waits_for,
            workflow_ctx_from=list(self._workflow_ctx_from)
            if self._workflow_ctx_from
            else None,
            args_from=merged_args_from,
            queue=self._queue,
            priority=self._priority,
            allow_failed_deps=self._allow_failed_deps,
            run_when=self._run_when,
            skip_when=self._skip_when,
            join=self._join,
            min_success=self._min_success,
            good_until=self._good_until,
            node_id=self._node_id,
        )


class TaskFunction(Protocol[P, T]):
    """
    A TaskFunction is a function that gets a @task decorator applied to it.
    Protocol extends the simple function signature to include the send and send_async methods,
    thus being able to be called with or without the @task decorator.

    The generic parameter T represents the success type in TaskResult[T, TaskError].
    """

    task_name: str

    @abstractmethod
    def __call__(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskResult[T, TaskError]: ...

    @abstractmethod
    def send(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult['TaskHandle[T]']: ...

    @abstractmethod
    async def send_async(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult['TaskHandle[T]']: ...

    @abstractmethod
    def schedule(
        self,
        delay: int,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult['TaskHandle[T]']: ...

    @abstractmethod
    def retry_send(
        self,
        error: TaskSendError,
    ) -> TaskSendResult['TaskHandle[T]']: ...

    @abstractmethod
    async def retry_send_async(
        self,
        error: TaskSendError,
    ) -> TaskSendResult['TaskHandle[T]']: ...

    @abstractmethod
    def retry_schedule(
        self,
        error: TaskSendError,
    ) -> TaskSendResult['TaskHandle[T]']: ...

    @abstractmethod
    def node(
        self,
        *,
        waits_for: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None = None,
        workflow_ctx_from: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None = None,
        args_from: dict[str, 'TaskNode[Any] | SubWorkflowNode[Any]'] | None = None,
        queue: str | None = None,
        priority: int | None = None,
        allow_failed_deps: bool = False,
        run_when: Callable[['WorkflowContext'], bool] | None = None,
        skip_when: Callable[['WorkflowContext'], bool] | None = None,
        join: Literal['all', 'any', 'quorum'] = 'all',
        min_success: int | None = None,
        good_until: datetime | None = None,
        node_id: str | None = None,
    ) -> 'NodeFactory[P, T]': ...


def create_task_wrapper(
    fn: Callable[P, TaskResult[T, TaskError]],
    app: 'Horsies',
    task_name: str,
    task_options: Optional['TaskOptions'] = None,
    *,
    exception_mapper: ExceptionMapper | None = None,
    default_unhandled_error_code: str | None = None,
) -> 'TaskFunction[P, T]':
    """
    Create a task wrapper for a specific app instance.
    Called by app.task() decorator.
    """

    hints = get_type_hints(fn)

    # Validate that return type is TaskResult[*, TaskError]
    fn_location = SourceLocation.from_function(fn)
    return_hint = hints.get('return')
    if return_hint is None:
        raise TaskDefinitionError(
            message='task function must declare an explicit return type',
            code=ErrorCode.TASK_NO_RETURN_TYPE,
            location=fn_location,
            notes=[f"function '{fn.__name__}' has no return type annotation"],
            help_text='add return type annotation: `-> TaskResult[YourType, TaskError]`',
        )

    if get_origin(return_hint) is not TaskResult:
        raise TaskDefinitionError(
            message='task function must return TaskResult',
            code=ErrorCode.TASK_INVALID_RETURN_TYPE,
            location=fn_location,
            notes=[
                f"function '{fn.__name__}' returns `{return_hint}`",
                'tasks must return TaskResult[T, TaskError]',
            ],
            help_text='change return type to `-> TaskResult[YourType, TaskError]`',
        )

    # Extract T and E from TaskResult[T, E] for runtime validation
    type_args = get_args(return_hint)
    if len(type_args) != 2:
        raise TaskDefinitionError(
            message='TaskResult must have exactly 2 type parameters',
            code=ErrorCode.TASK_INVALID_RETURN_TYPE,
            location=fn_location,
            notes=[
                f"function '{fn.__name__}' returns `{return_hint}`",
                'expected TaskResult[T, E] with exactly 2 type parameters',
            ],
            help_text='use `-> TaskResult[YourType, TaskError]`',
        )

    ok_type, err_type = type_args
    ok_type_adapter: TypeAdapter[Any] = TypeAdapter(ok_type)
    err_type_adapter: TypeAdapter[Any] = TypeAdapter(err_type)

    # Create a wrapper function that preserves the exact signature
    def wrapped_function(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskResult[T, TaskError]:
        try:
            result = fn(*args, **kwargs)

            if result is None:
                return TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.TASK_EXCEPTION,
                        message=f'Task {fn.__name__} returned None instead of TaskResult',
                        data={'task_name': task_name},
                    )
                )

            # Runtime type validation: validate result against declared types
            try:
                if result.is_ok():
                    # Validate ok value against T
                    ok_type_adapter.validate_python(result.ok)
                else:
                    # Validate err value against E
                    err_type_adapter.validate_python(result.err)
            except ValidationError as ve:
                # Type validation failed - return error result
                variant = 'ok' if result.is_ok() else 'err'
                expected_type = ok_type if result.is_ok() else err_type
                actual_value = result.ok if result.is_ok() else result.err
                return TaskResult(
                    err=TaskError(
                        exception=ve,
                        error_code=LibraryErrorCode.RETURN_TYPE_MISMATCH,
                        message=f'Task {fn.__name__} returned TaskResult({variant}={actual_value!r}) but expected type {expected_type}',
                        data={
                            'variant': variant,
                            'expected_type': str(expected_type),
                            'actual_value': str(actual_value),
                            'validation_errors': ve.errors(),
                        },
                    )
                )

            return result

        except KeyboardInterrupt:
            # Allow KeyboardInterrupt to propagate for graceful worker shutdown
            raise
        except WorkflowContextMissingIdError as e:
            error_result: TaskResult[T, TaskError] = TaskResult(
                err=TaskError(
                    exception=e,
                    error_code=LibraryErrorCode.WORKFLOW_CTX_MISSING_ID,
                    message=str(e),
                    data={'task_name': task_name},
                )
            )
            return error_result
        except BaseException as e:
            # Catch SystemExit, GeneratorExit, Exception, etc.
            error_code = resolve_exception_error_code(
                exc=e,
                task_mapper=exception_mapper,
                global_mapper=app.config.exception_mapper,
                task_default=default_unhandled_error_code,
                global_default=app.config.default_unhandled_error_code,
            )
            error_result: TaskResult[T, TaskError] = TaskResult(
                err=TaskError(
                    exception=e,
                    error_code=error_code,
                    message=f'Unhandled exception in task {fn.__name__}: {type(e).__name__}: {str(e)}',
                    data={'exception_type': type(e).__name__},
                )
            )
            return error_result

    # Pre-serialize task_options once so the send/send_async/schedule closures
    # below can reuse the JSON string instead of re-serializing on every call.
    _pre_serialized_options: str | None = None
    if task_options is not None:
        _opts_result = serialize_task_options(task_options)
        if is_err(_opts_result):
            raise ValueError(
                f'Failed to serialize task_options for {task_name}: {_opts_result.err_value}',
            )
        _pre_serialized_options = _opts_result.ok_value
    # ---- Broker-to-TaskSendError mapping ----

    def _map_broker_err(
        broker_err: BrokerOperationError,
        task_id: str,
        payload: TaskSendPayload,
    ) -> Err[TaskSendError]:
        """Map a BrokerOperationError to a TaskSendError."""
        # Detect SHA mismatch → PAYLOAD_MISMATCH
        if broker_err.code == BrokerErrorCode.PAYLOAD_MISMATCH:
            return Err(TaskSendError(
                code=TaskSendErrorCode.PAYLOAD_MISMATCH,
                message=broker_err.message,
                retryable=False,
                task_id=task_id,
                payload=payload,
                exception=broker_err.exception,
            ))
        return Err(TaskSendError(
            code=TaskSendErrorCode.ENQUEUE_FAILED,
            message=broker_err.message,
            retryable=broker_err.retryable,
            task_id=task_id,
            payload=payload,
            exception=broker_err.exception,
        ))

    # ---- Core send/schedule helpers ----

    def _do_send(
        task_id: str,
        payload: TaskSendPayload,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Core sync send logic. Used by send() and retry_send()."""
        try:
            broker = app.get_broker()
        except Exception as exc:
            return Err(TaskSendError(
                code=TaskSendErrorCode.ENQUEUE_FAILED,
                message=f'Failed to get broker for {task_name}: {exc}',
                retryable=False,
                task_id=task_id,
                payload=payload,
                exception=exc,
            ))

        last_err: TaskSendError | None = None
        max_attempts = 1 + (_SEND_RETRY_COUNT if app.config.resend_on_transient_err else 0)

        for attempt in range(max_attempts):
            if attempt > 0 and last_err is not None:
                delay_ms = min(
                    _SEND_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                    _SEND_RETRY_MAX_MS,
                )
                time.sleep(delay_ms / 1000.0)

            try:
                result = broker.enqueue(
                    task_name,
                    payload.queue_name,
                    task_id=task_id,
                    enqueue_sha=payload.enqueue_sha,
                    args_json=payload.args_json,
                    kwargs_json=payload.kwargs_json,
                    priority=payload.priority,
                    good_until=payload.good_until,
                    sent_at=payload.sent_at,
                    task_options=payload.task_options,
                )
            except Exception as exc:
                last_err = TaskSendError(
                    code=TaskSendErrorCode.ENQUEUE_FAILED,
                    message=f'Failed to enqueue task {task_name}: {exc}',
                    retryable=is_retryable_connection_error(exc),
                    task_id=task_id,
                    payload=payload,
                    exception=exc,
                )
                if last_err.retryable and attempt < max_attempts - 1:
                    continue
                return Err(last_err)

            if is_err(result):
                mapped = _map_broker_err(result.err_value, task_id, payload)
                send_err = mapped.err_value
                if send_err.retryable and attempt < max_attempts - 1:
                    last_err = send_err
                    continue
                return mapped

            return Ok(TaskHandle(result.ok_value, app, broker_mode=True))

        # Exhausted all attempts
        if last_err is not None:
            return Err(last_err)
        # Should not reach here, but satisfy type checker
        return Err(TaskSendError(
            code=TaskSendErrorCode.ENQUEUE_FAILED,
            message=f'Failed to enqueue task {task_name}: exhausted retry attempts',
            retryable=False,
            task_id=task_id,
            payload=payload,
        ))

    async def _do_send_async(
        task_id: str,
        payload: TaskSendPayload,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Core async send logic. Used by send_async() and retry_send_async()."""
        try:
            broker = app.get_broker()
        except Exception as exc:
            return Err(TaskSendError(
                code=TaskSendErrorCode.ENQUEUE_FAILED,
                message=f'Failed to get broker for {task_name}: {exc}',
                retryable=False,
                task_id=task_id,
                payload=payload,
                exception=exc,
            ))

        last_err: TaskSendError | None = None
        max_attempts = 1 + (_SEND_RETRY_COUNT if app.config.resend_on_transient_err else 0)

        for attempt in range(max_attempts):
            if attempt > 0 and last_err is not None:
                delay_ms = min(
                    _SEND_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                    _SEND_RETRY_MAX_MS,
                )
                await asyncio.sleep(delay_ms / 1000.0)

            try:
                result = await broker.enqueue_async(
                    task_name,
                    payload.queue_name,
                    task_id=task_id,
                    enqueue_sha=payload.enqueue_sha,
                    args_json=payload.args_json,
                    kwargs_json=payload.kwargs_json,
                    priority=payload.priority,
                    good_until=payload.good_until,
                    sent_at=payload.sent_at,
                    task_options=payload.task_options,
                )
            except Exception as exc:
                last_err = TaskSendError(
                    code=TaskSendErrorCode.ENQUEUE_FAILED,
                    message=f'Failed to enqueue task {task_name}: {exc}',
                    retryable=is_retryable_connection_error(exc),
                    task_id=task_id,
                    payload=payload,
                    exception=exc,
                )
                if last_err.retryable and attempt < max_attempts - 1:
                    continue
                return Err(last_err)

            if is_err(result):
                mapped = _map_broker_err(result.err_value, task_id, payload)
                send_err = mapped.err_value
                if send_err.retryable and attempt < max_attempts - 1:
                    last_err = send_err
                    continue
                return mapped

            return Ok(TaskHandle(result.ok_value, app, broker_mode=True))

        # Exhausted all attempts
        if last_err is not None:
            return Err(last_err)
        return Err(TaskSendError(
            code=TaskSendErrorCode.ENQUEUE_FAILED,
            message=f'Failed to enqueue task {task_name}: exhausted retry attempts',
            retryable=False,
            task_id=task_id,
            payload=payload,
        ))

    def _do_schedule(
        task_id: str,
        payload: TaskSendPayload,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Core sync schedule logic. Used by schedule() and retry_schedule()."""
        try:
            broker = app.get_broker()
        except Exception as exc:
            return Err(TaskSendError(
                code=TaskSendErrorCode.ENQUEUE_FAILED,
                message=f'Failed to get broker for {task_name}: {exc}',
                retryable=False,
                task_id=task_id,
                payload=payload,
                exception=exc,
            ))

        last_err: TaskSendError | None = None
        max_attempts = 1 + (_SEND_RETRY_COUNT if app.config.resend_on_transient_err else 0)

        for attempt in range(max_attempts):
            if attempt > 0 and last_err is not None:
                delay_ms = min(
                    _SEND_RETRY_INITIAL_MS * (2 ** (attempt - 1)),
                    _SEND_RETRY_MAX_MS,
                )
                time.sleep(delay_ms / 1000.0)

            try:
                result = broker.enqueue(
                    task_name,
                    payload.queue_name,
                    task_id=task_id,
                    enqueue_sha=payload.enqueue_sha,
                    args_json=payload.args_json,
                    kwargs_json=payload.kwargs_json,
                    priority=payload.priority,
                    good_until=payload.good_until,
                    sent_at=payload.sent_at,
                    enqueue_delay_seconds=payload.enqueue_delay_seconds,
                    task_options=payload.task_options,
                )
            except Exception as exc:
                last_err = TaskSendError(
                    code=TaskSendErrorCode.ENQUEUE_FAILED,
                    message=f'Failed to schedule task {task_name}: {exc}',
                    retryable=is_retryable_connection_error(exc),
                    task_id=task_id,
                    payload=payload,
                    exception=exc,
                )
                if last_err.retryable and attempt < max_attempts - 1:
                    continue
                return Err(last_err)

            if is_err(result):
                mapped = _map_broker_err(result.err_value, task_id, payload)
                send_err = mapped.err_value
                if send_err.retryable and attempt < max_attempts - 1:
                    last_err = send_err
                    continue
                return mapped

            return Ok(TaskHandle(result.ok_value, app, broker_mode=True))

        if last_err is not None:
            return Err(last_err)
        return Err(TaskSendError(
            code=TaskSendErrorCode.ENQUEUE_FAILED,
            message=f'Failed to schedule task {task_name}: exhausted retry attempts',
            retryable=False,
            task_id=task_id,
            payload=payload,
        ))

    # ---- Pre-send preparation ----

    def _prepare_send(
        args: tuple[Any, ...],
        kwargs_dict: dict[str, Any],
        enqueue_delay_seconds: int | None = None,
    ) -> TaskSendResult[tuple[str, TaskSendPayload]]:
        """Validate, serialize, and build a (task_id, TaskSendPayload) tuple.

        Returns Err(TaskSendError) on validation or serialization failure.
        """
        queue_name = task_options.queue_name if task_options else None
        try:
            validated_queue_name = app.validate_queue_name(queue_name)
            priority = effective_priority(app, validated_queue_name)
        except BaseException as exc:
            return Err(TaskSendError(
                code=TaskSendErrorCode.VALIDATION_FAILED,
                message=f'Queue validation failed for {task_name}: {exc}',
                retryable=False,
                exception=exc,
            ))

        task_id = str(uuid.uuid4())
        sent_at = datetime.now(timezone.utc)
        good_until = task_options.good_until if task_options else None

        # Serialize args
        args_json: str | None = None
        if args:
            args_r = args_to_json(args)
            if is_err(args_r):
                return Err(TaskSendError(
                    code=TaskSendErrorCode.VALIDATION_FAILED,
                    message=f'Failed to serialize args for {task_name}: {args_r.err_value}',
                    retryable=False,
                    task_id=task_id,
                    exception=args_r.err_value if isinstance(args_r.err_value, BaseException) else None,
                ))

            args_json = args_r.ok_value

        # Serialize kwargs
        kwargs_json: str | None = None
        if kwargs_dict:
            kwargs_r = kwargs_to_json(kwargs_dict)
            if is_err(kwargs_r):
                return Err(TaskSendError(
                    code=TaskSendErrorCode.VALIDATION_FAILED,
                    message=f'Failed to serialize kwargs for {task_name}: {kwargs_r.err_value}',
                    retryable=False,
                    task_id=task_id,
                    exception=kwargs_r.err_value if isinstance(kwargs_r.err_value, BaseException) else None,
                ))
            kwargs_json = kwargs_r.ok_value

        sha = enqueue_fingerprint(
            task_name=task_name,
            queue_name=validated_queue_name,
            priority=priority,
            args_json=args_json,
            kwargs_json=kwargs_json,
            sent_at=sent_at,
            good_until=good_until,
            enqueue_delay_seconds=enqueue_delay_seconds,
            task_options=_pre_serialized_options,
        )

        payload = TaskSendPayload(
            task_name=task_name,
            queue_name=validated_queue_name,
            priority=priority,
            args_json=args_json,
            kwargs_json=kwargs_json,
            sent_at=sent_at,
            good_until=good_until,
            enqueue_delay_seconds=enqueue_delay_seconds,
            task_options=_pre_serialized_options,
            enqueue_sha=sha,
        )
        return Ok((task_id, payload))

    # ---- Public send methods ----

    def send(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Execute task asynchronously via app's broker."""
        if app.are_sends_suppressed():
            return Err(TaskSendError(
                code=TaskSendErrorCode.SEND_SUPPRESSED,
                message=f'Task send suppressed for {task_name} during module import/discovery',
                retryable=False,
            ))

        prep = _prepare_send(args, kwargs)
        if is_err(prep):
            return prep
        task_id, payload = prep.ok_value
        return _do_send(task_id, payload)

    async def send_async(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Async variant for frameworks like FastAPI."""
        if app.are_sends_suppressed():
            return Err(TaskSendError(
                code=TaskSendErrorCode.SEND_SUPPRESSED,
                message=f'Task send (async) suppressed for {task_name} during module import/discovery',
                retryable=False,
            ))

        prep = _prepare_send(args, kwargs)
        if is_err(prep):
            return prep
        task_id, payload = prep.ok_value
        return await _do_send_async(task_id, payload)

    def schedule(
        delay: int,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Execute task asynchronously after a delay."""
        if app.are_sends_suppressed():
            return Err(TaskSendError(
                code=TaskSendErrorCode.SEND_SUPPRESSED,
                message=f'Task schedule suppressed for {task_name} during module import/discovery',
                retryable=False,
            ))

        prep = _prepare_send(args, kwargs, enqueue_delay_seconds=delay)
        if is_err(prep):
            return prep
        task_id, payload = prep.ok_value
        return _do_schedule(task_id, payload)

    # ---- Retry methods ----

    def _validate_retry(
        error: TaskSendError,
        *,
        require_delay: bool = False,
        reject_delay: bool = False,
    ) -> TaskSendResult[tuple[str, TaskSendPayload]]:
        """Validate a TaskSendError for retry eligibility.

        Returns Ok((task_id, payload)) or Err(TaskSendError(VALIDATION_FAILED)).
        """
        if error.code != TaskSendErrorCode.ENQUEUE_FAILED:
            return Err(TaskSendError(
                code=TaskSendErrorCode.VALIDATION_FAILED,
                message=f'retry is only valid for ENQUEUE_FAILED errors, got {error.code!r}',
                retryable=False,
            ))
        if error.payload is None or error.task_id is None:
            return Err(TaskSendError(
                code=TaskSendErrorCode.VALIDATION_FAILED,
                message='cannot retry: no payload or task_id on error',
                retryable=False,
            ))
        if error.payload.task_name != task_name:
            return Err(TaskSendError(
                code=TaskSendErrorCode.VALIDATION_FAILED,
                message=(
                    f'cross-task retry: error is for {error.payload.task_name!r} '
                    f'but this task is {task_name!r}'
                ),
                retryable=False,
            ))
        if require_delay and error.payload.enqueue_delay_seconds is None:
            return Err(TaskSendError(
                code=TaskSendErrorCode.VALIDATION_FAILED,
                message='cannot retry schedule: missing enqueue_delay_seconds',
                retryable=False,
            ))
        if reject_delay and error.payload.enqueue_delay_seconds is not None:
            return Err(TaskSendError(
                code=TaskSendErrorCode.VALIDATION_FAILED,
                message=(
                    'cannot retry scheduled task via retry_send — '
                    'use retry_schedule instead'
                ),
                retryable=False,
            ))
        return Ok((error.task_id, error.payload))

    def retry_send(
        error: TaskSendError,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Retry a failed send using the stored payload."""
        v = _validate_retry(error, reject_delay=True)
        if is_err(v):
            return v
        task_id, payload = v.ok_value
        return _do_send(task_id, payload)

    async def retry_send_async(
        error: TaskSendError,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Retry a failed async send using the stored payload."""
        v = _validate_retry(error, reject_delay=True)
        if is_err(v):
            return v
        task_id, payload = v.ok_value
        return await _do_send_async(task_id, payload)

    def retry_schedule(
        error: TaskSendError,
    ) -> TaskSendResult[TaskHandle[T]]:
        """Retry a failed schedule using the stored payload."""
        v = _validate_retry(error, require_delay=True)
        if is_err(v):
            return v
        task_id, payload = v.ok_value
        return _do_schedule(task_id, payload)

    class TaskFunctionImpl:
        def __init__(self) -> None:
            self.__name__ = fn.__name__
            self.__doc__ = fn.__doc__
            self.__annotations__ = fn.__annotations__
            self.task_name = task_name
            # Persist the declared queue (validated at definition time) so other components
            # (e.g., scheduler) can infer a task's home queue in CUSTOM mode.
            self.task_queue_name = task_options.queue_name if task_options else None
            # Keep a reference to the original function for introspection (signature checks).
            self._original_fn = fn
            # Expose declared TaskResult ok-type for workflow args_from type checks.
            self.task_ok_type = ok_type
            # Reuse the value already computed in the outer scope
            self.task_options_json: str | None = _pre_serialized_options
            # Expose mapper config for app.check() safety validation.
            # Frozen to prevent post-definition mutation.
            self.exception_mapper: Mapping[type[BaseException], str] | None = (
                MappingProxyType(exception_mapper) if exception_mapper is not None else None
            )
            self.default_unhandled_error_code: str | None = default_unhandled_error_code

        def __call__(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskResult[T, TaskError]:
            return wrapped_function(*args, **kwargs)

        def send(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskSendResult[TaskHandle[T]]:
            return send(*args, **kwargs)

        async def send_async(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskSendResult[TaskHandle[T]]:
            return await send_async(*args, **kwargs)

        def schedule(
            self,
            delay: int,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskSendResult[TaskHandle[T]]:
            return schedule(delay, *args, **kwargs)

        def retry_send(
            self,
            error: TaskSendError,
        ) -> TaskSendResult[TaskHandle[T]]:
            return retry_send(error)

        async def retry_send_async(
            self,
            error: TaskSendError,
        ) -> TaskSendResult[TaskHandle[T]]:
            return await retry_send_async(error)

        def retry_schedule(
            self,
            error: TaskSendError,
        ) -> TaskSendResult[TaskHandle[T]]:
            return retry_schedule(error)

        def node(
            self,
            *,
            waits_for: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None = None,
            workflow_ctx_from: Sequence['TaskNode[Any] | SubWorkflowNode[Any]'] | None = None,
            args_from: dict[str, 'TaskNode[Any] | SubWorkflowNode[Any]'] | None = None,
            queue: str | None = None,
            priority: int | None = None,
            allow_failed_deps: bool = False,
            run_when: Callable[['WorkflowContext'], bool] | None = None,
            skip_when: Callable[['WorkflowContext'], bool] | None = None,
            join: Literal['all', 'any', 'quorum'] = 'all',
            min_success: int | None = None,
            good_until: datetime | None = None,
            node_id: str | None = None,
        ) -> NodeFactory[P, T]:
            return NodeFactory(
                fn=self,  # type: ignore[arg-type]
                waits_for=waits_for,
                workflow_ctx_from=workflow_ctx_from,
                args_from=args_from,
                queue=queue,
                priority=priority,
                allow_failed_deps=allow_failed_deps,
                run_when=run_when,
                skip_when=skip_when,
                join=join,
                min_success=min_success,
                good_until=good_until,
                node_id=node_id,
            )

        # Copy metadata
        def __getattr__(self, name: str) -> Any:
            return getattr(wrapped_function, name)

    task_func = TaskFunctionImpl()

    return task_func
