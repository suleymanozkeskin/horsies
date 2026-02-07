# app/core/task_decorator.py
from __future__ import annotations
import asyncio
from typing import (
    Callable,
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
from datetime import datetime, timedelta, timezone
from pydantic import TypeAdapter, ValidationError
from horsies.core.codec.serde import serialize_task_options

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
from horsies.core.errors import TaskDefinitionError, ConfigurationError, ErrorCode, SourceLocation

P = ParamSpec('P')
T = TypeVar('T')
E = TypeVar('E')


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
    ) -> 'TaskInfo | None':
        """Fetch metadata for this task from the broker."""
        if not self._broker_mode or not self._app:
            raise RuntimeError(
                'TaskHandle.info() requires a broker-backed task handle '
                '(use .send() or .send_async())'
            )

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
    ) -> 'TaskInfo | None':
        """Async version of info()."""
        if not self._broker_mode or not self._app:
            raise RuntimeError(
                'TaskHandle.info_async() requires a broker-backed task handle '
                '(use .send() or .send_async())'
            )

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

        return TaskNode(
            fn=self._fn,
            args=args,
            kwargs=dict(kwargs),
            waits_for=list(self._waits_for) if self._waits_for else [],
            workflow_ctx_from=list(self._workflow_ctx_from)
            if self._workflow_ctx_from
            else None,
            args_from=dict(self._args_from) if self._args_from else {},
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
    ) -> 'TaskHandle[T]': ...

    @abstractmethod
    async def send_async(
        self,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> 'TaskHandle[T]': ...

    @abstractmethod
    def schedule(
        self,
        delay: int,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> 'TaskHandle[T]': ...

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

    def _immediate_error_handle(
        exception: BaseException,
        message: str,
    ) -> TaskHandle[T]:
        """Create a handle that already contains an error TaskResult."""
        handle: TaskHandle[T] = TaskHandle('<error>', app, broker_mode=False)
        handle.set_immediate_result(
            TaskResult(
                err=TaskError(
                    exception=exception,
                    error_code=LibraryErrorCode.UNHANDLED_EXCEPTION,
                    message=message,
                    data={
                        'task_name': task_name,
                        'exception_type': type(exception).__name__,
                    },
                )
            )
        )
        return handle

    # Create a wrapper function that preserves the exact signature
    def wrapped_function(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskResult[T, TaskError]:
        try:
            result = fn(*args, **kwargs)

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

    def send(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskHandle[T]:
        """Execute task asynchronously via app's broker."""
        # Prevent import side-effects: if the worker is importing modules for
        # discovery, suppress enqueuing and return an immediate error result.
        if hasattr(app, 'are_sends_suppressed') and app.are_sends_suppressed():
            try:
                app.logger.warning(
                    'Send suppressed for %s during module import/discovery; no task enqueued',
                    task_name,
                )
            except Exception:
                pass
            suppressed_handle: TaskHandle[T] = TaskHandle('<suppressed>')
            suppressed_handle.set_immediate_result(
                TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.SEND_SUPPRESSED,
                        message='Task send suppressed during module import/discovery',
                        data={'task_name': task_name},
                    )
                )
            )
            return suppressed_handle
        # VALIDATION AT EXECUTION TIME
        # Re-validate queue_name to catch any configuration changes
        queue_name = task_options.queue_name if task_options else None
        try:
            validated_queue_name = app.validate_queue_name(queue_name)
            priority = effective_priority(app, validated_queue_name)
        except BaseException as e:
            return _immediate_error_handle(
                e,
                f'Task execution error for {fn.__name__}: {e}',
            )

        try:
            broker = app.get_broker()
            good_until = task_options.good_until if task_options else None

            task_options_json = None
            if task_options:
                task_options_json = serialize_task_options(task_options)

            task_id = broker.enqueue(
                task_name,
                args,
                kwargs,
                validated_queue_name,
                priority=priority,
                good_until=good_until,
                task_options=task_options_json,
            )
            return TaskHandle(task_id, app, broker_mode=True)
        except BaseException as e:
            return _immediate_error_handle(
                e,
                f'Failed to enqueue task {fn.__name__}: {e}',
            )

    async def send_async(
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskHandle[T]:
        """Async variant for frameworks like FastAPI."""
        if hasattr(app, 'are_sends_suppressed') and app.are_sends_suppressed():
            try:
                app.logger.warning(
                    'Send (async) suppressed for %s during module import/discovery; no task enqueued',
                    task_name,
                )
            except Exception:
                pass
            suppressed_handle: TaskHandle[T] = TaskHandle('<suppressed>')
            suppressed_handle.set_immediate_result(
                TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.SEND_SUPPRESSED,
                        message='Task send suppressed during module import/discovery',
                        data={'task_name': task_name},
                    )
                )
            )
            return suppressed_handle
        queue_name = task_options.queue_name if task_options else None
        try:
            validated = app.validate_queue_name(queue_name)
            priority = effective_priority(app, validated)
        except BaseException as e:
            return _immediate_error_handle(
                e,
                f'Task execution error for {fn.__name__}: {e}',
            )
        try:
            broker = app.get_broker()
            good_until = task_options.good_until if task_options else None

            task_options_json = None
            if task_options:
                task_options_json = serialize_task_options(task_options)

            task_id = await broker.enqueue_async(
                task_name,
                args,
                kwargs,
                validated,
                priority=priority,
                good_until=good_until,
                task_options=task_options_json,
            )
            return TaskHandle(task_id, app, broker_mode=True)
        except BaseException as e:
            return _immediate_error_handle(
                e,
                f'Failed to enqueue task {fn.__name__}: {e}',
            )

    def schedule(
        delay: int,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskHandle[T]:
        """Execute task asynchronously after a delay."""
        if hasattr(app, 'are_sends_suppressed') and app.are_sends_suppressed():
            try:
                app.logger.warning(
                    'Schedule suppressed for %s during module import/discovery; no task enqueued',
                    task_name,
                )
            except Exception:
                pass
            suppressed_handle: TaskHandle[T] = TaskHandle('<suppressed>')
            suppressed_handle.set_immediate_result(
                TaskResult(
                    err=TaskError(
                        error_code=LibraryErrorCode.SEND_SUPPRESSED,
                        message='Task schedule suppressed during module import/discovery',
                        data={'task_name': task_name},
                    )
                )
            )
            return suppressed_handle
        # VALIDATION AT EXECUTION TIME
        # Re-validate queue_name to catch any configuration changes
        queue_name = task_options.queue_name if task_options else None
        try:
            validated_queue_name = app.validate_queue_name(queue_name)
            priority = effective_priority(app, validated_queue_name)
        except BaseException as e:
            return _immediate_error_handle(
                e,
                f'Task execution error for {fn.__name__}: {e}',
            )

        try:
            broker = app.get_broker()
            good_until = task_options.good_until if task_options else None
            sent_at = datetime.now(timezone.utc) + timedelta(seconds=delay)

            task_options_json = None
            if task_options:
                task_options_json = serialize_task_options(task_options)

            task_id = broker.enqueue(
                task_name,
                args,
                kwargs,
                validated_queue_name,
                priority=priority,
                good_until=good_until,
                sent_at=sent_at,
                task_options=task_options_json,
            )
            return TaskHandle(task_id, app, broker_mode=True)
        except BaseException as e:
            return _immediate_error_handle(
                e,
                f'Failed to schedule task {fn.__name__}: {e}',
            )

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
            # Pre-serialize task_options so workflow engine can access retry config
            self.task_options_json: str | None = (
                serialize_task_options(task_options) if task_options else None
            )
            # Expose mapper config for app.check() safety validation.
            self.exception_mapper: ExceptionMapper | None = exception_mapper
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
        ) -> TaskHandle[T]:
            return send(*args, **kwargs)

        async def send_async(
            self,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskHandle[T]:
            return await send_async(*args, **kwargs)

        def schedule(
            self,
            delay: int,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> TaskHandle[T]:
            return schedule(delay, *args, **kwargs)

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
