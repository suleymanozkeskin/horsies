# app/core/task_decorator.py
from __future__ import annotations
import asyncio
from typing import (
    Callable,
    get_origin,
    get_type_hints,
    get_args,
    ParamSpec,
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

from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.models.workflow import WorkflowContextMissingIdError
from horsies.core.errors import TaskDefinitionError, ErrorCode, SourceLocation

P = ParamSpec('P')
T = TypeVar('T')
E = TypeVar('E')


def effective_priority(
    app: 'Horsies',
    queue_name: str,
) -> int:
    if app.config.queue_mode.name == 'DEFAULT':
        return 100  # default priority, least important
    config = next(q for q in (app.config.custom_queues or []) if q.name == queue_name)
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

    def set_immediate_result(
        self,
        result: TaskResult[T, TaskError],
    ) -> None:
        """Internal method to set result for synchronous execution"""
        self._cached_result = result
        self._result_fetched = True


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


def create_task_wrapper(
    fn: Callable[P, TaskResult[T, TaskError]],
    app: 'Horsies',
    task_name: str,
    task_options: Optional['TaskOptions'] = None,
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
            error_result: TaskResult[T, TaskError] = TaskResult(
                err=TaskError(
                    exception=e,
                    error_code=LibraryErrorCode.UNHANDLED_EXCEPTION,
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

        # Copy metadata
        def __getattr__(self, name: str) -> Any:
            return getattr(wrapped_function, name)

    task_func = TaskFunctionImpl()

    return task_func
