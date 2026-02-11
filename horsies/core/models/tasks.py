# app/core/models/tasks.py
from __future__ import annotations
import datetime
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
    Optional,
    Literal,
    Self,
    Annotated,
    Union,
    overload,
)
from pydantic import BaseModel, ConfigDict, model_validator, Field
from pydantic.types import PositiveInt
from enum import Enum

if TYPE_CHECKING:
    from horsies.core.models.workflow import SubWorkflowSummary

from horsies.core.types.status import TaskStatus
from horsies.core.exception_mapper import validate_error_code_string

T = TypeVar('T')  # success payload
E = TypeVar('E')  # error payload (TaskError )


class _Unset:
    """Sentinel type for distinguishing 'not provided' from None."""

    __slots__ = ()


class LibraryErrorCode(str, Enum):
    """
    Library-defined error codes for infrastructure/runtime failures.

    These enumerate errors produced by the library runtime itself. User code
    should define domain-specific error codes as strings (e.g., "TOO_LARGE")
    or custom Enums for their own error categories.

    Categories:
    - Execution errors: UNHANDLED_EXCEPTION, TASK_EXCEPTION, WORKER_CRASHED
    - Retrieval errors: WAIT_TIMEOUT, TASK_NOT_FOUND, TASK_CANCELLED, RESULT_NOT_AVAILABLE
    - Broker errors: BROKER_ERROR
    - Worker errors: WORKER_RESOLUTION_ERROR, WORKER_SERIALIZATION_ERROR
    - Validation errors: RETURN_TYPE_MISMATCH, PYDANTIC_HYDRATION_ERROR
    - Lifecycle errors: SEND_SUPPRESSED
    - Workflow errors: UPSTREAM_SKIPPED, WORKFLOW_SUCCESS_CASE_NOT_MET,
      WORKFLOW_CONDITION_SKIP_WHEN_TRUE, WORKFLOW_CONDITION_RUN_WHEN_FALSE,
      WORKFLOW_CONDITION_EVALUATION_ERROR
    """

    # Execution errors
    UNHANDLED_EXCEPTION = 'UNHANDLED_EXCEPTION'
    TASK_EXCEPTION = 'TASK_EXCEPTION'
    WORKER_CRASHED = 'WORKER_CRASHED'

    # Retrieval errors (from handle.get() / get_async() / result_for())
    WAIT_TIMEOUT = 'WAIT_TIMEOUT'
    TASK_NOT_FOUND = 'TASK_NOT_FOUND'
    TASK_CANCELLED = 'TASK_CANCELLED'
    RESULT_NOT_AVAILABLE = 'RESULT_NOT_AVAILABLE'
    RESULT_NOT_READY = 'RESULT_NOT_READY'

    # Broker errors
    BROKER_ERROR = 'BROKER_ERROR'

    # Worker errors
    WORKER_RESOLUTION_ERROR = 'WORKER_RESOLUTION_ERROR'
    WORKER_SERIALIZATION_ERROR = 'WORKER_SERIALIZATION_ERROR'

    # Validation errors
    RETURN_TYPE_MISMATCH = 'RETURN_TYPE_MISMATCH'
    PYDANTIC_HYDRATION_ERROR = 'PYDANTIC_HYDRATION_ERROR'

    # Lifecycle errors
    SEND_SUPPRESSED = 'SEND_SUPPRESSED'

    # Workflow errors
    UPSTREAM_SKIPPED = 'UPSTREAM_SKIPPED'
    WORKFLOW_CTX_MISSING_ID = 'WORKFLOW_CTX_MISSING_ID'
    WORKFLOW_SUCCESS_CASE_NOT_MET = 'WORKFLOW_SUCCESS_CASE_NOT_MET'
    WORKFLOW_CONDITION_SKIP_WHEN_TRUE = 'WORKFLOW_CONDITION_SKIP_WHEN_TRUE'
    WORKFLOW_CONDITION_RUN_WHEN_FALSE = 'WORKFLOW_CONDITION_RUN_WHEN_FALSE'
    WORKFLOW_CONDITION_EVALUATION_ERROR = 'WORKFLOW_CONDITION_EVALUATION_ERROR'


class TaskError(BaseModel):
    """
    The error payload for a TaskResult.
    A task error can be returned by:
    - a task function (e.g. `return TaskResult(err=TaskError(...))`)
    - library failure (e.g. execution error, serialization error, etc.)
    """

    model_config = {'arbitrary_types_allowed': True}

    exception: Optional[dict[str, Any] | BaseException] = None
    # Library internal errors use LibraryErrorCode; user errors use str.
    error_code: Optional[Union[LibraryErrorCode, str]] = None
    data: Optional[Any] = None
    message: Optional[str] = None


class SubWorkflowError(TaskError):
    """
    Error representing a failed subworkflow.

    Allows parent tasks to distinguish subworkflow failures from regular
    task failures via pattern matching:

        match result.err:
            case SubWorkflowError() as e:
                print(f"Subworkflow {e.sub_workflow_id} failed")
            case TaskError() as e:
                print(f"Task error: {e.message}")
    """

    sub_workflow_id: str
    sub_workflow_summary: 'SubWorkflowSummary[Any]'


_UNSET: _Unset = _Unset()


class TaskResult(Generic[T, E]):
    """
    Discriminated union style result: exactly one of ok / err is set.
    Supports None as a valid success value (e.g., TaskResult[None, TaskError]).

    Internally uses tuple-based discriminated union for type narrowing:
    - (True, value) for success
    - (False, error) for failure
    """

    __slots__ = ('_data',)
    _data: tuple[Literal[True], T] | tuple[Literal[False], E]

    @overload
    def __init__(self, *, ok: T) -> None: ...

    @overload
    def __init__(self, *, err: E) -> None: ...

    def __init__(
        self,
        *,
        ok: T | _Unset = _UNSET,
        err: E | _Unset = _UNSET,
    ) -> None:
        ok_provided = not isinstance(ok, _Unset)
        err_provided = not isinstance(err, _Unset)

        if ok_provided and err_provided:
            raise ValueError('TaskResult cannot have both ok and err')
        if not ok_provided and not err_provided:
            raise ValueError('TaskResult must have exactly one of ok / err')

        # isinstance narrowing for assignment
        if not isinstance(ok, _Unset):
            self._data = (True, ok)
        elif not isinstance(err, _Unset):
            self._data = (False, err)
        else:
            raise ValueError('TaskResult must have exactly one of ok / err')

    # helpers
    def is_ok(self) -> bool:
        return self._data[0]

    def is_err(self) -> bool:
        return not self._data[0]

    @property
    def ok(self) -> T | None:
        """Access the success value, or None if this is an error result."""
        match self._data:
            case (True, value):
                return value
            case (False, _):
                return None

    @property
    def err(self) -> E | None:
        """Access the error value, or None if this is a success result."""
        match self._data:
            case (False, error):
                return error
            case (True, _):
                return None

    def unwrap(self) -> T:
        """Get the success value. Raises if result is error."""
        match self._data:
            case (True, value):
                return value
            case (False, _):
                raise ValueError('Result is not ok - check is_ok() first')

    def unwrap_err(self) -> E:
        """Get the error value. Raises if result is success."""
        match self._data:
            case (False, error):
                return error
            case (True, _):
                raise ValueError('Result is not error - check is_err() first')

    @property
    def ok_value(self) -> T:
        """Get the success value. Raises if result is error."""
        match self._data:
            case (True, value):
                return value
            case (False, _):
                raise ValueError('Result is not ok - check is_ok() first')

    @property
    def err_value(self) -> E:
        """Get the error value. Raises if result is success."""
        match self._data:
            case (False, error):
                return error
            case (True, _):
                raise ValueError('Result is not error - check is_err() first')


@dataclass
class TaskInfo:
    """Metadata for a broker-backed task."""

    task_id: str
    task_name: str
    status: TaskStatus
    queue_name: str
    priority: int
    retry_count: int
    max_retries: int
    next_retry_at: datetime.datetime | None
    sent_at: datetime.datetime | None
    claimed_at: datetime.datetime | None
    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None
    failed_at: datetime.datetime | None
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None
    result: TaskResult[Any, TaskError] | None = None
    failed_reason: str | None = None


class RetryPolicy(BaseModel):
    """
    Retry policy configuration for tasks.

    Two strategies supported:
    1. Fixed: Uses intervals list exactly as specified
    2. Exponential: Uses intervals[0] as base, exponentially increases

    Fields:
        max_retries: maximum number of retry attempts (initial send not counted)
        intervals: delay intervals in seconds between retry attempts
        backoff_strategy: 'fixed' uses intervals as-is, 'exponential' uses intervals[0] as base
        jitter: whether to add ±25% randomization to delays
        auto_retry_for: error codes that trigger automatic retries
    """

    model_config = ConfigDict(extra='forbid')

    max_retries: Annotated[
        int, Field(ge=1, le=20, description='Number of retry attempts (1-20)')
    ] = 3
    intervals: Annotated[
        list[
            Annotated[
                PositiveInt,
                Field(le=86400, description='Retry interval in seconds (1-86400)'),
            ]
        ],
        Field(min_length=1, max_length=20, description='List of retry intervals'),
    ] = [60, 300, 900]  # seconds: 1min, 5min, 15min
    backoff_strategy: Literal['fixed', 'exponential'] = 'fixed'
    jitter: bool = True
    auto_retry_for: Annotated[
        list[Union[str, LibraryErrorCode]],
        Field(min_length=1),
    ]

    @model_validator(mode='after')
    def validate_strategy_consistency(self) -> Self:
        """Validate that backoff strategy is consistent with intervals configuration."""

        if self.backoff_strategy == 'fixed':
            # Fixed strategy: intervals length should match max_retries
            if len(self.intervals) != self.max_retries:
                raise ValueError(
                    f'Fixed backoff strategy requires intervals length ({len(self.intervals)}) '
                    f'to match max_retries ({self.max_retries}). '
                    f'Either adjust intervals list or use exponential strategy.'
                )

        elif self.backoff_strategy == 'exponential':
            # Exponential strategy: should have exactly one base interval
            if len(self.intervals) != 1:
                raise ValueError(
                    f'Exponential backoff strategy requires exactly one base interval, '
                    f'got {len(self.intervals)} intervals. Use intervals=[base_seconds] for exponential backoff.'
                )

        return self

    @model_validator(mode='after')
    def validate_error_code_fields(self) -> Self:
        """Validate that auto_retry_for entries are valid error code strings."""
        for entry in self.auto_retry_for:
            code = entry.value if isinstance(entry, LibraryErrorCode) else entry
            err = validate_error_code_string(code, field_name='auto_retry_for')
            if err is not None:
                raise ValueError(err)
        return self

    # Convenience constructors to prevent misconfiguration at call sites
    @classmethod
    def fixed(
        cls,
        intervals: list[int],
        *,
        auto_retry_for: list[Union[str, LibraryErrorCode]],
        jitter: bool = True,
    ) -> 'RetryPolicy':
        """Create a fixed backoff policy where intervals length defines max_retries."""
        return cls(
            max_retries=len(intervals),
            intervals=intervals,
            backoff_strategy='fixed',
            jitter=jitter,
            auto_retry_for=auto_retry_for,
        )

    @classmethod
    def exponential(
        cls,
        base_seconds: int,
        *,
        max_retries: int,
        auto_retry_for: list[Union[str, LibraryErrorCode]],
        jitter: bool = True,
    ) -> 'RetryPolicy':
        """Create an exponential backoff policy using a single base interval.

        The policy uses base_seconds * 2**(attempt-1) per attempt.
        """
        return cls(
            max_retries=max_retries,
            intervals=[base_seconds],
            backoff_strategy='exponential',
            jitter=jitter,
            auto_retry_for=auto_retry_for,
        )


class TaskOptions(BaseModel):
    """
    Options for a task.

    Fields:
        task_name: Unique task identifier (mandatory - decoupled from function names)
        queue_name: Target queue name (validated against app config at definition time)
        good_until: Task expiry deadline (task skipped if not claimed by this time)
        retry_policy: Retry timing and backoff configuration (includes auto_retry_for)
    """

    model_config = ConfigDict(extra='forbid')

    task_name: str
    queue_name: Optional[str] = None
    good_until: Optional[datetime.datetime] = None
    retry_policy: Optional[RetryPolicy] = None


# Rebuild SubWorkflowError to resolve forward reference to SubWorkflowSummary
def _rebuild_subworkflow_error() -> None:
    """Rebuild SubWorkflowError after SubWorkflowSummary is importable."""
    from horsies.core.models.workflow import SubWorkflowSummary

    SubWorkflowError.model_rebuild()


_rebuild_subworkflow_error()
