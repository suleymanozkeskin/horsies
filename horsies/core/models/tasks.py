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
from pydantic import BaseModel, ConfigDict, field_validator, model_validator, Field
from pydantic.types import PositiveInt
from enum import Enum

if TYPE_CHECKING:
    from horsies.core.models.workflow import SubWorkflowSummary

from horsies.core.types.status import TaskStatus, TaskAttemptOutcome
from horsies.core.exception_mapper import validate_error_code_string

T = TypeVar('T')  # success payload
E = TypeVar('E')  # error payload (TaskError )


class _Unset:
    """Sentinel type for distinguishing 'not provided' from None."""

    __slots__ = ()


class OperationalErrorCode(str, Enum):
    """Something actually broke in execution.

    Typical caller behavior: log, retry conditionally, inspect infra/runtime failure.
    """

    UNHANDLED_EXCEPTION = 'UNHANDLED_EXCEPTION'
    TASK_EXCEPTION = 'TASK_EXCEPTION'
    WORKER_CRASHED = 'WORKER_CRASHED'
    BROKER_ERROR = 'BROKER_ERROR'
    WORKER_RESOLUTION_ERROR = 'WORKER_RESOLUTION_ERROR'
    WORKER_SERIALIZATION_ERROR = 'WORKER_SERIALIZATION_ERROR'
    RESULT_DESERIALIZATION_ERROR = 'RESULT_DESERIALIZATION_ERROR'
    WORKFLOW_ENQUEUE_FAILED = 'WORKFLOW_ENQUEUE_FAILED'
    SUBWORKFLOW_LOAD_FAILED = 'SUBWORKFLOW_LOAD_FAILED'


class ContractCode(str, Enum):
    """A code/typing/usage contract was violated.

    Typical caller behavior: fix code or type assumptions, do not blindly retry.
    """

    RETURN_TYPE_MISMATCH = 'RETURN_TYPE_MISMATCH'
    PYDANTIC_HYDRATION_ERROR = 'PYDANTIC_HYDRATION_ERROR'
    WORKFLOW_CTX_MISSING_ID = 'WORKFLOW_CTX_MISSING_ID'


class RetrievalCode(str, Enum):
    """The caller cannot get the value right now or cannot locate it.

    Typical caller behavior: wait, poll again, handle missing value as normal flow.
    """

    WAIT_TIMEOUT = 'WAIT_TIMEOUT'
    TASK_NOT_FOUND = 'TASK_NOT_FOUND'
    WORKFLOW_NOT_FOUND = 'WORKFLOW_NOT_FOUND'
    RESULT_NOT_AVAILABLE = 'RESULT_NOT_AVAILABLE'
    RESULT_NOT_READY = 'RESULT_NOT_READY'


class OutcomeCode(str, Enum):
    """A non-bug execution outcome or control-flow result.

    Typical caller behavior: branch on outcome, continue control flow.
    """

    TASK_CANCELLED = 'TASK_CANCELLED'
    WORKFLOW_PAUSED = 'WORKFLOW_PAUSED'
    WORKFLOW_FAILED = 'WORKFLOW_FAILED'
    WORKFLOW_CANCELLED = 'WORKFLOW_CANCELLED'
    UPSTREAM_SKIPPED = 'UPSTREAM_SKIPPED'
    SUBWORKFLOW_FAILED = 'SUBWORKFLOW_FAILED'
    WORKFLOW_SUCCESS_CASE_NOT_MET = 'WORKFLOW_SUCCESS_CASE_NOT_MET'


type BuiltInTaskCode = (
    OperationalErrorCode
    | ContractCode
    | RetrievalCode
    | OutcomeCode
)

BUILTIN_CODE_REGISTRY: dict[str, BuiltInTaskCode] = {}
"""Maps every built-in error code string to its canonical enum member.

Used by ``TaskError._coerce_error_code`` for round-trip coercion and by
``horsies check`` for reserved-code collision detection.
"""
for _family in (
    OperationalErrorCode,
    ContractCode,
    RetrievalCode,
    OutcomeCode,
):
    for _member in _family:
        if _member.value in BUILTIN_CODE_REGISTRY:
            _existing = BUILTIN_CODE_REGISTRY[_member.value]
            raise RuntimeError(
                f'Duplicate built-in error code {_member.value!r}: '
                f'{type(_existing).__name__}.{_existing.name} and '
                f'{type(_member).__name__}.{_member.name}',
            )
        BUILTIN_CODE_REGISTRY[_member.value] = _member


class TaskError(BaseModel):
    """
    The error payload for a TaskResult.

    A task error can be returned by:
    - a task function (e.g. ``return TaskResult(err=TaskError(...))``)
    - library failure (e.g. execution error, serialization error, etc.)

    ``error_code`` carries one of four built-in families
    (``OperationalErrorCode``, ``ContractCode``, ``RetrievalCode``,
    ``OutcomeCode``) or a plain user-defined ``str``.

    **Construction rule**: built-in code values must be passed as enum
    members, not raw strings.  ``TaskError(error_code="BROKER_ERROR")``
    raises ``ValueError``.  Use ``OperationalErrorCode.BROKER_ERROR``
    instead.

    **Rehydration**: use ``TaskError.from_persisted(data)`` or
    ``TaskError.from_persisted_json(raw)`` to restore payloads from
    JSON / DB storage.  Those paths coerce reserved strings back to
    enum members.

    **Important**: ``model_validate()`` and ``model_validate_json()``
    are not supported for rehydrating persisted payloads that contain
    built-in error code strings.  They enforce the strict construction
    rule and will reject reserved strings.  Always use
    ``from_persisted()`` / ``from_persisted_json()`` for stored data.
    """

    model_config = {'arbitrary_types_allowed': True}

    exception: Optional[dict[str, Any] | BaseException] = None
    error_code: Optional[Union[BuiltInTaskCode, str]] = None
    data: Optional[Any] = None
    message: Optional[str] = None

    @field_validator('error_code', mode='before')
    @classmethod
    def _reject_reserved_strings(cls, value: object) -> object:
        """Reject string values that match reserved built-in codes.

        Built-in codes must be passed as enum members from the four
        families, not as raw strings or user-defined str-Enum subclasses.
        Rehydration from persisted data should use ``from_persisted()``
        or ``from_persisted_json()`` which coerce instead of rejecting.
        """
        # Allow built-in BuiltInTaskCode enum members (they are str subclasses).
        # Reject any other str-like value that matches a reserved code,
        # including plain str, str subclasses, and foreign str-Enum types.
        _BUILTIN_FAMILIES = (
            OperationalErrorCode, ContractCode, RetrievalCode, OutcomeCode,
        )
        if isinstance(value, _BUILTIN_FAMILIES):
            return value
        if isinstance(value, str) and value in BUILTIN_CODE_REGISTRY:
            builtin = BUILTIN_CODE_REGISTRY[value]
            raise ValueError(
                f"'{value}' is a reserved built-in error code "
                f"({type(builtin).__name__}.{builtin.name}). "
                f"Pass the enum member directly, or use "
                f"TaskError.from_persisted() for stored payloads.",
            )
        return value

    @classmethod
    def from_persisted(cls, data: dict[str, Any]) -> 'TaskError':
        """Restore a TaskError from a persisted dict payload.

        Coerces reserved built-in strings back to enum members, then
        validates all fields through Pydantic.  Malformed payloads
        raise ``ValidationError`` just like ``model_validate()``.
        """
        coerced = dict(data)
        raw_code = coerced.get('error_code')
        if isinstance(raw_code, str) and raw_code in BUILTIN_CODE_REGISTRY:
            coerced['error_code'] = BUILTIN_CODE_REGISTRY[raw_code]
        # Use model_construct only for the coerced error_code, then validate.
        # We pass the already-coerced enum member so _reject_reserved_strings
        # sees an Enum, not a plain str.
        return cls.model_validate(coerced)

    @classmethod
    def from_persisted_json(cls, raw: str) -> 'TaskError':
        """Restore a TaskError from a persisted JSON string.

        Coerces reserved built-in strings back to enum members, then
        validates all fields through Pydantic.
        """
        import json

        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError(f'Expected dict, got {type(data).__name__}')
        return cls.from_persisted(data)


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
class TaskAttemptInfo:
    """Metadata for a single task execution attempt."""

    task_id: str
    attempt: int
    outcome: TaskAttemptOutcome
    will_retry: bool
    started_at: datetime.datetime
    finished_at: datetime.datetime
    error_code: str | None = None
    error_message: str | None = None
    failed_reason: str | None = None
    worker_id: str | None = None
    worker_hostname: str | None = None
    worker_pid: int | None = None
    worker_process_name: str | None = None


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
    enqueued_at: datetime.datetime
    claimed_at: datetime.datetime | None
    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None
    failed_at: datetime.datetime | None
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None
    error_code: str | None = None
    result: TaskResult[Any, TaskError] | None = None
    failed_reason: str | None = None
    attempts: list[TaskAttemptInfo] | None = None


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
        list[Union[str, BuiltInTaskCode]],
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
            code = entry.value if isinstance(entry, Enum) else entry
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
        auto_retry_for: list[Union[str, BuiltInTaskCode]],
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
        auto_retry_for: list[Union[str, BuiltInTaskCode]],
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

    @field_validator('good_until')
    @classmethod
    def reject_naive_good_until(cls, v: datetime.datetime | None) -> datetime.datetime | None:
        """Reject naive datetimes — they are ambiguous and break fingerprinting."""
        if v is not None and v.tzinfo is None:
            raise ValueError(
                'good_until must be timezone-aware (got naive datetime). '
                'Use e.g. datetime.now(timezone.utc) or a tz-aware value.',
            )
        return v


# Rebuild SubWorkflowError to resolve forward reference to SubWorkflowSummary
def _rebuild_subworkflow_error() -> None:
    """Rebuild SubWorkflowError after SubWorkflowSummary is importable."""
    from horsies.core.models.workflow import SubWorkflowSummary  # noqa: F811

    _ = SubWorkflowSummary  # keep in scope for model_rebuild forward ref resolution

    SubWorkflowError.model_rebuild()


_rebuild_subworkflow_error()
