# app/core/models/tasks.py
from __future__ import annotations
import datetime
from dataclasses import dataclass
from uuid import UUID
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    TypedDict,
    TypeVar,
    Optional,
    Literal,
    Self,
    Annotated,
    Final,
    cast,
    overload,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SkipValidation,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.types import PositiveInt
from enum import Enum

if TYPE_CHECKING:
    from horsies.core.models.outcome import TaskOutcome
    from horsies.core.models.workflow import SubWorkflowSummary

from horsies._catalog.task_bounds import (
    MAX_TASK_TIMEOUT_MS as MAX_TASK_TIMEOUT_MS,
    MIN_TASK_TIMEOUT_MS as MIN_TASK_TIMEOUT_MS,
)
from horsies._catalog.task_codes import ContractCode as ContractCode
from horsies._catalog.task_codes import OperationalCode as OperationalCode
from horsies._catalog.task_codes import OutcomeCode as OutcomeCode
from horsies._catalog.task_codes import RetrievalCode as RetrievalCode
from horsies.core.types.json_value import JsonInput, JsonValue
from horsies.core.types.status import TaskStatus, TaskAttemptOutcome
from horsies.core.exception_mapper import (
    RESERVED_NON_TASK_CODES,
    reserved_code_message,
)

type BuiltInTaskCode = (
    OperationalCode
    | ContractCode
    | RetrievalCode
    | OutcomeCode
)

BUILTIN_CODE_REGISTRY: dict[str, BuiltInTaskCode] = {}
"""Maps every built-in error code string to its canonical enum member.

Used by ``TaskError._validate_code`` for round-trip coercion and by
``horsies check`` for reserved-code collision detection.
"""
for _family in (
    OperationalCode,
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


_BUILTIN_FAMILIES = (
    OperationalCode, ContractCode, RetrievalCode, OutcomeCode,
)

BUILTIN_FAMILIES: Final = _BUILTIN_FAMILIES
"""The four enum classes of the built-in codes, for an ``isinstance`` check."""


C = TypeVar('C', covariant=True, default=str)
"""The type of the custom codes of a `TaskError`: `str`, or the `Enum` class
that the task declares with `error_codes=`."""


class ExceptionRecord(TypedDict):
    """Wire-safe dict form of a ``BaseException``.

    Produced by ``flatten_exception`` (``codec/error_payload.py``) before
    a ``TaskError`` carrying a live exception is encoded. Keeps the
    legacy ``type`` / ``message`` / ``traceback`` triple that downstream
    tooling depends on and adds ``module`` / ``repr`` for class
    disambiguation.
    """

    type: str
    module: str
    message: str
    repr: str
    traceback: str


if TYPE_CHECKING:

    class _ErrorData:
        """The static type of `TaskError.data`. It is not a run-time object.

        A field holds one type at run time. A descriptor gives a type checker two, and
        a subclass inherits both: `__set__` is the type of the `__init__` parameter.
        """

        @overload
        def __get__(self, instance: None, owner: type[Any], /) -> Self: ...

        @overload
        def __get__(self, instance: object, owner: type[Any], /) -> JsonValue: ...

        def __get__(self, instance: object | None, owner: type[Any], /) -> Any: ...

        def __set__(self, instance: object, value: JsonInput, /) -> None: ...


class TaskError(BaseModel, Generic[C]):
    """The error of a task: the ``Err`` of what a body returns, and the
    ``Failed`` of what a reader gets.

    ``code`` carries one of four built-in families
    (``OperationalCode``, ``ContractCode``, ``RetrievalCode``,
    ``OutcomeCode``) or a custom code of the task: a ``str``, or a member of
    the ``Enum`` class that the task declares with ``error_codes=``.
    ``TaskError[C]`` is generic in the type of the custom code, with the
    default ``str``: ``TaskError`` is ``TaskError[str]``. A task that returns
    ``Result[T, TaskError[E]]`` declares ``error_codes=E``; its body passes
    members of ``E``, and its readers get members of ``E``.

    **Required field**: ``code`` is required and must not be null, on
    the wire and in the type, because retry matching and outcome routing read
    it. A stored error without a code is stored data that does not parse.
    ``message``, ``data`` and ``exception`` are optional.

    **Construction**: built-in code values must be passed as enum
    members, not raw strings.  ``TaskError(code="DATABASE_ERROR",
    message="...")`` raises ``ValueError``.  Use ``OperationalCode.DATABASE_ERROR``
    instead.  A custom code is a ``str`` or a member of a custom ``Enum``
    whose value is that ``str``; both follow the code-text rules.

    **Serialization**: built-in codes are serialized as tagged dicts
    ``{"__builtin_task_code__": "DATABASE_ERROR"}`` so they round-trip
    through ``model_validate()`` / ``model_validate_json()`` without
    ambiguity.  A custom code is serialized as its text: the ``str``, or
    the value of the member. A decode gives the text; the reader of a task
    that declares an ``Enum`` turns it into the member.

    **Deserialization**: use standard ``model_validate()`` /
    ``model_validate_json()`` everywhere — including nested models.
    No special rehydration methods needed.

    **Stored form**: the ``err`` slot of a stored result holds the code
    under the key ``error_code``, the field name of the core's task error.
    ``encode_task_error`` writes that key at the place of ``code``, and
    ``decode_task_error`` reads it back into ``code``.

    **Unknown keys**: refused at construction and at decode. A stored value
    with a key that this class does not declare is stored data that does not
    parse. Subclasses inherit the rule, so ``SubWorkflowFailure`` accepts its
    own fields and no others.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    exception: BaseException | ExceptionRecord | None = None
    # The type checker sees the code type of the error. At run time the
    # before validator is the one check: it keeps a custom `Enum` member as
    # the member, which a `str` field would turn into its text.
    if TYPE_CHECKING:
        code: C | BuiltInTaskCode
    else:
        code: SkipValidation[object]
    # A writer gives `JsonInput`, and a reader gets `JsonValue`. No check runs at
    # construction: a TaskError is built inside `except` blocks, and that build must
    # not raise. `reportable_error` checks `data` at the wire.
    if TYPE_CHECKING:
        data: _ErrorData = _ErrorData()
    else:
        data: SkipValidation[JsonValue] = None
    message: str | None = None

    @field_validator('code', mode='before')
    @classmethod
    def _validate_code(cls, value: object) -> object:
        """Validate and coerce code values.

        Accepts:
        - BuiltInTaskCode enum members (pass through)
        - Tagged dict ``{"__builtin_task_code__": "..."}`` (coerce to enum)
        - Plain ``str`` not in reserved set (pass through)
        - A member of a custom ``Enum`` whose value is such a ``str`` (pass
          through as the member)

        Rejects:
        - ``None``
        - A custom code whose text matches a reserved built-in code
        - A custom code whose text is in ``RESERVED_NON_TASK_CODES``
          (``WORKFLOW_STOPPED``)
        - A custom ``Enum`` member whose value is not a ``str``
        - Any other type
        """
        if isinstance(value, _BUILTIN_FAMILIES):
            return value
        if value is None:
            raise ValueError('code must not be null')
        if isinstance(value, dict):
            if '__builtin_task_code__' not in value:
                raise ValueError(
                    'code dict must contain "__builtin_task_code__" key',
                )
            raw = cast('dict[str, object]', value)['__builtin_task_code__']
            if not isinstance(raw, str):
                raise ValueError(
                    f'"__builtin_task_code__" must be str, got {type(raw).__name__}',
                )
            member = BUILTIN_CODE_REGISTRY.get(raw)
            if member is None:
                raise ValueError(
                    f'Unknown built-in task code: {raw!r}',
                )
            return member
        if isinstance(value, Enum):
            member_value: object = value.value
            if not isinstance(member_value, str):
                raise ValueError(
                    f'a custom error code member must have a str value; '
                    f'{type(value).__name__}.{value.name} has a '
                    f'{type(member_value).__name__}',
                )
            _refuse_reserved_code_text(member_value)
            return value
        if isinstance(value, str):
            _refuse_reserved_code_text(value)
            return value
        raise ValueError(
            f'code must be a str, a custom Enum member or a built-in '
            f'code; got {type(value).__name__}',
        )

    @field_serializer('code', when_used='always')
    @classmethod
    def _serialize_code(
        cls,
        value: object,
        _info: Any,
    ) -> dict[str, str] | str:
        """Serialize code with tagged format for built-in codes.

        Built-in enum members become ``{"__builtin_task_code__": "VALUE"}``
        so round-trip identity is preserved. A custom code becomes its text:
        the ``str``, or the value of the member (the validator holds it to a
        ``str``).
        """
        if isinstance(value, _BUILTIN_FAMILIES):
            return {'__builtin_task_code__': value.value}
        return custom_code_text(value)

    @field_serializer('exception', when_used='json')
    @classmethod
    def _serialize_exception(
        cls,
        value: BaseException | ExceptionRecord | None,
        _info: Any,
    ) -> ExceptionRecord | None:
        """Serialize a live exception to its wire-safe ExceptionRecord form.

        json.dumps cannot encode a BaseException, so without this any caller
        doing model_dump(mode='json') / model_dump_json() crashes (the codec
        already excludes the field). None and an already-flattened dict pass
        through unchanged. Deferred import keeps traceback formatting in
        codec/error_payload, not in models/. Only runs for JSON serialization
        so python-mode model_dump still returns the live exception object.
        """
        if value is None or not isinstance(value, BaseException):
            return value
        from horsies.core.codec.error_payload import flatten_exception
        return flatten_exception(value)


def _refuse_reserved_code_text(text: str) -> None:
    """Raise ``ValueError`` for a custom code text that a built-in code or a
    reserved name holds."""
    if text in BUILTIN_CODE_REGISTRY:
        builtin = BUILTIN_CODE_REGISTRY[text]
        raise ValueError(
            f"'{text}' is a reserved built-in error code "
            f'({type(builtin).__name__}.{builtin.name}). '
            f'Pass the enum member directly.',
        )
    if text in RESERVED_NON_TASK_CODES:
        raise ValueError(
            reserved_code_message(text, field_name='code'),
        )


def custom_code_text(code: object) -> str:
    """The stored text of a custom code that ``TaskError`` accepted: the
    ``str``, or the ``str`` value of the member. Pure.

    Raises:
        AssertionError: ``code`` is not a custom code that the validator of
            ``TaskError`` accepts.
    """
    match code:
        case Enum(value=str() as text):
            return text
        case str():
            return code
        case _:
            raise AssertionError(
                f'a TaskError holds a custom code of type {type(code).__name__}',
            )


@dataclass(frozen=True, slots=True)
class TextCodes:
    """The custom codes of a task error are text: ``TaskError`` or
    ``TaskError[str]``."""


@dataclass(frozen=True, slots=True)
class EnumCodes:
    """The custom codes of a task error are the members of ``code_type``:
    ``TaskError[code_type]``."""

    code_type: type[Enum]


type CustomCodeType = TextCodes | EnumCodes
"""The type of the custom codes of a task error."""


@dataclass(frozen=True, slots=True)
class NotATaskErrorType:
    """The annotation is not ``TaskError``, ``TaskError[str]`` or
    ``TaskError[E]`` with ``E`` an ``Enum`` class."""


def custom_code_type_of(annot: object) -> CustomCodeType | NotATaskErrorType:
    """The custom code type of a ``TaskError`` annotation. Pure.

    ``TaskError`` and ``TaskError[str]`` are ``TextCodes``; ``TaskError[E]``
    with ``E`` an ``Enum`` class is ``EnumCodes(E)``. A parametrized
    ``TaskError`` is a class that pydantic builds, with the origin and the
    argument in its generic metadata.
    """
    if annot is TaskError:
        return TextCodes()
    metadata = getattr(annot, '__pydantic_generic_metadata__', None)
    if not isinstance(metadata, dict):
        return NotATaskErrorType()
    origin = cast('dict[str, object]', metadata).get('origin')
    args = cast('dict[str, object]', metadata).get('args')
    match origin, args:
        case (o, (a,)) if o is TaskError and a is str:
            return TextCodes()
        case (o, (type() as code_type,)) if o is TaskError and issubclass(code_type, Enum):
            return EnumCodes(code_type=code_type)
        case _:
            return NotATaskErrorType()


class SubWorkflowFailure(TaskError):
    """
    Error representing a failed subworkflow.

    Allows parent tasks to distinguish subworkflow failures from regular
    task failures via pattern matching:

        match outcome:
            case Failed(SubWorkflowFailure() as e):
                print(f"Subworkflow {e.sub_workflow_id} failed")
            case Failed(e):
                print(f"Task error: {e.message}")
            case Succeeded(value):
                print(value)
    """

    sub_workflow_id: str
    sub_workflow_summary: 'SubWorkflowSummary[Any]'


@dataclass
class TaskAttemptInfo:
    """Metadata for a single task execution attempt."""

    task_id: UUID
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
    """Metadata for a broker-backed task.

    Strict-serde (design §6) splits the legacy single ``result`` field
    into a three-field shape so callers can dispatch cleanly on whether
    typed decode succeeded:

    - ``raw_result``: the JSON envelope as stored, always populated when
      the task has a terminal payload. Available without any local task
      registration — safe for cross-process monitoring / dashboards.
    - ``decoded_result``: the typed ``TaskOutcome`` produced by
      ``decode_task_result(raw_result, ok_type)``. Populated only when
      the calling process has the task definition imported AND decode
      succeeds.
    - ``result_decoded``: ``True`` iff ``decoded_result`` was populated.

    ``execution`` is the stored ``Execution`` of a live task. It is ``None``
    for a task read from the history archive, which does not keep the class.

    ``attempts`` lists the attempts newest first (highest attempt number
    first), for a live task and for a task read from the history archive.
    """

    task_id: UUID
    task_name: str
    status: TaskStatus
    queue: str
    priority: int
    retry_count: int
    max_retries: int
    next_retry_at: datetime.datetime | None
    sent_at: datetime.datetime | None
    due_at: datetime.datetime
    claimed_at: datetime.datetime | None
    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None
    failed_at: datetime.datetime | None
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None
    error_code: str | None = None
    raw_result: 'dict[str, Any] | None' = None
    decoded_result: 'TaskOutcome[Any, TaskError] | None' = None
    result_decoded: bool = False
    failed_reason: str | None = None
    attempts: list[TaskAttemptInfo] | None = None
    execution: Execution | None = None


MAX_RETRY_ATTEMPTS: Final = 20
"""The largest `RetryPolicy.max_retries`."""

MIN_CANCEL_GRACE_MS: Final = 100
"""The smallest `cancel_grace_ms` of a `SHARED_ASYNC` or an `ISOLATED` task.

A healthy cleanup after a cancel ended within 15.4 ms in the measured set
(local network), so this is 6.5 times that. A lower value would send the
second cancel into healthy cleanups and drop their connections.
"""

MAX_CANCEL_GRACE_MS: Final = 10_000
"""The largest `cancel_grace_ms` of a `SHARED_ASYNC` or an `ISOLATED` task.

The longest library cleanup with an end in the measured set took 10.0 s
(psycopg 3.3.6 and later: a 5 s cancel request, then a 5 s drain). A larger
grace changes no measured outcome; it only keeps a seat longer.
"""

DEFAULT_CANCEL_GRACE_MS: Final = 1_000
"""The `cancel_grace_ms` that a `SHARED_ASYNC` task gets when it sets none.
An `ISOLATED` task that sets none gets no grace: no hard stop.

65 times the slowest healthy cleanup in the measured set. The task row
stores the value in effect when the task was sent, so a change of this
default does not change rows that are already queued.
"""


class Execution(str, Enum):
    """How a worker runs a task. Each task row stores its class.

    - `ISOLATED`: one task at a time in its own child process. The function
      can be `def` or `async def`, and `timeout_ms` is optional. The default.
    - `SHARED_ASYNC`: many `async def` tasks run as coroutines in one child
      process. The task must set `timeout_ms`. `cancel_grace_ms` is the time
      the task gets to stop after its cancel.
    - `SHARED_THREAD`: many `def` tasks run on threads in one child process.
      The task must set `timeout_ms`. A thread cannot be stopped, so the
      task cannot set `cancel_grace_ms`.

    An `ISOLATED` task can set `cancel_grace_ms`: after an operator cancel,
    the worker kills its child when the run has not ended that long after
    the cancel. With none, the cancel is cooperative only.
    """

    ISOLATED = 'ISOLATED'
    SHARED_ASYNC = 'SHARED_ASYNC'
    SHARED_THREAD = 'SHARED_THREAD'


class RetryPolicy(BaseModel):
    """
    Retry policy configuration for tasks.

    Two strategies supported:
    1. Fixed: Uses the intervals_seconds list exactly as specified
    2. Exponential: Uses intervals_seconds[0] as base, exponentially increases

    Fields:
        max_retries: maximum number of retry attempts (initial send not counted)
        intervals_seconds: delay intervals in seconds between retry attempts
        backoff_strategy: 'fixed' uses intervals_seconds as-is, 'exponential' uses
            intervals_seconds[0] as base
        jitter: whether to add ±25% randomization to delays
        max_delay_seconds: optional maximum delay after backoff and jitter
        retry_for: the code members that start a retry: members of the
            built-in code enums (`OperationalCode`, `ContractCode`,
            `RetrievalCode`, `OutcomeCode`), or members of the `Enum` class
            that the task declares with `error_codes=`. At least one.

    `retry_for` takes no plain `str`. The model keeps each entry as it
    is given: the task decorator checks the entries against the codes of
    the task (`check_retry_codes`) and refuses any other entry with
    `TASK_INVALID_OPTIONS`. The JSON form (`model_dump(mode='json')`), which
    the stored task options hold, has the value of each member.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid')

    max_retries: Annotated[
        int,
        Field(
            ge=1,
            le=MAX_RETRY_ATTEMPTS,
            description='Number of retry attempts (1-20)',
        ),
    ] = 3
    intervals_seconds: Annotated[
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
    max_delay_seconds: Annotated[
        Optional[PositiveInt],
        Field(description='Optional maximum retry delay in seconds'),
    ] = None
    # The type checker sees code members only. At run time the field keeps
    # each entry as it is given, so that the task decorator, which knows the
    # task, refuses a wrong entry with the task's name.
    if TYPE_CHECKING:
        retry_for: Annotated[
            list[BuiltInTaskCode | Enum],
            Field(min_length=1),
        ]
    else:
        retry_for: Annotated[
            list[object],
            Field(min_length=1),
        ]

    @model_validator(mode='after')
    def validate_strategy_consistency(self) -> Self:
        """Validate that backoff strategy is consistent with intervals_seconds configuration."""

        if self.backoff_strategy == 'fixed':
            # Fixed strategy: intervals_seconds length should match max_retries
            if len(self.intervals_seconds) != self.max_retries:
                raise ValueError(
                    f'Fixed backoff strategy requires intervals_seconds length '
                    f'({len(self.intervals_seconds)}) '
                    f'to match max_retries ({self.max_retries}). '
                    f'Either adjust intervals_seconds list or use exponential strategy.'
                )

        elif self.backoff_strategy == 'exponential':
            # Exponential strategy: should have exactly one base interval
            if len(self.intervals_seconds) != 1:
                raise ValueError(
                    f'Exponential backoff strategy requires exactly one base interval, '
                    f'got {len(self.intervals_seconds)} intervals. '
                    f'Use intervals_seconds=[base_seconds] for exponential backoff.'
                )

        return self

    # Convenience constructors to prevent misconfiguration at call sites
    @classmethod
    def fixed(
        cls,
        intervals_seconds: list[int],
        *,
        retry_for: list[BuiltInTaskCode | Enum],
        jitter: bool = True,
    ) -> 'RetryPolicy':
        """Create a fixed backoff policy where the length of intervals_seconds
        defines max_retries."""
        return cls(
            max_retries=len(intervals_seconds),
            intervals_seconds=intervals_seconds,
            backoff_strategy='fixed',
            jitter=jitter,
            retry_for=retry_for,
        )

    @classmethod
    def exponential(
        cls,
        base_seconds: int,
        *,
        max_retries: int,
        retry_for: list[BuiltInTaskCode | Enum],
        jitter: bool = True,
        max_delay_seconds: int | None = None,
    ) -> 'RetryPolicy':
        """Create an exponential backoff policy using a single base interval.

        The policy uses base_seconds * 2**(attempt-1) per attempt, optionally
        capped by max_delay_seconds.
        """
        return cls(
            max_retries=max_retries,
            intervals_seconds=[base_seconds],
            backoff_strategy='exponential',
            jitter=jitter,
            max_delay_seconds=max_delay_seconds,
            retry_for=retry_for,
        )


class TaskOptions(BaseModel):
    """
    Options for a task.

    Fields:
        task_name: Unique task identifier (mandatory - decoupled from function names)
        queue: Target queue name (validated against app config at definition time)
        good_until: Task expiry deadline (task skipped if not claimed by this time)
        retry_policy: Retry timing and backoff configuration (includes retry_for)
        timeout_ms: Execution time limit. Measured by the worker parent from
            the moment a child takes the task (time waiting for a free child
            does not count). On expiry the task fails with
            OutcomeCode.TASK_TIMEOUT and the child process is killed with the
            subprocesses in its session; retry only happens when
            OutcomeCode.TASK_TIMEOUT is listed in retry_for. Other tasks on the
            worker continue. The worker must start a new child, so timeouts
            are for exceptional hangs, not routine control flow. A
            SHARED_ASYNC task on a shared async child is cancelled instead,
            and its child is killed only when no task in it has time left.
            The value is from MIN_TASK_TIMEOUT_MS to MAX_TASK_TIMEOUT_MS.
        execution: How a worker runs the task (`Execution`). The default is
            `Execution.ISOLATED`.
        cancel_grace_ms: The cancel grace that the task declares, or None
            when it declares none. This model checks only the type. The task
            decorator checks the class and the bounds
            (MIN_CANCEL_GRACE_MS to MAX_CANCEL_GRACE_MS), and the stored
            options hold the resolved value (see `serialize_task_options`).
    """

    model_config = ConfigDict(extra='forbid')

    task_name: str
    queue: Optional[str] = None
    good_until: Optional[datetime.datetime] = None
    retry_policy: Optional[RetryPolicy] = None
    timeout_ms: Annotated[
        int,
        Field(
            ge=MIN_TASK_TIMEOUT_MS,
            le=MAX_TASK_TIMEOUT_MS,
            description='Execution time limit in milliseconds (1 s to about 49.7 days)',
        ),
    ] | None = None
    execution: Execution = Execution.ISOLATED
    cancel_grace_ms: int | None = None

    @field_validator('good_until')
    @classmethod
    def reject_naive_good_until(
        cls, value: datetime.datetime | None,
    ) -> datetime.datetime | None:
        """Reject naive datetimes — they are ambiguous and break fingerprinting."""
        if value is not None and value.tzinfo is None:
            raise ValueError(
                'good_until must be timezone-aware (got naive datetime). '
                'Use e.g. datetime.now(timezone.utc) or a tz-aware value.',
            )
        return value


# Rebuild SubWorkflowFailure to resolve forward reference to SubWorkflowSummary
def _rebuild_subworkflow_error() -> None:
    """Rebuild SubWorkflowFailure after SubWorkflowSummary is importable."""
    from horsies.core.models.workflow import SubWorkflowSummary  # noqa: F811

    _ = SubWorkflowSummary  # keep in scope for model_rebuild forward ref resolution

    SubWorkflowFailure.model_rebuild()


_rebuild_subworkflow_error()
