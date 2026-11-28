"""`TaskRuntime`: the facts of its run that a task body reads.

A task gets its runtime when its first parameter is `TaskRuntime`, before
the `*` of its keyword-only parameters:

```python
@app.task('read_token')
def read_token(runtime: TaskRuntime, *, order_id: str) -> Result[FencingToken, TaskError]:
    return Ok(runtime.fencing_token)
```

The runtime gives the facts of the attempt: `task_id` (the same on every
attempt), `attempt` (1 is the first run), `max_retries` (the retry budget of
the task) and `retries_left` (`max_retries - (attempt - 1)`, and 0 past the
budget). The runtime is the only way to read them: there is no global
`current_attempt()`.

The worker builds one runtime for each run and passes it as the first
argument. The runtime is not part of the wire contract: a send, a schedule
and a workflow node do not supply it. A test that calls the task function
directly passes `TaskRuntime.for_test(...)`, with an `AttemptNumber` that
`AttemptNumber.parse` builds and a `MaxRetries` that `MaxRetries.parse`
builds.

`TaskRuntime.cancel_state()` tells the body whether a cancel of its run was
requested, and why: by an operator (`cancel_task` with
`policy=CancelPolicy.INCLUDE_RUNNING`), or at the deadline of a `SHARED_ASYNC` or
`SHARED_THREAD` run. A `def` body reads it between steps and returns early.
An `async def` body also gets `asyncio.CancelledError` at its next `await`.

The Rust client has the same type: `horsies::TaskRuntime`
(`clients/rust/horsies/src/task/runtime.rs`), which a Rust task takes as its
first parameter.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Self, assert_never, final

from horsies.core.types.result import Err, Ok, Result


MAX_ATTEMPT: Final = 2**31
"""The largest attempt number: the retry count of a row is a Postgres
`integer` from 0, and the attempt is the retry count plus 1 (the core's
`MAX_ATTEMPT`, `core/src/events/values.rs`)."""


@final
@dataclass(frozen=True, slots=True)
class AttemptNumberRefused:
    """A number outside 1 to `MAX_ATTEMPT`, which is not an attempt number.

    Attributes:
        number: The refused number.
    """

    number: int

    @property
    def reason(self) -> str:
        """The refused number and the bounds, as text."""
        return (
            f'attempt number {self.number} is not in 1 to {MAX_ATTEMPT}; '
            'the first attempt of a task is 1'
        )


@final
@dataclass(frozen=True, slots=True)
class AttemptNumber:
    """The number of one attempt of a task: from 1 to `MAX_ATTEMPT`.

    `parse` builds one from an `int`, or refuses the number as an
    `AttemptNumberRefused`. The constructor asserts the bounds, so a number
    out of bounds that code builds directly is a code defect and crashes
    loud. The Rust client has the same bound: `AttemptNumber`
    (`core/src/events/values.rs`).

    Attributes:
        value: The attempt number. The first attempt of a task is 1.
    """

    value: int

    def __post_init__(self) -> None:
        assert 1 <= self.value <= MAX_ATTEMPT, f'illegal attempt number {self.value}'

    @classmethod
    def parse(cls, number: int) -> Result[Self, AttemptNumberRefused]:
        """The attempt number `number`, or its refusal when it is not in 1
        to `MAX_ATTEMPT`. Pure."""
        match number:
            case in_bounds if 1 <= in_bounds <= MAX_ATTEMPT:
                return Ok(cls(value=in_bounds))
            case _:
                return Err(AttemptNumberRefused(number=number))


FIRST_ATTEMPT: Final = AttemptNumber(value=1)
"""The first attempt of a task: the default of `TaskRuntime.for_test`."""

MAX_TASK_RETRIES: Final = 20
"""The largest retry budget of a task: the table holds `max_retries` from 0
to 20 (the core's `MAX_TASK_RETRIES`, `core/src/events/values.rs`)."""


@final
@dataclass(frozen=True, slots=True)
class MaxRetriesRefused:
    """A number outside 0 to `MAX_TASK_RETRIES`, which is not a retry budget.

    Attributes:
        number: The refused number.
    """

    number: int

    @property
    def reason(self) -> str:
        """The refused number and the bounds, as text."""
        return f'retry budget {self.number} is not in 0 to {MAX_TASK_RETRIES}'


@final
@dataclass(frozen=True, slots=True)
class MaxRetries:
    """The retry budget of a task: the retries that follow its first
    attempt, from 0 to `MAX_TASK_RETRIES`.

    `parse` builds one from an `int`, or refuses the number as a
    `MaxRetriesRefused`. The constructor asserts the bounds, so a number out
    of bounds that code builds directly is a code defect and crashes loud.
    The Rust client has the same bound: `MaxRetries`
    (`core/src/events/values.rs`).

    Attributes:
        value: The retry budget.
    """

    value: int

    def __post_init__(self) -> None:
        assert 0 <= self.value <= MAX_TASK_RETRIES, f'illegal retry budget {self.value}'

    @classmethod
    def parse(cls, number: int) -> Result[Self, MaxRetriesRefused]:
        """The retry budget `number`, or its refusal when it is not in 0 to
        `MAX_TASK_RETRIES`. Pure."""
        match number:
            case in_bounds if 0 <= in_bounds <= MAX_TASK_RETRIES:
                return Ok(cls(value=in_bounds))
            case _:
                return Err(MaxRetriesRefused(number=number))


NO_RETRIES: Final = MaxRetries(value=0)
"""A task that is not retried: the default of `TaskRuntime.for_test`."""


@final
@dataclass(frozen=True, slots=True)
class RetriesLeft:
    """The retries that a task has left after an attempt: from 0 to
    `MAX_TASK_RETRIES`. The Rust client has the same bound: `RetriesLeft`
    (`core/src/events/values.rs`).

    Attributes:
        value: The retries left.
    """

    value: int

    def __post_init__(self) -> None:
        assert 0 <= self.value <= MAX_TASK_RETRIES, f'illegal retries left {self.value}'


@final
@dataclass(frozen=True, slots=True)
class FencingToken:
    """The fencing token of one attempt of a task.

    Guarantees:

    - Two reads in one attempt give the same token.
    - An attempt that runs after another attempt of the same task gives a
      strictly greater `value`.
    - The value depends on no clock.

    Compare the values of tokens with the same `task_id` only: the tokens of
    two tasks have no order. A rerun, and a workflow node that a resume
    starts again, get a new task id, and their value starts at 1 again.

    Today `value` is the attempt number (1 to 21, at most
    `max_retries + 1`). Code compares values; it does not depend on the
    number.

    Attributes:
        task_id: The id of the task. The token orders the attempts of this
            task only.
        value: From 1 to `MAX_ATTEMPT`.
    """

    task_id: uuid.UUID
    value: int


class CancelCause(Enum):
    """Why the cancel of a run was requested.

    The Rust client has the same cause: `horsies::CancelCause`
    (`clients/rust/horsies/src/task/runtime.rs`).
    """

    DEADLINE = 'DEADLINE'
    """The deadline of the run: `timeout_ms` of a `SHARED_ASYNC` or a
    `SHARED_THREAD` run. The run ends as `TASK_TIMEOUT`."""

    OPERATOR = 'OPERATOR'
    """An operator cancelled the task: `cancel_task` with
    `policy=CancelPolicy.INCLUDE_RUNNING`. The task is `CANCELLED` already;
    what the body returns is dropped."""


@final
@dataclass(frozen=True, slots=True)
class CancelNotRequested:
    """No cancel of the run was requested."""


@final
@dataclass(frozen=True, slots=True)
class CancelRequested:
    """A cancel of the run was requested.

    Attributes:
        cause: Why. The first cause stays: a deadline after an operator
            cancel, or an operator cancel after a deadline, changes nothing.
    """

    cause: CancelCause


type RunCancelState = CancelNotRequested | CancelRequested
"""Whether a cancel of the run was requested, and why."""

NOT_REQUESTED: Final = CancelNotRequested()
"""The cancel state of a run before any request: the default of
`TaskRuntime.for_test`."""


@final
@dataclass(frozen=True, slots=True)
class CancelSet:
    """The request set the cancel state of the run."""


@final
@dataclass(frozen=True, slots=True)
class CancelAlreadySet:
    """An earlier request set the cancel state of the run; the request
    changed nothing.

    Attributes:
        cause: The cause of the earlier request.
    """

    cause: CancelCause


type CancelRequestOutcome = CancelSet | CancelAlreadySet
"""What one request did to the cancel state of a run."""


@final
class RunCancelCell:
    """The cancel state of one run. It turns from `CancelNotRequested` to
    `CancelRequested` at most once. Safe between threads: the control thread
    of a child, a seat thread and the deadline thread can request it while
    the body reads it.

    The worker child builds one cell for each run; `TaskRuntime.cancel_state`
    reads it.
    """

    __slots__ = ('_lock', '_state')

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: RunCancelState = NOT_REQUESTED

    @classmethod
    def holding(cls, state: RunCancelState) -> RunCancelCell:
        """A cell that holds `state` from the start, for a test runtime.
        Pure; it cannot fail."""
        cell = cls()
        cell._state = state
        return cell

    def request(self, cause: CancelCause) -> CancelRequestOutcome:
        """Request the cancel of the run for `cause`. The first request sets
        the state (`CancelSet`); a later one changes nothing
        (`CancelAlreadySet` with the first cause). No I/O."""
        with self._lock:
            match self._state:
                case CancelNotRequested():
                    self._state = CancelRequested(cause=cause)
                    return CancelSet()
                case CancelRequested(cause=first):
                    return CancelAlreadySet(cause=first)
                case _ as unreachable:
                    assert_never(unreachable)

    def state(self) -> RunCancelState:
        """The cancel state now. No I/O."""
        with self._lock:
            return self._state


@final
@dataclass(frozen=True, slots=True)
class TaskRuntime:
    """The facts of the current run that a task body reads.

    The worker builds one for each run. A task gets it when its first
    parameter is `TaskRuntime`. Build it with `for_test` in a test; the
    worker builds it with `run_task_runtime`.

    Attributes:
        task_id: The id of the task. It is the same on every attempt and the
            same as `TaskHandle.task_id`: a stable key for an idempotent
            write to another system.
        attempt: The number of this attempt. 1 is the first run, 2 the first
            retry; the same numbering as the attempt rows of the task.
        max_retries: The retry budget of the task: the retries that follow
            its first attempt.
    """

    task_id: uuid.UUID
    attempt: AttemptNumber
    max_retries: MaxRetries
    _cancel: RunCancelCell = field(repr=False, compare=False)

    @property
    def retries_left(self) -> RetriesLeft:
        """The retries left after this attempt: `max_retries - (attempt - 1)`,
        and 0 when the attempt is past the budget. A non-retryable error
        code or `good_until` can end the retries before it reaches 0, so the
        last attempt is not known in advance. No I/O."""
        return RetriesLeft(value=max(0, self.max_retries.value - (self.attempt.value - 1)))

    @property
    def fencing_token(self) -> FencingToken:
        """The fencing token of this attempt: the task id and the attempt
        number as `value`. No I/O."""
        return FencingToken(task_id=self.task_id, value=self.attempt.value)

    def cancel_state(self) -> RunCancelState:
        """Whether a cancel of this run was requested, and why.

        - `CancelNotRequested`: the body runs on.
        - `CancelRequested(cause=CancelCause.OPERATOR)`: an operator
          cancelled the task. The task is `CANCELLED` already and what the
          body returns is dropped, so the body can stop at once.
        - `CancelRequested(cause=CancelCause.DEADLINE)`: the deadline of a
          `SHARED_ASYNC` or `SHARED_THREAD` run fired. The run ends as
          `TASK_TIMEOUT`.

        An `async def` body also gets `asyncio.CancelledError` at its next
        `await`. A `def` body gets no signal: it reads this state between
        its steps. Two reads can differ only from `CancelNotRequested` to
        `CancelRequested`. No I/O.
        """
        return self._cancel.state()

    @classmethod
    def for_test(
        cls,
        *,
        task_id: uuid.UUID,
        attempt: AttemptNumber = FIRST_ATTEMPT,
        max_retries: MaxRetries = NO_RETRIES,
        cancel_state: RunCancelState = NOT_REQUESTED,
    ) -> TaskRuntime:
        """A runtime for a test that calls a task function directly.

        The fencing token has the value of `attempt`, as the worker gives to
        that attempt of the task `task_id`. A test of a retry passes the
        attempt number 2 from `AttemptNumber.parse(2)` and the budget from
        `MaxRetries.parse(3)`; `retries_left` follows from the two.
        `cancel_state()` gives `cancel_state` for the whole call: a test of
        the stop path of a body passes
        `CancelRequested(cause=CancelCause.OPERATOR)`. Pure; it cannot fail.

        Args:
            task_id: The task id.
            attempt: The attempt number. Default `FIRST_ATTEMPT` (1).
            max_retries: The retry budget. Default `NO_RETRIES` (0).
            cancel_state: The cancel state of the run. Default
                `CancelNotRequested()`.
        """
        return cls(
            task_id=task_id,
            attempt=attempt,
            max_retries=max_retries,
            _cancel=RunCancelCell.holding(cancel_state),
        )


def run_task_runtime(
    task_id: uuid.UUID,
    attempt: AttemptNumber,
    max_retries: MaxRetries,
    cancel: RunCancelCell,
) -> TaskRuntime:
    """The runtime of one run in a worker child: its task id, attempt number
    and retry budget, and the cancel cell that the child requests for that
    run. Pure."""
    return TaskRuntime(
        task_id=task_id,
        attempt=attempt,
        max_retries=max_retries,
        _cancel=cancel,
    )
