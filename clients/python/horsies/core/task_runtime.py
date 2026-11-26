"""`TaskRuntime`: the facts of its run that a task body reads.

A task gets its runtime when its first parameter is `TaskRuntime`, before
the `*` of its keyword-only parameters:

```python
@app.task('read_token')
def read_token(runtime: TaskRuntime, *, order_id: str) -> Result[FencingToken, TaskError]:
    return Ok(runtime.fencing_token)
```

The worker builds one runtime for each run and passes it as the first
argument. The runtime is not part of the wire contract: a send, a schedule
and a workflow node do not supply it. A test that calls the task function
directly passes `TaskRuntime.for_test(...)`, with an `AttemptNumber` that
`AttemptNumber.parse` builds.

`TaskRuntime.cancel_state()` tells the body whether a cancel of its run was
requested, and why: by an operator (`cancel_task` with
`include_running=True`), or at the deadline of a `SHARED_ASYNC` or
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
    `include_running=True`. The task is `CANCELLED` already; what the body
    returns is dropped."""


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
        fencing_token: The fencing token of this attempt.
    """

    fencing_token: FencingToken
    _cancel: RunCancelCell = field(repr=False, compare=False)

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
        cancel_state: RunCancelState = NOT_REQUESTED,
    ) -> TaskRuntime:
        """A runtime for a test that calls a task function directly.

        The token has the value of `attempt`, as the worker gives to that
        attempt of the task `task_id`. A test of a retry passes the
        attempt number 2 from `AttemptNumber.parse(2)`. `cancel_state()`
        gives `cancel_state` for the whole call: a test of the stop path of
        a body passes `CancelRequested(cause=CancelCause.OPERATOR)`. Pure;
        it cannot fail.

        Args:
            task_id: The task id of the token.
            attempt: The attempt number. Default `FIRST_ATTEMPT` (1).
            cancel_state: The cancel state of the run. Default
                `CancelNotRequested()`.
        """
        return cls(
            fencing_token=FencingToken(task_id=task_id, value=attempt.value),
            _cancel=RunCancelCell.holding(cancel_state),
        )


def run_task_runtime(fencing_token: FencingToken, cancel: RunCancelCell) -> TaskRuntime:
    """The runtime of one run in a worker child: its fencing token, and the
    cancel cell that the child requests for that run. Pure."""
    return TaskRuntime(fencing_token=fencing_token, _cancel=cancel)
