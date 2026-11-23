"""`TaskRuntime`: the facts of its run that a task body reads.

A task gets its runtime when its first parameter is `TaskRuntime`, before
the `*` of its keyword-only parameters:

```python
@app.task('read_token')
def read_token(runtime: TaskRuntime, *, order_id: str) -> TaskResult[FencingToken, TaskError]:
    return TaskResult(ok=runtime.fencing_token)
```

The worker builds one runtime for each run and passes it as the first
argument. The runtime is not part of the wire contract: a send, a schedule
and a workflow node do not supply it. A test that calls the task function
directly passes `TaskRuntime.for_test(...)`, with an `AttemptNumber` that
`AttemptNumber.parse` builds.

The Rust client has the same type: `horsies::TaskRuntime`
(`clients/rust/horsies/src/task/runtime.rs`), which a Rust task takes as its
first parameter.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Final, Self, final

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


@final
@dataclass(frozen=True, slots=True)
class TaskRuntime:
    """The facts of the current run that a task body reads.

    The worker builds one for each run. A task gets it when its first
    parameter is `TaskRuntime`.

    Attributes:
        fencing_token: The fencing token of this attempt.
    """

    fencing_token: FencingToken

    @classmethod
    def for_test(
        cls,
        *,
        task_id: uuid.UUID,
        attempt: AttemptNumber = FIRST_ATTEMPT,
    ) -> TaskRuntime:
        """A runtime for a test that calls a task function directly.

        The token has the value of `attempt`, as the worker gives to that
        attempt of the task `task_id`. A test of a retry passes the
        attempt number 2 from `AttemptNumber.parse(2)`. Pure; it cannot
        fail.

        Args:
            task_id: The task id of the token.
            attempt: The attempt number. Default `FIRST_ATTEMPT` (1).
        """
        return cls(fencing_token=FencingToken(task_id=task_id, value=attempt.value))
