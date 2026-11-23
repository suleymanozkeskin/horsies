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
directly passes `TaskRuntime.for_test(...)`.

The Rust client has the same type: `horsies::TaskRuntime`
(`clients/rust/horsies/src/task/runtime.rs`), which a Rust task takes as its
first parameter.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Final, final


MAX_ATTEMPT: Final = 2**31
"""The largest attempt number: the retry count of a row is a Postgres
`integer` from 0, and the attempt is the retry count plus 1 (the core's
`MAX_ATTEMPT`, `core/src/events/values.rs`)."""


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
    def for_test(cls, *, task_id: uuid.UUID, attempt: int = 1) -> TaskRuntime:
        """A runtime for a test that calls a task function directly.

        The token has the value `attempt`, as the worker gives to attempt
        `attempt` of the task `task_id`. A test of a retry passes
        `attempt=2`.

        Args:
            task_id: The task id of the token.
            attempt: The attempt number, from 1 to `MAX_ATTEMPT`. Default 1.

        Raises:
            ValueError: `attempt` is not in 1 to `MAX_ATTEMPT`. This is a
                defect in the test.
        """
        if not 1 <= attempt <= MAX_ATTEMPT:
            raise ValueError(
                f'TaskRuntime.for_test: attempt {attempt} is not in 1 to '
                f'{MAX_ATTEMPT}; the first attempt of a task is 1',
            )
        return cls(fencing_token=FencingToken(task_id=task_id, value=attempt))
