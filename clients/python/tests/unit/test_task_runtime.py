"""`TaskRuntime` and its `FencingToken`: the test helper, the registration
rules of the runtime parameter, the producer paths that leave it out, and
the injection on each kind of run.

The runs replace only the database steps around the body (the RUNNING
update). The Rust client has the same tests in
`clients/rust/horsies/src/task/runtime.rs` and
`core/src/worker/executor/child_host/run_work.rs`.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, SecretStr

import horsies._core as core
from horsies import (
    AttemptNumber,
    AttemptNumberRefused,
    CancelCause,
    CancelNotRequested,
    CancelRequested,
    Failed,
    FencingToken,
    Succeeded,
    TaskRuntime,
)
from horsies.core.app import Horsies
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.kwargs import (
    decode_kwargs,
    encode_kwargs_read_back,
    missing_required_kwargs,
    underlying_task_fn,
)
from horsies.core.codec.typed import decode_task_result
from horsies.core.errors import (
    HorsiesErrorCode,
    TaskDefinitionError,
    WorkflowValidationError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schedule import IntervalSchedule, TaskSchedule
from horsies.core.models.tasks import Execution, TaskError
from horsies.core.models.workflow import TaskNode, WorkflowSpec
from horsies.core.scheduler.validation import _signature_error
from horsies.core.task_runtime import (
    FIRST_ATTEMPT,
    MAX_ATTEMPT,
    CancelAlreadySet,
    CancelSet,
    RunCancelCell,
    run_task_runtime,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker import child_runner, shared_runner, thread_runner
from horsies.core.worker.child_frames import NoTraceCarrier
from horsies.core.worker.child_runner import RunTheTask
from horsies.core.worker.run_cancel import RunCancel
from horsies.core.worker.run_return import (
    EncodedResult,
    NotInWorkflow,
    StartedRunning,
)
from horsies.core.worker.task_loop import TaskLoop
from tests.fixtures.core_frames import async_work, shared_run_frame, thread_work
from tests.fixtures.run_return import envelope_of

pytestmark = [pytest.mark.unit]

TASK_ID = uuid.UUID('0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02')
CLAIMED_AT = datetime(2026, 11, 23, 9, 0, tzinfo=timezone.utc)
STARTED = StartedRunning(
    datetime(2026, 11, 23, 9, 0, 1, tzinfo=timezone.utc),
    NotInWorkflow(),
)
"""The start that the stubbed RUNNING update returns."""


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://user:password@localhost/unused'),
            ),
        ),
    )


APP = _app()


@APP.task('runtime.token')
def token_task(runtime: TaskRuntime, *, order_id: str) -> Result[FencingToken, TaskError]:
    _ = order_id
    return Ok(runtime.fencing_token)


@APP.task('runtime.token_async')
async def token_async_task(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    await asyncio.sleep(0)
    return Ok(runtime.fencing_token)


@APP.task('runtime.reads_twice')
def reads_twice_task(runtime: TaskRuntime) -> Result[list[FencingToken], TaskError]:
    return Ok([runtime.fencing_token, runtime.fencing_token])


@APP.task('runtime.plain')
def plain_task(*, order_id: str) -> Result[str, TaskError]:
    return Ok(order_id)


@APP.task('runtime.shared_async', execution=Execution.SHARED_ASYNC, timeout_ms=5_000)
async def shared_async_task(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    return Ok(runtime.fencing_token)


@APP.task('runtime.shared_thread', execution=Execution.SHARED_THREAD, timeout_ms=5_000)
def shared_thread_task(runtime: TaskRuntime) -> Result[FencingToken, TaskError]:
    return Ok(runtime.fencing_token)


@pytest.fixture
def task_loop() -> Iterator[TaskLoop]:
    """The loop of an isolated child whose app has an async task."""
    loop = TaskLoop()
    loop.open().unwrap()
    yield loop
    _ = loop.close()


# =============================================================================
# AttemptNumber and TaskRuntime.for_test
# =============================================================================


def _attempt(number: int) -> AttemptNumber:
    """The attempt number `number`, which the test states is in bounds."""
    match AttemptNumber.parse(number):
        case Ok(ok_value=attempt):
            return attempt
        case Err(err_value=refused):
            raise AssertionError(f'the test passes an attempt in bounds: {refused.reason}')


class TestAttemptNumber:
    @pytest.mark.parametrize('number', [1, 2, MAX_ATTEMPT])
    def test_a_number_in_the_bound_parses(self, number: int) -> None:
        assert AttemptNumber.parse(number) == Ok(AttemptNumber(value=number))

    @pytest.mark.parametrize('number', [0, -1, MAX_ATTEMPT + 1])
    def test_a_number_outside_the_bound_is_refused(self, number: int) -> None:
        refused = AttemptNumberRefused(number=number)

        assert AttemptNumber.parse(number) == Err(refused)
        assert refused.reason == (
            f'attempt number {number} is not in 1 to {MAX_ATTEMPT}; '
            'the first attempt of a task is 1'
        )

    @pytest.mark.parametrize('number', [0, MAX_ATTEMPT + 1])
    def test_a_direct_construction_outside_the_bound_is_a_defect(self, number: int) -> None:
        with pytest.raises(AssertionError, match=f'illegal attempt number {number}'):
            AttemptNumber(value=number)

    def test_the_first_attempt_is_1(self) -> None:
        assert FIRST_ATTEMPT == AttemptNumber(value=1)


class TestForTest:
    def test_the_runtime_holds_the_token_of_the_attempt(self) -> None:
        runtime = TaskRuntime.for_test(task_id=TASK_ID, attempt=_attempt(3))

        assert runtime.fencing_token == FencingToken(task_id=TASK_ID, value=3)

    def test_the_default_is_the_first_attempt(self) -> None:
        runtime = TaskRuntime.for_test(task_id=TASK_ID)

        assert runtime.fencing_token.value == 1

    def test_the_largest_attempt_is_accepted(self) -> None:
        runtime = TaskRuntime.for_test(task_id=TASK_ID, attempt=_attempt(MAX_ATTEMPT))

        assert runtime.fencing_token.value == MAX_ATTEMPT

    def test_a_direct_call_reads_the_runtime_that_the_test_passes(self) -> None:
        runtime = TaskRuntime.for_test(task_id=TASK_ID, attempt=_attempt(2))

        result = token_task(runtime, order_id='o-1')

        assert result == Ok(FencingToken(task_id=TASK_ID, value=2))

    def test_the_default_cancel_state_is_not_requested(self) -> None:
        runtime = TaskRuntime.for_test(task_id=TASK_ID)

        assert runtime.cancel_state() == CancelNotRequested()

    @pytest.mark.parametrize('cause', list(CancelCause))
    def test_the_runtime_gives_the_cancel_state_that_the_test_passes(
        self,
        cause: CancelCause,
    ) -> None:
        runtime = TaskRuntime.for_test(
            task_id=TASK_ID,
            cancel_state=CancelRequested(cause=cause),
        )

        assert runtime.cancel_state() == CancelRequested(cause=cause)
        assert runtime.cancel_state() == runtime.cancel_state()

    def test_two_runtimes_with_one_token_are_equal_whatever_their_cancel_state(
        self,
    ) -> None:
        cancelled = TaskRuntime.for_test(
            task_id=TASK_ID,
            cancel_state=CancelRequested(cause=CancelCause.OPERATOR),
        )

        assert cancelled == TaskRuntime.for_test(task_id=TASK_ID)

    def test_the_runtime_the_token_and_the_attempt_are_frozen(self) -> None:
        runtime = TaskRuntime.for_test(task_id=TASK_ID)

        with pytest.raises(AttributeError):
            setattr(runtime, 'fencing_token', FencingToken(task_id=TASK_ID, value=2))
        with pytest.raises(AttributeError):
            setattr(runtime.fencing_token, 'value', 2)
        with pytest.raises(AttributeError):
            setattr(_attempt(1), 'value', 2)


class TestRunCancelCell:
    """The cancel state of one run: set once, by the first cause."""

    def test_a_new_cell_holds_no_request(self) -> None:
        assert RunCancelCell().state() == CancelNotRequested()

    def test_the_first_request_sets_its_cause(self) -> None:
        cell = RunCancelCell()

        assert cell.request(CancelCause.OPERATOR) == CancelSet()
        assert cell.state() == CancelRequested(cause=CancelCause.OPERATOR)

    def test_a_later_request_keeps_the_first_cause(self) -> None:
        cell = RunCancelCell()
        _ = cell.request(CancelCause.DEADLINE)

        assert cell.request(CancelCause.OPERATOR) == CancelAlreadySet(
            cause=CancelCause.DEADLINE,
        )
        assert cell.state() == CancelRequested(cause=CancelCause.DEADLINE)

    def test_the_runtime_of_a_run_reads_its_cell_as_it_changes(self) -> None:
        cell = RunCancelCell()
        runtime = run_task_runtime(FencingToken(task_id=TASK_ID, value=1), cell)

        assert runtime.cancel_state() == CancelNotRequested()
        _ = cell.request(CancelCause.OPERATOR)
        assert runtime.cancel_state() == CancelRequested(cause=CancelCause.OPERATOR)


# =============================================================================
# Registration
# =============================================================================


class _HoldsRuntime(BaseModel):
    model_config = {'arbitrary_types_allowed': True}

    runtime: TaskRuntime


def _keyword_only(*, runtime: TaskRuntime) -> Result[int, TaskError]:
    _ = runtime
    return Ok(1)


def _optional_first(runtime: TaskRuntime | None, *, x: int) -> Result[int, TaskError]:
    _ = runtime
    return Ok(x)


def _with_default(
    runtime: TaskRuntime = TaskRuntime.for_test(task_id=TASK_ID),
) -> Result[int, TaskError]:
    _ = runtime
    return Ok(1)


def _second_positional(
    runtime: TaskRuntime,
    other: TaskRuntime,
) -> Result[int, TaskError]:
    _ = (runtime, other)
    return Ok(1)


def _in_a_container(*, runtimes: list[TaskRuntime]) -> Result[int, TaskError]:
    _ = runtimes
    return Ok(1)


def _in_a_model(*, holder: _HoldsRuntime) -> Result[int, TaskError]:
    _ = holder
    return Ok(1)


def _as_the_ok_type(runtime: TaskRuntime) -> Result[TaskRuntime, TaskError]:
    return Ok(runtime)


class TestRegistration:
    def test_a_first_runtime_parameter_registers_in_each_form(self) -> None:
        assert APP.tasks['runtime.token'] is token_task
        assert APP.tasks['runtime.token_async'] is token_async_task
        assert APP.tasks['runtime.shared_async'] is shared_async_task
        assert APP.tasks['runtime.shared_thread'] is shared_thread_task

    def test_a_positional_only_runtime_parameter_registers(self) -> None:
        app = _app()

        def positional_only(runtime: TaskRuntime, /, *, x: int) -> Result[int, TaskError]:
            _ = runtime
            return Ok(x)

        registered = app.task('runtime.positional_only')(positional_only)

        assert app.tasks['runtime.positional_only'] is registered

    @pytest.mark.parametrize(
        ('fn', 'subject'),
        [
            (_keyword_only, "parameter 'runtime'"),
            (_optional_first, "parameter 'runtime'"),
            (_with_default, "parameter 'runtime'"),
            (_second_positional, "parameter 'other'"),
            (_in_a_container, "parameter 'runtimes'"),
            (_in_a_model, "parameter 'holder'"),
            (_as_the_ok_type, 'return Result ok slot'),
        ],
        ids=[
            'keyword-only',
            'optional',
            'default',
            'second-positional',
            'in-a-container',
            'in-a-model',
            'ok-type',
        ],
    )
    def test_another_form_of_the_runtime_is_refused(self, fn: Any, subject: str) -> None:
        app = _app()

        with pytest.raises(TaskDefinitionError) as refused:
            app.task('runtime.refused')(fn)

        assert refused.value.code == HorsiesErrorCode.TASK_STRICT_SIGNATURE_VIOLATION
        assert subject in str(refused.value)
        assert 'TaskRuntime' in str(refused.value)
        assert 'runtime.refused' not in app.tasks


# =============================================================================
# Producer paths: the runtime is not part of the wire contract
# =============================================================================


class TestProducerPaths:
    def test_a_send_does_not_miss_the_runtime(self) -> None:
        missing = missing_required_kwargs(underlying_task_fn(token_task), {'order_id'})

        assert missing.producer == ()
        assert missing.workflow_only == ()

    def test_a_kwarg_that_names_the_runtime_is_refused_on_send(self) -> None:
        with pytest.raises(StrictJsonError, match='takes the TaskRuntime of the run'):
            encode_kwargs_read_back(
                underlying_task_fn(token_task),
                {'order_id': 'o-1', 'runtime': 1},
            )

    def test_a_wire_kwarg_that_names_the_runtime_is_refused_on_decode(self) -> None:
        with pytest.raises(StrictJsonError, match='takes the TaskRuntime of the run'):
            decode_kwargs(underlying_task_fn(token_task), {'order_id': 'o-1', 'runtime': 1})

    def test_a_workflow_node_does_not_miss_the_runtime(self) -> None:
        spec: WorkflowSpec[Any] = WorkflowSpec(
            name='runtime_node',
            tasks=[TaskNode(fn=token_task, kwargs={'order_id': 'o-1'})],
        )

        assert len(spec.tasks) == 1

    def test_a_workflow_node_kwarg_that_names_the_runtime_is_refused(self) -> None:
        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(
                name='runtime_node_kwarg',
                tasks=[
                    TaskNode(fn=token_task, kwargs={'order_id': 'o-1', 'runtime': 1}),
                ],
            )

        assert refused.value.code == HorsiesErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE

    def test_a_schedule_binds_without_the_runtime(self) -> None:
        schedule = TaskSchedule(
            name='runtime_schedule',
            task_name='runtime.token',
            pattern=IntervalSchedule(seconds=60),
            kwargs={'order_id': 'o-1'},
        )

        assert _signature_error(APP, schedule) is None

    def test_a_schedule_kwarg_that_names_the_runtime_is_refused(self) -> None:
        schedule = TaskSchedule(
            name='runtime_schedule_kwarg',
            task_name='runtime.token',
            pattern=IntervalSchedule(seconds=60),
            kwargs={'order_id': 'o-1', 'runtime': 1},
        )

        error = _signature_error(APP, schedule)

        assert error is not None
        assert error.code == HorsiesErrorCode.CONFIG_INVALID_SCHEDULE


# =============================================================================
# Injection on each kind of run
# =============================================================================


def _ok_of(envelope: str, ok_type: Any) -> Any:
    match decode_task_result(loads_json(envelope).unwrap(), ok_type):
        case Succeeded(value=value):
            return value
        case Failed(error=error):
            raise AssertionError(f'the stored result is an error: {error!r}')


def _run_isolated(
    task_name: str,
    task_loop: TaskLoop,
    *,
    attempt: int,
    input_json: str = '{}',
) -> str:
    with (
        patch.object(
            child_runner,
            '_confirm_ownership_and_set_running',
            MagicMock(return_value=STARTED),
        ),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)),
    ):
        return envelope_of(
            child_runner._run_task_entry(
                task_name=task_name,
                input_json=input_json,
                task_id=TASK_ID,
                master_worker_id='worker',
                work=RunTheTask(),
                claimed_at=CLAIMED_AT,
                task_loop=task_loop,
                orphan_self_heal=True,
                queue_name='default',
                attempt=attempt,
                trace=NoTraceCarrier(),
            ),
        )


def _encoded(steps: object) -> str:
    assert isinstance(steps, EncodedResult), steps
    return steps.envelope.text


class TestInjection:
    def test_an_isolated_def_run_gets_the_token_of_its_attempt(
        self,
        task_loop: TaskLoop,
    ) -> None:
        envelope = _run_isolated(
            'runtime.token',
            task_loop,
            attempt=3,
            input_json=json.dumps({'order_id': 'o-1'}),
        )

        assert _ok_of(envelope, FencingToken) == FencingToken(task_id=TASK_ID, value=3)

    def test_an_isolated_async_def_run_gets_the_token_of_its_attempt(
        self,
        task_loop: TaskLoop,
    ) -> None:
        envelope = _run_isolated('runtime.token_async', task_loop, attempt=1)

        assert _ok_of(envelope, FencingToken) == FencingToken(task_id=TASK_ID, value=1)

    def test_a_later_attempt_gets_a_greater_value(self, task_loop: TaskLoop) -> None:
        first = _ok_of(
            _run_isolated('runtime.token_async', task_loop, attempt=1),
            FencingToken,
        )
        second = _ok_of(
            _run_isolated('runtime.token_async', task_loop, attempt=2),
            FencingToken,
        )

        assert first.task_id == second.task_id == TASK_ID
        assert second.value > first.value

    def test_two_reads_in_one_attempt_are_equal(self, task_loop: TaskLoop) -> None:
        envelope = _run_isolated('runtime.reads_twice', task_loop, attempt=2)

        first, second = _ok_of(envelope, list[FencingToken])
        assert first == second == FencingToken(task_id=TASK_ID, value=2)

    def test_a_task_without_the_runtime_gets_no_positional_argument(
        self,
        task_loop: TaskLoop,
    ) -> None:
        envelope = _run_isolated(
            'runtime.plain',
            task_loop,
            attempt=1,
            input_json=json.dumps({'order_id': 'o-1'}),
        )

        assert _ok_of(envelope, str) == 'o-1'

    def test_a_shared_async_seat_gets_the_token_of_its_frame(self) -> None:
        frame = shared_run_frame(
            dispatch=1,
            task_id=TASK_ID,
            work=async_work(timeout_ms=5_000, cancel_grace_ms=100),
            task_name='runtime.shared_async',
            attempt=2,
        )

        with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
            steps = asyncio.run(shared_runner._seat_steps(frame, RunCancel()))

        token = _ok_of(_encoded(steps), FencingToken)
        assert token == FencingToken(task_id=TASK_ID, value=2)

    def test_a_shared_thread_seat_gets_the_token_of_its_frame(self) -> None:
        frame = shared_run_frame(
            dispatch=1,
            task_id=TASK_ID,
            work=thread_work(timeout_ms=5_000),
            task_name='runtime.shared_thread',
            attempt=4,
        )
        work = frame.work
        assert isinstance(work, core.ThreadTaskWork), work

        with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
            steps = thread_runner._thread_steps(frame, work, RunCancel())

        token = _ok_of(_encoded(steps), FencingToken)
        assert token == FencingToken(task_id=TASK_ID, value=4)
