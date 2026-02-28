"""Layer 1 e2e tests: task fundamentals."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from typing import Protocol

from horsies.core.models.task_pg import TaskModel
from horsies.core.types.status import TaskStatus
from horsies.core.models.tasks import LibraryErrorCode, TaskResult, TaskError
from horsies.core.task_decorator import TaskHandle
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.errors import ConfigurationError
from horsies.core.types.result import is_ok
from sqlalchemy import text

from tests.e2e.helpers.assertions import assert_ok, assert_err
from tests.e2e.helpers.db import poll_max_during
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.e2e.tasks import retry as retry_tasks
from tests.e2e.tasks import instance as default_instance
from tests.e2e.tasks import instance_retry_precedence
from tests.e2e.tasks import instance_custom
from tests.e2e.tasks import queues_custom


DEFAULT_INSTANCE = 'tests.e2e.tasks.instance:app'
RETRY_PRECEDENCE_INSTANCE = 'tests.e2e.tasks.instance_retry_precedence:app'
CUSTOM_INSTANCE = 'tests.e2e.tasks.instance_custom:app'


class _HealthcheckTask(Protocol):
    def send(self) -> TaskHandle[str]: ...


def _make_ready_check(task_func: _HealthcheckTask):
    handle: TaskHandle[str] | None = None

    def _check() -> bool:
        nonlocal handle
        if handle is None:
            handle = task_func.send()
        result = handle.get(timeout_ms=2000)
        return result.is_ok()

    return _check


@pytest.mark.e2e
def test_simple_task_lifecycle() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = basic_tasks.simple_task.send(5)
        result = handle.get(timeout_ms=5000)
        assert_ok(result, expected_value=10)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_status_transitions(broker: PostgresBroker) -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = basic_tasks.simple_task.send(5)
        result = handle.get(timeout_ms=5000)
        assert_ok(result)

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED
            assert task.started_at is not None
            assert task.completed_at is not None
            assert task.claimed_at is not None


@pytest.mark.e2e
def test_primitive_args() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.primitives_task.send(42, 3.14, 'hello', True, None).get(
            timeout_ms=5000
        )
        assert_ok(result, {'i': 42, 'f': 3.14, 's': 'hello', 'b': True, 'n': None})


@pytest.mark.e2e
def test_collection_args() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.collections_task.send([1, 2, 3], {'a': 1}, (4, 5)).get(
            timeout_ms=5000
        )
        assert_ok(result)
        output = result.unwrap()
        assert output['lst'] == [1, 2, 3]
        assert output['dct'] == {'a': 1}
        assert output['tpl'] == [4, 5]  # tuple serializes as list


@pytest.mark.e2e
def test_pydantic_model_args() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        user = basic_tasks.UserInput(name='Alice', age=30)
        result = basic_tasks.pydantic_task.send(user).get(timeout_ms=5000)
        assert_ok(result, 'Alice is 30')


@pytest.mark.e2e
def test_dataclass_args() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.dataclass_task.send(basic_tasks.DataInput(x=10, y=20)).get(
            timeout_ms=5000
        )
        assert_ok(result, 30)


@pytest.mark.e2e
def test_kwargs_with_defaults() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        r1 = basic_tasks.kwargs_task.send(5).get(timeout_ms=5000)
        assert_ok(r1, '5_default')

        r2 = basic_tasks.kwargs_task.send(5, optional='custom').get(timeout_ms=5000)
        assert_ok(r2, '5_custom')

        r3 = basic_tasks.kwargs_task.send(5, optional='x', multiplier=10).get(
            timeout_ms=5000
        )
        assert_ok(r3, '50_x')


@pytest.mark.e2e
def test_explicit_error_result() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.error_task.send().get(timeout_ms=5000)
        assert_err(result, expected_code='DELIBERATE_ERROR')
        assert result.err is not None
        assert result.err.message == 'This is intentional'
        assert result.err.data == {'key': 'value'}


@pytest.mark.e2e
def test_exception_becomes_error() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.exception_task.send().get(timeout_ms=5000)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.UNHANDLED_EXCEPTION
        assert result.err.message is not None
        assert 'ValueError' in result.err.message


@pytest.mark.e2e
def test_return_type_mismatch() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.type_mismatch_task.send().get(timeout_ms=5000)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.RETURN_TYPE_MISMATCH


@pytest.mark.e2e
def test_complex_result_serialization() -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.complex_result_task.send().get(timeout_ms=5000)
        assert_ok(result)
        output = result.unwrap()
        assert output.value == 42
        assert output.nested == {'a': [1, 2, 3], 'b': [4, 5]}


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_exhausted(broker: PostgresBroker) -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = retry_tasks.retry_exhausted_task.send()
        result = handle.get(timeout_ms=15000)
        assert_err(result, expected_code='TRANSIENT')

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 3
            assert task.status == TaskStatus.FAILED


@pytest.mark.e2e
def test_retry_eventual_success() -> None:
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmp:
        tmp.write('0')  # Initialize counter to 0
        state_path = tmp.name

    os.environ['E2E_RETRY_SUCCESS_PATH'] = state_path
    try:
        with run_worker(
            DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
        ):
            result = retry_tasks.retry_success_task.send().get(timeout_ms=15000)
            assert_ok(result, 'succeeded_on_attempt_3')
    finally:
        try:
            os.unlink(state_path)
        except FileNotFoundError:
            pass
        os.environ.pop('E2E_RETRY_SUCCESS_PATH', None)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_exponential_backoff_state(broker: PostgresBroker) -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = retry_tasks.exponential_task.send()

        # Wait for at least one retry to be scheduled
        deadline = time.time() + 5.0
        while time.time() < deadline:
            async with broker.session_factory() as session:
                task = await session.get(TaskModel, handle.task_id)
                if task and task.retry_count >= 1 and task.next_retry_at is not None:
                    break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError('retry_count did not advance')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_exponential_backoff_intervals(broker: PostgresBroker) -> None:
    """Exponential backoff: second retry interval exceeds first."""
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = retry_tasks.exponential_task.send()

        # Capture original sent_at before worker processes the task
        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None and task.sent_at is not None
            sent_at_snapshots: dict[int, datetime] = {0: task.sent_at}

        # Poll until retry_count reaches 2
        deadline = time.time() + 15.0
        while time.time() < deadline:
            async with broker.session_factory() as session:
                task = await session.get(TaskModel, handle.task_id)
                if task is not None and task.sent_at is not None:
                    rc = task.retry_count
                    if rc not in sent_at_snapshots:
                        sent_at_snapshots[rc] = task.sent_at
                    if rc >= 2:
                        break
            await asyncio.sleep(0.15)
        else:
            raise AssertionError(
                f'retry_count did not reach 2; snapshots: {sent_at_snapshots}'
            )

        # sent_at is updated to next_retry_at on each retry
        # base=1 exponential: delay_1 ≈ 1s (2^0), delay_2 ≈ 2s (2^1)
        assert {0, 1, 2}.issubset(sent_at_snapshots), (
            f'Missing retry counts; got: {sorted(sent_at_snapshots)}'
        )
        interval_1 = (sent_at_snapshots[1] - sent_at_snapshots[0]).total_seconds()
        interval_2 = (sent_at_snapshots[2] - sent_at_snapshots[1]).total_seconds()
        assert interval_2 > interval_1, (
            f'Expected increasing intervals for exponential backoff: '
            f'interval_1={interval_1:.2f}s, interval_2={interval_2:.2f}s'
        )


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_auto_retry_by_error_code(broker: PostgresBroker) -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = retry_tasks.retry_error_code_task.send()
        result = handle.get(timeout_ms=10000)
        assert result.is_err()

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 1


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_auto_retry_by_mapped_exception(broker: PostgresBroker) -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = retry_tasks.retry_mapped_exception_task.send()
        result = handle.get(timeout_ms=10000)
        assert result.is_err()

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 1


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_no_retry_for_non_matching(broker: PostgresBroker) -> None:
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = basic_tasks.no_retry_task.send()
        result = handle.get(timeout_ms=5000)
        assert_err(result, expected_code='PERMANENT')

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 0


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_precedence_task_mapper_wins_over_global_no_retry(
    broker: PostgresBroker,
) -> None:
    with run_worker(
        RETRY_PRECEDENCE_INSTANCE,
        ready_check=_make_ready_check(instance_retry_precedence.healthcheck),
    ):
        handle = instance_retry_precedence.task_mapper_wins_no_retry_task.send()
        result = handle.get(timeout_ms=10000)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'TASK_VALUE_ERROR'

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 0
            assert task.status == TaskStatus.FAILED


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_precedence_task_mapper_wins_and_retries(
    broker: PostgresBroker,
) -> None:
    with run_worker(
        RETRY_PRECEDENCE_INSTANCE,
        ready_check=_make_ready_check(instance_retry_precedence.healthcheck),
    ):
        handle = instance_retry_precedence.task_mapper_wins_retry_task.send()
        result = handle.get(timeout_ms=10000)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'TASK_VALUE_ERROR'

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 1
            assert task.status == TaskStatus.FAILED


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_precedence_global_mapper_triggers_retry(
    broker: PostgresBroker,
) -> None:
    with run_worker(
        RETRY_PRECEDENCE_INSTANCE,
        ready_check=_make_ready_check(instance_retry_precedence.healthcheck),
    ):
        handle = instance_retry_precedence.global_mapper_retry_task.send()
        result = handle.get(timeout_ms=10000)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == 'GLOBAL_VALUE_ERROR'

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 1
            assert task.status == TaskStatus.FAILED


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_precedence_exception_name_collision_no_retry(
    broker: PostgresBroker,
) -> None:
    with run_worker(
        RETRY_PRECEDENCE_INSTANCE,
        ready_check=_make_ready_check(instance_retry_precedence.healthcheck),
    ):
        handle = instance_retry_precedence.exception_name_collision_task.send()
        result = handle.get(timeout_ms=10000)
        assert result.is_err()
        assert result.err is not None
        assert result.err.error_code == LibraryErrorCode.UNHANDLED_EXCEPTION

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.retry_count == 0
            assert task.status == TaskStatus.FAILED


@pytest.mark.e2e
def test_default_mode_rejects_queue_name() -> None:
    # Default instance should reject queue_name at task definition time
    with pytest.raises(ConfigurationError, match='cannot specify queue_name'):

        @default_instance.app.task(task_name='e2e_default_reject', queue_name='custom')
        def default_reject_task(x: int) -> TaskResult[int, TaskError]:
            return basic_tasks.simple_task(x)


@pytest.mark.e2e
def test_custom_mode_queue_routing() -> None:
    with run_worker(
        CUSTOM_INSTANCE, ready_check=_make_ready_check(queues_custom.high_task)
    ):
        result_high = queues_custom.high_task.send().get(timeout_ms=5000)
        result_normal = queues_custom.normal_task.send().get(timeout_ms=5000)
        result_low = queues_custom.low_task.send().get(timeout_ms=5000)

        assert_ok(result_high, 'high')
        assert_ok(result_normal, 'normal')
        assert_ok(result_low, 'low')


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_custom_mode_queue_names_stored(custom_broker: PostgresBroker) -> None:
    with run_worker(
        CUSTOM_INSTANCE, ready_check=_make_ready_check(queues_custom.high_task)
    ):
        handle = queues_custom.high_task.send()
        result = handle.get(timeout_ms=5000)
        assert_ok(result, 'high')

        async with custom_broker.session_factory() as session:
            task = await session.get(TaskModel, handle.task_id)
            assert task is not None
            assert task.queue_name == 'high'


# =============================================================================
# L1.5 Queue Modes (continued)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_priority_ordering(custom_broker: PostgresBroker) -> None:
    """L1.5.4: High priority (lower number) tasks are claimed before low priority."""
    # Submit both tasks BEFORE starting worker to ensure fair priority ordering
    # Low priority (100) submitted first
    # Use explicit sent_at ordering to avoid timing sleeps.
    sent_base = datetime.now(timezone.utc)
    task_id_low = custom_broker.enqueue(
        task_name='e2e_low',
        args=(),
        kwargs={},
        queue_name='low',
        priority=100,
        sent_at=sent_base,
    ).unwrap()
    # High priority (1) submitted second
    task_id_high = custom_broker.enqueue(
        task_name='e2e_high',
        args=(),
        kwargs={},
        queue_name='high',
        priority=1,
        sent_at=sent_base + timedelta(milliseconds=50),
    ).unwrap()

    # Now start worker - should claim high priority first despite later submission
    with run_worker(
        CUSTOM_INSTANCE,
        processes=1,
        ready_check=_make_ready_check(queues_custom.high_task),
    ):
        # Wait for both to complete
        custom_broker.get_result(task_id_low, timeout_ms=10000)
        custom_broker.get_result(task_id_high, timeout_ms=10000)

        # Verify high started before low (started_at is set when task runs, more deterministic
        # than claimed_at which can be identical if both claimed in same transaction)
        async with custom_broker.session_factory() as session:
            t_high = await session.get(TaskModel, task_id_high)
            t_low = await session.get(TaskModel, task_id_low)
            assert t_high is not None
            assert t_low is not None
            assert t_high.started_at is not None
            assert t_low.started_at is not None
            assert t_high.started_at < t_low.started_at, (
                f'High priority task started at {t_high.started_at} should be before '
                f'low priority task started at {t_low.started_at}'
            )


@pytest.mark.e2e
def test_custom_mode_requires_queue_name() -> None:
    """L1.5.5: CUSTOM mode requires queue_name at definition time."""
    with pytest.raises(ConfigurationError, match='queue_name is required'):

        @instance_custom.app.task(task_name='e2e_missing_queue_name')
        def missing_queue_task() -> TaskResult[str, TaskError]:
            return TaskResult(ok='test')


# =============================================================================
# L1.6 Task Expiry (good_until)
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_task_expires_before_claim(broker: PostgresBroker) -> None:
    """L1.6.1: Task with past good_until is never claimed."""
    # Submit task that expires in 500ms
    expiry = datetime.now(timezone.utc) + timedelta(milliseconds=500)
    task_id = broker.enqueue(
        task_name='e2e_simple',
        args=(5,),
        kwargs={},
        queue_name='default',
        good_until=expiry,
    ).unwrap()

    # Wait until the task is definitely expired.
    while datetime.now(timezone.utc) <= expiry:
        await asyncio.sleep(0.05)

    # Start worker - should not claim expired task
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        # Observe for a short window and ensure the expired task is never claimed.
        deadline = time.time() + 2.0
        while time.time() < deadline:
            async with broker.session_factory() as session:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.PENDING  # Never claimed
            await asyncio.sleep(0.1)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_task_completes_before_expiry(broker: PostgresBroker) -> None:
    """L1.6.2: Task with future good_until completes successfully."""
    expiry = datetime.now(timezone.utc) + timedelta(seconds=60)
    task_id = broker.enqueue(
        task_name='e2e_simple',
        args=(5,),
        kwargs={},
        queue_name='default',
        good_until=expiry,
    ).unwrap()

    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = broker.get_result(task_id, timeout_ms=5000)
        assert_ok(result, expected_value=10)

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, task_id)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_good_until_already_past(broker: PostgresBroker) -> None:
    """L1.6.3: Task with already-past good_until at enqueue time is never claimed."""
    past = datetime.now(timezone.utc) - timedelta(seconds=10)
    task_id = broker.enqueue(
        task_name='e2e_simple',
        args=(5,),
        kwargs={},
        queue_name='default',
        good_until=past,
    ).unwrap()

    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        await asyncio.sleep(2.0)  # Give worker time to potentially claim

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, task_id)
            assert task is not None
            assert task.status == TaskStatus.PENDING  # Dead on arrival


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_blocked_by_good_until_expiry(broker: PostgresBroker) -> None:
    """L1.6.4: Retry is skipped and task fails when next retry would exceed good_until."""
    good_until = datetime.now(timezone.utc) + timedelta(seconds=10)
    task_options = json.dumps({
        'retry_policy': {
            'max_retries': 3,
            'intervals': [60],
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['TRANSIENT'],
        },
        'good_until': good_until.isoformat(),
    })
    task_id = broker.enqueue(
        task_name='e2e_retry_exhausted',
        args=(),
        kwargs={},
        queue_name='default',
        good_until=good_until,
        task_options=task_options,
    ).unwrap()

    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = broker.get_result(task_id, timeout_ms=15000)
        assert_err(result, expected_code='TRANSIENT')

        async with broker.session_factory() as session:
            task = await session.get(TaskModel, task_id)
            assert task is not None
            assert task.status == TaskStatus.FAILED
            assert task.retry_count == 0


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_retry_succeeds_within_good_until(broker: PostgresBroker) -> None:
    """L1.6.5: Retry proceeds and succeeds when all retries fit within good_until."""
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmp:
        tmp.write('0')
        state_path = tmp.name

    good_until = datetime.now(timezone.utc) + timedelta(seconds=30)
    task_options = json.dumps({
        'retry_policy': {
            'max_retries': 3,
            'intervals': [1, 1, 1],
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['TRANSIENT'],
        },
        'good_until': good_until.isoformat(),
    })
    os.environ['E2E_RETRY_SUCCESS_PATH'] = state_path
    try:
        task_id = broker.enqueue(
            task_name='e2e_retry_success',
            args=(),
            kwargs={},
            queue_name='default',
            good_until=good_until,
            task_options=task_options,
        ).unwrap()

        with run_worker(
            DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
        ):
            result = broker.get_result(task_id, timeout_ms=20000)
            assert_ok(result, expected_value='succeeded_on_attempt_3')

            async with broker.session_factory() as session:
                task = await session.get(TaskModel, task_id)
                assert task is not None
                assert task.status == TaskStatus.COMPLETED
                assert task.retry_count == 2
    finally:
        try:
            os.unlink(state_path)
        except FileNotFoundError:
            pass
        os.environ.pop('E2E_RETRY_SUCCESS_PATH', None)


# =============================================================================
# L1.7 Error Scenarios
# =============================================================================


@pytest.mark.e2e
def test_worker_resolution_error(broker: PostgresBroker) -> None:
    """L1.7.1: Unknown task_name returns WORKER_RESOLUTION_ERROR."""
    task_id = broker.enqueue(
        task_name='nonexistent_task_xyz',
        args=(),
        kwargs={},
        queue_name='default',
    ).unwrap()

    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = broker.get_result(task_id, timeout_ms=5000)
        assert_err(result, expected_code=LibraryErrorCode.WORKER_RESOLUTION_ERROR)


@pytest.mark.e2e
def test_unserializable_result() -> None:
    """L1.7.2: Unserializable result returns WORKER_SERIALIZATION_ERROR."""
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.unserializable_result_task.send().get(timeout_ms=5000)
        assert_err(result, expected_code=LibraryErrorCode.WORKER_SERIALIZATION_ERROR)


@pytest.mark.e2e
def test_task_not_found(broker: PostgresBroker) -> None:
    """L1.7.3: Non-existent task_id returns TASK_NOT_FOUND."""
    fake_id = '00000000-0000-0000-0000-000000000000'
    result = broker.get_result(fake_id, timeout_ms=1000)
    assert_err(result, expected_code=LibraryErrorCode.TASK_NOT_FOUND)


@pytest.mark.e2e
def test_wait_timeout(broker: PostgresBroker) -> None:
    """L1.7.4: Short timeout returns WAIT_TIMEOUT when task doesn't complete."""
    # Submit task but DON'T start worker
    task_id = broker.enqueue(
        task_name='e2e_simple',
        args=(5,),
        kwargs={},
        queue_name='default',
    ).unwrap()

    # Wait with very short timeout - task never completes
    result = broker.get_result(task_id, timeout_ms=500)
    assert_err(result, expected_code=LibraryErrorCode.WAIT_TIMEOUT)


@pytest.mark.e2e
def test_return_none_from_task() -> None:
    """L1.7.5: Task returning None instead of TaskResult produces TASK_EXCEPTION."""
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.return_none_task.send().get(timeout_ms=5000)
        assert_err(result, expected_code=LibraryErrorCode.TASK_EXCEPTION)
        assert result.err is not None
        assert 'None' in (result.err.message or '')


@pytest.mark.e2e
def test_library_error_code_in_user_task() -> None:
    """L1.7.6: Task returning LibraryErrorCode is preserved correctly."""
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = basic_tasks.error_code_task.send().get(timeout_ms=5000)
        assert_err(result, expected_code=LibraryErrorCode.TASK_EXCEPTION)
        assert result.err is not None
        assert result.err.message == 'boom'


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_argument_deserialization_error(broker: PostgresBroker) -> None:
    """L1.7.7: Corrupted args JSON triggers WORKER_SERIALIZATION_ERROR."""
    task_id = str(uuid4())
    # Store args as a JSON string instead of a JSON array
    async with broker.session_factory() as session:
        await session.execute(
            text("""
                INSERT INTO horsies_tasks
                    (id, task_name, queue_name, status, args, kwargs, priority, sent_at,
                     claimed, retry_count, max_retries)
                VALUES
                    (:tid, 'e2e_simple', 'default', 'PENDING', :args, '{}', 100, now(),
                     FALSE, 0, 0)
            """),
            {'tid': task_id, 'args': '"not_a_list"'},
        )
        await session.commit()

    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = broker.get_result(task_id, timeout_ms=5000)
        assert_err(result, expected_code=LibraryErrorCode.WORKER_SERIALIZATION_ERROR)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_malformed_pydantic_args(broker: PostgresBroker) -> None:
    """L1.7.8: Invalid Pydantic model data in args triggers WORKER_SERIALIZATION_ERROR."""
    task_id = str(uuid4())
    # Pydantic marker with data that fails model_validate (age is not coercible to int)
    corrupted_args = json.dumps([{
        '__pydantic_model__': True,
        'module': 'tests.e2e.tasks.basic',
        'qualname': 'UserInput',
        'data': {'name': 'Alice', 'age': 'not_a_number'},
    }])
    async with broker.session_factory() as session:
        await session.execute(
            text("""
                INSERT INTO horsies_tasks
                    (id, task_name, queue_name, status, args, kwargs, priority, sent_at,
                     claimed, retry_count, max_retries)
                VALUES
                    (:tid, 'e2e_pydantic', 'default', 'PENDING', :args, '{}', 100, now(),
                     FALSE, 0, 0)
            """),
            {'tid': task_id, 'args': corrupted_args},
        )
        await session.commit()

    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        result = broker.get_result(task_id, timeout_ms=5000)
        assert_err(result, expected_code=LibraryErrorCode.WORKER_SERIALIZATION_ERROR)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_task_cancelled(broker: PostgresBroker) -> None:
    """L1.7.9: Cancelled task returns TASK_CANCELLED from get_result."""
    task_id = broker.enqueue(
        task_name='e2e_simple',
        args=(5,),
        kwargs={},
        queue_name='default',
    ).unwrap()

    # Cancel directly in DB (no cancel API exists)
    async with broker.session_factory() as session:
        await session.execute(
            text("UPDATE horsies_tasks SET status = 'CANCELLED' WHERE id = :tid"),
            {'tid': task_id},
        )
        await session.commit()

    result = broker.get_result(task_id, timeout_ms=1000)
    assert_err(result, expected_code=LibraryErrorCode.TASK_CANCELLED)
    assert result.err is not None
    assert result.err.data is not None
    assert result.err.data['task_id'] == task_id


# =============================================================================
# L1.8 Concurrency & Fairness
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_multiple_tasks_concurrent(broker: PostgresBroker) -> None:
    """L1.8.1: Multiple processes handle concurrent tasks (DB proves parallelism)."""
    num_tasks = 4
    task_duration_ms = 800
    num_processes = 2

    with run_worker(
        DEFAULT_INSTANCE,
        processes=num_processes,
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        handles = [
            basic_tasks.slow_task.send(task_duration_ms) for _ in range(num_tasks)
        ]

        # Observe runtime concurrency directly from DB instead of wall-clock timing.
        max_running = await poll_max_during(
            broker.session_factory,
            "SELECT COUNT(*) FROM horsies_tasks WHERE status = 'RUNNING'",
            duration_s=3.0,
            poll_interval=0.05,
        )

        # All tasks should succeed
        results = [h.get(timeout_ms=15000) for h in handles]
        assert all(r.is_ok() for r in results)

        # Guard against vacuous sampling and prove at least 2 tasks overlapped.
        assert max_running > 0, 'Polling observed 0 RUNNING tasks — test may be vacuous'
        assert max_running >= 2, (
            f'Expected parallel execution with >=2 RUNNING tasks, observed {max_running}'
        )


@pytest.mark.e2e
def test_skip_locked_prevents_double_claim() -> None:
    """L1.8.2: Two workers should not claim the same task (atomic file proves single execution)."""
    with tempfile.TemporaryDirectory() as log_dir:
        os.environ['E2E_IDEMPOTENT_LOG_DIR'] = log_dir
        try:
            with run_worker(
                DEFAULT_INSTANCE,
                processes=2,
                ready_check=_make_ready_check(basic_tasks.healthcheck),
            ):
                with run_worker(
                    DEFAULT_INSTANCE,
                    processes=2,
                    ready_check=_make_ready_check(basic_tasks.healthcheck),
                ):
                    # Use unique tokens for each task
                    tokens = [f'task_{i}' for i in range(20)]
                    handles = [
                        basic_tasks.idempotent_task.send(token) for token in tokens
                    ]
                    results = [h.get(timeout_ms=15000) for h in handles]

                    # All tasks should succeed (no DOUBLE_EXECUTION errors)
                    for i, r in enumerate(results):
                        assert r.is_ok(), f'Task {i} failed: {r.err}'

                    # Verify each token file exists exactly once (atomic creation proves single execution)
                    for token in tokens:
                        token_file = Path(log_dir) / token
                        assert token_file.exists(), f'Token file {token} not created'
        finally:
            os.environ.pop('E2E_IDEMPOTENT_LOG_DIR', None)


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_max_claim_batch(broker: PostgresBroker) -> None:
    """L1.8.3: Worker with max_claim_batch=1 never claims more than 1 task at a time."""
    max_claim_batch = 1
    num_tasks = 6
    task_duration_ms = 300

    with run_worker(
        DEFAULT_INSTANCE,
        processes=1,  # Single process to simplify assertion
        extra_args=[f'--max-claim-batch={max_claim_batch}'],
        ready_check=_make_ready_check(basic_tasks.healthcheck),
    ):
        # Submit slow tasks
        handles = [
            basic_tasks.slow_task.send(task_duration_ms) for _ in range(num_tasks)
        ]

        # Poll DB while tasks are running to check CLAIMED count
        max_claimed_seen = 0
        deadline = time.time() + 10.0
        all_done = False

        while time.time() < deadline and not all_done:
            async with broker.session_factory() as session:
                # Count CLAIMED tasks (not yet RUNNING)
                result = await session.execute(
                    text("SELECT COUNT(*) FROM horsies_tasks WHERE status = 'CLAIMED'")
                )
                claimed_count = result.scalar() or 0
                max_claimed_seen = max(max_claimed_seen, claimed_count)

                # Check if all done
                result = await session.execute(
                    text(
                        "SELECT COUNT(*) FROM horsies_tasks WHERE status IN ('PENDING', 'CLAIMED', 'RUNNING')"
                    )
                )
                pending_count = result.scalar() or 0
                all_done = pending_count == 0

            await asyncio.sleep(0.05)

        # Verify batch limit was respected
        assert (
            max_claimed_seen <= max_claim_batch
        ), f'CLAIMED count {max_claimed_seen} exceeded max_claim_batch {max_claim_batch}'

        # All tasks should complete successfully
        results = [h.get(timeout_ms=15000) for h in handles]
        assert all(r.is_ok() for r in results)


# =============================================================================
# L1.9 TaskHandle API
# =============================================================================


@pytest.mark.e2e
def test_task_handle_info() -> None:
    """L1.9.1: TaskHandle.info() returns task metadata after completion."""
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = basic_tasks.simple_task.send(5)
        result = handle.get(timeout_ms=5000)
        assert_ok(result)

        info_result = handle.info()
        assert is_ok(info_result)
        info = info_result.ok_value
        assert info is not None
        assert info.task_id == handle.task_id
        assert info.task_name == 'e2e_simple'
        assert info.status == TaskStatus.COMPLETED
        assert info.queue_name == 'default'
        assert info.started_at is not None
        assert info.completed_at is not None


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_task_handle_info_async() -> None:
    """L1.9.2: TaskHandle.info_async() returns task metadata after completion."""
    with run_worker(
        DEFAULT_INSTANCE, ready_check=_make_ready_check(basic_tasks.healthcheck)
    ):
        handle = basic_tasks.simple_task.send(5)
        result = handle.get(timeout_ms=5000)
        assert_ok(result)

        info_result = await handle.info_async()
        assert is_ok(info_result)
        info = info_result.ok_value
        assert info is not None
        assert info.task_id == handle.task_id
        assert info.task_name == 'e2e_simple'
        assert info.status == TaskStatus.COMPLETED
        assert info.started_at is not None
        assert info.completed_at is not None


# =============================================================================
# L1.10 Send Suppression
# =============================================================================


@pytest.mark.e2e
def test_send_suppressed() -> None:
    """L1.10.1: Task send during suppression returns SEND_SUPPRESSED."""
    os.environ['TASKLIB_SUPPRESS_SENDS'] = '1'
    try:
        handle = basic_tasks.simple_task.send(5)
        assert handle.task_id == '<suppressed>'
        result = handle.get(timeout_ms=1000)
        assert_err(result, expected_code=LibraryErrorCode.SEND_SUPPRESSED)
        assert result.err is not None
        assert result.err.data is not None
        assert result.err.data['task_name'] == 'e2e_simple'
    finally:
        os.environ.pop('TASKLIB_SUPPRESS_SENDS', None)


# =============================================================================
# L1.11 Result Hydration
# =============================================================================


@pytest.mark.e2e
@pytest.mark.asyncio(loop_scope='function')
async def test_pydantic_hydration_error(broker: PostgresBroker) -> None:
    """L1.11.1: Unresolvable Pydantic model in stored result triggers PYDANTIC_HYDRATION_ERROR."""
    task_id = str(uuid4())
    # Simulate a completed task whose result contains an unresolvable Pydantic model
    corrupted_result = json.dumps({
        '__task_result__': True,
        'ok': {
            '__pydantic_model__': True,
            'module': 'nonexistent.module.that.does.not.exist',
            'qualname': 'FakeModel',
            'data': {'field': 'value'},
        },
        'err': None,
    })
    async with broker.session_factory() as session:
        await session.execute(
            text("""
                INSERT INTO horsies_tasks
                    (id, task_name, queue_name, status, result, priority, sent_at, completed_at,
                     claimed, retry_count, max_retries)
                VALUES
                    (:tid, 'e2e_simple', 'default', 'COMPLETED', :result, 100, now(), now(),
                     FALSE, 0, 0)
            """),
            {'tid': task_id, 'result': corrupted_result},
        )
        await session.commit()

    result = broker.get_result(task_id, timeout_ms=1000)
    assert_err(result, expected_code=LibraryErrorCode.PYDANTIC_HYDRATION_ERROR)
