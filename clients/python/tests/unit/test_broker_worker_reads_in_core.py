"""The worker state reads, the class reads and the worker ping through the core handle.

The core answers with one record class per type and per union member; the
broker builds the frozen dataclasses of ``horsies.core.models.health`` and
``horsies.core.models.class_coverage`` from them, and maps a failure record
to its error. A history limit that Python's statement refused is answered by
the handle before any core call: a refused limit needs no database. The rows
and their order are the core's.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies._core import (
    AllSlotsServingRecord,
    CoreConnectionError,
    BudgetLoadRecord,
    ExecutionClassBudgetRecord,
    ExecutionClassCoverageRecord,
    ExecutionClassReportRecord,
    DownSlotRecord,
    NoneDueRecord,
    OldestWaitingRecord,
    PendingStreamRecord,
    ProfileNamedRecord,
    ProfileNotRecordedRecord,
    ResourceShortageRecord,
    SlotsDownRecord,
    StreamCoverageRecord,
    StreamServedRecord,
    StreamUnservedRecord,
    StreamWithoutCapacityRecord,
    TooManyPendingCalls,
    WorkerExecutionClassLoadRecord,
    WorkerPongRecord,
    WorkerPongsRecord,
    WorkerStateAbsent,
    WorkerStateRecord,
    WorkerStatesRecord,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    DatabaseErrorCode,
    DatabaseInvalidInput,
    DatabaseListenerDisabled,
    DatabaseOperationFailed,
    DatabaseResult,
)
from horsies.core.models.boot_failure import (
    ResourceShortage,
    ShortageErrno,
    ThreadStartRefused,
)
from horsies.core.models.class_coverage import (
    BudgetLoad,
    ExecutionClassCoverage,
    ExecutionClassReport,
    NoneDue,
    OldestWaiting,
    PendingStream,
    StreamCoverage,
    StreamServed,
    StreamUnserved,
    StreamWithoutCapacity,
    WorkerExecutionClassLoad,
)
from horsies.core.models.cluster_cap import ClusterCap, NoClusterCap
from horsies.core.models.health import (
    AllSlotsServing,
    ExecutionClassBudget,
    ExecutionClassBudgets,
    DownSlot,
    ProfileNamed,
    ProfileNotRecorded,
    SlotsDown,
    SnapshotProfile,
    WorkerPong,
    WorkerStateSnapshot,
)
from horsies.core.models.health_reads import (
    MinResponses,
    MinResponsesAtLeast,
    MinResponsesFullWindow,
    PingTargetWorker,
    WorkerHistoryLimit,
    WorkerHistoryLimitAllRetained,
    WorkerHistoryLimitNewest,
)
from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import ClaimLimits, NoTaskLimit, TaskLimit
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok
from tests.unit.test_broker_postgres import _make_broker  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.unit

_AT: Final = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
_STARTED: Final = datetime(2026, 11, 16, 9, 30, tzinfo=UTC)


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a core record: an instance of ``record_class`` for a
    class pattern, with ``fields``."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _broker_answering(method: str, answer: object) -> PostgresBroker:
    """A broker whose core handle answers ``method`` with ``answer``: a
    record, or an exception that the call raises."""
    broker = cast(PostgresBroker, _make_broker())
    handle = MagicMock()
    match answer:
        case BaseException():
            setattr(handle, method, AsyncMock(side_effect=answer))
        case _:
            setattr(handle, method, AsyncMock(return_value=answer))
    broker._core_handle = handle  # pyright: ignore[reportPrivateUsage]
    return broker


_PROFILE_NAMED_RECORD: Final = _record(ProfileNamedRecord, name='batch')


def _state_record(
    worker_id: str,
    slot_health: object,
    profile: object = _PROFILE_NAMED_RECORD,
    cluster_wide_cap: int | None = None,
) -> MagicMock:
    return _record(
        WorkerStateRecord,
        worker_id=worker_id,
        snapshot_at=_AT,
        hostname='host-1',
        pid=4242,
        child_count=4,
        per_queue_per_pass=8,
        claimed_per_worker=None,
        cluster_wide_cap=cluster_wide_cap,
        queues=['default', 'priority'],
        queue_priorities={'default': 10},
        queue_max_concurrency={},
        recovery_config_json='{"requeue_stale_claimed": true, "check_interval_ms": 30000}',
        tasks_running=3,
        tasks_claimed=1,
        memory_usage_mb=120.5,
        memory_percent=None,
        cpu_percent=12.0,
        children_memory_mb=None,
        worker_started_at=_STARTED,
        slot_health=slot_health,
        execution_class_budgets=[
            _record(
                ExecutionClassBudgetRecord,
                budget='ISOLATED',
                capacity=2,
                serves=['ISOLATED', 'SHARED_ASYNC'],
            ),
        ],
        profile=profile,
    )


_SLOTS_DOWN_RECORD: Final = _record(
    SlotsDownRecord,
    slots=[
        _record(
            DownSlotRecord,
            slot=1,
            cause=_record(ResourceShortageRecord, errno='EMFILE'),
            detail='OSError: [Errno 24] Too many open files',
            down_since=_AT,
            failed_boots=2,
        ),
        _record(
            DownSlotRecord,
            slot=3,
            cause='THREAD_START_REFUSED',
            detail="RuntimeError: can't start new thread",
            down_since=_AT,
            failed_boots=1,
        ),
    ],
)


def _snapshot(
    worker_id: str,
    slot_health: AllSlotsServing | SlotsDown,
    profile: SnapshotProfile = ProfileNamed(name='batch'),
    cluster_wide_cap: ClusterCap | NoClusterCap = NoClusterCap(),
) -> WorkerStateSnapshot:
    return WorkerStateSnapshot(
        worker_id=worker_id,
        snapshot_at=_AT,
        hostname='host-1',
        pid=4242,
        child_count=4,
        claim_limits=ClaimLimits(
            per_queue_per_pass=TaskLimit(tasks=8),
            claimed_per_worker=NoTaskLimit(),
        ),
        cluster_wide_cap=cluster_wide_cap,
        queues=['default', 'priority'],
        queue_priorities={'default': 10},
        queue_max_concurrency={},
        recovery_config={'requeue_stale_claimed': True, 'check_interval_ms': 30000},
        tasks_running=3,
        tasks_claimed=1,
        memory_usage_mb=120.5,
        memory_percent=None,
        cpu_percent=12.0,
        children_memory_mb=None,
        worker_started_at=_STARTED,
        slot_health=slot_health,
        execution_class_budgets=ExecutionClassBudgets(
            (
                ExecutionClassBudget(
                    budget=Execution.ISOLATED,
                    capacity=2,
                    serves=(Execution.ISOLATED, Execution.SHARED_ASYNC),
                ),
            ),
        ),
        profile=profile,
    )


_SLOTS_DOWN: Final = SlotsDown(
    (
        DownSlot(
            slot=1,
            cause=ResourceShortage(errno=ShortageErrno.EMFILE),
            detail='OSError: [Errno 24] Too many open files',
            down_since=_AT,
            failed_boots=2,
        ),
        DownSlot(
            slot=3,
            cause=ThreadStartRefused(),
            detail="RuntimeError: can't start new thread",
            down_since=_AT,
            failed_boots=1,
        ),
    ),
)


class TestWorkerStates:
    @pytest.mark.asyncio
    async def test_each_record_becomes_its_snapshot_field_by_field(self) -> None:
        broker = _broker_answering(
            'worker_states_async',
            _record(
                WorkerStatesRecord,
                states=[
                    _state_record('w1', _record(AllSlotsServingRecord)),
                    _state_record('w2', _SLOTS_DOWN_RECORD),
                ],
            ),
        )

        result = await broker.list_worker_states_async()

        assert result == Ok([
            _snapshot('w1', AllSlotsServing()),
            _snapshot('w2', _SLOTS_DOWN),
        ])

    @pytest.mark.asyncio
    async def test_each_profile_record_becomes_its_snapshot_profile(self) -> None:
        broker = _broker_answering(
            'worker_states_async',
            _record(
                WorkerStatesRecord,
                states=[
                    _state_record('w1', _record(AllSlotsServingRecord)),
                    _state_record(
                        'w2',
                        _record(AllSlotsServingRecord),
                        _record(ProfileNotRecordedRecord),
                    ),
                ],
            ),
        )

        result = await broker.list_worker_states_async()

        assert result == Ok([
            _snapshot('w1', AllSlotsServing(), ProfileNamed(name='batch')),
            _snapshot('w2', AllSlotsServing(), ProfileNotRecorded()),
        ])

    @pytest.mark.asyncio
    async def test_a_stored_cap_is_a_cluster_cap_and_none_is_no_cap(self) -> None:
        broker = _broker_answering(
            'worker_states_async',
            _record(
                WorkerStatesRecord,
                states=[
                    _state_record('w1', _record(AllSlotsServingRecord), cluster_wide_cap=20),
                    _state_record('w2', _record(AllSlotsServingRecord)),
                ],
            ),
        )

        result = await broker.list_worker_states_async()

        assert result == Ok([
            _snapshot('w1', AllSlotsServing(), cluster_wide_cap=ClusterCap(tasks=20)),
            _snapshot('w2', AllSlotsServing(), cluster_wide_cap=NoClusterCap()),
        ])

    @pytest.mark.asyncio
    async def test_a_worker_with_no_snapshot_is_none(self) -> None:
        broker = _broker_answering(
            'worker_state_async', _record(WorkerStateAbsent, worker_id='missing'),
        )

        result = await broker.get_worker_state_async('missing')

        assert result == Ok(None)
        handle: Any = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        handle.worker_state_async.assert_awaited_once_with('missing')

    @pytest.mark.asyncio
    async def test_a_known_worker_is_its_snapshot(self) -> None:
        broker = _broker_answering(
            'worker_state_async', _state_record('w9', _record(AllSlotsServingRecord)),
        )

        result = await broker.get_worker_state_async('w9')

        assert result == Ok(_snapshot('w9', AllSlotsServing()))

    @pytest.mark.parametrize(
        ('limit', 'handle_limit'),
        [(WorkerHistoryLimitAllRetained(), None), (WorkerHistoryLimitNewest(count=7), 7)],
        ids=['all-retained', 'newest'],
    )
    @pytest.mark.asyncio
    async def test_the_history_limit_reaches_the_handle_as_its_count(
        self, limit: WorkerHistoryLimit, handle_limit: int | None,
    ) -> None:
        broker = _broker_answering(
            'worker_state_history_async', _record(WorkerStatesRecord, states=[]),
        )

        result = await broker.get_worker_state_history_async('w1', limit=limit)

        assert result == Ok([])
        handle: Any = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        handle.worker_state_history_async.assert_awaited_once_with('w1', limit=handle_limit)


class TestHistoryLimit:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'limit',
        [0, -1, -(2 ** 70), False],
        ids=['zero', 'negative', 'below-64-bits', 'false'],
    )
    async def test_a_limit_of_zero_or_less_is_invalid_input_before_any_core_call(
        self, limit: int,
    ) -> None:
        """The handle or the core refuses; no database is reachable here, and
        the refusal of a value outside 64 bits or of ``False`` needs none."""
        broker = cast(PostgresBroker, _make_broker())

        result = await broker.get_worker_state_history_async(
            'w1', limit=WorkerHistoryLimitNewest(count=limit),
        )

        match result:
            case Err(DatabaseInvalidInput(message=message, cause=NoCause())):
                assert message == f'limit must be positive when set, got {limit}'
            case _:
                raise AssertionError(f'expected INVALID_INPUT, got {result!r}')

    @pytest.mark.asyncio
    @pytest.mark.parametrize('limit', [2 ** 70, True], ids=['above-64-bits', 'true'])
    async def test_a_limit_that_postgres_refuses_is_a_query_failure_before_any_core_call(
        self, limit: int,
    ) -> None:
        broker = cast(PostgresBroker, _make_broker())

        result = await broker.get_worker_state_history_async(
            'w1', limit=WorkerHistoryLimitNewest(count=limit),
        )

        match result:
            case Err(DatabaseOperationFailed() as error):
                assert error.code is DatabaseErrorCode.MONITORING_QUERY_FAILED
                assert error.retryable is False
                assert error.message.startswith('get_worker_state_history failed: limit ')
            case _:
                raise AssertionError(f'expected MONITORING_QUERY_FAILED, got {result!r}')


class TestClassReads:
    @pytest.mark.asyncio
    async def test_the_coverage_record_becomes_the_fleet_check(self) -> None:
        broker = _broker_answering(
            'execution_class_coverage_async',
            _record(
                ExecutionClassCoverageRecord,
                checked_at=_AT,
                live_workers=2,
                streams=[
                    _record(
                        StreamCoverageRecord,
                        queue='a',
                        execution='ISOLATED',
                        coverage=_record(StreamServedRecord, workers=2, capacity=4),
                    ),
                    _record(
                        StreamCoverageRecord,
                        queue='a',
                        execution='SHARED_ASYNC',
                        coverage=_record(StreamWithoutCapacityRecord, workers=1),
                    ),
                    _record(
                        StreamCoverageRecord,
                        queue='b',
                        execution='SHARED_THREAD',
                        coverage=_record(StreamUnservedRecord),
                    ),
                ],
            ),
        )

        result = await broker.get_execution_class_coverage_async()

        assert result == Ok(
            ExecutionClassCoverage(
                checked_at=_AT,
                live_workers=2,
                streams=(
                    StreamCoverage('a', Execution.ISOLATED, StreamServed(workers=2, capacity=4)),
                    StreamCoverage('a', Execution.SHARED_ASYNC, StreamWithoutCapacity(workers=1)),
                    StreamCoverage('b', Execution.SHARED_THREAD, StreamUnserved()),
                ),
            ),
        )

    @pytest.mark.asyncio
    async def test_the_report_record_becomes_the_class_report(self) -> None:
        broker = _broker_answering(
            'execution_class_report_async',
            _record(
                ExecutionClassReportRecord,
                checked_at=_AT,
                live_window=timedelta(seconds=120),
                streams=[
                    _record(
                        PendingStreamRecord,
                        queue='a',
                        execution='ISOLATED',
                        coverage=_record(StreamUnservedRecord),
                        wait=_record(
                            OldestWaitingRecord,
                            oldest_due_at=_STARTED,
                            wait=timedelta(seconds=90),
                        ),
                    ),
                    _record(
                        PendingStreamRecord,
                        queue='b',
                        execution='SHARED_ASYNC',
                        coverage=_record(StreamServedRecord, workers=1, capacity=8),
                        wait=_record(NoneDueRecord, next_due_at=_AT),
                    ),
                ],
                workers=[
                    _record(
                        WorkerExecutionClassLoadRecord,
                        worker_id='w1',
                        snapshot_at=_AT,
                        budgets=[
                            _record(
                                BudgetLoadRecord,
                                budget='SHARED_ASYNC',
                                capacity=8,
                                serves=['SHARED_ASYNC'],
                                in_flight=3,
                            ),
                        ],
                    ),
                ],
            ),
        )

        result = await broker.get_execution_class_report_async()

        assert result == Ok(
            ExecutionClassReport(
                checked_at=_AT,
                live_window=timedelta(seconds=120),
                streams=(
                    PendingStream(
                        'a',
                        Execution.ISOLATED,
                        StreamUnserved(),
                        OldestWaiting(oldest_due_at=_STARTED, wait=timedelta(seconds=90)),
                    ),
                    PendingStream(
                        'b',
                        Execution.SHARED_ASYNC,
                        StreamServed(workers=1, capacity=8),
                        NoneDue(next_due_at=_AT),
                    ),
                ),
                workers=(
                    WorkerExecutionClassLoad(
                        worker_id='w1',
                        snapshot_at=_AT,
                        budgets=(
                            BudgetLoad(
                                budget=Execution.SHARED_ASYNC,
                                capacity=8,
                                serves=(Execution.SHARED_ASYNC,),
                                in_flight=3,
                            ),
                        ),
                    ),
                ),
            ),
        )


type _Read = Callable[[PostgresBroker], Awaitable[DatabaseResult[Any]]]


async def _list(broker: PostgresBroker) -> DatabaseResult[Any]:
    return await broker.list_worker_states_async()


async def _one(broker: PostgresBroker) -> DatabaseResult[Any]:
    return await broker.get_worker_state_async('w1')


async def _history(broker: PostgresBroker) -> DatabaseResult[Any]:
    return await broker.get_worker_state_history_async(
        'w1', limit=WorkerHistoryLimitNewest(count=5),
    )


async def _coverage(broker: PostgresBroker) -> DatabaseResult[Any]:
    return await broker.get_execution_class_coverage_async()


async def _report(broker: PostgresBroker) -> DatabaseResult[Any]:
    return await broker.get_execution_class_report_async()


_READS: Final[list[tuple[str, str, _Read]]] = [
    ('worker_states_async', 'list_worker_states', _list),
    ('worker_state_async', 'get_worker_state', _one),
    ('worker_state_history_async', 'get_worker_state_history', _history),
    ('execution_class_coverage_async', 'get_execution_class_coverage', _coverage),
    ('execution_class_report_async', 'get_execution_class_report', _report),
]
_READ_IDS: Final = [read for _, read, _ in _READS]


class TestFailures:
    @pytest.mark.parametrize(('method', 'read', 'call'), _READS, ids=_READ_IDS)
    @pytest.mark.parametrize('retryable', [True, False])
    @pytest.mark.asyncio
    async def test_a_read_failure_keeps_its_message_and_retry_rule(
        self, method: str, read: str, call: _Read, retryable: bool,
    ) -> None:
        message = f'{read} failed: connection refused'
        cause = CoreConnectionError(read, 'connection refused', retryable)
        broker = _broker_answering(
            method,
            DatabaseOperationFailed(code=DatabaseErrorCode.MONITORING_QUERY_FAILED, message=message, retryable=retryable, cause=cause),
        )

        result = await call(broker)

        match result:
            case Err(DatabaseOperationFailed() as error):
                assert error.code is DatabaseErrorCode.MONITORING_QUERY_FAILED
                assert error.message == message
                assert error.retryable is retryable
                assert error.cause is cause
            case _:
                raise AssertionError(f'expected MONITORING_QUERY_FAILED, got {result!r}')

    @pytest.mark.asyncio
    async def test_a_core_history_refusal_is_invalid_input(self) -> None:
        message = 'limit must be positive when set, got 0'
        broker = _broker_answering(
            'worker_state_history_async',
            DatabaseInvalidInput(message=message, cause=NoCause()),
        )

        result = await broker.get_worker_state_history_async(
            'w1', limit=WorkerHistoryLimitNewest(count=0),
        )

        assert result == Err(DatabaseInvalidInput(message=message, cause=NoCause()))

    @pytest.mark.parametrize(('method', 'read', 'call'), _READS, ids=_READ_IDS)
    @pytest.mark.asyncio
    async def test_a_core_boundary_error_is_a_monitoring_failure(
        self, method: str, read: str, call: _Read,
    ) -> None:
        raised = TooManyPendingCalls(f'CoreHandle.{method}', 65_536)
        broker = _broker_answering(method, raised)

        result = await call(broker)

        match result:
            case Err(DatabaseOperationFailed() as error):
                assert error.code is DatabaseErrorCode.MONITORING_QUERY_FAILED
                assert error.cause is raised
                assert error.retryable is True
                assert error.message.startswith(f'{read} failed: ')
            case _:
                raise AssertionError(f'expected MONITORING_QUERY_FAILED, got {result!r}')

    @pytest.mark.parametrize(('method', 'read', 'call'), _READS, ids=_READ_IDS)
    @pytest.mark.asyncio
    async def test_another_code_from_the_core_is_a_defect(
        self, method: str, read: str, call: _Read,
    ) -> None:
        broker = _broker_answering(
            method, DatabaseOperationFailed(code=DatabaseErrorCode.DATABASE_PING_FAILED, message='x', retryable=False, cause=CoreConnectionError('test_operation', 'x', True)),
        )

        with pytest.raises(AssertionError, match='DATABASE_PING_FAILED'):
            await call(broker)


class TestWorkerPing:
    @pytest.mark.asyncio
    async def test_the_pongs_become_worker_pongs_and_the_arguments_pass_as_given(self) -> None:
        broker = _broker_answering(
            'ping_workers_async',
            _record(
                WorkerPongsRecord,
                pongs=[
                    _record(
                        WorkerPongRecord,
                        worker_id='w1',
                        hostname='host-1',
                        pid=101,
                        round_trip_ms=1.25,
                    ),
                ],
            ),
        )

        result = await broker.ping_workers_async(
            target=PingTargetWorker(worker_id='w1'),
            wait_ms=500,
            min_responses=MinResponsesAtLeast(count=1),
        )

        assert result == Ok(
            [WorkerPong(worker_id='w1', hostname='host-1', pid=101, round_trip_ms=1.25)],
        )
        handle: Any = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        handle.ping_workers_async.assert_awaited_once_with(
            target_worker_id='w1', wait_ms=500, min_responses=1,
        )

    @pytest.mark.asyncio
    async def test_the_defaults_ask_every_worker_for_the_full_window(self) -> None:
        broker = _broker_answering('ping_workers_async', _record(WorkerPongsRecord, pongs=[]))

        result = await broker.ping_workers_async()

        assert result == Ok([])
        handle: Any = broker._core_handle  # pyright: ignore[reportPrivateUsage]
        handle.ping_workers_async.assert_awaited_once_with(
            target_worker_id=None, wait_ms=2_000, min_responses=None,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('wait_ms', 'min_responses', 'message'),
        [
            (0, MinResponsesFullWindow(), 'wait_ms must be positive, got 0'),
            (-1, MinResponsesFullWindow(), 'wait_ms must be positive, got -1'),
            (
                1.5,
                MinResponsesFullWindow(),
                'ping_workers request refused: wait_ms must be an int, not float',
            ),
            (
                True,
                MinResponsesFullWindow(),
                'ping_workers request refused: wait_ms must be an int, not bool',
            ),
            (
                2 ** 64,
                MinResponsesFullWindow(),
                f'wait_ms must be at most {2 ** 64 - 1}, got {2 ** 64}',
            ),
            (1_000, MinResponsesAtLeast(count=0), 'min_responses must be >= 1 when set, got 0'),
        ],
        ids=['zero', 'negative', 'float', 'bool', 'above-the-range', 'no-replies'],
    )
    async def test_a_refused_argument_is_invalid_input_before_any_core_call(
        self, wait_ms: object, min_responses: MinResponses, message: str,
    ) -> None:
        """The handle refuses with Python's text; no database is reachable here."""
        broker = cast(PostgresBroker, _make_broker())

        result = await broker.ping_workers_async(
            wait_ms=cast(int, wait_ms), min_responses=min_responses,
        )

        assert result == Err(DatabaseInvalidInput(message=message, cause=NoCause()))

    @pytest.mark.asyncio
    async def test_a_broker_without_a_listener_is_listener_disabled(self) -> None:
        message = 'ping_workers needs a listener: this broker has no listener'
        broker = _broker_answering(
            'ping_workers_async',
            DatabaseListenerDisabled(message=message),
        )

        result = await broker.ping_workers_async()

        assert result == Err(DatabaseListenerDisabled(message=message))

    @pytest.mark.parametrize('retryable', [True, False])
    @pytest.mark.asyncio
    async def test_a_ping_failure_keeps_its_message_and_retry_rule(self, retryable: bool) -> None:
        message = 'ping_workers failed: connection refused'
        cause = CoreConnectionError('ping_workers', 'connection refused', retryable)
        broker = _broker_answering(
            'ping_workers_async',
            DatabaseOperationFailed(code=DatabaseErrorCode.WORKER_PING_FAILED, message=message, retryable=retryable, cause=cause),
        )

        result = await broker.ping_workers_async()

        match result:
            case Err(DatabaseOperationFailed() as error):
                assert error.code is DatabaseErrorCode.WORKER_PING_FAILED
                assert error.message == message
                assert error.retryable is retryable
                assert error.cause is cause
            case _:
                raise AssertionError(f'expected WORKER_PING_FAILED, got {result!r}')

    @pytest.mark.asyncio
    async def test_a_core_boundary_error_is_a_ping_failure(self) -> None:
        raised = TooManyPendingCalls('CoreHandle.ping_workers_async', 65_536)
        broker = _broker_answering('ping_workers_async', raised)

        result = await broker.ping_workers_async()

        match result:
            case Err(DatabaseOperationFailed() as error):
                assert error.code is DatabaseErrorCode.WORKER_PING_FAILED
                assert error.cause is raised
                assert error.retryable is True
                assert error.message.startswith('ping_workers failed: ')
            case _:
                raise AssertionError(f'expected WORKER_PING_FAILED, got {result!r}')

    @pytest.mark.asyncio
    async def test_another_code_from_the_core_is_a_defect(self) -> None:
        broker = _broker_answering(
            'ping_workers_async',
            DatabaseOperationFailed(code=DatabaseErrorCode.DATABASE_PING_FAILED, message='x', retryable=False, cause=CoreConnectionError('test_operation', 'x', True)),
        )

        with pytest.raises(AssertionError, match='DATABASE_PING_FAILED'):
            await broker.ping_workers_async()
