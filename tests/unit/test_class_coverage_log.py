"""The worker's rate-bounded log of pending streams that no live worker can run."""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationFailed
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.class_coverage import (
    ClassCoverage,
    ClassesNotRecorded,
    StreamCoverage,
    StreamServed,
    StreamUnserved,
    StreamWithoutCapacity,
)
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok
from horsies.core.worker.class_coverage_log import (
    CLASS_COVERAGE_LOG_INTERVAL_S,
    NOT_RECORDED_IDS_LOGGED,
    CheckFailedKey,
    CoverageLogBound,
    StreamKey,
    describe_not_recorded,
    describe_uncovered,
)
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.worker.runtime import _ReaperPassState
from horsies.core.worker.worker import Worker, WorkerConfig
from tests.broker_resources_support import attach_resources, fake_resources

AT = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
UNSERVED = StreamCoverage('default', Execution.ISOLATED, StreamUnserved())
NO_CAPACITY = StreamCoverage(
    'default', Execution.SHARED_ASYNC, StreamWithoutCapacity(2)
)
SERVED = StreamCoverage('default', Execution.SHARED_THREAD, StreamServed(1, 4))


@pytest.mark.unit
class TestCoverageLogBound:
    def test_a_key_logs_once_per_interval(self) -> None:
        bound = CoverageLogBound()
        key = StreamKey('default', Execution.ISOLATED)

        assert bound.take(key, 100.0) is True
        assert bound.take(key, 100.0 + CLASS_COVERAGE_LOG_INTERVAL_S - 1) is False
        assert bound.take(key, 100.0 + CLASS_COVERAGE_LOG_INTERVAL_S) is True

    def test_keys_are_bounded_independently(self) -> None:
        bound = CoverageLogBound()
        assert bound.take(StreamKey('a', Execution.ISOLATED), 0.0) is True
        assert bound.take(StreamKey('b', Execution.ISOLATED), 0.0) is True
        assert bound.take(CheckFailedKey(), 0.0) is True

    def test_keys_past_the_interval_are_dropped(self) -> None:
        bound = CoverageLogBound()
        bound.take(StreamKey('a', Execution.ISOLATED), 0.0)
        bound.take(StreamKey('b', Execution.ISOLATED), CLASS_COVERAGE_LOG_INTERVAL_S)

        assert set(bound.last_logged_at) == {StreamKey('b', Execution.ISOLATED)}

    def test_served_streams_never_log(self) -> None:
        bound = CoverageLogBound()
        assert bound.take_uncovered([SERVED], 0.0) == ()
        assert bound.last_logged_at == {}

    def test_unserved_and_capacity_less_streams_log_in_order(self) -> None:
        bound = CoverageLogBound()
        assert bound.take_uncovered([UNSERVED, SERVED, NO_CAPACITY], 0.0) == (
            UNSERVED,
            NO_CAPACITY,
        )

    def test_a_stream_does_not_log_again_within_the_interval(self) -> None:
        bound = CoverageLogBound()
        bound.take_uncovered([UNSERVED], 0.0)

        assert bound.take_uncovered([UNSERVED], 1.0) == ()
        assert bound.take_uncovered([UNSERVED], CLASS_COVERAGE_LOG_INTERVAL_S) == (
            UNSERVED,
        )


@pytest.mark.unit
class TestDescribeUncovered:
    def test_an_unserved_isolated_stream_names_the_isolated_retry(self) -> None:
        text = describe_uncovered(UNSERVED)
        assert "PENDING ISOLATED tasks on queue 'default'" in text
        assert 'no live worker serves this class' in text
        assert 'isolated children' in text
        assert 'went back to the queue as ISOLATED' in text

    @pytest.mark.parametrize(
        'execution', [Execution.SHARED_ASYNC, Execution.SHARED_THREAD]
    )
    def test_an_unserved_shared_stream_names_both_kinds_of_children(
        self,
        execution: Execution,
    ) -> None:
        text = describe_uncovered(StreamCoverage('q', execution, StreamUnserved()))
        assert f'{execution.value} children or with isolated children' in text

    def test_a_capacity_less_stream_names_its_serving_workers(self) -> None:
        text = describe_uncovered(NO_CAPACITY)
        assert '2 live worker(s) serve this class' in text
        assert 'down slots' in text

    def test_a_served_stream_has_no_text(self) -> None:
        with pytest.raises(AssertionError):
            describe_uncovered(SERVED)


def _broker_returning(result: object) -> MagicMock:
    broker = MagicMock()
    broker.get_class_coverage_async = AsyncMock(return_value=result)
    return broker


@pytest.mark.unit
class TestDescribeNotRecorded:
    def test_it_names_each_worker_left_out(self) -> None:
        text = describe_not_recorded(
            [ClassesNotRecorded('w-old-1', AT), ClassesNotRecorded('w-old-2', AT)],
        )

        assert text.startswith('2 live worker(s) are left out')
        assert 'no class_capacity' in text
        assert text.endswith('Workers: w-old-1, w-old-2.')

    def test_it_names_at_most_the_bound_and_counts_the_rest(self) -> None:
        workers = [
            ClassesNotRecorded(f'w-{index:02d}', AT)
            for index in range(NOT_RECORDED_IDS_LOGGED + 3)
        ]

        text = describe_not_recorded(workers)

        assert f'w-{NOT_RECORDED_IDS_LOGGED - 1:02d}' in text
        assert f'w-{NOT_RECORDED_IDS_LOGGED:02d}' not in text
        assert text.endswith(' and 3 more.')

    def test_no_worker_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            describe_not_recorded([])


@pytest.mark.unit
@pytest.mark.asyncio
class TestReaperCoverageCheck:
    """``ReaperMixin._check_class_coverage``: reads, levels and bounds."""

    async def test_an_unserved_stream_logs_an_error_once(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _broker_returning(Ok(ClassCoverage(AT, 1, (), (UNSERVED, SERVED))))
        state = _ReaperPassState()

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            await ReaperMixin()._check_class_coverage(broker, state)
            await ReaperMixin()._check_class_coverage(broker, state)

        records = [r for r in caplog.records if 'PENDING' in r.getMessage()]
        assert [r.levelno for r in records] == [logging.ERROR]
        assert broker.get_class_coverage_async.await_count == 2

    async def test_a_capacity_less_stream_logs_a_warning(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _broker_returning(Ok(ClassCoverage(AT, 2, (), (NO_CAPACITY,))))

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            await ReaperMixin()._check_class_coverage(broker, _ReaperPassState())

        records = [r for r in caplog.records if 'PENDING' in r.getMessage()]
        assert [r.levelno for r in records] == [logging.WARNING]

    async def test_a_failed_check_logs_one_warning_per_interval(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        failure = BrokerOperationFailed(
            code=BrokerErrorCode.MONITORING_QUERY_FAILED,
            message='get_class_coverage refused: worker w1 ...',
            retryable=False,
            exception=ValueError('x'),
        )
        broker = _broker_returning(Err(failure))
        state = _ReaperPassState()

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            await ReaperMixin()._check_class_coverage(broker, state)
            await ReaperMixin()._check_class_coverage(broker, state)

        records = [
            r for r in caplog.records if 'coverage check failed' in r.getMessage()
        ]
        assert len(records) == 1
        assert 'worker w1' in records[0].getMessage()

    async def test_workers_left_out_log_one_warning_per_interval_with_streams(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A live worker without recorded budgets does not stop the check: the
        unserved stream still logs, and the left-out worker is named once."""
        coverage = ClassCoverage(
            AT,
            1,
            (ClassesNotRecorded('w-old', AT),),
            (UNSERVED,),
        )
        broker = _broker_returning(Ok(coverage))
        state = _ReaperPassState()

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            await ReaperMixin()._check_class_coverage(broker, state)
            await ReaperMixin()._check_class_coverage(broker, state)

        left_out = [r for r in caplog.records if 'left out' in r.getMessage()]
        assert [r.levelno for r in left_out] == [logging.WARNING]
        assert 'w-old' in left_out[0].getMessage()
        pending = [r for r in caplog.records if 'PENDING' in r.getMessage()]
        assert [r.levelno for r in pending] == [logging.ERROR]

    async def test_no_read_before_the_first_check_time(self) -> None:
        broker = _broker_returning(Ok(ClassCoverage(AT, 0, (), (UNSERVED,))))
        state = _ReaperPassState(class_coverage_checks_from=float('inf'))

        await ReaperMixin()._check_class_coverage(broker, state)

        broker.get_class_coverage_async.assert_not_awaited()

    async def test_the_reaper_loop_starts_checks_one_interval_after_its_start(
        self,
    ) -> None:
        """The workers that start together write their first snapshot before
        any of them checks, so a fleet start logs no unserved stream."""
        cfg = WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
            parent_pool_size=1,
            parent_max_overflow=0,
            recovery_config=RecoveryConfig(check_interval_ms=7_000),
        )
        worker = Worker(session_factory=MagicMock(), listener=MagicMock(), cfg=cfg)
        worker.broker = MagicMock()
        attach_resources(worker.broker, fake_resources())
        seen: list[_ReaperPassState] = []

        async def _pass(*args: Any, **kwargs: Any) -> None:
            seen.append(args[4])
            worker._stop.set()

        worker._run_reaper_pass = AsyncMock(side_effect=_pass)
        started = time.monotonic()
        await asyncio.wait_for(worker._reaper_loop(), timeout=3.0)

        assert len(seen) == 1
        assert seen[0].class_coverage_checks_from >= started + 7.0
        assert seen[0].class_coverage_checks_from <= time.monotonic() + 7.0
