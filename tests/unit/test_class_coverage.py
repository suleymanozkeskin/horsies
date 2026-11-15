"""The pure functions of the fleet check and the class report.

Each outcome of each function has a test: a stream is served, served
without capacity, or unserved; a stream waits or has no due row; a live
snapshot reads, is missing its budgets (the worker is left out and named),
or is unreadable; a worker's rows fit its budgets or do not.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.models.class_coverage import (
    BudgetLoad,
    ClassesNotRecorded,
    ClassReadRefused,
    LiveWorker,
    NoneDue,
    OldestWaiting,
    StreamCoverage,
    StreamHead,
    StreamServed,
    StreamUnserved,
    StreamWithoutCapacity,
    WorkerClassLoad,
    budget_loads,
    class_coverage,
    class_report,
    live_worker,
    stream_coverage,
    stream_wait,
)
from horsies.core.models.health import ClassBudget
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok

ISOLATED = Execution.ISOLATED
ASYNC = Execution.SHARED_ASYNC
THREAD = Execution.SHARED_THREAD
AT = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


def _isolated_worker(worker_id: str, capacity: int, queues: set[str]) -> LiveWorker:
    """A worker with isolated children only: its budget serves every class."""
    return LiveWorker(
        worker_id=worker_id,
        snapshot_at=AT,
        queues=frozenset(queues),
        budgets=(ClassBudget(ISOLATED, capacity, (ISOLATED, ASYNC, THREAD)),),
    )


def _async_only_worker(worker_id: str, capacity: int, queues: set[str]) -> LiveWorker:
    """A worker with SHARED_ASYNC children and no isolated ones."""
    return LiveWorker(
        worker_id=worker_id,
        snapshot_at=AT,
        queues=frozenset(queues),
        budgets=(ClassBudget(ASYNC, capacity, (ASYNC,)),),
    )


@pytest.mark.unit
class TestStreamCoverage:
    def test_served_sums_the_capacity_of_each_serving_worker(self) -> None:
        workers = [
            _isolated_worker('w1', 4, {'default'}),
            _isolated_worker('w2', 2, {'default'}),
        ]
        assert stream_coverage('default', ASYNC, workers) == StreamServed(
            workers=2,
            capacity=6,
        )

    def test_served_without_capacity_when_every_serving_budget_is_down(self) -> None:
        workers = [_isolated_worker('w1', 0, {'default'})]
        assert stream_coverage('default', ISOLATED, workers) == StreamWithoutCapacity(
            workers=1,
        )

    def test_a_worker_with_capacity_makes_the_stream_served(self) -> None:
        workers = [
            _isolated_worker('w1', 0, {'default'}),
            _isolated_worker('w2', 1, {'default'}),
        ]
        assert stream_coverage('default', ISOLATED, workers) == StreamServed(2, 1)

    def test_unserved_when_no_live_worker_exists(self) -> None:
        assert stream_coverage('default', ISOLATED, []) == StreamUnserved()

    def test_unserved_when_the_serving_worker_runs_another_queue(self) -> None:
        workers = [_isolated_worker('w1', 4, {'emails'})]
        assert stream_coverage('default', ISOLATED, workers) == StreamUnserved()

    def test_an_isolated_retry_waits_when_no_worker_has_isolated_children(self) -> None:
        """A shared task that went back to the queue as ISOLATED is unserved
        while every live worker runs only SHARED_ASYNC children."""
        workers = [_async_only_worker('w1', 40, {'default'})]
        assert stream_coverage('default', ISOLATED, workers) == StreamUnserved()
        assert stream_coverage('default', ASYNC, workers) == StreamServed(1, 40)

    def test_a_worker_that_serves_no_class_serves_nothing(self) -> None:
        worker = LiveWorker('w1', AT, frozenset({'default'}), ())
        for execution in Execution:
            assert stream_coverage('default', execution, [worker]) == StreamUnserved()


@pytest.mark.unit
class TestStreamWait:
    def test_the_oldest_due_head_is_the_wait(self) -> None:
        heads = [AT - timedelta(seconds=30), AT - timedelta(seconds=90)]
        assert stream_wait(heads, AT) == OldestWaiting(
            enqueued_at=AT - timedelta(seconds=90),
            wait=timedelta(seconds=90),
        )

    def test_a_head_due_later_does_not_count_as_waiting(self) -> None:
        heads = [AT + timedelta(minutes=5), AT - timedelta(seconds=10)]
        assert stream_wait(heads, AT) == OldestWaiting(
            AT - timedelta(seconds=10),
            timedelta(seconds=10),
        )

    def test_a_head_due_now_waits_zero(self) -> None:
        assert stream_wait([AT], AT) == OldestWaiting(AT, timedelta(0))

    def test_none_due_names_the_earliest_due_time(self) -> None:
        heads = [AT + timedelta(minutes=5), AT + timedelta(minutes=1)]
        assert stream_wait(heads, AT) == NoneDue(next_due_at=AT + timedelta(minutes=1))

    def test_no_head_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            stream_wait([], AT)


@pytest.mark.unit
class TestLiveWorker:
    def test_a_recorded_snapshot_builds_a_live_worker(self) -> None:
        stored = {'ISOLATED': {'capacity': 2, 'serves': ['ISOLATED']}}
        assert live_worker('w1', AT, ['a', 'b'], stored) == Ok(
            LiveWorker(
                'w1',
                AT,
                frozenset({'a', 'b'}),
                (ClassBudget(ISOLATED, 2, (ISOLATED,)),),
            ),
        )

    def test_a_snapshot_without_budgets_is_a_worker_left_out(self) -> None:
        assert live_worker('w-old', AT, ['a'], None) == Ok(
            ClassesNotRecorded('w-old', AT),
        )

    def test_an_unreadable_snapshot_is_refused_with_the_worker_named(self) -> None:
        match live_worker('w-bad', AT, ['a'], {'EXCLUSIVE': {}}):
            case Err(ClassReadRefused(reason=reason)):
                assert 'w-bad' in reason
                assert 'class_capacity is unreadable' in reason
            case other:
                raise AssertionError(other)


@pytest.mark.unit
class TestBudgetLoads:
    def test_rows_count_against_the_budget_that_serves_their_class(self) -> None:
        worker = LiveWorker(
            'w1',
            AT,
            frozenset({'default'}),
            (
                ClassBudget(ISOLATED, 4, (ISOLATED, THREAD)),
                ClassBudget(ASYNC, 40, (ASYNC,)),
            ),
        )
        held = {ISOLATED: 1, THREAD: 2, ASYNC: 39}
        assert budget_loads(worker, held) == Ok(
            (
                BudgetLoad(ISOLATED, 4, (ISOLATED, THREAD), in_flight=3),
                BudgetLoad(ASYNC, 40, (ASYNC,), in_flight=39),
            ),
        )

    def test_a_worker_that_holds_nothing_has_all_capacity_free(self) -> None:
        worker = _isolated_worker('w1', 4, {'default'})
        match budget_loads(worker, {}):
            case Ok((load,)):
                assert load.in_flight == 0
                assert load.free == 4
            case other:
                raise AssertionError(other)

    def test_rows_of_a_class_that_no_budget_serves_are_refused(self) -> None:
        worker = _async_only_worker('w1', 40, {'default'})
        match budget_loads(worker, {ISOLATED: 1}):
            case Err(ClassReadRefused(reason=reason)):
                assert 'w1' in reason
                assert 'ISOLATED' in reason
            case other:
                raise AssertionError(other)

    def test_free_is_never_below_zero(self) -> None:
        """A prefetch buffer holds more rows than the capacity."""
        assert BudgetLoad(ISOLATED, 2, (ISOLATED,), in_flight=5).free == 0


@pytest.mark.unit
class TestClassCoverage:
    def test_streams_come_in_queue_then_execution_order(self) -> None:
        workers = [_isolated_worker('w1', 1, {'a', 'b'})]
        coverage = class_coverage(
            AT,
            workers,
            [],
            [('b', ISOLATED), ('a', THREAD), ('a', ISOLATED)],
        )

        assert coverage.checked_at == AT
        assert coverage.live_workers == 1
        assert coverage.streams == (
            StreamCoverage('a', ISOLATED, StreamServed(1, 1)),
            StreamCoverage('a', THREAD, StreamServed(1, 1)),
            StreamCoverage('b', ISOLATED, StreamServed(1, 1)),
        )

    def test_no_pending_row_gives_no_stream(self) -> None:
        coverage = class_coverage(AT, [], [], [])
        assert coverage.streams == ()
        assert coverage.live_workers == 0
        assert coverage.not_recorded == ()

    def test_workers_left_out_serve_no_stream_and_are_named_in_id_order(
        self,
    ) -> None:
        not_recorded = [
            ClassesNotRecorded('w-old-2', AT),
            ClassesNotRecorded('w-old-1', AT),
        ]
        coverage = class_coverage(
            AT,
            [_async_only_worker('w1', 2, {'a'})],
            not_recorded,
            [('a', ISOLATED)],
        )

        assert coverage.live_workers == 1
        assert coverage.not_recorded == (
            ClassesNotRecorded('w-old-1', AT),
            ClassesNotRecorded('w-old-2', AT),
        )
        assert coverage.streams == (StreamCoverage('a', ISOLATED, StreamUnserved()),)


@pytest.mark.unit
class TestClassReport:
    def test_heads_of_one_stream_merge_into_one_wait(self) -> None:
        heads = [
            StreamHead('a', ISOLATED, AT - timedelta(seconds=5)),
            StreamHead('a', ISOLATED, AT - timedelta(seconds=50)),
            StreamHead('a', ASYNC, AT + timedelta(seconds=60)),
        ]
        match class_report(AT, [_async_only_worker('w1', 3, {'a'})], [], heads, {}):
            case Ok(report):
                pass
            case Err(refused):
                raise AssertionError(refused)

        assert [(s.queue_name, s.execution) for s in report.streams] == [
            ('a', ISOLATED),
            ('a', ASYNC),
        ]
        isolated, shared = report.streams
        assert isolated.wait == OldestWaiting(
            AT - timedelta(seconds=50), timedelta(seconds=50)
        )
        assert isolated.state == StreamUnserved()
        assert shared.wait == NoneDue(AT + timedelta(seconds=60))
        assert shared.state == StreamServed(1, 3)

    def test_workers_come_in_worker_id_order_with_their_rows(self) -> None:
        workers = [_isolated_worker('w2', 2, {'a'}), _isolated_worker('w1', 4, {'a'})]
        in_flight = {('w1', ISOLATED): 3, ('w2', THREAD): 1}
        match class_report(AT, workers, [], [], in_flight):
            case Ok(report):
                pass
            case Err(refused):
                raise AssertionError(refused)

        all_classes = (ISOLATED, ASYNC, THREAD)
        assert report.workers == (
            WorkerClassLoad('w1', AT, (BudgetLoad(ISOLATED, 4, all_classes, 3),)),
            WorkerClassLoad('w2', AT, (BudgetLoad(ISOLATED, 2, all_classes, 1),)),
        )

    def test_rows_that_no_budget_serves_refuse_the_report(self) -> None:
        workers = [_async_only_worker('w1', 3, {'a'})]
        result = class_report(AT, workers, [], [], {('w1', ISOLATED): 1})
        assert isinstance(result, Err)

    def test_workers_left_out_are_named_and_carry_no_load(self) -> None:
        match class_report(
            AT,
            [_isolated_worker('w1', 2, {'a'})],
            [ClassesNotRecorded('w-old', AT)],
            [],
            {},
        ):
            case Ok(report):
                pass
            case Err(refused):
                raise AssertionError(refused)

        assert [worker.worker_id for worker in report.workers] == ['w1']
        assert report.not_recorded == (ClassesNotRecorded('w-old', AT),)
