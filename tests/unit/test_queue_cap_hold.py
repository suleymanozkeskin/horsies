"""The soft-cap start check: which held rows of a capped queue may start.

A held row starts while its queue's count is under the cap and a child of
its route (its budget) has a free slot or seat at that moment.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Final
from unittest.mock import MagicMock

import pytest

from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import SharedAsyncChildren
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.slots import (
    Busy,
    Closing,
    CloseReason,
    Idle,
    Open,
    SeatRuns,
    SlotPool,
    Starting,
)
from horsies.core.worker.worker import Worker
from horsies.core.worker.queue_cap_hold import (
    HeldRow,
    QueueCapHold,
    StartedClaim,
    read_queue_starts,
    start_check_caps,
)

pytestmark = [pytest.mark.unit]

COLUMNS = ('queue_name', 'running', 'started_not_running')
ISOLATED: Final = Execution.ISOLATED
SHARED_ASYNC: Final = Execution.SHARED_ASYNC
SHARED_THREAD: Final = Execution.SHARED_THREAD
CLAIMED_AT: Final = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)
# Free capacity that never limits the rows of these tests.
MANY_FREE: Final[Mapping[Execution, int]] = {ISOLATED: 100}


def _row(
    task_id: str,
    queue_name: str = 'capped',
    budget: Execution = ISOLATED,
) -> HeldRow:
    return HeldRow(
        task_id=task_id,
        queue_name=queue_name,
        budget=budget,
        claimed_at=CLAIMED_AT,
        row={'id': task_id},
    )


def _hold(caps: dict[str, int], *rows: HeldRow) -> QueueCapHold:
    hold = QueueCapHold(caps)
    for row in rows:
        hold.hold(row)
    return hold


class TestStartCheckCaps:
    def test_soft_cap_mode_checks_every_capped_queue(self) -> None:
        assert start_check_caps(2, {'capped': 3}) == {'capped': 3}

    def test_hard_cap_mode_checks_no_queue(self) -> None:
        assert start_check_caps(0, {'capped': 3}) == {}

    def test_negative_prefetch_buffer_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            start_check_caps(-1, {'capped': 3})


class TestHolds:
    def test_a_capped_queue_is_held(self) -> None:
        assert QueueCapHold({'capped': 1}).holds('capped')

    def test_an_uncapped_queue_is_not_held(self) -> None:
        assert not QueueCapHold({'capped': 1}).holds('free')

    def test_holding_a_row_of_an_uncapped_queue_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            QueueCapHold({'capped': 1}).hold(_row('t1', 'free'))


class TestAdmit:
    def test_a_queue_under_its_cap_starts_its_row(self) -> None:
        hold = _hold({'capped': 2}, _row('t1'))

        assert hold.admit({'capped': 1}, MANY_FREE) == [_row('t1')]
        assert hold.held_count() == 0
        assert hold.started_ids() == ('t1',)
        assert hold.started_claims() == (StartedClaim(task_id='t1', claimed_at=CLAIMED_AT),)

    def test_a_queue_at_its_cap_keeps_its_row(self) -> None:
        hold = _hold({'capped': 2}, _row('t1'))

        assert hold.admit({'capped': 2}, MANY_FREE) == []
        assert hold.held_count() == 1
        assert hold.started_ids() == ()

    def test_a_queue_with_no_count_has_0(self) -> None:
        hold = _hold({'capped': 1}, _row('t1'))

        assert hold.admit({}, MANY_FREE) == [_row('t1')]

    def test_rows_start_in_claim_order_up_to_the_room(self) -> None:
        hold = _hold({'capped': 3}, _row('t1'), _row('t2'), _row('t3'))

        assert hold.admit({'capped': 1}, MANY_FREE) == [_row('t1'), _row('t2')]
        assert hold.drain() == [_row('t3')]

    def test_queues_have_their_own_room(self) -> None:
        hold = _hold(
            {'a': 1, 'b': 1},
            _row('a1', 'a'),
            _row('a2', 'a'),
            _row('b1', 'b'),
        )

        assert hold.admit({}, MANY_FREE) == [_row('a1', 'a'), _row('b1', 'b')]
        assert hold.held_queues() == ('a',)

    def test_a_cap_of_0_starts_nothing(self) -> None:
        hold = _hold({'capped': 0}, _row('t1'))

        assert hold.admit({}, MANY_FREE) == []


class TestAdmitOnlyWithAFreeChild:
    """A row under its cap starts only while a child of its route is free,
    so it goes to a child at once and never waits in the slot pool."""

    def test_a_row_under_its_cap_with_no_free_child_stays_held(self) -> None:
        hold = _hold({'capped': 1}, _row('t1'))

        assert hold.admit({}, {ISOLATED: 0}) == []
        assert hold.held_count() == 1
        assert hold.started_ids() == ()

    def test_a_budget_with_no_free_capacity_entry_has_0(self) -> None:
        hold = _hold({'capped': 1}, _row('t1', budget=SHARED_ASYNC))

        assert hold.admit({}, {ISOLATED: 3}) == []
        assert hold.held_count() == 1

    def test_the_free_capacity_bounds_the_rows_started_in_one_pass(self) -> None:
        hold = _hold({'capped': 5}, _row('t1'), _row('t2'), _row('t3'))

        assert hold.admit({}, {ISOLATED: 2}) == [_row('t1'), _row('t2')]
        assert hold.drain() == [_row('t3')]

    def test_queues_that_share_a_budget_share_its_free_capacity(self) -> None:
        hold = _hold({'a': 1, 'b': 1}, _row('a1', 'a'), _row('b1', 'b'))

        assert hold.admit({}, {ISOLATED: 1}) == [_row('a1', 'a')]
        assert hold.held_queues() == ('b',)

    def test_a_row_of_a_budget_with_room_starts_while_another_budget_is_full(
        self,
    ) -> None:
        hold = _hold(
            {'capped': 2},
            _row('t1', budget=ISOLATED),
            _row('t2', budget=SHARED_THREAD),
        )

        assert hold.admit({}, {ISOLATED: 0, SHARED_THREAD: 1}) == [
            _row('t2', budget=SHARED_THREAD),
        ]
        assert hold.drain() == [_row('t1', budget=ISOLATED)]

    def test_a_queue_at_its_cap_keeps_its_row_with_a_free_child(self) -> None:
        hold = _hold({'capped': 1}, _row('t1'))

        assert hold.admit({'capped': 1}, {ISOLATED: 1}) == []

    def test_a_kept_row_starts_on_a_later_pass_with_a_free_child(self) -> None:
        hold = _hold({'capped': 1}, _row('t1'))
        assert hold.admit({}, {ISOLATED: 0}) == []

        assert hold.admit({}, {ISOLATED: 1}) == [_row('t1')]
        assert hold.started_ids() == ('t1',)


def _worker_with_pool(**cfg: Any) -> tuple[Worker, SlotPool]:
    """A worker with a slot pool that has not booted: every slot is
    ``Starting`` until a test sets its state."""
    worker = Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(dsn='x', psycopg_dsn='y', queues=['default'], **cfg),
    )
    pool = SlotPool(worker._slot_pool_config(), pending_limit=10)
    worker._slots = pool
    return worker, pool


def _open(seats: int, runs: int) -> Open:
    """An open shared child with ``runs`` of its ``seats`` in use."""
    table = SeatRuns(seats)
    for dispatch in range(runs):
        table._by_dispatch[dispatch] = MagicMock()
    return Open(table)


@pytest.mark.asyncio(loop_scope='function')
class TestFreeChildren:
    """The free capacity of each budget: what takes a task at once."""

    async def test_isolated_counts_the_idle_slots(self) -> None:
        worker, pool = _worker_with_pool(processes=3)
        pool._slots[0].state = Idle()
        pool._slots[1].state = Busy(task_id='t1')
        pool._slots[2].state = Starting()

        assert worker._free_children(ISOLATED) == 1

    async def test_isolated_tasks_that_wait_for_a_slot_take_idle_slots(
        self,
    ) -> None:
        worker, pool = _worker_with_pool(processes=2)
        pool._slots[0].state = Idle()
        pool._slots[1].state = Idle()
        pool._pending.append(MagicMock())

        assert worker._free_children(ISOLATED) == 1

        pool._pending.append(MagicMock())
        pool._pending.append(MagicMock())
        assert worker._free_children(ISOLATED) == 0

    async def test_shared_counts_the_free_seats_of_open_children(self) -> None:
        worker, pool = _worker_with_pool(
            processes=1,
            shared_async=SharedAsyncChildren(count=3, seats=4),
        )
        pool._shared_slots[0].state = _open(seats=4, runs=1)
        pool._shared_slots[1].state = Closing(
            reason=CloseReason.TASK_COUNT,
            runs=SeatRuns(4),
        )
        pool._shared_slots[2].state = Starting()

        assert worker._free_children(SHARED_ASYNC) == 3

    async def test_shared_tasks_that_wait_for_a_seat_take_free_seats(self) -> None:
        worker, pool = _worker_with_pool(
            processes=1,
            shared_async=SharedAsyncChildren(count=1, seats=2),
        )
        pool._shared_slots[0].state = _open(seats=2, runs=0)
        pool._shared_pending.append(MagicMock())

        assert worker._free_children(SHARED_ASYNC) == 1

    async def test_a_stopping_pool_has_no_free_child(self) -> None:
        worker, pool = _worker_with_pool(processes=1)
        pool._slots[0].state = Idle()
        pool._stopping = True

        assert worker._free_children(ISOLATED) == 0


class TestEndAndDrain:
    def test_end_forgets_a_started_row(self) -> None:
        hold = _hold({'capped': 1}, _row('t1'))
        _ = hold.admit({}, MANY_FREE)

        hold.end('t1')

        assert hold.started_ids() == ()

    def test_end_of_a_row_that_was_not_started_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            QueueCapHold({'capped': 1}).end('t1')

    def test_drain_takes_every_held_row_in_claim_order(self) -> None:
        hold = _hold({'a': 1, 'b': 1}, _row('a1', 'a'), _row('b1', 'b'))

        assert hold.drain() == [_row('a1', 'a'), _row('b1', 'b')]
        assert hold.held_count() == 0
        assert hold.held_queues() == ()


class TestReadQueueStarts:
    def test_count_is_running_plus_started_not_running(self) -> None:
        assert read_queue_starts(COLUMNS, [('a', 2, 1), ('b', 0, 3)]) == {'a': 3, 'b': 3}

    def test_no_row_is_no_count(self) -> None:
        assert read_queue_starts(COLUMNS, []) == {}

    def test_a_queue_counted_twice_is_a_defect(self) -> None:
        with pytest.raises(AssertionError):
            read_queue_starts(COLUMNS, [('a', 1, 0), ('a', 1, 0)])
