"""The soft-cap start check: which held rows of a capped queue may start."""

from __future__ import annotations

import pytest

from horsies.core.worker.queue_cap_hold import (
    HeldRow,
    QueueCapHold,
    read_queue_starts,
    start_check_caps,
)

pytestmark = [pytest.mark.unit]

COLUMNS = ('queue_name', 'running', 'started_not_running')


def _row(task_id: str, queue_name: str = 'capped') -> HeldRow:
    return HeldRow(task_id=task_id, queue_name=queue_name, row={'id': task_id})


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

        assert hold.admit({'capped': 1}) == [_row('t1')]
        assert hold.held_count() == 0
        assert hold.started_ids() == ('t1',)

    def test_a_queue_at_its_cap_keeps_its_row(self) -> None:
        hold = _hold({'capped': 2}, _row('t1'))

        assert hold.admit({'capped': 2}) == []
        assert hold.held_count() == 1
        assert hold.started_ids() == ()

    def test_a_queue_with_no_count_has_0(self) -> None:
        hold = _hold({'capped': 1}, _row('t1'))

        assert hold.admit({}) == [_row('t1')]

    def test_rows_start_in_claim_order_up_to_the_room(self) -> None:
        hold = _hold({'capped': 3}, _row('t1'), _row('t2'), _row('t3'))

        assert hold.admit({'capped': 1}) == [_row('t1'), _row('t2')]
        assert hold.drain() == [_row('t3')]

    def test_queues_have_their_own_room(self) -> None:
        hold = _hold(
            {'a': 1, 'b': 1},
            _row('a1', 'a'),
            _row('a2', 'a'),
            _row('b1', 'b'),
        )

        assert hold.admit({}) == [_row('a1', 'a'), _row('b1', 'b')]
        assert hold.held_queues() == ('a',)

    def test_a_cap_of_0_starts_nothing(self) -> None:
        hold = _hold({'capped': 0}, _row('t1'))

        assert hold.admit({}) == []


class TestEndAndDrain:
    def test_end_forgets_a_started_row(self) -> None:
        hold = _hold({'capped': 1}, _row('t1'))
        _ = hold.admit({})

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
