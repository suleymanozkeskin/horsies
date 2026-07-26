"""Unit tests for the SSE event coalescer.

The coalescer is what keeps a burst of status changes from costing every
connected browser one invalidation each. Covers window batching, dedup within
a window, the id cap degrading to "invalidate the whole topic", and that a
drained window starts clean.
"""

from __future__ import annotations

import pytest

from horsies.web.events import (
    CHANNEL_TOPICS,
    MAX_IDS_PER_EVENT,
    EventCoalescer,
)

pytestmark = [pytest.mark.unit]


class TestChannelTopics:
    """The three monitoring channels map onto the three client topics."""

    def test_every_channel_maps_to_a_topic(self) -> None:
        assert CHANNEL_TOPICS == {
            'horsies_task_status': 'tasks',
            'horsies_workflow_status': 'workflows',
            'horsies_worker_state': 'workers',
        }


class TestEventCoalescer:
    """One event per topic per window, carrying the ids that changed."""

    def test_empty_window_produces_nothing(self) -> None:
        coalescer = EventCoalescer()

        assert coalescer.drain() == []

    def test_batches_ids_for_one_topic(self) -> None:
        coalescer = EventCoalescer()
        coalescer.record('tasks', 'a')
        coalescer.record('tasks', 'b')

        events = coalescer.drain()

        assert len(events) == 1
        assert events[0].topic == 'tasks'
        assert events[0].ids == ['a', 'b']

    def test_duplicate_ids_within_a_window_collapse(self) -> None:
        coalescer = EventCoalescer()
        for _ in range(5):
            coalescer.record('tasks', 'same-id')

        events = coalescer.drain()

        assert events[0].ids == ['same-id']

    def test_topics_are_batched_independently(self) -> None:
        coalescer = EventCoalescer()
        coalescer.record('tasks', 'task-1')
        coalescer.record('workflows', 'run-1')
        coalescer.record('workers', 'worker-1')

        by_topic = {event.topic: event.ids for event in coalescer.drain()}

        assert by_topic == {
            'tasks': ['task-1'],
            'workflows': ['run-1'],
            'workers': ['worker-1'],
        }

    def test_draining_starts_a_fresh_window(self) -> None:
        coalescer = EventCoalescer()
        coalescer.record('tasks', 'a')
        coalescer.drain()

        assert coalescer.drain() == []

    def test_ids_recorded_after_a_drain_form_the_next_event(self) -> None:
        coalescer = EventCoalescer()
        coalescer.record('tasks', 'a')
        coalescer.drain()
        coalescer.record('tasks', 'b')

        events = coalescer.drain()

        assert events[0].ids == ['b']

    def test_exactly_the_cap_still_carries_every_id(self) -> None:
        coalescer = EventCoalescer(max_ids=3)
        for index in range(3):
            coalescer.record('tasks', f'id-{index}')

        events = coalescer.drain()

        assert events[0].ids == ['id-0', 'id-1', 'id-2']

    def test_one_past_the_cap_degrades_to_the_whole_topic(self) -> None:
        """An empty id list is the client's signal to invalidate everything."""
        coalescer = EventCoalescer(max_ids=3)
        for index in range(4):
            coalescer.record('tasks', f'id-{index}')

        events = coalescer.drain()

        assert len(events) == 1
        assert events[0].topic == 'tasks'
        assert events[0].ids == []

    def test_an_overflowed_window_stays_overflowed(self) -> None:
        coalescer = EventCoalescer(max_ids=2)
        for index in range(10):
            coalescer.record('tasks', f'id-{index}')

        assert coalescer.drain()[0].ids == []

    def test_overflow_does_not_leak_into_the_next_window(self) -> None:
        coalescer = EventCoalescer(max_ids=2)
        for index in range(5):
            coalescer.record('tasks', f'id-{index}')
        coalescer.drain()

        coalescer.record('tasks', 'fresh')

        assert coalescer.drain()[0].ids == ['fresh']

    def test_overflow_is_per_topic(self) -> None:
        coalescer = EventCoalescer(max_ids=2)
        for index in range(5):
            coalescer.record('tasks', f'id-{index}')
        coalescer.record('workers', 'worker-1')

        by_topic = {event.topic: event.ids for event in coalescer.drain()}

        assert by_topic['tasks'] == []
        assert by_topic['workers'] == ['worker-1']

    def test_default_cap_matches_the_published_constant(self) -> None:
        coalescer = EventCoalescer()
        for index in range(MAX_IDS_PER_EVENT):
            coalescer.record('tasks', f'id-{index}')

        assert len(coalescer.drain()[0].ids) == MAX_IDS_PER_EVENT
