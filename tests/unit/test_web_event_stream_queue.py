"""Unit tests for the bounded queue of one SSE stream.

A stream's queue holds at most ``SUBSCRIBER_QUEUE_CAPACITY`` items. On
overflow its queued events are dropped and it takes one event with no ids
(invalidate the whole topic) per topic; an end marker (``None``) stays last.
"""

from __future__ import annotations

from asyncio import Queue

import pytest

from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationFailed,
)
from horsies.core.types.result import Err, Result
from tests.dualrun.reference.web_events import (
    CHANNEL_TOPICS,
    SUBSCRIBER_QUEUE_CAPACITY,
    EventBroadcaster,
    StreamItem,
    TopicEvent,
    new_stream_queue,
    push_stream_item,
    refresh_events,
)

pytestmark = [pytest.mark.unit]


def _drain(queue: Queue[StreamItem]) -> list[StreamItem]:
    items: list[StreamItem] = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


def _fill(queue: Queue[StreamItem]) -> None:
    for index in range(SUBSCRIBER_QUEUE_CAPACITY):
        push_stream_item(queue, TopicEvent(topic='tasks', ids=[f'id-{index}']))


class TestRefreshEvents:
    def test_one_event_with_no_ids_per_topic(self) -> None:
        assert refresh_events() == [
            TopicEvent(topic='tasks', ids=[]),
            TopicEvent(topic='workflows', ids=[]),
            TopicEvent(topic='workers', ids=[]),
        ]
        assert len(refresh_events()) == len(CHANNEL_TOPICS)


class TestPushStreamItem:
    def test_the_queue_is_bounded(self) -> None:
        assert new_stream_queue().maxsize == SUBSCRIBER_QUEUE_CAPACITY == 256

    def test_items_below_the_capacity_keep_their_order(self) -> None:
        queue = new_stream_queue()
        first = TopicEvent(topic='tasks', ids=['a'])
        second = TopicEvent(topic='workers', ids=['w'])

        push_stream_item(queue, first)
        push_stream_item(queue, second)

        assert _drain(queue) == [first, second]

    def test_a_queue_at_the_capacity_holds_every_item(self) -> None:
        queue = new_stream_queue()

        _fill(queue)

        assert queue.qsize() == SUBSCRIBER_QUEUE_CAPACITY
        assert _drain(queue)[-1] == TopicEvent(
            topic='tasks', ids=[f'id-{SUBSCRIBER_QUEUE_CAPACITY - 1}'],
        )

    def test_an_event_past_the_capacity_replaces_the_queue_with_refreshes(self) -> None:
        queue = new_stream_queue()
        _fill(queue)

        push_stream_item(queue, TopicEvent(topic='workflows', ids=['late']))

        assert _drain(queue) == refresh_events()

    def test_an_end_marker_past_the_capacity_stays_after_the_refreshes(self) -> None:
        queue = new_stream_queue()
        _fill(queue)

        push_stream_item(queue, None)

        assert _drain(queue) == [*refresh_events(), None]

    def test_a_dropped_end_marker_is_queued_again_last(self) -> None:
        queue = new_stream_queue()
        for index in range(SUBSCRIBER_QUEUE_CAPACITY - 1):
            push_stream_item(queue, TopicEvent(topic='tasks', ids=[f'id-{index}']))
        push_stream_item(queue, None)

        push_stream_item(queue, TopicEvent(topic='tasks', ids=['late']))

        assert _drain(queue) == [*refresh_events(), None]


class _ListenerThatFails(PostgresListener):
    """A listener whose start fails: the broadcaster is degraded."""

    async def start(self) -> Result[None, BrokerOperationFailed]:
        return Err(
            BrokerOperationFailed(
                code=BrokerErrorCode.LISTENER_START_FAILED,
                message='start refused by the test',
                retryable=False,
                exception=RuntimeError('start refused by the test'),
            ),
        )


class _DegradedBroadcaster(EventBroadcaster):
    def _build_listener(self) -> PostgresListener:
        return _ListenerThatFails('postgresql://unused@localhost/unused')


class TestBroadcasterStreams:
    @pytest.mark.asyncio
    async def test_a_published_event_past_the_capacity_gives_refreshes(self) -> None:
        """The degraded stream holds its end marker; events overflow it."""
        broadcaster = _DegradedBroadcaster('postgresql://unused@localhost/unused')
        queue = await broadcaster.subscribe()
        assert queue.maxsize == SUBSCRIBER_QUEUE_CAPACITY

        for index in range(SUBSCRIBER_QUEUE_CAPACITY):
            broadcaster._publish(  # pyright: ignore[reportPrivateUsage]
                TopicEvent(topic='tasks', ids=[f'id-{index}']),
            )

        assert _drain(queue) == [*refresh_events(), None]
