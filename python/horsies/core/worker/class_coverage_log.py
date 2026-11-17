"""The worker's log of pending streams that no live worker can run.

Each reaper pass reads the fleet check (``get_class_coverage_async``). A
pending (queue, class) stream that no live worker serves is an error; one
whose serving workers have no capacity in service is a warning. Each worker
process logs one stream at most once per ``CLASS_COVERAGE_LOG_INTERVAL_S``,
and a failed check at most once per the same interval.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final, TypeAlias, assert_never

from horsies.core.models.class_coverage import (
    StreamCoverage,
    StreamServed,
    StreamUnserved,
    StreamWithoutCapacity,
)
from horsies.core.models.tasks import Execution

# The least time, in seconds, between two log lines of one stream (or of a
# failed check) from one worker process.
CLASS_COVERAGE_LOG_INTERVAL_S: Final = 300.0


@dataclass(frozen=True, slots=True)
class StreamKey:
    """The log key of one pending stream."""

    queue_name: str
    execution: Execution


@dataclass(frozen=True, slots=True)
class CheckFailedKey:
    """The log key of a failed coverage check."""


LogKey: TypeAlias = StreamKey | CheckFailedKey


@dataclass(slots=True)
class CoverageLogBound:
    """When this process last logged each key, in ``time.monotonic()`` seconds.

    It holds only the keys logged within the last interval, so its size is
    bounded by the streams that the check returns plus one.
    """

    last_logged_at: dict[LogKey, float] = field(default_factory=dict[LogKey, float])

    def take(self, key: LogKey, now: float) -> bool:
        """Whether ``key`` may log at ``now``. A ``True`` records it as logged."""
        self.last_logged_at = {
            logged: at
            for logged, at in self.last_logged_at.items()
            if now - at < CLASS_COVERAGE_LOG_INTERVAL_S
        }
        if key in self.last_logged_at:
            return False
        self.last_logged_at[key] = now
        return True

    def take_uncovered(
        self,
        streams: Sequence[StreamCoverage],
        now: float,
    ) -> tuple[StreamCoverage, ...]:
        """The unserved and capacity-less streams that may log at ``now``,
        in the order given. Each one returned is recorded as logged."""
        due: list[StreamCoverage] = []
        for stream in streams:
            match stream.state:
                case StreamServed():
                    continue
                case StreamUnserved() | StreamWithoutCapacity():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            if self.take(StreamKey(stream.queue_name, stream.execution), now):
                due.append(stream)
        return tuple(due)


def describe_uncovered(stream: StreamCoverage) -> str:
    """The log text of a stream that no live worker can run.

    A served stream is a defect of the caller (``AssertionError``).
    """
    subject = f'PENDING {stream.execution.value} tasks on queue {stream.queue_name!r}'
    match stream.state:
        case StreamUnserved():
            return (
                f'{subject} wait: no live worker serves this class. '
                f'{_serving_workers_hint(stream.execution)}'
            )
        case StreamWithoutCapacity(workers=workers):
            return (
                f'{subject} wait: {workers} live worker(s) serve this class, '
                'and none has a child in service for it. Check the down '
                'slots of those workers.'
            )
        case StreamServed():
            raise AssertionError(f'{subject}: a served stream has no log line')
        case _ as unreachable:
            assert_never(unreachable)


def _serving_workers_hint(execution: Execution) -> str:
    match execution:
        case Execution.ISOLATED:
            return (
                'Start a worker with isolated children on this queue. A shared '
                'task that went back to the queue as ISOLATED also waits here.'
            )
        case Execution.SHARED_ASYNC | Execution.SHARED_THREAD:
            return (
                f'Start a worker with {execution.value} children or with '
                'isolated children on this queue.'
            )
        case _ as unreachable:
            assert_never(unreachable)
