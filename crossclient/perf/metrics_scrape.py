"""Reads of the metrics endpoint of a drain worker.

- ``read_endpoint_counts``: one read of ``/metrics`` at the end of a drain,
  for the telemetry columns of a cell: the series count and the drop
  counters.
- ``EndpointReader``: the reads of the ``scraped`` level and above while a
  drain runs: ``/metrics`` and ``/livez`` every interval, on one thread of
  the harness, as a Prometheus server reads a target. Its tally counts the
  answers.

Each read opens a new connection and has a time bound. A read before the
worker bound its endpoint is not answered.
"""

from __future__ import annotations

import re
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import timedelta
from typing import Final, assert_never

from crossclient.perf.telemetry_levels import ListenOn

READ_TIMEOUT: Final = timedelta(seconds=2)
"""The longest time of one read of the endpoint."""

MAX_EXPOSITION_BYTES: Final = 16 * 1024 * 1024
"""The most bytes of one ``/metrics`` answer that a read takes."""

EVENTS_DROPPED: Final = 'horsies_telemetry_events_dropped_total'
SERIES_DROPPED: Final = 'horsies_telemetry_series_dropped_total'
HANDLER_DROPPED: Final = 'horsies_telemetry_handler_dropped_total'
HANDLER_ERRORS: Final = 'horsies_telemetry_handler_errors_total'
QUEUE_FULL_CAUSE: Final = 'queue_full'
CHILD_DOWN_CAUSE: Final = 'child_down'

_SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)(\s+\S+)?$')
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


@dataclass(frozen=True, slots=True)
class Sample:
    """One line of samples of the Prometheus text format: one series."""

    name: str
    labels: tuple[tuple[str, str], ...]
    value: float

    def label(self, key: str) -> str | None:
        """The value of label ``key``; ``None`` when the series has no such
        label."""
        for name, value in self.labels:
            if name == key:
                return value
        return None


@dataclass(frozen=True, slots=True)
class ExpositionMalformed:
    """A line of the answer is not a sample of the Prometheus text format."""

    line_number: int
    line: str


def parse_exposition(text: str) -> tuple[Sample, ...] | ExpositionMalformed:
    """The samples of a Prometheus text format answer. Comment lines and empty
    lines are skipped. Pure."""
    samples: list[Sample] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.startswith('#'):
            continue
        matched = _SAMPLE.match(line)
        if matched is None:
            return ExpositionMalformed(line_number=number, line=line)
        name, labels, value, _timestamp = matched.groups()
        try:
            number_value = float(value)
        except ValueError:
            return ExpositionMalformed(line_number=number, line=line)
        samples.append(
            Sample(
                name=name,
                labels=tuple(_LABEL.findall(labels or '')),
                value=number_value,
            )
        )
    return tuple(samples)


@dataclass(frozen=True, slots=True)
class EndpointCounts:
    """The telemetry columns of a cell, from one read of ``/metrics``.

    ``series`` is the number of sample lines (each histogram bucket, sum and
    count is a series). The drop counts are the totals of the counters since
    the start of the worker."""

    series: int
    events_dropped_queue_full: int
    events_dropped_child_down: int
    series_dropped: int
    handler_dropped: int
    handler_errors: int


def endpoint_counts(samples: tuple[Sample, ...]) -> EndpointCounts:
    """The telemetry columns of the samples of one read. A counter with no
    series counts 0: the worker writes a series at its first sample. Pure."""
    return EndpointCounts(
        series=len(samples),
        events_dropped_queue_full=_total(samples, EVENTS_DROPPED, QUEUE_FULL_CAUSE),
        events_dropped_child_down=_total(samples, EVENTS_DROPPED, CHILD_DOWN_CAUSE),
        series_dropped=_total(samples, SERIES_DROPPED, None),
        handler_dropped=_total(samples, HANDLER_DROPPED, None),
        handler_errors=_total(samples, HANDLER_ERRORS, None),
    )


def _total(samples: tuple[Sample, ...], name: str, cause: str | None) -> int:
    """The sum of the series of counter ``name``; with ``cause``, only the
    series of that ``cause`` label."""
    return int(
        sum(
            sample.value
            for sample in samples
            if sample.name == name and (cause is None or sample.label('cause') == cause)
        )
    )


@dataclass(frozen=True, slots=True)
class Answered:
    """The endpoint answered with ``status`` and ``body``."""

    status: int
    body: bytes


@dataclass(frozen=True, slots=True)
class NotAnswered:
    """No answer: the connection was refused, failed or timed out."""

    cause: str


type EndpointAnswer = Answered | NotAnswered


def get(listen: ListenOn, path: str) -> EndpointAnswer:
    """``GET http://host:port<path>``, bounded by ``READ_TIMEOUT``; the body
    is read up to ``MAX_EXPOSITION_BYTES``. A status other than 200 is an
    answer."""
    url = f'http://{listen.argument()}{path}'
    try:
        with urllib.request.urlopen(
            url, timeout=READ_TIMEOUT.total_seconds()
        ) as answer:
            return Answered(
                status=answer.status, body=answer.read(MAX_EXPOSITION_BYTES)
            )
    except urllib.error.HTTPError as refused:
        return Answered(status=refused.code, body=refused.read(MAX_EXPOSITION_BYTES))
    except (urllib.error.URLError, OSError) as failure:
        return NotAnswered(cause=f'{type(failure).__name__}: {failure}')


@dataclass(frozen=True, slots=True)
class EndpointCountsUnread:
    """The read at the end of the drain failed: no answer, a status other
    than 200, or an answer that is not the Prometheus text format."""

    cause: str


def read_endpoint_counts(listen: ListenOn) -> EndpointCounts | EndpointCountsUnread:
    """One read of ``/metrics`` and its telemetry columns."""
    match get(listen, '/metrics'):
        case NotAnswered(cause=cause):
            return EndpointCountsUnread(cause=f'/metrics not answered: {cause}')
        case Answered(status=200, body=body):
            match parse_exposition(body.decode('utf-8', 'replace')):
                case ExpositionMalformed(line_number=number, line=line):
                    return EndpointCountsUnread(
                        cause=f'/metrics line {number} is not a sample: {line!r}'
                    )
                case tuple() as samples:
                    return endpoint_counts(samples)
                case _ as unreachable:
                    assert_never(unreachable)
        case Answered(status=status):
            return EndpointCountsUnread(cause=f'/metrics answered {status}')
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class ReadTally:
    """The answers of the endpoint to the reads during one drain.

    ``metrics_not_answered`` and ``livez_not_answered`` include the reads
    before the worker bound its endpoint. ``livez_late`` counts the answers
    503 of ``/livez``: the claim loop of the worker did not turn within its
    allowance."""

    metrics_answered: int
    metrics_not_answered: int
    livez_live: int
    livez_late: int
    livez_not_answered: int


@dataclass(frozen=True, slots=True)
class ReaderDidNotStop:
    """The thread of the reader did not end within its bound after the stop:
    a read hangs past ``READ_TIMEOUT``."""

    listen: ListenOn


class EndpointReader:
    """Reads ``/metrics`` and ``/livez`` every ``interval`` on its own thread,
    at most ``max_rounds`` times, from ``start`` until ``stop``."""

    def __init__(self, listen: ListenOn, interval: timedelta, max_rounds: int) -> None:
        self._listen = listen
        self._interval = interval
        self._max_rounds = max_rounds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name='perf-endpoint-reader',
            daemon=True,
        )
        self._metrics_answered = 0
        self._metrics_not_answered = 0
        self._livez_live = 0
        self._livez_late = 0
        self._livez_not_answered = 0

    def start(self) -> None:
        """Start the thread. The first read is one interval after the start."""
        self._thread.start()

    def stop(self) -> ReadTally | ReaderDidNotStop:
        """Stop the reads and give the tally. Waits for the thread at most two
        read bounds and one interval."""
        self._stop.set()
        self._thread.join((2 * READ_TIMEOUT + self._interval).total_seconds())
        if self._thread.is_alive():
            return ReaderDidNotStop(listen=self._listen)
        return ReadTally(
            metrics_answered=self._metrics_answered,
            metrics_not_answered=self._metrics_not_answered,
            livez_live=self._livez_live,
            livez_late=self._livez_late,
            livez_not_answered=self._livez_not_answered,
        )

    def _run(self) -> None:
        for _ in range(self._max_rounds):
            if self._stop.wait(self._interval.total_seconds()):
                return
            match get(self._listen, '/metrics'):
                case Answered(status=200):
                    self._metrics_answered += 1
                case Answered() | NotAnswered():
                    self._metrics_not_answered += 1
                case _ as unreachable:
                    assert_never(unreachable)
            match get(self._listen, '/livez'):
                case Answered(status=200):
                    self._livez_live += 1
                case Answered(status=503):
                    self._livez_late += 1
                case Answered() | NotAnswered():
                    self._livez_not_answered += 1
                case _ as unreachable:
                    assert_never(unreachable)
