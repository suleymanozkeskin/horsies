"""The reads of the worker endpoint in the overhead measurement: the
telemetry columns of one read of ``/metrics``, and the reads during a
drain."""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar, Final

import pytest

from crossclient.perf.metrics_scrape import (
    EndpointCounts,
    EndpointCountsUnread,
    EndpointReader,
    ExpositionMalformed,
    ReadTally,
    Sample,
    endpoint_counts,
    parse_exposition,
    read_endpoint_counts,
)
from crossclient.perf.telemetry_levels import ListenOn

pytestmark = [pytest.mark.unit]

ROUNDS_WAIT: Final = timedelta(seconds=1)
"""The wait for a reader of 20 ms rounds to make all of its 2 or 3 rounds:
each round is two local reads."""

EXPOSITION: Final = """\
# HELP horsies_telemetry_events_dropped_total Events that did not reach the handler child.
# TYPE horsies_telemetry_events_dropped_total counter
horsies_telemetry_events_dropped_total{cause="queue_full"} 3
horsies_telemetry_events_dropped_total{cause="child_down"} 40
# TYPE horsies_telemetry_handler_dropped_total counter
horsies_telemetry_handler_dropped_total{handler="app.a"} 2
horsies_telemetry_handler_dropped_total{handler="app.b"} 5
horsies_task_run_duration_seconds_bucket{task="t",le="0.005"} 10
horsies_task_run_duration_seconds_sum{task="t"} 0.25
horsies_task_run_duration_seconds_count{task="t"} 10
"""


class TestExposition:
    def test_comments_are_skipped_and_labels_read(self) -> None:
        samples = parse_exposition(EXPOSITION)
        assert isinstance(samples, tuple)
        assert len(samples) == 7
        assert samples[0] == Sample(
            name='horsies_telemetry_events_dropped_total',
            labels=(('cause', 'queue_full'),),
            value=3.0,
        )

    def test_a_line_that_is_not_a_sample_is_malformed(self) -> None:
        assert parse_exposition('# a comment\nnot a sample line here\n') == (
            ExpositionMalformed(line_number=2, line='not a sample line here')
        )

    def test_the_counts_sum_each_counter_and_count_the_series(self) -> None:
        samples = parse_exposition(EXPOSITION)
        assert isinstance(samples, tuple)
        assert endpoint_counts(samples) == EndpointCounts(
            series=7,
            events_dropped_queue_full=3,
            events_dropped_child_down=40,
            series_dropped=0,
            handler_dropped=7,
            handler_errors=0,
        )


class _Endpoint(BaseHTTPRequestHandler):
    """``/metrics`` answers the class attribute ``metrics_status`` with
    ``EXPOSITION``; ``/livez`` answers ``livez_status``."""

    metrics_status: ClassVar[int] = 200
    livez_status: ClassVar[int] = 200

    def do_GET(self) -> None:
        match self.path:
            case '/metrics':
                self._answer(type(self).metrics_status, EXPOSITION.encode())
            case '/livez':
                self._answer(type(self).livez_status, b'')
            case _:
                self._answer(404, b'')

    def _answer(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        _ = self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - the name of the base method
        _ = (format, args)


@pytest.fixture
def endpoint() -> Iterator[ListenOn]:
    _Endpoint.metrics_status = 200
    _Endpoint.livez_status = 200
    server = ThreadingHTTPServer(('127.0.0.1', 0), _Endpoint)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield ListenOn(host='127.0.0.1', port=server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def _closed_port() -> ListenOn:
    """An address on which nothing listens: a port that was bound and
    released."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = int(probe.getsockname()[1])
    return ListenOn(host='127.0.0.1', port=port)


class TestReadAtTheEndOfADrain:
    def test_an_answer_gives_the_counts(self, endpoint: ListenOn) -> None:
        match read_endpoint_counts(endpoint):
            case EndpointCounts(series=series, events_dropped_child_down=child_down):
                assert (series, child_down) == (7, 40)
            case other:
                pytest.fail(f'expected EndpointCounts, got {other!r}')

    def test_a_status_other_than_200_is_unread(self, endpoint: ListenOn) -> None:
        _Endpoint.metrics_status = 500
        assert read_endpoint_counts(endpoint) == EndpointCountsUnread(
            cause='/metrics answered 500'
        )

    def test_no_answer_is_unread(self) -> None:
        match read_endpoint_counts(_closed_port()):
            case EndpointCountsUnread(cause=cause):
                assert cause.startswith('/metrics not answered')
            case other:
                pytest.fail(f'expected EndpointCountsUnread, got {other!r}')


class TestReadsDuringADrain:
    def test_the_tally_counts_each_answer(self, endpoint: ListenOn) -> None:
        _Endpoint.livez_status = 503
        reader = EndpointReader(endpoint, timedelta(milliseconds=20), max_rounds=3)
        reader.start()
        _ = threading.Event().wait(ROUNDS_WAIT.total_seconds())
        assert reader.stop() == ReadTally(
            metrics_answered=3,
            metrics_not_answered=0,
            livez_live=0,
            livez_late=3,
            livez_not_answered=0,
        )

    def test_reads_of_an_endpoint_that_is_not_bound_are_not_answered(self) -> None:
        reader = EndpointReader(
            _closed_port(), timedelta(milliseconds=20), max_rounds=2
        )
        reader.start()
        _ = threading.Event().wait(ROUNDS_WAIT.total_seconds())
        assert reader.stop() == ReadTally(
            metrics_answered=0,
            metrics_not_answered=2,
            livez_live=0,
            livez_late=0,
            livez_not_answered=2,
        )
