"""`horsies metrics`: the fleet metrics of the app's database at `GET /metrics`.

The core serves the endpoint (`prepare_fleet_metrics`,
`FleetMetricsServer.run`). The fleet state is read at once and then at most
once per collect interval; a scrape renders the last read and never reads,
so scrapes add no load on the database. A failed read is
`horsies_exporter_collect_failed{query} 1` in the text, never an end of the
command. The `queue` and `schedule` label values are the queues and the
schedules of the app config; every other name is `(unknown)`. A config name
that cannot be a label value of its own (empty, `(unknown)`, or past the
bound of names) counts in the `(unknown)` series too, and the run logs one
WARNING line that names it.
"""

from __future__ import annotations

import argparse
import logging
import signal
from dataclasses import dataclass
from types import FrameType
from typing import Final, assert_never

from horsies._core import (
    FleetMetricsFailed,
    FleetMetricsServer,
    FleetMetricsStopped,
    ListenAddress,
    ListenAddressRefused,
    parse_listen_address,
    prepare_fleet_metrics,
)
from horsies.core.app import Horsies
from horsies.core.errors import HorsiesError

EXIT_STOPPED: Final = 0
"""The exit code of `horsies metrics` after a stop request."""

EXIT_FAILED: Final = 1
"""The exit code of `horsies metrics` after a failure."""


@dataclass(frozen=True, slots=True)
class FleetLabelNames:
    """The queue and schedule names that the fleet metrics take as label values."""

    queues: tuple[str, ...]
    schedules: tuple[str, ...]


def fleet_label_names(app: Horsies) -> FleetLabelNames:
    """The queues of the app (`default` in `QueueMode.DEFAULT`) and the names of
    the schedules of its schedule config. Pure."""
    schedule = app.config.schedule
    schedules: tuple[str, ...] = (
        tuple(entry.name for entry in schedule.schedules)
        if schedule is not None
        else ()
    )
    return FleetLabelNames(
        queues=tuple(app.get_valid_queue_names()), schedules=schedules
    )


def listen_address(text: str) -> ListenAddress:
    """The `--metrics-listen` argument: `HOST:PORT` or `[IPV6]:PORT`, the port 1 to
    65535. Pure.

    Raises `argparse.ArgumentTypeError` with the refusal, which argparse
    reports as a usage error (exit 2).
    """
    match parse_listen_address(text):
        case ListenAddress() as address:
            return address
        case ListenAddressRefused(message=message):
            raise argparse.ArgumentTypeError(message)
        case _ as unreachable:
            assert_never(unreachable)


def _install_stop_handlers(server: FleetMetricsServer, logger: logging.Logger) -> None:
    """`SIGTERM` and `SIGINT` ask the run to stop. The handlers run on the main
    thread between the wait slices of the run."""

    def _stop(signum: int, _frame: FrameType | None) -> None:
        logger.info('Received signal %s, stopping the fleet metrics server...', signum)
        _ = server.request_stop()

    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        _ = signal.signal(stop_signal, _stop)


def exit_code_of(
    end: FleetMetricsStopped | FleetMetricsFailed,
    logger: logging.Logger,
) -> int:
    """The exit code of a run end, after its log line."""
    match end:
        case FleetMetricsStopped():
            logger.info('Fleet metrics server stopped')
            return EXIT_STOPPED
        case FleetMetricsFailed(message=message):
            logger.error('Fleet metrics server failed: %s', message)
            return EXIT_FAILED
        case _ as unreachable:
            assert_never(unreachable)


def run_fleet_metrics(
    app: Horsies,
    listen: ListenAddress,
    logger: logging.Logger,
) -> int:
    """Serve the fleet metrics of the database of `app` on `listen` until
    `SIGTERM` or `SIGINT`, on the main thread. Returns the exit code of
    `horsies metrics`: 0 at a stop, 1 when the broker does not build or the
    address does not bind.
    """
    try:
        broker = app.get_broker()
    except HorsiesError as error:
        logger.error(str(error))
        return EXIT_FAILED
    names = fleet_label_names(app)
    server = prepare_fleet_metrics(
        broker._core_handle,  # pyright: ignore[reportPrivateUsage]
        listen=listen,
        queue_names=list(names.queues),
        schedule_names=list(names.schedules),
    )
    _install_stop_handlers(server, logger)
    try:
        end = server.run()
    except KeyboardInterrupt:
        logger.info('Fleet metrics server interrupted by user')
        return EXIT_STOPPED
    return exit_code_of(end, logger)
