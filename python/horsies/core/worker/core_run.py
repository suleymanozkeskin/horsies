"""`horsies worker` on the core: the worker manifest, the core run on the main
thread, and the exit code.

The CLI discovers the app, imports its task modules, and then hands the
worker to the core. The core runs the claim loop, the children, the outcome
writes, the heartbeats and the reaper on its own threads. This module:

- builds the worker manifest of the app (`to_worker_manifest`,
  `python_child_launch`);
- sets the panic rule of a worker process before the first core call;
- calls `prepare_worker`, then `PreparedWorker.run` on the main thread, with
  `SIGTERM` and `SIGINT` handlers that ask the run to stop;
- maps the end of the run to the exit contract: 0 only on an operator stop,
  else 1.
"""

from __future__ import annotations

import logging
import signal
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import FrameType
from typing import assert_never

from horsies import _core
from horsies._catalog import worker_manifest as wire
from horsies.core.app import Horsies
from horsies.core.history.identity.keys import IDEMPOTENCY_WINDOW_DEFAULT
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.gc_policy import freeze_startup_objects
from horsies.core.worker.row_class import registered_task_classes
from horsies.core.worker_manifest import (
    ValueAboveManifestBound,
    current_interpreter_flags,
    log_level_of,
    python_child_launch,
    to_worker_manifest,
)

EXIT_STOPPED = 0
"""The run ended at an operator stop."""

EXIT_FAILED = 1
"""Any other end: a refused manifest or profile, a failed start, a failure
after the start."""


class StartedHost:
    """The host of a core worker: `on_started` freezes the objects of the
    start, as the Python worker does before its first claim."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def on_started(self) -> None:
        """Called once, on the callback thread, before the first claim.
        `freeze_startup_objects` does no I/O and cannot fail."""
        freeze_startup_objects()
        self._logger.info('Worker started')


@dataclass(frozen=True, slots=True)
class WorkerLaunch:
    """What the CLI resolved about the app before the core takes over."""

    app_locator: str
    imports: tuple[str, ...]
    sys_path_roots: tuple[str, ...]
    profile: str | None
    loglevel: str


def worker_manifest_json(
    app: Horsies,
    launch: WorkerLaunch,
) -> Result[str, ValueAboveManifestBound]:
    """The JSON text of the worker manifest of `app`. Reads `sys.executable`,
    the interpreter flags, `sys.path` and `sys.argv`; no other I/O.

    Errors: `ValueAboveManifestBound` for the first config value above its
    manifest bound.
    """
    child_launch = python_child_launch(
        executable=sys.executable,
        interpreter_flags=current_interpreter_flags(),
        app_locator=launch.app_locator,
        imports=launch.imports,
        sys_path_roots=launch.sys_path_roots,
        parent_sys_path=tuple(sys.path),
        parent_argv=tuple(sys.argv),
        log_level=log_level_of(launch.loglevel),
    )
    tasks = registered_task_classes(
        (name, task.task_options_json) for name, task in app.tasks.items()
    )
    match to_worker_manifest(
        app.config,
        profile=launch.profile,
        tasks=tasks,
        child_launch=child_launch,
        idempotency_reservation_window=_reservation_window(),
    ):
        case Ok(manifest):
            return Ok(wire.dump_worker_manifest(manifest))
        case Err(above):
            return Err(above)
        case _ as unreachable:
            assert_never(unreachable)


def _reservation_window() -> timedelta:
    """The reservation window of a keyed send from a worker child: the
    default of `PostgresBroker`, the one window the CLI broker uses."""
    return IDEMPOTENCY_WINDOW_DEFAULT


def _install_stop_handlers(
    worker: _core.PreparedWorker,
    logger: logging.Logger,
) -> None:
    """`SIGTERM` and `SIGINT` ask the run to stop. The handlers run on the
    main thread between the wait slices of the run."""

    def _stop(signum: int, _frame: FrameType | None) -> None:
        logger.info('Received signal %s, stopping worker...', signum)
        _ = worker.request_stop()

    for stop_signal in (signal.SIGTERM, signal.SIGINT):
        signal.signal(stop_signal, _stop)


def _exit_code_of(
    end: _core.WorkerStopped | _core.WorkerBootFailed | _core.WorkerFailed,
    logger: logging.Logger,
) -> int:
    match end:
        case _core.WorkerStopped():
            logger.info('Worker stopped')
            return EXIT_STOPPED
        case _core.WorkerBootFailed(cause=cause, message=message):
            logger.error('Worker start failed (%s): %s', cause, message)
            return EXIT_FAILED
        case _core.WorkerFailed(cause=cause, message=message):
            logger.error('Worker failed (%s): %s', cause, message)
            return EXIT_FAILED
        case _ as unreachable:
            assert_never(unreachable)


def run_core_worker(
    app: Horsies,
    launch: WorkerLaunch,
    logger: logging.Logger,
    report_refusals: Callable[[Sequence[str]], None],
) -> int:
    """Build the manifest, prepare the worker and run it on the main thread.
    Returns the exit code of `horsies worker`.

    `report_refusals` prints a startup report of the given lines (a value
    above its manifest bound, a refused profile).
    """
    match worker_manifest_json(app, launch):
        case Ok(manifest_json):
            pass
        case Err(above):
            report_refusals(
                (
                    f'{above.path} is {above.value}, above the manifest bound '
                    f'{above.maximum}',
                ),
            )
            return EXIT_FAILED
        case _ as unreachable:
            assert_never(unreachable)
    # A panic in a core background task aborts the worker process; its
    # supervisor restarts it. Set before the first core call.
    _core.set_panic_rule('ABORT_PROCESS')
    match _core.prepare_worker(manifest_json, host=StartedHost(logger)):
        case _core.PreparedWorker() as worker:
            pass
        case _core.WorkerProfileRefused(refusals=refusals):
            report_refusals(tuple(refusals))
            return EXIT_FAILED
        case _ as unreachable:
            assert_never(unreachable)
    _install_stop_handlers(worker, logger)
    logger.info('Starting worker...')
    try:
        end = worker.run()
    except KeyboardInterrupt:
        logger.info('Worker interrupted by user')
        return EXIT_STOPPED
    return _exit_code_of(end, logger)

