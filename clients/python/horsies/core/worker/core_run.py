"""`horsies worker` on the core: the worker manifest, the core run on the main
thread, and the exit code.

The CLI discovers the app, checks it, picks the profile and imports its task
modules, and then hands the worker to the core. The core runs the claim
loop, the children, the outcome writes, the heartbeats and the reaper on its
own threads. This module:

- builds the worker manifest of the app (`to_worker_manifest`,
  `python_child_launch`); AppConfig validation already refused every value
  above a manifest bound, and an app locator in `__main__` is a startup
  error;
- sets the panic rule of a worker process before the first core call;
- calls `prepare_worker` with a `CoreWorkerHost`, then `PreparedWorker.run`
  on the main thread, with `SIGTERM` and `SIGINT` handlers that ask the run
  to stop;
- maps the end of the run to the exit contract: 0 only on an operator stop,
  else 1.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import FrameType
from typing import TYPE_CHECKING, Final, assert_never

from horsies import _core
from horsies._catalog import worker_manifest as wire
from horsies.core.app import Horsies
from horsies.core.errors import ConfigurationError, ErrorCode, HorsiesError
from horsies.core.history.identity.keys import IDEMPOTENCY_WINDOW_DEFAULT
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.gc_policy import freeze_startup_objects
from horsies.core.worker.row_class import registered_task_classes
from horsies.core.worker_manifest import (
    CurrentDirectory,
    CurrentDirectoryRead,
    CurrentDirectoryUnreadable,
    MainModuleLocator,
    TaskFacts,
    child_argv,
    child_sys_path,
    current_interpreter_flags,
    log_level_of,
    python_child_launch,
    to_worker_manifest,
)
from horsies.core.workflows.child_spec_source import build_child_spec

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker

EXIT_STOPPED = 0
"""The run ended at an operator stop."""

EXIT_FAILED = 1
"""Any other end: a refused profile, a broker that does not build, a failed
start, a failure after the start."""

type ReportStartupErrors = Callable[[Sequence[HorsiesError]], None]
"""Prints the startup report of the given errors."""


class CoreWorkerHost:
    """The host of a core worker, which the core calls on its callback
    thread:

    - `on_started`: once, before the first claim; it freezes the objects of
      the start, as the Python worker does before its first claim.
    - `build_child_spec`: the child of each sub-workflow node that a
      workflow step of the worker starts, built with the app of `broker`.
    """

    def __init__(self, broker: PostgresBroker, logger: logging.Logger) -> None:
        self._broker = broker
        self._logger = logger

    def on_started(self) -> None:
        """`freeze_startup_objects` does no I/O and cannot fail."""
        freeze_startup_objects()
        self._logger.info('Worker started')

    def build_child_spec(
        self, request: _core.ChildSpecRequest,
    ) -> _core.ChildSpecBuilt | _core.ChildSpecRefused:
        """The child of the node of `request`. Does no I/O; it runs the
        child's `build_with`.

        Raises:
            SerializationError: a defect of a child node row, as
                `build_child_spec` names it.
        """
        return build_child_spec(self._broker, request)


@dataclass(frozen=True, slots=True)
class WorkerLaunch:
    """What the CLI resolved about the app before the core takes over."""

    app_locator: str
    imports: tuple[str, ...]
    sys_path_roots: tuple[str, ...]
    profile: str
    loglevel: str


def read_current_directory() -> CurrentDirectoryRead:
    """The current directory of this process (`os.getcwd`).

    `CurrentDirectoryUnreadable`: `os.getcwd` raised `OSError`; the
    directory was removed, or a part of its path cannot be read.
    """
    try:
        return CurrentDirectory(os.getcwd())
    except OSError as error:
        return CurrentDirectoryUnreadable(f'{type(error).__name__}: {error}')


def worker_manifest_json(
    app: Horsies,
    launch: WorkerLaunch,
    logger: logging.Logger,
) -> Result[str, MainModuleLocator]:
    """The JSON text of the worker manifest of `app`. Reads
    `sys.executable`, the interpreter flags, `sys.path`, `sys.argv` and the
    current directory, and logs a warning for each `sys.path` entry that no
    child gets (`child_sys_path`); no other I/O. The config of `app` passed
    AppConfig validation, so every value is within its manifest bound
    (`to_worker_manifest`).

    Err: `MainModuleLocator`, the app locator is in `__main__`
    (`python_child_launch`).
    """
    sys_path = child_sys_path(tuple(sys.path), read_current_directory())
    for left in sys_path.left_out:
        logger.warning(
            'Leaving sys.path entry %s out of worker children: %s',
            left.entry,
            left.reason,
        )
    launched = python_child_launch(
        executable=sys.executable,
        interpreter_flags=current_interpreter_flags(),
        app_locator=launch.app_locator,
        imports=launch.imports,
        sys_path_roots=launch.sys_path_roots,
        parent_sys_path=sys_path,
        parent_argv=child_argv(tuple(sys.argv)),
        log_level=log_level_of(launch.loglevel),
    )
    match launched:
        case Ok(child_launch):
            pass
        case Err(refused):
            return Err(refused)
        case _ as unreachable:
            assert_never(unreachable)
    classes = registered_task_classes(
        (name, task.task_options_json) for name, task in app.tasks.items()
    )
    tasks = {
        name: TaskFacts(execution=classes[name], error_codes=task.task_error_codes)
        for name, task in app.tasks.items()
    }
    manifest = to_worker_manifest(
        app.config,
        profile=launch.profile,
        tasks=tasks,
        child_launch=child_launch,
        idempotency_reservation_window=_reservation_window(),
    )
    return Ok(wire.dump_worker_manifest(manifest))


PROFILE_REFUSED_HELP: Final = (
    'fix the profile that the line names in AppConfig.worker_profiles, or start '
    'the worker with a defined profile: horsies worker <app> --profile <name>'
)
"""The help of each line of the core's profile refusal."""


def profile_refused_error(refusal: str) -> ConfigurationError:
    """The startup error of one line of the core's profile refusal."""
    error = ConfigurationError(
        message=refusal,
        code=ErrorCode.CONFIG_INVALID_WORKER_PROFILE,
        help_text=PROFILE_REFUSED_HELP,
    )
    error.location = None
    return error


MAIN_MODULE_LOCATOR_HELP: Final = (
    'start the worker with the module path or the file path of the app: '
    'horsies worker <module>:<app>, or horsies worker <path/to/file.py>:<app>'
)
"""The help of the startup error of an app locator in `__main__`."""


def main_module_locator_error(refused: MainModuleLocator) -> ConfigurationError:
    """The startup error of an app locator in `__main__`."""
    error = ConfigurationError(
        message=(
            f'app locator {refused.app_locator!r} cannot work: a child is a new '
            'program with its own __main__'
        ),
        code=ErrorCode.WORKER_INVALID_LOCATOR,
        help_text=MAIN_MODULE_LOCATOR_HELP,
    )
    error.location = None
    return error


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
        _ = signal.signal(stop_signal, _stop)


def exit_code_of(
    end: _core.WorkerStopped | _core.WorkerBootFailed | _core.WorkerFailed,
    logger: logging.Logger,
) -> int:
    """The exit code of a run end, after its log line."""
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


def _child_spec_broker(app: Horsies) -> Result[PostgresBroker, HorsiesError]:
    """The broker whose app builds the sub-workflow children. Built before
    the run, so a broker that does not build is a startup error, not a
    failure of a workflow step. Starts nothing and connects nowhere.

    Errors: the `HorsiesError` of `Horsies.get_broker` (`BROKER_INIT_FAILED`).
    """
    try:
        return Ok(app.get_broker())
    except HorsiesError as error:
        return Err(error)


def run_core_worker(
    app: Horsies,
    launch: WorkerLaunch,
    logger: logging.Logger,
    report: ReportStartupErrors,
) -> int:
    """Build the manifest, prepare the worker and run it on the main thread.
    Returns the exit code of `horsies worker`.

    `report` prints the startup report of an app locator in `__main__`, of
    a broker that does not build, or of a profile that the core refuses.
    """
    match worker_manifest_json(app, launch, logger):
        case Ok(manifest_json):
            pass
        case Err(refused):
            report((main_module_locator_error(refused),))
            return EXIT_FAILED
        case _ as unreachable:
            assert_never(unreachable)
    # A panic in a core background task aborts the worker process; its
    # supervisor restarts it. Set before the first core call.
    _core.set_panic_rule('ABORT_PROCESS')
    match _child_spec_broker(app):
        case Ok(broker):
            pass
        case Err(error):
            report((error,))
            return EXIT_FAILED
        case _ as unreachable:
            assert_never(unreachable)
    match _core.prepare_worker(manifest_json, host=CoreWorkerHost(broker, logger)):
        case _core.PreparedWorker() as worker:
            pass
        case _core.WorkerProfileRefused(refusals=refusals):
            report(tuple(profile_refused_error(line) for line in refusals))
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
    return exit_code_of(end, logger)
