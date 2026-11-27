"""The scheduler service of a Python app, on the core's scheduler.

``Scheduler`` keeps the parts that need Python: the import of the task
modules, the check and encode of each schedule's kwargs against the task's
signature (``prepare_schedules``), and the queue, priority and retention class
of each schedule (``to_scheduler_manifest``). It gives them to
``horsies._core.CoreScheduler`` as one scheduler manifest. The core runs the
start (the client family check and the state rows), each tick (the due read,
the existence heal and one fire per due schedule), the slot calculator and the
config digest, with the database time as the only clock, and logs each
outcome.
"""

from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass
from typing import NoReturn, Optional

from horsies._catalog.scheduler_manifest import dump_scheduler_manifest
from horsies._core import (
    CoreScheduler,
    SchedulerStarted,
    SchedulerStartFailed,
    SchedulerStartRefused,
    SchedulerStopped,
)
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.payload_guard import warn_if_payload_large
from horsies.core.errors import ConfigurationError, HorsiesErrorCode
from horsies.core.logging import get_logger
from horsies.core.models.schedule import ScheduleConfig
from horsies.core.scheduler.manifest import to_scheduler_manifest
from horsies.core.scheduler.validation import PreparedSchedules, prepare_schedules
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.utils.imports import import_file_path

logger = get_logger('scheduler')


@dataclass(frozen=True, slots=True)
class _NotBuilt:
    """No start and no run built the core scheduler yet."""


@dataclass(frozen=True, slots=True)
class _Built:
    """The core scheduler of this service, on its broker's core handle."""

    core: CoreScheduler


def _raise_start_not_completed(
    outcome: SchedulerStartRefused | SchedulerStartFailed,
) -> NoReturn:
    """Raise the error of a start that did not complete.

    Raises:
        ConfigurationError: the core refused a schedule before any state
            write (``CONFIG_INVALID_SCHEDULE``; the refusal in ``notes``).
        RuntimeError: the start failed before the state start: the connect,
            the schema install or the client family check.
    """
    match outcome:
        case SchedulerStartRefused(schedule=schedule, reason=reason):
            raise ConfigurationError(
                message=f"schedule '{schedule}' cannot start",
                code=HorsiesErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=[reason],
                help_text=(
                    "fix the schedule's pattern, timezone or kwargs, or the task's "
                    'options, as the note says'
                ),
            )
        case SchedulerStartFailed(message=message):
            raise RuntimeError(f'Scheduler start failed: {message}')


class Scheduler:
    """
    Scheduler service: the schedules of the app's ``ScheduleConfig``, fired by
    the core.

    Runs as a separate process/dyno from workers (``horsies scheduler``).
    """

    def __init__(self, app: Horsies):
        self.app = app
        self.broker: Optional[PostgresBroker] = None
        self._core: _NotBuilt | _Built = _NotBuilt()
        # A stop request before the core scheduler exists; the core scheduler
        # gets it when it is built.
        self._stop_requested = False

        # Validate that schedule config exists
        if not app.config.schedule:
            raise ConfigurationError(
                message='app config must have schedule configuration',
                code=HorsiesErrorCode.CONFIG_INVALID_SCHEDULE,
                notes=['schedule config is None or not provided in AppConfig'],
                help_text='add schedule=ScheduleConfig(...) to your AppConfig',
            )

        self.schedule_config: ScheduleConfig = app.config.schedule

        logger.info(
            f'Scheduler initialized with {len(self.schedule_config.schedules)} schedules, '
            f'check_interval={self.schedule_config.check_interval_seconds}s'
        )

    async def start(self) -> None:
        """Build the core scheduler once, then start it: connect, install the
        schema once per broker handle, check the client family and store the
        state rows of new and changed schedules. A start that completed
        returns at once.

        Raises:
            HorsiesError: broker construction failed (``get_broker``).
            RegistryError | ConfigurationError: schedule validation failed
                (``prepare_schedules``, the first error), or the core refused
                a schedule (a zone it does not know, a send command it
                refuses, or a pattern with no slot after the database time).
            RuntimeError: the connect, the schema install or the client
                family check failed.
            All propagate through run_forever to the CLI boundary: fail-fast
            at boot, exit 1; nothing has been enqueued yet.
        """
        core = self._core_scheduler()
        match await core.start_async():
            case SchedulerStarted():
                logger.info('Scheduler started successfully')
            case SchedulerStartRefused() | SchedulerStartFailed() as not_started:
                _raise_start_not_completed(not_started)

    async def stop(self) -> None:
        """Clean shutdown of scheduler: the stop request, then the broker close.

        Total: broker close returns a Result that is logged on Err, so a
        stop inside run_forever's finally cannot mask the original error.
        """
        self.request_stop()

        if self.broker:
            close_result = await self.broker.close_async()
            if is_err(close_result):
                logger.error(f'Broker close failed: {close_result.err_value.message}')
            else:
                logger.info('Broker closed')

        logger.info('Scheduler stopped')

    def request_stop(self) -> None:
        """Request scheduler to stop gracefully: the run ends at its next tick
        boundary; a fire in flight runs to its end."""
        self._stop_requested = True
        match self._core:
            case _Built(core=core):
                core.request_stop()
            case _NotBuilt():
                pass

    async def run_forever(self) -> None:
        """The start when none completed, then one tick at once and one per
        ``check_interval_seconds`` until a stop request. With the master
        switch off or no schedule, the wait for the stop request.

        The core logs each failed tick and runs the next one. stop() always
        runs via finally once the loop is entered.

        Raises:
            The errors of ``start()``: a start failure propagates to the CLI
            boundary (exit 1).
        """
        logger.info('Starting scheduler loop')

        try:
            core = self._core_scheduler()
            match await core.run_async():
                case SchedulerStopped():
                    pass
                case SchedulerStartRefused() | SchedulerStartFailed() as not_started:
                    _raise_start_not_completed(not_started)
        finally:
            await self.stop()

    def _core_scheduler(self) -> CoreScheduler:
        """The core scheduler of this service, built once: the broker, the
        import of the task modules, the prepared wire text of each enabled
        schedule, then the manifest. Does no database I/O.

        Raises:
            HorsiesError: broker construction failed (``get_broker``).
            RegistryError | ConfigurationError: the first schedule that
                ``prepare_schedules`` refuses.
        """
        match self._core:
            case _Built(core=core):
                return core
            case _NotBuilt():
                pass
        broker = self.app.get_broker()
        self.broker = broker
        logger.info('Broker initialized')

        # Import task modules so task registry is populated for scheduled names
        self._preload_task_modules()

        prepared = self._prepare_schedules()
        self._warn_large_kwargs(prepared)
        manifest = to_scheduler_manifest(self.app, self.schedule_config, prepared)
        core = CoreScheduler(
            broker._core_handle,  # pyright: ignore[reportPrivateUsage]
            dump_scheduler_manifest(manifest),
        )
        if self._stop_requested:
            core.request_stop()
        self._core = _Built(core=core)
        return core

    def _prepare_schedules(self) -> PreparedSchedules:
        """Fail fast on the first invalid schedule and return the prepared text.

        Shares :func:`prepare_schedules` with ``Horsies.check()``; ``errors[0]``
        is the first bad schedule in iteration order, preserving the original
        raise-on-first behaviour. ``check`` collects all of them.
        """
        match prepare_schedules(self.app, self.schedule_config):
            case Ok(prepared_schedules):
                return prepared_schedules
            case Err(errors):
                raise errors[0]

    def _warn_large_kwargs(self, prepared: PreparedSchedules) -> None:
        """Log the payload size warning for each schedule whose kwargs text is
        over ``payload.warn_bytes``, once per task and process."""
        for schedule in self.schedule_config.schedules:
            prepared_schedule = prepared.get(schedule.name)
            if prepared_schedule is None or prepared_schedule.input_json is None:
                continue
            warn_if_payload_large(
                self.app.config.payload,
                task_name=schedule.task_name,
                kind='kwargs',
                payload_text=prepared_schedule.input_json,
            )

    def _preload_task_modules(self) -> None:
        """Import discovered task modules so @app.task registrations run."""
        modules = self.app.get_discovered_task_modules()

        if not modules:
            logger.warning(
                'No task modules discovered; scheduler may not resolve tasks'
            )
            return

        # Save/restore rather than force-False: a caller that already
        # suppressed sends (e.g. app.check()) must keep its setting.
        prev_suppress = self.app._suppress_sends  # pyright: ignore[reportPrivateUsage]
        self.app.suppress_sends(True)
        try:
            for module in modules:
                if module.endswith('.py') or os.path.sep in module:
                    try:
                        import_file_path(os.path.abspath(module))
                    except Exception as e:
                        logger.warning(f"Failed to import task module '{module}': {e}")
                else:
                    # Ensure repo root is on sys.path for module imports
                    cwd = os.getcwd()
                    if cwd not in sys.path:
                        sys.path.insert(0, cwd)
                    try:
                        importlib.import_module(module)
                    except Exception as e:
                        logger.warning(f"Failed to import task module '{module}': {e}")
        finally:
            self.app.suppress_sends(prev_suppress)
