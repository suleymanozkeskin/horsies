"""Runner heartbeats for RUNNING tasks, written by the worker process.

The reaper fails a RUNNING row that has no runner heartbeat for
``running_stale_threshold_ms``. The worker process writes these heartbeats for
the tasks that its children hold. A task that holds the GIL in its child
cannot stop them, and neither can a blocked event loop of the worker.

Two daemon threads, apart from the event loop:

- The beat thread reads ``LiveDispatches`` once per
  ``runner_heartbeat_interval_ms``. It keeps the dispatches whose child
  process still runs, and writes one statement that beats all of them. The
  statement beats a row only while it is RUNNING, owned by this worker, at
  the claim generation of the dispatch, and without the finalizing marker.
- The watchdog acts on ``StaleRunningRecovery``, decided once at worker
  start from the recovery config.

  - ``StaleRunningRecoveryOn``: the reaper fails a RUNNING row that has no
    runner heartbeat for the threshold. The watchdog fails closed when the
    beat thread is dead, or when no beat landed for half of the threshold. It
    kills every child that holds a task, asks the event loop to stop the
    worker with ``RunnerHeartbeatLostError``, and exits the process with
    ``RUNNER_HEARTBEAT_LOST_EXIT_CODE`` when the worker has not stopped within
    ``FAIL_CLOSED_EXIT_GRACE_S``. So the reaper does not fail a task that
    still runs because this worker could not write its heartbeats.
  - ``StaleRunningRecoveryOff``: no reaper of this config fails a RUNNING
    row, so a missing beat puts no running task at risk. The watchdog logs
    late beats and a dead beat thread, and the worker keeps running.

  Failed writes and late beats are logged at most once per
  ``BEAT_FAILURE_LOG_INTERVAL_S``, and once at the start of each failure run.

The first heartbeat of an attempt is not written here: the child writes it in
the transaction that sets the row RUNNING.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, LiteralString, Protocol, TypeAlias, assert_never

import psutil
import psycopg
from psycopg.conninfo import make_conninfo

from horsies.core.codec.json_io import dumps_json
from horsies.core.logging import get_logger
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.types.result import Err, Ok
from horsies.core.worker.live_dispatches import LiveDispatch, LiveDispatches

logger = get_logger('worker')

# The fail-closed window is running_stale_threshold_ms divided by this. The
# other half is the time left to kill the children before the reaper acts.
FAIL_CLOSED_WINDOW_DIVISOR: Final = 2
# libpq connect_timeout of the heartbeat connection, in whole seconds.
BEAT_CONNECT_TIMEOUT_S: Final = 10
# Wait before the first retry after a failed write. It doubles per failure.
BEAT_RETRY_FIRST_DELAY_S: Final = 0.25
BEAT_RETRY_DOUBLINGS_MAX: Final = 5
# No retry waits longer than this, or longer than one beat interval.
BEAT_RETRY_MAX_DELAY_S: Final = 5.0
# How often the watchdog checks the beat thread and the time since the last
# beat. It is below half of the smallest fail-closed window (1 s).
WATCH_INTERVAL_S: Final = 0.25
# Failed writes and late beats are logged at most once per this time.
BEAT_FAILURE_LOG_INTERVAL_S: Final = 60.0
# With stale running recovery off, a beat is late after this many intervals.
LATE_BEAT_INTERVALS: Final = 2
# Time that the worker gets to stop by itself after it failed closed.
FAIL_CLOSED_EXIT_GRACE_S: Final = 120.0
# Exit code of the worker process when it failed closed and did not stop
# within the grace. No other horsies process uses it.
RUNNER_HEARTBEAT_LOST_EXIT_CODE: Final = 77
# Bound of each thread join in ``RunnerHeartbeat.stop``.
THREAD_STOP_JOIN_S: Final = 5.0
BEAT_THREAD_NAME: Final = 'horsies-runner-heartbeat'
WATCH_THREAD_NAME: Final = 'horsies-runner-heartbeat-watch'

# One statement per interval. ``dispatches`` is a JSON array of
# {"task_id", "claimed_at"}. Each row is fenced like every other write of a
# dispatch: this worker, and the claim generation of the dispatch (a NULL
# claimed_at disables the generation half, as in OwnedClaim). A row with the
# finalizing marker gets no beat: its child has ended the attempt, and the
# reaper reads the marker for that row. Each parameter has an explicit type:
# worker_id appears twice, and Postgres refuses two deduced types for it.
INSERT_RUNNER_HEARTBEATS_SQL: Final[LiteralString] = """
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT t.id, CAST(%(worker_id)s AS VARCHAR), 'runner', NOW(),
           CAST(%(hostname)s AS VARCHAR), CAST(%(pid)s AS INTEGER)
    FROM jsonb_to_recordset(CAST(%(dispatches)s AS jsonb))
         AS d(task_id uuid, claimed_at timestamptz)
    JOIN horsies_tasks t ON t.id = d.task_id
    WHERE t.status = 'RUNNING'
      AND t.claimed_by_worker_id = CAST(%(worker_id)s AS VARCHAR)
      AND (d.claimed_at IS NULL OR t.claimed_at = d.claimed_at)
      AND t.finalizing_at IS NULL
"""


class RunnerHeartbeatLostError(RuntimeError):
    """The worker failed closed because its runner heartbeats stopped.

    Process-fatal: the watchdog already killed every child that held a task,
    so no task runs without a heartbeat. ``run_forever`` raises it after the
    stop, and the worker exits non-zero for a supervisor restart.
    """


@dataclass(frozen=True, slots=True)
class BeatTiming:
    """How often the beat thread writes."""

    interval_s: float


def beat_timing(recovery: RecoveryConfig) -> BeatTiming:
    """Read the beat interval from ``recovery``."""
    return BeatTiming(interval_s=recovery.runner_heartbeat_interval_ms / 1000)


@dataclass(frozen=True, slots=True)
class StaleRunningRecoveryOn:
    """The reaper fails a RUNNING row with no runner heartbeat for ``threshold_ms``.

    The worker fails closed when no beat lands for half of it. RecoveryConfig
    keeps the threshold at two beat intervals or more, so the half holds at
    least one interval.
    """

    threshold_ms: int

    def fail_closed_window_s(self) -> float:
        """Half of the threshold, in seconds."""
        return self.threshold_ms / 1000 / FAIL_CLOSED_WINDOW_DIVISOR


@dataclass(frozen=True, slots=True)
class StaleRunningRecoveryOff:
    """No reaper of this config fails a RUNNING row.

    ``auto_fail_stale_running`` is off, or the worker has no recovery config
    and starts no reaper. A beat that cannot land puts no running task at
    risk, so the worker logs it and keeps running.
    """


StaleRunningRecovery: TypeAlias = StaleRunningRecoveryOn | StaleRunningRecoveryOff


def stale_running_recovery(recovery: RecoveryConfig | None) -> StaleRunningRecovery:
    """Decide, from the worker's recovery config, whether beats must fail closed."""
    match recovery:
        case None:
            return StaleRunningRecoveryOff()
        case RecoveryConfig(auto_fail_stale_running=True):
            return StaleRunningRecoveryOn(recovery.running_stale_threshold_ms)
        case RecoveryConfig():
            return StaleRunningRecoveryOff()


@dataclass(frozen=True, slots=True)
class BeatsWritten:
    """The statement committed. ``rows`` rows passed the fence and got a beat."""

    rows: int


@dataclass(frozen=True, slots=True)
class BeatWriteFailed:
    """The statement did not commit. The connection was closed after it."""

    detail: str


BeatWriteOutcome: TypeAlias = BeatsWritten | BeatWriteFailed


class BeatWriter(Protocol):
    """Writes one heartbeat statement. Used by the beat thread only."""

    def write(self, dispatches: Sequence[LiveDispatch]) -> BeatWriteOutcome:
        """Beat every row of ``dispatches`` that passes the fence."""
        ...

    def close(self) -> None:
        """Release the connection. Does not raise."""
        ...


@dataclass(frozen=True, slots=True)
class _PsycopgDefaultPrepare:
    """The worker sets no ``prepare_threshold``: psycopg's default applies."""


@dataclass(frozen=True, slots=True)
class _PrepareThreshold:
    """``prepare_threshold`` from the worker. None never prepares (PgBouncer)."""

    value: int | None


_PrepareSetting: TypeAlias = _PsycopgDefaultPrepare | _PrepareThreshold


@dataclass(frozen=True, slots=True)
class _ConnectSettings:
    """The conninfo holds the password, so it stays out of the repr."""

    conninfo: str = field(repr=False)
    prepare: _PrepareSetting


def _connect_settings(
    database_url: str,
    connect_kwargs: Mapping[str, object],
) -> _ConnectSettings:
    """Build the heartbeat connection settings from the worker's connect kwargs.

    The kwargs come from ``PostgresConfig.pooled_connect_args``: libpq TCP
    settings, and ``prepare_threshold``. A value of another type is a defect
    in the caller: ``AssertionError``. A URL that libpq cannot parse raises
    ``psycopg.ProgrammingError`` at worker start.
    """
    libpq_params: dict[str, str | int | None] = {
        'connect_timeout': BEAT_CONNECT_TIMEOUT_S,
    }
    prepare: _PrepareSetting = _PsycopgDefaultPrepare()
    for name, value in connect_kwargs.items():
        match name, value:
            case 'prepare_threshold', int() | None:
                prepare = _PrepareThreshold(value)
            case 'prepare_threshold', _:
                raise AssertionError(
                    f'prepare_threshold has type {type(value).__name__}',
                )
            case _, str() | int():
                libpq_params[name] = value
            case _, _:
                raise AssertionError(
                    f'connect parameter {name!r} has type {type(value).__name__}',
                )
    return _ConnectSettings(
        conninfo=make_conninfo(database_url, **libpq_params),
        prepare=prepare,
    )


class PostgresBeatWriter:
    """Writes runner heartbeats over one sync psycopg connection.

    The connection opens at the first write, and again after any failure.
    It uses autocommit, so each statement is its own transaction, and it
    works behind PgBouncer in transaction mode.
    """

    def __init__(
        self,
        *,
        database_url: str,
        connect_kwargs: Mapping[str, object],
        worker_id: str,
    ) -> None:
        self._settings = _connect_settings(database_url, connect_kwargs)
        self._worker_id = worker_id
        self._hostname = socket.gethostname()
        self._pid = os.getpid()
        self._connection: psycopg.Connection[Any] | None = None

    def write(self, dispatches: Sequence[LiveDispatch]) -> BeatWriteOutcome:
        """Write one statement. A database error closes the connection."""
        params = {
            'worker_id': self._worker_id,
            'hostname': self._hostname,
            'pid': self._pid,
            'dispatches': _dispatches_json(dispatches),
        }
        try:
            connection = self._open_connection()
            with connection.cursor() as cursor:
                cursor.execute(INSERT_RUNNER_HEARTBEATS_SQL, params)
                return BeatsWritten(rows=cursor.rowcount)
        except psycopg.Error as exc:
            self.close()
            return BeatWriteFailed(detail=f'{type(exc).__name__}: {exc}')

    def _open_connection(self) -> psycopg.Connection[Any]:
        connection = self._connection
        if connection is not None and not connection.closed:
            return connection
        match self._settings.prepare:
            case _PsycopgDefaultPrepare():
                connection = psycopg.connect(self._settings.conninfo, autocommit=True)
            case _PrepareThreshold(value=value):
                connection = psycopg.connect(
                    self._settings.conninfo,
                    autocommit=True,
                    prepare_threshold=value,
                )
            case _ as unreachable:
                assert_never(unreachable)
        self._connection = connection
        return connection

    def close(self) -> None:
        """Close the connection. A close error is logged, not raised."""
        connection, self._connection = self._connection, None
        if connection is None:
            return
        try:
            connection.close()
        except psycopg.Error as exc:
            logger.debug('Closing the runner heartbeat connection failed: %s', exc)


def _dispatches_json(dispatches: Sequence[LiveDispatch]) -> str:
    """The ``dispatches`` parameter: task ids and claim generations as JSON."""
    rows = [
        {
            'task_id': dispatch.task_id,
            'claimed_at': (
                None if dispatch.claimed_at is None else dispatch.claimed_at.isoformat()
            ),
        }
        for dispatch in dispatches
    ]
    match dumps_json(rows):
        case Ok(text):
            return text
        case Err(exc):
            raise AssertionError(f'dispatch rows are not JSON: {exc}')


@dataclass(frozen=True, slots=True)
class ProcessRuns:
    """The child process exists and is not a zombie."""


@dataclass(frozen=True, slots=True)
class ProcessEnded:
    """The child process is gone, or it is a zombie that waits to be reaped."""


@dataclass(frozen=True, slots=True)
class ProcessUnreadable:
    """The state of the child process could not be read."""

    detail: str


ProcessState: TypeAlias = ProcessRuns | ProcessEnded | ProcessUnreadable


def child_process_state(pid: int) -> ProcessState:
    """Read whether child ``pid`` still runs. Sends no signal and reaps nothing."""
    try:
        status = psutil.Process(pid).status()
    except psutil.NoSuchProcess:
        return ProcessEnded()
    except psutil.Error as exc:
        return ProcessUnreadable(f'{type(exc).__name__}: {exc}')
    match status:
        case psutil.STATUS_ZOMBIE | psutil.STATUS_DEAD:
            return ProcessEnded()
        case _:
            return ProcessRuns()


@dataclass(frozen=True, slots=True)
class SessionKilled:
    """SIGKILL went to the child's session."""


@dataclass(frozen=True, slots=True)
class SessionGone:
    """The child's session no longer exists."""


@dataclass(frozen=True, slots=True)
class KillRefused:
    """The OS refused the kill."""

    detail: str


SessionKill: TypeAlias = SessionKilled | SessionGone | KillRefused


def kill_child_session(pid: int) -> SessionKill:
    """SIGKILL the session of child ``pid``. A booted child leads its own session."""
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        return SessionGone()
    except PermissionError as exc:
        return KillRefused(str(exc))
    return SessionKilled()


def retry_delay_s(consecutive_failures: int, interval_s: float) -> float:
    """Wait before the next write after ``consecutive_failures`` failures.

    Starts at ``BEAT_RETRY_FIRST_DELAY_S`` and doubles, up to
    ``BEAT_RETRY_MAX_DELAY_S`` and never above one interval.
    """
    doublings = min(max(consecutive_failures - 1, 0), BEAT_RETRY_DOUBLINGS_MAX)
    return min(
        BEAT_RETRY_FIRST_DELAY_S * 2**doublings,
        BEAT_RETRY_MAX_DELAY_S,
        interval_s,
    )


@dataclass(frozen=True, slots=True)
class NoBeatFailure:
    """No write failed since the last beat that landed."""


@dataclass(frozen=True, slots=True)
class BeatThreadDied:
    """The beat thread ended while the worker ran."""

    detail: str


@dataclass(frozen=True, slots=True)
class BeatsOverdue:
    """No beat landed for longer than ``window_s``.

    With stale running recovery on, ``window_s`` is the fail-closed window;
    with it off, it is ``LATE_BEAT_INTERVALS`` beat intervals.
    """

    uncovered_s: float
    window_s: float
    last_failure: BeatWriteFailed | NoBeatFailure


@dataclass(frozen=True, slots=True)
class WatchFailed:
    """The watchdog itself hit a defect."""

    detail: str


FailClosedCause: TypeAlias = BeatThreadDied | BeatsOverdue | WatchFailed


def describe_last_failure(last_failure: BeatWriteFailed | NoBeatFailure) -> str:
    """The last failed write, or that the last write has not returned."""
    match last_failure:
        case BeatWriteFailed(detail=detail):
            return f'last error: {detail}'
        case NoBeatFailure():
            return 'the last write has not returned'
        case _ as unreachable:
            assert_never(unreachable)


def describe_fail_closed(cause: FailClosedCause) -> str:
    """Text for the critical log line and for ``RunnerHeartbeatLostError``."""
    match cause:
        case BeatThreadDied(detail=detail):
            return f'the runner heartbeat thread died: {detail}'
        case BeatsOverdue(
            uncovered_s=uncovered_s,
            window_s=window_s,
            last_failure=last_failure,
        ):
            return (
                f'no runner heartbeat landed for {uncovered_s:.1f}s, past '
                f'{window_s:.1f}s (half of running_stale_threshold_ms); '
                f'{describe_last_failure(last_failure)}'
            )
        case WatchFailed(detail=detail):
            return f'the runner heartbeat watchdog failed: {detail}'
        case _ as unreachable:
            assert_never(unreachable)


@dataclass(frozen=True, slots=True)
class ExitPolicy:
    """How the watchdog ends the process when the worker does not stop."""

    grace_s: float = FAIL_CLOSED_EXIT_GRACE_S
    exit_process: Callable[[int], object] = field(default=os._exit)


@dataclass(frozen=True, slots=True)
class _NeverLogged:
    pass


@dataclass(frozen=True, slots=True)
class _LoggedAt:
    at_s: float


class _LogGate:
    """Allows one log line per ``interval_s``. Used by one thread only."""

    def __init__(self, interval_s: float) -> None:
        self._interval_s = interval_s
        self._last: _NeverLogged | _LoggedAt = _NeverLogged()

    def due(self, now_s: float) -> bool:
        """Whether a line may be logged now. A True answer counts as logged."""
        match self._last:
            case _NeverLogged():
                pass
            case _LoggedAt(at_s=at_s) if now_s - at_s < self._interval_s:
                return False
            case _LoggedAt():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        self._last = _LoggedAt(now_s)
        return True

    def reset(self) -> None:
        """The next ``due`` is True: a new run of failures starts."""
        self._last = _NeverLogged()


@dataclass(frozen=True, slots=True)
class _Coverage:
    covered_at_s: float
    consecutive_failures: int
    last_failure: BeatWriteFailed | NoBeatFailure


@dataclass(frozen=True, slots=True)
class _BeatThreadRuns:
    pass


@dataclass(frozen=True, slots=True)
class _BeatsCurrent:
    pass


@dataclass(frozen=True, slots=True)
class _WatchStopped:
    pass


@dataclass(frozen=True, slots=True)
class _StopRequested:
    pass


@dataclass(frozen=True, slots=True)
class _LoopClosed:
    pass


class RunnerHeartbeat:
    """The beat thread and the watchdog of one slot pool. Start with ``launch``.

    With ``StaleRunningRecoveryOn``, ``on_fail_closed`` runs on ``loop``,
    once, after the watchdog killed the children that held tasks. It must
    stop the worker. With ``StaleRunningRecoveryOff`` it never runs. ``stop``
    ends both threads; the worker calls it after its children have stopped.
    """

    def __init__(
        self,
        *,
        live: LiveDispatches,
        writer: BeatWriter,
        timing: BeatTiming,
        stale_running: StaleRunningRecovery,
        loop: asyncio.AbstractEventLoop,
        on_fail_closed: Callable[[FailClosedCause], None],
        exit_policy: ExitPolicy,
    ) -> None:
        self._live = live
        self._writer = writer
        self._timing = timing
        self._stale_running = stale_running
        self._failure_log = _LogGate(BEAT_FAILURE_LOG_INTERVAL_S)
        self._loop = loop
        self._on_fail_closed = on_fail_closed
        self._exit_policy = exit_policy
        self._stopping = threading.Event()
        self._state_lock = threading.Lock()
        self._coverage = _Coverage(
            covered_at_s=time.monotonic(),
            consecutive_failures=0,
            last_failure=NoBeatFailure(),
        )
        self._beat_thread_state: _BeatThreadRuns | BeatThreadDied = _BeatThreadRuns()
        self._beat_thread = threading.Thread(
            target=self._run_beats,
            name=BEAT_THREAD_NAME,
            daemon=True,
        )
        self._watch_thread = threading.Thread(
            target=self._run_watch,
            name=WATCH_THREAD_NAME,
            daemon=True,
        )

    @classmethod
    def launch(
        cls,
        *,
        live: LiveDispatches,
        writer: BeatWriter,
        timing: BeatTiming,
        stale_running: StaleRunningRecovery,
        loop: asyncio.AbstractEventLoop,
        on_fail_closed: Callable[[FailClosedCause], None],
        exit_policy: ExitPolicy = ExitPolicy(),
    ) -> RunnerHeartbeat:
        """Build the heartbeat and start both threads."""
        heartbeat = cls(
            live=live,
            writer=writer,
            timing=timing,
            stale_running=stale_running,
            loop=loop,
            on_fail_closed=on_fail_closed,
            exit_policy=exit_policy,
        )
        heartbeat._beat_thread.start()
        heartbeat._watch_thread.start()
        return heartbeat

    def stop(self) -> None:
        """End both threads, each join bounded by ``THREAD_STOP_JOIN_S``.

        Safe to call more than once. A beat thread blocked in a write is left
        to end with the process, as a daemon thread.
        """
        self._stopping.set()
        for thread in (self._beat_thread, self._watch_thread):
            if thread is not threading.current_thread():
                thread.join(THREAD_STOP_JOIN_S)

    # ----- beat thread -----

    def _run_beats(self) -> None:
        """Beat thread body. A defect ends the thread; the watchdog sees it."""
        try:
            delay_s = self._timing.interval_s
            while not self._stopping.wait(delay_s):
                delay_s = self._beat_once()
        except BaseException as exc:
            with self._state_lock:
                self._beat_thread_state = BeatThreadDied(f'{type(exc).__name__}: {exc}')
            logger.critical('Runner heartbeat thread failed', exc_info=True)
        finally:
            self._writer.close()

    def _beat_once(self) -> float:
        """Beat each held task whose child runs. Return the wait to the next write."""
        started_at = time.monotonic()
        dispatches = self._running_dispatches()
        if not dispatches:
            self._record_covered(started_at)
            return self._timing.interval_s
        outcome = self._writer.write(dispatches)
        match outcome:
            case BeatsWritten():
                self._record_covered(started_at)
                return max(0.0, started_at + self._timing.interval_s - time.monotonic())
            case BeatWriteFailed():
                failures = self._record_failure(outcome)
                self._log_failed_write(outcome, failures)
                return retry_delay_s(failures, self._timing.interval_s)
            case _ as unreachable:
                assert_never(unreachable)

    def _log_failed_write(self, failed: BeatWriteFailed, failures: int) -> None:
        """Log the first failure of a run, then at most once per log interval."""
        now_s = time.monotonic()
        if failures == 1:
            self._failure_log.reset()
        if not self._failure_log.due(now_s):
            return
        uncovered_s = now_s - self._read_coverage().covered_at_s
        match self._stale_running:
            case StaleRunningRecoveryOn() as on:
                consequence = f'the worker stops at {on.fail_closed_window_s():.1f}s'
            case StaleRunningRecoveryOff():
                consequence = 'no reaper fails a RUNNING row, so the worker runs on'
            case _ as unreachable:
                assert_never(unreachable)
        logger.error(
            'Runner heartbeat write failed (attempt %d, %.1fs since the last '
            'beat; %s): %s',
            failures,
            uncovered_s,
            consequence,
            failed.detail,
        )

    def _running_dispatches(self) -> tuple[LiveDispatch, ...]:
        """The held dispatches whose child process still runs.

        A child whose state cannot be read is kept: dropping its beat could
        make the reaper fail a task that runs.
        """
        running: list[LiveDispatch] = []
        for dispatch in self._live.snapshot():
            match child_process_state(dispatch.pid):
                case ProcessRuns():
                    running.append(dispatch)
                case ProcessEnded():
                    pass
                case ProcessUnreadable(detail=detail):
                    logger.warning(
                        'Cannot read the state of child pid=%s of task %s (%s); '
                        'its runner heartbeat continues',
                        dispatch.pid,
                        dispatch.task_id,
                        detail,
                    )
                    running.append(dispatch)
                case _ as unreachable:
                    assert_never(unreachable)
        return tuple(running)

    def _record_covered(self, covered_at_s: float) -> None:
        with self._state_lock:
            previous = self._coverage
            self._coverage = _Coverage(
                covered_at_s=covered_at_s,
                consecutive_failures=0,
                last_failure=NoBeatFailure(),
            )
        if previous.consecutive_failures > 0:
            logger.info(
                'Runner heartbeat write recovered after %d failed attempt(s)',
                previous.consecutive_failures,
            )

    def _record_failure(self, failed: BeatWriteFailed) -> int:
        with self._state_lock:
            previous = self._coverage
            self._coverage = _Coverage(
                covered_at_s=previous.covered_at_s,
                consecutive_failures=previous.consecutive_failures + 1,
                last_failure=failed,
            )
            return self._coverage.consecutive_failures

    def _read_coverage(self) -> _Coverage:
        with self._state_lock:
            return self._coverage

    # ----- watchdog -----

    def _run_watch(self) -> None:
        """Watchdog body. Fails closed only when stale running recovery is on."""
        match self._stale_running:
            case StaleRunningRecoveryOn() as on:
                cause = self._watch_until_cause(on.fail_closed_window_s())
                match cause:
                    case _WatchStopped():
                        return
                    case BeatThreadDied() | BeatsOverdue() | WatchFailed():
                        self._fail_closed(cause)
                    case _ as unreachable:
                        assert_never(unreachable)
            case StaleRunningRecoveryOff():
                self._watch_and_log()
            case _ as unreachable:
                assert_never(unreachable)

    def _watch_until_cause(self, window_s: float) -> FailClosedCause | _WatchStopped:
        """Check every ``WATCH_INTERVAL_S`` until a cause or a stop.

        A defect in this check is itself a cause: the worker fails closed
        rather than run on without a watchdog.
        """
        try:
            while not self._stopping.wait(WATCH_INTERVAL_S):
                check = self._check_beats(window_s)
                match check:
                    case _BeatsCurrent():
                        continue
                    case BeatThreadDied() | BeatsOverdue():
                        return check
                    case _ as unreachable:
                        assert_never(unreachable)
            return _WatchStopped()
        except Exception as exc:
            logger.critical('Runner heartbeat watchdog failed', exc_info=True)
            return WatchFailed(f'{type(exc).__name__}: {exc}')

    def _watch_and_log(self) -> None:
        """Watchdog body with stale running recovery off: log, never stop.

        Logs late beats at most once per ``BEAT_FAILURE_LOG_INTERVAL_S``, and
        a dead beat thread once. A defect in this check is logged, and the
        watchdog ends: no task depends on it.
        """
        late_after_s = LATE_BEAT_INTERVALS * self._timing.interval_s
        late_log = _LogGate(BEAT_FAILURE_LOG_INTERVAL_S)
        try:
            while not self._stopping.wait(WATCH_INTERVAL_S):
                check = self._check_beats(late_after_s)
                match check:
                    case _BeatsCurrent():
                        late_log.reset()
                    case BeatsOverdue():
                        if late_log.due(time.monotonic()):
                            logger.warning(
                                'No runner heartbeat landed for %.1fs; the worker '
                                'runs on, because no reaper fails a RUNNING row '
                                '(auto_fail_stale_running is off, or no recovery '
                                'config); %s',
                                check.uncovered_s,
                                describe_last_failure(check.last_failure),
                            )
                    case BeatThreadDied():
                        logger.critical(
                            '%s. The worker runs on without runner heartbeats, '
                            'because no reaper fails a RUNNING row '
                            '(auto_fail_stale_running is off, or no recovery '
                            'config)',
                            describe_fail_closed(check),
                        )
                        return
                    case _ as unreachable:
                        assert_never(unreachable)
        except Exception:
            logger.critical('Runner heartbeat watchdog failed', exc_info=True)

    def _check_beats(
        self,
        window_s: float,
    ) -> BeatThreadDied | BeatsOverdue | _BeatsCurrent:
        with self._state_lock:
            thread_state = self._beat_thread_state
            coverage = self._coverage
        match thread_state:
            case BeatThreadDied():
                return thread_state
            case _BeatThreadRuns():
                pass
            case _ as unreachable:
                assert_never(unreachable)
        if not self._beat_thread.is_alive() and not self._stopping.is_set():
            return BeatThreadDied('the thread ended with no exception')
        uncovered_s = time.monotonic() - coverage.covered_at_s
        if uncovered_s > window_s:
            return BeatsOverdue(
                uncovered_s=uncovered_s,
                window_s=window_s,
                last_failure=coverage.last_failure,
            )
        return _BeatsCurrent()

    def _fail_closed(self, cause: FailClosedCause) -> None:
        """Kill the children that hold tasks, stop the worker, exit if it does not stop.

        Ends the process with ``RUNNER_HEARTBEAT_LOST_EXIT_CODE`` when the
        worker does not call ``stop`` within the grace, when the event loop
        is closed, or when a step here raises.
        """
        try:
            logger.critical(
                'Stopping the worker: %s. Every child that holds a task is '
                'killed now, so the reaper cannot fail a task that still runs',
                describe_fail_closed(cause),
            )
            self._kill_held_children()
            request = self._request_worker_stop(cause)
            match request:
                case _StopRequested():
                    if self._stopping.wait(self._exit_policy.grace_s):
                        return
                    logger.critical(
                        'The worker did not stop within %.0fs; exiting with code %d',
                        self._exit_policy.grace_s,
                        RUNNER_HEARTBEAT_LOST_EXIT_CODE,
                    )
                case _LoopClosed():
                    logger.critical(
                        'The event loop is closed; exiting with code %d',
                        RUNNER_HEARTBEAT_LOST_EXIT_CODE,
                    )
                case _ as unreachable:
                    assert_never(unreachable)
        except BaseException:
            logger.critical(
                'Failing closed raised; exiting with code %d',
                RUNNER_HEARTBEAT_LOST_EXIT_CODE,
                exc_info=True,
            )
        self._exit_policy.exit_process(RUNNER_HEARTBEAT_LOST_EXIT_CODE)

    def _kill_held_children(self) -> None:
        """Close the registry; SIGKILL the session of each child that held a task."""
        for dispatch in self._live.close():
            kill = kill_child_session(dispatch.pid)
            match kill:
                case SessionKilled():
                    logger.critical(
                        'Killed child pid=%s, which held task %s',
                        dispatch.pid,
                        dispatch.task_id,
                    )
                case SessionGone():
                    pass
                case KillRefused(detail=detail):
                    logger.critical(
                        'Cannot kill child pid=%s of task %s: %s',
                        dispatch.pid,
                        dispatch.task_id,
                        detail,
                    )
                case _ as unreachable:
                    assert_never(unreachable)

    def _request_worker_stop(
        self,
        cause: FailClosedCause,
    ) -> _StopRequested | _LoopClosed:
        try:
            self._loop.call_soon_threadsafe(self._on_fail_closed, cause)
        except RuntimeError:
            return _LoopClosed()
        return _StopRequested()
