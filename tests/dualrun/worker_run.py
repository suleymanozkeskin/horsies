"""Whole-run pairs of the worker: the reference worker and the candidate worker
run as processes, each on a fresh ``dualrun_`` database, with the same app,
profile and sends. Their snapshots and their per-task reports must be equal.

One side of a pair:

1. Write the sends: the task ids and the ``sent_at`` of the scenario, which
   both sides share, and the stored options of each registered task
   (``enqueue_async``, the write of every send).
2. Read the database clock, then start the worker of the side with the
   profile of the scenario.
3. Wait until every sent task has its history row (it is terminal), at most
   ``TERMINAL_WAIT``.
4. Stop the worker with ``SIGTERM`` and wait at most ``STOP_WAIT`` for its
   exit. A worker that does not exit is killed with its process group.
5. Read the clock again, then capture the snapshot and the task report.

The snapshot compares by ``WHOLE_RUN_CATALOG``: the baseline rules, with the
process ids of a run ignored. The heartbeat and worker-state tables are left
out of the compare: their row counts follow the run time, not a decision of
the code. Run times inside the run window compare as ``DuringRun``, the
worker id as its label. Speed numbers never come from these pairs.

Before the worker switch both sides start ``horsies worker``: a pair then
proves that the harness itself is stable. The switch points
``REFERENCE_WORKER`` at the copy of the Python worker parent.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Final, LiteralString
from uuid import UUID

import psycopg

from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.task_decorator import effective_priority
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.apps import whole_run_app
from tests.dualrun.capture import (
    CaptureError,
    ClockReadFailed,
    Connection,
    capture_snapshot,
    read_database_clock,
)
from tests.dualrun.catalog import BASELINE_CATALOG
from tests.dualrun.clients import open_broker, sqlalchemy_url
from tests.dualrun.compare import SnapshotDiff, compare_snapshots
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.normalize import NormalizedSnapshot
from tests.dualrun.rules import ColumnSpec, Ignored, SnapshotCatalog, TableRules
from tests.dualrun.values import RunWindowInvalid, make_run_window

APP_LOCATOR: Final = 'tests.dualrun.apps.whole_run_app:app'

REPO_ROOT: Final = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
)

TERMINAL_WAIT: Final = timedelta(seconds=120)
"""The longest wait for every sent task to be terminal."""

TERMINAL_POLL: Final = timedelta(milliseconds=250)

STOP_WAIT: Final = timedelta(seconds=60)
"""The longest wait for the worker exit after ``SIGTERM``: the drain timeout
of every profile of the app (5 s) plus the bounded stop steps."""

KILL_WAIT: Final = timedelta(seconds=10)

UNCOMPARED_TABLES: Final = frozenset({'horsies_heartbeats', 'horsies_worker_states'})
"""Tables whose row count follows the run time: one heartbeat per interval,
one state sample per interval."""

PROCESS_ID_COLUMNS: Final = frozenset({'worker_pid', 'last_worker_pid'})
"""Columns that hold the process id of a worker child of the run."""

_RUN_PROCESS_ID: Final = Ignored(
    reason='the process id of a worker child; each run starts its own processes',
)


@dataclass(frozen=True, slots=True)
class WorkerCommand:
    """How one side starts its worker: ``argv`` then the worker arguments."""

    name: str
    argv: tuple[str, ...]


REFERENCE_WORKER: Final = WorkerCommand(
    name='reference',
    argv=(sys.executable, '-m', 'horsies.core.cli', 'worker'),
)
CANDIDATE_WORKER: Final = WorkerCommand(
    name='candidate',
    argv=(sys.executable, '-m', 'horsies.core.cli', 'worker'),
)


@dataclass(frozen=True, slots=True)
class Send:
    """One send with its fixed identity."""

    task_id: UUID
    task_name: str
    input_json: str | None
    task_options: str | None


@dataclass(frozen=True, slots=True)
class Scenario:
    """The profile and the sends of one pair. The ``n``-th send has
    ``sent_at`` ``first_sent_at + n ms`` on both sides."""

    name: str
    profile: str
    sends: tuple[Send, ...]
    first_sent_at: datetime


def build_scenario(
    name: str,
    profile: str,
    sends: Sequence[tuple[object, str | None]],
) -> Scenario:
    """A scenario of ``(registered task, input JSON)`` sends: each gets a
    task id minted now, and the first ``sent_at`` is now. Both sides of the
    pair use these values."""
    return Scenario(
        name=name,
        profile=profile,
        sends=tuple(
            send_of(task, mint_task_id(), input_json) for task, input_json in sends
        ),
        first_sent_at=datetime.now(UTC),
    )


# ---- The per-task report ----


@dataclass(frozen=True, slots=True)
class AttemptReport:
    attempt: int
    outcome: str
    error_code: str | None


@dataclass(frozen=True, slots=True)
class Terminal:
    """The task has its history row."""

    status: str
    error_code: str | None
    retry_count: int


@dataclass(frozen=True, slots=True)
class Live:
    """The task is in the live table only: it did not end."""

    status: str
    retry_count: int


@dataclass(frozen=True, slots=True)
class Absent:
    """Neither table holds the task."""


type TaskState = Terminal | Live | Absent


@dataclass(frozen=True, slots=True)
class TaskReport:
    state: TaskState
    attempts: tuple[AttemptReport, ...]


@dataclass(frozen=True, slots=True)
class ReportDifference:
    task_id: UUID
    reference: TaskReport
    candidate: TaskReport


def compare_reports(
    reference: dict[UUID, TaskReport],
    candidate: dict[UUID, TaskReport],
) -> tuple[ReportDifference, ...]:
    """Every task whose report differs, in task id order. Both reports cover
    the same sends (asserted). Pure."""
    assert reference.keys() == candidate.keys(), 'the two reports cover other tasks'
    return tuple(
        ReportDifference(
            task_id=task_id, reference=reference[task_id], candidate=candidate[task_id]
        )
        for task_id in sorted(reference)
        if reference[task_id] != candidate[task_id]
    )


# ---- The whole-run catalog ----


def _whole_run_table(table: TableRules) -> TableRules:
    match table.name in UNCOMPARED_TABLES:
        case True:
            return table
        case False:
            columns = tuple(
                ColumnSpec(name=column.name, rule=_RUN_PROCESS_ID)
                if column.name in PROCESS_ID_COLUMNS
                else column
                for column in table.columns
            )
            return dataclasses.replace(table, columns=columns)


def whole_run_catalog(base: SnapshotCatalog) -> SnapshotCatalog:
    """``base`` with the process id columns of a run ignored. The tables of
    ``UNCOMPARED_TABLES`` keep their rules: they are captured, then left out
    by ``compared_part``. Pure."""
    return SnapshotCatalog(
        tables=tuple(_whole_run_table(table) for table in base.tables)
    )


WHOLE_RUN_CATALOG: Final = whole_run_catalog(BASELINE_CATALOG)


def compared_part(snapshot: NormalizedSnapshot) -> NormalizedSnapshot:
    """``snapshot`` without the tables of ``UNCOMPARED_TABLES``. Pure."""
    return NormalizedSnapshot(
        tables=tuple(
            table
            for table in snapshot.tables
            if table.rules.name not in UNCOMPARED_TABLES
        ),
        ambiguities=tuple(
            group
            for group in snapshot.ambiguities
            if group.table not in UNCOMPARED_TABLES
        ),
        partition_tree=snapshot.partition_tree,
    )


# ---- Sends ----


def send_of(task: object, task_id: UUID, input_json: str | None) -> Send:
    """The send of a registered task of the app, with its stored options."""
    task_name = getattr(task, 'task_name')
    task_options = getattr(task, 'task_options_json')
    assert isinstance(task_name, str), f'{task!r} is not a registered task'
    assert task_options is None or isinstance(task_options, str), task_options
    return Send(
        task_id=task_id,
        task_name=task_name,
        input_json=input_json,
        task_options=task_options,
    )


async def _write_sends(database: DualRunDatabase, scenario: Scenario) -> None:
    priority = effective_priority(whole_run_app.app, 'default')
    async with open_broker(database) as broker:
        for position, send in enumerate(scenario.sends):
            written = await broker.enqueue_async(
                send.task_name,
                'default',
                task_id=send.task_id,
                send_kind=SendKind.FIRST,
                input_json=send.input_json,
                priority=priority,
                sent_at=scenario.first_sent_at + timedelta(milliseconds=position),
                task_options=send.task_options,
            )
            match written:
                case Ok(task_id):
                    assert task_id == send.task_id, (task_id, send)
                case Err(refused):
                    raise AssertionError(
                        f'the send of {send.task_id} was refused: {refused}'
                    )


# ---- One side ----


class StopEnd(Enum):
    """How the worker of a side ended after ``SIGTERM``."""

    EXITED = 'exited'
    KILLED_AFTER_STOP_WAIT = 'killed after the stop wait'


@dataclass(frozen=True, slots=True)
class WorkerEnd:
    end: StopEnd
    exit_code: int | None
    log_tail: str


@dataclass(frozen=True, slots=True)
class SideRun:
    """What one side left: the comparable snapshot, the task report and how
    its worker ended."""

    snapshot: NormalizedSnapshot
    report: dict[UUID, TaskReport]
    worker: WorkerEnd


@dataclass(frozen=True, slots=True)
class TasksNotTerminal:
    """A sent task had no history row when ``TERMINAL_WAIT`` ran out."""

    side: str
    open_tasks: tuple[UUID, ...]
    worker: WorkerEnd


@dataclass(frozen=True, slots=True)
class WorkerExitedEarly:
    """The worker exited before every task was terminal."""

    side: str
    worker: WorkerEnd


@dataclass(frozen=True, slots=True)
class CaptureFailed:
    side: str
    cause: CaptureError | ClockReadFailed | RunWindowInvalid


type SideFailure = TasksNotTerminal | WorkerExitedEarly | CaptureFailed


_HISTORY_SQL: Final[LiteralString] = (
    'SELECT task_id, status, error_code, retry_count FROM horsies_task_history '
    'WHERE task_id = ANY(%s)'
)
_LIVE_SQL: Final[LiteralString] = (
    'SELECT id, status, retry_count FROM horsies_tasks WHERE id = ANY(%s)'
)
_ATTEMPTS_SQL: Final[LiteralString] = (
    'SELECT task_id, attempt, outcome, error_code FROM horsies_task_attempts '
    'WHERE task_id = ANY(%s) ORDER BY task_id, attempt'
)


def _open_tasks(connection: Connection, task_ids: list[UUID]) -> tuple[UUID, ...]:
    """The sent tasks with no history row yet."""
    rows = connection.execute(_HISTORY_SQL, (task_ids,)).fetchall()
    terminal = {row[0] for row in rows}
    return tuple(task_id for task_id in task_ids if task_id not in terminal)


def _terminal_row(row: tuple[object, ...]) -> tuple[UUID, Terminal]:
    match row:
        case (
            UUID() as task_id,
            str() as status,
            str()
            | None as error_code,
            int() as retries,
        ):
            return task_id, Terminal(
                status=status, error_code=error_code, retry_count=retries
            )
        case other:
            raise AssertionError(f'a history row of the report is {other!r}')


def _live_row(row: tuple[object, ...]) -> tuple[UUID, Live]:
    match row:
        case (UUID() as task_id, str() as status, int() as retries):
            return task_id, Live(status=status, retry_count=retries)
        case other:
            raise AssertionError(f'a live row of the report is {other!r}')


def _attempt_row(row: tuple[object, ...]) -> tuple[UUID, AttemptReport]:
    match row:
        case (
            UUID() as task_id,
            int() as attempt,
            str() as outcome,
            str()
            | None as code,
        ):
            return task_id, AttemptReport(
                attempt=attempt, outcome=outcome, error_code=code
            )
        case other:
            raise AssertionError(f'an attempt row of the report is {other!r}')


def read_task_report(
    connection: Connection,
    task_ids: list[UUID],
) -> dict[UUID, TaskReport]:
    """The report of every sent task. Raises ``psycopg.Error`` on a database error."""
    terminal = dict(
        _terminal_row(row)
        for row in connection.execute(_HISTORY_SQL, (task_ids,)).fetchall()
    )
    live = dict(
        _live_row(row) for row in connection.execute(_LIVE_SQL, (task_ids,)).fetchall()
    )
    attempts: dict[UUID, list[AttemptReport]] = {task_id: [] for task_id in task_ids}
    for row in connection.execute(_ATTEMPTS_SQL, (task_ids,)).fetchall():
        task_id, attempt = _attempt_row(row)
        attempts[task_id].append(attempt)
    report: dict[UUID, TaskReport] = {}
    for task_id in task_ids:
        state: TaskState
        match terminal.get(task_id), live.get(task_id):
            case Terminal() as ended, _:
                state = ended
            case None, Live() as running:
                state = running
            case None, None:
                state = Absent()
        report[task_id] = TaskReport(state=state, attempts=tuple(attempts[task_id]))
    return report


def _start_worker(
    command: WorkerCommand,
    database: DualRunDatabase,
    profile: str,
    log_path: str,
) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env['PYTHONPATH'] = REPO_ROOT
    env[whole_run_app.DATABASE_URL_ENV] = sqlalchemy_url(database)
    with open(log_path, 'wb') as log:
        return subprocess.Popen(
            [*command.argv, APP_LOCATOR, f'--profile={profile}', '--loglevel=info'],
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
            cwd=REPO_ROOT,
        )


def _log_tail(log_path: str, limit: int = 4_000) -> str:
    with open(log_path, 'rb') as log:
        text = log.read().decode('utf-8', errors='replace')
    return text[-limit:]


def _stop_worker(process: subprocess.Popen[bytes], log_path: str) -> WorkerEnd:
    """``SIGTERM`` to the worker, then wait; kill its process group after
    ``STOP_WAIT``."""
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        exit_code = process.wait(timeout=STOP_WAIT.total_seconds())
        return WorkerEnd(
            end=StopEnd.EXITED, exit_code=exit_code, log_tail=_log_tail(log_path)
        )
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            exit_code = process.wait(timeout=KILL_WAIT.total_seconds())
        except subprocess.TimeoutExpired:
            exit_code = None
        return WorkerEnd(
            end=StopEnd.KILLED_AFTER_STOP_WAIT,
            exit_code=exit_code,
            log_tail=_log_tail(log_path),
        )


def _wait_terminal(
    connection: Connection,
    process: subprocess.Popen[bytes],
    task_ids: list[UUID],
) -> tuple[UUID, ...] | None:
    """Poll until no sent task is open (``()``), the bound runs out (the open
    tasks), or the worker exits (``None``). Bounded by ``TERMINAL_WAIT``."""
    deadline = time.monotonic() + TERMINAL_WAIT.total_seconds()
    open_tasks = _open_tasks(connection, task_ids)
    while open_tasks and time.monotonic() < deadline:
        if process.poll() is not None:
            return None
        time.sleep(TERMINAL_POLL.total_seconds())
        open_tasks = _open_tasks(connection, task_ids)
    return open_tasks


def run_side(
    command: WorkerCommand,
    database: DualRunDatabase,
    scenario: Scenario,
) -> Result[SideRun, SideFailure]:
    """Run one side of ``scenario`` on ``database``, whose baseline is
    installed. Leaves no worker process behind."""
    asyncio.run(_write_sends(database, scenario))
    task_ids = [send.task_id for send in scenario.sends]
    log_fd, log_path = tempfile.mkstemp(
        prefix=f'whole-run-{command.name}-', suffix='.log'
    )
    os.close(log_fd)
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_database_clock(connection):
            case Ok(started):
                pass
            case Err(clock_failed):
                return Err(CaptureFailed(side=command.name, cause=clock_failed))
        process = _start_worker(command, database, scenario.profile, log_path)
        try:
            open_tasks = _wait_terminal(connection, process, task_ids)
        finally:
            worker = _stop_worker(process, log_path)
        match open_tasks:
            case None:
                return Err(WorkerExitedEarly(side=command.name, worker=worker))
            case ():
                pass
            case still_open:
                return Err(
                    TasksNotTerminal(
                        side=command.name, open_tasks=still_open, worker=worker
                    )
                )
        match read_database_clock(connection):
            case Ok(ended):
                pass
            case Err(clock_failed):
                return Err(CaptureFailed(side=command.name, cause=clock_failed))
        match make_run_window(started, ended):
            case Ok(window):
                pass
            case Err(invalid):
                return Err(CaptureFailed(side=command.name, cause=invalid))
        match capture_snapshot(connection, WHOLE_RUN_CATALOG, window):
            case Ok(snapshot):
                pass
            case Err(capture_error):
                return Err(CaptureFailed(side=command.name, cause=capture_error))
        report = read_task_report(connection, task_ids)
    return Ok(SideRun(snapshot=compared_part(snapshot), report=report, worker=worker))


@dataclass(frozen=True, slots=True)
class PairRun:
    """Both sides of one scenario and their differences."""

    reference: SideRun
    candidate: SideRun
    snapshot_diff: SnapshotDiff
    report_differences: tuple[ReportDifference, ...]


def run_pair(
    scenario: Scenario,
    reference_database: DualRunDatabase,
    candidate_database: DualRunDatabase,
) -> Result[PairRun, SideFailure]:
    """Run the reference side, then the candidate side, and compare them. The
    sides run one after the other, so they do not share the machine."""
    match run_side(REFERENCE_WORKER, reference_database, scenario):
        case Ok(reference):
            pass
        case Err(failed):
            return Err(failed)
    match run_side(CANDIDATE_WORKER, candidate_database, scenario):
        case Ok(candidate):
            pass
        case Err(failed):
            return Err(failed)
    return Ok(
        PairRun(
            reference=reference,
            candidate=candidate,
            snapshot_diff=compare_snapshots(reference.snapshot, candidate.snapshot),
            report_differences=compare_reports(reference.report, candidate.report),
        ),
    )
