"""Parity runs: one scenario on the Python client and on the Rust client,
each on its own fresh database, and the compare of what each left.

Rust and Python are not interoperable: the two sides never share a
database. One side, on an empty `dualrun_` database:

1. Read the database clock.
2. Start one worker of the side per profile of the scenario, and the
   scheduler of the side for a scenario with `ScheduleSlots`, before the
   send process or after it (`WorkerStart`). The first operation of the
   side installs the schema, so the client family row is the side's.
3. Run the send process of the side (`python -m tests.parity.send`, or
   `horsies-parity send`): it sends the tasks, reruns tasks, starts the
   workflows, runs the actions on them and pings, and prints the ids of the
   sends, reruns and starts in order, and the counts of the ping answers.
4. For a scenario with `ScheduleSlots`: wait for the state row of the
   schedule, move its next run back, and wait until the scheduler caught up
   and each slot task has its history row; the slot count must be the one
   that the move makes due.
5. Wait until every sent task and every task of the started workflows has
   its history row and every started workflow ended, at most
   `TERMINAL_WAIT`.
6. Stop each process with `SIGTERM`; kill its process group after
   `STOP_WAIT`.
7. Read the clock again, capture the snapshot with the whole-run catalog
   (`tests/dualrun/worker_run.py`), and read the report: the task of each
   send and rerun, each node of each started workflow, the status of each
   started workflow, the ping answers, the run count of the schedule and
   each slot task, keyed by position.

The compare: the snapshots must be equal except for the planned difference,
the `family` of `horsies_client_family` (`PYTHON` against `RUST`); the
reports must be equal. The worker health tables are not in the compare
(`UNCOMPARED_TABLES`): the ping answers in the report are the compare of a
health scenario.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Final, LiteralString
from uuid import UUID

import psycopg

from horsies.core.types.result import Err, Ok, Result
from tests.parity import app as parity
from tests.dualrun.capture import capture_snapshot, read_database_clock
from tests.dualrun.compare import (
    ColumnDifference,
    DiffEntry,
    compare_snapshots,
    render_entry,
)
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.normalize import NormalizedSnapshot
from tests.dualrun.values import ExactValue, make_run_window
from tests.dualrun.worker_run import WHOLE_RUN_CATALOG, compared_part
from tests.parity.app import DATABASE_URL_ENV
from tests.parity.scenarios import NoScheduler, Scenario, ScheduleSlots, WorkerStart

REPO_ROOT: Final = Path(__file__).resolve().parents[2]

RUST_BINARY_ENV: Final = 'HORSIES_PARITY_RUST_BIN'
"""The path of the Rust scenario binary; default `target/debug/horsies-parity`."""

PYTHON_APP_LOCATOR: Final = 'tests.parity.app:app'

TERMINAL_WAIT: Final = timedelta(seconds=120)
TERMINAL_POLL: Final = timedelta(milliseconds=250)
SEND_WAIT: Final = timedelta(seconds=60)
STOP_WAIT: Final = timedelta(seconds=60)
KILL_WAIT: Final = timedelta(seconds=10)

PLANNED_TABLE: Final = 'horsies_client_family'
PLANNED_COLUMN: Final = 'family'


class Client(Enum):
    """The client of one side."""

    PYTHON = 'PYTHON'
    RUST = 'RUST'


def rust_binary() -> Path:
    """The Rust scenario binary: `HORSIES_PARITY_RUST_BIN`, else the debug
    build of `horsies-parity-tests`."""
    configured = os.environ.get(RUST_BINARY_ENV)
    match configured:
        case str() as path if path:
            return Path(path)
        case _:
            return REPO_ROOT / 'target' / 'debug' / 'horsies-parity'


@dataclass(frozen=True, slots=True)
class SideCommands:
    """The commands of one side, before their arguments."""

    worker: list[str]
    send: list[str]
    scheduler: list[str]


def _commands(client: Client) -> SideCommands:
    """The worker, send and scheduler commands of `client`."""
    match client:
        case Client.PYTHON:
            return SideCommands(
                worker=[sys.executable, '-m', 'horsies.core.cli', 'worker', PYTHON_APP_LOCATOR],
                send=[sys.executable, '-m', 'tests.parity.send'],
                scheduler=[
                    sys.executable, '-m', 'horsies.core.cli', 'scheduler', PYTHON_APP_LOCATOR,
                ],
            )
        case Client.RUST:
            binary = str(rust_binary())
            return SideCommands(
                worker=[binary, 'worker'], send=[binary, 'send'], scheduler=[binary, 'scheduler'],
            )


def _environment(database: DualRunDatabase) -> dict[str, str]:
    env = os.environ.copy()
    env['PYTHONPATH'] = str(REPO_ROOT)
    env[DATABASE_URL_ENV] = database.url
    env['RUST_BACKTRACE'] = '1'
    return env


# ---- The report ----


@dataclass(frozen=True, slots=True)
class AttemptReport:
    attempt: int
    outcome: str
    error_code: str | None


@dataclass(frozen=True, slots=True)
class TaskReport:
    """The history row of a task (`status`, `error_code`, `retry_count`) and
    its attempts, or `None` for each when it has no history row."""

    status: str | None
    error_code: str | None
    retry_count: int | None
    attempts: tuple[AttemptReport, ...]


@dataclass(frozen=True, slots=True)
class NodeReport:
    status: str
    task: TaskReport | None


@dataclass(frozen=True, slots=True)
class PingReport:
    """The count of workers that answered the ping of every worker, and of
    those that answered the ping of the first of them by its id."""

    workers: int
    targeted: int


@dataclass(frozen=True, slots=True)
class ScheduleReport:
    """The run count of the state row of the schedule."""

    run_count: int


type ReportKey = tuple[str, int] | tuple[str, int, int]
type ReportEntry = TaskReport | NodeReport | PingReport | ScheduleReport | str


@dataclass(frozen=True, slots=True)
class SentIds:
    """The ids and ping answers that the send process printed, by
    position."""

    tasks: tuple[UUID, ...]
    workflows: tuple[UUID, ...]
    pings: tuple[PingReport, ...]


@dataclass(frozen=True, slots=True)
class NoSlots:
    """The scenario ran no scheduler."""


@dataclass(frozen=True, slots=True)
class FiredSlots:
    """The slot tasks of the schedule, in slot order."""

    tasks: tuple[UUID, ...]


type ScheduleOutcome = NoSlots | FiredSlots


_HISTORY_SQL: Final[LiteralString] = (
    'SELECT status, error_code, retry_count FROM horsies_task_history WHERE task_id = %s'
)
_ATTEMPTS_SQL: Final[LiteralString] = (
    'SELECT attempt, outcome, error_code FROM horsies_task_attempts '
    'WHERE task_id = %s ORDER BY attempt'
)
_NODES_SQL: Final[LiteralString] = (
    'SELECT task_index, status, task_id FROM horsies_workflow_tasks '
    'WHERE workflow_id = %s ORDER BY task_index'
)
_WORKFLOW_SQL: Final[LiteralString] = 'SELECT status FROM horsies_workflows WHERE id = %s'
_WORKFLOW_TASKS_SQL: Final[LiteralString] = (
    'SELECT wt.task_id FROM horsies_workflow_tasks wt '
    'JOIN horsies_workflows w ON w.id = wt.workflow_id '
    'WHERE (w.id = ANY(%s) OR w.root_workflow_id = ANY(%s)) AND wt.task_id IS NOT NULL'
)
_OPEN_WORKFLOWS_SQL: Final[LiteralString] = (
    'SELECT count(*) FROM horsies_workflows '
    'WHERE (id = ANY(%s) OR root_workflow_id = ANY(%s)) '
    "AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')"
)
_ENDED_TASKS_SQL: Final[LiteralString] = (
    'SELECT count(*) FROM horsies_task_history WHERE task_id = ANY(%s)'
)
_SCHEDULE_STATE_SQL: Final[LiteralString] = (
    'SELECT next_run_at > now(), run_count FROM horsies_schedule_state '
    'WHERE schedule_name = %s'
)
_SCHEDULE_BACKDATE_SQL: Final[LiteralString] = (
    'UPDATE horsies_schedule_state SET next_run_at = now() - %s::interval '
    'WHERE schedule_name = %s'
)
_SLOT_TASKS_SQL: Final[LiteralString] = (
    'SELECT id, sent_at FROM horsies_tasks WHERE task_name = %s '
    'UNION SELECT task_id, sent_at FROM horsies_task_history WHERE task_name = %s '
    'ORDER BY 2, 1'
)


def _task_report(connection: psycopg.Connection[tuple[object, ...]], task_id: UUID) -> TaskReport:
    attempts: list[AttemptReport] = []
    for row in connection.execute(_ATTEMPTS_SQL, (task_id,)).fetchall():
        match row:
            case (int() as attempt, str() as outcome, str() | None as code):
                attempts.append(AttemptReport(attempt=attempt, outcome=outcome, error_code=code))
            case other:
                raise AssertionError(f'an attempt row of the report is {other!r}')
    match connection.execute(_HISTORY_SQL, (task_id,)).fetchone():
        case None:
            return TaskReport(status=None, error_code=None, retry_count=None, attempts=tuple(attempts))
        case (str() as status, str() | None as code, int() as retries):
            return TaskReport(
                status=status, error_code=code, retry_count=retries, attempts=tuple(attempts)
            )
        case other:
            raise AssertionError(f'a history row of the report is {other!r}')


def _schedule_report(connection: psycopg.Connection[tuple[object, ...]]) -> ScheduleReport:
    match connection.execute(_SCHEDULE_STATE_SQL, (parity.SCHEDULE_NAME,)).fetchone():
        case (bool(), int() as run_count):
            return ScheduleReport(run_count=run_count)
        case other:
            raise AssertionError(f'the schedule state row of the report is {other!r}')


def read_report(
    connection: psycopg.Connection[tuple[object, ...]],
    sent: SentIds,
    schedule: ScheduleOutcome,
) -> dict[ReportKey, ReportEntry]:
    """The report of every send and rerun, every started workflow, every
    ping, and the schedule and its slots, keyed by position. Raises
    `psycopg.Error` on a database error."""
    report: dict[ReportKey, ReportEntry] = {}
    for index, task_id in enumerate(sent.tasks):
        report[('task', index)] = _task_report(connection, task_id)
    for index, ping in enumerate(sent.pings):
        report[('ping', index)] = ping
    match schedule:
        case FiredSlots(tasks=slots):
            report[('schedule', 0)] = _schedule_report(connection)
            for index, task_id in enumerate(slots):
                report[('slot', index)] = _task_report(connection, task_id)
        case NoSlots():
            pass
    for index, workflow_id in enumerate(sent.workflows):
        match connection.execute(_WORKFLOW_SQL, (workflow_id,)).fetchone():
            case (str() as status,):
                report[('workflow', index)] = status
            case other:
                raise AssertionError(f'the workflow row of the report is {other!r}')
        for row in connection.execute(_NODES_SQL, (workflow_id,)).fetchall():
            match row:
                case (int() as node, str() as status, UUID() | None as task_id):
                    task = None if task_id is None else _task_report(connection, task_id)
                    report[('node', index, node)] = NodeReport(status=status, task=task)
                case other:
                    raise AssertionError(f'a node row of the report is {other!r}')
    return report


# ---- One side ----


@dataclass(frozen=True, slots=True)
class SideRun:
    """What one side left: the comparable snapshot and the report."""

    client: Client
    snapshot: NormalizedSnapshot
    report: dict[ReportKey, ReportEntry]


@dataclass(frozen=True, slots=True)
class SideFailed:
    """One side did not reach its stop point or its capture; `detail` names
    the step and holds the worker's last output."""

    client: Client
    detail: str


def _parse_sent(output: str) -> SentIds:
    tasks: list[UUID] = []
    workflows: list[UUID] = []
    pings: list[PingReport] = []
    for line in output.splitlines():
        match json.loads(line):
            case {'kind': 'task', 'index': int() as index, 'id': str() as task_id}:
                assert index == len(tasks), f'a task line out of order: {line}'
                tasks.append(UUID(task_id))
            case {'kind': 'workflow', 'index': int() as index, 'id': str() as workflow_id}:
                assert index == len(workflows), f'a workflow line out of order: {line}'
                workflows.append(UUID(workflow_id))
            case {
                'kind': 'ping',
                'index': int() as index,
                'workers': int() as workers,
                'targeted': int() as targeted,
            }:
                assert index == len(pings), f'a ping line out of order: {line}'
                pings.append(PingReport(workers=workers, targeted=targeted))
            case other:
                raise AssertionError(f'a send line is {other!r}')
    return SentIds(tasks=tuple(tasks), workflows=tuple(workflows), pings=tuple(pings))


def _all_ended(connection: psycopg.Connection[tuple[object, ...]], sent: SentIds) -> bool:
    workflows = list(sent.workflows)
    node_tasks = [
        row[0]
        for row in connection.execute(_WORKFLOW_TASKS_SQL, (workflows, workflows)).fetchall()
    ]
    task_ids = list(sent.tasks) + node_tasks
    ended = connection.execute(_ENDED_TASKS_SQL, (task_ids,)).fetchone()
    open_workflows = connection.execute(_OPEN_WORKFLOWS_SQL, (workflows, workflows)).fetchone()
    return ended == (len(set(task_ids)),) and open_workflows == (0,)


def _wait_until(
    processes: Sequence[StartedProcess], reached: Callable[[], bool], what: str,
) -> str | None:
    """`None` once `reached` holds; the reason when a process exited first
    or `TERMINAL_WAIT` passed before `what`."""
    deadline = time.monotonic() + TERMINAL_WAIT.total_seconds()
    while not reached():
        exited = [
            f'{started.name} exited with {started.process.returncode} first'
            for started in processes
            if started.process.poll() is not None
        ]
        if exited:
            return '; '.join(exited)
        if time.monotonic() > deadline:
            return f'not {what} within {TERMINAL_WAIT}'
        time.sleep(TERMINAL_POLL.total_seconds())
    return None


def _stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        _ = process.wait(timeout=STOP_WAIT.total_seconds())
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        _ = process.wait(timeout=KILL_WAIT.total_seconds())


def _tail(path: Path, limit: int = 4_000) -> str:
    return path.read_bytes().decode('utf-8', errors='replace')[-limit:]


@dataclass(frozen=True, slots=True)
class StartedProcess:
    """A worker or the scheduler of a side, with its log file."""

    name: str
    process: subprocess.Popen[bytes]
    log_path: Path


def _start_process(
    name: str, command: list[str], env: dict[str, str], log_dir: Path,
) -> StartedProcess:
    log_path = log_dir / f'{name}.log'
    with log_path.open('wb') as log:
        process = subprocess.Popen(
            [*command, '--loglevel=info'],
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
            cwd=REPO_ROOT,
        )
    return StartedProcess(name=name, process=process, log_path=log_path)


def _start_processes(
    commands: SideCommands, scenario: Scenario, env: dict[str, str], log_dir: Path,
) -> list[StartedProcess]:
    """One worker per profile of `scenario`, then the scheduler of a
    scenario with `ScheduleSlots`, each with its own log file."""
    started = [
        _start_process(f'worker-{position}', [*commands.worker, f'--profile={profile}'], env, log_dir)
        for position, profile in enumerate(scenario.profiles)
    ]
    match scenario.scheduler:
        case ScheduleSlots():
            started.append(_start_process('scheduler', commands.scheduler, env, log_dir))
        case NoScheduler():
            pass
    return started


def _run_send(
    send_command: list[str], scenario: Scenario, env: dict[str, str],
) -> Result[SentIds, str]:
    """The ids that the send process of the side printed, or why it failed."""
    sending = subprocess.run(
        [*send_command, scenario.name],
        capture_output=True,
        env=env,
        cwd=REPO_ROOT,
        timeout=SEND_WAIT.total_seconds(),
        check=False,
    )
    match sending.returncode:
        case 0:
            return Ok(_parse_sent(sending.stdout.decode()))
        case code:
            return Err(f'send exited {code}: {sending.stderr.decode(errors="replace")}')


def _tails(processes: Sequence[StartedProcess]) -> str:
    return '\n'.join(f'--- {started.name}\n{_tail(started.log_path)}' for started in processes)


def _schedule_caught_up(connection: psycopg.Connection[tuple[object, ...]]) -> bool:
    """The next run of the schedule is after the database time."""
    match connection.execute(_SCHEDULE_STATE_SQL, (parity.SCHEDULE_NAME,)).fetchone():
        case (bool() as caught_up, int()):
            return caught_up
        case None:
            return False
        case other:
            raise AssertionError(f'the schedule state row is {other!r}')


def _slot_tasks(connection: psycopg.Connection[tuple[object, ...]]) -> tuple[UUID, ...]:
    """The tasks of the schedule's task name, in slot order."""
    rows = connection.execute(_SLOT_TASKS_SQL, (parity.SLOT_TASK_NAME, parity.SLOT_TASK_NAME))
    slots: list[UUID] = []
    for row in rows.fetchall():
        match row:
            case (UUID() as task_id, _sent_at):
                slots.append(task_id)
            case other:
                raise AssertionError(f'a slot task row is {other!r}')
    return tuple(slots)


def _slots_ended(connection: psycopg.Connection[tuple[object, ...]]) -> bool:
    """The schedule caught up, and each slot task has its history row."""
    if not _schedule_caught_up(connection):
        return False
    slots = _slot_tasks(connection)
    ended = connection.execute(_ENDED_TASKS_SQL, (list(slots),)).fetchone()
    return ended == (len(slots),)


def _fire_slots(
    connection: psycopg.Connection[tuple[object, ...]],
    schedule: ScheduleSlots,
    processes: Sequence[StartedProcess],
) -> Result[FiredSlots, str]:
    """Move the next run of the schedule back by `schedule.backdate` once
    its state row exists, and wait until the scheduler fired the due slots
    and each slot task ended. The slot tasks, or why not."""
    assert schedule.backdate % parity.SCHEDULE_INTERVAL != timedelta(0), (
        f'{schedule}: a backdate that is a multiple of the interval puts a slot '
        'at the database time of the move'
    )
    exists = _wait_until(
        processes,
        lambda: connection.execute(
            _SCHEDULE_STATE_SQL, (parity.SCHEDULE_NAME,),
        ).fetchone() is not None,
        f'the state row of schedule {parity.SCHEDULE_NAME} written',
    )
    if exists is not None:
        return Err(exists)
    moved = connection.execute(
        _SCHEDULE_BACKDATE_SQL, (schedule.backdate, parity.SCHEDULE_NAME),
    ).rowcount
    assert moved == 1, f'the move of schedule {parity.SCHEDULE_NAME} changed {moved} rows'
    fired = _wait_until(
        processes,
        lambda: _slots_ended(connection),
        f'the schedule {parity.SCHEDULE_NAME} caught up with each slot task ended',
    )
    if fired is not None:
        return Err(fired)
    slots = _slot_tasks(connection)
    due = schedule.backdate // parity.SCHEDULE_INTERVAL + 1
    if len(slots) != due:
        return Err(f'the scheduler fired {len(slots)} slots; the move made {due} due')
    return Ok(FiredSlots(tasks=slots))


def run_side(
    client: Client, database: DualRunDatabase, scenario: Scenario,
) -> Result[SideRun, SideFailed]:
    """Run `scenario` with `client` on the empty `database`. Leaves no
    process behind."""
    commands = _commands(client)
    env = _environment(database)
    log_dir = Path(tempfile.mkdtemp(prefix=f'parity-{client.value.lower()}-'))
    processes: list[StartedProcess] = []
    try:
        with psycopg.connect(database.url, autocommit=True) as connection:
            match read_database_clock(connection):
                case Ok(started):
                    pass
                case Err(failed):
                    return Err(SideFailed(client=client, detail=f'clock: {failed}'))
            try:
                match scenario.worker_start:
                    case WorkerStart.BEFORE_SENDS:
                        processes = _start_processes(commands, scenario, env, log_dir)
                        sent_ids = _run_send(commands.send, scenario, env)
                    case WorkerStart.AFTER_ACTIONS:
                        sent_ids = _run_send(commands.send, scenario, env)
                        processes = _start_processes(commands, scenario, env, log_dir)
                match sent_ids:
                    case Ok(sent):
                        pass
                    case Err(detail):
                        return Err(SideFailed(client=client, detail=f'{detail}\n{_tails(processes)}'))
                match scenario.scheduler:
                    case ScheduleSlots() as slots_run:
                        assert scenario.sends == () and scenario.workflows == (), (
                            f'{scenario.name}: a scenario with ScheduleSlots sends nothing'
                        )
                        match _fire_slots(connection, slots_run, processes):
                            case Ok(fired):
                                schedule: ScheduleOutcome = fired
                            case Err(detail):
                                return Err(
                                    SideFailed(client=client, detail=f'{detail}\n{_tails(processes)}'),
                                )
                    case NoScheduler():
                        schedule = NoSlots()
                waited = _wait_until(
                    processes,
                    lambda: _all_ended(connection, sent),
                    'every task and workflow ended',
                )
            finally:
                for running in processes:
                    _stop(running.process)
            if waited is not None:
                return Err(SideFailed(client=client, detail=f'{waited}\n{_tails(processes)}'))
            match read_database_clock(connection):
                case Ok(ended):
                    pass
                case Err(failed):
                    return Err(SideFailed(client=client, detail=f'clock: {failed}'))
            match make_run_window(started, ended):
                case Ok(window):
                    pass
                case Err(invalid):
                    return Err(SideFailed(client=client, detail=f'run window: {invalid}'))
            match capture_snapshot(connection, WHOLE_RUN_CATALOG, window):
                case Ok(snapshot):
                    pass
                case Err(capture_error):
                    return Err(SideFailed(client=client, detail=f'capture: {capture_error}'))
            report = read_report(connection, sent, schedule)
        return Ok(SideRun(client=client, snapshot=compared_part(snapshot), report=report))
    finally:
        shutil.rmtree(log_dir)


# ---- The compare ----


def _is_planned(entry: DiffEntry) -> bool:
    """The planned difference: the client family row records each client."""
    match entry:
        case ColumnDifference(
            table=table,
            column=column,
            reference=ExactValue(text='PYTHON'),
            candidate=ExactValue(text='RUST'),
        ):
            return table == PLANNED_TABLE and column == PLANNED_COLUMN
        case _:
            return False


@dataclass(frozen=True, slots=True)
class ParityResult:
    """The unplanned snapshot differences, rendered, and the report keys
    whose entries differ, with both entries."""

    snapshot_differences: tuple[str, ...]
    planned_seen: bool
    report_differences: tuple[tuple[ReportKey, ReportEntry | None, ReportEntry | None], ...]


def compare_sides(python: SideRun, rust: SideRun) -> ParityResult:
    """Compare the Python side (reference) with the Rust side (candidate).
    Pure."""
    diff = compare_snapshots(python.snapshot, rust.snapshot)
    planned = tuple(entry for entry in diff.entries if _is_planned(entry))
    unplanned = tuple(render_entry(entry) for entry in diff.entries if not _is_planned(entry))
    keys = sorted(set(python.report) | set(rust.report), key=str)
    report_differences = tuple(
        (key, python.report.get(key), rust.report.get(key))
        for key in keys
        if python.report.get(key) != rust.report.get(key)
    )
    return ParityResult(
        snapshot_differences=unplanned,
        planned_seen=len(planned) == 1,
        report_differences=report_differences,
    )
