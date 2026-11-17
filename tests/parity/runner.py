"""Parity runs: one scenario on the Python client and on the Rust client,
each on its own fresh database, and the compare of what each left.

Rust and Python are not interoperable: the two sides never share a
database. One side, on an empty `dualrun_` database:

1. Read the database clock.
2. Start one worker of the side per profile of the scenario, before the
   send process or after it (`WorkerStart`). The first operation of the
   side installs the schema, so the client family row is the side's.
3. Run the send process of the side (`python -m tests.parity.send`, or
   `horsies-parity send`): it sends the tasks, starts the workflows, runs
   the actions on them, and prints the ids of the sends and starts in order.
4. Wait until every sent task and every task of the started workflows has
   its history row and every started workflow ended, at most
   `TERMINAL_WAIT`.
5. Stop each worker with `SIGTERM`; kill its process group after
   `STOP_WAIT`.
6. Read the clock again, capture the snapshot with the whole-run catalog
   (`tests/dualrun/worker_run.py`), and read the report: the task of each
   send, each node of each started workflow, and the status of each
   started workflow, keyed by position.

The compare: the snapshots must be equal except for the planned difference,
the `family` of `horsies_client_family` (`PYTHON` against `RUST`); the
reports must be equal.
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
from tests.parity.scenarios import Scenario, WorkerStart

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


def _commands(client: Client) -> tuple[list[str], list[str]]:
    """The worker command and the send command of `client`, before their
    arguments."""
    match client:
        case Client.PYTHON:
            return (
                [sys.executable, '-m', 'horsies.core.cli', 'worker', PYTHON_APP_LOCATOR],
                [sys.executable, '-m', 'tests.parity.send'],
            )
        case Client.RUST:
            binary = str(rust_binary())
            return ([binary, 'worker'], [binary, 'send'])


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


type ReportKey = tuple[str, int] | tuple[str, int, int]
type ReportEntry = TaskReport | NodeReport | str


@dataclass(frozen=True, slots=True)
class SentIds:
    """The ids that the send process printed, by position."""

    tasks: tuple[UUID, ...]
    workflows: tuple[UUID, ...]


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


def read_report(
    connection: psycopg.Connection[tuple[object, ...]], sent: SentIds,
) -> dict[ReportKey, ReportEntry]:
    """The report of every send and every started workflow, keyed by
    position. Raises `psycopg.Error` on a database error."""
    report: dict[ReportKey, ReportEntry] = {}
    for index, task_id in enumerate(sent.tasks):
        report[('task', index)] = _task_report(connection, task_id)
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
    for line in output.splitlines():
        match json.loads(line):
            case {'kind': 'task', 'index': int() as index, 'id': str() as task_id}:
                assert index == len(tasks), f'a task line out of order: {line}'
                tasks.append(UUID(task_id))
            case {'kind': 'workflow', 'index': int() as index, 'id': str() as workflow_id}:
                assert index == len(workflows), f'a workflow line out of order: {line}'
                workflows.append(UUID(workflow_id))
            case other:
                raise AssertionError(f'a send line is {other!r}')
    return SentIds(tasks=tuple(tasks), workflows=tuple(workflows))


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
    workers: Sequence[subprocess.Popen[bytes]], reached: Callable[[], bool],
) -> str | None:
    """`None` once `reached` holds; the reason when a worker exited first or
    `TERMINAL_WAIT` passed."""
    deadline = time.monotonic() + TERMINAL_WAIT.total_seconds()
    while not reached():
        exited = [
            f'worker {position} exited with {worker.returncode} first'
            for position, worker in enumerate(workers)
            if worker.poll() is not None
        ]
        if exited:
            return '; '.join(exited)
        if time.monotonic() > deadline:
            return f'not every task and workflow ended within {TERMINAL_WAIT}'
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
class StartedWorker:
    process: subprocess.Popen[bytes]
    log_path: Path


def _start_workers(
    worker_command: list[str], scenario: Scenario, env: dict[str, str], log_dir: Path,
) -> list[StartedWorker]:
    """One worker per profile of `scenario`, each with its own log file."""
    started: list[StartedWorker] = []
    for position, profile in enumerate(scenario.profiles):
        log_path = log_dir / f'worker-{position}.log'
        with log_path.open('wb') as log:
            process = subprocess.Popen(
                [*worker_command, f'--profile={profile}', '--loglevel=info'],
                stdout=log,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                env=env,
                cwd=REPO_ROOT,
            )
        started.append(StartedWorker(process=process, log_path=log_path))
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


def _tails(workers: Sequence[StartedWorker]) -> str:
    return '\n'.join(
        f'--- worker {position}\n{_tail(worker.log_path)}'
        for position, worker in enumerate(workers)
    )


def run_side(
    client: Client, database: DualRunDatabase, scenario: Scenario,
) -> Result[SideRun, SideFailed]:
    """Run `scenario` with `client` on the empty `database`. Leaves no
    process behind."""
    worker_command, send_command = _commands(client)
    env = _environment(database)
    log_dir = Path(tempfile.mkdtemp(prefix=f'parity-{client.value.lower()}-'))
    workers: list[StartedWorker] = []
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
                        workers = _start_workers(worker_command, scenario, env, log_dir)
                        sent_ids = _run_send(send_command, scenario, env)
                    case WorkerStart.AFTER_ACTIONS:
                        sent_ids = _run_send(send_command, scenario, env)
                        workers = _start_workers(worker_command, scenario, env, log_dir)
                match sent_ids:
                    case Ok(sent):
                        pass
                    case Err(detail):
                        return Err(SideFailed(client=client, detail=detail))
                waited = _wait_until(
                    [worker.process for worker in workers],
                    lambda: _all_ended(connection, sent),
                )
            finally:
                for worker in workers:
                    _stop(worker.process)
            if waited is not None:
                return Err(SideFailed(client=client, detail=f'{waited}\n{_tails(workers)}'))
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
            report = read_report(connection, sent)
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
