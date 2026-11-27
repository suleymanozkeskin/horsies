"""Parity runs: one scenario on the Python client (the reference) and on a
candidate client, each on its own fresh database, and the compare of what
each left. The candidates are the Rust client, and the TypeScript client on
Node and on Bun (`CANDIDATES`).

The client families are not interoperable: two sides never share a
database. One side, on an empty `dualrun_` database:

1. Read the database clock.
2. Start one worker of the side per profile of the scenario, and the
   scheduler of the side for a scenario with `ScheduleSlots`, before the
   send process or after it (`WorkerStart`). The first worker starts
   alone; the others start once it logged `WORKER_READY_LINE`, and the
   start ends once each of them logged it too. So the sends of a
   `BEFORE_SENDS` scenario start after every worker of the side ended its
   own start.
   The first operation of the side installs the schema, so the client
   family row is the side's.
3. Run the send process of the side (`python -m crossclient.parity.send`,
   `horsies-parity send`, or `<node|bun> <TypeScript entry> send`): it
   sends the tasks, reruns tasks, starts the workflows, runs the actions on
   them and pings, and prints the ids of the sends, reruns and starts in
   order, and the counts of the ping answers.
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
   (`crossclient/parity/snapshot/worker_run.py`), and read the report: the task of each
   send and rerun, each node of each started workflow, the status of each
   started workflow, the ping answers, the run count of the schedule and
   each slot task, keyed by position.

The compare: the snapshots must be equal except for the planned difference,
the `family` of `horsies_client_family` (`PYTHON` against the family of the
candidate: `RUST` or `TYPESCRIPT`); the reports must be equal. A candidate
row with the value of another family is an unplanned difference. The worker
health tables are not in the compare (`UNCOMPARED_TABLES`): the ping answers
in the report are the compare of a health scenario.

A side runs only the scenarios that it offers (`scenario_offered`): the
TypeScript side on Bun does not offer a scenario with `SHARED_THREAD`
children, because a TypeScript worker on Bun refuses them at its start.

The other snapshot differences are judged by the hour rule of the whole-run
compare (`judge_snapshots`): a `DualRunMismatch` when the run spans of both
sides lie in one UTC hour, a `ClockBoundaryStraddle` when they do not. The
install and the worker of each side create the hourly heartbeat leaves from
their own hour on, and a leaf name is read relative to the hour of the run
window, so two sides across an hour can differ by a leaf for the clock
alone. The two sides run one after the other, so their spans can lie in two
hours.
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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Final, LiteralString
from uuid import UUID

import psycopg

from horsies.core.models.worker_profile import NoSharedThreadChildren, SharedThreadChildren
from horsies.core.types.result import Err, Ok, Result
from crossclient.parity import app as parity
from crossclient.parity.snapshot.capture import capture_snapshot, read_database_clock
from crossclient.parity.snapshot.compare import (
    ColumnDifference,
    DiffEntry,
    SnapshotDiff,
    compare_snapshots,
)
from crossclient.parity.snapshot.databases import DualRunDatabase
from crossclient.parity.snapshot.normalize import NormalizedSnapshot
from crossclient.parity.snapshot.values import ExactValue, RunWindow, make_run_window
from crossclient.parity.snapshot.worker_run import (
    WHOLE_RUN_CATALOG,
    PairJudgement,
    compared_part,
    judge_snapshots,
)
from crossclient.parity.app import DATABASE_URL_ENV
from crossclient.parity.scenarios import NoScheduler, Scenario, ScheduleSlots, WorkerStart

REPO_ROOT: Final = Path(__file__).resolve().parents[2]

RUST_BINARY_ENV: Final = 'HORSIES_PARITY_RUST_BIN'
"""The path of the Rust scenario binary; default `target/debug/horsies-parity`."""

RUST_BUILD: Final = 'cargo build -p horsies-parity-tests'
"""The command that builds the Rust scenario binary."""

TYPESCRIPT_ENTRY_ENV: Final = 'HORSIES_PARITY_TS_ENTRY'
"""The entry of the TypeScript parity program: an absolute path, or a path
relative to the repository root; default `TYPESCRIPT_ENTRY_DEFAULT`."""

TYPESCRIPT_ENTRY_DEFAULT: Final = Path('clients/typescript/tests/parity/src/main.ts')

NODE_BINARY_ENV: Final = 'HORSIES_PARITY_NODE_BIN'
"""The Node program of the TypeScript side on Node; default `node` on `PATH`."""

BUN_BINARY_ENV: Final = 'HORSIES_PARITY_BUN_BIN'
"""The Bun program of the TypeScript side on Bun; default `bun` on `PATH`."""

NATIVE_ADDON_PATH_ENV: Final = 'HORSIES_NATIVE_ADDON_PATH'
"""The dev build of the addon that the main thread of each process of the
TypeScript parity program loads: the absolute path of its `.node` file."""

NATIVE_ADDON_BUILD: Final = 'bun run --cwd clients/typescript/bridge build:dev'
"""The command that builds the dev addon
(`clients/typescript/bridge/horsies.<platform>.node`)."""

PYTHON_APP_LOCATOR: Final = 'crossclient.parity.app:app'

TERMINAL_WAIT: Final = timedelta(seconds=120)
WORKER_READY_LINE: Final = 'worker ready'
"""The line that the core worker logs once its startup coverage and its
children are done (`core/src/worker/worker.rs`)."""
READY_LOG_BYTES: Final = 1_000_000
"""The tail of a worker log that the ready check reads."""
TERMINAL_POLL: Final = timedelta(milliseconds=250)
SEND_WAIT: Final = timedelta(seconds=60)
STOP_WAIT: Final = timedelta(seconds=60)
KILL_WAIT: Final = timedelta(seconds=10)

PLANNED_TABLE: Final = 'horsies_client_family'
PLANNED_COLUMN: Final = 'family'


class Family(Enum):
    """The client family of one side: the stored value of
    `horsies_client_family.family`."""

    PYTHON = 'PYTHON'
    RUST = 'RUST'
    TYPESCRIPT = 'TYPESCRIPT'


class JsRuntime(Enum):
    """The JavaScript runtime of a TypeScript side."""

    NODE = 'node'
    BUN = 'bun'


@dataclass(frozen=True, slots=True)
class PythonSide:
    """The Python client with the parity app of `crossclient/parity/app.py`:
    the reference of every compare."""


@dataclass(frozen=True, slots=True)
class RustSide:
    """The Rust client with the scenario binary of `clients/rust/tests/parity`."""


@dataclass(frozen=True, slots=True)
class TypeScriptSide:
    """The TypeScript client on `runtime`, with the parity program of
    `clients/typescript/tests/parity`."""

    runtime: JsRuntime


type CandidateSide = RustSide | TypeScriptSide
type Side = PythonSide | CandidateSide

REFERENCE: Final = PythonSide()
"""The side that each candidate is compared with."""

CANDIDATES: Final[tuple[CandidateSide, ...]] = (
    RustSide(),
    TypeScriptSide(runtime=JsRuntime.NODE),
    TypeScriptSide(runtime=JsRuntime.BUN),
)
"""The sides that each run against the reference, in this order."""


def family_of(side: Side) -> Family:
    """The family value that the client of `side` stores. Pure."""
    match side:
        case PythonSide():
            return Family.PYTHON
        case RustSide():
            return Family.RUST
        case TypeScriptSide():
            return Family.TYPESCRIPT


def side_name(side: Side) -> str:
    """The name of `side` in test ids and log folders: `python`, `rust`,
    `typescript-node` or `typescript-bun`. Pure."""
    match side:
        case PythonSide():
            return 'python'
        case RustSide():
            return 'rust'
        case TypeScriptSide(runtime=runtime):
            return f'typescript-{runtime.value}'


def rust_binary(environment: Mapping[str, str]) -> Path:
    """The Rust scenario binary: `HORSIES_PARITY_RUST_BIN` of `environment`,
    else the debug build of `horsies-parity-tests`. Pure."""
    match environment.get(RUST_BINARY_ENV):
        case str() as path if path:
            return Path(path)
        case _:
            return REPO_ROOT / 'target' / 'debug' / 'horsies-parity'


def typescript_entry(environment: Mapping[str, str]) -> Path:
    """The entry of the TypeScript parity program: `HORSIES_PARITY_TS_ENTRY`
    of `environment`, else `TYPESCRIPT_ENTRY_DEFAULT`, against the
    repository root when it is not absolute. Pure."""
    match environment.get(TYPESCRIPT_ENTRY_ENV):
        case str() as path if path:
            return REPO_ROOT / path
        case _:
            return REPO_ROOT / TYPESCRIPT_ENTRY_DEFAULT


def _runtime_env(runtime: JsRuntime) -> str:
    """The variable that names the program of `runtime`. Pure."""
    match runtime:
        case JsRuntime.NODE:
            return NODE_BINARY_ENV
        case JsRuntime.BUN:
            return BUN_BINARY_ENV


def runtime_program(runtime: JsRuntime, environment: Mapping[str, str]) -> str:
    """The program of `runtime`: the variable of `_runtime_env` in
    `environment`, else the runtime's name on `PATH`. Pure."""
    match environment.get(_runtime_env(runtime)):
        case str() as program if program:
            return program
        case _:
            return runtime.value


@dataclass(frozen=True, slots=True)
class ProgramPresent:
    """Each program that the side runs exists."""


@dataclass(frozen=True, slots=True)
class ProgramMissing:
    """A program of the side does not exist. `hint` names each missing part
    and the command or the variable that gives it."""

    hint: str


type SideProgram = ProgramPresent | ProgramMissing


def side_program(side: Side, environment: Mapping[str, str]) -> SideProgram:
    """Whether the programs of `side` exist, with the variables of
    `environment`. The Python side runs in this interpreter. Reads the file
    system and `PATH`; starts nothing."""
    match side:
        case PythonSide():
            return ProgramPresent()
        case RustSide():
            binary = rust_binary(environment)
            if binary.is_file():
                return ProgramPresent()
            return ProgramMissing(
                hint=f'the Rust scenario binary {binary} does not exist: {RUST_BUILD}, '
                f'or set {RUST_BINARY_ENV}',
            )
        case TypeScriptSide(runtime=runtime):
            missing = _typescript_missing(runtime, environment)
            if not missing:
                return ProgramPresent()
            return ProgramMissing(hint='; '.join(missing))


def _typescript_missing(runtime: JsRuntime, environment: Mapping[str, str]) -> list[str]:
    """The missing parts of the TypeScript side on `runtime`: the runtime
    program, the entry and the dev addon. Reads the file system and `PATH`."""
    missing: list[str] = []
    program = runtime_program(runtime, environment)
    if shutil.which(program) is None:
        missing.append(
            f'the {runtime.value} program {program!r} is not found: put {runtime.value} on PATH, '
            f'or set {_runtime_env(runtime)}',
        )
    entry = typescript_entry(environment)
    if not entry.is_file():
        missing.append(f'the TypeScript parity entry {entry} does not exist: set {TYPESCRIPT_ENTRY_ENV}')
    addon = environment.get(NATIVE_ADDON_PATH_ENV, '')
    if not (Path(addon).is_absolute() and Path(addon).is_file()):
        missing.append(
            f'{NATIVE_ADDON_PATH_ENV} ({addon!r}) names no dev build of the addon: {NATIVE_ADDON_BUILD}, '
            f'then set {NATIVE_ADDON_PATH_ENV} to the absolute path of the built .node file',
        )
    return missing


@dataclass(frozen=True, slots=True)
class Offered:
    """The side runs the scenario."""


@dataclass(frozen=True, slots=True)
class NotOffered:
    """The side does not run the scenario; `reason` names the profile and
    the rule."""

    reason: str


type ScenarioOffer = Offered | NotOffered


def scenario_offered(side: Side, scenario: Scenario) -> ScenarioOffer:
    """Whether `side` runs `scenario`. Only the TypeScript side on Bun leaves
    scenarios out: a TypeScript worker on Bun refuses a profile with
    `SHARED_THREAD` children at its start, because Bun does not cap the heap
    of a worker thread (`resourceLimits`). Pure."""
    match side:
        case PythonSide() | RustSide():
            return Offered()
        case TypeScriptSide(runtime=runtime):
            match runtime:
                case JsRuntime.NODE:
                    return Offered()
                case JsRuntime.BUN:
                    return _bun_offer(scenario)


def _bun_offer(scenario: Scenario) -> ScenarioOffer:
    """`NotOffered` when a profile of `scenario` has `SHARED_THREAD` children
    in the parity app. Pure."""
    refused = [profile for profile in scenario.profiles if _has_shared_thread_children(profile)]
    if not refused:
        return Offered()
    return NotOffered(
        reason=f'{scenario.name} runs the profile {", ".join(refused)} with SHARED_THREAD '
        'children; a TypeScript worker on Bun refuses SHARED_THREAD children at its start, '
        'because Bun does not cap the heap of a worker thread',
    )


def _has_shared_thread_children(profile: str) -> bool:
    """Whether the profile `profile` of the parity app starts `SHARED_THREAD`
    children. An unknown profile is a defect of the scenario table: `KeyError`.
    Pure."""
    match parity.config.worker_profiles[profile].shared_thread:
        case SharedThreadChildren():
            return True
        case NoSharedThreadChildren():
            return False


@dataclass(frozen=True, slots=True)
class SideCommands:
    """The commands of one side, before their arguments."""

    worker: list[str]
    send: list[str]
    scheduler: list[str]


def side_commands(side: Side, environment: Mapping[str, str]) -> SideCommands:
    """The worker, send and scheduler commands of `side`, with the variables
    of `environment`. Pure."""
    match side:
        case PythonSide():
            return SideCommands(
                worker=[sys.executable, '-m', 'horsies.core.cli', 'worker', PYTHON_APP_LOCATOR],
                send=[sys.executable, '-m', 'crossclient.parity.send'],
                scheduler=[
                    sys.executable, '-m', 'horsies.core.cli', 'scheduler', PYTHON_APP_LOCATOR,
                ],
            )
        case RustSide():
            binary = str(rust_binary(environment))
            return SideCommands(
                worker=[binary, 'worker'], send=[binary, 'send'], scheduler=[binary, 'scheduler'],
            )
        case TypeScriptSide(runtime=runtime):
            program = runtime_program(runtime, environment)
            entry = str(typescript_entry(environment))
            return SideCommands(
                worker=[program, entry, 'worker'],
                send=[program, entry, 'send'],
                scheduler=[program, entry, 'scheduler'],
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
_SCHEDULE_TABLE_SQL: Final[LiteralString] = (
    "SELECT to_regclass('horsies_schedule_state') IS NOT NULL"
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
    """What one side left: the comparable snapshot and the report, and the
    span of its run, from the first clock reading to the last."""

    side: Side
    snapshot: NormalizedSnapshot
    report: dict[ReportKey, ReportEntry]
    span: RunWindow


@dataclass(frozen=True, slots=True)
class SideFailed:
    """One side did not reach its stop point or its capture; `detail` names
    the step and holds the worker's last output."""

    side: Side
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
            [*command, '--log-level=info'],
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
            cwd=REPO_ROOT,
        )
    return StartedProcess(name=name, process=process, log_path=log_path)


@dataclass(frozen=True, slots=True)
class WorkersReady:
    """Each started worker of the side logged `WORKER_READY_LINE`."""


@dataclass(frozen=True, slots=True)
class WorkerNotReady:
    """A worker did not log `WORKER_READY_LINE`: a process of the side
    exited first, or `TERMINAL_WAIT` passed. `detail` names the worker and
    the cause."""

    detail: str


type Readiness = WorkersReady | WorkerNotReady


@dataclass(frozen=True, slots=True)
class StartedSide:
    """The processes that the start of a side left running, and whether
    each worker got ready. After `WorkerNotReady` no later process of the
    side started."""

    processes: tuple[StartedProcess, ...]
    readiness: Readiness


def _logged_ready(worker: StartedProcess) -> bool:
    return WORKER_READY_LINE in _tail(worker.log_path, limit=READY_LOG_BYTES)


def _wait_ready(
    running: Sequence[StartedProcess], workers: Sequence[StartedProcess],
) -> Readiness:
    """`WorkersReady` once each of `workers` logged `WORKER_READY_LINE`;
    `WorkerNotReady` when a process of `running` exited first, or
    `TERMINAL_WAIT` passed for one worker."""
    for worker in workers:
        match _wait_until(running, partial(_logged_ready, worker), f'{worker.name} ready'):
            case None:
                pass
            case str() as cause:
                return WorkerNotReady(detail=f'{worker.name} not ready: {cause}')
    return WorkersReady()


def _start_processes(
    commands: SideCommands, scenario: Scenario, env: dict[str, str], log_dir: Path,
) -> StartedSide:
    """One worker per profile of `scenario`, then the scheduler of a
    scenario with `ScheduleSlots`, each with its own log file.

    The first worker starts alone, and the others start once its log shows
    `WORKER_READY_LINE`: on a fresh database a worker whose startup coverage
    meets the coverage gate of another starting worker waits for the gate
    with its start backoff (`coverage_gate_busy`), on both clients. The
    others start together,
    and the scheduler starts once each of them logged `WORKER_READY_LINE`
    too. A worker that does not get ready ends the start with
    `WorkerNotReady`."""
    first, *others = scenario.profiles
    leader = _start_process('worker-0', [*commands.worker, f'--profile={first}'], env, log_dir)
    match _wait_ready([leader], [leader]):
        case WorkersReady():
            pass
        case WorkerNotReady() as not_ready:
            return StartedSide(processes=(leader,), readiness=not_ready)
    followers = tuple(
        _start_process(f'worker-{position}', [*commands.worker, f'--profile={profile}'], env, log_dir)
        for position, profile in enumerate(others, start=1)
    )
    workers = (leader, *followers)
    match _wait_ready(workers, followers):
        case WorkersReady():
            pass
        case WorkerNotReady() as not_ready:
            return StartedSide(processes=workers, readiness=not_ready)
    match scenario.scheduler:
        case ScheduleSlots():
            scheduler = _start_process('scheduler', commands.scheduler, env, log_dir)
            return StartedSide(processes=(*workers, scheduler), readiness=WorkersReady())
        case NoScheduler():
            return StartedSide(processes=workers, readiness=WorkersReady())


def _send_when_ready(
    readiness: Readiness, send_command: list[str], scenario: Scenario, env: dict[str, str],
) -> Result[SentIds, str]:
    """The send of a `BEFORE_SENDS` scenario: it runs only once each worker
    of the side got ready."""
    match readiness:
        case WorkersReady():
            return _run_send(send_command, scenario, env)
        case WorkerNotReady(detail=detail):
            return Err(detail)


def _sent_then_ready(sent: Result[SentIds, str], readiness: Readiness) -> Result[SentIds, str]:
    """The send of an `AFTER_ACTIONS` scenario and the readiness of the
    workers that started after it: the earlier failure of the two."""
    match sent, readiness:
        case (Err(), _) | (Ok(), WorkersReady()):
            return sent
        case (Ok(), WorkerNotReady(detail=detail)):
            return Err(detail)


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


def _schedule_state_written(connection: psycopg.Connection[tuple[object, ...]]) -> bool:
    """The state row of the schedule exists. Before a process of the side
    installed the schema, the table does not exist, and no row is written."""
    match connection.execute(_SCHEDULE_TABLE_SQL).fetchone():
        case (True,):
            return connection.execute(
                _SCHEDULE_STATE_SQL, (parity.SCHEDULE_NAME,),
            ).fetchone() is not None
        case (False,):
            return False
        case other:
            raise AssertionError(f'to_regclass of the schedule state table returned {other!r}')


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
        lambda: _schedule_state_written(connection),
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
    side: Side, database: DualRunDatabase, scenario: Scenario,
) -> Result[SideRun, SideFailed]:
    """Run `scenario` with the client of `side` on the empty `database`.
    Leaves no process behind."""
    env = _environment(database)
    commands = side_commands(side, env)
    log_dir = Path(tempfile.mkdtemp(prefix=f'parity-{side_name(side)}-'))
    processes: list[StartedProcess] = []
    try:
        with psycopg.connect(database.url, autocommit=True) as connection:
            match read_database_clock(connection):
                case Ok(started):
                    pass
                case Err(failed):
                    return Err(SideFailed(side=side, detail=f'clock: {failed}'))
            try:
                match scenario.worker_start:
                    case WorkerStart.BEFORE_SENDS:
                        started_side = _start_processes(commands, scenario, env, log_dir)
                        processes = list(started_side.processes)
                        sent_ids = _send_when_ready(
                            started_side.readiness, commands.send, scenario, env,
                        )
                    case WorkerStart.AFTER_ACTIONS:
                        sent_first = _run_send(commands.send, scenario, env)
                        started_side = _start_processes(commands, scenario, env, log_dir)
                        processes = list(started_side.processes)
                        sent_ids = _sent_then_ready(sent_first, started_side.readiness)
                match sent_ids:
                    case Ok(sent):
                        pass
                    case Err(detail):
                        return Err(SideFailed(side=side, detail=f'{detail}\n{_tails(processes)}'))
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
                                    SideFailed(side=side, detail=f'{detail}\n{_tails(processes)}'),
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
                return Err(SideFailed(side=side, detail=f'{waited}\n{_tails(processes)}'))
            match read_database_clock(connection):
                case Ok(ended):
                    pass
                case Err(failed):
                    return Err(SideFailed(side=side, detail=f'clock: {failed}'))
            match make_run_window(started, ended):
                case Ok(window):
                    pass
                case Err(invalid):
                    return Err(SideFailed(side=side, detail=f'run window: {invalid}'))
            match capture_snapshot(connection, WHOLE_RUN_CATALOG, window):
                case Ok(snapshot):
                    pass
                case Err(capture_error):
                    return Err(SideFailed(side=side, detail=f'capture: {capture_error}'))
            report = read_report(connection, sent, schedule)
        return Ok(
            SideRun(
                side=side,
                snapshot=compared_part(snapshot),
                report=report,
                span=window,
            ),
        )
    finally:
        shutil.rmtree(log_dir)


# ---- The compare ----


def _is_planned(entry: DiffEntry, reference: Family, candidate: Family) -> bool:
    """The planned difference: the client family row records the family of
    each side, `reference` on the reference side and `candidate` on the
    candidate side. A row with any other value is not planned. Pure."""
    match entry:
        case ColumnDifference(
            table=table,
            column=column,
            reference=ExactValue(text=reference_text),
            candidate=ExactValue(text=candidate_text),
        ):
            return (
                table == PLANNED_TABLE
                and column == PLANNED_COLUMN
                and reference_text == reference.value
                and candidate_text == candidate.value
            )
        case _:
            return False


@dataclass(frozen=True, slots=True)
class ParityResult:
    """The judgement of the unplanned snapshot differences, whether the
    planned difference was seen once, and the report keys whose entries
    differ, with both entries."""

    snapshots: PairJudgement
    planned_seen: bool
    report_differences: tuple[tuple[ReportKey, ReportEntry | None, ReportEntry | None], ...]


def compare_sides(reference: SideRun, candidate: SideRun) -> ParityResult:
    """Compare the reference side (the Python side) with a candidate side.
    The planned difference is the client family row with the family of each
    side (`family_of`). The snapshot differences other than the planned one
    are judged by `judge_snapshots` on the run spans of both sides; no entry
    is left out. Pure."""
    families = (family_of(reference.side), family_of(candidate.side))
    diff = compare_snapshots(reference.snapshot, candidate.snapshot)
    planned = tuple(entry for entry in diff.entries if _is_planned(entry, *families))
    unplanned = SnapshotDiff(
        entries=tuple(entry for entry in diff.entries if not _is_planned(entry, *families)),
    )
    keys = sorted(set(reference.report) | set(candidate.report), key=str)
    report_differences = tuple(
        (key, reference.report.get(key), candidate.report.get(key))
        for key in keys
        if reference.report.get(key) != candidate.report.get(key)
    )
    return ParityResult(
        snapshots=judge_snapshots(unplanned, reference.span, candidate.span),
        planned_seen=len(planned) == 1,
        report_differences=report_differences,
    )
