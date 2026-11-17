"""Whole-run pairs of the worker: the reference worker and the candidate worker
run as processes, each on a fresh ``dualrun_`` database, with the same app,
profile and sends. Their snapshots and their per-task reports must be equal.

One side of a pair:

1. Write the sends: the task ids and the ``sent_at`` of the scenario, which
   both sides share, and the stored options of each registered task
   (``enqueue_async``, the write of every send). Start the workflows of the
   scenario with their fixed ids (``start_workflow_async``). Apply the row
   edits of the scenario (stored options without a key, a stored class that
   does not fit the registered task).
2. Read the database clock, then start the worker of the side with the
   profile of the scenario and a new marker directory.
3. Wait for the stop point of the scenario, at most ``TERMINAL_WAIT``:
   - ``StopWhenTerminal``: every sent task and every task of the started
     workflows has its history row, and every started workflow is
     COMPLETED, FAILED, CANCELLED or PAUSED;
   - ``StopWhenRunning``: every sent task is RUNNING, so the stop arrives
     while they run;
   - ``PauseWhenRunning``: the first node writes its marker; the runner
     pauses the workflow and writes the release marker, then waits as
     ``StopWhenTerminal``.
4. Stop the worker with ``SIGTERM`` and wait at most ``STOP_WAIT`` for its
   exit. A worker that does not exit is killed with its process group.
5. Read the clock again, then capture the snapshot and the report: each
   sent task, each node of each started workflow, and the status of each
   started workflow. Every sent task must be terminal at this point.

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
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Final, LiteralString
from uuid import UUID

import psycopg

from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.task_decorator import effective_priority
from horsies.core.models.workflow import WorkflowSpec
from horsies.core.types.result import Err, Ok, Result
from horsies.core.workflows.lifecycle import pause_workflow, start_workflow_async
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
class WorkflowStart:
    """One workflow start with its fixed id, after the sends. The ``n``-th
    start has ``sent_at`` ``first_sent_at + (sends + n) ms``."""

    spec: WorkflowSpec[Any]
    workflow_id: UUID


@dataclass(frozen=True, slots=True)
class DropOptionKey:
    """Remove ``key`` from the stored options of send ``send``, after the
    send: options that the worker cannot use."""

    send: int
    key: str


@dataclass(frozen=True, slots=True)
class SetStoredClass:
    """Set the stored execution class of send ``send`` to ``execution``,
    after the send: a class that the registered task does not have."""

    send: int
    execution: str


type RowEdit = DropOptionKey | SetStoredClass


@dataclass(frozen=True, slots=True)
class StopWhenTerminal:
    """Stop once every task is terminal and every workflow has ended or
    paused."""


@dataclass(frozen=True, slots=True)
class StopWhenRunning:
    """Stop once every sent task is RUNNING; the drain ends them."""


@dataclass(frozen=True, slots=True)
class PauseWhenRunning:
    """Pause ``workflow_id`` once marker ``running`` exists, then write
    marker ``release``; stop as ``StopWhenTerminal``."""

    workflow_id: UUID
    running: str
    release: str


type StopRule = StopWhenTerminal | StopWhenRunning | PauseWhenRunning


@dataclass(frozen=True, slots=True)
class Scenario:
    """The profile, the sends, the workflow starts, the row edits and the
    stop rule of one pair. The ``n``-th send has ``sent_at``
    ``first_sent_at + n ms`` on both sides."""

    name: str
    profile: str
    sends: tuple[Send, ...]
    first_sent_at: datetime
    workflows: tuple[WorkflowStart, ...]
    row_edits: tuple[RowEdit, ...]
    stop: StopRule


def build_scenario(
    name: str,
    profile: str,
    sends: Sequence[tuple[object, str | None]],
    *,
    workflows: Sequence[WorkflowSpec[Any]] = (),
    row_edits: Sequence[RowEdit] = (),
    stop: StopRule = StopWhenTerminal(),
) -> Scenario:
    """A scenario of ``(registered task, input JSON)`` sends: each gets a
    task id minted now, each workflow a workflow id minted now, and the
    first ``sent_at`` is now. Both sides of the pair use these values."""
    return Scenario(
        name=name,
        profile=profile,
        sends=tuple(
            send_of(task, mint_task_id(), input_json) for task, input_json in sends
        ),
        first_sent_at=datetime.now(UTC),
        workflows=tuple(
            WorkflowStart(spec=spec, workflow_id=mint_task_id()) for spec in workflows
        ),
        row_edits=tuple(row_edits),
        stop=stop,
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
class NodeKey:
    """A node of a started workflow: its workflow id and task index."""

    workflow_id: UUID
    task_index: int


@dataclass(frozen=True, slots=True)
class WorkflowKey:
    """The workflow row of a started workflow."""

    workflow_id: UUID


@dataclass(frozen=True, slots=True)
class NoTaskRow:
    """The node has no task: a sub-workflow node, or a node that never
    became ready."""


@dataclass(frozen=True, slots=True)
class NodeReport:
    status: str
    task: TaskReport | NoTaskRow


@dataclass(frozen=True, slots=True)
class WorkflowReport:
    status: str


type ReportKey = UUID | NodeKey | WorkflowKey
type ReportEntry = TaskReport | NodeReport | WorkflowReport


@dataclass(frozen=True, slots=True)
class ReportDifference:
    key: ReportKey
    reference: ReportEntry
    candidate: ReportEntry


def compare_reports(
    reference: dict[ReportKey, ReportEntry],
    candidate: dict[ReportKey, ReportEntry],
) -> tuple[ReportDifference, ...]:
    """Every entry whose report differs, in key text order. Both reports
    cover the same sends and workflows (asserted). Pure."""
    assert reference.keys() == candidate.keys(), 'the two reports cover other entries'
    return tuple(
        ReportDifference(key=key, reference=reference[key], candidate=candidate[key])
        for key in sorted(reference, key=str)
        if reference[key] != candidate[key]
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
        for position, start in enumerate(scenario.workflows, start=len(scenario.sends)):
            started = await start_workflow_async(
                start.spec,
                broker,
                str(start.workflow_id),
                scenario.first_sent_at + timedelta(milliseconds=position),
            )
            match started:
                case Ok(_handle):
                    pass
                case Err(refused):
                    raise AssertionError(
                        f'the start of workflow {start.workflow_id} was refused: {refused}'
                    )


_DROP_OPTION_KEY_SQL: Final[LiteralString] = (
    'UPDATE horsies_tasks SET task_options = (task_options::jsonb - %s)::text '
    'WHERE id = %s'
)
_SET_STORED_CLASS_SQL: Final[LiteralString] = (
    'UPDATE horsies_tasks SET execution = %s WHERE id = %s'
)


def _apply_row_edits(connection: Connection, scenario: Scenario) -> None:
    """Apply the row edits of ``scenario`` to its sent rows, before any
    worker runs. Each edit changes exactly one row (asserted)."""
    for edit in scenario.row_edits:
        match edit:
            case DropOptionKey(send=send, key=key):
                changed = connection.execute(
                    _DROP_OPTION_KEY_SQL, (key, scenario.sends[send].task_id)
                ).rowcount
            case SetStoredClass(send=send, execution=execution):
                changed = connection.execute(
                    _SET_STORED_CLASS_SQL, (execution, scenario.sends[send].task_id)
                ).rowcount
        assert changed == 1, f'{edit!r} changed {changed} rows'


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
    report: dict[ReportKey, ReportEntry]
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
class PauseRefused:
    """The pause of a ``PauseWhenRunning`` scenario did not pause."""

    side: str
    detail: str
    worker: WorkerEnd


@dataclass(frozen=True, slots=True)
class CaptureFailed:
    side: str
    cause: CaptureError | ClockReadFailed | RunWindowInvalid


type SideFailure = TasksNotTerminal | WorkerExitedEarly | PauseRefused | CaptureFailed


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
    marker_dir: str,
) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env['PYTHONPATH'] = REPO_ROOT
    env[whole_run_app.DATABASE_URL_ENV] = sqlalchemy_url(database)
    env[whole_run_app.MARKER_DIR_ENV] = marker_dir
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


_WORKFLOW_TASK_IDS_SQL: Final[LiteralString] = (
    'SELECT wt.task_id FROM horsies_workflow_tasks wt '
    'JOIN horsies_workflows w ON w.id = wt.workflow_id '
    'WHERE (w.id = ANY(%s) OR w.root_workflow_id = ANY(%s)) AND wt.task_id IS NOT NULL'
)
_OPEN_WORKFLOWS_SQL: Final[LiteralString] = (
    'SELECT count(*) FROM horsies_workflows '
    'WHERE (id = ANY(%s) OR root_workflow_id = ANY(%s)) '
    "AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED', 'PAUSED')"
)
_RUNNING_SQL: Final[LiteralString] = (
    "SELECT count(*) FROM horsies_tasks WHERE id = ANY(%s) AND status = 'RUNNING'"
)
_NODES_SQL: Final[LiteralString] = (
    'SELECT workflow_id, task_index, status, task_id FROM horsies_workflow_tasks '
    'WHERE workflow_id = ANY(%s) ORDER BY workflow_id, task_index'
)
_WORKFLOWS_SQL: Final[LiteralString] = (
    'SELECT id, status FROM horsies_workflows WHERE id = ANY(%s)'
)


def _count(connection: Connection, sql: LiteralString, parameters: tuple[object, ...]) -> int:
    row = connection.execute(sql, parameters).fetchone()
    match row:
        case (int() as count,):
            return count
        case other:
            raise AssertionError(f'a count of the runner is {other!r}')


def _task_id_row(row: tuple[object, ...]) -> UUID:
    match row:
        case (UUID() as task_id,):
            return task_id
        case other:
            raise AssertionError(f'a workflow task id row is {other!r}')


def _workflow_ids(scenario: Scenario) -> list[UUID]:
    return [start.workflow_id for start in scenario.workflows]


def _all_ended(connection: Connection, scenario: Scenario) -> bool:
    """Every sent task and every task of the started workflows is terminal,
    and every started workflow (and child) ended or paused."""
    workflow_ids = _workflow_ids(scenario)
    workflow_tasks = [
        _task_id_row(row)
        for row in connection.execute(
            _WORKFLOW_TASK_IDS_SQL, (workflow_ids, workflow_ids)
        ).fetchall()
    ]
    task_ids = [send.task_id for send in scenario.sends] + workflow_tasks
    return (
        not _open_tasks(connection, task_ids)
        and _count(connection, _OPEN_WORKFLOWS_SQL, (workflow_ids, workflow_ids)) == 0
    )


def _all_running(connection: Connection, scenario: Scenario) -> bool:
    task_ids = [send.task_id for send in scenario.sends]
    return _count(connection, _RUNNING_SQL, (task_ids,)) == len(task_ids)


class WaitEnd(Enum):
    """How a wait of the runner ended."""

    REACHED = 'reached'
    BOUND_PASSED = 'bound passed'
    WORKER_EXITED = 'worker exited'


@dataclass(frozen=True, slots=True)
class PauseNotApplied:
    """The pause of a ``PauseWhenRunning`` scenario answered ``outcome``."""

    outcome: str


def _wait_until(
    process: subprocess.Popen[bytes],
    reached: Callable[[], bool],
    deadline: float,
) -> WaitEnd:
    """Poll ``reached`` until it holds, the monotonic ``deadline`` passes, or
    the worker exits."""
    while not reached():
        if process.poll() is not None:
            return WaitEnd.WORKER_EXITED
        if time.monotonic() > deadline:
            return WaitEnd.BOUND_PASSED
        time.sleep(TERMINAL_POLL.total_seconds())
    return WaitEnd.REACHED


async def _pause(database: DualRunDatabase, workflow_id: UUID) -> str:
    """Pause ``workflow_id``; the text of the outcome, ``Ok(True)`` when it
    paused."""
    async with open_broker(database) as broker:
        return repr(await pause_workflow(broker, str(workflow_id)))


def _wait_for_stop_point(
    connection: Connection,
    process: subprocess.Popen[bytes],
    scenario: Scenario,
    database: DualRunDatabase,
    marker_dir: str,
) -> WaitEnd | PauseNotApplied:
    """Wait for the stop point of ``scenario``, at most ``TERMINAL_WAIT``."""
    deadline = time.monotonic() + TERMINAL_WAIT.total_seconds()
    match scenario.stop:
        case StopWhenTerminal():
            return _wait_until(process, lambda: _all_ended(connection, scenario), deadline)
        case StopWhenRunning():
            return _wait_until(process, lambda: _all_running(connection, scenario), deadline)
        case PauseWhenRunning(workflow_id=workflow_id, running=running, release=release):
            running_path = os.path.join(marker_dir, running)
            waited = _wait_until(process, lambda: os.path.exists(running_path), deadline)
            if waited is not WaitEnd.REACHED:
                return waited
            paused = asyncio.run(_pause(database, workflow_id))
            if paused != repr(Ok(True)):
                return PauseNotApplied(outcome=paused)
            with open(os.path.join(marker_dir, release), 'w', encoding='ascii') as marker:
                marker.write('released')
            return _wait_until(process, lambda: _all_ended(connection, scenario), deadline)


def _node_row(row: tuple[object, ...]) -> tuple[UUID, int, str, UUID | None]:
    match row:
        case (UUID() as workflow_id, int() as index, str() as status, UUID() | None as task_id):
            return workflow_id, index, status, task_id
        case other:
            raise AssertionError(f'a node row of the report is {other!r}')


def _workflow_row(row: tuple[object, ...]) -> tuple[UUID, str]:
    match row:
        case (UUID() as workflow_id, str() as status):
            return workflow_id, status
        case other:
            raise AssertionError(f'a workflow row of the report is {other!r}')


def read_report(
    connection: Connection,
    scenario: Scenario,
) -> dict[ReportKey, ReportEntry]:
    """The report of every send, every node of every started workflow, and
    every started workflow. Raises ``psycopg.Error`` on a database error."""
    report: dict[ReportKey, ReportEntry] = {}
    for task_id, entry in read_task_report(
        connection, [send.task_id for send in scenario.sends]
    ).items():
        report[task_id] = entry
    workflow_ids = _workflow_ids(scenario)
    nodes = [
        _node_row(row)
        for row in connection.execute(_NODES_SQL, (workflow_ids,)).fetchall()
    ]
    node_tasks = read_task_report(
        connection, [task_id for _w, _i, _s, task_id in nodes if task_id is not None]
    )
    for workflow_id, index, status, task_id in nodes:
        task: TaskReport | NoTaskRow = (
            NoTaskRow() if task_id is None else node_tasks[task_id]
        )
        report[NodeKey(workflow_id=workflow_id, task_index=index)] = NodeReport(
            status=status, task=task
        )
    for row in connection.execute(_WORKFLOWS_SQL, (workflow_ids,)).fetchall():
        workflow_id, status = _workflow_row(row)
        report[WorkflowKey(workflow_id=workflow_id)] = WorkflowReport(status=status)
    return report


def run_side(
    command: WorkerCommand,
    database: DualRunDatabase,
    scenario: Scenario,
) -> Result[SideRun, SideFailure]:
    """Run one side of ``scenario`` on ``database``, whose baseline is
    installed. Leaves no worker process and no marker directory behind."""
    asyncio.run(_write_sends(database, scenario))
    task_ids = [send.task_id for send in scenario.sends]
    log_fd, log_path = tempfile.mkstemp(
        prefix=f'whole-run-{command.name}-', suffix='.log'
    )
    os.close(log_fd)
    marker_dir = tempfile.mkdtemp(prefix=f'whole-run-{command.name}-markers-')
    try:
        return _run_side_in(command, database, scenario, task_ids, log_path, marker_dir)
    finally:
        shutil.rmtree(marker_dir)


def _run_side_in(
    command: WorkerCommand,
    database: DualRunDatabase,
    scenario: Scenario,
    task_ids: list[UUID],
    log_path: str,
    marker_dir: str,
) -> Result[SideRun, SideFailure]:
    with psycopg.connect(database.url, autocommit=True) as connection:
        _apply_row_edits(connection, scenario)
        match read_database_clock(connection):
            case Ok(started):
                pass
            case Err(clock_failed):
                return Err(CaptureFailed(side=command.name, cause=clock_failed))
        process = _start_worker(
            command, database, scenario.profile, log_path, marker_dir
        )
        try:
            waited = _wait_for_stop_point(
                connection, process, scenario, database, marker_dir
            )
        finally:
            worker = _stop_worker(process, log_path)
        match waited:
            case WaitEnd.REACHED:
                pass
            case WaitEnd.WORKER_EXITED:
                return Err(WorkerExitedEarly(side=command.name, worker=worker))
            case WaitEnd.BOUND_PASSED:
                return Err(
                    TasksNotTerminal(
                        side=command.name,
                        open_tasks=_open_tasks(connection, task_ids),
                        worker=worker,
                    )
                )
            case PauseNotApplied(outcome=outcome):
                return Err(PauseRefused(side=command.name, detail=outcome, worker=worker))
        still_open = _open_tasks(connection, task_ids)
        if still_open:
            return Err(
                TasksNotTerminal(side=command.name, open_tasks=still_open, worker=worker)
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
        report = read_report(connection, scenario)
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
