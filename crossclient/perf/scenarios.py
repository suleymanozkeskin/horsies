# pyright: reportPrivateUsage=false
"""The paths under measurement, seeded and executed as the runtime executes them.

Each scenario runs the statement the library actually issues, with the
parameters its caller actually passes. A benchmark that reimplements the
statement measures the benchmark.

Two shapes, because the workloads genuinely differ: a single-row operation
consumes one seeded row per call, and a batch operation selects its own rows
under a bound and consumes many. Reporting a batch as though it were one row
would flatter it by the batch size.

On the baseline schema each scenario has one implementation, the statement
that the product sends, so both sides of a run execute it (a control run). A
release is compared with another release by running the harness of each
release on one server in one session (``crossclient/perf/releases/README.md``).
The candidate slot stays for a comparison of two implementations on one
schema; a gate refuses a scenario without a candidate unless the run is a
control run.

A third shape calls a public operation of the library (``public_calls.py``)
in place of a statement: the library sends its own statements, so the
harness times the call and counts what the server recorded. The call is the
same public API in each release, so the harness of each release measures the
same work through its own product.

A fourth shape drains sent tasks with a worker that the public CLI starts
(``worker_drain.py``): the worker claims, runs and finishes them, and the
harness reports the drain as throughput.

Seeded rows carry ids of the column type: one UUIDv7 per scenario, role,
kind of row and index (``seed_rows``). The ids of one role and kind share
their leading digits, so a run finds and deletes its own rows by an id range
on the primary key, also the rows of a run that crashed.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Final, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

from crossclient.perf.public_calls import (
    INPUT_TEXT_BYTES,
    SendCall,
    WorkflowStartCall,
    delete_sent_tasks,
    delete_started_workflows,
)
from crossclient.perf.statistics import Budget
from crossclient.perf.worker_drain import (
    DRAIN_COUNT_INTERVAL,
    DRAIN_ISOLATED_CHILDREN,
    DRAIN_PROFILE_NAME,
    DRAIN_TASK_COUNT,
    DRAIN_TIMEOUT,
    WORKER_STOP_TIMEOUT,
    DrainSender,
    count_terminal_tasks,
    delete_drained_tasks,
    start_drain_worker,
)
from crossclient.perf.worker_process import StartedWorker, WorkerStartFailed

WORKER_ID = 'perf-harness'
WORKER_HOSTNAME = 'perf-harness-host'
WORKER_PID = 4242
WORKER_PROCESS_NAME = 'perf-harness-child'
CLAIMED_AT = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)

ID_BIRTH: Final = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
"""The UUIDv7 birth of every seeded id. It is earlier than every terminal
time a run writes, so a move records no clock exception for a seeded id."""

ID_INDEX_HEX_DIGITS: Final = 10
"""The row index fills the last 10 hex digits (40 bits) of a seeded id."""

MAX_SEEDED_INDEX: Final = 16**ID_INDEX_HEX_DIGITS - 1
"""The largest row index a seeded id can hold."""


# Every insert names every column of the table, as the product's writers do
# (the baseline schema has no column defaults). The retention class is
# `forever`: the install creates its one static history leaf, while a period
# class has leaves only after a maintenance pass, which a measurement database
# does not run. The id of row g is the role's prefix followed by g in
# ID_INDEX_HEX_DIGITS hex digits, the form `SeedIds.id_at` computes.
_TASK_COLUMNS: Final = """
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at, command_fingerprint_version,
        command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
        finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
        worker_process_name, retry_count, max_retries, next_retry_at,
        good_until, is_workflow_task, updated_at, retain_rerun_input,
        rerun_of_task_id, rerun_root_task_id
"""

_SEEDED_IDS: Final = """
    FROM generate_series(1, CAST(:count AS integer)) AS g,
        LATERAL (
            SELECT CAST(:id_prefix || lpad(to_hex(g), 10, '0') AS uuid) AS id
        ) AS s
"""

# A RUNNING row as the claim and the start of an attempt leave it: owned by
# WORKER_ID at the claim generation CLAIMED_AT, started 30 s ago. A plain task
# carries the 32-byte command fingerprint of a send; a workflow task carries
# none (CHECK horsies_tasks_command_fingerprint_by_origin).
_INSERT_RUNNING_SQL = text(f"""
    INSERT INTO horsies_tasks ({_TASK_COLUMNS})
    SELECT
        s.id, 'perf.task', 'default', 100, '{{}}', NULL, 'ISOLATED',
        'forever', NOW(), NOW(),
        CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE 1 END,
        CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL
             ELSE sha256(uuid_send(s.id)) END,
        NULL, 'RUNNING', NOW(),
        CAST(:claimed_at AS timestamptz), :worker_id, NULL,
        NOW() - INTERVAL '30 seconds',
        NULL, NULL, :worker_pid, :worker_hostname,
        :worker_process_name, 0, 0, NULL,
        NULL, CAST(:is_workflow_task AS boolean), NOW(), FALSE,
        NULL, NULL
    {_SEEDED_IDS}
""")


# A seeded task ends either live or, after a terminal write, in history. The
# terminal writes delete the task's attempt rows with the live row, and no
# scenario writes a retry, so no attempt row outlives a measured operation.
_DELETE_LIVE_TASKS_SQL = text("""
    DELETE FROM horsies_tasks
    WHERE id BETWEEN CAST(:lower AS uuid) AND CAST(:upper AS uuid)
""")

_DELETE_HISTORY_TASKS_SQL = text("""
    DELETE FROM horsies_task_history
    WHERE task_id BETWEEN CAST(:lower AS uuid) AND CAST(:upper AS uuid)
""")


class Role(Enum):
    """Whose rows a seeded id names."""

    BASELINE = 'base'
    CANDIDATE = 'cand'
    BALLAST = 'ball'


class RowKind(Enum):
    """The table a seeded id is the primary key of."""

    TASK = 'task'
    WORKFLOW = 'workflow'
    WORKFLOW_NODE = 'workflow-node'
    DEPENDENT_NODE = 'dependent-node'


@dataclass(frozen=True, slots=True)
class SeedIds:
    """The ids of one kind of row of one role in one scenario.

    ``prefix`` is 22 hex digits: the 48-bit UUIDv7 birth ``ID_BIRTH``, the
    version digit 7, and 34 bits of the SHA-256 of the scenario, role and kind
    in the random fields (the variant bits set). The row index fills the last
    ``ID_INDEX_HEX_DIGITS`` hex digits, so the ids of one ``SeedIds`` lie in
    the closed range ``lower`` to ``upper`` and no other ``SeedIds`` shares it
    unless their digests collide in 34 bits.
    """

    prefix: str

    def id_at(self, index: int) -> str:
        """The id of row ``index``, from 1 to ``MAX_SEEDED_INDEX``.

        Raises:
            ValueError: ``index`` is outside that range. Indexes come from
                the harness's own counts, so this is a defect of the caller.
        """
        if not 1 <= index <= MAX_SEEDED_INDEX:
            raise ValueError(
                f'a seeded row index runs from 1 to {MAX_SEEDED_INDEX}, got {index}'
            )
        return str(uuid.UUID(hex=f'{self.prefix}{index:0{ID_INDEX_HEX_DIGITS}x}'))

    def ids(self, count: int) -> Iterator[str]:
        """The ids of rows 1 to ``count``, in order."""
        for index in range(1, count + 1):
            yield self.id_at(index)

    @property
    def lower(self) -> str:
        """The lowest id of the range (index 0, never seeded)."""
        return str(uuid.UUID(hex=f'{self.prefix}{0:0{ID_INDEX_HEX_DIGITS}x}'))

    @property
    def upper(self) -> str:
        """The highest id of the range."""
        return str(
            uuid.UUID(hex=f'{self.prefix}{MAX_SEEDED_INDEX:0{ID_INDEX_HEX_DIGITS}x}')
        )

    def bounds(self) -> dict[str, str]:
        """The parameters of a range delete."""
        return {'lower': self.lower, 'upper': self.upper}


@dataclass(frozen=True, slots=True)
class SeedRows:
    """The rows that one role of one scenario seeds. Row g of each kind
    belongs together: workflow g holds node g, which links task g, and, in a
    scenario that seeds one, dependent node g of node g."""

    tasks: SeedIds
    workflows: SeedIds
    nodes: SeedIds
    dependent_nodes: SeedIds


def seed_ids(scenario_name: str, role: Role, kind: RowKind) -> SeedIds:
    """The id range of one kind of row of one role in one scenario. Pure.

    Stable across runs, so a later run deletes the rows of a crashed one.
    """
    digest = hashlib.sha256(
        f'{scenario_name}/{role.value}/{kind.value}'.encode('utf-8')
    ).digest()
    birth_ms = int(ID_BIRTH.timestamp() * 1000)
    random_a = int.from_bytes(digest[0:2], 'big') & 0x0FFF
    variant_and_random = 0x80 | (digest[2] & 0x3F)
    prefix = f'{birth_ms:012x}7{random_a:03x}{variant_and_random:02x}{digest[3:5].hex()}'
    assert len(prefix) == 32 - ID_INDEX_HEX_DIGITS, prefix
    return SeedIds(prefix=prefix)


def seed_rows(scenario_name: str, role: Role) -> SeedRows:
    """The rows of one role in one scenario. Pure."""
    return SeedRows(
        tasks=seed_ids(scenario_name, role, RowKind.TASK),
        workflows=seed_ids(scenario_name, role, RowKind.WORKFLOW),
        nodes=seed_ids(scenario_name, role, RowKind.WORKFLOW_NODE),
        dependent_nodes=seed_ids(scenario_name, role, RowKind.DEPENDENT_NODE),
    )


type Seed = Callable[[Connection, SeedRows, int], None]
type Cleanup = Callable[[Connection, SeedRows], None]
type RowCheck = Callable[[Sequence[Mapping[str, Any]]], int]
"""Checks the rows one statement returned and gives the number of task rows it
moved to history. Raises ``RuntimeError`` when the statement did not apply the
transition of its seeded rows."""


@dataclass(frozen=True, slots=True)
class Invocation:
    """One statement the product sends, with the parameters its caller supplies."""

    statement: TextClause
    parameters: dict[str, Any]
    operation: str
    check: RowCheck


type InvocationFactory = Callable[[str], Invocation]
type BatchInvocationFactory = Callable[[], Invocation]


@dataclass(frozen=True, slots=True)
class SingleRowScenario:
    """One seeded row consumed per measured operation."""

    name: str
    description: str
    p50_budget: Budget
    p99_budget: Budget
    payload_bytes: int
    seed: Seed
    cleanup: Cleanup
    baseline: Callable[[Connection, str], int]
    candidate: Callable[[Connection, str], int] | None
    baseline_invocation: InvocationFactory
    candidate_invocation: InvocationFactory | None
    exact_client_statements_per_operation: int | None = None
    exact_write_transactions_per_operation: int = 1


@dataclass(frozen=True, slots=True)
class BatchScenario:
    """One bounded batch consumed per measured operation."""

    name: str
    description: str
    lock_p95_budget: Budget
    minimum_throughput_ratio: float
    batch_size: int
    seed: Seed
    cleanup: Cleanup
    baseline: Callable[[Connection], int]
    candidate: Callable[[Connection], int] | None
    baseline_invocation: BatchInvocationFactory
    candidate_invocation: BatchInvocationFactory | None
    exact_client_statements_per_operation: int | None = 1
    exact_write_transactions_per_operation: int = 1


class PublicCall(Protocol):
    """A client of the library whose ``call`` runs one public operation."""

    def call(self) -> None:
        """Run the operation once.

        Raises:
            RuntimeError: the library returned an error. The run stops: a
                measured operation must apply.
        """
        ...

    def close(self) -> None:
        """Release the connections of the client."""
        ...


type OpenPublicCall = Callable[[str], PublicCall]
"""Builds the client of a scenario for a database URL. No I/O."""

type PublicCleanup = Callable[[Connection], None]
"""Deletes every row that the calls of a scenario wrote, and commits."""


@dataclass(frozen=True, slots=True)
class PublicCallScenario:
    """One call of a public operation of the library per measured operation.

    The library opens its own connections and sends its own statements, so no
    plan is captured. Both sides of a run share one client and run the same
    call: every run is a control run. ``payload_bytes`` is the length of the
    text argument that one call sends.
    """

    name: str
    description: str
    p50_budget: Budget
    p99_budget: Budget
    payload_bytes: int
    open_call: OpenPublicCall
    cleanup: PublicCleanup


type OpenDrainSender = Callable[[str], DrainSender]
"""Builds the sender of a drain for a database URL. No I/O."""

type StartDrainWorker = Callable[[str], StartedWorker | WorkerStartFailed]
"""Starts the worker of a drain on a database URL. Starts a process."""

type CountTerminal = Callable[[Connection], int]
"""Reads the number of tasks of the drain that are terminal, and ends its
transaction."""


@dataclass(frozen=True, slots=True)
class WorkerDrainScenario:
    """One drain per measured operation.

    A drain sends ``task_count`` tasks (``open_sender``), then starts one
    worker (``start_worker``). The operation runs from the start of the
    worker until ``count_terminal`` reads every task terminal, once every
    ``count_interval``; a drain that takes longer than ``timeout`` stops the
    run. Then the worker gets SIGTERM and ``stop_timeout`` to exit 0. Both
    sides run the same drain: every run is a control run. The statement and
    transaction counts of a worker depend on its timing, so the sides are not
    held to equal counts.
    """

    name: str
    description: str
    minimum_throughput_ratio: float
    task_count: int
    timeout: timedelta
    count_interval: timedelta
    stop_timeout: timedelta
    open_sender: OpenDrainSender
    start_worker: StartDrainWorker
    count_terminal: CountTerminal
    cleanup: PublicCleanup


type StatementScenario = SingleRowScenario | BatchScenario
"""A scenario whose operation is one statement that the harness sends."""

type Scenario = StatementScenario | PublicCallScenario | WorkerDrainScenario


def payload_of(size_bytes: int) -> str:
    """A result payload of a stated size, as the child would have written it."""
    filler = 'x' * max(0, size_bytes - 16)
    return f'{{"ok": "{filler}"}}'


def _seed_parameters(ids: SeedIds, count: int) -> dict[str, Any]:
    return {'id_prefix': ids.prefix, 'count': count}


def seed_running_tasks(
    connection: Connection,
    rows: SeedRows,
    count: int,
    *,
    is_workflow_task: bool = False,
) -> None:
    connection.execute(
        _INSERT_RUNNING_SQL,
        {
            **_seed_parameters(rows.tasks, count),
            'is_workflow_task': is_workflow_task,
            'claimed_at': CLAIMED_AT,
            'worker_id': WORKER_ID,
            'worker_pid': WORKER_PID,
            'worker_hostname': WORKER_HOSTNAME,
            'worker_process_name': WORKER_PROCESS_NAME,
        },
    )
    connection.commit()


FUSED_BATCH_MAX_ROWS: Final = 64
"""The most rows of one batched plain-success statement of the product."""

_COMPLETE_TASKS_FUSED_SQL = text("""
    SELECT outcome FROM horsies_complete_tasks_fused(
        CAST(:task_ids AS uuid[]), CAST(:claimed_ats AS timestamptz[]),
        CAST(:results AS text[]), CAST(:worker_id AS text), TRUE
    )
    ORDER BY ordinality
""")
"""The batch of plain successes of the product: one completion per row, each
under its own fence (``horsies_complete_tasks_fused``)."""


def seed_terminal_ballast(
    connection: Connection,
    *,
    rows: SeedRows,
    count: int,
    payload_bytes: int,
) -> None:
    """Terminal rows in history, written by the product's terminal write.

    A heap where every row is fresh is not the heap this runs against in
    production, and a scan over one answers a question nobody asked. The
    live table holds live rows only (CHECK horsies_tasks_status_live), so
    each ballast row is seeded RUNNING and completed by the statement of a
    batch of plain successes (``horsies_complete_tasks_fused``), at most
    ``FUSED_BATCH_MAX_ROWS`` rows per statement and one transaction each. The measured statements then meet a
    history leaf with rows in it and a live table with their dead tuples.

    Raises:
        RuntimeError: a completion did not apply.
    """
    seed_running_tasks(connection, rows, count)
    payload = payload_of(payload_bytes)
    task_ids = list(rows.tasks.ids(count))
    for start in range(0, count, FUSED_BATCH_MAX_ROWS):
        batch = task_ids[start : start + FUSED_BATCH_MAX_ROWS]
        outcomes = connection.execute(
            _COMPLETE_TASKS_FUSED_SQL,
            {
                'task_ids': batch,
                'claimed_ats': [CLAIMED_AT] * len(batch),
                'results': [payload] * len(batch),
                'worker_id': WORKER_ID,
            },
        ).scalars().all()
        refused = [outcome for outcome in outcomes if outcome != 'APPLIED']
        if len(outcomes) != len(batch) or refused:
            connection.rollback()
            raise RuntimeError(
                f'ballast completion applied {len(outcomes) - len(refused)} of '
                f'{len(batch)} rows; first refusal: {refused[:1]!r}'
            )
        connection.commit()


def delete_seeded(connection: Connection, rows: SeedRows) -> None:
    """Delete the task rows of ``rows``, live and in history."""
    connection.execute(_DELETE_LIVE_TASKS_SQL, rows.tasks.bounds())
    connection.execute(_DELETE_HISTORY_TASKS_SQL, rows.tasks.bounds())
    connection.commit()


def analyze(connection: Connection) -> None:
    """Statistics must describe the seeded tables, not the empty ones."""
    connection.execute(text('ANALYZE horsies_tasks'))
    connection.execute(text('ANALYZE horsies_task_history'))
    connection.commit()


PUBLIC_CALL_P50: Final = Budget(fraction=0.10, floor_ms=0.5)
PUBLIC_CALL_P99: Final = Budget(fraction=0.15, floor_ms=1.5)
MINIMUM_DRAIN_THROUGHPUT_RATIO: Final = 0.90

SCENARIOS: tuple[Scenario, ...] = (
    PublicCallScenario(
        name='send',
        description=(
            'send of one task with one 200-byte text argument '
            '(`send`), one PENDING task row per call'
        ),
        p50_budget=PUBLIC_CALL_P50,
        p99_budget=PUBLIC_CALL_P99,
        payload_bytes=INPUT_TEXT_BYTES,
        open_call=SendCall,
        cleanup=delete_sent_tasks,
    ),
    PublicCallScenario(
        name='workflow-start',
        description=(
            'start of a one-node workflow whose node has one 200-byte text '
            'argument (`WorkflowSpec.start`): the workflow row, the node row '
            'and the task row of the node per call'
        ),
        p50_budget=PUBLIC_CALL_P50,
        p99_budget=PUBLIC_CALL_P99,
        payload_bytes=INPUT_TEXT_BYTES,
        open_call=WorkflowStartCall,
        cleanup=delete_started_workflows,
    ),
    WorkerDrainScenario(
        name='claim-finish',
        description=(
            f'claim, run and finish of {DRAIN_TASK_COUNT} sent no-op tasks by '
            f'one worker that `horsies worker --profile {DRAIN_PROFILE_NAME}` '
            f'starts ({DRAIN_ISOLATED_CHILDREN} isolated children, not '
            'recycled). One operation is one drain: the time from the start '
            'of the worker process until every task is COMPLETED or FAILED, '
            'read by a count every '
            f'{DRAIN_COUNT_INTERVAL // timedelta(milliseconds=1)} ms'
        ),
        minimum_throughput_ratio=MINIMUM_DRAIN_THROUGHPUT_RATIO,
        task_count=DRAIN_TASK_COUNT,
        timeout=DRAIN_TIMEOUT,
        count_interval=DRAIN_COUNT_INTERVAL,
        stop_timeout=WORKER_STOP_TIMEOUT,
        open_sender=DrainSender,
        start_worker=start_drain_worker,
        count_terminal=count_terminal_tasks,
        cleanup=delete_drained_tasks,
    ),
)
"""The scenarios of this release: public operations that run on 0.7.0 and on
this release with the same call, and the drain of sent tasks by a worker that
the public CLI starts."""


def scenario_by_name(name: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    known = ', '.join(s.name for s in SCENARIOS)
    raise KeyError(f'unknown scenario {name!r}; known scenarios: {known}')
