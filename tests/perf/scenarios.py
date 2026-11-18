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
release on one server in one session (``tests/perf/releases/README.md``).
The candidate slot stays for a comparison of two implementations on one
schema; a gate refuses a scenario without a candidate unless the run is a
control run.

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
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

from horsies.core.lifecycle.commands import (
    CompleteTaskFused,
)
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    Applied,
)
from horsies.core.lifecycle.persistence import (
    FUSED_BATCH_MAX_ROWS,
    FusedCompletionBatch,
    _fused_outcomes,
    _fused_statement,
)
from tests.perf.statistics import Budget

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


type Scenario = SingleRowScenario | BatchScenario


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
    batch of plain successes, at most ``FUSED_BATCH_MAX_ROWS`` rows per
    statement and one transaction each. The measured statements then meet a
    history leaf with rows in it and a live table with their dead tuples.

    Raises:
        RuntimeError: a completion did not apply.
    """
    seed_running_tasks(connection, rows, count)
    payload = payload_of(payload_bytes)
    task_ids = list(rows.tasks.ids(count))
    for start in range(0, count, FUSED_BATCH_MAX_ROWS):
        batch = FusedCompletionBatch(
            commands=tuple(
                CompleteTaskFused(
                    task_id=task_id,
                    fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=CLAIMED_AT),
                    result_json=payload,
                )
                for task_id in task_ids[start : start + FUSED_BATCH_MAX_ROWS]
            )
        )
        statement = _fused_statement(batch)
        result = connection.execute(
            statement.statement,
            dict(statement.parameters),
        )
        outcomes = _fused_outcomes(batch, _mappings(result.mappings().all()))
        refused = [
            outcome for outcome in outcomes if not isinstance(outcome, Applied)
        ]
        if refused:
            connection.rollback()
            raise RuntimeError(
                f'ballast completion refused {len(refused)} of '
                f'{len(batch.commands)} rows; first: {refused[0]!r}'
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


def _mappings(rows: Sequence[Mapping[Any, Any]]) -> list[dict[str, Any]]:
    return [{str(key): value for key, value in row.items()} for row in rows]


SCENARIOS: tuple[Scenario, ...] = ()
"""The scenarios of this release: none until scenarios on public operations
land."""


def scenario_by_name(name: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    known = ', '.join(s.name for s in SCENARIOS)
    raise KeyError(f'unknown scenario {name!r}; known scenarios: {known}')
