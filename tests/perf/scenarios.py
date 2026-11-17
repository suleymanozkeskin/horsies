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
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Final, assert_never, cast

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

from horsies.core.brokers.postgres import _EXPIRE_BATCH_SIZE
from horsies.core.brokers.stale_running import (
    FINISH_STALE_ATTEMPT_SQL,
    StaleCandidate,
    StaleThresholds,
    crash_result_json,
    crash_retry_request,
    finish_stale_parameters,
)
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.lifecycle.commands import (
    CompleteTaskFused,
    ExpireOwnedClaim,
    ExpirePendingTasks,
    TerminalizationCommand,
)
from horsies.core.lifecycle.fences import OwnedClaim, WorkerOwned
from horsies.core.lifecycle.outcomes import (
    Applied,
    CapacityWoken,
    FailureApplied,
    NothingOwed,
    PhaseTwoOwed,
    PhaseTwoStep,
    RetryNotRequested,
    StaleFailureApplied,
    SuccessApplied,
    decode_failed_attempt_row,
    decode_outcome_row,
    decode_stale_attempt_row,
    decode_workflow_success_row,
)
from horsies.core.lifecycle.persistence import (
    FUSED_BATCH_MAX_ROWS,
    FusedCompletionBatch,
    _fused_outcomes,
    _fused_statement,
    _log_outcome,
    apply_sync,
    batch_call_for,
    call_for,
)
from horsies.core.models.tasks import (
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.workflows.node_finish import (
    NODE_FINISH_FUNCTION,
    CapacityWake,
    _Dependents,
    _parameters as _node_finish_parameters,
)
from horsies.core.workflows.steps import (
    NODE_FINISH_FUNCTION_SQL,
    NodeFinishDisposition,
    decode_step_rows,
    require_no_child_starts,
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

# Budgets as declared for each path, before anything was measured.
FUSED_P50 = Budget(fraction=0.05, floor_ms=0.2)
FUSED_P99 = Budget(fraction=0.10, floor_ms=1.0)
SINGLE_ROW_P50 = Budget(fraction=0.10, floor_ms=0.5)
SINGLE_ROW_P99 = Budget(fraction=0.15, floor_ms=1.5)
BATCH_LOCK_P95 = Budget(fraction=0.10, floor_ms=1.0)
MINIMUM_BATCH_THROUGHPUT_RATIO = 0.90

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

_INSERT_PENDING_EXPIRED_SQL = text(f"""
    INSERT INTO horsies_tasks ({_TASK_COLUMNS})
    SELECT
        s.id, 'perf.task', 'default', 100, '{{}}', NULL, 'ISOLATED',
        'forever', NOW(), NOW(), 1,
        sha256(uuid_send(s.id)), NULL, 'PENDING', NOW(),
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, 0, 0, NULL,
        NOW() - INTERVAL '1 hour', FALSE, NOW(), FALSE,
        NULL, NULL
    {_SEEDED_IDS}
""")

_INSERT_CLAIMED_EXPIRED_SQL = text(f"""
    INSERT INTO horsies_tasks ({_TASK_COLUMNS})
    SELECT
        s.id, 'perf.task', 'default', 100, '{{}}', NULL, 'ISOLATED',
        'forever', NOW(), NOW(), 1,
        sha256(uuid_send(s.id)), NULL, 'CLAIMED', NOW(),
        CAST(:claimed_at AS timestamptz), :worker_id,
        NOW() + INTERVAL '5 minutes', NULL,
        NULL, NULL, NULL, NULL,
        NULL, 0, 0, NULL,
        NOW() - INTERVAL '1 hour', FALSE, NOW(), FALSE,
        NULL, NULL
    {_SEEDED_IDS}
""")

# A RUNNING workflow per seeded workflow task: the workflow row and its node 0,
# which links the task of the same index. No sub-workflow node is below node 0
# (downstream_subworkflow NONE). The workflow's output is node 0; with no other
# node, phase 2 completes the workflow.
_INSERT_WORKFLOWS_SQL = text("""
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index,
        success_policy, result, error, parent_workflow_id, parent_task_index,
        root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    )
    SELECT
        w.id, 'perf.workflow', NULL, 'RUNNING', 'fail', 0,
        NULL, NULL, NULL, NULL, NULL,
        w.id, 0, NOW(), NOW(), NOW(),
        NULL, NOW()
    FROM generate_series(1, CAST(:count AS integer)) AS g,
        LATERAL (
            SELECT CAST(:workflow_id_prefix || lpad(to_hex(g), 10, '0') AS uuid) AS id
        ) AS w
""")

_NODE_COLUMNS: Final = """
        id, workflow_id, task_index, node_id, task_name, input, queue_name,
        priority, dependencies, args_from, workflow_ctx_from,
        allow_failed_deps, join_type, min_success, task_options, max_retries,
        execution, good_until, retention_class_key, downstream_subworkflow,
        status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
        sub_definition_key, sub_workflow_params, sub_workflow_summary,
        result, error, created_at, started_at, completed_at
"""

_INSERT_WORKFLOW_NODES_SQL = text(f"""
    INSERT INTO horsies_workflow_tasks ({_NODE_COLUMNS})
    SELECT
        r.node_id, r.workflow_id, 0, 'node_0', 'perf.task', '{{}}', 'default',
        100, '{{}}', NULL, NULL,
        FALSE, 'all', NULL, NULL, 0,
        'ISOLATED', NULL, 'forever', 'NONE',
        'RUNNING', r.task_id, FALSE, NULL, NULL,
        NULL, NULL, NULL,
        NULL, NULL, NOW(), NOW(), NULL
    FROM generate_series(1, CAST(:count AS integer)) AS g,
        LATERAL (
            SELECT
                CAST(:node_id_prefix || lpad(to_hex(g), 10, '0') AS uuid) AS node_id,
                CAST(:workflow_id_prefix || lpad(to_hex(g), 10, '0') AS uuid) AS workflow_id,
                CAST(:id_prefix || lpad(to_hex(g), 10, '0') AS uuid) AS task_id
        ) AS r
""")

# The dependent of node 0, for the scenario whose phase 2 reads the dependents:
# node 1, PENDING and without a task row, joins on node 0 alone (join all, no
# args_from). Phase 2 promotes it: the node goes ENQUEUED and its task row is
# minted. The workflow then has a live node and stays RUNNING.
_INSERT_DEPENDENT_NODES_SQL = text(f"""
    INSERT INTO horsies_workflow_tasks ({_NODE_COLUMNS})
    SELECT
        r.node_id, r.workflow_id, 1, 'node_1', 'perf.task', '{{}}', 'default',
        100, '{{0}}', NULL, NULL,
        FALSE, 'all', NULL, NULL, 0,
        'ISOLATED', NULL, 'forever', 'NONE',
        'PENDING', NULL, FALSE, NULL, NULL,
        NULL, NULL, NULL,
        NULL, NULL, NOW(), NULL, NULL
    FROM generate_series(1, CAST(:count AS integer)) AS g,
        LATERAL (
            SELECT
                CAST(:node_id_prefix || lpad(to_hex(g), 10, '0') AS uuid) AS node_id,
                CAST(:workflow_id_prefix || lpad(to_hex(g), 10, '0') AS uuid) AS workflow_id
        ) AS r
""")

# A promotion mints the task row of a dependent node with an id from the
# database clock, outside every seeded range. The delete finds it through the
# link of its node, before the node rows go with their workflow.
_DELETE_LINKED_LIVE_TASKS_SQL = text("""
    DELETE FROM horsies_tasks t
    USING horsies_workflow_tasks n
    WHERE n.workflow_id BETWEEN CAST(:lower AS uuid) AND CAST(:upper AS uuid)
      AND t.id = n.task_id
""")

# The delete of a workflow row cascades to its node rows and their phase-2
# records (ON DELETE CASCADE).
_DELETE_WORKFLOWS_SQL = text("""
    DELETE FROM horsies_workflows
    WHERE id BETWEEN CAST(:lower AS uuid) AND CAST(:upper AS uuid)
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


def seed_expired_pending_tasks(
    connection: Connection,
    rows: SeedRows,
    count: int,
) -> None:
    connection.execute(
        _INSERT_PENDING_EXPIRED_SQL,
        _seed_parameters(rows.tasks, count),
    )
    connection.commit()


def seed_expired_claimed_tasks(
    connection: Connection,
    rows: SeedRows,
    count: int,
) -> None:
    connection.execute(
        _INSERT_CLAIMED_EXPIRED_SQL,
        {
            **_seed_parameters(rows.tasks, count),
            'claimed_at': CLAIMED_AT,
            'worker_id': WORKER_ID,
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


def seed_workflow_success_tasks(
    connection: Connection,
    rows: SeedRows,
    count: int,
) -> None:
    """One-node RUNNING workflows whose only task is RUNNING."""
    seed_running_tasks(connection, rows, count, is_workflow_task=True)
    connection.execute(
        _INSERT_WORKFLOWS_SQL,
        {'workflow_id_prefix': rows.workflows.prefix, 'count': count},
    )
    connection.execute(
        _INSERT_WORKFLOW_NODES_SQL,
        {
            **_seed_parameters(rows.tasks, count),
            'workflow_id_prefix': rows.workflows.prefix,
            'node_id_prefix': rows.nodes.prefix,
        },
    )
    connection.commit()


def seed_workflow_dependent_tasks(
    connection: Connection,
    rows: SeedRows,
    count: int,
) -> None:
    """Two-node RUNNING workflows: node 0 links a RUNNING task, and node 1,
    PENDING, depends on node 0."""
    seed_workflow_success_tasks(connection, rows, count)
    connection.execute(
        _INSERT_DEPENDENT_NODES_SQL,
        {
            'count': count,
            'workflow_id_prefix': rows.workflows.prefix,
            'node_id_prefix': rows.dependent_nodes.prefix,
        },
    )
    connection.commit()


def delete_workflow_seeded(connection: Connection, rows: SeedRows) -> None:
    """Delete the live task rows that the nodes of ``rows`` link (the task
    rows that a promotion minted among them), the workflow rows with their
    nodes, then the task rows of ``rows``."""
    connection.execute(_DELETE_LINKED_LIVE_TASKS_SQL, rows.workflows.bounds())
    connection.execute(_DELETE_WORKFLOWS_SQL, rows.workflows.bounds())
    connection.commit()
    delete_seeded(connection, rows)


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


def _one_row(
    rows: Sequence[Mapping[str, Any]],
    operation: str,
) -> Mapping[str, Any]:
    if len(rows) != 1:
        raise RuntimeError(f'{operation} returned {len(rows)} rows; expected one')
    return rows[0]


def _terminalization_applied(
    command: TerminalizationCommand,
    operation: str,
) -> RowCheck:
    """The check of a single-row terminal write: one APPLIED outcome row."""

    def check(rows: Sequence[Mapping[str, Any]]) -> int:
        outcome = decode_outcome_row(_one_row(rows, operation))
        if not isinstance(outcome, Applied):
            raise RuntimeError(
                f'{operation} returned {type(outcome).__name__} '
                'for a seeded eligible task'
            )
        _log_outcome(command, outcome)
        return 1

    return check


def _execute_invocation(connection: Connection, invocation: Invocation) -> int:
    result = connection.execute(invocation.statement, invocation.parameters)
    return invocation.check(_mappings(result.mappings().all()))


def _fused_invocation(payload: str) -> InvocationFactory:
    def build(task_id: str) -> Invocation:
        command = CompleteTaskFused(
            task_id=task_id,
            fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=CLAIMED_AT),
            result_json=payload,
        )
        statement, parameters = call_for(command)
        operation = 'fused completion operation'
        return Invocation(
            statement=statement,
            parameters=dict(parameters),
            operation=operation,
            check=_terminalization_applied(command, operation),
        )

    return build


_EXPIRY_RESULT = serialize_error_payload(
    TaskResult(
        err=TaskError(
            error_code=OutcomeCode.TASK_EXPIRED,
            message='Task expired: good_until deadline passed before execution started',
        )
    )
)


def _owned_expiry_command(task_id: str) -> ExpireOwnedClaim:
    return ExpireOwnedClaim(
        task_id=task_id,
        fence=WorkerOwned(worker_id=WORKER_ID),
        result_json=_EXPIRY_RESULT,
        error_code='TASK_EXPIRED',
    )


def _owned_expiry_invocation(task_id: str) -> Invocation:
    command = _owned_expiry_command(task_id)
    statement, parameters = call_for(command)
    operation = 'owned claim expiry operation'
    return Invocation(
        statement=statement,
        parameters=dict(parameters),
        operation=operation,
        check=_terminalization_applied(command, operation),
    )


def _run_owned_expiry(connection: Connection, task_id: str) -> int:
    """The child's expiry on its synchronous psycopg cursor, as it sends it."""
    raw_connection = cast(Any, connection.connection.driver_connection)
    with raw_connection.cursor() as cursor:
        outcome = apply_sync(cursor, _owned_expiry_command(task_id))
        if not isinstance(outcome, Applied):
            raw_connection.rollback()
            raise RuntimeError(
                'owned claim expiry operation returned '
                f'{type(outcome).__name__} for a seeded eligible task'
            )
        raw_connection.commit()
    return 1


def _run_terminal_invocation(
    invocation_for: InvocationFactory,
) -> Callable[[Connection, str], int]:
    def run(connection: Connection, task_id: str) -> int:
        transitioned = _execute_invocation(connection, invocation_for(task_id))
        connection.commit()
        return transitioned

    return run


def _pending_expiry_invocation(batch_size: int) -> BatchInvocationFactory:
    def build() -> Invocation:
        command = ExpirePendingTasks(
            batch_size=batch_size,
            result_json=payload_of(200),
            error_code='TASK_EXPIRED',
        )
        statement, parameters = batch_call_for(command)

        def check(rows: Sequence[Mapping[str, Any]]) -> int:
            outcomes = [decode_outcome_row(row) for row in rows]
            if len(outcomes) != batch_size or not all(
                isinstance(outcome, Applied) for outcome in outcomes
            ):
                raise RuntimeError(
                    'pending expiry operation did not apply exactly '
                    f'{batch_size} transitions'
                )
            for outcome in outcomes:
                _log_outcome(command, outcome)
            return len(outcomes)

        return Invocation(
            statement=statement,
            parameters=dict(parameters),
            operation='pending expiry batch operation',
            check=check,
        )

    return build


def _run_pending_expiry(
    invocation_for: BatchInvocationFactory,
) -> Callable[[Connection], int]:
    def run(connection: Connection) -> int:
        transitioned = _execute_invocation(connection, invocation_for())
        connection.commit()
        return transitioned

    return run


# The call texts of the two writers that only the core sends, as
# crates/horsies-core/src/broker/attempt_writes.rs holds them: the function,
# its arguments in order, and the type the core binds each parameter as
# (uuid, text, timestamptz, int4, float8). The core passes the wake argument
# of the failed attempt as the constant TRUE. The function body is the
# measured work.
_FINISH_FAILED_ATTEMPT_SQL = text("""
    SELECT * FROM horsies_finish_failed_attempt(
        CAST(:task_id AS uuid), CAST(:worker_id AS text),
        CAST(:claimed_at AS timestamptz), CAST(:retry_kind AS text),
        CAST(:expected_retry_count AS integer),
        CAST(:retry_delay_s AS double precision),
        CAST(:retry_placement AS text), CAST(:result AS text),
        CAST(:error_code AS text), CAST(:failed_reason AS text),
        CAST(:attempt_outcome AS text), CAST(:attempt_message AS text),
        CAST(:attempt_reason AS text), CAST(:claimed_row_rule AS text), TRUE
    )
""")

_FINISH_WORKFLOW_SUCCESS_SQL = text("""
    SELECT * FROM horsies_finish_workflow_success(
        CAST(:task_id AS uuid), CAST(:worker_id AS text),
        CAST(:claimed_at AS timestamptz), CAST(:result AS text)
    )
""")

_FAILURE_ERROR_CODE = 'PERF_FAILURE'
_FAILURE_MESSAGE = 'terminal application failure measurement'
_FAILURE_RESULT = serialize_error_payload(
    TaskResult(
        err=TaskError(
            error_code=_FAILURE_ERROR_CODE,
            message=_FAILURE_MESSAGE,
        )
    )
)


def _failed_attempt_applied(rows: Sequence[Mapping[str, Any]]) -> int:
    operation = 'failed attempt operation'
    match decode_failed_attempt_row(_one_row(rows, operation)):
        case FailureApplied(retry=RetryNotRequested(), after=CapacityWoken()):
            return 1
        case other:
            raise RuntimeError(
                f'{operation} returned {other!r} for a seeded eligible task'
            )


def _failed_attempt_invocation(task_id: str) -> Invocation:
    """A task error with no retry, as the worker's failure path sends it.

    The child reported a result, so the claimed-row rule is REFUSE. The
    attempt row is FAILED with the error's message; the task keeps the
    error code and no failed reason.
    """
    return Invocation(
        statement=_FINISH_FAILED_ATTEMPT_SQL,
        parameters={
            'task_id': task_id,
            'worker_id': WORKER_ID,
            'claimed_at': CLAIMED_AT,
            'retry_kind': 'NONE',
            'expected_retry_count': None,
            'retry_delay_s': None,
            'retry_placement': None,
            'result': _FAILURE_RESULT,
            'error_code': _FAILURE_ERROR_CODE,
            'failed_reason': None,
            'attempt_outcome': 'FAILED',
            'attempt_message': _FAILURE_MESSAGE,
            'attempt_reason': None,
            'claimed_row_rule': 'REFUSE',
        },
        operation='failed attempt operation',
        check=_failed_attempt_applied,
    )


_STALE_THRESHOLDS = StaleThresholds(
    stale_after_ms=5_000,
    finalizing_stale_after_ms=5_000,
)


def _stale_candidate(task_id: str) -> StaleCandidate:
    """The scan row of a seeded stale task. Its time facts are fixed, so the
    crash result has one size in every operation."""
    return StaleCandidate(
        task_id=task_id,
        retry_count=0,
        max_retries=0,
        task_options=None,
        good_until=None,
        claimed_by_worker_id=WORKER_ID,
        worker_pid=WORKER_PID,
        worker_hostname=WORKER_HOSTNAME,
        worker_process_name=WORKER_PROCESS_NAME,
        started_at=CLAIMED_AT,
        db_now=CLAIMED_AT + timedelta(seconds=30),
    )


# The crash result names no task, so the result of the first baseline row
# serves every row, and its serialization stays out of the measured time.
_STALE_RESULT = crash_result_json(
    _stale_candidate(
        seed_ids('stale-attempt-terminal', Role.BASELINE, RowKind.TASK).id_at(1)
    ),
    _STALE_THRESHOLDS,
).unwrap()


def _stale_attempt_applied(rows: Sequence[Mapping[str, Any]]) -> int:
    operation = 'stale attempt operation'
    match decode_stale_attempt_row(_one_row(rows, operation)):
        case StaleFailureApplied(retry=RetryNotRequested(), after=NothingOwed()):
            return 1
        case other:
            raise RuntimeError(
                f'{operation} returned {other!r} for a seeded stale task'
            )


def _stale_attempt_invocation(task_id: str) -> Invocation:
    """The stale recovery of one candidate, with the parameters the reaper
    builds: the crash retry its policy asks for (none: max_retries 0)."""
    candidate = _stale_candidate(task_id)
    return Invocation(
        statement=FINISH_STALE_ATTEMPT_SQL,
        parameters=finish_stale_parameters(
            candidate,
            _STALE_THRESHOLDS,
            crash_retry_request(candidate),
            _STALE_RESULT,
        ),
        operation='stale attempt operation',
        check=_stale_attempt_applied,
    )


_WORKFLOW_RESULT: TaskResult[int, Any] = TaskResult(ok=1)
_WORKFLOW_RESULT_JSON = dumps_json(encode_task_result(_WORKFLOW_RESULT, int)).unwrap()


def _workflow_success_applied(step: PhaseTwoStep) -> RowCheck:
    """The check of phase 1: one APPLIED row that owes the phase 2 of
    ``step``, the step of the seeded graph."""

    def check(rows: Sequence[Mapping[str, Any]]) -> int:
        operation = 'workflow success operation'
        match decode_workflow_success_row(_one_row(rows, operation)):
            case SuccessApplied(phase_two=PhaseTwoOwed(step=owed)) if owed is step:
                return 1
            case other:
                raise RuntimeError(
                    f'{operation} returned {other!r} for a seeded workflow task; '
                    f'expected the phase 2 of {step.value}'
                )

    return check


def _workflow_success_invocation(step: PhaseTwoStep) -> InvocationFactory:
    """Phase 1 of a workflow task's success: the attempt row, the move and
    the phase-2 record."""

    def build(task_id: str) -> Invocation:
        return Invocation(
            statement=_FINISH_WORKFLOW_SUCCESS_SQL,
            parameters={
                'task_id': task_id,
                'worker_id': WORKER_ID,
                'claimed_at': CLAIMED_AT,
                'result': _WORKFLOW_RESULT_JSON,
            },
            operation='workflow success operation',
            check=_workflow_success_applied(step),
        )

    return build


def _node_finish_parameters_for(step: PhaseTwoStep, task_id: str) -> dict[str, object]:
    """The parameters that ``finish_node`` sends in its one-statement form for
    ``step``, with the capacity wake of the worker's phase 2. Pure."""
    match step:
        case PhaseTwoStep.NO_DEPENDENTS:
            dependents = _Dependents.NONE
        case PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW:
            dependents = _Dependents.READ
        case PhaseTwoStep.SUBWORKFLOW_REACHABLE:
            raise AssertionError(
                'no scenario seeds a sub-workflow node; its phase 2 is the '
                'transaction form'
            )
        case _ as unreachable:
            assert_never(unreachable)
    return _node_finish_parameters(task_id, dependents, CapacityWake.SEND)


def _run_workflow_success(
    step: PhaseTwoStep,
    *,
    workflow_status: WorkflowStatus,
) -> Callable[[Connection, str], int]:
    """Phase 1 and phase 2 of a workflow task's success, each its own write
    transaction. Phase 1 reports ``step``; phase 2 is
    ``horsies_workflow_node_finish`` with the dependents argument of that step
    (``finish_node``): the node write from the phase-2 record, the capacity
    wake, the promotion of the dependents and the completion check, after
    which the workflow is ``workflow_status``."""
    phase_one = _workflow_success_invocation(step)

    def run(connection: Connection, task_id: str) -> int:
        transitioned = _execute_invocation(connection, phase_one(task_id))
        connection.commit()
        rows = connection.execute(
            NODE_FINISH_FUNCTION_SQL,
            _node_finish_parameters_for(step, task_id),
        ).fetchall()
        outcome = require_no_child_starts(
            decode_step_rows(rows, NODE_FINISH_FUNCTION, NodeFinishDisposition),
            NODE_FINISH_FUNCTION,
        )
        if (
            outcome.disposition is not NodeFinishDisposition.APPLIED
            or outcome.workflow_status != workflow_status.value
        ):
            connection.rollback()
            raise RuntimeError(
                f'workflow node finish returned {outcome.disposition.value} with '
                f'workflow status {outcome.workflow_status!r} for a seeded '
                f'workflow; expected APPLIED with {workflow_status.value!r}'
            )
        connection.commit()
        return transitioned

    return run


# The batch bound is imported rather than restated: a round number here would
# measure a batch the system never sends, and a copied one would keep measuring
# the old batch after the runtime changed its mind.
PRODUCTION_EXPIRE_BATCH_SIZE = _EXPIRE_BATCH_SIZE

_PENDING_EXPIRY = _pending_expiry_invocation(PRODUCTION_EXPIRE_BATCH_SIZE)
_FUSED_SMALL = _fused_invocation(payload_of(200))
_FUSED_LARGE = _fused_invocation(payload_of(1024 * 1024))
_WORKFLOW_SUCCESS = _workflow_success_invocation(PhaseTwoStep.NO_DEPENDENTS)
_WORKFLOW_SUCCESS_WITH_DEPENDENT = _workflow_success_invocation(
    PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW,
)

SCENARIOS: tuple[Scenario, ...] = (
    SingleRowScenario(
        name='fused-completion-small-result',
        description='plain task completion, one statement, small result',
        p50_budget=FUSED_P50,
        p99_budget=FUSED_P99,
        payload_bytes=200,
        seed=seed_running_tasks,
        cleanup=delete_seeded,
        baseline=_run_terminal_invocation(_FUSED_SMALL),
        candidate=None,
        baseline_invocation=_FUSED_SMALL,
        candidate_invocation=None,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='fused-completion-1mib-result',
        description='plain task completion at the result-size warning threshold',
        p50_budget=FUSED_P50,
        p99_budget=FUSED_P99,
        payload_bytes=1024 * 1024,
        seed=seed_running_tasks,
        cleanup=delete_seeded,
        baseline=_run_terminal_invocation(_FUSED_LARGE),
        candidate=None,
        baseline_invocation=_FUSED_LARGE,
        candidate_invocation=None,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='owned-claim-expiry',
        description='deadline expiry of a worker-owned claim before user code starts',
        p50_budget=SINGLE_ROW_P50,
        p99_budget=SINGLE_ROW_P99,
        payload_bytes=len(_EXPIRY_RESULT.encode('utf-8')),
        seed=seed_expired_claimed_tasks,
        cleanup=delete_seeded,
        baseline=_run_owned_expiry,
        candidate=None,
        baseline_invocation=_owned_expiry_invocation,
        candidate_invocation=None,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='failed-attempt-terminal',
        description=(
            'terminal failure of a task error with no retry: attempt row, move '
            'and capacity wake in one statement'
        ),
        p50_budget=SINGLE_ROW_P50,
        p99_budget=SINGLE_ROW_P99,
        payload_bytes=len(_FAILURE_RESULT.encode('utf-8')),
        seed=seed_running_tasks,
        cleanup=delete_seeded,
        baseline=_run_terminal_invocation(_failed_attempt_invocation),
        candidate=None,
        baseline_invocation=_failed_attempt_invocation,
        candidate_invocation=None,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='stale-attempt-terminal',
        description=(
            'stale RUNNING recovery with no retry: staleness judged under the '
            'row lock, attempt row and move in one statement'
        ),
        p50_budget=SINGLE_ROW_P50,
        p99_budget=SINGLE_ROW_P99,
        payload_bytes=len(_STALE_RESULT.encode('utf-8')),
        seed=seed_running_tasks,
        cleanup=delete_seeded,
        baseline=_run_terminal_invocation(_stale_attempt_invocation),
        candidate=None,
        baseline_invocation=_stale_attempt_invocation,
        candidate_invocation=None,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='workflow-success-phase2',
        description=(
            'one-node workflow success: phase 1 (attempt row, move, phase-2 '
            'record), then phase 2 with no dependents read (node write, '
            'capacity wake, workflow completion)'
        ),
        p50_budget=SINGLE_ROW_P50,
        p99_budget=Budget(fraction=0.15, floor_ms=2.0),
        payload_bytes=len(_WORKFLOW_RESULT_JSON.encode('utf-8')),
        seed=seed_workflow_success_tasks,
        cleanup=delete_workflow_seeded,
        baseline=_run_workflow_success(
            PhaseTwoStep.NO_DEPENDENTS,
            workflow_status=WorkflowStatus.COMPLETED,
        ),
        candidate=None,
        baseline_invocation=_WORKFLOW_SUCCESS,
        candidate_invocation=None,
        exact_client_statements_per_operation=2,
        exact_write_transactions_per_operation=2,
    ),
    SingleRowScenario(
        name='workflow-success-phase2-dependent',
        description=(
            'two-node workflow success: phase 1 (attempt row, move, phase-2 '
            'record), then phase 2 with the dependents read (node write, '
            'capacity wake, promotion of the dependent node with its task row)'
        ),
        p50_budget=SINGLE_ROW_P50,
        p99_budget=Budget(fraction=0.15, floor_ms=2.0),
        payload_bytes=len(_WORKFLOW_RESULT_JSON.encode('utf-8')),
        seed=seed_workflow_dependent_tasks,
        cleanup=delete_workflow_seeded,
        baseline=_run_workflow_success(
            PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW,
            workflow_status=WorkflowStatus.RUNNING,
        ),
        candidate=None,
        baseline_invocation=_WORKFLOW_SUCCESS_WITH_DEPENDENT,
        candidate_invocation=None,
        exact_client_statements_per_operation=2,
        exact_write_transactions_per_operation=2,
    ),
    BatchScenario(
        name='pending-expiry-batch',
        description='deadline expiry of unclaimed tasks, one bounded batch',
        lock_p95_budget=BATCH_LOCK_P95,
        minimum_throughput_ratio=MINIMUM_BATCH_THROUGHPUT_RATIO,
        batch_size=PRODUCTION_EXPIRE_BATCH_SIZE,
        seed=seed_expired_pending_tasks,
        cleanup=delete_seeded,
        baseline=_run_pending_expiry(_PENDING_EXPIRY),
        candidate=None,
        baseline_invocation=_PENDING_EXPIRY,
        candidate_invocation=None,
    ),
)


def scenario_by_name(name: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    known = ', '.join(s.name for s in SCENARIOS)
    raise KeyError(f'unknown scenario {name!r}; known scenarios: {known}')
