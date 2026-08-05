# pyright: reportPrivateUsage=false
"""The paths under measurement, seeded and executed as the runtime executes them.

Each scenario runs the statement the library actually issues, with the
parameters its caller actually passes. A benchmark that reimplements the
statement measures the benchmark.

Two shapes, because the workloads genuinely differ: a single-row operation
consumes one seeded row per call, and a batch operation selects its own rows
under a bound and consumes many. Reporting a batch as though it were one row
would flatter it by the batch size.

The candidate side is the database-owned implementation. ``--control`` makes
the candidate side execute the baseline explicitly; a gate refuses a scenario
whose candidate does not exist.
"""

from __future__ import annotations

import asyncio
import atexit
import hashlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.sql.elements import TextClause

from horsies.core.brokers.postgres import (
    _EXPIRE_BATCH_SIZE,
    EXPIRE_PENDING_TASKS_SQL,
)
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_result
from horsies.core.lifecycle.commands import (
    CompleteLockedTask,
    CompleteTaskFused,
    FailLockedTask,
    TerminalizationCommand,
)
from horsies.core.lifecycle.fences import OwnedClaim, PriorLockedRead
from horsies.core.lifecycle.outcomes import Applied, decode_outcome_row
from horsies.core.lifecycle.persistence import (
    _log_outcome,
    call_for,
)
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.worker.sql import (
    FINALIZE_TASK_COMPLETED_SQL,
    MARK_TASK_COMPLETED_SQL,
    MARK_TASK_FAILED_SQL,
    NOTIFY_TASK_QUEUE_SQL,
    SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
    UPSERT_TASK_ATTEMPT_SQL,
)
from horsies.core.workflows.engine import on_workflow_task_complete
from tests.perf.statistics import Budget

WORKER_ID = 'perf-harness'
CLAIMED_AT = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)

# Budgets as declared for each path, before anything was measured.
FUSED_P50 = Budget(fraction=0.05, floor_ms=0.2)
FUSED_P99 = Budget(fraction=0.10, floor_ms=1.0)
SINGLE_ROW_P50 = Budget(fraction=0.10, floor_ms=0.5)
SINGLE_ROW_P99 = Budget(fraction=0.15, floor_ms=1.5)
BATCH_P50 = Budget(fraction=0.10, floor_ms=0.5)
BATCH_P99 = Budget(fraction=0.15, floor_ms=1.5)

_INSERT_RUNNING_SQL = text("""
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, status, args, kwargs, enqueue_sha,
        is_workflow_task, claimed, claimed_by_worker_id, claimed_at,
        started_at, result
    )
    SELECT
        :prefix || g, 'perf.task', 'default', 'RUNNING', '[]', '{}',
        repeat('0', 64), :is_workflow_task, TRUE, :worker_id, :claimed_at,
        NOW() - INTERVAL '30 seconds', NULL
    FROM generate_series(1, :count) AS g
""")

_INSERT_PENDING_EXPIRED_SQL = text("""
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, status, args, kwargs, enqueue_sha,
        is_workflow_task, claimed, good_until
    )
    SELECT
        :prefix || g, 'perf.task', 'default', 'PENDING', '[]', '{}',
        repeat('0', 64), FALSE, FALSE, NOW() - INTERVAL '1 hour'
    FROM generate_series(1, :count) AS g
""")

# A heap where every row is fresh is not the heap this runs against in
# production, and a scan over one answers a question nobody asked. Terminal
# rows are seeded alongside so the measured statements meet a table with
# history in it.
_INSERT_TERMINAL_BALLAST_SQL = text("""
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, status, args, kwargs, enqueue_sha,
        is_workflow_task, claimed, completed_at, terminal_at, result
    )
    SELECT
        :prefix || g, 'perf.task', 'default', 'COMPLETED', '[]', '{}',
        repeat('0', 64), FALSE, FALSE,
        NOW() - (g || ' seconds')::INTERVAL,
        NOW() - (g || ' seconds')::INTERVAL,
        :payload
    FROM generate_series(1, :count) AS g
""")

_DELETE_SEEDED_SQL = text("DELETE FROM horsies_tasks WHERE id LIKE :prefix || '%'")

_INSERT_WORKFLOWS_SQL = text("""
    INSERT INTO horsies_workflows (
        id, name, status, on_error, output_task_index, depth,
        root_workflow_id, sent_at, created_at, started_at, updated_at
    )
    SELECT
        'wf-' || :prefix || g, 'perf.workflow', 'RUNNING', 'FAIL', 0, 0,
        'wf-' || :prefix || g, NOW(), NOW(), NOW(), NOW()
    FROM generate_series(1, :count) AS g
""")

_INSERT_WORKFLOW_TASKS_SQL = text("""
    INSERT INTO horsies_workflow_tasks (
        id, workflow_id, task_index, node_id, task_name, task_args,
        task_kwargs, queue_name, priority, dependencies, allow_failed_deps,
        join_type, is_subworkflow, status, task_id, created_at
    )
    SELECT
        'wt-' || :prefix || g, 'wf-' || :prefix || g, 0, 'node_0',
        'perf.task', '[]', '{}', 'default', 100, '{}', FALSE, 'all', FALSE,
        'RUNNING', :prefix || g, NOW()
    FROM generate_series(1, :count) AS g
""")

_DELETE_WORKFLOW_TASKS_SQL = text(
    "DELETE FROM horsies_workflow_tasks WHERE task_id LIKE :prefix || '%'"
)
_DELETE_WORKFLOWS_SQL = text(
    "DELETE FROM horsies_workflows WHERE id LIKE 'wf-' || :prefix || '%'"
)

type Seed = Callable[[Connection, str, int], None]
type Cleanup = Callable[[Connection, str], None]


@dataclass(frozen=True, slots=True)
class Invocation:
    """One actual terminal statement with the parameters its caller supplies."""

    statement: TextClause
    parameters: dict[str, Any]
    candidate: bool
    operation: str
    command: TerminalizationCommand | None = None


type InvocationFactory = Callable[[str], Invocation]


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
    p50_budget: Budget
    p99_budget: Budget
    batch_size: int
    seed: Seed
    cleanup: Cleanup
    baseline: Callable[[Connection], int]
    candidate: Callable[[Connection], int] | None
    exact_client_statements_per_operation: int | None = 1
    exact_write_transactions_per_operation: int = 1


type Scenario = SingleRowScenario | BatchScenario


def id_prefix(scenario_name: str, role: str) -> str:
    """A short, stable id prefix.

    Task ids are 36 characters — the width of the uuid the runtime writes —
    and scenario names are longer than the budget that leaves. Hashing the
    name keeps the prefix short, keeps it stable across runs so a crashed run
    can be cleaned up, and keeps two scenarios from sharing rows.
    """
    digest = hashlib.sha256(scenario_name.encode('utf-8')).hexdigest()[:6]
    return f'pf-{digest}-{role}-'


def payload_of(size_bytes: int) -> str:
    """A result payload of a stated size, as the child would have written it."""
    filler = 'x' * max(0, size_bytes - 16)
    return f'{{"ok": "{filler}"}}'


def seed_running_tasks(
    connection: Connection,
    prefix: str,
    count: int,
    *,
    is_workflow_task: bool = False,
) -> None:
    connection.execute(
        _INSERT_RUNNING_SQL,
        {
            'prefix': prefix,
            'count': count,
            'worker_id': WORKER_ID,
            'claimed_at': CLAIMED_AT,
            'is_workflow_task': is_workflow_task,
        },
    )
    connection.commit()


def seed_expired_pending_tasks(
    connection: Connection,
    prefix: str,
    count: int,
) -> None:
    connection.execute(
        _INSERT_PENDING_EXPIRED_SQL,
        {'prefix': prefix, 'count': count},
    )
    connection.commit()


def seed_terminal_ballast(
    connection: Connection,
    *,
    prefix: str,
    count: int,
    payload_bytes: int,
) -> None:
    connection.execute(
        _INSERT_TERMINAL_BALLAST_SQL,
        {'prefix': prefix, 'count': count, 'payload': payload_of(payload_bytes)},
    )
    connection.commit()


def seed_workflow_success_tasks(
    connection: Connection,
    prefix: str,
    count: int,
) -> None:
    """One-node RUNNING workflows whose only task is ready to finish."""
    seed_running_tasks(
        connection,
        prefix,
        count,
        is_workflow_task=True,
    )
    connection.execute(_INSERT_WORKFLOWS_SQL, {'prefix': prefix, 'count': count})
    connection.execute(
        _INSERT_WORKFLOW_TASKS_SQL,
        {'prefix': prefix, 'count': count},
    )
    connection.commit()


def delete_seeded(connection: Connection, prefix: str) -> None:
    connection.execute(_DELETE_SEEDED_SQL, {'prefix': prefix})
    connection.commit()


def delete_workflow_seeded(connection: Connection, prefix: str) -> None:
    connection.execute(_DELETE_WORKFLOW_TASKS_SQL, {'prefix': prefix})
    connection.execute(_DELETE_WORKFLOWS_SQL, {'prefix': prefix})
    connection.execute(_DELETE_SEEDED_SQL, {'prefix': prefix})
    connection.commit()


def analyze(connection: Connection) -> None:
    """Statistics must describe the seeded table, not the empty one."""
    connection.execute(text('ANALYZE horsies_tasks'))
    connection.commit()


def task_ids(prefix: str, count: int) -> Iterator[str]:
    for index in range(1, count + 1):
        yield f'{prefix}{index}'


def _baseline_applied(result: Any, *, operation: str) -> int:
    if result.fetchone() is None:
        raise RuntimeError(f'{operation} did not transition its seeded task')
    return 1


def _candidate_applied(
    result: Any,
    *,
    operation: str,
    command: TerminalizationCommand,
) -> int:
    rows = result.mappings().all()
    if len(rows) != 1:
        raise RuntimeError(f'{operation} returned {len(rows)} rows; expected one')
    outcome = decode_outcome_row({str(key): value for key, value in rows[0].items()})
    if not isinstance(outcome, Applied):
        raise RuntimeError(
            f'{operation} returned {type(outcome).__name__} '
            'for a seeded eligible task'
        )
    _log_outcome(command, outcome)
    return 1


def _execute_invocation(connection: Connection, invocation: Invocation) -> int:
    result = connection.execute(invocation.statement, invocation.parameters)
    if invocation.candidate:
        if invocation.command is None:
            raise RuntimeError(
                f'{invocation.operation} has no command for outcome handling'
            )
        return _candidate_applied(
            result,
            operation=invocation.operation,
            command=invocation.command,
        )
    return _baseline_applied(result, operation=invocation.operation)


def _fused_invocation(
    payload: str,
    *,
    candidate: bool,
) -> InvocationFactory:
    def build(task_id: str) -> Invocation:
        if candidate:
            command = CompleteTaskFused(
                task_id=task_id,
                fence=OwnedClaim(
                    worker_id=WORKER_ID,
                    claimed_at=CLAIMED_AT,
                ),
                result_json=payload,
                notify_channel='task_queue_default',
                notify_payload=f'capacity:{task_id}',
            )
            statement, parameters = call_for(command)
            return Invocation(
                statement=statement,
                parameters=dict(parameters),
                candidate=True,
                operation='fused completion operation',
                command=command,
            )
        return Invocation(
            statement=FINALIZE_TASK_COMPLETED_SQL,
            parameters={
                'id': task_id,
                'wid': WORKER_ID,
                'result_json': payload,
                'notify_channel': 'task_queue_default',
                'notify_payload': f'capacity:{task_id}',
                'claimed_at': CLAIMED_AT,
            },
            candidate=False,
            operation='fused completion statement',
        )

    return build


def _locked_completion_invocation(
    payload: str,
    *,
    candidate: bool,
) -> InvocationFactory:
    def build(task_id: str) -> Invocation:
        if candidate:
            command = CompleteLockedTask(
                task_id=task_id,
                fence=PriorLockedRead(worker_id=WORKER_ID),
                result_json=payload,
            )
            statement, parameters = call_for(command)
            return Invocation(
                statement=statement,
                parameters=dict(parameters),
                candidate=True,
                operation='locked completion operation',
                command=command,
            )
        return Invocation(
            statement=MARK_TASK_COMPLETED_SQL,
            parameters={'id': task_id, 'wid': WORKER_ID, 'result_json': payload},
            candidate=False,
            operation='locked completion statement',
        )

    return build


_FAILURE_RESULT = serialize_error_payload(
    TaskResult(
        err=TaskError(
            error_code='PERF_FAILURE',
            message='terminal application failure measurement',
        )
    )
)


def _failure_invocation(*, candidate: bool) -> InvocationFactory:
    def build(task_id: str) -> Invocation:
        if candidate:
            command = FailLockedTask(
                task_id=task_id,
                fence=PriorLockedRead(worker_id=WORKER_ID),
                result_json=_FAILURE_RESULT,
                error_code='PERF_FAILURE',
                failed_reason=None,
            )
            statement, parameters = call_for(command)
            return Invocation(
                statement=statement,
                parameters=dict(parameters),
                candidate=True,
                operation='terminal application failure operation',
                command=command,
            )
        return Invocation(
            statement=MARK_TASK_FAILED_SQL,
            parameters={
                'id': task_id,
                'wid': WORKER_ID,
                'result_json': _FAILURE_RESULT,
                'error_code': 'PERF_FAILURE',
            },
            candidate=False,
            operation='terminal application failure statement',
        )

    return build


def _run_terminal_invocation(
    invocation_for: InvocationFactory,
) -> Callable[[Connection, str], int]:
    def run(connection: Connection, task_id: str) -> int:
        transitioned = _execute_invocation(connection, invocation_for(task_id))
        connection.commit()
        return transitioned

    return run


def _upsert_completed_attempt(connection: Connection, task_id: str) -> None:
    context = connection.execute(
        SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
        {'id': task_id, 'wid': WORKER_ID, 'claimed_at': CLAIMED_AT},
    ).fetchone()
    if context is None:
        raise RuntimeError('workflow completion could not lock its seeded task')
    db_now = context.db_now
    connection.execute(
        UPSERT_TASK_ATTEMPT_SQL,
        {
            'task_id': task_id,
            'attempt': (context.retry_count or 0) + 1,
            'outcome': 'COMPLETED',
            'will_retry': False,
            'started_at': context.started_at or db_now,
            'finished_at': db_now,
            'error_code': None,
            'error_message': None,
            'failed_reason': None,
            'worker_id': context.claimed_by_worker_id,
            'worker_hostname': context.worker_hostname,
            'worker_pid': context.worker_pid,
            'worker_process_name': context.worker_process_name,
        },
    )


class _SyncConnectionAsAsyncSession:
    """The workflow engine's async execute seam over the measured connection."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.info: dict[str, Any] = {}

    async def execute(self, statement: Any, parameters: Any = None) -> Any:
        return self.connection.execute(statement, parameters or {})


_WORKFLOW_RUNNER = asyncio.Runner()
atexit.register(_WORKFLOW_RUNNER.close)
_WORKFLOW_RESULT: TaskResult[int, Any] = TaskResult(ok=1)
_WORKFLOW_RESULT_JSON = dumps_json(encode_task_result(_WORKFLOW_RESULT, int)).unwrap()


def _run_workflow_success(
    invocation_for: InvocationFactory,
) -> Callable[[Connection, str], int]:
    def run(connection: Connection, task_id: str) -> int:
        _upsert_completed_attempt(connection, task_id)
        _execute_invocation(connection, invocation_for(task_id))
        connection.commit()

        session = _SyncConnectionAsAsyncSession(connection)
        _WORKFLOW_RUNNER.run(
            on_workflow_task_complete(
                cast(Any, session),
                task_id,
                _WORKFLOW_RESULT,
                None,
                task_name='perf.task',
            )
        )
        connection.execute(
            NOTIFY_TASK_QUEUE_SQL,
            {'c2': 'task_queue_default', 'p': f'capacity:{task_id}'},
        )
        connection.commit()
        return 1

    return run


def _run_pending_expiry(batch_size: int) -> Callable[[Connection], int]:
    def run(connection: Connection) -> int:
        result = connection.execute(
            EXPIRE_PENDING_TASKS_SQL,
            {
                'result': payload_of(200),
                'error_code': 'TASK_EXPIRED',
                'batch_size': batch_size,
            },
        )
        transitioned = int(result.rowcount or 0)
        connection.commit()
        if transitioned != batch_size:
            raise RuntimeError(
                f'pending expiry transitioned {transitioned} of {batch_size} rows'
            )
        return transitioned

    return run


# The batch bound is imported rather than restated: a round number here would
# measure a batch the system never sends, and a copied one would keep measuring
# the old batch after the runtime changed its mind.
PRODUCTION_EXPIRE_BATCH_SIZE = _EXPIRE_BATCH_SIZE

_FUSED_SMALL_BASELINE = _fused_invocation(payload_of(200), candidate=False)
_FUSED_SMALL_CANDIDATE = _fused_invocation(payload_of(200), candidate=True)
_FUSED_LARGE_BASELINE = _fused_invocation(
    payload_of(1024 * 1024),
    candidate=False,
)
_FUSED_LARGE_CANDIDATE = _fused_invocation(
    payload_of(1024 * 1024),
    candidate=True,
)
_LOCKED_BASELINE = _locked_completion_invocation(payload_of(200), candidate=False)
_LOCKED_CANDIDATE = _locked_completion_invocation(payload_of(200), candidate=True)
_FAILURE_BASELINE = _failure_invocation(candidate=False)
_FAILURE_CANDIDATE = _failure_invocation(candidate=True)
_WORKFLOW_BASELINE = _locked_completion_invocation(
    _WORKFLOW_RESULT_JSON,
    candidate=False,
)
_WORKFLOW_CANDIDATE = _locked_completion_invocation(
    _WORKFLOW_RESULT_JSON,
    candidate=True,
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
        baseline=_run_terminal_invocation(_FUSED_SMALL_BASELINE),
        candidate=_run_terminal_invocation(_FUSED_SMALL_CANDIDATE),
        baseline_invocation=_FUSED_SMALL_BASELINE,
        candidate_invocation=_FUSED_SMALL_CANDIDATE,
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
        baseline=_run_terminal_invocation(_FUSED_LARGE_BASELINE),
        candidate=_run_terminal_invocation(_FUSED_LARGE_CANDIDATE),
        baseline_invocation=_FUSED_LARGE_BASELINE,
        candidate_invocation=_FUSED_LARGE_CANDIDATE,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='locked-completion',
        description='task completion transition under a prior locked read',
        p50_budget=SINGLE_ROW_P50,
        p99_budget=SINGLE_ROW_P99,
        payload_bytes=200,
        seed=seed_running_tasks,
        cleanup=delete_seeded,
        baseline=_run_terminal_invocation(_LOCKED_BASELINE),
        candidate=_run_terminal_invocation(_LOCKED_CANDIDATE),
        baseline_invocation=_LOCKED_BASELINE,
        candidate_invocation=_LOCKED_CANDIDATE,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='terminal-application-failure',
        description='terminal application failure transition, retry already denied',
        p50_budget=SINGLE_ROW_P50,
        p99_budget=SINGLE_ROW_P99,
        payload_bytes=len(_FAILURE_RESULT.encode('utf-8')),
        seed=seed_running_tasks,
        cleanup=delete_seeded,
        baseline=_run_terminal_invocation(_FAILURE_BASELINE),
        candidate=_run_terminal_invocation(_FAILURE_CANDIDATE),
        baseline_invocation=_FAILURE_BASELINE,
        candidate_invocation=_FAILURE_CANDIDATE,
        exact_client_statements_per_operation=1,
    ),
    SingleRowScenario(
        name='workflow-success-phase2',
        description=(
            'one-node workflow success through terminal persistence, phase 2, '
            'workflow completion and queue wake'
        ),
        p50_budget=SINGLE_ROW_P50,
        p99_budget=Budget(fraction=0.15, floor_ms=2.0),
        payload_bytes=len(_WORKFLOW_RESULT_JSON.encode('utf-8')),
        seed=seed_workflow_success_tasks,
        cleanup=delete_workflow_seeded,
        baseline=_run_workflow_success(_WORKFLOW_BASELINE),
        candidate=_run_workflow_success(_WORKFLOW_CANDIDATE),
        baseline_invocation=_WORKFLOW_BASELINE,
        candidate_invocation=_WORKFLOW_CANDIDATE,
        exact_write_transactions_per_operation=2,
    ),
    BatchScenario(
        name='pending-expiry-batch',
        description='deadline expiry of unclaimed tasks, one bounded batch',
        p50_budget=BATCH_P50,
        p99_budget=BATCH_P99,
        batch_size=PRODUCTION_EXPIRE_BATCH_SIZE,
        seed=seed_expired_pending_tasks,
        cleanup=delete_seeded,
        baseline=_run_pending_expiry(PRODUCTION_EXPIRE_BATCH_SIZE),
        candidate=None,
    ),
)


def scenario_by_name(name: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    known = ', '.join(s.name for s in SCENARIOS)
    raise KeyError(f'unknown scenario {name!r}; known scenarios: {known}')
