"""The paths under measurement, seeded and executed as the runtime executes them.

Each scenario runs the statement the library actually issues, with the
parameters its caller actually passes. A benchmark that reimplements the
statement measures the benchmark.

Two shapes, because the workloads genuinely differ: a single-row operation
consumes one seeded row per call, and a batch operation selects its own rows
under a bound and consumes many. Reporting a batch as though it were one row
would flatter it by the batch size.

The candidate side is the database-owned implementation. Until it exists, a
scenario runs baseline against baseline — which is not a placeholder but the
harness's own control: an equal comparison must produce an interval containing
zero, and a harness that cannot report "no difference" when there is none
cannot be trusted to report one when there is.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection

from horsies.core.brokers.postgres import (
    EXPIRE_PENDING_TASKS_SQL,
    _EXPIRE_BATCH_SIZE,
)
from horsies.core.worker.sql import (
    FINALIZE_TASK_COMPLETED_SQL,
    MARK_TASK_COMPLETED_SQL,
)
from tests.perf.statistics import Budget

WORKER_ID = 'perf-harness'

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
        repeat('0', 64), FALSE, TRUE, :worker_id, NOW() - INTERVAL '1 minute',
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


@dataclass(frozen=True, slots=True)
class SingleRowScenario:
    """One seeded row consumed per measured operation."""

    name: str
    description: str
    p50_budget: Budget
    p99_budget: Budget
    payload_bytes: int
    baseline: Callable[[Connection, str], None]
    candidate: Callable[[Connection, str], None] | None


@dataclass(frozen=True, slots=True)
class BatchScenario:
    """One bounded batch consumed per measured operation."""

    name: str
    description: str
    p50_budget: Budget
    p99_budget: Budget
    batch_size: int
    baseline: Callable[[Connection], None]
    candidate: Callable[[Connection], None] | None


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
    *,
    prefix: str,
    count: int,
) -> None:
    connection.execute(
        _INSERT_RUNNING_SQL,
        {'prefix': prefix, 'count': count, 'worker_id': WORKER_ID},
    )
    connection.commit()


def seed_expired_pending_tasks(
    connection: Connection,
    *,
    prefix: str,
    count: int,
) -> None:
    connection.execute(
        _INSERT_PENDING_EXPIRED_SQL, {'prefix': prefix, 'count': count},
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


def delete_seeded(connection: Connection, *, prefix: str) -> None:
    connection.execute(_DELETE_SEEDED_SQL, {'prefix': prefix})
    connection.commit()


def analyze(connection: Connection) -> None:
    """Statistics must describe the seeded table, not the empty one."""
    connection.execute(text('ANALYZE horsies_tasks'))
    connection.commit()


def task_ids(prefix: str, count: int) -> Iterator[str]:
    for index in range(1, count + 1):
        yield f'{prefix}{index}'


def _run_fused_completion(payload: str) -> Callable[[Connection, str], None]:
    def run(connection: Connection, task_id: str) -> None:
        connection.execute(
            FINALIZE_TASK_COMPLETED_SQL,
            {
                'id': task_id,
                'wid': WORKER_ID,
                'result_json': payload,
                'notify_channel': 'task_queue_default',
                'notify_payload': f'capacity:{task_id}',
                'claimed_at': None,
            },
        )
        connection.commit()

    return run


def _run_locked_completion(payload: str) -> Callable[[Connection, str], None]:
    def run(connection: Connection, task_id: str) -> None:
        connection.execute(
            MARK_TASK_COMPLETED_SQL,
            {'id': task_id, 'wid': WORKER_ID, 'result_json': payload},
        )
        connection.commit()

    return run


def _run_pending_expiry(batch_size: int) -> Callable[[Connection], None]:
    def run(connection: Connection) -> None:
        connection.execute(
            EXPIRE_PENDING_TASKS_SQL,
            {
                'result': payload_of(200),
                'error_code': 'TASK_EXPIRED',
                'batch_size': batch_size,
            },
        )
        connection.commit()

    return run


# The batch bound is imported rather than restated: a round number here would
# measure a batch the system never sends, and a copied one would keep measuring
# the old batch after the runtime changed its mind.
PRODUCTION_EXPIRE_BATCH_SIZE = _EXPIRE_BATCH_SIZE

SCENARIOS: tuple[Scenario, ...] = (
    SingleRowScenario(
        name='fused-completion-small-result',
        description='plain task completion, one statement, small result',
        p50_budget=FUSED_P50,
        p99_budget=FUSED_P99,
        payload_bytes=200,
        baseline=_run_fused_completion(payload_of(200)),
        candidate=None,
    ),
    SingleRowScenario(
        name='fused-completion-1mib-result',
        description='plain task completion at the result-size warning threshold',
        p50_budget=FUSED_P50,
        p99_budget=FUSED_P99,
        payload_bytes=1024 * 1024,
        baseline=_run_fused_completion(payload_of(1024 * 1024)),
        candidate=None,
    ),
    SingleRowScenario(
        name='locked-completion',
        description='workflow-node completion under a prior locked read',
        p50_budget=SINGLE_ROW_P50,
        p99_budget=SINGLE_ROW_P99,
        payload_bytes=200,
        baseline=_run_locked_completion(payload_of(200)),
        candidate=None,
    ),
    BatchScenario(
        name='pending-expiry-batch',
        description='deadline expiry of unclaimed tasks, one bounded batch',
        p50_budget=BATCH_P50,
        p99_budget=BATCH_P99,
        batch_size=PRODUCTION_EXPIRE_BATCH_SIZE,
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
