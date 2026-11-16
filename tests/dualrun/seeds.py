"""Seed rows of the operation dual-runs: plain task rows in a chosen live state.

Every time is the database clock plus an offset, so two runs of one seed
hold the same logical state. Each row gets a new UUIDv7 id and a fingerprint
derived from it, as a plain send writes. Task names must be distinct inside
one seed: the snapshot labels rows in task name order.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Final, LiteralString

from sqlalchemy.ext.asyncio import create_async_engine

from tests.dualrun.capture import Connection

DEFAULT_LEASE: Final = timedelta(seconds=30)
SEED_HOST: Final = 'host-a'
SEED_PID: Final = 4242
SEED_PROCESS: Final = 'worker-4242'


class SeedStatus(Enum):
    """The live status of a seeded row."""

    PENDING = 'PENDING'
    CLAIMED = 'CLAIMED'
    RUNNING = 'RUNNING'


@dataclass(frozen=True, slots=True)
class TaskSeed:
    """One plain task row.

    ``claimed_ago`` is required for CLAIMED and RUNNING and absent for PENDING.
    A CLAIMED row's lease ends ``lease`` after its claim; a RUNNING row starts
    ``started_ago`` before now and carries the seed host and pid, and a
    finalizing marker ``finalizing_ago`` before now when that is set.
    """

    task_name: str
    status: SeedStatus
    worker_id: str | None = None
    claimed_ago: timedelta | None = None
    lease: timedelta = DEFAULT_LEASE
    started_ago: timedelta | None = None
    queue_name: str = 'default'
    execution: str = 'SHARED_ASYNC'
    retry_count: int = 0
    max_retries: int = 0
    task_options: str | None = None
    good_until_in: timedelta | None = None
    finalizing_ago: timedelta | None = None


@dataclass(frozen=True, slots=True)
class SeededTask:
    """The raw facts of one seeded row."""

    task_id: uuid.UUID
    claimed_at: datetime | None


_INSERT_TASK_SQL: Final[LiteralString] = """
    WITH clock AS (SELECT now() AS at)
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    )
    SELECT ids.id, %(task_name)s, %(queue_name)s, 50, '{}', %(task_options)s, %(execution)s, 'standard_30d',
        clock.at - interval '1 hour',
        clock.at - interval '1 hour',
        CASE WHEN %(is_workflow_task)s THEN NULL ELSE 1 END,
        CASE WHEN %(is_workflow_task)s THEN NULL ELSE sha256(convert_to(ids.id::text, 'UTF8')) END,
        NULL, %(status)s::text,
        clock.at - interval '1 hour',
        clock.at - %(claimed_ago)s::interval, %(worker_id)s::text,
        CASE WHEN %(status)s::text = 'CLAIMED' THEN clock.at - %(claimed_ago)s::interval + %(lease)s::interval END,
        clock.at - %(started_ago)s::interval, clock.at - %(finalizing_ago)s::interval,
        CASE WHEN %(finalizing_ago)s::interval IS NOT NULL THEN %(worker_id)s::text END,
        %(pid)s, %(host)s, %(process)s, %(retry_count)s, %(max_retries)s, NULL,
        clock.at + %(good_until_in)s::interval,
        %(is_workflow_task)s, clock.at - interval '1 hour', TRUE, NULL, NULL
    FROM (SELECT horsies_uuidv7() AS id) ids, clock
    RETURNING id, claimed_at
"""


def _seed_parameters(seed: TaskSeed, is_workflow_task: bool) -> dict[str, object]:
    running = seed.status is SeedStatus.RUNNING
    return {
        'is_workflow_task': is_workflow_task,
        'task_name': seed.task_name,
        'queue_name': seed.queue_name,
        'task_options': seed.task_options,
        'execution': seed.execution,
        'status': seed.status.value,
        'claimed_ago': seed.claimed_ago,
        'worker_id': seed.worker_id,
        'lease': seed.lease,
        'started_ago': seed.started_ago,
        'finalizing_ago': seed.finalizing_ago,
        'pid': SEED_PID if running else None,
        'host': SEED_HOST if running else None,
        'process': SEED_PROCESS if running else None,
        'retry_count': seed.retry_count,
        'max_retries': seed.max_retries,
        'good_until_in': seed.good_until_in,
    }


def insert_task(connection: Connection, seed: TaskSeed) -> SeededTask:
    """Insert one plain task row; raises ``psycopg.Error`` on a database error."""
    return _insert_task_row(connection, seed, is_workflow_task=False)


def _insert_task_row(
    connection: Connection, seed: TaskSeed, *, is_workflow_task: bool
) -> SeededTask:
    assert seed.finalizing_ago is None or seed.status is SeedStatus.RUNNING, (
        f'only a RUNNING seed carries a finalizing marker: {seed!r}'
    )
    match seed.status, seed.claimed_ago, seed.worker_id, seed.started_ago:
        case SeedStatus.PENDING, None, None, None:
            pass
        case SeedStatus.CLAIMED, timedelta(), str(), None:
            pass
        case SeedStatus.RUNNING, timedelta(), str(), timedelta():
            pass
        case _:
            raise AssertionError(f'a seed whose fields do not fit its status: {seed!r}')
    record = connection.execute(
        _INSERT_TASK_SQL, _seed_parameters(seed, is_workflow_task)
    ).fetchone()
    match record:
        case (uuid.UUID() as task_id, datetime() | None as claimed_at):
            return SeededTask(task_id=task_id, claimed_at=claimed_at)
        case _:
            raise AssertionError(f'the seed insert returned {record!r}')


# ---- Workflow tasks ----


@dataclass(frozen=True, slots=True)
class WorkflowLink:
    """The workflow and node of a seeded workflow task.

    ``node_status`` None seeds the task with no node row (an orphan).
    """

    workflow_status: str
    node_status: str | None


_INSERT_WORKFLOW_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result,
        error, parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at,
        created_at, started_at, completed_at, updated_at
    )
    SELECT ids.id, %(name)s, 'dualrun.v1', %(status)s::text, 'fail', 0, NULL, NULL,
        NULL, NULL, NULL, ids.id, 0, now() - interval '1 hour',
        now() - interval '1 hour', now() - interval '1 hour', NULL, now() - interval '1 hour'
    FROM (SELECT horsies_uuidv7() AS id) ids
    RETURNING id
"""

_INSERT_NODE_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflow_tasks (
        downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
        dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type, min_success,
        task_options, max_retries, execution, good_until, retention_class_key, status, task_id,
        is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key,
        sub_workflow_summary, result, error, created_at, started_at, completed_at
    )
    SELECT 'NONE', horsies_uuidv7(), %(workflow_id)s, 0, %(task_name)s, %(task_name)s, '{}', %(queue_name)s, 50,
        ARRAY[]::integer[], NULL, NULL, FALSE, 'all', NULL,
        NULL, 0, %(execution)s, NULL, 'standard_30d', %(status)s::text, %(task_id)s,
        FALSE, NULL, NULL, NULL,
        NULL, NULL, NULL, now() - interval '1 hour',
        CASE WHEN %(status)s::text = 'RUNNING' THEN now() - interval '1 minute' END, NULL
"""


def insert_workflow_task(
    connection: Connection, seed: TaskSeed, link: WorkflowLink
) -> SeededTask:
    """A workflow of one node and its task row; raises ``psycopg.Error`` on a database error.

    The workflow is named after the task, so labels follow the task name.
    """
    record = connection.execute(
        _INSERT_WORKFLOW_SQL, {'name': seed.task_name, 'status': link.workflow_status}
    ).fetchone()
    assert record is not None, 'the workflow insert returns its id'
    (workflow_id,) = record
    task = _insert_task_row(connection, seed, is_workflow_task=True)
    if link.node_status is not None:
        _ = connection.execute(
            _INSERT_NODE_SQL,
            {
                'workflow_id': workflow_id,
                'task_name': seed.task_name,
                'queue_name': seed.queue_name,
                'execution': seed.execution,
                'status': link.node_status,
                'task_id': task.task_id,
            },
        )
    return task


# ---- History and heartbeat coverage ----


class SeedCoverage(Enum):
    """The partition coverage a seed needs before its rows."""

    NONE = 'no history or heartbeat leaves'
    HISTORY_AND_BEATS = 'history leaves, and heartbeat leaves from the previous hour on'
    """The production coverage pass (``tests.integration.history_seeding``)
    and the hourly heartbeat leaf of the previous hour, so a beat dated up to
    one hour back has a leaf whatever the minute of the run."""


async def _cover(url: str) -> None:
    from horsies.core.history.heartbeats.partitioning import (
        CreateHourlyHeartbeatLeaf,
        create_hourly_heartbeat_leaf,
        hourly_leaf_ref,
    )
    from horsies.core.history.outcomes import LeafAlreadyConformant, LeafCreated
    from horsies.core.history.partitions.catalog import database_now
    from tests.integration.history_seeding import ensure_history_seedable

    engine = create_async_engine(url.replace('postgresql://', 'postgresql+psycopg://', 1))
    try:
        async with engine.begin() as connection:
            await ensure_history_seedable(connection)
            now = await database_now(connection)
            previous_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            match await create_hourly_heartbeat_leaf(
                connection,
                CreateHourlyHeartbeatLeaf(leaf=hourly_leaf_ref(previous_hour)),
            ):
                case LeafCreated() | LeafAlreadyConformant():
                    pass
                case refused:
                    raise AssertionError(f'the previous hour heartbeat leaf: {refused!r}')
    finally:
        await engine.dispose()


def cover(url: str, coverage: SeedCoverage) -> None:
    """Create the leaves of ``coverage``; raises ``psycopg.Error`` or ``SQLAlchemyError`` on a database error."""
    match coverage:
        case SeedCoverage.NONE:
            pass
        case SeedCoverage.HISTORY_AND_BEATS:
            asyncio.run(_cover(url))


_INSERT_BEAT_SQL: Final[LiteralString] = """
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    VALUES (%(task_id)s, %(sender_id)s, %(role)s::text, now() - %(ago)s::interval, %(host)s, %(pid)s)
"""


class BeatRole(Enum):
    """The sender of a heartbeat."""

    CLAIMER = 'claimer'
    RUNNER = 'runner'


def insert_beat(
    connection: Connection,
    task_id: uuid.UUID,
    worker_id: str,
    role: BeatRole,
    ago: timedelta,
) -> None:
    """One heartbeat ``ago`` before now. Needs ``SeedCoverage.HISTORY_AND_BEATS``; ``ago`` at most one hour.

    The sender is the worker id itself, so the beat and the row's claim
    columns share one label.
    """
    assert timedelta(0) <= ago <= timedelta(hours=1), f'a beat {ago} ago has no leaf'
    _ = connection.execute(
        _INSERT_BEAT_SQL,
        {
            'task_id': task_id,
            'sender_id': worker_id,
            'role': role.value,
            'ago': ago,
            'host': SEED_HOST,
            'pid': SEED_PID,
        },
    )
