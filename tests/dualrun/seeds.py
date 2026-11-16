"""Seed rows of the operation dual-runs: plain task rows in a chosen live state.

Every time is the database clock plus an offset, so two runs of one seed
hold the same logical state. Each row gets a new UUIDv7 id and a fingerprint
derived from it, as a plain send writes. Task names must be distinct inside
one seed: the snapshot labels rows in task name order.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Final, LiteralString

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
    ``started_ago`` before now and carries the seed host and pid.
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
        clock.at - interval '1 hour', 1, sha256(convert_to(ids.id::text, 'UTF8')), NULL, %(status)s::text,
        clock.at - interval '1 hour',
        clock.at - %(claimed_ago)s::interval, %(worker_id)s::text,
        CASE WHEN %(status)s::text = 'CLAIMED' THEN clock.at - %(claimed_ago)s::interval + %(lease)s::interval END,
        clock.at - %(started_ago)s::interval, NULL, NULL,
        %(pid)s, %(host)s, %(process)s, %(retry_count)s, %(max_retries)s, NULL,
        clock.at + %(good_until_in)s::interval,
        FALSE, clock.at - interval '1 hour', TRUE, NULL, NULL
    FROM (SELECT horsies_uuidv7() AS id) ids, clock
    RETURNING id, claimed_at
"""


def _seed_parameters(seed: TaskSeed) -> dict[str, object]:
    running = seed.status is SeedStatus.RUNNING
    return {
        'task_name': seed.task_name,
        'queue_name': seed.queue_name,
        'task_options': seed.task_options,
        'execution': seed.execution,
        'status': seed.status.value,
        'claimed_ago': seed.claimed_ago,
        'worker_id': seed.worker_id,
        'lease': seed.lease,
        'started_ago': seed.started_ago,
        'pid': SEED_PID if running else None,
        'host': SEED_HOST if running else None,
        'process': SEED_PROCESS if running else None,
        'retry_count': seed.retry_count,
        'max_retries': seed.max_retries,
        'good_until_in': seed.good_until_in,
    }


def insert_task(connection: Connection, seed: TaskSeed) -> SeededTask:
    """Insert one row; raises ``psycopg.Error`` on a database error."""
    match seed.status, seed.claimed_ago, seed.worker_id, seed.started_ago:
        case SeedStatus.PENDING, None, None, None:
            pass
        case SeedStatus.CLAIMED, timedelta(), str(), None:
            pass
        case SeedStatus.RUNNING, timedelta(), str(), timedelta():
            pass
        case _:
            raise AssertionError(f'a seed whose fields do not fit its status: {seed!r}')
    record = connection.execute(_INSERT_TASK_SQL, _seed_parameters(seed)).fetchone()
    match record:
        case (uuid.UUID() as task_id, datetime() | None as claimed_at):
            return SeededTask(task_id=task_id, claimed_at=claimed_at)
        case _:
            raise AssertionError(f'the seed insert returned {record!r}')
