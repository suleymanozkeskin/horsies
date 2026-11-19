"""Self-comparison runs: one sequence of real writes into two fresh databases.

Every difference between the two snapshots of a self-comparison is a defect
of the catalog (a column whose rule keeps a run-dependent value) or of the
snapshot code, because both databases ran the same writes.

Two sequences:

- ``run_function_calls``:
  ``core/src/sql_tests/cases/baseline_function_calls.sql``,
  which calls every SQL function of the baseline (claim, completions,
  failures with retry, expiry, cancels, phase 2, history moves, key
  reservations). The fixed ids of the script are replaced by new UUIDv7
  values from the database for each run, and the script commits.
- ``run_seeded_history``: the production coverage pass (retention class,
  daily history leaves, hourly heartbeat leaves, staged readers), a task
  moved into a daily leaf, a heartbeat, and a detached leaf.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, LiteralString

import psycopg
from psycopg import sql
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from horsies.core.types.result import Err, Ok, Result
from tests.parity.snapshot.capture import (
    CaptureError,
    ClockReadFailed,
    Connection,
    capture_snapshot,
    read_database_clock,
)
from tests.parity.snapshot.catalog import BASELINE_CATALOG
from tests.parity.snapshot.databases import (
    DatabaseSetupFailed,
    DualRunDatabase,
    install_baseline_schema,
)
from tests.parity.snapshot.normalize import NormalizedSnapshot
from tests.parity.snapshot.values import (
    CalendarUnit,
    RunWindow,
    RunWindowInvalid,
    make_run_window,
)
from tests.sqlalchemy_url import sqlalchemy_url

FUNCTION_CALLS_SQL: Final = (
    Path(__file__).parents[3]
    / 'core'
    / 'src'
    / 'sql_tests'
    / 'cases'
    / 'baseline_function_calls.sql'
)
_FIXED_ID: Final = re.compile(r"'(00000000-0000-7000-8000-[0-9a-f]{12})'")
_PUBLISH_CHECK: Final = (
    "SELECT expect_value('publish_history_readers', "
    "(SELECT outcome FROM horsies_publish_history_readers()), 'PUBLISHED');"
)
"""The script's check of the reader publication. The core's SQL tests run
the script on the bare baseline, where the call publishes the readers."""
_PUBLISH_CHECK_AFTER_INSTALL: Final = _PUBLISH_CHECK.replace(
    "'PUBLISHED'", "'ALREADY_CURRENT'"
)
"""The same check on a database that the core installed: the install
published the readers, so the call answers ``ALREADY_CURRENT``."""
_NEW_IDS_SQL: Final[LiteralString] = (
    'SELECT horsies_uuidv7() FROM generate_series(1, %(count)s)'
)

SEEDED_CLASS_KEY: Final = 'standard_30d'
_HOUR_SECONDS: Final = 3600


@dataclass(frozen=True, slots=True)
class SelfCheckStepFailed:
    """A step of a self-comparison run failed. The database is left as the step left it."""

    step: str
    cause: str


@dataclass(frozen=True, slots=True)
class SelfCheckRun:
    """The snapshot of one run and its window."""

    snapshot: NormalizedSnapshot
    window: RunWindow


def _failed(
    step: str,
    error: DatabaseSetupFailed
    | ClockReadFailed
    | CaptureError
    | RunWindowInvalid
    | psycopg.Error
    | str,
) -> Err[SelfCheckStepFailed]:
    return Err(SelfCheckStepFailed(step=step, cause=str(error)))


def straddles(window: RunWindow, unit: CalendarUnit) -> bool:
    """Whether the window crosses a day or hour boundary (UTC), so generated names may name either side."""
    match unit:
        case CalendarUnit.DAY:
            return (
                window.start.astimezone(UTC).date() != window.end.astimezone(UTC).date()
            )
        case CalendarUnit.HOUR:
            return (
                window.start.timestamp() // _HOUR_SECONDS
                != window.end.timestamp() // _HOUR_SECONDS
            )


def function_calls_script(fixed_ids: dict[str, str]) -> str:
    """The function-call script for a database that the core installed: each
    fixed id replaced by ``fixed_ids[id]``, and the reader publication
    expected to answer ``ALREADY_CURRENT``."""
    text = FUNCTION_CALLS_SQL.read_text(encoding='utf-8')
    assert text.count(_PUBLISH_CHECK) == 1, (
        f'{FUNCTION_CALLS_SQL.name} checks the reader publication once: {_PUBLISH_CHECK}'
    )
    text = text.replace(_PUBLISH_CHECK, _PUBLISH_CHECK_AFTER_INSTALL)
    return _FIXED_ID.sub(lambda found: f"'{fixed_ids[found.group(1)]}'", text)


def _fresh_ids(connection: Connection, count: int) -> list[str]:
    records = connection.execute(_NEW_IDS_SQL, {'count': count}).fetchall()
    return [str(record[0]) for record in records]


def read_run_start(connection: Connection) -> Result[datetime, SelfCheckStepFailed]:
    """The database clock at the start of a run, before any write of the run."""
    match read_database_clock(connection):
        case Err(failed):
            return _failed('read the run start', failed)
        case Ok(now):
            return Ok(now)


def capture_run(
    connection: Connection, start: datetime
) -> Result[SelfCheckRun, SelfCheckStepFailed]:
    """The snapshot of the database, with the run window from ``start`` to the clock now."""
    match read_database_clock(connection):
        case Err(failed):
            return _failed('read the run end', failed)
        case Ok(end):
            pass
    match make_run_window(start, end):
        case Err(invalid):
            return _failed('make the run window', invalid)
        case Ok(window):
            pass
    match capture_snapshot(connection, BASELINE_CATALOG, window):
        case Err(error):
            return _failed('capture the snapshot', error)
        case Ok(snapshot):
            return Ok(SelfCheckRun(snapshot=snapshot, window=window))


def run_function_calls(
    database: DualRunDatabase,
) -> Result[SelfCheckRun, SelfCheckStepFailed]:
    """Install, run the function-call script with new ids, commit, capture."""
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_run_start(connection):
            case Err(failed):
                return Err(failed)
            case Ok(start):
                pass
        match install_baseline_schema(database):
            case Err(failed):
                return _failed('install the baseline', failed)
            case Ok():
                pass
        fixed = sorted(
            set(_FIXED_ID.findall(FUNCTION_CALLS_SQL.read_text(encoding='utf-8')))
        )
        try:
            fresh = _fresh_ids(connection, len(fixed))
            with connection.transaction():
                _ = connection.execute(
                    function_calls_script(dict(zip(fixed, fresh, strict=True))).encode(
                        'utf-8'
                    )
                )
        except psycopg.Error as exc:
            return _failed('run the function-call script', exc)
        return capture_run(connection, start)


# ---- Seeded history ----

_MOVED_TASK_SQL: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    )
    SELECT ids.id, 'report.build', 'default', 50, '{"day":1}', NULL, 'ISOLATED', %(class_key)s, now(),
        now(), 1, sha256(convert_to(ids.id::text, 'UTF8')), NULL, 'RUNNING', now(),
        now(), 'worker-' || md5(ids.id::text), NULL, now(), NULL, NULL,
        7, 'host-a', 'proc-a', 0, 1, NULL, NULL,
        FALSE, now(), TRUE, NULL, NULL
    FROM (SELECT horsies_uuidv7() AS id) ids
    RETURNING id, claimed_by_worker_id
"""

_COMPLETE_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_complete_task_fused(
        %(task_id)s, %(worker_id)s, (SELECT claimed_at FROM horsies_tasks WHERE id = %(task_id)s),
        '{"rows":12}', TRUE
    )
"""

_FAILED_ATTEMPT_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_finish_failed_attempt(
        %(task_id)s, %(worker_id)s, (SELECT claimed_at FROM horsies_tasks WHERE id = %(task_id)s),
        'RETRY', 0, 1.0, 'SAME_CLASS', '{"err":1}', 'E1', 'reason', 'FAILED', 'timed out', 'reason', 'REFUSE', TRUE
    )
"""

_RECLAIM_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = %(worker_id)s, started_at = now(),
        worker_pid = 7, worker_hostname = 'host-a', worker_process_name = 'proc-a', updated_at = now()
    WHERE id = %(task_id)s
"""

_HEARTBEAT_SQL: Final[LiteralString] = """
    INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid)
    VALUES (%(task_id)s, 'runner', now(), %(worker_id)s, 'host-a', 7)
"""

_LEAF_OF_TASK_SQL: Final[LiteralString] = """
    SELECT tableoid::regclass::text FROM horsies_task_history WHERE task_id = %(task_id)s
"""


def _detach_sql(parent: str, leaf: str) -> sql.Composed:
    return sql.SQL('ALTER TABLE {parent} DETACH PARTITION {leaf}').format(
        parent=sql.Identifier(parent),
        leaf=sql.Identifier(leaf),
    )


_MARK_DETACHED_SQL: Final[LiteralString] = """
    UPDATE horsies_task_history_leaf_catalog SET detached_at = now() WHERE leaf_name = %(leaf)s
"""


async def _seed(url: str) -> None:
    """The production coverage pass: class registration, leaves, heartbeat leaves, staged readers."""
    from tests.history_seeding import ensure_history_seedable

    engine = create_async_engine(sqlalchemy_url(url))
    try:
        async with engine.begin() as connection:
            await ensure_history_seedable(connection)
    finally:
        await engine.dispose()


def seed_history(database: DualRunDatabase) -> Result[None, SelfCheckStepFailed]:
    """Run the production coverage pass on an installed database."""
    try:
        asyncio.run(_seed(database.url))
    except (psycopg.Error, SQLAlchemyError) as exc:
        return _failed('run the coverage pass', str(exc))
    return Ok(None)


def _seeded_writes(connection: Connection) -> None:
    """A task that fails once, runs again and completes into a daily leaf; a heartbeat; that leaf detached.

    Raises ``psycopg.Error`` on a database error; the caller's transaction rolls back.
    """
    moved = connection.execute(
        _MOVED_TASK_SQL, {'class_key': SEEDED_CLASS_KEY}
    ).fetchone()
    assert moved is not None, 'the task insert returns its row'
    task_id, worker_id = moved
    ids = {'task_id': task_id, 'worker_id': worker_id}
    _ = connection.execute(_HEARTBEAT_SQL, ids)
    retried = connection.execute(_FAILED_ATTEMPT_SQL, ids).fetchone()
    assert retried == (
        'RETRY_SCHEDULED',
    ), f'finish_failed_attempt returned {retried!r}'
    _ = connection.execute(_RECLAIM_SQL, ids)
    outcome = connection.execute(
        _COMPLETE_SQL, {'task_id': task_id, 'worker_id': worker_id}
    ).fetchone()
    assert outcome == ('APPLIED',), f'complete_task_fused returned {outcome!r}'
    latest = connection.execute(_LEAF_OF_TASK_SQL, {'task_id': task_id}).fetchone()
    assert latest is not None, 'the moved task has no history row'
    _ = connection.execute(
        _detach_sql(f'horsies_task_history_{SEEDED_CLASS_KEY}', str(latest[0]))
    )
    _ = connection.execute(_MARK_DETACHED_SQL, {'leaf': latest[0]})


def run_seeded_history(
    database: DualRunDatabase,
) -> Result[SelfCheckRun, SelfCheckStepFailed]:
    """Install, run the production coverage pass, move a task into a daily leaf, detach a leaf, capture."""
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_run_start(connection):
            case Err(failed):
                return Err(failed)
            case Ok(start):
                pass
        match install_baseline_schema(database):
            case Err(failed):
                return _failed('install the baseline', failed)
            case Ok():
                pass
        match seed_history(database):
            case Err(failed):
                return Err(failed)
            case Ok():
                pass
        try:
            with connection.transaction():
                _seeded_writes(connection)
        except psycopg.Error as exc:
            return _failed('write the seeded history', exc)
        return capture_run(connection, start)
