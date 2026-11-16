"""Dual-run of the stale and expired list switch: the client statements against the list functions.

Reference: ``tests/dualrun/reference/monitoring_lists.py`` (the client
statements of #1075, with the 10,000-row bound). Candidate: ``PostgresBroker``
(``horsies_list_stale_tasks`` and ``horsies_list_expired_tasks``). Both
read one seeded database, so the rows compare as they are; ``expired_for``
is measured at each statement's own time, so it compares within the time
between the two reads. Every seeded runner beat is at or after its task's
start, as the start writes the first beat with the RUNNING row.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import Any, Final, LiteralString

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.reference import monitoring_lists as reference
from tests.dualrun.seeds import SeedCoverage, cover

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_READ_GAP: Final = timedelta(seconds=30)
"""More than the time between the reference read and the candidate read."""

# RUNNING tasks started :started_minutes ago, each with :beats runner beats,
# the latest :beat_minutes ago (1 ms apart between tasks; NULL: no beat).
_SEED_RUNNING: Final[LiteralString] = """
    WITH seeded AS (
        INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at, command_fingerprint_version,
            command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
            claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
            finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
            retain_rerun_input, rerun_of_task_id, rerun_root_task_id
        )
        SELECT horsies_uuidv7(), %(task_name)s::text, 'default', 50, '{}', NULL, 'SHARED_ASYNC',
               'standard_30d', now() - interval '1 hour', now() - interval '1 hour', 1,
               sha256(convert_to(n::text || %(task_name)s::text, 'UTF8')), NULL, 'RUNNING',
               now() - interval '1 hour', now() - make_interval(mins => %(started_minutes)s::int),
               'worker-a', NULL, now() - make_interval(mins => %(started_minutes)s::int), NULL,
               NULL, 4242, 'host-a', 'child-1', 0, 0, NULL, NULL, FALSE,
               now() - interval '1 hour', TRUE, NULL, NULL
        FROM generate_series(1, %(count)s::int) AS n
        RETURNING id
    )
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT seeded.id, 'worker-a', 'runner',
           now() - make_interval(mins => %(beat_minutes)s::int)
               - make_interval(secs => (row_number() OVER (ORDER BY seeded.id)) / 1000.0)
               - make_interval(mins => beat.n - 1),
           'host-a', 4242
    FROM seeded CROSS JOIN generate_series(1, %(beats)s::int) AS beat(n)
"""

_SEED_EXPIRED: Final[LiteralString] = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution,
        retention_class_key, sent_at, created_at, command_fingerprint_version,
        command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
        claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
        finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
        retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
        retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    )
    SELECT horsies_uuidv7(), %(task_name)s::text, 'default', 50, '{}', NULL, 'SHARED_ASYNC',
           'standard_30d', now() - interval '1 hour', now() - interval '1 hour', 1,
           sha256(convert_to(n::text || %(task_name)s::text, 'UTF8')), NULL, 'PENDING',
           now() - interval '1 hour', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
           0, 0, NULL,
           now() + make_interval(mins => %(deadline_in_minutes)s::int)
               - make_interval(secs => n / 1000.0),
           FALSE, now() - interval '1 hour', TRUE, NULL, NULL
    FROM generate_series(1, %(count)s::int) AS n
"""


def _seed_running(
    connection: psycopg.Connection[tuple[object, ...]],
    task_name: str,
    *,
    count: int,
    started_minutes: int,
    beat_minutes: int,
    beats: int,
) -> None:
    _ = connection.execute(
        _SEED_RUNNING,
        {
            'task_name': task_name,
            'count': count,
            'started_minutes': started_minutes,
            'beat_minutes': beat_minutes,
            'beats': beats,
        },
    )


def _seed_expired(
    connection: psycopg.Connection[tuple[object, ...]],
    task_name: str,
    *,
    count: int,
    deadline_in_minutes: int,
) -> None:
    _ = connection.execute(
        _SEED_EXPIRED,
        {'task_name': task_name, 'count': count, 'deadline_in_minutes': deadline_in_minutes},
    )


def _broker(database: DualRunDatabase) -> PostgresBroker:
    url = database.url.replace('postgresql://', 'postgresql+psycopg://', 1)
    return PostgresBroker(PostgresConfig(database_url=SecretStr(url)))


async def _install(database: DualRunDatabase) -> None:
    broker = _broker(database)
    try:
        installed = await broker.ensure_schema_initialized()
        assert installed.is_ok(), installed
    finally:
        await broker.close_async()


async def _candidate(database: DualRunDatabase, thresholds: tuple[int, ...]) -> tuple[list[list[dict[str, Any]]], list[dict[str, Any]]]:
    broker = _broker(database)
    try:
        stale: list[list[dict[str, Any]]] = []
        for threshold in thresholds:
            match await broker.get_stale_tasks(stale_threshold_minutes=threshold):
                case Ok(rows):
                    stale.append(rows)
                case Err(error):
                    raise AssertionError(f'the stale list failed: {error!r}')
        match await broker.get_expired_tasks():
            case Ok(rows):
                expired = rows
            case Err(error):
                raise AssertionError(f'the expired list failed: {error!r}')
        return stale, expired
    finally:
        await broker.close_async()


def _without_expired_for(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: value for key, value in row.items() if key != 'expired_for'} for row in rows]


def _assert_expired_for_close(
    reference_rows: list[dict[str, Any]], candidate_rows: list[dict[str, Any]]
) -> None:
    for before, after in zip(reference_rows, candidate_rows, strict=True):
        gap = after['expired_for'] - before['expired_for']
        assert timedelta(0) <= gap <= _READ_GAP, (before, after)


def _dual_run(
    database: DualRunDatabase,
    seed: Callable[[psycopg.Connection[tuple[object, ...]]], None],
    thresholds: tuple[int, ...],
) -> tuple[list[list[dict[str, Any]]], list[dict[str, Any]], list[list[dict[str, Any]]], list[dict[str, Any]]]:
    """Install, cover, seed; the reference lists, then the candidate lists."""
    asyncio.run(_install(database))
    cover(database.url, SeedCoverage.HISTORY_AND_BEATS)
    with psycopg.connect(database.url, autocommit=True) as connection:
        seed(connection)
        reference_stale = [reference.stale_tasks(connection, threshold) for threshold in thresholds]
        reference_expired = reference.expired_tasks(connection)
    candidate_stale, candidate_expired = asyncio.run(_candidate(database, thresholds))
    return reference_stale, reference_expired, candidate_stale, candidate_expired


def _mixed_seed(connection: psycopg.Connection[tuple[object, ...]]) -> None:
    # Stale at 10 minutes: started 50 minutes ago, last of two beats 30 minutes ago.
    _seed_running(connection, 'lists.stale', count=3, started_minutes=50, beat_minutes=30, beats=2)
    # Started 50 minutes ago, beat 5 minutes ago: stale only at a threshold below 5.
    _seed_running(connection, 'lists.recent_beat', count=2, started_minutes=50, beat_minutes=5, beats=1)
    # Started 50 minutes ago, never beat: stale at every threshold below 50.
    _seed_running(connection, 'lists.no_beat', count=1, started_minutes=50, beat_minutes=0, beats=0)
    # Started 3 minutes ago, never beat.
    _seed_running(connection, 'lists.just_started', count=1, started_minutes=3, beat_minutes=0, beats=0)
    _seed_expired(connection, 'lists.expired', count=4, deadline_in_minutes=-2)
    _seed_expired(connection, 'lists.not_yet', count=2, deadline_in_minutes=30)


def test_the_list_functions_answer_what_the_client_statements_answered(
    empty_database: MakeDatabase,
) -> None:
    thresholds = (0, 1, 4, 10, 60, 525_600)

    reference_stale, reference_expired, candidate_stale, candidate_expired = _dual_run(
        empty_database(), _mixed_seed, thresholds
    )

    assert candidate_stale == reference_stale
    assert [len(rows) for rows in candidate_stale] == [7, 7, 6, 4, 0, 0]
    assert _without_expired_for(candidate_expired) == _without_expired_for(reference_expired)
    assert len(candidate_expired) == 4
    _assert_expired_for_close(reference_expired, candidate_expired)


def test_both_cut_a_list_past_the_bound_at_the_same_rows(
    empty_database: MakeDatabase,
) -> None:
    def seed(connection: psycopg.Connection[tuple[object, ...]]) -> None:
        _seed_running(
            connection, 'lists.stale', count=reference.ROW_BOUND + 1,
            started_minutes=50, beat_minutes=30, beats=1,
        )
        _seed_expired(connection, 'lists.expired', count=reference.ROW_BOUND + 1, deadline_in_minutes=-2)

    reference_stale, reference_expired, candidate_stale, candidate_expired = _dual_run(
        empty_database(), seed, (10,)
    )

    assert [len(rows) for rows in candidate_stale] == [reference.ROW_BOUND]
    assert candidate_stale == reference_stale
    assert len(candidate_expired) == reference.ROW_BOUND
    assert _without_expired_for(candidate_expired) == _without_expired_for(reference_expired)


def test_the_comparison_sees_a_candidate_that_reads_another_threshold(
    empty_database: MakeDatabase,
) -> None:
    """The reference reads 4 minutes and the candidate 10; the lists must differ."""
    database = empty_database()
    reference_stale, _, _, _ = _dual_run(database, _mixed_seed, (4,))
    candidate_stale, _ = asyncio.run(_candidate(database, (10,)))

    assert candidate_stale != reference_stale
