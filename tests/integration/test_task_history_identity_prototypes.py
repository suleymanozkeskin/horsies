"""Correctness gates for three disposable task-identity candidates."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes.identity import (
    CANDIDATE_IDEMPOTENCY_WINDOW_DEFAULT,
    CANDIDATE_IDEMPOTENCY_WINDOW_MAX,
    EnqueueCommandV1,
    ScopedIdempotencyKey,
)
from tests.task_history_prototypes.identity_schema import (
    extend_identity_history_leaves,
    install_identity_candidates,
    install_staged_lookup_prototype,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


_CANDIDATES = ('no_directory', 'key_registry', 'combined_registry')


@pytest_asyncio.fixture
async def identity_schema(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs the v26 base schema
) -> AsyncIterator[AsyncConnection]:
    schema = PrototypeSchema(f'history_identity_{uuid4().hex[:12]}')
    connection = await engine.connect()
    try:
        await install_archive_candidates(connection, schema)
        await install_identity_candidates(connection, schema)
        await connection.commit()
        connection.info['task_history_schema'] = schema
        yield connection
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()
        await connection.close()


def _schema(connection: AsyncConnection) -> PrototypeSchema:
    schema = connection.info.get('task_history_schema')
    assert isinstance(schema, PrototypeSchema)
    return schema


def _command() -> EnqueueCommandV1:
    return EnqueueCommandV1(
        task_name='billing.capture',
        queue_name='payments',
        priority=10,
        args_json=None,
        kwargs_json='{"order_id":"order-1"}',
        good_until=None,
        enqueue_delay_seconds=None,
        task_options_json=None,
        retention_class_key='finite_30d_v1',
        rerun_of_task_id=None,
        rerun_root_task_id=None,
    )


async def _enqueue(
    connection: AsyncConnection,
    candidate: str,
    *,
    task_id: str,
    command: EnqueueCommandV1,
    key: ScopedIdempotencyKey | None,
    key_window: str | timedelta | None = None,
) -> tuple[str, str, int | None]:
    schema = _schema(connection)
    row = (
        await connection.execute(
            text(
                f"""
                SELECT (outcome).outcome, (outcome).task_id,
                       (outcome).observed_fingerprint_version
                FROM (
                    SELECT {schema.sql}.enqueue_{candidate}(
                        CAST(:task_id AS varchar(36)), CAST(:task_name AS text),
                        CAST(:key_digest AS bytea),
                        CAST(:key_scope_version AS smallint),
                        CAST(:key_window AS interval),
                        CAST(1 AS smallint), CAST(:fingerprint AS bytea),
                        CAST(:retention_class_key AS text)
                    ) AS outcome
                ) AS applied
                """
            ),
            {
                'task_id': task_id,
                'task_name': command.task_name,
                'key_digest': key.digest if key else None,
                'key_scope_version': 1 if key else None,
                'key_window': (
                    key_window or CANDIDATE_IDEMPOTENCY_WINDOW_DEFAULT
                ) if key else None,
                'fingerprint': command.fingerprint,
                'retention_class_key': command.retention_class_key,
            },
        )
    ).one()
    return row.outcome, row.task_id, row.observed_fingerprint_version


async def _enqueue_committed(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    candidate: str,
    *,
    task_id: str,
    command: EnqueueCommandV1,
    key: ScopedIdempotencyKey | None,
) -> tuple[str, str, int | None]:
    async with engine.begin() as connection:
        connection.info['task_history_schema'] = schema
        return await _enqueue(
            connection,
            candidate,
            task_id=task_id,
            command=command,
            key=key,
        )


async def _lookup(
    connection: AsyncConnection,
    candidate: str,
    task_id: str,
) -> tuple[bool, str | None, str | None, int | None, bytes | None]:
    schema = _schema(connection)
    row = (
        await connection.execute(
            text(
                f"""
                SELECT (located).found, (located).location, (located).task_id,
                       (located).fingerprint_version,
                       (located).command_fingerprint
                FROM (
                    SELECT {schema.sql}.lookup_{candidate}(
                        CAST(:task_id AS varchar(36))
                    ) AS located
                ) AS lookup
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    return (
        row.found,
        row.location,
        row.task_id,
        row.fingerprint_version,
        bytes(row.command_fingerprint) if row.command_fingerprint is not None else None,
    )


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_same_key_replays_and_different_fingerprint_conflicts(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'order-1')
    first_id = str(uuid4())
    assert await _enqueue(
        identity_schema,
        candidate,
        task_id=first_id,
        command=command,
        key=key,
    ) == ('APPLIED', first_id, None)
    assert await _enqueue(
        identity_schema,
        candidate,
        task_id=str(uuid4()),
        command=command,
        key=key,
    ) == ('REPLAY', first_id, 1)

    changed = replace(command, priority=11)
    assert await _enqueue(
        identity_schema,
        candidate,
        task_id=str(uuid4()),
        command=changed,
        key=key,
    ) == ('CONFLICT', first_id, 1)


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_same_task_identity_requires_same_key_and_fingerprint(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    command = _command()
    task_id = str(uuid4())
    key = ScopedIdempotencyKey(command.task_name, 'order-1')
    await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=key,
    )
    assert await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=key,
    ) == ('REPLAY', task_id, 1)
    assert await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=None,
    ) == ('CONFLICT', task_id, 1)


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_replay_survives_terminal_move_for_declared_window(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'order-1')
    task_id = str(uuid4())
    await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=key,
    )
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    moved = (
        await identity_schema.execute(
            text(
                f"""
                SELECT {schema.sql}.terminalize_{terminal_prefix}(
                    :task_id, statement_timestamp()
                )
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    assert moved is True
    assert await _enqueue(
        identity_schema,
        candidate,
        task_id=str(uuid4()),
        command=command,
        key=key,
    ) == ('REPLAY', task_id, 1)


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_terminal_window_is_the_duration_snapshotted_at_enqueue(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'snapshotted-window')
    task_id = str(uuid4())
    terminal_at = datetime(2026, 8, 5, tzinfo=timezone.utc)
    await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=key,
        key_window='3 days',
    )
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    await identity_schema.execute(
        text(
            f"""
            SELECT {schema.sql}.terminalize_{terminal_prefix}(
                :task_id, :terminal_at
            )
            """
        ),
        {'task_id': task_id, 'terminal_at': terminal_at},
    )

    match candidate:
        case 'no_directory':
            relation = 'no_directory_history'
            window_column = 'idempotency_window'
            expiry_column = 'idempotency_expires_at'
        case 'key_registry':
            relation = 'key_reservations'
            window_column = 'reservation_window'
            expiry_column = 'expires_at'
        case 'combined_registry':
            relation = 'combined_registry'
            window_column = 'key_window'
            expiry_column = 'key_expires_at'
        case _:
            pytest.fail(f'unrecognized identity candidate: {candidate}')
    observed = (
        await identity_schema.execute(
            text(
                f"""
                SELECT {window_column} AS reservation_window,
                       {expiry_column} AS expires_at
                FROM {schema.sql}.{relation}
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert observed.reservation_window == timedelta(days=3)
    assert observed.expires_at == terminal_at + timedelta(days=3)


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_unkeyed_enqueue_creates_no_key_only_registry_row(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    first_id = str(uuid4())
    second_id = str(uuid4())
    assert (
        await _enqueue(
            identity_schema, candidate, task_id=first_id, command=command, key=None
        )
    )[0] == 'APPLIED'
    assert (
        await _enqueue(
            identity_schema, candidate, task_id=second_id, command=command, key=None
        )
    )[0] == 'APPLIED'
    if candidate == 'key_registry':
        count = (
            await identity_schema.execute(
                text(f'SELECT count(*) FROM {schema.sql}.key_reservations')
            )
        ).scalar_one()
        assert count == 0
    if candidate == 'combined_registry':
        count = (
            await identity_schema.execute(
                text(f'SELECT count(*) FROM {schema.sql}.combined_registry')
            )
        ).scalar_one()
        assert count == 2


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_concurrent_same_key_commits_exactly_one_request(
    identity_schema: AsyncConnection,
    db_url: str,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'concurrent-order')
    concurrent_engine = create_async_engine(
        db_url,
        pool_size=64,
        max_overflow=0,
    )
    try:
        outcomes = await asyncio.gather(
            *(
                _enqueue_committed(
                    concurrent_engine,
                    schema,
                    candidate,
                    task_id=str(uuid4()),
                    command=command,
                    key=key,
                )
                for _ in range(64)
            )
        )
    finally:
        await concurrent_engine.dispose()
    applied = [outcome for outcome in outcomes if outcome[0] == 'APPLIED']
    replays = [outcome for outcome in outcomes if outcome[0] == 'REPLAY']
    assert len(applied) == 1
    assert len(replays) == 63
    assert {outcome[1] for outcome in outcomes} == {applied[0][1]}


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_concurrent_same_key_different_fingerprints_classify_by_winner(
    identity_schema: AsyncConnection,
    db_url: str,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    key = ScopedIdempotencyKey(_command().task_name, 'concurrent-conflict')
    commands = tuple(
        _command() if index % 2 == 0 else replace(_command(), priority=11)
        for index in range(64)
    )
    task_ids = tuple(str(uuid4()) for _ in commands)
    concurrent_engine = create_async_engine(
        db_url,
        pool_size=64,
        max_overflow=0,
    )
    try:
        outcomes = await asyncio.gather(
            *(
                _enqueue_committed(
                    concurrent_engine,
                    schema,
                    candidate,
                    task_id=task_id,
                    command=command,
                    key=key,
                )
                for task_id, command in zip(task_ids, commands, strict=True)
            )
        )
    finally:
        await concurrent_engine.dispose()

    applied_indexes = tuple(
        index for index, outcome in enumerate(outcomes) if outcome[0] == 'APPLIED'
    )
    assert len(applied_indexes) == 1
    applied_index = applied_indexes[0]
    applied_task_id = task_ids[applied_index]
    winning_fingerprint = commands[applied_index].fingerprint
    for command, outcome in zip(commands, outcomes, strict=True):
        expected = 'REPLAY' if command.fingerprint == winning_fingerprint else 'CONFLICT'
        if outcome[0] == 'APPLIED':
            assert command.fingerprint == winning_fingerprint
        else:
            assert outcome[0] == expected
            assert outcome[1] == applied_task_id


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_uncertain_commit_replay_returns_the_committed_request(
    identity_schema: AsyncConnection,
    db_url: str,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'uncertain-commit')
    first_id = str(uuid4())
    replay_id = str(uuid4())
    isolated_engine = create_async_engine(db_url)
    try:
        await _enqueue_committed(
            isolated_engine,
            schema,
            candidate,
            task_id=first_id,
            command=command,
            key=key,
        )
        replay = await _enqueue_committed(
            isolated_engine,
            schema,
            candidate,
            task_id=replay_id,
            command=command,
            key=key,
        )
    finally:
        await isolated_engine.dispose()
    assert replay == ('REPLAY', first_id, 1)


@pytest.mark.parametrize(
    ('candidate', 'expected_outcome'),
    [
        ('no_directory', 'APPLIED'),
        ('key_registry', 'REPLAY'),
        ('combined_registry', 'REPLAY'),
    ],
)
async def test_post_history_removal_follows_reservation_storage_lifetime(
    identity_schema: AsyncConnection,
    candidate: str,
    expected_outcome: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'history-removed')
    first_id = str(uuid4())
    second_id = str(uuid4())
    await _enqueue(
        identity_schema,
        candidate,
        task_id=first_id,
        command=command,
        key=key,
        key_window='30 days',
    )
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    history_prefix = 'combined' if candidate == 'combined_registry' else candidate
    await identity_schema.execute(
        text(
            f"""
            SELECT {schema.sql}.terminalize_{terminal_prefix}(
                CAST(:task_id AS varchar(36)), statement_timestamp()
            )
            """
        ),
        {'task_id': first_id},
    )
    await identity_schema.execute(
        text(
            f"""
            DELETE FROM {schema.sql}.{history_prefix}_history
            WHERE task_id = :task_id
            """
        ),
        {'task_id': first_id},
    )

    outcome = await _enqueue(
        identity_schema,
        candidate,
        task_id=second_id,
        command=command,
        key=key,
    )
    assert outcome[0] == expected_outcome
    assert outcome[1] == (second_id if expected_outcome == 'APPLIED' else first_id)


@pytest.mark.parametrize(
    ('candidate', 'cleanup_function'),
    [
        ('key_registry', 'cleanup_key_reservations'),
        ('combined_registry', 'cleanup_combined_reservations'),
    ],
)
async def test_expired_registry_reservations_clean_in_bounded_batches(
    identity_schema: AsyncConnection,
    candidate: str,
    cleanup_function: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    expired_ids: list[str] = []
    for ordinal in range(2):
        task_id = str(uuid4())
        expired_ids.append(task_id)
        await _enqueue(
            identity_schema,
            candidate,
            task_id=task_id,
            command=command,
            key=ScopedIdempotencyKey(command.task_name, f'expired-{ordinal}'),
            key_window='1 microsecond',
        )
        await identity_schema.execute(
            text(
                f"""
                SELECT {schema.sql}.terminalize_{terminal_prefix}(
                    CAST(:task_id AS varchar(36)), statement_timestamp()
                )
                """
            ),
            {'task_id': task_id},
        )
    live_id = str(uuid4())
    await _enqueue(
        identity_schema,
        candidate,
        task_id=live_id,
        command=command,
        key=ScopedIdempotencyKey(command.task_name, 'still-live'),
    )
    await identity_schema.execute(text('SELECT pg_sleep(0.001)'))

    cleaned: list[str] = []
    for _ in range(3):
        rows = (
            await identity_schema.execute(
                text(
                    f"""
                    SELECT task_id
                    FROM {schema.sql}.{cleanup_function}(1)
                    """
                )
            )
        ).scalars()
        cleaned.extend(rows)
    assert set(cleaned) == set(expired_ids)
    assert len(cleaned) == 2

    if candidate == 'key_registry':
        remaining = (
            await identity_schema.execute(
                text(
                    f"""
                    SELECT task_id FROM {schema.sql}.key_reservations
                    ORDER BY task_id
                    """
                )
            )
        ).scalars().all()
        assert remaining == [live_id]
    else:
        rows = (
            await identity_schema.execute(
                text(
                    f"""
                    SELECT task_id, idempotency_key_digest
                    FROM {schema.sql}.combined_registry
                    ORDER BY task_id
                    """
                )
            )
        ).all()
        assert {row.task_id for row in rows} == {*expired_ids, live_id}
        assert {
            row.task_id
            for row in rows
            if row.idempotency_key_digest is not None
        } == {live_id}


@pytest.mark.parametrize(
    ('cleanup_function', 'batch_size'),
    [
        ('cleanup_key_reservations', None),
        ('cleanup_key_reservations', 0),
        ('cleanup_key_reservations', -1),
        ('cleanup_combined_reservations', None),
        ('cleanup_combined_reservations', 0),
        ('cleanup_combined_reservations', -1),
    ],
)
async def test_reservation_cleanup_rejects_invalid_bound_before_mutation(
    identity_schema: AsyncConnection,
    cleanup_function: str,
    batch_size: int | None,
) -> None:
    schema = _schema(identity_schema)
    with pytest.raises(DBAPIError, match='batch size must be a positive integer'):
        await identity_schema.execute(
            text(
                f"""
                SELECT task_id
                FROM {schema.sql}.{cleanup_function}(
                    CAST(:batch_size AS integer)
                )
                """
            ),
            {'batch_size': batch_size},
        )


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_expired_key_can_be_reused_without_removing_old_task_identity(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'reusable-order')
    first_id = str(uuid4())
    second_id = str(uuid4())
    await _enqueue(
        identity_schema,
        candidate,
        task_id=first_id,
        command=command,
        key=key,
        key_window='1 microsecond',
    )
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    await identity_schema.execute(
        text(
            f"""
            SELECT {schema.sql}.terminalize_{terminal_prefix}(
                CAST(:task_id AS varchar(36)), statement_timestamp()
            )
            """
        ),
        {'task_id': first_id},
    )
    await identity_schema.execute(text('SELECT pg_sleep(0.001)'))

    assert await _enqueue(
        identity_schema,
        candidate,
        task_id=second_id,
        command=command,
        key=key,
    ) == ('APPLIED', second_id, None)

    history_prefix = 'combined' if candidate == 'combined_registry' else candidate
    old_count = (
        await identity_schema.execute(
            text(
                f"""
                SELECT count(*)
                FROM {schema.sql}.{history_prefix}_history
                WHERE task_id = :task_id
                """
            ),
            {'task_id': first_id},
        )
    ).scalar_one()
    assert old_count == 1
    if candidate == 'combined_registry':
        rows = (
            await identity_schema.execute(
                text(
                    f"""
                    SELECT task_id, idempotency_key_digest
                    FROM {schema.sql}.combined_registry
                    ORDER BY task_id
                    """
                )
            )
        ).all()
        assert len(rows) == 2
        assert sum(row.idempotency_key_digest is not None for row in rows) == 1


@pytest.mark.parametrize(
    ('key_window', 'message'),
    [
        ('0 seconds', 'idempotency window must be positive'),
        (
            CANDIDATE_IDEMPOTENCY_WINDOW_MAX + timedelta(microseconds=1),
            'idempotency window must not exceed 30 days',
        ),
    ],
)
@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_enqueue_rejects_invalid_window_before_mutation(
    identity_schema: AsyncConnection,
    candidate: str,
    key_window: str | timedelta,
    message: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    task_id = str(uuid4())
    key = ScopedIdempotencyKey(command.task_name, 'invalid-window')
    with pytest.raises(DBAPIError, match=message):
        await _enqueue(
            identity_schema,
            candidate,
            task_id=task_id,
            command=command,
            key=key,
            key_window=key_window,
        )
    await identity_schema.rollback()
    live_prefix = 'combined' if candidate == 'combined_registry' else candidate
    count = (
        await identity_schema.execute(
            text(
                f"""
                SELECT count(*) FROM {schema.sql}.{live_prefix}_live
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).scalar_one()
    assert count == 0


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_enqueue_accepts_proposed_maximum_window(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    command = _command()
    task_id = str(uuid4())
    key = ScopedIdempotencyKey(command.task_name, 'maximum-window')
    outcome = await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=key,
        key_window=CANDIDATE_IDEMPOTENCY_WINDOW_MAX,
    )
    assert outcome == ('APPLIED', task_id, None)


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_missing_history_leaf_rolls_back_location_transition(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'missing-leaf')
    task_id = str(uuid4())
    await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=key,
    )
    await identity_schema.commit()
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate

    with pytest.raises(DBAPIError, match='no partition.*found'):
        await identity_schema.execute(
            text(
                f"""
                SELECT {schema.sql}.terminalize_{terminal_prefix}(
                    CAST(:task_id AS varchar(36)),
                    '2030-08-05T12:00:00Z'::timestamptz
                )
                """
            ),
            {'task_id': task_id},
        )
    await identity_schema.rollback()

    live_prefix = 'combined' if candidate == 'combined_registry' else candidate
    locations = (
        await identity_schema.execute(
            text(
                f"""
                SELECT
                    (SELECT count(*) FROM {schema.sql}.{live_prefix}_live
                     WHERE task_id = :task_id) AS live_count,
                    (SELECT count(*) FROM {schema.sql}.{live_prefix}_history
                     WHERE task_id = :task_id) AS history_count
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert tuple(locations) == (1, 0)

    match candidate:
        case 'combined_registry':
            disposition = (
                await identity_schema.execute(
                    text(
                        f"""
                        SELECT location
                        FROM {schema.sql}.combined_registry
                        WHERE task_id = :task_id
                        """
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert disposition == 'LIVE'
        case 'key_registry':
            disposition = (
                await identity_schema.execute(
                    text(
                        f"""
                        SELECT disposition
                        FROM {schema.sql}.key_reservations
                        WHERE task_id = :task_id
                        """
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert disposition == 'LIVE'
        case 'no_directory':
            pass
        case _:
            pytest.fail(f'unrecognized identity candidate: {candidate}')


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_point_lookup_distinguishes_live_history_and_absence(
    identity_schema: AsyncConnection,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    task_id = str(uuid4())
    await _enqueue(
        identity_schema,
        candidate,
        task_id=task_id,
        command=command,
        key=None,
    )
    assert await _lookup(identity_schema, candidate, task_id) == (
        True,
        'LIVE',
        task_id,
        1,
        command.fingerprint,
    )

    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    await identity_schema.execute(
        text(
            f"""
            SELECT {schema.sql}.terminalize_{terminal_prefix}(
                CAST(:task_id AS varchar(36)),
                '2026-08-05T12:00:00Z'::timestamptz
            )
            """
        ),
        {'task_id': task_id},
    )
    assert await _lookup(identity_schema, candidate, task_id) == (
        True,
        'HISTORY',
        task_id,
        1,
        command.fingerprint,
    )
    assert await _lookup(identity_schema, candidate, str(uuid4())) == (
        False,
        None,
        None,
        None,
        None,
    )


async def test_staged_lookup_uses_static_newest_first_leaf_probes(
    identity_schema: AsyncConnection,
) -> None:
    schema = _schema(identity_schema)
    await extend_identity_history_leaves(
        identity_schema,
        schema,
        target_leaf_count=8,
    )
    newest_first_leaves = tuple(
        f'key_registry_history_finite_{year}'
        for year in range(2033, 2025, -1)
    )
    await install_staged_lookup_prototype(
        identity_schema,
        schema,
        prefix='key_registry',
        newest_first_leaf_names=newest_first_leaves,
    )

    identifiers = {
        'live': str(uuid4()),
        'forever': str(uuid4()),
        'recent': str(uuid4()),
        'oldest': str(uuid4()),
    }
    fingerprint = bytes.fromhex('01' * 32)
    await identity_schema.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.key_registry_live (
                task_id, task_name, fingerprint_version,
                command_fingerprint, retention_class_key, created_at
            ) VALUES (
                :live, 'prototype.task', 1, :fingerprint,
                'finite_30d_v1', '2026-08-05T00:00:00Z'
            )
            """
        ),
        {**identifiers, 'fingerprint': fingerprint},
    )
    await identity_schema.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.key_registry_history (
                task_id, task_name, fingerprint_version,
                command_fingerprint, retention_class_key, terminal_at
            ) VALUES
                (:forever, 'prototype.task', 1, :fingerprint,
                 'forever', '2026-08-05T00:00:00Z'),
                (:recent, 'prototype.task', 1, :fingerprint,
                 'finite_30d_v1', '2033-06-01T00:00:00Z'),
                (:oldest, 'prototype.task', 1, :fingerprint,
                 'finite_30d_v1', '2026-06-01T00:00:00Z')
            """
        ),
        {**identifiers, 'fingerprint': fingerprint},
    )
    await identity_schema.commit()

    async def lookup_with_relation_locks(
        task_id: str,
    ) -> tuple[
        tuple[bool, str | None, str | None, int | None, bytes | None],
        set[str],
    ]:
        located = await _lookup(
            identity_schema,
            'key_registry_staged',
            task_id,
        )
        locked = {
            row.relname
            for row in (
                await identity_schema.execute(
                    text(
                        """
                        SELECT relation.relname
                        FROM pg_locks AS held
                        JOIN pg_class AS relation
                          ON relation.oid = held.relation
                        JOIN pg_namespace AS namespace
                          ON namespace.oid = relation.relnamespace
                        WHERE held.pid = pg_backend_pid()
                          AND namespace.nspname = :schema
                          AND relation.relkind IN ('r', 'p')
                        """
                    ),
                    {'schema': schema.name},
                )
            ).all()
        }
        await identity_schema.rollback()
        return located, locked

    all_finite_leaves = set(newest_first_leaves)
    expected_locks = {
        'live': {'key_registry_live'},
        'forever': {
            'key_registry_live',
            'key_registry_history_forever',
        },
        'recent': {
            'key_registry_live',
            'key_registry_history_forever',
            newest_first_leaves[0],
        },
        'oldest': {
            'key_registry_live',
            'key_registry_history_forever',
            *all_finite_leaves,
        },
    }
    for kind, task_id in identifiers.items():
        located, locked = await lookup_with_relation_locks(task_id)
        assert located == (
            True,
            'LIVE' if kind == 'live' else 'HISTORY',
            task_id,
            1,
            fingerprint,
        )
        assert locked == expected_locks[kind]

    absent, absent_locks = await lookup_with_relation_locks(str(uuid4()))
    assert absent == (False, None, None, None, None)
    assert absent_locks == {
        'key_registry_live',
        'key_registry_history_forever',
        *all_finite_leaves,
    }

    definition = (
        await identity_schema.execute(
            text(
                """
                SELECT pg_get_functiondef(
                    to_regprocedure(
                        :signature
                    )
                )
                """
            ),
            {
                'signature': (
                    f'{schema.name}.lookup_key_registry_staged(character varying)'
                )
            },
        )
    ).scalar_one()
    assert 'EXECUTE' not in definition
    live_position = definition.index('key_registry_live')
    forever_position = definition.index('key_registry_history_forever')
    newest_position = definition.index(newest_first_leaves[0])
    oldest_position = definition.index(newest_first_leaves[-1])
    assert live_position < forever_position < newest_position < oldest_position
    assert definition.count('key_registry_history_finite_') == 8
    assert 'key_registry_history\n' not in definition


async def test_lookup_leaf_generator_reaches_declared_count(
    identity_schema: AsyncConnection,
) -> None:
    schema = _schema(identity_schema)
    for invalid_count in (0, 1, 513):
        with pytest.raises(ValueError, match='between 2 and 512'):
            await extend_identity_history_leaves(
                identity_schema,
                schema,
                target_leaf_count=invalid_count,
            )
    await extend_identity_history_leaves(
        identity_schema,
        schema,
        target_leaf_count=4,
    )
    counts = (
        await identity_schema.execute(
            text(
                """
                SELECT parent.relname, count(*) AS leaves
                FROM pg_class parent
                JOIN pg_namespace namespace ON namespace.oid = parent.relnamespace
                CROSS JOIN LATERAL pg_partition_tree(parent.oid) tree
                WHERE namespace.nspname = :schema
                  AND parent.relname IN (
                      'no_directory_history_finite',
                      'key_registry_history_finite',
                      'combined_history_finite'
                  )
                  AND tree.isleaf
                GROUP BY parent.relname
                ORDER BY parent.relname
                """
            ),
            {'schema': schema.name},
        )
    ).all()
    assert {row.relname: row.leaves for row in counts} == {
        'combined_history_finite': 4,
        'key_registry_history_finite': 4,
        'no_directory_history_finite': 4,
    }

    forever_leaves = (
        await identity_schema.execute(
            text(
                """
                SELECT count(*)
                FROM pg_class child
                JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
                WHERE namespace.nspname = :schema
                  AND child.relname IN (
                      'no_directory_history_forever',
                      'key_registry_history_forever',
                      'combined_history_forever'
                  )
                """
            ),
            {'schema': schema.name},
        )
    ).scalar_one()
    assert forever_leaves == 3

    no_directory_leaf_indexes = (
        await identity_schema.execute(
            text(
                """
                SELECT child.relname,
                       count(index_relation.indexrelid) FILTER (
                           WHERE pg_get_indexdef(index_relation.indexrelid)
                               LIKE '%(idempotency_key_digest, idempotency_expires_at)%'
                       ) AS key_indexes
                FROM pg_class parent
                JOIN pg_namespace namespace ON namespace.oid = parent.relnamespace
                CROSS JOIN LATERAL pg_partition_tree(parent.oid) tree
                JOIN pg_class child ON child.oid = tree.relid
                LEFT JOIN pg_index index_relation
                    ON index_relation.indrelid = child.oid
                WHERE namespace.nspname = :schema
                  AND parent.relname = 'no_directory_history'
                  AND tree.isleaf
                GROUP BY child.relname
                ORDER BY child.relname
                """
            ),
            {'schema': schema.name},
        )
    ).all()
    assert len(no_directory_leaf_indexes) == 5
    assert all(row.key_indexes == 1 for row in no_directory_leaf_indexes)
