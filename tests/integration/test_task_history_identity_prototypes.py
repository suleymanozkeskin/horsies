"""Correctness gates for three disposable task-identity candidates."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes.identity import (
    EnqueueCommandV1,
    ScopedIdempotencyKey,
)
from tests.task_history_prototypes.identity_schema import (
    extend_identity_history_leaves,
    install_identity_candidates,
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
                    :task_id, :terminal_at, interval '24 hours'
                )
                """
            ),
            {
                'task_id': task_id,
                'terminal_at': datetime(2026, 8, 5, tzinfo=timezone.utc),
            },
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
    engine: AsyncEngine,
    candidate: str,
) -> None:
    schema = _schema(identity_schema)
    command = _command()
    key = ScopedIdempotencyKey(command.task_name, 'concurrent-order')
    outcomes = await asyncio.gather(
        *(
            _enqueue_committed(
                engine,
                schema,
                candidate,
                task_id=str(uuid4()),
                command=command,
                key=key,
            )
            for _ in range(8)
        )
    )
    applied = [outcome for outcome in outcomes if outcome[0] == 'APPLIED']
    replays = [outcome for outcome in outcomes if outcome[0] == 'REPLAY']
    assert len(applied) == 1
    assert len(replays) == 7
    assert {outcome[1] for outcome in outcomes} == {applied[0][1]}


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
    )
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    await identity_schema.execute(
        text(
            f"""
            SELECT {schema.sql}.terminalize_{terminal_prefix}(
                CAST(:task_id AS varchar(36)), statement_timestamp(),
                interval '1 microsecond'
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


@pytest.mark.parametrize('candidate', _CANDIDATES)
async def test_terminalization_rejects_invalid_window_before_mutation(
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
    await identity_schema.commit()
    terminal_prefix = 'combined' if candidate == 'combined_registry' else candidate
    with pytest.raises(DBAPIError, match='idempotency window must be positive'):
        await identity_schema.execute(
            text(
                f"""
                SELECT {schema.sql}.terminalize_{terminal_prefix}(
                    CAST(:task_id AS varchar(36)), statement_timestamp(),
                    interval '0'
                )
                """
            ),
            {'task_id': task_id},
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
    assert count == 1


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
                    '2030-08-05T12:00:00Z'::timestamptz,
                    interval '24 hours'
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
                '2026-08-05T12:00:00Z'::timestamptz,
                interval '24 hours'
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
