"""Rules that the baseline schema holds for history rows and phase-2 pending rows.

The move functions are the only writers of `horsies_task_history`. The CHECK
constraints that stay on the table refuse the combinations that no move
writes: rerun input on a COMPLETED row or a workflow row, and a result on an
administrative cancel. The history tree has no DEFAULT partition, so a row
whose class or terminal time has no leaf cannot be stored. The pending table
takes only the closed value sets that its CHECK constraints name.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Final

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from tests.attempt_snapshot_support import encode_attempt_snapshot
from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    register_class,
    terminalization_schema_fixture,
)
from tests.integration.terminal_writes import TaskFailure, fail_running_task
from tests.rerun_envelope_support import frame_rerun_envelope

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY: Final = 'standard_30d'
WORKER: Final = 'worker-rules-1'
FAILURE_RESULT: Final = '{"error":{"code":"BOOM"}}'

CHECK_VIOLATION: Final = '23514'
STRING_TOO_LONG: Final = '22001'

terminalization_schema = terminalization_schema_fixture('task_history_it_rules')


INSERT_FULL_HISTORY_ROW_SQL: Final = """
INSERT INTO horsies_task_history (
    task_id, task_name, queue_name, priority,
    command_fingerprint_version, command_fingerprint,
    status, terminalization_kind, terminal_at,
    retention_class_key, sent_at, enqueued_at, created_at,
    retry_count, max_retries,
    result_envelope_version, result_payload, result_digest,
    is_workflow_task, history_schema_version,
    attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
    rerun_input_disposition, rerun_input_version,
    rerun_input_digest, rerun_input_inline
) VALUES (
    CAST(:task_id AS uuid), 'it.rules', 'default', 50,
    1, :fingerprint,
    :status, :terminalization_kind, :terminal_at,
    :class_key, :terminal_at, :terminal_at, :terminal_at,
    0, 0,
    1, :result_payload, :result_digest,
    :is_workflow_task, 1,
    :attempt_archive_version, :attempt_snapshot, :attempt_snapshot_digest,
    :rerun_input_disposition, :rerun_input_version,
    :rerun_input_digest, :rerun_input_inline
)
"""


def history_row(
    *,
    status: str,
    terminalization_kind: str,
    is_workflow_task: bool,
    result_payload: bytes | None,
    rerun_input: bytes | None,
) -> dict[str, object]:
    """One history row; `rerun_input` set gives an INLINE disposition."""
    from horsies.core.history.identity.uuid7 import mint_task_id

    task_id = str(mint_task_id())
    snapshot = encode_attempt_snapshot(())
    return {
        'task_id': task_id,
        'fingerprint': sha256(task_id.encode()).digest(),
        'status': status,
        'terminalization_kind': terminalization_kind,
        'terminal_at': datetime.now(timezone.utc),
        'class_key': CLASS_KEY,
        'result_payload': result_payload,
        'result_digest': (
            sha256(result_payload).digest() if result_payload is not None else None
        ),
        'is_workflow_task': is_workflow_task,
        'attempt_archive_version': snapshot.version,
        'attempt_snapshot': snapshot.payload,
        'attempt_snapshot_digest': snapshot.digest,
        'rerun_input_disposition': (
            'INLINE' if rerun_input is not None else 'NEVER_ELIGIBLE'
        ),
        'rerun_input_version': 1 if rerun_input is not None else None,
        'rerun_input_digest': (
            sha256(rerun_input).digest() if rerun_input is not None else None
        ),
        'rerun_input_inline': rerun_input,
    }


def refused_constraint(raised: DBAPIError) -> str | None:
    """The constraint that the server named for the refusal."""
    assert isinstance(raised.orig, psycopg.errors.CheckViolation), raised.orig
    return raised.orig.diag.constraint_name


async def insert_refused(
    connection: AsyncConnection, row: dict[str, object]
) -> str | None:
    """Insert `row` in a savepoint that the server refuses; name the refusal."""
    with pytest.raises(IntegrityError) as raised:
        async with connection.begin_nested():
            await connection.execute(text(INSERT_FULL_HISTORY_ROW_SQL), row)
    return refused_constraint(raised.value)


async def fail_running(connection: AsyncConnection, task_id: str) -> str:
    """The worker's failure of the RUNNING task of ``WORKER``: it writes
    attempt 1 (FAILED, ``BOOM``, ``exploded``) and moves the row."""
    return await fail_running_task(
        connection,
        task_id,
        worker_id=WORKER,
        failure=TaskFailure(
            result_json=FAILURE_RESULT,
            error_code='BOOM',
            failed_reason='exploded',
            attempt_message='exploded',
            attempt_reason='exploded',
        ),
    )


async def test_a_retained_failure_moves_one_envelope_copy_into_its_class_leaf(
    terminalization_schema: HistorySchema,
) -> None:
    """The move stores the rerun envelope once, in the leaf of its class
    and terminal day, and leaves no live row and no attempt row."""
    input_text = '{"order":"order-1"}'
    envelope = frame_rerun_envelope(input_text, None)
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        task_id = await insert_live_task(
            connection,
            class_key=CLASS_KEY,
            worker=WORKER,
            retain=True,
            input_text=input_text,
        )
        assert await fail_running(connection, task_id) == 'APPLIED'

        row = (
            await connection.execute(
                text(
                    """
                    SELECT h.rerun_input_disposition, h.rerun_input_inline,
                           h.rerun_input_digest, h.result_payload,
                           h.attempt_snapshot, h.retention_class_key,
                           h.terminal_at, leaf.relname AS leaf_name
                    FROM horsies_task_history AS h
                    JOIN pg_class AS leaf ON leaf.oid = h.tableoid
                    WHERE h.task_id = CAST(:task_id AS uuid)
                    """
                ),
                {'task_id': task_id},
            )
        ).one()
        assert row.rerun_input_disposition == 'INLINE'
        assert bytes(row.rerun_input_inline) == envelope
        assert bytes(row.rerun_input_digest) == sha256(envelope).digest()
        assert bytes(row.result_payload) == FAILURE_RESULT.encode()
        assert envelope not in bytes(row.attempt_snapshot)
        assert row.retention_class_key == CLASS_KEY

        cataloged_leaf = (
            await connection.execute(
                text(
                    'SELECT leaf_name FROM horsies_task_history_leaf_catalog '
                    'WHERE class_key = :class_key '
                    'AND lower_anchor <= :terminal_at '
                    'AND :terminal_at < upper_anchor '
                    'AND detached_at IS NULL'
                ),
                {'class_key': CLASS_KEY, 'terminal_at': row.terminal_at},
            )
        ).scalar_one()
        assert row.leaf_name == cataloged_leaf

        remaining = (
            await connection.execute(
                text(
                    'SELECT '
                    '(SELECT count(*) FROM horsies_tasks '
                    ' WHERE id = CAST(:task_id AS uuid)), '
                    '(SELECT count(*) FROM horsies_task_attempts '
                    ' WHERE task_id = CAST(:task_id AS uuid))'
                ),
                {'task_id': task_id},
            )
        ).one()
        assert tuple(remaining) == (0, 0)


@pytest.mark.parametrize(
    ('status', 'is_workflow_task'),
    [('COMPLETED', False), ('FAILED', True)],
    ids=['completed-row', 'workflow-row'],
)
async def test_a_row_that_is_never_eligible_cannot_store_rerun_input(
    terminalization_schema: HistorySchema,
    status: str,
    is_workflow_task: bool,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        refused = await insert_refused(
            connection,
            history_row(
                status=status,
                terminalization_kind=(
                    'COMPLETE_LOCKED' if status == 'COMPLETED' else 'FAIL_RUNNING'
                ),
                is_workflow_task=is_workflow_task,
                result_payload=b'{}',
                rerun_input=frame_rerun_envelope('{}', None),
            ),
        )
        assert refused == 'horsies_task_history_rerun_input_shape'


async def test_an_administrative_cancel_row_cannot_store_a_result(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        refused = await insert_refused(
            connection,
            history_row(
                status='CANCELLED',
                terminalization_kind='CANCEL_ADMIN',
                is_workflow_task=False,
                result_payload=b'{"ok":"prior"}',
                rerun_input=None,
            ),
        )
        assert refused == 'horsies_task_history_result_domain'


async def test_the_history_tree_has_no_default_partition(
    terminalization_schema: HistorySchema,
) -> None:
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        parents = (
            await connection.execute(
                text(
                    """
                    SELECT tree.relid::regclass::text AS parent,
                           partitioned.partdefid
                    FROM pg_partition_tree('horsies_task_history') AS tree
                    JOIN pg_partitioned_table AS partitioned
                      ON partitioned.partrelid = tree.relid
                    """
                )
            )
        ).all()
        names = {row.parent for row in parents}
        assert {
            'horsies_task_history',
            'horsies_task_history_forever',
            f'horsies_task_history_{CLASS_KEY}',
        } <= names
        assert {row.parent: int(row.partdefid) for row in parents} == {
            name: 0 for name in names
        }


async def test_a_move_with_no_leaf_for_its_row_keeps_the_task_and_its_reservation_live(
    terminalization_schema: HistorySchema,
) -> None:
    """The class is registered, but no leaf covers the terminal time."""
    uncovered_class = 'standard_31d'
    key_digest = sha256(b'uncovered-class-key').digest()
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        await register_class(connection, uncovered_class, duration_days=31)
        task_id = await insert_live_task(
            connection,
            class_key=uncovered_class,
            worker=WORKER,
            key_digest=key_digest,
        )
        claimed = (
            await connection.execute(
                text(
                    'SELECT outcome FROM horsies_key_reservation_claim('
                    "CAST(:digest AS bytea), CAST(1 AS smallint), interval '1 hour', "
                    'CAST(1 AS smallint), :fingerprint, CAST(:task_id AS uuid))'
                ),
                {
                    'digest': key_digest,
                    'fingerprint': sha256(task_id.encode()).digest(),
                    'task_id': task_id,
                },
            )
        ).scalar_one()
        assert claimed == 'APPLIED'

        with pytest.raises(DBAPIError, match='no partition of relation'):
            async with connection.begin_nested():
                await fail_running(connection, task_id)

        state = (
            await connection.execute(
                text(
                    'SELECT '
                    '(SELECT count(*) FROM horsies_tasks '
                    ' WHERE id = CAST(:task_id AS uuid)) AS live, '
                    '(SELECT count(*) FROM horsies_task_history '
                    ' WHERE task_id = CAST(:task_id AS uuid)) AS history, '
                    '(SELECT disposition FROM horsies_key_reservations '
                    ' WHERE task_id = CAST(:task_id AS uuid)) AS reservation'
                ),
                {'task_id': task_id},
            )
        ).one()
        assert tuple(state) == (1, 0, 'LIVE')


async def seed_pending_row(connection: AsyncConnection) -> str:
    """A workflow task that failed: the move writes its HISTORY pending row."""
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        worker=WORKER,
        is_workflow_task=True,
    )
    workflow_id = await create_workflow(connection, status='RUNNING')
    await link_workflow_node(
        connection, task_id, workflow_id=workflow_id, node_status='RUNNING'
    )
    assert await fail_running(connection, task_id) == 'APPLIED'
    return task_id


async def test_the_pending_row_pins_its_node_to_its_workflow(
    terminalization_schema: HistorySchema,
) -> None:
    """The pending row names its node by (node row id, workflow id), so the
    node cannot move to another workflow while the row exists."""
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        task_id = await seed_pending_row(connection)
        other_workflow_id = await create_workflow(connection, status='RUNNING')
        with pytest.raises(IntegrityError) as raised:
            async with connection.begin_nested():
                await connection.execute(
                    text(
                        'UPDATE horsies_workflow_tasks '
                        'SET workflow_id = CAST(:workflow_id AS uuid) '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'workflow_id': other_workflow_id, 'task_id': task_id},
                )
        cause = raised.value.orig
        assert isinstance(cause, psycopg.errors.ForeignKeyViolation), cause
        assert (
            cause.diag.constraint_name
            == 'horsies_workflow_phase2_pending_node_fkey'
        )


@pytest.mark.parametrize(
    ('column', 'value'),
    [
        ('history_class', 'h' * 65),
        ('history_class', 'é' * 33),
        ('terminalization_kind', 't' * 33),
        ('terminalization_kind', 'é' * 17),
        ('last_failure_class', 'f' * 65),
        ('last_failure_class', 'é' * 33),
    ],
)
async def test_a_pending_column_refuses_a_value_outside_its_bound(
    terminalization_schema: HistorySchema,
    column: str,
    value: str,
) -> None:
    """Each text column holds a closed set; any other value is refused."""
    async with terminalization_schema.engine.begin() as connection:
        await prepare_move_storage(connection, CLASS_KEY)
        task_id = await seed_pending_row(connection)
        # One retaining attempt, so the attempt fields agree with a stored
        # failure class and only the value under test can be refused.
        await connection.execute(
            text(
                'UPDATE horsies_workflow_phase2_pending '
                'SET attempt_count = 1, last_attempt_at = now(), '
                "last_failure_class = 'SOURCE_ABSENT' "
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
        with pytest.raises(DBAPIError) as raised:
            async with connection.begin_nested():
                await connection.execute(
                    text(
                        f'UPDATE horsies_workflow_phase2_pending '
                        f'SET {column} = :value '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id, 'value': value},
                )
        cause = raised.value.orig
        assert isinstance(cause, psycopg.Error), cause
        assert cause.sqlstate in {CHECK_VIOLATION, STRING_TOO_LONG}, cause
        stored = (
            await connection.execute(
                text(
                    f'SELECT {column} FROM horsies_workflow_phase2_pending '
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()
        assert stored != value
