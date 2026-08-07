"""The completion-family move against real PostgreSQL.

Behavioral proof of the reconciled transaction: an applied completion
moves the row with its full projection, the attempt snapshot decodes
byte-identically through the Python codec, the reservation window starts
from the live row's digest, and the miss classifier reports replay,
foreign terminalization, absence, and lost claims through the staged
provenance path. The COMPLETED-to-NEVER_ELIGIBLE eligibility test is the
coverage obligation recorded at the envelope-shape reconciliation: the
gate candidate never exercises the completed branch, so this suite owns
it.
"""

from __future__ import annotations

from datetime import timedelta, timezone
from hashlib import sha256
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.archive.attempts import decode_attempt_snapshot
from horsies.core.history.archive.versions import DecodedArchiveValue
from tests.integration.task_history_harness import (
    HistorySchema,
    insert_live_task as insert_live_task_for,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'it_move'
WORKER = 'worker-move-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_move'
)


async def prepare_storage(connection: AsyncConnection) -> None:
    await prepare_move_storage(connection, CLASS_KEY)


async def insert_live_task(
    connection: AsyncConnection,
    *,
    status: str = 'RUNNING',
    worker: str | None = WORKER,
    key_digest: bytes | None = None,
    retain: bool = True,
    prepared_disposition: str = 'DECLINED_BY_POLICY',
    prepared_inline: bytes | None = None,
) -> str:
    return await insert_live_task_for(
        connection,
        class_key=CLASS_KEY,
        status=status,
        worker=worker,
        key_digest=key_digest,
        retain=retain,
        prepared_disposition=prepared_disposition,
        prepared_inline=prepared_inline,
    )


async def complete_fused(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker: str = WORKER,
    result: str = '{"ok":true}',
) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_complete_task_fused('
                'CAST(:task_id AS uuid), :worker, NULL, :result, '
                "'task_done', CAST(:task_id AS text))"
            ),
            {'task_id': task_id, 'worker': worker, 'result': result},
        )
    ).one()


class TestAppliedCompletion:
    @pytest.mark.asyncio
    async def test_fused_completion_moves_the_row(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection)
            outcome = await complete_fused(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            assert outcome.terminalization_kind == 'COMPLETE_FUSED'

            history = (
                await connection.execute(
                    text(
                        'SELECT status, terminalization_kind, terminal_at, '
                        'retention_anchor_at, result_payload, result_digest, '
                        'retry_count, last_claimed_worker_id, '
                        'attempt_snapshot, attempt_snapshot_digest, '
                        'attempt_archive_version '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert history.status == 'COMPLETED'
            assert history.retention_anchor_at == history.terminal_at
            assert bytes(history.result_payload) == b'{"ok":true}'
            assert bytes(history.result_digest) == sha256(b'{"ok":true}').digest()
            assert history.last_claimed_worker_id == WORKER

            decoded = decode_attempt_snapshot(
                version=history.attempt_archive_version,
                codec='json-utf8',
                content_type='application/json',
                payload=bytes(history.attempt_snapshot),
                digest=bytes(history.attempt_snapshot_digest),
            )
            assert isinstance(decoded, DecodedArchiveValue)
            (attempt,) = decoded.value
            assert attempt.attempt == 1
            assert attempt.outcome == 'COMPLETED'
            assert attempt.worker_id == WORKER

            live_left = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_tasks '
                        'WHERE id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            attempts_left = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM horsies_task_attempts '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert (live_left, attempts_left) == (0, 0)

    @pytest.mark.asyncio
    async def test_completed_is_never_eligible_even_with_prepared_inline(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The reconciliation coverage obligation: the gate candidate is
        FAIL_RUNNING-only, so the completed eligibility branch is owned
        here — a completed request archives NEVER_ELIGIBLE with a null
        envelope no matter what enqueue prepared."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(
                connection,
                retain=True,
                prepared_disposition='INLINE',
                prepared_inline=b'{"precious":"input"}',
            )
            outcome = await complete_fused(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            row = (
                await connection.execute(
                    text(
                        'SELECT rerun_input_disposition, rerun_input_version, '
                        'rerun_input_codec, rerun_input_content_type, '
                        'rerun_input_digest, rerun_input_inline, '
                        'rerun_input_reference '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert row.rerun_input_disposition == 'NEVER_ELIGIBLE'
            assert row.rerun_input_version is None
            assert row.rerun_input_codec is None
            assert row.rerun_input_content_type is None
            assert row.rerun_input_digest is None
            assert row.rerun_input_inline is None
            assert row.rerun_input_reference is None

    @pytest.mark.asyncio
    async def test_reservation_window_starts_at_terminalization(
        self, terminalization_schema: HistorySchema
    ) -> None:
        digest = sha256(b'move-key').digest()
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection, key_digest=digest)
            await connection.execute(
                text(
                    'SELECT horsies_key_reservation_claim('
                    ':digest, CAST(1 AS smallint), '
                    "make_interval(hours => 1), CAST(1 AS smallint), "
                    ':fingerprint, CAST(:task_id AS uuid))'
                ),
                {
                    'digest': digest,
                    'fingerprint': sha256(task_id.encode()).digest(),
                    'task_id': task_id,
                },
            )
            await complete_fused(connection, task_id)
            reservation = (
                await connection.execute(
                    text(
                        'SELECT disposition, expires_at, '
                        '(SELECT terminal_at FROM horsies_task_history '
                        ' WHERE task_id = CAST(:task_id AS uuid)) AS terminal_at '
                        'FROM horsies_key_reservations '
                        'WHERE idempotency_key_digest = :digest'
                    ),
                    {'task_id': task_id, 'digest': digest},
                )
            ).one()
            assert reservation.disposition == 'TERMINAL'
            assert reservation.expires_at == (
                reservation.terminal_at + timedelta(hours=1)
            )


class TestMissClassification:
    @pytest.mark.asyncio
    async def test_replay_is_already_applied_from_history(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection)
            first = await complete_fused(connection, task_id)
            replay = await complete_fused(connection, task_id)
            assert replay.outcome == 'ALREADY_APPLIED'
            assert replay.terminalization_kind == 'COMPLETE_FUSED'
            assert replay.terminal_at == first.terminal_at
            assert replay.observed_status == 'COMPLETED'

    @pytest.mark.asyncio
    async def test_foreign_kind_is_a_source_state_conflict(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection)
            await complete_fused(connection, task_id)
            foreign = (
                await connection.execute(
                    text(
                        'SELECT * FROM horsies_complete_locked_task('
                        'CAST(:task_id AS uuid), :worker, :result)'
                    ),
                    {
                        'task_id': task_id,
                        'worker': WORKER,
                        'result': '{"ok":true}',
                    },
                )
            ).one()
            assert foreign.outcome == 'SOURCE_STATE_CONFLICT'
            assert foreign.guard_kind == 'FOREIGN_TERMINALIZATION'
            assert foreign.terminalization_kind == 'COMPLETE_FUSED'

    @pytest.mark.asyncio
    async def test_never_seen_task_is_absent(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            absent = await complete_fused(connection, str(uuid4()))
            assert absent.outcome == 'TASK_ABSENT'

    @pytest.mark.asyncio
    async def test_foreign_claim_is_a_lost_claim(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection, worker='worker-other')
            outcome = await complete_fused(connection, task_id)
            assert outcome.outcome == 'LOST_CLAIM'
            assert outcome.observed_worker_id == 'worker-other'


class TestMoveInvariants:
    @pytest.mark.asyncio
    async def test_duplicate_identity_fails_closed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection)
            await connection.execute(
                text(
                    'INSERT INTO horsies_task_history ('
                    'task_id, task_name, queue_name, priority, '
                    'command_fingerprint_version, command_fingerprint, '
                    'status, terminalization_kind, terminal_at, '
                    'retention_anchor_at, retention_class_key, enqueued_at, '
                    'created_at, retry_count, max_retries, '
                    'result_envelope_version, result_codec, '
                    'result_content_type, is_workflow_task, '
                    'history_schema_version, attempt_archive_version, '
                    'attempt_snapshot_codec, attempt_snapshot_content_type, '
                    'attempt_snapshot, attempt_snapshot_digest, '
                    'rerun_input_disposition'
                    ') VALUES ('
                    'CAST(:task_id AS uuid), \'it.move\', \'default\', 50, '
                    '1, :fingerprint, '
                    "'COMPLETED', 'COMPLETE_FUSED', statement_timestamp(), "
                    'statement_timestamp(), :class_key, statement_timestamp(), '
                    'statement_timestamp(), 0, 0, '
                    "1, 'json-utf8', 'application/json', FALSE, "
                    '1, 1, '
                    "'json-utf8', 'application/json', "
                    ':snapshot, :snapshot_digest, '
                    "'NEVER_ELIGIBLE')"
                ),
                {
                    'task_id': task_id,
                    'fingerprint': sha256(task_id.encode()).digest(),
                    'class_key': CLASS_KEY,
                    'snapshot': b'[]',
                    'snapshot_digest': sha256(b'[]').digest(),
                },
            )
            with pytest.raises(DBAPIError, match='multiple locations'):
                await complete_fused(connection, task_id)
