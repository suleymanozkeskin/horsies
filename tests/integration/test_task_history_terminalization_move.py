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

from datetime import datetime, timedelta, timezone
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
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    create_workflow,
    frozen_history_row,
    insert_live_task as insert_live_task_for,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'standard_30d'
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
    input_text: str = '{}',
) -> str:
    return await insert_live_task_for(
        connection,
        class_key=CLASS_KEY,
        status=status,
        worker=worker,
        key_digest=key_digest,
        retain=retain,
        input_text=input_text,
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
                'CAST(:task_id AS uuid), :worker, '
                'COALESCE((SELECT claimed_at FROM horsies_tasks WHERE id = CAST(:task_id AS uuid)), NOW()), '
                ':result, TRUE)'
            ),
            {'task_id': task_id, 'worker': worker, 'result': result},
        )
    ).one()


async def claim_generation(connection: AsyncConnection, task_id: str) -> datetime:
    """The claim generation (``claimed_at``) that the live row holds."""
    generation = (
        await connection.execute(
            text('SELECT claimed_at FROM horsies_tasks WHERE id = CAST(:task_id AS uuid)'),
            {'task_id': task_id},
        )
    ).scalar_one()
    assert isinstance(generation, datetime), generation
    return generation


async def complete_locked(
    connection: AsyncConnection,
    task_id: str,
    *,
    claimed_at: datetime,
    worker: str = WORKER,
    result: str = '{"ok":true}',
) -> Any:
    """The other member of the completion class: phase 1 of a workflow
    task's success (``horsies_finish_workflow_success``, kind
    COMPLETE_LOCKED) under the claim ``worker`` held at ``claimed_at``."""
    return (
        await connection.execute(
            text(
                'SELECT * FROM horsies_finish_workflow_success('
                'CAST(:task_id AS uuid), :worker, '
                'CAST(:claimed_at AS TIMESTAMPTZ), :result)'
            ),
            {
                'task_id': task_id,
                'worker': worker,
                'claimed_at': claimed_at,
                'result': result,
            },
        )
    ).one()


async def cancel_admin(connection: AsyncConnection, task_id: str) -> Any:
    """A member of a different class — CANCEL_ADMIN stands alone
    (``horsies_cancel_task``, a RUNNING row included)."""
    return (
        await connection.execute(
            text('SELECT * FROM horsies_cancel_task(CAST(:task_id AS uuid), TRUE)'),
            {'task_id': task_id},
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
                        'result_payload, result_digest, '
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
            assert bytes(history.result_payload) == b'{"ok":true}'
            assert bytes(history.result_digest) == sha256(b'{"ok":true}').digest()
            assert history.last_claimed_worker_id == WORKER

            decoded = decode_attempt_snapshot(
                version=history.attempt_archive_version,
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
    async def test_completed_is_never_eligible_even_with_retained_input(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The reconciliation coverage obligation: the gate candidate is
        FAIL_RUNNING-only, so the completed eligibility branch is owned
        here — a completed request archives NEVER_ELIGIBLE with a null
        envelope even when its input is retained."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(
                connection,
                retain=True,
                input_text='{"precious":"input"}',
            )
            outcome = await complete_fused(connection, task_id)
            assert outcome.outcome == 'APPLIED'
            row = (
                await connection.execute(
                    text(
                        'SELECT rerun_input_disposition, rerun_input_version, '
                        'rerun_input_digest, rerun_input_inline '
                        'FROM horsies_task_history '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert row.rerun_input_disposition == 'NEVER_ELIGIBLE'
            assert row.rerun_input_version is None
            assert row.rerun_input_digest is None
            assert row.rerun_input_inline is None

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
    async def test_a_different_kind_of_the_same_class_is_already_applied(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """Class membership decides replay, not kind equality.

        COMPLETE_LOCKED and COMPLETE_FUSED commit the same effect, so a
        locked completion arriving after a fused one learns that nothing
        new happened. The classifier is told the whole class; a
        single-member array here would report the sibling as foreign.
        """
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection)
            generation = await claim_generation(connection, task_id)
            first = await complete_fused(connection, task_id)
            sibling = await complete_locked(
                connection, task_id, claimed_at=generation
            )
            assert sibling.outcome == 'ALREADY_APPLIED'
            assert sibling.terminalization_kind == 'COMPLETE_FUSED'
            assert sibling.terminal_at == first.terminal_at
            assert sibling.observed_status == 'COMPLETED'
            assert sibling.guard_kind is None

    @pytest.mark.asyncio
    async def test_foreign_kind_is_a_source_state_conflict(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """A kind from another class means a different event won.

        An administrative cancellation stands in its own class, so
        finding a committed completion is not its own replay: the caller
        is told it was overtaken, and by what.
        """
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection)
            await complete_fused(connection, task_id)
            foreign = await cancel_admin(connection, task_id)
            assert foreign.outcome == 'SOURCE_STATE_CONFLICT'
            assert foreign.guard_kind == 'FOREIGN_TERMINALIZATION'
            assert foreign.terminalization_kind == 'COMPLETE_FUSED'
            assert foreign.observed_status == 'COMPLETED'

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
    async def test_deferred_terminalization_requires_a_result_payload(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """A workflow task cannot defer phase 2 with nothing to defer.

        The families that defer — completion-locked, running failure,
        stale failure, claimed expiry — hand the node's progression to
        the outbox, and the outbox carries the result's digest as a NOT
        NULL column. A deferred terminalization without a payload would
        therefore owe progression it could record no evidence for, so
        the move refuses it at the source rather than writing evidence
        that cannot be verified.

        This is the invariant that makes "a terminal workflow task with
        no result" unreachable on any deferring path: such a task either
        belongs to a family that advances its node inline, or has no
        node to advance.
        """
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task_for(
                connection,
                class_key=CLASS_KEY,
                worker=WORKER,
                is_workflow_task=True,
            )
            await link_workflow_node(
                connection,
                task_id,
                workflow_id=await create_workflow(
                    connection, status='RUNNING'
                ),
                node_status='RUNNING',
            )
            # The move itself, with no result: the writers of a run refuse
            # a NULL result before they call it.
            with pytest.raises(DBAPIError) as raised:
                await connection.execute(
                    text(
                        'SELECT horsies_move_task_to_history('
                        '(SELECT t FROM horsies_tasks AS t '
                        'WHERE t.id = CAST(:task_id AS uuid) FOR UPDATE), '
                        "'COMPLETED', 'COMPLETE_LOCKED', NOW(), NULL, NULL, NULL)"
                    ),
                    {'task_id': task_id},
                )
            # The MESSAGE, not merely a failure. With the refusal
            # removed the operation still fails — on
            # result_digest NOT NULL, from the outbox insert — so a bare
            # `pytest.raises(DBAPIError)` would pass with the defect
            # present and report a guard that is not guarding. The
            # deliberate refusal sits ahead of that incidental
            # constraint so the operator reads a sentence naming the
            # problem instead of a column name implying it.
            assert 'requires a result payload' in str(raised.value)

    @pytest.mark.asyncio
    async def test_duplicate_identity_fails_closed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_storage(connection)
            task_id = await insert_live_task(connection)
            await connection.execute(
                text(INSERT_HISTORY_ROW_SQL),
                frozen_history_row(
                    task_id=task_id,
                    class_key=CLASS_KEY,
                    terminal_at=datetime.now(UTC),
                ),
            )
            with pytest.raises(DBAPIError, match='multiple locations'):
                await complete_fused(connection, task_id)
