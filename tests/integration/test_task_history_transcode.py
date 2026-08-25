"""The transcode executor against real PostgreSQL: the ruled matrix.

The full five-stage lifecycle over history rows created by the real
failure move: plan under an open maintenance session, bounded copy to
readiness, verification capturing the six-field token, the non-queuing
swap, and finalize with the WAL measurement — then the data proof that
payloads were re-framed with digests recomputed and the correctness
lines hold (zero mismatches, zero invalid targets, zero source-version
rows after swap). Refusals are typed: no maintenance, busy locks under
a real conflicting holder, exhaustion carrying the ruled diagnostic
payload, a mutation between verification and swap moving the token,
and a catalog detachment surfacing inside the locked window.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

import horsies.core.history.transcode.executor as executor_module
from horsies.core.history.transcode.executor import (
    TranscodeStateError,
    finalize_transcode,
    plan_transcode,
    run_copy_batch,
    swap_transcode,
    swap_with_retries,
    verify_transcode,
)
from horsies.core.history.transcode.jobs import job_state_fragments
from horsies.core.history.transcode.maintenance import (
    begin_transcode_maintenance,
    finish_transcode_maintenance,
)
from horsies.core.history.transcode.outcomes import (
    ArchiveComponent,
    TranscodeCopyBatch,
    TranscodeFinalized,
    TranscodeLeafBusy,
    TranscodePlan,
    TranscodePlanRejected,
    TranscodeReadyForVerification,
    TranscodeSwap,
    TranscodeSwapBusy,
    TranscodeSwapExhausted,
)

from horsies.core.history.identity.uuid7 import MonotonicUuid7Generator

from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    frozen_history_row,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_transcode'
WORKER = 'worker-tc-1'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_transcode'
)


async def install_job_state(connection: AsyncConnection) -> None:
    """The gate itself rides the fixture; the job manifest is M12's."""
    for fragment in job_state_fragments():
        await connection.execute(text(fragment))


async def seed_moved_tasks(connection: AsyncConnection, count: int) -> None:
    """Real moves: failed tasks whose attempt snapshots land in history."""
    for _ in range(count):
        task_id = await insert_live_task(
            connection, class_key=CLASS_KEY, worker=WORKER
        )
        await connection.execute(
            text(
                'INSERT INTO horsies_task_attempts '
                '(task_id, attempt, outcome, will_retry, started_at, '
                'finished_at, error_code) VALUES '
                "(CAST(:task_id AS uuid), 1, 'FAILED', FALSE, "
                'statement_timestamp(), statement_timestamp(), '
                "'BOOM')"
            ),
            {'task_id': task_id},
        )
        outcome = (
            await connection.execute(
                text(
                    'SELECT outcome FROM horsies_fail_locked_task('
                    "CAST(:task_id AS uuid), :worker, '{}', 'BOOM', 'x')"
                ),
                {'task_id': task_id, 'worker': WORKER},
            )
        ).one()
        assert outcome.outcome == 'APPLIED'


def v7_with_birth(birth: datetime) -> str:
    """Mint a v7 identifier whose embedded birth is the given instant."""
    milliseconds = int(birth.timestamp() * 1_000)
    generator = MonotonicUuid7Generator(clock_ms=lambda: milliseconds)
    return generator.mint()


GATED_HISTORY_ROW_SQL = INSERT_HISTORY_ROW_SQL.replace(
    ', history_schema_version\n',
    """, history_schema_version,
    attempt_archive_version, attempt_snapshot_codec,
    attempt_snapshot_content_type, attempt_snapshot,
    attempt_snapshot_digest, rerun_input_disposition
""",
    1,
).replace(
    ', :history_schema_version\n',
    """, :history_schema_version,
    :attempt_archive_version, :attempt_snapshot_codec,
    :attempt_snapshot_content_type, :attempt_snapshot,
    :attempt_snapshot_digest, :rerun_input_disposition
""",
    1,
)


async def seed_frozen_rows(
    connection: AsyncConnection, *, day: datetime, count: int
) -> None:
    """Direct inserts carrying the gated NOT NULL component columns."""
    snapshot = b'[]'
    for hour in range(count):
        terminal_at = day.replace(
            hour=6 + hour, minute=0, second=0, microsecond=0
        )
        await connection.execute(
            text(GATED_HISTORY_ROW_SQL),
            {
                **frozen_history_row(
                    task_id=v7_with_birth(
                        terminal_at - timedelta(seconds=30)
                    ),
                    class_key=CLASS_KEY,
                    terminal_at=terminal_at,
                ),
                'attempt_archive_version': 1,
                'attempt_snapshot_codec': 'json-utf8',
                'attempt_snapshot_content_type': 'application/json',
                'attempt_snapshot': snapshot,
                'attempt_snapshot_digest': sha256(snapshot).digest(),
                'rerun_input_disposition': 'NEVER_ELIGIBLE',
            },
        )


async def run_to_ready(connection: AsyncConnection, job_id: str) -> None:
    while True:
        outcome = await run_copy_batch(
            connection, job_id=job_id, batch_size=2
        )
        if isinstance(outcome, TranscodeReadyForVerification):
            return
        assert isinstance(outcome, TranscodeCopyBatch), outcome


class TestFullLifecycle:
    @pytest.mark.asyncio
    async def test_copy_returns_busy_when_partition_maintenance_owns_leaf(
        self, terminalization_schema: HistorySchema
    ) -> None:
        job_id = str(uuid4())
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await install_job_state(connection)
            await seed_moved_tasks(connection, 1)
            await begin_transcode_maintenance(
                connection, session_id=str(uuid4())
            )
            plan = await plan_transcode(
                connection,
                job_id=job_id,
                component=ArchiveComponent.ATTEMPTS,
                source_version=1,
                target_version=2,
                source_codec='json-utf8',
                target_codec='framed-v2',
            )
            assert isinstance(plan, TranscodePlan), plan
            lock_row = (
                await connection.execute(
                    text(
                        'SELECT catalog.class_key, catalog.lower_anchor, '
                        'relations.source_relation_name '
                        'FROM horsies_archive_replacement_relations AS relations '
                        'JOIN horsies_task_history_leaf_catalog AS catalog '
                        'ON catalog.leaf_name = relations.source_relation_name '
                        'WHERE relations.job_id = CAST(:job_id AS uuid) '
                        'ORDER BY relations.relation_ordinal LIMIT 1'
                    ),
                    {'job_id': job_id},
                )
            ).one()

        holder = await terminalization_schema.engine.connect()
        transaction = await holder.begin()
        try:
            await holder.execute(
                text(
                    'SELECT pg_advisory_xact_lock('
                    'horsies_task_history_leaf_lock_key(:class_key, :anchor))'
                ),
                {
                    'class_key': lock_row.class_key,
                    'anchor': lock_row.lower_anchor,
                },
            )
            async with terminalization_schema.engine.begin() as connection:
                outcome = await run_copy_batch(
                    connection,
                    job_id=job_id,
                    batch_size=2,
                )
            assert outcome == TranscodeLeafBusy(
                job_id=job_id,
                leaf_name=lock_row.source_relation_name,
            )
        finally:
            await transaction.rollback()
            await holder.close()

    @pytest.mark.asyncio
    async def test_plan_copy_verify_swap_finalize(
        self, terminalization_schema: HistorySchema
    ) -> None:
        job_id = str(uuid4())
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await install_job_state(connection)
            await seed_moved_tasks(connection, 5)
            await begin_transcode_maintenance(
                connection, session_id=str(uuid4())
            )
            plan = await plan_transcode(
                connection,
                job_id=job_id,
                component=ArchiveComponent.ATTEMPTS,
                source_version=1,
                target_version=2,
                source_codec='json-utf8',
                target_codec='framed-v2',
            )
            assert isinstance(plan, TranscodePlan), plan
            assert plan.copied_rows == 5
            assert plan.reversible
            # The qualified budget ratios: peak disk 5/4, WAL 3/2, both
            # directions — ceilings over the affected relation bytes.
            bytes_affected = plan.affected_relation_bytes
            assert plan.peak_additional_disk_budget_bytes == -(
                -bytes_affected * 5 // 4
            )
            assert plan.wal_budget_bytes == -(-bytes_affected * 3 // 2)
            assert (
                plan.rollback_wal_budget_bytes == plan.wal_budget_bytes
            )
            assert (
                plan.rollback_peak_additional_disk_budget_bytes
                == plan.peak_additional_disk_budget_bytes
            )

            before = (
                await connection.execute(
                    text(
                        'SELECT task_id, attempt_snapshot, '
                        'attempt_snapshot_digest '
                        'FROM horsies_task_history ORDER BY task_id'
                    )
                )
            ).all()

            await run_to_ready(connection, job_id)
            verification = await verify_transcode(
                connection, job_id=job_id
            )
            assert verification.verified, verification
            assert verification.replacement_row_mismatches == 0
            assert verification.invalid_target_rows == 0

            swapped = await swap_transcode(connection, job_id=job_id)
            assert isinstance(swapped, TranscodeSwap)
            assert swapped.relations_swapped == plan.relation_count

            # The swapped-in replacement carries the enqueue-order
            # index, asserted by column composition: the replacement's
            # index NAMES differ from the canonical derivation, which is
            # exactly why the property is the contract.
            from horsies.core.history.partitions.catalog import (
                read_leaf_ordering_index_exists,
            )

            swapped_leaves = [
                str(row.source_relation_name)
                for row in (
                    await connection.execute(
                        text(
                            'SELECT source_relation_name '
                            'FROM horsies_archive_replacement_relations '
                            'WHERE job_id = CAST(:job_id AS uuid)'
                        ),
                        {'job_id': job_id},
                    )
                ).all()
            ]
            assert swapped_leaves
            for leaf_name in swapped_leaves:
                assert await read_leaf_ordering_index_exists(
                    connection, leaf_name
                ), leaf_name

            finalized = await finalize_transcode(
                connection, job_id=job_id
            )
            assert finalized == TranscodeFinalized(
                job_id=job_id,
                retired_source_version=1,
                decoder_retirement_ready=True,
            )

            after = (
                await connection.execute(
                    text(
                        'SELECT task_id, attempt_archive_version, '
                        'attempt_snapshot_codec, attempt_snapshot, '
                        'attempt_snapshot_digest '
                        'FROM horsies_task_history ORDER BY task_id'
                    )
                )
            ).all()
            assert len(after) == len(before) == 5
            for old, new in zip(before, after, strict=True):
                assert new.task_id == old.task_id
                assert new.attempt_archive_version == 2
                assert new.attempt_snapshot_codec == 'framed-v2'
                framed = bytes.fromhex('4832') + bytes(
                    old.attempt_snapshot
                )
                assert bytes(new.attempt_snapshot) == framed
                assert bytes(new.attempt_snapshot_digest) == sha256(
                    framed
                ).digest()

            wal = (
                await connection.execute(
                    text(
                        'SELECT wal_bytes FROM '
                        'horsies_archive_replacement_jobs '
                        'WHERE job_id = CAST(:job_id AS uuid)'
                    ),
                    {'job_id': job_id},
                )
            ).scalar_one()
            assert wal is not None and int(wal) > 0

            await finish_transcode_maintenance(
                connection,
                session_id=(
                    await connection.execute(
                        text(
                            'SELECT session_id FROM '
                            'horsies_archive_maintenance_sessions '
                            'ORDER BY started_at DESC LIMIT 1'
                        )
                    )
                ).scalar_one(),
            )


class TestMultiRelationSwap:
    @pytest.mark.asyncio
    async def test_two_leaves_swap_in_one_window(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """Rows on two days: the job spans two leaves and the swap's
        sorted-order arm rebinds both inside one locked window."""
        job_id = str(uuid4())
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await install_job_state(connection)
            today = datetime.now(UTC)
            await seed_frozen_rows(connection, day=today, count=3)
            await seed_frozen_rows(
                connection, day=today + timedelta(days=1), count=3
            )
            await begin_transcode_maintenance(
                connection, session_id=str(uuid4())
            )
            plan = await plan_transcode(
                connection,
                job_id=job_id,
                component=ArchiveComponent.RESULT,
                source_version=1,
                target_version=2,
                source_codec='json-utf8',
                target_codec='framed-v2',
            )
            assert isinstance(plan, TranscodePlan), plan
            assert plan.relation_count == 2
            assert plan.copied_rows == 6

            await run_to_ready(connection, job_id)
            verification = await verify_transcode(
                connection, job_id=job_id
            )
            assert verification.verified, verification

            swapped = await swap_transcode(connection, job_id=job_id)
            assert isinstance(swapped, TranscodeSwap)
            assert swapped.relations_swapped == 2

            finalized = await finalize_transcode(
                connection, job_id=job_id
            )
            assert isinstance(finalized, TranscodeFinalized)

            rows = (
                await connection.execute(
                    text(
                        'SELECT result_envelope_version, result_codec, '
                        'result_payload, result_digest '
                        'FROM horsies_task_history'
                    )
                )
            ).all()
            assert len(rows) == 6
            framed = bytes.fromhex('4832') + b'{}'
            for row in rows:
                assert row.result_envelope_version == 2
                assert row.result_codec == 'framed-v2'
                assert bytes(row.result_payload) == framed
                assert bytes(row.result_digest) == sha256(framed).digest()


class TestTypedRefusals:
    @pytest.mark.asyncio
    async def test_plan_without_maintenance_is_refused(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await install_job_state(connection)
            outcome = await plan_transcode(
                connection,
                job_id=str(uuid4()),
                component=ArchiveComponent.ATTEMPTS,
                source_version=1,
                target_version=2,
                source_codec='json-utf8',
                target_codec='framed-v2',
            )
            assert isinstance(outcome, TranscodePlanRejected)
            assert 'maintenance' in outcome.reason

    @pytest.mark.asyncio
    async def test_mutation_between_verify_and_swap_refuses(
        self, terminalization_schema: HistorySchema
    ) -> None:
        job_id = str(uuid4())
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await install_job_state(connection)
            await seed_moved_tasks(connection, 3)
            await begin_transcode_maintenance(
                connection, session_id=str(uuid4())
            )
            plan = await plan_transcode(
                connection,
                job_id=job_id,
                component=ArchiveComponent.ATTEMPTS,
                source_version=1,
                target_version=2,
                source_codec='json-utf8',
                target_codec='framed-v2',
            )
            assert isinstance(plan, TranscodePlan)
            await run_to_ready(connection, job_id)
            verification = await verify_transcode(
                connection, job_id=job_id
            )
            assert verification.verified
            # A write between verification and swap: the mutation guard
            # bumps the generation, the token moves, the swap refuses.
            source_name = (
                await connection.execute(
                    text(
                        'SELECT source_relation_name FROM '
                        'horsies_archive_replacement_relations '
                        'WHERE job_id = CAST(:job_id AS uuid) '
                        'ORDER BY relation_ordinal LIMIT 1'
                    ),
                    {'job_id': job_id},
                )
            ).scalar_one()
            await connection.execute(
                text(
                    f'DELETE FROM "{source_name}" WHERE ctid = ('
                    f'SELECT ctid FROM "{source_name}" LIMIT 1)'
                )
            )
            with pytest.raises(
                TranscodeStateError, match='verification changed'
            ):
                await swap_transcode(connection, job_id=job_id)


class TestSwapContention:
    @pytest.mark.asyncio
    async def test_held_lock_is_busy_and_exhaustion_carries_blockers(
        self,
        terminalization_schema: HistorySchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        job_id = str(uuid4())
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            await install_job_state(connection)
            await seed_moved_tasks(connection, 3)
            await begin_transcode_maintenance(
                connection, session_id=str(uuid4())
            )
            plan = await plan_transcode(
                connection,
                job_id=job_id,
                component=ArchiveComponent.ATTEMPTS,
                source_version=1,
                target_version=2,
                source_codec='json-utf8',
                target_codec='framed-v2',
            )
            assert isinstance(plan, TranscodePlan)
            await run_to_ready(connection, job_id)
            verification = await verify_transcode(
                connection, job_id=job_id
            )
            assert verification.verified

        blocker_engine = terminalization_schema.engine
        async with blocker_engine.connect() as holder:
            await holder.execute(
                text(
                    'SELECT task_id FROM horsies_task_history '
                    'LIMIT 1 FOR UPDATE'
                )
            )
            async with terminalization_schema.engine.begin() as connection:
                busy = await swap_transcode(connection, job_id=job_id)
            assert isinstance(busy, TranscodeSwapBusy)

            # Exhaustion with the ceilings shrunk for the test — the
            # production values are pinned by the vocabulary suite.
            monkeypatch.setattr(
                executor_module, 'SWAP_LOCK_ATTEMPTS_MAXIMUM', 3
            )
            monkeypatch.setattr(
                executor_module, 'SWAP_RETRY_BACKOFF_SECONDS', 0.01
            )
            exhausted = await swap_with_retries(
                terminalization_schema.engine, job_id=job_id
            )
            assert isinstance(exhausted, TranscodeSwapExhausted)
            assert exhausted.attempts == 3
            assert exhausted.blocker_capture_failed is False
            assert exhausted.blockers, 'expected the holding session'
            for blocker in exhausted.blockers:
                assert blocker.pid > 0
                assert blocker.query is None or len(blocker.query) <= 1024

        # Holder released: the same job swaps cleanly.
        async with terminalization_schema.engine.begin() as connection:
            swapped = await swap_transcode(connection, job_id=job_id)
            assert isinstance(swapped, TranscodeSwap)
