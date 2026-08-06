"""Executable checks for the rerun-input paired-terminalization collector.

The measurement compares a same-row terminal update with a direct
live-to-history transition that carries a prepared rerun-input envelope. These
checks establish the two things a latency number cannot: that the transition
satisfies its exact structural obligations, and that the harness detects a
candidate which violates them. A collector that cannot fail has not proven a
pass.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from hashlib import sha256
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes.rerun_terminalization_evidence import (
    INLINE_BOUND_BYTES,
    InstalledComparison,
    RerunInputDisposition,
    StructuralViolation,
    assert_candidate_structure,
    deployed_task_index_definitions,
    install_rerun_terminalization_prototype,
    stored_history_form,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.transcode import (
    install_archive_transcode_prototype,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_WORKER = 'worker-1'
_RESULT = '{"ok": "' + 'x' * 184 + '"}'
_ENVELOPE = b'{"v":"' + b'q' * (INLINE_BOUND_BYTES - 8) + b'"}'
_FINITE_CLASS = 'finite_30d_v1'


async def _install(
    connection: AsyncConnection,
    *,
    duplicate_envelope: bool,
) -> InstalledComparison:
    schema = PrototypeSchema(f'rerun_term_{uuid4().hex[:10]}')
    await install_archive_candidates(connection, schema)
    await install_archive_transcode_prototype(connection, schema)
    installed = await install_rerun_terminalization_prototype(
        connection,
        schema,
        duplicate_envelope=duplicate_envelope,
    )
    await connection.commit()
    return installed


@pytest_asyncio.fixture
async def installed(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
) -> AsyncIterator[tuple[AsyncConnection, InstalledComparison]]:
    async with engine.connect() as connection:
        comparison = await _install(connection, duplicate_envelope=False)
        try:
            yield connection, comparison
        finally:
            await connection.rollback()
            await remove_archive_candidates(connection, comparison.schema)
            await connection.commit()


async def _seed_candidate_task(
    connection: AsyncConnection,
    comparison: InstalledComparison,
    *,
    attempts: int = 4,
    disposition: RerunInputDisposition = RerunInputDisposition.INLINE,
    retain: bool = True,
    is_workflow_task: bool = False,
) -> str:
    task_id = str(uuid4())
    carries = disposition.carries_envelope
    await connection.execute(
        text(
            f"""
            INSERT INTO {comparison.candidate.live_tasks} (
                id, task_name, queue_name, priority, status,
                args, kwargs, enqueue_sha, is_workflow_task,
                claimed, claimed_by_worker_id, claimed_at, started_at,
                retention_class_key, input_digest,
                retain_rerun_input, prepared_rerun_input_disposition,
                prepared_rerun_input_version, prepared_rerun_input_codec,
                prepared_rerun_input_content_type,
                prepared_rerun_input_digest, prepared_rerun_input_inline
            ) VALUES (
                :task_id, 'prototype.rerun', 'default', 100, 'RUNNING',
                '[]', '{{}}', repeat('a', 64), :is_workflow_task,
                TRUE, :worker, NOW(), NOW(),
                :retention_class, :digest,
                :retain, :disposition,
                :version, :codec, :content_type,
                :envelope_digest, :envelope
            )
            """
        ),
        {
            'task_id': task_id,
            'is_workflow_task': is_workflow_task,
            'worker': _WORKER,
            'retention_class': _FINITE_CLASS,
            'digest': sha256(_ENVELOPE).digest(),
            'retain': retain,
            'disposition': disposition.value,
            'version': 1 if carries else None,
            'codec': 'json-utf8' if carries else None,
            'content_type': 'application/json' if carries else None,
            'envelope_digest': sha256(_ENVELOPE).digest() if carries else None,
            'envelope': _ENVELOPE if carries else None,
        },
    )
    for attempt in range(1, attempts + 1):
        await connection.execute(
            text(
                f"""
                INSERT INTO {comparison.candidate.live_attempts} (
                    task_id, attempt, outcome, will_retry,
                    started_at, finished_at, worker_id
                ) VALUES (
                    :task_id, :attempt, 'FAILED', FALSE, NOW(), NOW(), :worker
                )
                """
            ),
            {'task_id': task_id, 'attempt': attempt, 'worker': _WORKER},
        )
    await connection.commit()
    return task_id


async def _terminalize(
    connection: AsyncConnection,
    comparison: InstalledComparison,
    task_id: str,
) -> None:
    await connection.execute(
        text(
            f'SELECT * FROM {comparison.schema.sql}.candidate_fail_locked_task('
            ':task_id, :worker, :result, :error_code, NULL)'
        ),
        {
            'task_id': task_id,
            'worker': _WORKER,
            'result': _RESULT,
            'error_code': 'PROTOTYPE_FAILURE',
        },
    )
    await connection.commit()


class TestPairedRelationsAreComparable:
    async def test_both_sides_replay_the_deployed_index_set(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        deployed = await deployed_task_index_definitions(connection)

        per_side = len(comparison.replayed_indexes) // 2

        assert per_side == len(deployed)
        assert len(comparison.replayed_indexes) == 2 * len(deployed)

    async def test_each_side_carries_the_same_index_count_in_the_catalog(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        counts = {
            side.side.value: (
                await connection.execute(
                    text(
                        'SELECT COUNT(*) FROM pg_indexes '
                        'WHERE schemaname = :schema AND tablename = :relation'
                    ),
                    {
                        'schema': comparison.schema.name,
                        'relation': side.live_tasks_name,
                    },
                )
            ).scalar_one()
            for side in (comparison.baseline, comparison.candidate)
        }

        assert counts['baseline'] == counts['candidate']
        # Primary key plus every replayed secondary index.
        assert counts['baseline'] == 1 + len(comparison.replayed_indexes) // 2


class TestHonestCandidateSatisfiesItsObligations:
    async def test_committed_transition_meets_every_structural_requirement(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        task_id = await _seed_candidate_task(connection, comparison)

        await _terminalize(connection, comparison, task_id)
        outcome = await assert_candidate_structure(
            connection,
            comparison,
            task_id=task_id,
            expected_envelope=_ENVELOPE,
            expected_retention_class=_FINITE_CLASS,
        )

        assert outcome.violations == ()
        assert outcome.passed

    async def test_history_row_routes_to_the_run_date_finite_leaf(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        task_id = await _seed_candidate_task(connection, comparison)

        await _terminalize(connection, comparison, task_id)
        leaf = (
            await connection.execute(
                text(
                    f"""
                    SELECT c.relname
                    FROM {comparison.schema.sql}.history_aggregate AS h
                    JOIN pg_class AS c ON c.oid = h.tableoid
                    WHERE h.task_id = :task_id
                    """
                ),
                {'task_id': task_id},
            )
        ).scalar_one()

        assert leaf == comparison.finite_leaf

    @pytest.mark.parametrize('attempts', (1, 4))
    async def test_attempt_rows_are_absent_after_commit(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
        attempts: int,
    ) -> None:
        connection, comparison = installed
        task_id = await _seed_candidate_task(
            connection,
            comparison,
            attempts=attempts,
        )

        await _terminalize(connection, comparison, task_id)
        remaining = (
            await connection.execute(
                text(
                    f'SELECT COUNT(*) FROM {comparison.candidate.live_attempts} '
                    'WHERE task_id = :task_id'
                ),
                {'task_id': task_id},
            )
        ).scalar_one()

        assert remaining == 0


class TestEligibilityPrecedesPolicy:
    async def test_workflow_backing_request_carries_no_envelope(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        task_id = await _seed_candidate_task(
            connection,
            comparison,
            disposition=RerunInputDisposition.NEVER_ELIGIBLE,
            is_workflow_task=True,
        )

        await _terminalize(connection, comparison, task_id)
        stored = (
            await connection.execute(
                text(
                    f'SELECT rerun_input_form, rerun_input_inline '
                    f'FROM {comparison.schema.sql}.history_aggregate '
                    'WHERE task_id = :task_id'
                ),
                {'task_id': task_id},
            )
        ).one()

        assert stored.rerun_input_form is None
        assert stored.rerun_input_inline is None

    async def test_declined_snapshot_carries_no_envelope(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        task_id = await _seed_candidate_task(
            connection,
            comparison,
            disposition=RerunInputDisposition.DECLINED_BY_POLICY,
            retain=False,
        )

        await _terminalize(connection, comparison, task_id)
        stored = (
            await connection.execute(
                text(
                    f'SELECT rerun_input_form, rerun_input_inline '
                    f'FROM {comparison.schema.sql}.history_aggregate '
                    'WHERE task_id = :task_id'
                ),
                {'task_id': task_id},
            )
        ).one()

        assert stored.rerun_input_form is None
        assert stored.rerun_input_inline is None

    @pytest.mark.parametrize(
        ('disposition', 'expected'),
        (
            (RerunInputDisposition.INLINE, 'INLINE'),
            (RerunInputDisposition.REFERENCE, 'REFERENCE'),
            (RerunInputDisposition.DECLINED_BY_POLICY, None),
            (RerunInputDisposition.OVER_BOUND, None),
            (RerunInputDisposition.NEVER_ELIGIBLE, None),
        ),
    )
    def test_ratified_disposition_maps_onto_the_measured_column(
        self,
        disposition: RerunInputDisposition,
        expected: str | None,
    ) -> None:
        assert stored_history_form(disposition) == expected


class TestDetectionControl:
    """The harness must fail a candidate that violates an exact obligation."""

    async def test_second_envelope_copy_is_detected(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
    ) -> None:
        async with engine.connect() as connection:
            comparison = await _install(connection, duplicate_envelope=True)
            try:
                task_id = await _seed_candidate_task(connection, comparison)
                await _terminalize(connection, comparison, task_id)
                outcome = await assert_candidate_structure(
                    connection,
                    comparison,
                    task_id=task_id,
                    expected_envelope=_ENVELOPE,
                    expected_retention_class=_FINITE_CLASS,
                )
            finally:
                await connection.rollback()
                await remove_archive_candidates(connection, comparison.schema)
                await connection.commit()

        assert not outcome.passed
        assert (
            StructuralViolation.ENVELOPE_COPIED_MORE_THAN_ONCE
            in outcome.violations
        )

    async def test_the_same_workload_passes_without_the_defect(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        # The control is only evidence if the sole difference is the defect.
        connection, comparison = installed
        task_id = await _seed_candidate_task(connection, comparison)

        await _terminalize(connection, comparison, task_id)
        outcome = await assert_candidate_structure(
            connection,
            comparison,
            task_id=task_id,
            expected_envelope=_ENVELOPE,
            expected_retention_class=_FINITE_CLASS,
        )

        assert outcome.passed
