"""Executable checks for the rerun-input paired-terminalization collector.

The measurement compares a same-row terminal update with a direct
live-to-history transition that carries a prepared rerun-input envelope. These
checks establish the two things a latency number cannot: that the transition
satisfies its exact structural obligations, and that the harness detects a
candidate which violates them. A collector that cannot fail has not proven a
pass.

Reproduction, from the worktree root. The database is named explicitly rather
than left to the ambient environment: without `HORSIES_TEST_DATABASE_URL` the
integration conftest falls back to `localhost:5432`, which is a shared bench,
and a run that lands there is measuring someone else's server in someone
else's timezone.

    docker compose -f tests/fixtures/perf/compose.yaml up -d --wait pg16
    uv run python -m tests.perf --apply-schema \
      --dsn postgresql+psycopg://postgres:testpassword@localhost:15446/horsies
    HORSIES_TEST_DATABASE_URL=postgresql+psycopg://postgres:testpassword@localhost:15446/horsies \
      uv run pytest tests/integration/test_rerun_terminalization_prototypes.py -q

The suite asserts its own timezone independence, so a bench in any session
timezone reproduces the same result.
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
from tests.perf.counters import Counts
from tests.perf.statistics import Verdict
from tests.task_history_prototypes.rerun_terminalization_evidence import (
    INLINE_BOUND_BYTES,
    InstalledComparison,
    PayloadShape,
    RerunInputDisposition,
    StructuralViolation,
    assert_candidate_structure,
    compare_wal,
    deployed_task_index_definitions,
    install_rerun_terminalization_prototype,
    install_statement_counters,
    measure_cell,
    read_statement_counter_prerequisite,
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

# Small enough to run in a review loop, large enough that the interleaving,
# the discarded warm-up block and the counters are all genuinely exercised.
# The gate run declares its own, far larger, counts.
_DRIVER_OBSERVATIONS = 40
_DRIVER_BLOCK_SIZE = 20
_DRIVER_RESAMPLES = 200

# The deliberately tightened WAL limit the detection control runs under:
# no proportional allowance and a one-byte floor, which the candidate
# necessarily exceeds because it copies the envelope into history.
_TIGHTENED_WAL_FRACTION = 0.0
_TIGHTENED_WAL_FLOOR_BYTES = 1


def _counts(*, wal_bytes: int, terminal_rows: int) -> Counts:
    return Counts(
        client_statements=terminal_rows,
        nested_statements=0,
        client_rows=terminal_rows,
        nested_rows=0,
        terminal_rows=terminal_rows,
        wal_records=terminal_rows,
        wal_bytes=wal_bytes,
        wal_fpi=0,
        write_transactions=terminal_rows,
    )


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



async def _require_counters(connection: AsyncConnection) -> None:
    """Skip with the named prerequisite rather than failing four times over.

    A bench without pg_stat_statements preloaded cannot count statements at
    all. Letting the driver tests fail there reports four unexplained errors
    about a measurement; naming the missing prerequisite reports the one fact
    that is actually true about the environment.
    """
    prerequisite = await read_statement_counter_prerequisite(connection)
    if not prerequisite.usable:
        pytest.skip(f'{prerequisite.availability.value}: {prerequisite.reason}')
    await install_statement_counters(connection)


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


class TestMeasurementDriver:
    """The driver has to produce a judged cell before any of it means anything."""

    async def test_honest_cell_reports_every_budget_it_judged(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        await _require_counters(connection)

        cell = await measure_cell(
            connection,
            comparison,
            payload_shape=PayloadShape.COMPRESSIBLE,
            attempts_per_task=1,
            observations=_DRIVER_OBSERVATIONS,
            block_size=_DRIVER_BLOCK_SIZE,
            resamples=_DRIVER_RESAMPLES,
            seed=20260806,
        )

        assert cell.observations_per_side == _DRIVER_OBSERVATIONS
        assert len(cell.baseline.samples_ms) == _DRIVER_OBSERVATIONS
        assert len(cell.candidate.samples_ms) == _DRIVER_OBSERVATIONS
        # p50, the p95 lock envelope, and p99 are each judged.
        assert {round(c.percentile) for c in cell.comparisons} == {50, 95, 99}
        assert cell.structural.passed
        assert all(outcome.passed for outcome in cell.exact_counts)

    async def test_exact_counts_are_one_statement_per_observation(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        await _require_counters(connection)

        cell = await measure_cell(
            connection,
            comparison,
            payload_shape=PayloadShape.COMPRESSIBLE,
            attempts_per_task=1,
            observations=_DRIVER_OBSERVATIONS,
            block_size=_DRIVER_BLOCK_SIZE,
            resamples=_DRIVER_RESAMPLES,
            seed=20260806,
        )

        for outcome in cell.exact_counts:
            assert outcome.client_statements == _DRIVER_OBSERVATIONS
            assert outcome.write_transactions >= _DRIVER_OBSERVATIONS
            assert outcome.violations == ()

    async def test_candidate_records_more_wal_per_task_than_the_baseline(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        # Not a budget assertion: the candidate copies a 64 KiB envelope into
        # history and deletes a live row, so a measurement reporting no extra
        # WAL at all would mean the probe never attributed the candidate's work.
        connection, comparison = installed
        await _require_counters(connection)

        cell = await measure_cell(
            connection,
            comparison,
            payload_shape=PayloadShape.INCOMPRESSIBLE,
            attempts_per_task=1,
            observations=_DRIVER_OBSERVATIONS,
            block_size=_DRIVER_BLOCK_SIZE,
            resamples=_DRIVER_RESAMPLES,
            seed=20260806,
        )

        assert cell.wal.candidate_bytes_per_task > 0
        assert cell.wal.baseline_bytes_per_task > 0
        # Regression: bracketing the counter probe once around the whole run
        # instead of once per block handed each side the sum of both, and the
        # two sides then reported byte-identical WAL. Equality here is the
        # signature of that conflation, not a plausible measurement.
        assert (
            cell.wal.candidate_bytes_per_task
            != cell.wal.baseline_bytes_per_task
        )
        assert cell.wal.delta_bytes_per_task > INLINE_BOUND_BYTES / 2


class TestWalDetectionControl:
    """The WAL comparison must fail when it is deliberately given no room."""

    async def test_tightened_threshold_fails_and_declares_what_it_used(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        connection, comparison = installed
        await _require_counters(connection)

        cell = await measure_cell(
            connection,
            comparison,
            payload_shape=PayloadShape.COMPRESSIBLE,
            attempts_per_task=1,
            observations=_DRIVER_OBSERVATIONS,
            block_size=_DRIVER_BLOCK_SIZE,
            resamples=_DRIVER_RESAMPLES,
            seed=20260806,
            wal_fraction=_TIGHTENED_WAL_FRACTION,
            wal_floor_bytes=_TIGHTENED_WAL_FLOOR_BYTES,
        )

        assert cell.wal.verdict is Verdict.FAIL
        assert cell.verdict is Verdict.FAIL
        # The control is only reproducible if the artifact says what it used.
        assert cell.wal.fraction == _TIGHTENED_WAL_FRACTION
        assert cell.wal.floor_bytes == _TIGHTENED_WAL_FLOOR_BYTES
        assert cell.wal.limit_bytes == float(_TIGHTENED_WAL_FLOOR_BYTES)
        assert cell.wal.deliberately_tightened

    def test_declared_budget_is_not_marked_as_tightened(self) -> None:
        # Guards the flag itself: if it read true under the declared budget,
        # every artifact would claim to be a control.
        comparison = compare_wal(
            baseline=_counts(wal_bytes=1_000, terminal_rows=10),
            candidate=_counts(wal_bytes=1_200, terminal_rows=10),
        )

        assert not comparison.deliberately_tightened
        assert comparison.verdict is Verdict.PASS

    def test_the_same_measurement_passes_under_the_declared_budget(
        self,
        installed: tuple[AsyncConnection, InstalledComparison],
    ) -> None:
        # Isolates the tightening from the workload: same counts, two limits.
        baseline = _counts(wal_bytes=100_000, terminal_rows=100)
        candidate = _counts(wal_bytes=140_000, terminal_rows=100)

        declared = compare_wal(baseline=baseline, candidate=candidate)
        tightened = compare_wal(
            baseline=baseline,
            candidate=candidate,
            fraction=_TIGHTENED_WAL_FRACTION,
            floor_bytes=_TIGHTENED_WAL_FLOOR_BYTES,
        )

        assert declared.verdict is Verdict.PASS
        assert tightened.verdict is Verdict.FAIL


class TestLeafBoundsAreTimezoneIndependent:
    """A leaf must tile against the shared fixture's leaves in any session zone.

    The shared fixture states its daily bounds as explicit UTC instants. A bare
    date literal is resolved in the session timezone instead, so on a server
    east of UTC this collector's leaf began before the previous day's leaf
    ended and PostgreSQL refused it as an overlap. The installer worked on a
    UTC bench and failed everywhere else, which is the shape of bug that makes
    a green run unreproducible rather than wrong.
    """

    @pytest.mark.parametrize(
        'session_timezone',
        ('UTC', 'Europe/Berlin', 'Pacific/Kiritimati', 'Etc/GMT+12'),
    )
    async def test_installer_succeeds_under_any_session_timezone(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
        session_timezone: str,
    ) -> None:
        async with engine.connect() as connection:
            await connection.execute(
                text(f"SET TIME ZONE '{session_timezone}'")
            )
            schema = PrototypeSchema(f'rerun_tz_{uuid4().hex[:10]}')
            await install_archive_candidates(connection, schema)
            await install_archive_transcode_prototype(connection, schema)
            try:
                comparison = await install_rerun_terminalization_prototype(
                    connection,
                    schema,
                )
                await connection.commit()

                assert comparison.finite_leaf.startswith(
                    'history_aggregate_finite_'
                )
            finally:
                await connection.rollback()
                await remove_archive_candidates(connection, schema)
                await connection.commit()

    @pytest.mark.parametrize(
        'session_timezone',
        ('UTC', 'Europe/Berlin', 'Etc/GMT+12'),
    )
    async def test_leaf_is_named_for_the_utc_date_it_covers(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,  # noqa: ARG001 - installs schema v26
        session_timezone: str,
    ) -> None:
        # The name and the bounds have to agree, or the same-date collision
        # check against the shared fixture's leaf stops being sound.
        async with engine.connect() as connection:
            await connection.execute(
                text(f"SET TIME ZONE '{session_timezone}'")
            )
            utc_date = (
                await connection.execute(
                    text("SELECT (now() AT TIME ZONE 'UTC')::date")
                )
            ).scalar_one()
            schema = PrototypeSchema(f'rerun_tz_{uuid4().hex[:10]}')
            await install_archive_candidates(connection, schema)
            await install_archive_transcode_prototype(connection, schema)
            try:
                comparison = await install_rerun_terminalization_prototype(
                    connection,
                    schema,
                )
                await connection.commit()

                expected = (
                    'history_aggregate_finite_'
                    f'{utc_date.strftime("%Y_%m_%d")}'
                )
                assert comparison.finite_leaf == expected
            finally:
                await connection.rollback()
                await remove_archive_candidates(connection, schema)
                await connection.commit()
