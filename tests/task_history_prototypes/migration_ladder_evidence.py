"""Rung-1 measurement for the laddered migration-capacity campaign.

One offline cutover of a seeded legacy install, driven stage by stage so the
two model terms come from different instruments: the fixed term summed from
directly-timed stage boundaries, the per-row term from a least-squares fit over
the copy stage's own batch trajectory. Neither is inferred from the other, and
the regression's intercept travels beside the measured fixed term rather than
folded into it.

Preparation carries its own per-row figure. It is row-proportional like the
copy stage, so folding it into the fixed term would misreport a constant, and
folding it into the copy slope would attribute cost where it was not measured.
It is reported in the itemized remainder, fitted from its own trajectory, and
validated by nothing yet.

This run produces coefficients and a footprint declaration. It produces NO rung
verdict: a rung is judged against an estimate fitted from a previous rung, and
this is where the estimate comes from.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.cutover.ladder import (
    BatchCommit,
    FittedRun,
    MeasuredRun,
    fit_run,
)
from horsies.core.history.cutover.preparation import (
    PreparationComplete,
    prepare_legacy_batch,
)
from horsies.core.history.cutover.relocation import (
    RelocationComplete,
    relocate_terminal_batch,
)


_CLASS_KEY = 'finite_30d_v1'

# Evidence is rewritten this often inside a batch loop so that a run killed at
# the lane's hard cap still yields the trajectory up to the kill. A ten-million
# row rung died at the cap having written nothing, and the trajectory it
# destroyed was the diagnosis. The write is atomic and fsynced, so it costs
# single-digit milliseconds against multi-second batches: at one flush per 50
# batches over 1,000 batches that is on the order of 200ms inside a window of
# hours. It is not free, and this comment states the bound rather than claiming
# the measured path is untouched.
FLUSH_EVERY_BATCHES: Final = 50

# A stage whose per-batch duration drifts more than this fraction of its own
# mean batch, across the whole run, is not extrapolated. Sandwiched from
# measurement: a linear copy stage drifted 1% and 3% of its mean batch on the
# two majors, while a stage whose selection rescanned already-finished rows
# drifted +22%. Five percent sits 1.7x above the largest passing measurement
# and 4.4x below the failing one.
PER_BATCH_TREND_GATE_FRACTION: Final = 0.05

# Called with a stage name and the commits recorded so far.
ProgressSink = Callable[[str, tuple[BatchCommit, ...]], None]


class RungMeasurementError(Exception):
    """The rung could not be measured as declared."""


@dataclass(frozen=True, slots=True)
class StageDuration:
    """One stage's wall time, and which model term it belongs to."""

    name: str
    seconds: float
    term: str  # 'fixed' | 'per_row' | 'itemized'


@dataclass(frozen=True, slots=True)
class Trajectory:
    """One caller-driven loop's committed batch series."""

    stage: str
    batch_size: int
    batches: int
    rows: int
    seconds: float
    commits: tuple[BatchCommit, ...]

    @property
    def rows_per_second(self) -> float:
        return self.rows / self.seconds if self.seconds else 0.0


@dataclass(frozen=True, slots=True)
class PerBatchTrend:
    """Whether one stage's per-batch cost holds still across the run.

    A least-squares slope exists for any trajectory; its existence says
    nothing about whether the relation is linear. When per-batch duration
    rises with batch index, the fitted slope is an average over the measured
    range rather than a per-row cost, and extrapolating it under-predicts.
    """

    stage: str
    batches: int
    mean_batch_seconds: float
    trend_seconds_per_batch: float
    total_drift_seconds: float
    drift_fraction_of_mean: float
    gate_fraction: float
    within_gate: bool


def compute_per_batch_trend(trajectory: Trajectory) -> PerBatchTrend:
    """Fit per-batch duration against batch index and gate the drift.

    Durations are differences of the cumulative elapsed series, so the first
    batch carries any stage entry cost. That is the honest reading: it is
    time the stage spent before its first commit.
    """
    commits = trajectory.commits
    if len(commits) < 2:
        raise RungMeasurementError(
            f'{trajectory.stage} needs at least two committed batches to '
            'show a per-batch trend; one batch cannot exhibit drift'
        )
    durations = [commits[0].elapsed_seconds] + [
        commits[index].elapsed_seconds - commits[index - 1].elapsed_seconds
        for index in range(1, len(commits))
    ]
    count = len(durations)
    mean_duration = sum(durations) / count
    if mean_duration <= 0.0:
        raise RungMeasurementError(
            f'{trajectory.stage} reported a non-positive mean batch '
            f'duration ({mean_duration}); the trend gate cannot be applied'
        )
    indexes = list(range(count))
    mean_index = sum(indexes) / count
    denominator = sum((index - mean_index) ** 2 for index in indexes)
    if denominator <= 0.0:
        raise RungMeasurementError(
            f'{trajectory.stage} batch indexes do not vary; the per-batch '
            'trend is undefined'
        )
    trend = (
        sum(
            (index - mean_index) * (duration - mean_duration)
            for index, duration in zip(indexes, durations)
        )
        / denominator
    )
    drift = trend * count
    fraction = drift / mean_duration
    return PerBatchTrend(
        stage=trajectory.stage,
        batches=count,
        mean_batch_seconds=mean_duration,
        trend_seconds_per_batch=trend,
        total_drift_seconds=drift,
        drift_fraction_of_mean=fraction,
        gate_fraction=PER_BATCH_TREND_GATE_FRACTION,
        within_gate=abs(fraction) <= PER_BATCH_TREND_GATE_FRACTION,
    )


@dataclass(frozen=True, slots=True)
class RungMeasurement:
    """Everything rung 1 establishes, with the seams visible."""

    rows: int
    attempts_per_task: int
    batch_size: int
    stages: tuple[StageDuration, ...]
    relocation: Trajectory
    preparation: Trajectory
    fitted: FittedRun
    preparation_seconds_per_million_rows: float
    trends: tuple[PerBatchTrend, ...]
    fixed_seconds: float
    total_seconds: float
    itemized_remainder_seconds: float
    relation_bytes_before: int
    relation_bytes_after: int
    peak_relation_bytes: int
    rung_verdict: str = 'no rung verdict by design'
    preflight_estimate: str = (
        'meaningless on rung 1: preflight consumes coefficients and this '
        'rung produces them'
    )


async def _relation_bytes(connection: AsyncConnection) -> int:
    """Total bytes of the relations the cutover reads and writes."""
    return int(
        (
            await connection.execute(
                text(
                    """
                    SELECT COALESCE(SUM(
                        pg_total_relation_size(c.oid)
                    ), 0)::bigint
                    FROM pg_class AS c
                    JOIN pg_namespace AS n ON n.oid = c.relnamespace
                    WHERE n.nspname = current_schema()
                      AND c.relkind IN ('r', 'p')
                      AND (
                        c.relname = 'horsies_tasks'
                        OR c.relname = 'horsies_task_attempts'
                        OR c.relname LIKE 'horsies_task_history%'
                      )
                    """
                )
            )
        ).scalar_one()
    )


async def seed_legacy_install(
    connection: AsyncConnection,
    *,
    rows: int,
    attempts_per_task: int,
    class_key: str,
    seed_batch: int = 50_000,
) -> None:
    """Bulk-seed terminal rows as 0.4.x left them.

    Row-at-a-time seeding is what the correctness suites use; at a million
    rows it would dominate the run it is supposed to set up. The column set
    mirrors the correctness helper exactly, including the transitional
    columns in their pre-backfill state — `prepared_rerun_input_disposition`
    stays NULL, which is the marker preparation selects on.
    """
    if rows <= 0 or attempts_per_task < 0:
        raise RungMeasurementError('rows must be positive, attempts non-negative')
    for start in range(0, rows, seed_batch):
        count = min(seed_batch, rows - start)
        await connection.execute(
            text(
                """
                INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, status, result,
                    args, kwargs, task_options,
                    enqueued_at, created_at, started_at, claimed_at,
                    terminal_at, terminalization_kind,
                    retry_count, max_retries, error_code,
                    claimed_by_worker_id, worker_hostname, worker_pid,
                    worker_process_name, is_workflow_task, enqueue_sha,
                    command_fingerprint_version, command_fingerprint,
                    retention_class_key, retain_rerun_input,
                    prepared_rerun_input_disposition
                )
                SELECT
                    gen_random_uuid(), 'legacy.task', 'default', 50,
                    'COMPLETED', '{"ok": true}',
                    '[]', '{}', NULL,
                    now() - interval '2 hours', now() - interval '2 hours',
                    now() - interval '1 hour', now() - interval '1 hour',
                    now() - interval '30 minutes', 'COMPLETE_LOCKED',
                    0, 0, NULL,
                    'legacy-worker', 'legacy-host', 4242, 'legacy-proc',
                    FALSE, repeat('a', 64),
                    1, decode(md5(series::text) || md5((series + 1)::text), 'hex'),
                    :class_key, FALSE,
                    NULL
                FROM generate_series(CAST(:lo AS bigint), CAST(:hi AS bigint)) AS series
                """
            ),
            {'class_key': class_key, 'lo': start + 1, 'hi': start + count},
        )
        await connection.commit()

    if attempts_per_task:
        await connection.execute(
            text(
                """
                INSERT INTO horsies_task_attempts (
                    task_id, attempt, outcome, will_retry,
                    started_at, finished_at, failed_reason
                )
                SELECT t.id, a.attempt, 'COMPLETED', FALSE,
                       now() - interval '1 hour', now() - interval '59 minutes',
                       NULL
                FROM horsies_tasks AS t
                CROSS JOIN generate_series(1, CAST(:attempts AS integer)) AS a(attempt)
                WHERE t.task_name = 'legacy.task'
                """
            ),
            {'attempts': attempts_per_task},
        )
        await connection.commit()


async def _drive_preparation(
    connection: AsyncConnection,
    *,
    batch_size: int,
    retain_default: bool,
    on_progress: ProgressSink,
) -> Trajectory:
    """Run preparation to completion, recording its own trajectory."""
    commits: list[BatchCommit] = []
    cumulative = 0
    batches = 0
    started = time.perf_counter()
    while True:
        outcome = await prepare_legacy_batch(
            connection,
            retain_default=retain_default,
            batch_size=batch_size,
        )
        await connection.commit()
        if isinstance(outcome, PreparationComplete):
            break
        cumulative += outcome.rows_prepared
        batches += 1
        commits.append(
            BatchCommit(
                cumulative_rows=cumulative,
                elapsed_seconds=time.perf_counter() - started,
            )
        )
        if batches % FLUSH_EVERY_BATCHES == 0:
            on_progress('preparation', tuple(commits))
    return Trajectory(
        stage='preparation',
        batch_size=batch_size,
        batches=batches,
        rows=cumulative,
        seconds=time.perf_counter() - started,
        commits=tuple(commits),
    )


async def _drive_relocation(
    connection: AsyncConnection,
    *,
    batch_size: int,
    on_progress: ProgressSink,
) -> Trajectory:
    """Run relocation to completion, recording the copy-stage trajectory."""
    commits: list[BatchCommit] = []
    cumulative = 0
    batches = 0
    started = time.perf_counter()
    while True:
        outcome = await relocate_terminal_batch(
            connection,
            batch_size=batch_size,
        )
        await connection.commit()
        if isinstance(outcome, RelocationComplete):
            break
        cumulative += outcome.rows_relocated
        batches += 1
        commits.append(
            BatchCommit(
                cumulative_rows=cumulative,
                elapsed_seconds=time.perf_counter() - started,
            )
        )
        if batches % FLUSH_EVERY_BATCHES == 0:
            on_progress('relocation', tuple(commits))
    return Trajectory(
        stage='relocation',
        batch_size=batch_size,
        batches=batches,
        rows=cumulative,
        seconds=time.perf_counter() - started,
        commits=tuple(commits),
    )


def _slope_per_million(trajectory: Trajectory) -> float:
    """Least-squares slope over one trajectory, in seconds per million rows."""
    if len({commit.cumulative_rows for commit in trajectory.commits}) < 2:
        raise RungMeasurementError(
            f'{trajectory.stage} needs at least two distinct commit points; '
            'one point cannot separate a slope from an intercept'
        )
    xs = [c.cumulative_rows / 1_000_000 for c in trajectory.commits]
    ys = [c.elapsed_seconds for c in trajectory.commits]
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denominator = sum((x - mean_x) ** 2 for x in xs)
    return sum(
        (x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)
    ) / denominator


async def measure_rung(
    connection: AsyncConnection,
    *,
    rows: int,
    attempts_per_task: int,
    batch_size: int,
    class_key: str,
    backup_label: str,
    on_progress: ProgressSink,
) -> RungMeasurement:
    """Drive one offline cutover, timing every stage at its boundary.

    Stage attribution follows the ratified budget exactly: the fixed term is
    preflight plus the binding switch plus validation, and nothing else is
    quietly added to it. Drain, program installation and preparation are
    itemized beside it — preparation with its own per-row figure, because it
    scales with the same variable the copy slope models and belongs in
    neither term.
    """
    from horsies.core.history.cutover.drain import verify_drained
    from horsies.core.history.cutover.identity import (
        normalize_attempt_identity,
    )
    from horsies.core.history.cutover.preflight import (
        RelocationCoefficients,
        run_preflight,
    )
    from horsies.core.history.cutover.program import install_programs
    from horsies.core.history.cutover.tighten import (
        confirmation_phrase,
        tighten_to_frozen,
    )
    from horsies.core.history.cutover.validation import validate_cutover

    stages: list[StageDuration] = []
    peak_bytes = bytes_before = await _relation_bytes(connection)

    async def timed(name: str, term: str, coroutine: Any) -> Any:
        started = time.perf_counter()
        outcome = await coroutine
        stages.append(
            StageDuration(
                name=name,
                seconds=time.perf_counter() - started,
                term=term,
            )
        )
        # Every stage boundary is a flush point, so a run killed between
        # stages still names the stage it completed last.
        on_progress(name, ())
        return outcome

    run_started = time.perf_counter()

    # Preflight takes coefficients and returns an estimate built from them.
    # Rung 1 is where coefficients come from, so it has none: it is run with
    # a null-coefficient placeholder, timed for its stage cost, and its
    # ESTIMATE IS MEANINGLESS BY CONSTRUCTION on this rung. That is recorded
    # rather than hidden — an artifact carrying an estimate nobody should
    # read is worse than one that says why.
    await timed(
        'preflight',
        'fixed',
        run_preflight(
            connection,
            coefficients=RelocationCoefficients(
                seconds_per_million_rows=0.0, fixed_seconds=0.0
            ),
        ),
    )
    await timed('drain', 'itemized', verify_drained(connection))
    await timed(
        'identity', 'itemized', normalize_attempt_identity(connection)
    )
    await timed('program', 'itemized', install_programs(connection))
    await connection.commit()

    preparation = await _drive_preparation(
        connection,
        batch_size=batch_size,
        retain_default=False,
        on_progress=on_progress,
    )
    stages.append(
        StageDuration(
            name='preparation', seconds=preparation.seconds, term='itemized'
        )
    )
    peak_bytes = max(peak_bytes, await _relation_bytes(connection))

    relocation = await _drive_relocation(
        connection, batch_size=batch_size, on_progress=on_progress
    )
    stages.append(
        StageDuration(
            name='relocation', seconds=relocation.seconds, term='per_row'
        )
    )
    peak_bytes = max(peak_bytes, await _relation_bytes(connection))

    await timed(
        'tighten',
        'fixed',
        tighten_to_frozen(
            connection,
            backup_label=backup_label,
            operator_confirmation=confirmation_phrase(backup_label),
        ),
    )
    await connection.commit()
    await timed('validation', 'fixed', validate_cutover(connection))
    await connection.commit()

    total_seconds = time.perf_counter() - run_started
    fixed_seconds = sum(s.seconds for s in stages if s.term == 'fixed')
    itemized = sum(s.seconds for s in stages if s.term == 'itemized')

    fitted = fit_run(
        MeasuredRun(
            rows=rows,
            seconds=total_seconds,
            fixed_seconds=fixed_seconds,
            commits=relocation.commits,
        )
    )

    return RungMeasurement(
        rows=rows,
        attempts_per_task=attempts_per_task,
        batch_size=batch_size,
        stages=tuple(stages),
        relocation=relocation,
        preparation=preparation,
        fitted=fitted,
        preparation_seconds_per_million_rows=_slope_per_million(preparation),
        trends=(
            compute_per_batch_trend(preparation),
            compute_per_batch_trend(relocation),
        ),
        fixed_seconds=fixed_seconds,
        total_seconds=total_seconds,
        itemized_remainder_seconds=itemized,
        relation_bytes_before=bytes_before,
        relation_bytes_after=await _relation_bytes(connection),
        peak_relation_bytes=peak_bytes,
    )


@dataclass(frozen=True, slots=True)
class FootprintDeclaration:
    """What rung 1 measured, and what it implies for the next rung.

    Section 11.1.2 selects rung 2's vehicle from this: the extrapolated peak
    must fit the runner with at least 25% headroom to ride the same lane.
    """

    rung_rows: int
    peak_relation_bytes: int
    bytes_per_row: float
    next_rung_rows: int
    extrapolated_peak_bytes: int
    runner_free_bytes: int
    headroom_fraction: float
    fits_same_lane: bool


RUNG2_HEADROOM = 0.25


def declare_footprint(
    measurement: RungMeasurement,
    *,
    next_rung_rows: int,
    runner_free_bytes: int,
) -> FootprintDeclaration:
    """Extrapolate this rung's measured peak to the next rung's row count."""
    per_row = measurement.peak_relation_bytes / measurement.rows
    extrapolated = int(per_row * next_rung_rows)
    headroom = (
        (runner_free_bytes - extrapolated) / extrapolated
        if extrapolated
        else 0.0
    )
    return FootprintDeclaration(
        rung_rows=measurement.rows,
        peak_relation_bytes=measurement.peak_relation_bytes,
        bytes_per_row=per_row,
        next_rung_rows=next_rung_rows,
        extrapolated_peak_bytes=extrapolated,
        runner_free_bytes=runner_free_bytes,
        headroom_fraction=headroom,
        fits_same_lane=headroom >= RUNG2_HEADROOM,
    )


@dataclass(frozen=True, slots=True)
class MigrationLadderEvidence:
    conditions: Any
    workload: dict[str, int | str]
    measurement: RungMeasurement
    footprint: FootprintDeclaration


async def collect_migration_ladder_evidence(
    engine: Any,
    *,
    commit: str,
    run_kind: Any,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    rows: int,
    attempts_per_task: int,
    batch_size: int,
    next_rung_rows: int,
    data_path: Any,
    checkpoint_path: Path | None = None,
) -> MigrationLadderEvidence:
    """Seed, cut over, and measure one rung on a disposable database.

    The database is created for this rung and dropped after it. The cutover is
    a one-way program — it installs replacement objects and freezes the legacy
    posture — so a rung cannot be repeated against a database that already ran
    one, and reusing a database would measure the wrong thing rather than fail
    loudly.
    """
    import uuid as _uuid
    from shutil import disk_usage

    from sqlalchemy.ext.asyncio import create_async_engine

    from tests.integration.task_history_harness import prepare_move_storage
    from tests.task_history_prototypes.evidence import (
        collect_operational_conditions,
    )
    from tests.task_history_prototypes.qualification_io import (
        AtomicEvidenceWriter,
    )

    writer = AtomicEvidenceWriter(checkpoint_path)

    def flush_progress(
        stage: str, commits: tuple[BatchCommit, ...]
    ) -> None:
        """Write what is known so far, so a killed run still says something.

        The partial document is deliberately shaped unlike the finished one:
        it carries `status: in_progress`, so a reader cannot mistake a
        snapshot from a killed run for a completed measurement.
        """
        writer.write(
            {
                'status': 'in_progress',
                'scenario': 'migration-ladder',
                'last_stage': stage,
                'batches_committed': len(commits),
                'commits': commits,
                'workload': {
                    'rung_rows': rows,
                    'attempts_per_task': attempts_per_task,
                    'batch_size': batch_size,
                    'next_rung_rows': next_rung_rows,
                },
            }
        )

    # render_as_string, not str(): SQLAlchemy masks the password in the
    # plain string form, and the disposable database is reached through a
    # fresh connection that needs the real one.
    admin_url = engine.url.render_as_string(hide_password=False)
    base = admin_url.rsplit('/', 1)[0]
    name = f'ladder_rung_{_uuid.uuid4().hex[:12]}'
    admin = create_async_engine(admin_url, isolation_level='AUTOCOMMIT')
    async with admin.connect() as connection:
        await connection.execute(text(f'CREATE DATABASE {name}'))
    await admin.dispose()

    rung_dsn = f'{base}/{name}'
    try:
        # The perf helper's public entry runs its own event loop, which
        # cannot nest inside this one. Applying the schema through the broker
        # directly is the same work without the loop.
        from pydantic import SecretStr

        from horsies.core.brokers.postgres import PostgresBroker
        from horsies.core.models.broker import PostgresConfig

        rung_broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(rung_dsn))
        )
        try:
            await rung_broker.ensure_schema_initialized()
        finally:
            await rung_broker.close_async()
        rung_engine = create_async_engine(rung_dsn)
        try:
            async with rung_engine.connect() as connection:
                conditions = await collect_operational_conditions(
                    connection,
                    commit=commit,
                    run_kind=run_kind,
                    server_image=server_image,
                    host_description=host_description,
                    storage_description=storage_description,
                    demo_quiesced=demo_quiesced,
                    cache_posture=(
                        'disposable database per rung; seeded immediately '
                        'before the measured cutover'
                    ),
                    prepared_posture=(
                        'constant batch size across both caller-driven loops'
                    ),
                )
                await prepare_move_storage(connection, _CLASS_KEY)
                await connection.commit()
                await seed_legacy_install(
                    connection,
                    rows=rows,
                    attempts_per_task=attempts_per_task,
                    class_key=_CLASS_KEY,
                )
                measurement = await measure_rung(
                    connection,
                    rows=rows,
                    attempts_per_task=attempts_per_task,
                    batch_size=batch_size,
                    class_key=_CLASS_KEY,
                    backup_label=f'ladder-{name}',
                    on_progress=flush_progress,
                )
        finally:
            await rung_engine.dispose()
    finally:
        admin = create_async_engine(
            admin_url, isolation_level='AUTOCOMMIT'
        )
        async with admin.connect() as connection:
            await connection.execute(
                text(f'DROP DATABASE IF EXISTS {name} WITH (FORCE)')
            )
        await admin.dispose()

    return MigrationLadderEvidence(
        conditions=conditions,
        workload={
            'rung_rows': rows,
            'attempts_per_task': attempts_per_task,
            'batch_size': batch_size,
            'next_rung_rows': next_rung_rows,
            'class_key': _CLASS_KEY,
            'rung_verdict': measurement.rung_verdict,
            'preflight_estimate': measurement.preflight_estimate,
            'fixed_term_stages': 'preflight + tighten + validation',
            'itemized_stages': 'drain + identity + program + preparation',
        },
        measurement=measurement,
        footprint=declare_footprint(
            measurement,
            next_rung_rows=next_rung_rows,
            runner_free_bytes=disk_usage(data_path).free,
        ),
    )
