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
from dataclasses import dataclass
from typing import Any

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
        connection, batch_size=batch_size, retain_default=False
    )
    stages.append(
        StageDuration(
            name='preparation', seconds=preparation.seconds, term='itemized'
        )
    )
    peak_bytes = max(peak_bytes, await _relation_bytes(connection))

    relocation = await _drive_relocation(connection, batch_size=batch_size)
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
        fixed_seconds=fixed_seconds,
        total_seconds=total_seconds,
        itemized_remainder_seconds=itemized,
        relation_bytes_before=bytes_before,
        relation_bytes_after=await _relation_bytes(connection),
        peak_relation_bytes=peak_bytes,
    )
