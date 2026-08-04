"""Orchestrating a comparison so the machine cannot decide the answer.

Two implementations measured one after the other on a shared runner do not
produce a comparison; they produce two readings taken under different
conditions, and whichever ran while a neighbouring job compacted its logs
loses. Blocks are therefore interleaved — one block of each side, alternating,
until both have their sample counts — so whatever drifts during the run drifts
through both sides equally.

The first block of each side is discarded. A cold shared buffer cache, an
unplanned statement and an unwarmed connection are real costs, but they are
paid once and would otherwise land entirely on whichever side ran first.

Wall-clock is measured with no EXPLAIN in the loop, because the instrumentation
that reports a plan also changes the time it takes to execute.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from itertools import islice
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import assert_never

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from tests.perf.counters import Counts, CounterProbe, install_extension
from tests.perf.scenarios import (
    BatchScenario,
    Scenario,
    SingleRowScenario,
    analyze,
    delete_seeded,
    id_prefix,
    seed_expired_pending_tasks,
    seed_running_tasks,
    seed_terminal_ballast,
    task_ids,
)
from tests.perf.statistics import Comparison, Verdict, compare, worst

BALLAST_ROWS = 100_000


class Side(Enum):
    """Which implementation a block is measuring."""

    BASELINE = 'baseline'
    CANDIDATE = 'candidate'


@dataclass(frozen=True, slots=True)
class Conditions:
    """Everything a reader needs to know a number is comparable to another.

    A measurement without its conditions is a number without a claim. These
    travel with the result rather than being described in a message, because
    the message will not be there in a year.
    """

    scenario: str
    description: str
    server_version: str
    fsync: str
    synchronous_commit: str
    observations_per_side: int
    block_size: int
    ballast_rows: int
    payload_bytes: int
    batch_size: int | None
    resamples: int
    seed: int
    comparison: str
    demo_quiesced: bool
    measured_at: str


@dataclass(frozen=True, slots=True)
class Measurement:
    """One side's timings and what the server counted while they ran."""

    samples_ms: list[float]
    counts: Counts


@dataclass(frozen=True, slots=True)
class RunResult:
    conditions: Conditions
    baseline: Measurement
    candidate: Measurement
    comparisons: list[Comparison]
    verdict: Verdict


def run_scenario(
    engine: Engine,
    *,
    scenario: Scenario,
    observations: int,
    block_size: int,
    resamples: int,
    seed: int,
    demo_quiesced: bool,
) -> RunResult:
    """Measure both sides of one scenario and judge the difference."""
    baseline_prefix = id_prefix(scenario.name, 'base')
    candidate_prefix = id_prefix(scenario.name, 'cand')
    ballast_prefix = id_prefix(scenario.name, 'ball')

    with engine.connect() as connection:
        install_extension(connection)
        server_version = _server_setting(connection, 'server_version')
        fsync = _server_setting(connection, 'fsync')
        synchronous_commit = _server_setting(connection, 'synchronous_commit')

        for stale in (baseline_prefix, candidate_prefix, ballast_prefix):
            delete_seeded(connection, prefix=stale)
        seed_terminal_ballast(
            connection,
            prefix=ballast_prefix,
            count=BALLAST_ROWS,
            payload_bytes=200,
        )
        try:
            baseline, candidate = _measure_interleaved(
                connection,
                scenario=scenario,
                baseline_prefix=baseline_prefix,
                candidate_prefix=candidate_prefix,
                observations=observations,
                block_size=block_size,
            )
        finally:
            for seeded in (baseline_prefix, candidate_prefix, ballast_prefix):
                delete_seeded(connection, prefix=seeded)

    comparisons = [
        compare(
            baseline=baseline.samples_ms,
            candidate=candidate.samples_ms,
            percentile=percentile,
            budget=budget,
            resamples=resamples,
            seed=seed,
        )
        for percentile, budget in (
            (50.0, scenario.p50_budget),
            (99.0, scenario.p99_budget),
        )
    ]

    return RunResult(
        conditions=Conditions(
            scenario=scenario.name,
            description=scenario.description,
            server_version=server_version,
            fsync=fsync,
            synchronous_commit=synchronous_commit,
            observations_per_side=len(baseline.samples_ms),
            block_size=block_size,
            ballast_rows=BALLAST_ROWS,
            payload_bytes=_payload_bytes(scenario),
            batch_size=_batch_size(scenario),
            resamples=resamples,
            seed=seed,
            comparison=_comparison_label(scenario),
            demo_quiesced=demo_quiesced,
            measured_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
        ),
        baseline=baseline,
        candidate=candidate,
        comparisons=comparisons,
        verdict=worst([c.verdict for c in comparisons]),
    )


def _measure_interleaved(
    connection: Connection,
    *,
    scenario: Scenario,
    baseline_prefix: str,
    candidate_prefix: str,
    observations: int,
    block_size: int,
) -> tuple[Measurement, Measurement]:
    """Alternate blocks between the two sides until both are complete.

    Each side gets its own seeded id range: an operation consumes the row it
    acts on, so the two sides cannot share rows and must be given rows in the
    same state.
    """
    blocks = -(-observations // block_size)
    prefixes = {Side.BASELINE: baseline_prefix, Side.CANDIDATE: candidate_prefix}

    # A single-row operation consumes the row named in its parameters, so both
    # sides can be seeded once from disjoint id ranges. A batch operation
    # selects its own targets under a predicate no caller controls, so a shared
    # pool would let one side consume rows seeded for the other — the sides
    # would not be running the same workload, and every number would be
    # measuring the interference. Batch blocks are therefore seeded one at a
    # time, immediately before the block that consumes them.
    ids: dict[Side, Iterator[str]] = {}
    match scenario:
        case SingleRowScenario():
            # One warmup block per side beyond the measured ones.
            per_side_rows = (blocks + 1) * block_size
            for side, prefix in prefixes.items():
                _seed_for(scenario, connection, prefix=prefix, count=per_side_rows)
                ids[side] = task_ids(prefix, per_side_rows)
            analyze(connection)
        case BatchScenario():
            for side in prefixes:
                ids[side] = iter(())
        case _ as unreachable:
            assert_never(unreachable)

    samples: dict[Side, list[float]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    counts: dict[Side, list[Counts]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    probe = CounterProbe(connection)

    for side in (Side.BASELINE, Side.CANDIDATE):
        _seed_block(scenario, connection, prefix=prefixes[side], block_size=block_size)
        _run_block(connection, scenario, ids[side], block_size, side=side)

    for _ in range(blocks):
        for side in (Side.BASELINE, Side.CANDIDATE):
            _seed_block(
                scenario, connection, prefix=prefixes[side], block_size=block_size,
            )
            probe.begin()
            samples[side] += _run_block(
                connection, scenario, ids[side], block_size, side=side,
            )
            counts[side].append(probe.finish())

    return (
        Measurement(
            samples_ms=samples[Side.BASELINE],
            counts=_total(counts[Side.BASELINE]),
        ),
        Measurement(
            samples_ms=samples[Side.CANDIDATE],
            counts=_total(counts[Side.CANDIDATE]),
        ),
    )


def _run_block(
    connection: Connection,
    scenario: Scenario,
    ids: Iterator[str],
    block_size: int,
    *,
    side: Side,
) -> list[float]:
    """One block of operations, each timed on its own."""
    samples: list[float] = []
    match scenario:
        case SingleRowScenario():
            run_one = _single_row_callable(scenario, side)
            for task_id in _take(ids, block_size):
                started = time.perf_counter_ns()
                run_one(connection, task_id)
                samples.append((time.perf_counter_ns() - started) / 1_000_000)
        case BatchScenario():
            run_batch = _batch_callable(scenario, side)
            for _ in range(block_size):
                started = time.perf_counter_ns()
                run_batch(connection)
                samples.append((time.perf_counter_ns() - started) / 1_000_000)
        case _ as unreachable:
            assert_never(unreachable)
    return samples


def _take(ids: Iterator[str], count: int) -> list[str]:
    # islice rather than zip against a range: zip pulls from the iterator
    # before it checks the range, so it discards one id per call and the seed
    # runs short by exactly the number of blocks.
    taken = list(islice(ids, count))
    if len(taken) != count:
        raise RuntimeError(
            f'seeded rows exhausted after {len(taken)} of {count}: an operation '
            f'consumed more rows than the seed provided'
        )
    return taken


def _single_row_callable(
    scenario: SingleRowScenario,
    side: Side,
) -> Callable[[Connection, str], None]:
    match side:
        case Side.BASELINE:
            return scenario.baseline
        case Side.CANDIDATE:
            return scenario.candidate or scenario.baseline
        case _ as unreachable:
            assert_never(unreachable)


def _batch_callable(
    scenario: BatchScenario,
    side: Side,
) -> Callable[[Connection], None]:
    match side:
        case Side.BASELINE:
            return scenario.baseline
        case Side.CANDIDATE:
            return scenario.candidate or scenario.baseline
        case _ as unreachable:
            assert_never(unreachable)


def _total(blocks: list[Counts]) -> Counts:
    return Counts(
        client_statements=sum(b.client_statements for b in blocks),
        nested_statements=sum(b.nested_statements for b in blocks),
        rows_affected=sum(b.rows_affected for b in blocks),
        wal_records=sum(b.wal_records for b in blocks),
        wal_bytes=sum(b.wal_bytes for b in blocks),
        wal_fpi=sum(b.wal_fpi for b in blocks),
        write_transactions=sum(b.write_transactions for b in blocks),
    )


def _seed_block(
    scenario: Scenario,
    connection: Connection,
    *,
    prefix: str,
    block_size: int,
) -> None:
    """Rows a batch block will consume, seeded immediately before it runs."""
    match scenario:
        case SingleRowScenario():
            return
        case BatchScenario(batch_size=batch_size):
            # Clear first: a batch operation transitions its rows rather than
            # removing them, so last block's ids are still present and would
            # collide. Every block therefore starts from a pool holding exactly
            # what it is meant to consume.
            delete_seeded(connection, prefix=prefix)
            seed_expired_pending_tasks(
                connection, prefix=prefix, count=block_size * batch_size,
            )
            analyze(connection)
        case _ as unreachable:
            assert_never(unreachable)


def _seed_for(
    scenario: Scenario,
    connection: Connection,
    *,
    prefix: str,
    count: int,
) -> None:
    match scenario:
        case SingleRowScenario():
            seed_running_tasks(connection, prefix=prefix, count=count)
        case BatchScenario():
            seed_expired_pending_tasks(connection, prefix=prefix, count=count)
        case _ as unreachable:
            assert_never(unreachable)


def _payload_bytes(scenario: Scenario) -> int:
    match scenario:
        case SingleRowScenario(payload_bytes=payload_bytes):
            return payload_bytes
        case BatchScenario():
            return 200
        case _ as unreachable:
            assert_never(unreachable)


def _batch_size(scenario: Scenario) -> int | None:
    match scenario:
        case SingleRowScenario():
            return None
        case BatchScenario(batch_size=batch_size):
            return batch_size
        case _ as unreachable:
            assert_never(unreachable)


def _comparison_label(scenario: Scenario) -> str:
    """What the run actually compared, stated where a reader will see it."""
    match scenario:
        case SingleRowScenario(candidate=None) | BatchScenario(candidate=None):
            return 'existing statement against itself (harness control)'
        case SingleRowScenario() | BatchScenario():
            return 'existing statement against database-owned operation'
        case _ as unreachable:
            assert_never(unreachable)


def _server_setting(connection: Connection, name: str) -> str:
    return str(connection.execute(text(f'SHOW {name}')).scalar_one())
