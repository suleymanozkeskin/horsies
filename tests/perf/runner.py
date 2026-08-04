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

import json
import time
from collections.abc import Callable, Iterator
from itertools import islice
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, assert_never, cast

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from tests.perf.counters import Counts, CounterProbe, install_extension
from tests.perf.scenarios import (
    BatchScenario,
    Invocation,
    Scenario,
    SingleRowScenario,
    analyze,
    delete_seeded,
    id_prefix,
    seed_terminal_ballast,
    task_ids,
)
from tests.perf.statistics import Comparison, Verdict, compare, worst

BALLAST_ROWS = 100_000
WAL_RECORD_DELTA_PER_TERMINAL_ROW = 0.10


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
    full_page_writes: str
    synchronous_commit: str
    autovacuum: str
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
class PlanEvidence:
    """One instrumented execution per side, outside the latency samples."""

    baseline: dict[str, Any]
    candidate: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RunResult:
    conditions: Conditions
    baseline: Measurement
    candidate: Measurement
    plans: PlanEvidence | None
    comparisons: list[Comparison]
    contract_violations: tuple[str, ...]
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
    control: bool = False,
) -> RunResult:
    """Measure both sides of one scenario and judge the difference."""
    baseline_prefix = id_prefix(scenario.name, 'base')
    candidate_prefix = id_prefix(scenario.name, 'cand')
    ballast_prefix = id_prefix(scenario.name, 'ball')

    with engine.connect() as connection:
        install_extension(connection)
        server_version = server_setting(connection, 'server_version')
        fsync = server_setting(connection, 'fsync')
        full_page_writes = server_setting(connection, 'full_page_writes')
        synchronous_commit = server_setting(connection, 'synchronous_commit')
        autovacuum = server_setting(connection, 'autovacuum')

        for stale in (baseline_prefix, candidate_prefix):
            scenario.cleanup(connection, stale)
        delete_seeded(connection, ballast_prefix)
        seed_terminal_ballast(
            connection,
            prefix=ballast_prefix,
            count=BALLAST_ROWS,
            payload_bytes=200,
        )
        try:
            baseline, candidate, plans = _measure_interleaved(
                connection,
                scenario=scenario,
                baseline_prefix=baseline_prefix,
                candidate_prefix=candidate_prefix,
                observations=observations,
                block_size=block_size,
                control=control,
            )
        finally:
            # A statement failure leaves PostgreSQL's transaction aborted.
            # Roll it back before cleanup so the original error is not masked
            # by a second "current transaction is aborted" cleanup error.
            connection.rollback()
            for seeded in (baseline_prefix, candidate_prefix):
                scenario.cleanup(connection, seeded)
            delete_seeded(connection, ballast_prefix)

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

    conditions = Conditions(
        scenario=scenario.name,
        description=scenario.description,
        server_version=server_version,
        fsync=fsync,
        full_page_writes=full_page_writes,
        synchronous_commit=synchronous_commit,
        autovacuum=autovacuum,
        observations_per_side=len(baseline.samples_ms),
        block_size=block_size,
        ballast_rows=BALLAST_ROWS,
        payload_bytes=_payload_bytes(scenario),
        batch_size=_batch_size(scenario),
        resamples=resamples,
        seed=seed,
        comparison=_comparison_label(scenario, control=control),
        demo_quiesced=demo_quiesced,
        measured_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
    )
    contract_violations = _contract_violations(
        scenario=scenario,
        conditions=conditions,
        baseline=baseline,
        candidate=candidate,
        plans=plans,
        control=control,
    )
    latency_verdict = worst([c.verdict for c in comparisons])

    return RunResult(
        conditions=conditions,
        baseline=baseline,
        candidate=candidate,
        plans=plans,
        comparisons=comparisons,
        contract_violations=contract_violations,
        verdict=Verdict.FAIL if contract_violations else latency_verdict,
    )


def _measure_interleaved(
    connection: Connection,
    *,
    scenario: Scenario,
    baseline_prefix: str,
    candidate_prefix: str,
    observations: int,
    block_size: int,
    control: bool,
) -> tuple[Measurement, Measurement, PlanEvidence | None]:
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
    plans: PlanEvidence | None = None
    match scenario:
        case SingleRowScenario():
            # One warmup block per side beyond the measured ones.
            per_side_rows = (blocks + 1) * block_size
            for side, prefix in prefixes.items():
                _seed_for(scenario, connection, prefix=prefix, count=per_side_rows)
                ids[side] = task_ids(prefix, per_side_rows)
            analyze(connection)
            plans = _capture_plans(
                connection,
                scenario,
                baseline_warmup_id=f'{baseline_prefix}1',
                candidate_warmup_id=f'{candidate_prefix}1',
                baseline_task_id=f'{baseline_prefix}2',
                candidate_task_id=f'{candidate_prefix}2',
                control=control,
            )
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
        _run_block(
            connection,
            scenario,
            ids[side],
            block_size,
            side=side,
            control=control,
        )

    for _ in range(blocks):
        for side in (Side.BASELINE, Side.CANDIDATE):
            _seed_block(
                scenario,
                connection,
                prefix=prefixes[side],
                block_size=block_size,
            )
            probe.begin()
            block_samples, terminal_rows = _run_block(
                connection,
                scenario,
                ids[side],
                block_size,
                side=side,
                control=control,
            )
            samples[side] += block_samples
            counts[side].append(probe.finish(terminal_rows=terminal_rows))

    return (
        Measurement(
            samples_ms=samples[Side.BASELINE],
            counts=_total(counts[Side.BASELINE]),
        ),
        Measurement(
            samples_ms=samples[Side.CANDIDATE],
            counts=_total(counts[Side.CANDIDATE]),
        ),
        plans,
    )


def _capture_plans(
    connection: Connection,
    scenario: SingleRowScenario,
    *,
    baseline_warmup_id: str,
    candidate_warmup_id: str,
    baseline_task_id: str,
    candidate_task_id: str,
    control: bool,
) -> PlanEvidence | None:
    """Capture plan, buffer and WAL evidence without consuming measured rows."""
    candidate_factory = (
        scenario.baseline_invocation if control else scenario.candidate_invocation
    )
    if candidate_factory is None:
        return None
    # Each side gets an unrecorded instrumented execution first. Otherwise the
    # side captured second inherits catalog, index and heap pages read by the
    # first, turning shared-buffer evidence into an execution-order artifact.
    _explain(
        connection,
        scenario.baseline_invocation(baseline_warmup_id),
    )
    _explain(
        connection,
        candidate_factory(candidate_warmup_id),
    )
    return PlanEvidence(
        baseline=_explain(
            connection,
            scenario.baseline_invocation(baseline_task_id),
        ),
        candidate=_explain(
            connection,
            candidate_factory(candidate_task_id),
        ),
    )


def _explain(connection: Connection, invocation: Invocation) -> dict[str, Any]:
    statement = text(
        'EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)\n'
        + invocation.statement.text
    )
    try:
        raw: object = connection.execute(
            statement,
            invocation.parameters,
        ).scalar_one()
    finally:
        # EXPLAIN ANALYZE executes the transition. Rollback leaves the seeded
        # row eligible for the warmup and keeps plan work out of all counters.
        connection.rollback()
    if isinstance(raw, str):
        raw = cast(object, json.loads(raw))
    if not isinstance(raw, list):
        raise RuntimeError(
            f'{invocation.operation} returned an unexpected EXPLAIN document'
        )
    document = cast(list[object], raw)
    if len(document) != 1 or not isinstance(document[0], dict):
        raise RuntimeError(
            f'{invocation.operation} returned an unexpected EXPLAIN document'
        )
    return cast(dict[str, Any], document[0])


def _run_block(
    connection: Connection,
    scenario: Scenario,
    ids: Iterator[str],
    block_size: int,
    *,
    side: Side,
    control: bool,
) -> tuple[list[float], int]:
    """One block of operations, each timed on its own."""
    samples: list[float] = []
    terminal_rows = 0
    match scenario:
        case SingleRowScenario():
            run_one = _single_row_callable(scenario, side, control=control)
            for task_id in _take(ids, block_size):
                started = time.perf_counter_ns()
                terminal_rows += run_one(connection, task_id)
                samples.append((time.perf_counter_ns() - started) / 1_000_000)
        case BatchScenario():
            run_batch = _batch_callable(scenario, side, control=control)
            for _ in range(block_size):
                started = time.perf_counter_ns()
                terminal_rows += run_batch(connection)
                samples.append((time.perf_counter_ns() - started) / 1_000_000)
        case _ as unreachable:
            assert_never(unreachable)
    return samples, terminal_rows


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
    *,
    control: bool,
) -> Callable[[Connection, str], int]:
    match side:
        case Side.BASELINE:
            return scenario.baseline
        case Side.CANDIDATE:
            return (
                scenario.baseline
                if control
                else scenario.candidate or scenario.baseline
            )
        case _ as unreachable:
            assert_never(unreachable)


def _batch_callable(
    scenario: BatchScenario,
    side: Side,
    *,
    control: bool,
) -> Callable[[Connection], int]:
    match side:
        case Side.BASELINE:
            return scenario.baseline
        case Side.CANDIDATE:
            return (
                scenario.baseline
                if control
                else scenario.candidate or scenario.baseline
            )
        case _ as unreachable:
            assert_never(unreachable)


def _total(blocks: list[Counts]) -> Counts:
    return Counts(
        client_statements=sum(b.client_statements for b in blocks),
        nested_statements=sum(b.nested_statements for b in blocks),
        client_rows=sum(b.client_rows for b in blocks),
        nested_rows=sum(b.nested_rows for b in blocks),
        terminal_rows=sum(b.terminal_rows for b in blocks),
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
            scenario.cleanup(connection, prefix)
            scenario.seed(connection, prefix, block_size * batch_size)
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
    scenario.seed(connection, prefix, count)


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


def _comparison_label(scenario: Scenario, *, control: bool) -> str:
    """What the run actually compared, stated where a reader will see it."""
    match scenario:
        case _ if control:
            return 'existing statement against itself (harness control)'
        case SingleRowScenario(candidate=None) | BatchScenario(candidate=None):
            return 'existing statement against itself (harness control)'
        case SingleRowScenario() | BatchScenario():
            return 'existing statement against database-owned operation'
        case _ as unreachable:
            assert_never(unreachable)


def _contract_violations(
    *,
    scenario: Scenario,
    conditions: Conditions,
    baseline: Measurement,
    candidate: Measurement,
    plans: PlanEvidence | None,
    control: bool,
) -> tuple[str, ...]:
    """Exact and WAL budgets for a real before/after comparison.

    Latency is sampled and has confidence intervals. These facts are counted,
    so they fail directly: an interval cannot make an extra commit or WAL
    record less real.
    """
    if control or scenario.candidate is None:
        return ()

    before = baseline.counts
    after = candidate.counts
    expected_rows = conditions.observations_per_side * (
        scenario.batch_size if isinstance(scenario, BatchScenario) else 1
    )
    violations: list[str] = []

    if plans is None:
        violations.append('instrumented plan evidence is missing')

    if before.terminal_rows != expected_rows:
        violations.append(
            f'baseline terminal rows {before.terminal_rows} != {expected_rows}'
        )
    if after.terminal_rows != expected_rows:
        violations.append(
            f'candidate terminal rows {after.terminal_rows} != {expected_rows}'
        )
    if after.client_statements > before.client_statements:
        violations.append(
            f'client statements increased: {before.client_statements} -> '
            f'{after.client_statements}'
        )
    if after.write_transactions != before.write_transactions:
        violations.append(
            f'write transactions changed: {before.write_transactions} -> '
            f'{after.write_transactions}'
        )
    expected_transactions = (
        conditions.observations_per_side
        * scenario.exact_write_transactions_per_operation
    )
    if before.write_transactions != expected_transactions:
        violations.append(
            f'baseline write transactions {before.write_transactions} != '
            f'{expected_transactions}'
        )
    if after.write_transactions != expected_transactions:
        violations.append(
            f'candidate write transactions {after.write_transactions} != '
            f'{expected_transactions}'
        )

    if scenario.exact_client_statements_per_operation is not None:
        expected_statements = (
            conditions.observations_per_side
            * scenario.exact_client_statements_per_operation
        )
        if before.client_statements != expected_statements:
            violations.append(
                f'baseline client statements {before.client_statements} != '
                f'{expected_statements}'
            )
        if after.client_statements != expected_statements:
            violations.append(
                f'candidate client statements {after.client_statements} != '
                f'{expected_statements}'
            )

    wal_record_delta = after.wal_records_per_row - before.wal_records_per_row
    if wal_record_delta > WAL_RECORD_DELTA_PER_TERMINAL_ROW:
        violations.append(
            'WAL records per terminal row increased by '
            f'{wal_record_delta:.3f}; limit '
            f'{WAL_RECORD_DELTA_PER_TERMINAL_ROW:.3f}'
        )
    wal_byte_limit = max(before.wal_bytes_per_row * 0.10, 128.0)
    wal_byte_delta = after.wal_bytes_per_row - before.wal_bytes_per_row
    if wal_byte_delta > wal_byte_limit:
        violations.append(
            'WAL bytes per terminal row increased by '
            f'{wal_byte_delta:.0f}; limit {wal_byte_limit:.0f}'
        )

    return tuple(violations)


def server_setting(connection: Connection, name: str) -> str:
    return str(connection.execute(text(f'SHOW {name}')).scalar_one())
