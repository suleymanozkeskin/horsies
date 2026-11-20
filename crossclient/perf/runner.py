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

A public call scenario and a worker drain also read the CPU time of their
client and of PostgreSQL per block or drain (``resources.py``), and a drain
reads the telemetry columns of its worker endpoint (``metrics_scrape.py``).
With a telemetry level (``telemetry_levels.py``) the baseline side runs with
no telemetry and the candidate side with the level.
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

from crossclient.perf.counters import Counts, CounterProbe, install_extension
from crossclient.perf.metrics_scrape import (
    EndpointCounts,
    EndpointCountsUnread,
    EndpointReader,
    ReaderDidNotStop,
    ReadTally,
    read_endpoint_counts,
)
from crossclient.perf.readings import (
    BlockReading,
    BlockReadings,
    Cost,
    CostMeasure,
    DrainReading,
    DrainReadings,
    MeasuredCost,
    Readings,
    StatementReadings,
    UnmeasuredCost,
)
from crossclient.perf.resources import (
    CgroupCpuStat,
    CpuReading,
    CpuSeconds,
    NoPostgresCpuSource,
    NotMeasured,
    PostgresCpuMeter,
    PostgresCpuSource,
    WorkerCpu,
    WorkerCpuUnreadable,
    producer_cpu_now,
    read_worker_cpu,
)
from crossclient.perf.scenarios import (
    BatchScenario,
    Invocation,
    PublicCall,
    PublicCallScenario,
    Role,
    Scenario,
    SeedRows,
    SingleRowScenario,
    StatementScenario,
    WorkerDrainScenario,
    analyze,
    delete_seeded,
    seed_rows,
    seed_terminal_ballast,
)
from crossclient.perf.telemetry_levels import (
    AppTelemetry,
    ListenOn,
    NoEndpoint,
    NotRead,
    ProducerLevel,
    ReadEvery,
    TelemetryLevel,
    WorkerOnlyLevel,
    WorkerTelemetry,
    level_description,
    producer_telemetry,
    worker_telemetry,
)
from crossclient.perf.worker_drain import DrainSender
from crossclient.perf.worker_process import (
    Drained,
    DrainOutcome,
    DrainTimedOut,
    StartedWorker,
    WorkerExitedEarly,
    WorkerKilled,
    WorkerStartFailed,
    WorkerStop,
    WorkerStopped,
    remove_log,
    stop_worker,
    wait_for_drain,
    worker_state,
)
from crossclient.perf.statistics import (
    Comparison,
    ThroughputComparison,
    UnitCost,
    Verdict,
    compare,
    compare_cost,
    compare_throughput,
    worst,
)

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
    telemetry_level: str
    telemetry_description: str
    postgres_cpu_source: str


@dataclass(frozen=True, slots=True)
class Measurement:
    """One side's timings, what the server counted while they ran, and the
    CPU and telemetry readings of each unit."""

    samples_ms: list[float]
    counts: Counts
    readings: Readings


@dataclass(frozen=True, slots=True)
class PlanEvidence:
    """One instrumented execution per side, outside the latency samples."""

    baseline: dict[str, Any]
    candidate: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RunResult:
    scenario: Scenario
    conditions: Conditions
    baseline: Measurement
    candidate: Measurement
    plans: PlanEvidence | None
    comparisons: list[Comparison]
    throughput: ThroughputComparison | None
    costs: tuple[Cost, ...]
    contract_violations: tuple[str, ...]
    verdict: Verdict


def run_scenario(
    engine: Engine,
    *,
    dsn: str,
    scenario: Scenario,
    observations: int,
    block_size: int,
    resamples: int,
    seed: int,
    demo_quiesced: bool,
    telemetry: TelemetryLevel,
    postgres_cpu: PostgresCpuSource,
    control: bool = False,
) -> RunResult:
    """Measure both sides of one scenario and judge the difference.

    ``dsn`` is the database of ``engine``, in the form the library accepts:
    the client of a public call scenario connects with it. ``telemetry`` is
    the level of the candidate side of a public call or a drain.

    Raises:
        AssertionError: a statement scenario with a telemetry level, or a
            public call scenario with a level that changes only the worker:
            the entry point refuses both.
    """
    baseline_rows = seed_rows(scenario.name, Role.BASELINE)
    candidate_rows = seed_rows(scenario.name, Role.CANDIDATE)
    ballast_rows = seed_rows(scenario.name, Role.BALLAST)

    with engine.connect() as connection:
        install_extension(connection)
        server_version = server_setting(connection, 'server_version')
        fsync = server_setting(connection, 'fsync')
        full_page_writes = server_setting(connection, 'full_page_writes')
        synchronous_commit = server_setting(connection, 'synchronous_commit')
        autovacuum = server_setting(connection, 'autovacuum')

        for stale in (baseline_rows, candidate_rows):
            _cleanup(scenario, connection, stale)
        delete_seeded(connection, ballast_rows)
        seed_terminal_ballast(
            connection,
            rows=ballast_rows,
            count=BALLAST_ROWS,
            payload_bytes=200,
        )
        try:
            match scenario:
                case SingleRowScenario() | BatchScenario():
                    assert (
                        telemetry is TelemetryLevel.OFF
                    ), f'{scenario.name}: a statement scenario takes no telemetry level'
                    baseline, candidate, plans = _measure_interleaved(
                        connection,
                        scenario=scenario,
                        baseline_rows=baseline_rows,
                        candidate_rows=candidate_rows,
                        observations=observations,
                        block_size=block_size,
                        control=control,
                    )
                case PublicCallScenario():
                    baseline, candidate, plans = _measure_public_interleaved(
                        connection,
                        dsn=dsn,
                        scenario=scenario,
                        observations=observations,
                        block_size=block_size,
                        producer_app=_producer_app(telemetry),
                        postgres_cpu=postgres_cpu,
                    )
                case WorkerDrainScenario():
                    baseline, candidate, plans = _measure_drains_interleaved(
                        connection,
                        dsn=dsn,
                        scenario=scenario,
                        drains=observations,
                        telemetry=telemetry,
                        postgres_cpu=postgres_cpu,
                    )
                case _ as unreachable:
                    assert_never(unreachable)
        finally:
            # A statement failure leaves PostgreSQL's transaction aborted.
            # Roll it back before cleanup so the original error is not masked
            # by a second "current transaction is aborted" cleanup error.
            connection.rollback()
            for seeded in (baseline_rows, candidate_rows):
                _cleanup(scenario, connection, seeded)
            delete_seeded(connection, ballast_rows)

    match scenario:
        case SingleRowScenario() | PublicCallScenario():
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
            throughput = None
        case BatchScenario():
            comparisons = [
                compare(
                    baseline=baseline.samples_ms,
                    candidate=candidate.samples_ms,
                    percentile=95.0,
                    budget=scenario.lock_p95_budget,
                    resamples=resamples,
                    seed=seed,
                )
            ]
            throughput = compare_throughput(
                baseline_ms=baseline.samples_ms,
                candidate_ms=candidate.samples_ms,
                rows_per_operation=scenario.batch_size,
                minimum_ratio=scenario.minimum_throughput_ratio,
                resamples=resamples,
                seed=seed,
            )
        case WorkerDrainScenario():
            comparisons = []
            throughput = compare_throughput(
                baseline_ms=baseline.samples_ms,
                candidate_ms=candidate.samples_ms,
                rows_per_operation=scenario.task_count,
                minimum_ratio=scenario.minimum_throughput_ratio,
                resamples=resamples,
                seed=seed,
            )
        case _ as unreachable:
            assert_never(unreachable)

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
        comparison=_comparison_label(scenario, control=control, telemetry=telemetry),
        demo_quiesced=demo_quiesced,
        measured_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
        telemetry_level=telemetry.value,
        telemetry_description=level_description(telemetry),
        postgres_cpu_source=_postgres_cpu_label(postgres_cpu),
    )
    contract_violations = _contract_violations(
        scenario=scenario,
        conditions=conditions,
        baseline=baseline,
        candidate=candidate,
        plans=plans,
        control=control,
    )
    performance_verdict = worst(
        [
            *(comparison.verdict for comparison in comparisons),
            *([throughput.verdict] if throughput is not None else []),
        ]
    )

    return RunResult(
        scenario=scenario,
        conditions=conditions,
        baseline=baseline,
        candidate=candidate,
        plans=plans,
        comparisons=comparisons,
        throughput=throughput,
        costs=compare_costs(
            baseline.readings,
            candidate.readings,
            resamples=resamples,
            seed=seed,
        ),
        contract_violations=contract_violations,
        verdict=Verdict.FAIL if contract_violations else performance_verdict,
    )


def _measure_interleaved(
    connection: Connection,
    *,
    scenario: StatementScenario,
    baseline_rows: SeedRows,
    candidate_rows: SeedRows,
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
    rows = {Side.BASELINE: baseline_rows, Side.CANDIDATE: candidate_rows}

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
            for side, side_rows in rows.items():
                _seed_for(scenario, connection, rows=side_rows, count=per_side_rows)
                ids[side] = side_rows.tasks.ids(per_side_rows)
            analyze(connection)
            plans = _capture_plans(
                connection,
                scenario,
                baseline_warmup_id=baseline_rows.tasks.id_at(1),
                candidate_warmup_id=candidate_rows.tasks.id_at(1),
                baseline_task_id=baseline_rows.tasks.id_at(2),
                candidate_task_id=candidate_rows.tasks.id_at(2),
                control=control,
            )
        case BatchScenario():
            for side in rows:
                ids[side] = iter(())
            plans = _capture_batch_plans(
                connection,
                scenario,
                baseline_rows=baseline_rows,
                candidate_rows=candidate_rows,
                control=control,
            )
        case _ as unreachable:
            assert_never(unreachable)

    samples: dict[Side, list[float]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    counts: dict[Side, list[Counts]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    probe = CounterProbe(connection)

    for side in (Side.BASELINE, Side.CANDIDATE):
        _seed_block(scenario, connection, rows=rows[side], block_size=block_size)
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
                rows=rows[side],
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
            readings=StatementReadings(),
        ),
        Measurement(
            samples_ms=samples[Side.CANDIDATE],
            counts=_total(counts[Side.CANDIDATE]),
            readings=StatementReadings(),
        ),
        plans,
    )


def _measure_public_interleaved(
    connection: Connection,
    *,
    dsn: str,
    scenario: PublicCallScenario,
    observations: int,
    block_size: int,
    producer_app: AppTelemetry,
    postgres_cpu: PostgresCpuSource,
) -> tuple[Measurement, Measurement, None]:
    """Alternate blocks of the public call between the two sides until both
    are complete; the first block of each side is a warmup.

    Each side has its own client: the baseline with no telemetry, the
    candidate with ``producer_app``. The pool and the first connection of
    each client are paid in its warmup block. The rows the calls write stay
    until the cleanup of the run. No plan is captured: the statements are
    the library's. Each block reads the CPU time of the harness process and
    of PostgreSQL across it.
    """
    blocks = -(-observations // block_size)
    samples: dict[Side, list[float]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    counts: dict[Side, list[Counts]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    readings: dict[Side, list[BlockReading]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    analyze(connection)
    probe = CounterProbe(connection)
    postgres = PostgresCpuMeter(postgres_cpu)
    clients = {
        Side.BASELINE: scenario.open_call(dsn, AppTelemetry.NONE),
        Side.CANDIDATE: scenario.open_call(dsn, producer_app),
    }
    try:
        for warmup_side in (Side.BASELINE, Side.CANDIDATE):
            _ = _run_public_block(clients[warmup_side], block_size)
        for _ in range(blocks):
            for side in (Side.BASELINE, Side.CANDIDATE):
                probe.begin()
                # The probe's reads assign no transaction id. Ending their
                # transaction leaves the harness session idle while the
                # library writes on its own connections.
                connection.commit()
                postgres.begin()
                producer_started = producer_cpu_now()
                samples[side] += _run_public_block(clients[side], block_size)
                producer_s = producer_cpu_now() - producer_started
                postgres_used = postgres.finish()
                counts[side].append(probe.finish(terminal_rows=0))
                connection.commit()
                readings[side].append(
                    BlockReading(
                        calls=block_size,
                        producer_s=producer_s,
                        postgres=postgres_used,
                    )
                )
    finally:
        for client in clients.values():
            client.close()
    return (
        Measurement(
            samples_ms=samples[Side.BASELINE],
            counts=_total(counts[Side.BASELINE]),
            readings=BlockReadings(blocks=tuple(readings[Side.BASELINE])),
        ),
        Measurement(
            samples_ms=samples[Side.CANDIDATE],
            counts=_total(counts[Side.CANDIDATE]),
            readings=BlockReadings(blocks=tuple(readings[Side.CANDIDATE])),
        ),
        None,
    )


def _run_public_block(client: PublicCall, block_size: int) -> list[float]:
    """One block of public calls, each timed on its own. A call moves no task
    row to history."""
    samples: list[float] = []
    for _ in range(block_size):
        started = time.perf_counter_ns()
        client.call()
        samples.append((time.perf_counter_ns() - started) / 1_000_000)
    return samples


@dataclass(frozen=True, slots=True)
class _DrainSide:
    """The sender and the worker of one side of a drain run."""

    sender: DrainSender
    telemetry: WorkerTelemetry


def _measure_drains_interleaved(
    connection: Connection,
    *,
    dsn: str,
    scenario: WorkerDrainScenario,
    drains: int,
    telemetry: TelemetryLevel,
    postgres_cpu: PostgresCpuSource,
) -> tuple[Measurement, Measurement, None]:
    """Alternate drains between the two sides until each side has ``drains``;
    the first drain of each side is a warmup.

    The baseline side sends and drains with no telemetry; the candidate side
    with ``telemetry``. Each drain starts a new worker, so each sample pays
    the start of a worker process. No plan is captured: the statements are
    the worker's.
    """
    samples: dict[Side, list[float]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    counts: dict[Side, list[Counts]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    readings: dict[Side, list[DrainReading]] = {Side.BASELINE: [], Side.CANDIDATE: []}
    analyze(connection)
    probe = CounterProbe(connection)
    postgres = PostgresCpuMeter(postgres_cpu)
    workers = {
        Side.BASELINE: worker_telemetry(TelemetryLevel.OFF),
        Side.CANDIDATE: worker_telemetry(telemetry),
    }
    sides: dict[Side, _DrainSide] = {}
    try:
        for side, worker in workers.items():
            sides[side] = _DrainSide(
                sender=scenario.open_sender(dsn, worker.app),
                telemetry=worker,
            )
        for warmup_side in (Side.BASELINE, Side.CANDIDATE):
            _ = _drain_once(
                connection,
                dsn=dsn,
                scenario=scenario,
                side=sides[warmup_side],
                probe=probe,
                postgres=postgres,
            )
        for _ in range(drains):
            for side in (Side.BASELINE, Side.CANDIDATE):
                elapsed_ms, drain_counts, reading = _drain_once(
                    connection,
                    dsn=dsn,
                    scenario=scenario,
                    side=sides[side],
                    probe=probe,
                    postgres=postgres,
                )
                samples[side].append(elapsed_ms)
                counts[side].append(drain_counts)
                readings[side].append(reading)
    finally:
        for drain_side in sides.values():
            drain_side.sender.close()
    return (
        Measurement(
            samples_ms=samples[Side.BASELINE],
            counts=_total(counts[Side.BASELINE]),
            readings=DrainReadings(drains=tuple(readings[Side.BASELINE])),
        ),
        Measurement(
            samples_ms=samples[Side.CANDIDATE],
            counts=_total(counts[Side.CANDIDATE]),
            readings=DrainReadings(drains=tuple(readings[Side.CANDIDATE])),
        ),
        None,
    )


def _drain_once(
    connection: Connection,
    *,
    dsn: str,
    scenario: WorkerDrainScenario,
    side: _DrainSide,
    probe: CounterProbe,
    postgres: PostgresCpuMeter,
) -> tuple[float, Counts, DrainReading]:
    """One drain: delete the tasks of the last drain, send the tasks, start a
    worker, time it until every task is terminal, read its CPU time and its
    endpoint, then stop it.

    The probe and the PostgreSQL CPU reading bracket the worker from its
    start until the drain. Gives the time in milliseconds, the server counts
    and the readings.

    Raises:
        RuntimeError: the worker did not start, did not drain the tasks in
            ``scenario.timeout``, exited before it drained them, did not exit
            0 after SIGTERM, or a reading failed (its CPU time, the
            PostgreSQL CPU time, the endpoint). The message names the log of
            the worker, which stays in the temporary directory.
    """
    scenario.cleanup(connection)
    side.sender.send(scenario.task_count)
    probe.begin()
    # The probe's reads assign no transaction id. Ending their transaction
    # leaves the harness session idle while the worker writes.
    connection.commit()
    postgres.begin()
    match scenario.start_worker(dsn, side.telemetry):
        case WorkerStartFailed(command=command, cause=cause):
            raise RuntimeError(f'the worker did not start: {cause}; command: {command}')
        case StartedWorker() as worker:
            pass
    reader = _start_reader(side.telemetry, scenario)
    try:
        match _wait_for_drain(connection, scenario=scenario, worker=worker):
            case Drained(elapsed_ns=elapsed_ns):
                worker_cpu = _required_worker_cpu(worker, scenario)
                postgres_used = postgres.finish()
                drain_counts = probe.finish(terminal_rows=scenario.task_count)
                connection.commit()
                endpoint = _required_endpoint_counts(side.telemetry, worker, scenario)
                reads = _required_tally(_stop_reader(reader), worker, scenario)
            case DrainTimedOut() | WorkerExitedEarly() as failed:
                raise RuntimeError(
                    f'{scenario.name}: {failed!r}; worker log: {worker.log_path}'
                )
    except BaseException:
        # Explicit discards: the error that stopped the drain is the report;
        # the stops only free the database, the port and the host.
        _ = _stop_reader(reader)
        _ = stop_worker(worker.process, scenario.stop_timeout)
        raise
    stop_started_ns = time.perf_counter_ns()
    stop = stop_worker(worker.process, scenario.stop_timeout)
    stop_s = (time.perf_counter_ns() - stop_started_ns) / 1_000_000_000
    _require_clean_stop(stop, scenario=scenario, worker=worker)
    remove_log(worker)
    return (
        elapsed_ns / 1_000_000,
        drain_counts,
        DrainReading(
            tasks=scenario.task_count,
            worker=worker_cpu,
            postgres=postgres_used,
            stop_s=stop_s,
            endpoint=endpoint,
            reads=reads,
        ),
    )


def _start_reader(
    telemetry: WorkerTelemetry,
    scenario: WorkerDrainScenario,
) -> EndpointReader | NotRead:
    """Start the reads of the endpoint during the drain, at a level that has
    them. At most one read per interval until the drain timeout."""
    match telemetry.endpoint, telemetry.scrape:
        case ListenOn() as listen, ReadEvery(interval=interval):
            reader = EndpointReader(
                listen,
                interval,
                max_rounds=scenario.timeout // interval + 1,
            )
            reader.start()
            return reader
        case ((ListenOn() | NoEndpoint()), NotRead()):
            return NotRead()
        case NoEndpoint(), ReadEvery():
            raise AssertionError(
                f'the level {telemetry.level.value} reads an endpoint it does not serve'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _stop_reader(
    reader: EndpointReader | NotRead,
) -> ReadTally | ReaderDidNotStop | NotRead:
    match reader:
        case EndpointReader():
            return reader.stop()
        case NotRead():
            return reader
        case _ as unreachable:
            assert_never(unreachable)


def _required_tally(
    stopped: ReadTally | ReaderDidNotStop | NotRead,
    worker: StartedWorker,
    scenario: WorkerDrainScenario,
) -> ReadTally | NotRead:
    """The tally of the reads, or the run stops.

    Raises:
        RuntimeError: the reader thread did not stop.
    """
    match stopped:
        case ReadTally() | NotRead():
            return stopped
        case ReaderDidNotStop(listen=listen):
            raise RuntimeError(
                f'{scenario.name}: the reader of {listen.argument()} did not stop; '
                f'worker log: {worker.log_path}'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _required_worker_cpu(
    worker: StartedWorker, scenario: WorkerDrainScenario
) -> WorkerCpu:
    """The CPU time of the drained worker, or the run stops.

    Raises:
        RuntimeError: the CPU time could not be read.
    """
    match read_worker_cpu(worker.process.pid):
        case WorkerCpu() as cpu:
            return cpu
        case WorkerCpuUnreadable(pid=pid, cause=cause):
            raise RuntimeError(
                f'{scenario.name}: the CPU time of worker {pid} is not readable: '
                f'{cause}; worker log: {worker.log_path}'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _required_endpoint_counts(
    telemetry: WorkerTelemetry,
    worker: StartedWorker,
    scenario: WorkerDrainScenario,
) -> EndpointCounts | NoEndpoint:
    """The telemetry columns of the worker endpoint at the end of the drain,
    at a level with an endpoint, or the run stops.

    Raises:
        RuntimeError: the read failed.
    """
    match telemetry.endpoint:
        case NoEndpoint():
            return NoEndpoint()
        case ListenOn() as listen:
            match read_endpoint_counts(listen):
                case EndpointCounts() as endpoint_counts:
                    return endpoint_counts
                case EndpointCountsUnread(cause=cause):
                    raise RuntimeError(
                        f'{scenario.name}: the endpoint {listen.argument()} of the '
                        f'drained worker: {cause}; worker log: {worker.log_path}'
                    )
                case _ as unreachable:
                    assert_never(unreachable)
        case _ as unreachable:
            assert_never(unreachable)


def _wait_for_drain(
    connection: Connection,
    *,
    scenario: WorkerDrainScenario,
    worker: StartedWorker,
) -> DrainOutcome:
    return wait_for_drain(
        count_terminal=lambda: scenario.count_terminal(connection),
        state_of_worker=lambda: worker_state(worker.process),
        expected=scenario.task_count,
        started_ns=worker.started_ns,
        timeout=scenario.timeout,
        interval=scenario.count_interval,
        clock_ns=time.perf_counter_ns,
        sleep=time.sleep,
    )


def _require_clean_stop(
    stop: WorkerStop,
    *,
    scenario: WorkerDrainScenario,
    worker: StartedWorker,
) -> None:
    """A drained worker exits 0 after SIGTERM (the stop contract of
    ``horsies worker``).

    Raises:
        RuntimeError: it exited with another code, or was killed.
    """
    match stop:
        case WorkerStopped(exit_code=0):
            return
        case WorkerStopped() | WorkerKilled():
            raise RuntimeError(
                f'{scenario.name}: the drained worker did not stop clean: '
                f'{stop!r}; worker log: {worker.log_path}'
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


def _capture_batch_plans(
    connection: Connection,
    scenario: BatchScenario,
    *,
    baseline_rows: SeedRows,
    candidate_rows: SeedRows,
    control: bool,
) -> PlanEvidence | None:
    """Capture each discovery plan against its own isolated eligible pool."""
    candidate_factory = (
        scenario.baseline_invocation if control else scenario.candidate_invocation
    )
    if candidate_factory is None:
        return None

    scenario.cleanup(connection, baseline_rows)
    scenario.cleanup(connection, candidate_rows)
    scenario.seed(connection, baseline_rows, scenario.batch_size)
    analyze(connection)
    baseline = _explain(connection, scenario.baseline_invocation())
    scenario.cleanup(connection, baseline_rows)

    scenario.seed(connection, candidate_rows, scenario.batch_size)
    analyze(connection)
    candidate = _explain(connection, candidate_factory())
    scenario.cleanup(connection, candidate_rows)
    return PlanEvidence(baseline=baseline, candidate=candidate)


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
    scenario: StatementScenario,
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
    scenario: StatementScenario,
    connection: Connection,
    *,
    rows: SeedRows,
    block_size: int,
) -> None:
    """Rows a batch block will consume, seeded immediately before it runs."""
    match scenario:
        case SingleRowScenario():
            return
        case BatchScenario(batch_size=batch_size):
            # Clear first: a batch operation moves its rows to history, where
            # last block's ids are still present, and a second move of one id
            # is refused by the duplicate identity guard. Every block
            # therefore starts from a pool holding exactly what it is meant
            # to consume.
            scenario.cleanup(connection, rows)
            scenario.seed(connection, rows, block_size * batch_size)
            analyze(connection)
        case _ as unreachable:
            assert_never(unreachable)


def _seed_for(
    scenario: StatementScenario,
    connection: Connection,
    *,
    rows: SeedRows,
    count: int,
) -> None:
    scenario.seed(connection, rows, count)


def _cleanup(scenario: Scenario, connection: Connection, rows: SeedRows) -> None:
    """Delete the rows that ``scenario`` wrote for one role, and commit."""
    match scenario:
        case SingleRowScenario() | BatchScenario():
            scenario.cleanup(connection, rows)
        case PublicCallScenario() | WorkerDrainScenario():
            # The calls and the sends write rows with ids that the library
            # mints; the cleanup finds them by the names the scenario owns,
            # so it has no role.
            scenario.cleanup(connection)
        case _ as unreachable:
            assert_never(unreachable)


def _payload_bytes(scenario: Scenario) -> int:
    match scenario:
        case SingleRowScenario(payload_bytes=payload_bytes):
            return payload_bytes
        case PublicCallScenario(payload_bytes=payload_bytes):
            return payload_bytes
        case BatchScenario():
            return 200
        case WorkerDrainScenario():
            # The no-op task takes no argument.
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _batch_size(scenario: Scenario) -> int | None:
    match scenario:
        case SingleRowScenario() | PublicCallScenario():
            return None
        case BatchScenario(batch_size=batch_size):
            return batch_size
        case WorkerDrainScenario(task_count=task_count):
            return task_count
        case _ as unreachable:
            assert_never(unreachable)


def _comparison_label(
    scenario: Scenario,
    *,
    control: bool,
    telemetry: TelemetryLevel,
) -> str:
    """What the run actually compared, stated where a reader will see it."""
    match scenario:
        case PublicCallScenario() if telemetry is not TelemetryLevel.OFF:
            return f'public call, telemetry off against telemetry {telemetry.value}'
        case WorkerDrainScenario() if telemetry is not TelemetryLevel.OFF:
            return f'worker drain, telemetry off against telemetry {telemetry.value}'
        case PublicCallScenario():
            return 'public call against itself (harness control)'
        case WorkerDrainScenario():
            return 'worker drain against itself (harness control)'
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
    record less real. A public call scenario and a worker drain have one
    implementation, which both sides run, so they have no contract to break.
    """
    match scenario:
        case PublicCallScenario() | WorkerDrainScenario():
            return ()
        case SingleRowScenario() | BatchScenario():
            pass
        case _ as unreachable:
            assert_never(unreachable)
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


def _producer_app(telemetry: TelemetryLevel) -> AppTelemetry:
    """The app of the candidate client of a public call scenario.

    Raises:
        AssertionError: a level that changes only the worker; the entry point
            refuses it for a public call scenario.
    """
    match producer_telemetry(telemetry):
        case ProducerLevel(app=app):
            return app
        case WorkerOnlyLevel(level=level):
            raise AssertionError(
                f'a public call scenario takes no level that changes only the '
                f'worker: {level.value}'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _postgres_cpu_label(source: PostgresCpuSource) -> str:
    """Where the PostgreSQL CPU time comes from, for the conditions."""
    match source:
        case CgroupCpuStat(path=path):
            return f'`usage_usec` of {path}'
        case NoPostgresCpuSource():
            return 'not measured (no --postgres-cpu-stat)'
        case _ as unreachable:
            assert_never(unreachable)


def compare_costs(
    baseline: Readings,
    candidate: Readings,
    *,
    resamples: int,
    seed: int,
) -> tuple[Cost, ...]:
    """The cost comparisons of the readings of the two sides. A statement
    scenario has none.

    Raises:
        AssertionError: the two sides hold readings of different kinds: a
            defect of the runner.
    """
    match baseline, candidate:
        case StatementReadings(), StatementReadings():
            return ()
        case BlockReadings(blocks=baseline_blocks), BlockReadings(
            blocks=candidate_blocks
        ):
            return (
                _measured(
                    CostMeasure.PRODUCER_CPU,
                    [
                        UnitCost(block.calls, block.producer_s)
                        for block in baseline_blocks
                    ],
                    [
                        UnitCost(block.calls, block.producer_s)
                        for block in candidate_blocks
                    ],
                    resamples=resamples,
                    seed=seed,
                ),
                _postgres_cost(
                    CostMeasure.POSTGRES_CPU_PER_CALL,
                    [(block.calls, block.postgres) for block in baseline_blocks],
                    [(block.calls, block.postgres) for block in candidate_blocks],
                    resamples=resamples,
                    seed=seed,
                ),
            )
        case DrainReadings(drains=baseline_drains), DrainReadings(
            drains=candidate_drains
        ):
            return (
                _measured(
                    CostMeasure.WORKER_CPU,
                    [
                        UnitCost(drain.tasks, drain.worker.total_s)
                        for drain in baseline_drains
                    ],
                    [
                        UnitCost(drain.tasks, drain.worker.total_s)
                        for drain in candidate_drains
                    ],
                    resamples=resamples,
                    seed=seed,
                ),
                _measured(
                    CostMeasure.WORKER_PARENT_CPU,
                    [
                        UnitCost(drain.tasks, drain.worker.parent_s)
                        for drain in baseline_drains
                    ],
                    [
                        UnitCost(drain.tasks, drain.worker.parent_s)
                        for drain in candidate_drains
                    ],
                    resamples=resamples,
                    seed=seed,
                ),
                _measured(
                    CostMeasure.WORKER_CHILDREN_CPU,
                    [
                        UnitCost(drain.tasks, drain.worker.children_s)
                        for drain in baseline_drains
                    ],
                    [
                        UnitCost(drain.tasks, drain.worker.children_s)
                        for drain in candidate_drains
                    ],
                    resamples=resamples,
                    seed=seed,
                ),
                _postgres_cost(
                    CostMeasure.POSTGRES_CPU_PER_TASK,
                    [(drain.tasks, drain.postgres) for drain in baseline_drains],
                    [(drain.tasks, drain.postgres) for drain in candidate_drains],
                    resamples=resamples,
                    seed=seed,
                ),
                _measured(
                    CostMeasure.WORKER_STOP,
                    [UnitCost(1, drain.stop_s) for drain in baseline_drains],
                    [UnitCost(1, drain.stop_s) for drain in candidate_drains],
                    resamples=resamples,
                    seed=seed,
                ),
            )
        case _:
            raise AssertionError(
                f'the sides hold readings of different kinds: '
                f'{type(baseline).__name__} and {type(candidate).__name__}'
            )


def _measured(
    measure: CostMeasure,
    baseline: list[UnitCost],
    candidate: list[UnitCost],
    *,
    resamples: int,
    seed: int,
) -> MeasuredCost:
    return MeasuredCost(
        measure=measure,
        comparison=compare_cost(
            baseline=baseline,
            candidate=candidate,
            resamples=resamples,
            seed=seed,
        ),
    )


def _postgres_cost(
    measure: CostMeasure,
    baseline: list[tuple[int, CpuReading]],
    candidate: list[tuple[int, CpuReading]],
    *,
    resamples: int,
    seed: int,
) -> Cost:
    """The PostgreSQL cost, when every unit of both sides measured it."""
    baseline_units = _measured_units(baseline)
    candidate_units = _measured_units(candidate)
    match baseline_units, candidate_units:
        case list(), list():
            return _measured(
                measure,
                baseline_units,
                candidate_units,
                resamples=resamples,
                seed=seed,
            )
        case NotMeasured(reason=reason), _:
            return UnmeasuredCost(measure=measure, reason=reason)
        case _, NotMeasured(reason=reason):
            return UnmeasuredCost(measure=measure, reason=reason)
        case _ as unreachable:
            assert_never(unreachable)


def _measured_units(
    units: list[tuple[int, CpuReading]],
) -> list[UnitCost] | NotMeasured:
    """The costs of ``units``, or the first reason that one was not measured."""
    measured: list[UnitCost] = []
    for operations, reading in units:
        match reading:
            case CpuSeconds(seconds=seconds):
                measured.append(UnitCost(operations, seconds))
            case NotMeasured():
                return reading
            case _ as unreachable:
                assert_never(unreachable)
    return measured


def server_setting(connection: Connection, name: str) -> str:
    return str(connection.execute(text(f'SHOW {name}')).scalar_one())
