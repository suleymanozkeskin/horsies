# pyright: reportPrivateUsage=false
"""A run with a telemetry level: what the entry point refuses, how the costs
of the two sides compare, and what the summary states."""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from pathlib import Path
from typing import Final, NoReturn

import pytest

from crossclient.perf import __main__ as perf_main
from crossclient.perf.__main__ import _control_disagreements, _telemetry_refusals
from crossclient.perf.counters import Counts
from crossclient.perf.metrics_scrape import EndpointCounts, ReadTally
from crossclient.perf.readings import (
    BlockReading,
    BlockReadings,
    CostMeasure,
    DrainReading,
    DrainReadings,
    MeasuredCost,
    StatementReadings,
    UnmeasuredCost,
)
from crossclient.perf.report import render_summary, summary_filename
from crossclient.perf.resources import CpuSeconds, NotMeasured, WorkerCpu
from crossclient.perf.runner import (
    Conditions,
    Measurement,
    RunResult,
    _comparison_label,
    compare_costs,
)
from crossclient.perf.scenarios import PublicCallScenario, WorkerDrainScenario
from crossclient.perf.statistics import Budget, ThroughputComparison, Verdict
from crossclient.perf.telemetry_levels import (
    NoEndpoint,
    NotRead,
    TelemetryLevel,
    level_description,
)

pytestmark = [pytest.mark.unit]

_UNUSED_DSN: Final = 'postgresql://perf@127.0.0.1:1/unused'
"""Port 1 has no server: a refusal comes before any connection."""


def _never_run(*_args: object) -> NoReturn:
    raise AssertionError('these tests run no scenario')


_PUBLIC_CALL: Final = PublicCallScenario(
    name='telemetry-public-call',
    description='a public call scenario built for these tests',
    p50_budget=Budget(fraction=0.10, floor_ms=0.5),
    p99_budget=Budget(fraction=0.15, floor_ms=1.5),
    payload_bytes=200,
    open_call=_never_run,
    cleanup=_never_run,
)

_DRAIN: Final = WorkerDrainScenario(
    name='telemetry-drain',
    description='a worker drain scenario built for these tests',
    minimum_throughput_ratio=0.90,
    task_count=10_000,
    timeout=timedelta(minutes=10),
    count_interval=timedelta(milliseconds=100),
    stop_timeout=timedelta(seconds=60),
    open_sender=_never_run,
    start_worker=_never_run,
    count_terminal=_never_run,
    cleanup=_never_run,
)

_COUNTS: Final = Counts(
    client_statements=200,
    nested_statements=0,
    client_rows=200,
    nested_rows=0,
    terminal_rows=0,
    wal_records=1_000,
    wal_bytes=100_000,
    wal_fpi=0,
    write_transactions=200,
)

_ENDPOINT: Final = EndpointCounts(
    series=180,
    events_dropped_queue_full=0,
    events_dropped_child_down=2,
    series_dropped=0,
    handler_dropped=0,
    handler_errors=0,
)

_TALLY: Final = ReadTally(
    metrics_answered=6,
    metrics_not_answered=1,
    livez_live=6,
    livez_late=0,
    livez_not_answered=1,
)


def _drain(
    worker_s: float,
    postgres: CpuSeconds | NotMeasured,
    endpoint: EndpointCounts | NoEndpoint,
    reads: ReadTally | NotRead,
) -> DrainReading:
    return DrainReading(
        tasks=10_000,
        worker=WorkerCpu(
            parent_s=worker_s / 2,
            children_s=worker_s / 2,
            children_pids=(101, 102, 103, 104, 105),
        ),
        postgres=postgres,
        stop_s=1.0,
        endpoint=endpoint,
        reads=reads,
    )


_OFF_DRAINS: Final = DrainReadings(
    drains=tuple(
        _drain(10.0 + index * 0.1, CpuSeconds(12.0), NoEndpoint(), NotRead())
        for index in range(4)
    )
)
_HANDLER_DRAINS: Final = DrainReadings(
    drains=tuple(
        _drain(11.0 + index * 0.1, CpuSeconds(12.0), _ENDPOINT, _TALLY)
        for index in range(4)
    )
)


class TestEntryPointRefusals:
    def test_a_level_with_control_is_refused(self) -> None:
        with pytest.raises(SystemExit) as raised:
            perf_main.main(
                [
                    '--dsn',
                    _UNUSED_DSN,
                    '--scenario',
                    'claim-finish',
                    '--scale',
                    'smoke',
                    '--control',
                    '--telemetry',
                    'tracing',
                ]
            )
        assert raised.value.code == 2

    @pytest.mark.parametrize('scenario', ['send', 'workflow-start', 'all'])
    def test_a_worker_level_with_a_public_call_is_refused(self, scenario: str) -> None:
        with pytest.raises(SystemExit) as raised:
            perf_main.main(
                [
                    '--dsn',
                    _UNUSED_DSN,
                    '--scenario',
                    scenario,
                    '--scale',
                    'smoke',
                    '--telemetry',
                    'handler',
                ]
            )
        assert raised.value.code == 2

    def test_an_unreadable_cpu_stat_file_is_refused_before_the_run(
        self,
        tmp_path: Path,
    ) -> None:
        with pytest.raises(SystemExit) as raised:
            perf_main.main(
                [
                    '--dsn',
                    _UNUSED_DSN,
                    '--scenario',
                    'claim-finish',
                    '--scale',
                    'smoke',
                    '--postgres-cpu-stat',
                    str(tmp_path / 'cpu.stat'),
                ]
            )
        assert raised.value.code == 2

    def test_a_public_call_takes_tracing(self) -> None:
        assert _telemetry_refusals((_PUBLIC_CALL,), TelemetryLevel.TRACING) == []

    @pytest.mark.parametrize(
        'level', list(TelemetryLevel), ids=lambda level: level.value
    )
    def test_a_drain_takes_every_level(self, level: TelemetryLevel) -> None:
        assert _telemetry_refusals((_DRAIN,), level) == []

    def test_a_worker_level_names_the_public_call_and_the_levels_it_takes(self) -> None:
        (refusal,) = _telemetry_refusals((_PUBLIC_CALL,), TelemetryLevel.METRICS)
        assert _PUBLIC_CALL.name in refusal
        assert 'off or tracing' in refusal


class TestComparisonLabel:
    def test_a_level_names_both_sides(self) -> None:
        assert (
            _comparison_label(_DRAIN, control=False, telemetry=TelemetryLevel.HANDLER)
            == 'worker drain, telemetry off against telemetry handler'
        )

    def test_off_stays_a_control(self) -> None:
        assert 'control' in _comparison_label(
            _DRAIN, control=False, telemetry=TelemetryLevel.OFF
        )


def _result(
    scenario: PublicCallScenario | WorkerDrainScenario,
    level: TelemetryLevel,
    baseline: Measurement,
    candidate: Measurement,
) -> RunResult:
    return RunResult(
        scenario=scenario,
        conditions=Conditions(
            scenario=scenario.name,
            description=scenario.description,
            server_version='16.4',
            fsync='off',
            full_page_writes='off',
            synchronous_commit='off',
            autovacuum='off',
            observations_per_side=4,
            block_size=1,
            ballast_rows=100_000,
            payload_bytes=0,
            batch_size=10_000,
            resamples=200,
            seed=20260804,
            comparison=_comparison_label(scenario, control=False, telemetry=level),
            demo_quiesced=True,
            measured_at='2026-11-20T14:03:05+00:00',
            telemetry_level=level.value,
            telemetry_description=level_description(level),
            postgres_cpu_source='`usage_usec` of /sys/fs/cgroup/pg/cpu.stat',
        ),
        baseline=baseline,
        candidate=candidate,
        plans=None,
        comparisons=[],
        throughput=ThroughputComparison(
            baseline_rows_per_second=2_000.0,
            candidate_rows_per_second=1_950.0,
            ratio=0.975,
            ci_low_ratio=0.95,
            ci_high_ratio=1.0,
            minimum_ratio=0.90,
            verdict=Verdict.PASS,
        ),
        costs=compare_costs(
            baseline.readings,
            candidate.readings,
            resamples=200,
            seed=20260804,
        ),
        contract_violations=(),
        verdict=Verdict.PASS,
    )


class TestCosts:
    def test_a_drain_compares_each_cost(self) -> None:
        costs = compare_costs(_OFF_DRAINS, _HANDLER_DRAINS, resamples=50, seed=1)
        assert [cost.measure for cost in costs] == [
            CostMeasure.WORKER_CPU,
            CostMeasure.WORKER_PARENT_CPU,
            CostMeasure.WORKER_CHILDREN_CPU,
            CostMeasure.POSTGRES_CPU_PER_TASK,
            CostMeasure.WORKER_STOP,
        ]
        match costs[0]:
            case MeasuredCost(comparison=comparison):
                assert comparison.baseline_per_operation_s == pytest.approx(
                    10.15 / 10_000
                )
                assert comparison.candidate_per_operation_s == pytest.approx(
                    11.15 / 10_000
                )
            case UnmeasuredCost():
                pytest.fail('worker CPU is always measured')

    def test_postgres_not_measured_on_a_side_is_named(self) -> None:
        unmeasured = DrainReadings(
            drains=tuple(
                dataclasses.replace(drain, postgres=NotMeasured(reason='no source'))
                for drain in _OFF_DRAINS.drains
            )
        )
        costs = compare_costs(unmeasured, _HANDLER_DRAINS, resamples=50, seed=1)
        assert costs[3] == UnmeasuredCost(
            measure=CostMeasure.POSTGRES_CPU_PER_TASK,
            reason='no source',
        )

    def test_a_public_call_compares_the_producer_and_postgres(self) -> None:
        blocks = BlockReadings(
            blocks=tuple(
                BlockReading(calls=100, producer_s=0.05, postgres=CpuSeconds(0.02))
                for _ in range(3)
            )
        )
        costs = compare_costs(blocks, blocks, resamples=50, seed=1)
        assert [cost.measure for cost in costs] == [
            CostMeasure.PRODUCER_CPU,
            CostMeasure.POSTGRES_CPU_PER_CALL,
        ]

    def test_a_statement_scenario_has_no_cost(self) -> None:
        assert (
            compare_costs(StatementReadings(), StatementReadings(), resamples=1, seed=1)
            == ()
        )

    def test_readings_of_two_kinds_are_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='different kinds'):
            _ = compare_costs(StatementReadings(), _OFF_DRAINS, resamples=1, seed=1)


class TestSummary:
    def _drain_result(self) -> RunResult:
        return _result(
            _DRAIN,
            TelemetryLevel.HANDLER,
            Measurement(samples_ms=[5_000.0] * 4, counts=_COUNTS, readings=_OFF_DRAINS),
            Measurement(
                samples_ms=[5_100.0] * 4, counts=_COUNTS, readings=_HANDLER_DRAINS
            ),
        )

    def test_the_summary_states_the_level_the_costs_and_the_telemetry(self) -> None:
        summary = render_summary(self._drain_result())
        assert '| telemetry of the candidate side | `handler`: ' in summary
        assert '## Resources' in summary
        assert (
            '| worker CPU per task (parent and children) | 1.0150 ms | 1.1150 ms |'
            in summary
        )
        assert '## Telemetry' in summary
        assert '| series (largest) | no endpoint | 180 |' in summary
        assert '| events dropped, handler child down | no endpoint | 8 |' in summary
        assert '| `/livez` late (503) | not read | 0 |' in summary

    def test_each_run_of_a_cell_has_its_own_file(self) -> None:
        assert (
            summary_filename(self._drain_result())
            == '2026-11-20-140305-telemetry-drain-telemetry-handler-pg16.md'
        )


class TestPublicCallCounts:
    def test_tracing_is_held_to_the_statements_of_no_telemetry(self) -> None:
        """Tracing stores the carrier in the row that the call writes, so a
        statement more on the candidate side is a defect of the run."""
        blocks = BlockReadings(
            blocks=(
                BlockReading(calls=100, producer_s=0.05, postgres=CpuSeconds(0.02)),
            )
        )
        result = _result(
            _PUBLIC_CALL,
            TelemetryLevel.TRACING,
            Measurement(samples_ms=[1.0], counts=_COUNTS, readings=blocks),
            Measurement(
                samples_ms=[1.0],
                counts=dataclasses.replace(_COUNTS, client_statements=201),
                readings=blocks,
            ),
        )
        (report,) = _control_disagreements(result)
        assert 'client statements 200 vs 201' in report
