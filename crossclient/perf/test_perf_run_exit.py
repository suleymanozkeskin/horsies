# pyright: reportPrivateUsage=false
"""Exit semantics of a measurement run, per scale and comparison mode.

A control run compares an implementation against itself, so its latency
verdict carries no information about the code — at smoke scale p99 rests on
a handful of samples, and a neighbour's burst during one side's blocks
produces a narrow interval around a real difference in the runner. What a
control run answers for is exact counts, which fail the run at every scale.

Regression anchor: a CI control run at smoke scale reported a p99 delta of
+4.6 ms against a 1 ms budget with agreeing server counts, and the run
exited nonzero. The same job at the same commit passed minutes earlier.
"""

from __future__ import annotations

import dataclasses
from typing import NoReturn

import pytest

from crossclient.perf import __main__ as perf_main
from crossclient.perf.__main__ import (
    GATE_ENVIRONMENT,
    _block_size_for,
    _control_disagreements,
    _exit_status,
    _gate_environment_violations,
    _has_candidate,
    _minimum_observations,
    _observations_for,
    _select_scenarios,
)
from datetime import timedelta

from crossclient.perf.counters import Counts
from crossclient.perf.scenarios import (
    PublicCallScenario,
    Scenario,
    SingleRowScenario,
    WorkerDrainScenario,
)
from crossclient.perf.runner import (
    Conditions,
    Measurement,
    PlanEvidence,
    RunResult,
    _comparison_label,
    _contract_violations,
)
from crossclient.perf.readings import StatementReadings
from crossclient.perf.statistics import Budget, Verdict
from crossclient.perf.telemetry_levels import TelemetryLevel


def _never_run(*_args: object) -> NoReturn:
    raise AssertionError('the tests of the exit rules run no scenario')


_SCENARIO = SingleRowScenario(
    name='run-exit-example',
    description='a scenario built for these tests',
    p50_budget=Budget(fraction=0.10, floor_ms=1.0),
    p99_budget=Budget(fraction=0.15, floor_ms=2.0),
    payload_bytes=1024,
    seed=_never_run,
    cleanup=_never_run,
    baseline=_never_run,
    candidate=None,
    baseline_invocation=_never_run,
    candidate_invocation=None,
    exact_client_statements_per_operation=1,
)
"""A single-row scenario with no candidate side, one statement per operation."""

_PUBLIC_CALL = PublicCallScenario(
    name='run-exit-public-call',
    description='a public call scenario built for these tests',
    p50_budget=Budget(fraction=0.10, floor_ms=0.5),
    p99_budget=Budget(fraction=0.15, floor_ms=1.5),
    payload_bytes=200,
    open_call=_never_run,
    cleanup=_never_run,
)
"""A public call scenario: one implementation, which both sides run."""

_DRAIN = WorkerDrainScenario(
    name='run-exit-drain',
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
"""A worker drain: one implementation, which both sides run."""


def _conditions(comparison: str, observations: int = 200) -> Conditions:
    return Conditions(
        scenario=_SCENARIO.name,
        description='a scenario built for these tests',
        server_version='PostgreSQL 16.4',
        fsync='off',
        full_page_writes='off',
        synchronous_commit='off',
        autovacuum='off',
        observations_per_side=observations,
        block_size=100,
        ballast_rows=100_000,
        payload_bytes=1024,
        batch_size=None,
        resamples=200,
        seed=20260804,
        comparison=comparison,
        demo_quiesced=False,
        measured_at='2026-08-04T00:00:00+00:00',
        telemetry_level=TelemetryLevel.OFF.value,
        telemetry_description='no telemetry',
        postgres_cpu_source='not measured (no --postgres-cpu-stat)',
    )


def _counts(
    client_statements: int = 200,
    nested_statements: int = 0,
    client_rows: int = 200,
    nested_rows: int = 0,
    terminal_rows: int = 200,
    write_transactions: int = 200,
    wal_records: int = 1_000,
    wal_bytes: int = 100_000,
) -> Counts:
    return Counts(
        client_statements=client_statements,
        nested_statements=nested_statements,
        client_rows=client_rows,
        nested_rows=nested_rows,
        terminal_rows=terminal_rows,
        wal_records=wal_records,
        wal_bytes=wal_bytes,
        wal_fpi=0,
        write_transactions=write_transactions,
    )


def _with_candidate() -> Scenario:
    """A scenario with a candidate side, for the checks of a real comparison.

    On the baseline schema no scenario has a second implementation, so the
    contract checks are exercised on a scenario whose candidate is its own
    baseline.
    """
    return dataclasses.replace(
        _SCENARIO,
        candidate=_SCENARIO.baseline,
        candidate_invocation=_SCENARIO.baseline_invocation,
    )


def _control_result(
    baseline: Counts,
    candidate: Counts,
    verdict: Verdict = Verdict.PASS,
) -> RunResult:
    return RunResult(
        scenario=_SCENARIO,
        conditions=_conditions('existing statement against itself (harness control)'),
        baseline=Measurement(
            readings=StatementReadings(), samples_ms=[1.0], counts=baseline
        ),
        candidate=Measurement(
            readings=StatementReadings(), samples_ms=[1.0], counts=candidate
        ),
        plans=None,
        comparisons=[],
        throughput=None,
        costs=(),
        contract_violations=(),
        verdict=verdict,
    )


class TestSmokeExitStatus:
    def test_control_latency_fail_is_not_judged(self) -> None:
        assert _exit_status('smoke', [(Verdict.FAIL, True)]) == 0

    def test_non_control_latency_fail_still_fails(self) -> None:
        assert _exit_status('smoke', [(Verdict.FAIL, False)]) == 1

    def test_inconclusive_is_tolerated_in_both_modes(self) -> None:
        outcomes = [(Verdict.INCONCLUSIVE, True), (Verdict.INCONCLUSIVE, False)]
        assert _exit_status('smoke', outcomes) == 0

    def test_one_judged_failure_among_passes_fails(self) -> None:
        outcomes = [
            (Verdict.PASS, False),
            (Verdict.FAIL, True),
            (Verdict.FAIL, False),
        ]
        assert _exit_status('smoke', outcomes) == 1


class TestGateExitStatus:
    @pytest.mark.parametrize('verdict', [Verdict.FAIL, Verdict.INCONCLUSIVE])
    @pytest.mark.parametrize('is_control', [True, False])
    def test_anything_short_of_pass_fails_closed(
        self,
        verdict: Verdict,
        is_control: bool,
    ) -> None:
        assert _exit_status('gate', [(Verdict.PASS, False), (verdict, is_control)]) == 1

    def test_all_pass_exits_zero(self) -> None:
        assert _exit_status('gate', [(Verdict.PASS, True), (Verdict.PASS, False)]) == 0


class TestControlCountChecks:
    def test_agreeing_counts_report_nothing(self) -> None:
        result = _control_result(_counts(), _counts())
        assert _control_disagreements(result) == []

    def test_client_statement_disagreement_is_reported(self) -> None:
        result = _control_result(_counts(), _counts(client_statements=201))
        report = _control_disagreements(result)
        assert len(report) == 1
        assert 'client statements 200 vs 201' in report[0]

    def test_client_rows_disagreement_is_reported(self) -> None:
        result = _control_result(_counts(), _counts(client_rows=199))
        report = _control_disagreements(result)
        assert len(report) == 1
        assert 'client rows 200 vs 199' in report[0]

    def test_nested_statements_disagreement_is_reported(self) -> None:
        result = _control_result(_counts(), _counts(nested_statements=1))
        report = _control_disagreements(result)
        assert len(report) == 1
        assert 'nested statements 0 vs 1' in report[0]

    def test_nested_rows_disagreement_is_reported(self) -> None:
        result = _control_result(_counts(), _counts(nested_rows=1))
        report = _control_disagreements(result)
        assert len(report) == 1
        assert 'nested rows 0 vs 1' in report[0]

    def test_terminal_rows_disagreement_is_reported(self) -> None:
        result = _control_result(_counts(), _counts(terminal_rows=199))
        report = _control_disagreements(result)
        assert len(report) == 1
        assert 'terminal rows 200 vs 199' in report[0]

    def test_count_disagreement_is_reported_even_when_latency_passed(self) -> None:
        result = _control_result(
            _counts(),
            _counts(client_statements=400),
            verdict=Verdict.PASS,
        )
        assert _control_disagreements(result) != []

    def test_non_control_comparison_is_not_held_to_count_equality(self) -> None:
        result = RunResult(
            scenario=_SCENARIO,
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=_counts()
            ),
            candidate=Measurement(
                readings=StatementReadings(),
                samples_ms=[1.0],
                counts=_counts(client_statements=100),
            ),
            plans=None,
            comparisons=[],
            throughput=None,
            costs=(),
            contract_violations=(),
            verdict=Verdict.PASS,
        )
        assert _control_disagreements(result) == []


class TestCandidateContractChecks:
    def _violations(
        self,
        baseline: Counts,
        candidate: Counts,
    ) -> tuple[str, ...]:
        return _contract_violations(
            scenario=_with_candidate(),
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=baseline
            ),
            candidate=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=candidate
            ),
            plans=PlanEvidence(baseline={}, candidate={}),
            control=False,
        )

    def test_equal_counts_and_wal_pass(self) -> None:
        assert self._violations(_counts(), _counts()) == ()

    def test_extra_client_statement_fails(self) -> None:
        report = self._violations(_counts(), _counts(client_statements=201))
        assert any('client statements increased' in item for item in report)

    def test_commit_change_fails(self) -> None:
        report = self._violations(_counts(), _counts(write_transactions=201))
        assert any('write transactions changed' in item for item in report)

    def test_equal_extra_commits_fail_the_declared_path_shape(self) -> None:
        report = self._violations(
            _counts(write_transactions=400),
            _counts(write_transactions=400),
        )
        assert any('baseline write transactions 400 != 200' in item for item in report)
        assert any('candidate write transactions 400 != 200' in item for item in report)

    def test_wal_record_increase_at_limit_passes(self) -> None:
        assert self._violations(_counts(), _counts(wal_records=1_020)) == ()

    def test_wal_record_increase_beyond_limit_fails(self) -> None:
        report = self._violations(_counts(), _counts(wal_records=1_021))
        assert any('WAL records per terminal row increased' in item for item in report)

    def test_wal_bytes_use_percentage_or_128_byte_floor(self) -> None:
        baseline = _counts(wal_bytes=100_000)
        within = _counts(wal_bytes=125_600)
        outside = _counts(wal_bytes=125_800)
        assert self._violations(baseline, within) == ()
        assert any(
            'WAL bytes per terminal row increased' in item
            for item in self._violations(baseline, outside)
        )

    def test_missing_terminal_transition_fails(self) -> None:
        report = self._violations(_counts(), _counts(terminal_rows=199))
        assert any('candidate terminal rows 199 != 200' in item for item in report)

    def test_missing_plan_evidence_fails(self) -> None:
        report = _contract_violations(
            scenario=_with_candidate(),
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=_counts()
            ),
            candidate=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=_counts()
            ),
            plans=None,
            control=False,
        )
        assert 'instrumented plan evidence is missing' in report

    def test_a_scenario_without_candidate_has_no_contract_to_break(self) -> None:
        report = _contract_violations(
            scenario=_SCENARIO,
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=_counts()
            ),
            candidate=Measurement(
                readings=StatementReadings(),
                samples_ms=[1.0],
                counts=_counts(client_statements=400),
            ),
            plans=None,
            control=False,
        )
        assert report == ()


class TestGateEnvironment:
    def test_conforming_environment_has_no_violations(self) -> None:
        assert _gate_environment_violations(dict(GATE_ENVIRONMENT)) == []

    def test_autovacuum_on_is_a_named_violation(self) -> None:
        observed = {**GATE_ENVIRONMENT, 'autovacuum': 'on'}
        report = _gate_environment_violations(observed)
        assert len(report) == 1
        assert "autovacuum is 'on'" in report[0]

    def test_full_page_writes_on_is_a_named_violation(self) -> None:
        observed = {**GATE_ENVIRONMENT, 'full_page_writes': 'on'}
        report = _gate_environment_violations(observed)
        assert len(report) == 1
        assert "full_page_writes is 'on'" in report[0]

    def test_a_setting_the_server_did_not_answer_is_a_violation(self) -> None:
        observed = dict(GATE_ENVIRONMENT)
        del observed['fsync']
        report = _gate_environment_violations(observed)
        assert len(report) == 1
        assert 'fsync' in report[0]


class TestScenarioSelection:
    def test_candidates_contains_every_and_only_real_comparison(self) -> None:
        selected = _select_scenarios('candidates')
        assert all(_has_candidate(scenario) for scenario in selected)
        assert {scenario.name for scenario in selected} == {
            scenario.name
            for scenario in _select_scenarios('all')
            if _has_candidate(scenario)
        }

    def test_all_selects_the_public_operations_of_both_releases(self) -> None:
        """The release comparison pairs these by name with the v0.7.0 harness."""
        public = {
            scenario.name
            for scenario in _select_scenarios('all')
            if isinstance(scenario, PublicCallScenario)
        }
        assert public == {'send', 'workflow-start'}

    def test_a_public_call_scenario_is_never_a_candidate(self) -> None:
        assert not _has_candidate(_PUBLIC_CALL)

    def test_all_selects_the_worker_drain(self) -> None:
        """Claim and finish run through a worker that the CLI of each
        release starts; the release comparison pairs the drain by name."""
        drains = {
            scenario.name
            for scenario in _select_scenarios('all')
            if isinstance(scenario, WorkerDrainScenario)
        }
        assert drains == {'claim-finish'}

    def test_a_selection_of_no_scenario_is_refused(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A run that measures nothing must not exit as if it passed."""

        def select_nothing(selector: str) -> tuple[Scenario, ...]:
            del selector
            return ()

        monkeypatch.setattr(perf_main, '_select_scenarios', select_nothing)
        with pytest.raises(SystemExit) as raised:
            perf_main.main(
                [
                    '--dsn',
                    'postgresql://perf@localhost/unused',
                    '--scenario',
                    'candidates',
                    '--scale',
                    'smoke',
                ]
            )
        assert raised.value.code == 2


class TestObservationCount:
    def test_default_uses_the_declared_gate_minimum(self) -> None:
        scenario = _SCENARIO
        assert _observations_for(scenario, 'gate', None) == 10_000
        assert _minimum_observations(scenario, 'gate') == 10_000

    def test_override_supports_an_inconclusive_retry(self) -> None:
        scenario = _SCENARIO
        assert _observations_for(scenario, 'gate', 20_000) == 20_000


class TestPublicCallScenario:
    """A public call scenario runs one call of the release on both sides."""

    def test_a_gate_without_control_is_refused(self) -> None:
        """The gate needs a second implementation; a public call has one.
        The refusal comes before any connection: port 1 has no server."""
        with pytest.raises(SystemExit) as raised:
            perf_main.main(
                [
                    '--dsn',
                    'postgresql://perf@127.0.0.1:1/unused',
                    '--scenario',
                    'send',
                    '--scale',
                    'gate',
                    '--demo-quiesced',
                ]
            )
        assert raised.value.code == 2

    def test_the_comparison_is_labelled_a_control(self) -> None:
        """The control count checks apply to it, with or without --control."""
        assert 'control' in _comparison_label(
            _PUBLIC_CALL, control=False, telemetry=TelemetryLevel.OFF
        )
        assert 'control' in _comparison_label(
            _PUBLIC_CALL, control=True, telemetry=TelemetryLevel.OFF
        )

    def test_a_count_disagreement_between_its_sides_is_reported(self) -> None:
        result = dataclasses.replace(
            _control_result(_counts(), _counts(client_statements=201)),
            scenario=_PUBLIC_CALL,
            conditions=_conditions(
                _comparison_label(
                    _PUBLIC_CALL, control=False, telemetry=TelemetryLevel.OFF
                )
            ),
        )
        report = _control_disagreements(result)
        assert len(report) == 1
        assert 'client statements 200 vs 201' in report[0]

    def test_it_has_no_contract_of_a_real_comparison(self) -> None:
        report = _contract_violations(
            scenario=_PUBLIC_CALL,
            conditions=_conditions('public call against itself (harness control)'),
            baseline=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=_counts()
            ),
            candidate=Measurement(
                readings=StatementReadings(),
                samples_ms=[1.0],
                counts=_counts(client_statements=400),
            ),
            plans=None,
            control=False,
        )
        assert report == ()

    def test_it_takes_the_single_row_observation_counts(self) -> None:
        assert _minimum_observations(_PUBLIC_CALL, 'gate') == 10_000
        assert _minimum_observations(_PUBLIC_CALL, 'smoke') == 200

    def test_its_block_is_the_requested_size(self) -> None:
        assert _block_size_for(_PUBLIC_CALL, 100) == 100


class TestWorkerDrainScenario:
    """A worker drain runs one drain of the release on both sides."""

    def test_a_gate_without_control_is_refused(self) -> None:
        """The gate needs a second implementation; a drain has one. The
        refusal comes before any connection or worker: port 1 has no server."""
        with pytest.raises(SystemExit) as raised:
            perf_main.main(
                [
                    '--dsn',
                    'postgresql://perf@127.0.0.1:1/unused',
                    '--scenario',
                    'claim-finish',
                    '--scale',
                    'gate',
                    '--demo-quiesced',
                ]
            )
        assert raised.value.code == 2

    def test_observations_below_the_gate_drains_are_refused(self) -> None:
        with pytest.raises(SystemExit) as raised:
            perf_main.main(
                [
                    '--dsn',
                    'postgresql://perf@127.0.0.1:1/unused',
                    '--scenario',
                    'claim-finish',
                    '--scale',
                    'gate',
                    '--demo-quiesced',
                    '--control',
                    '--observations',
                    str(perf_main.GATE_DRAINS - 1),
                ]
            )
        assert raised.value.code == 2

    def test_it_is_never_a_candidate(self) -> None:
        assert not _has_candidate(_DRAIN)

    def test_the_comparison_is_labelled_a_control(self) -> None:
        """A smoke run does not judge its throughput verdict; a gate does."""
        assert 'control' in _comparison_label(
            _DRAIN, control=False, telemetry=TelemetryLevel.OFF
        )
        assert 'control' in _comparison_label(
            _DRAIN, control=True, telemetry=TelemetryLevel.OFF
        )

    def test_a_count_disagreement_between_its_sides_is_not_reported(self) -> None:
        """The statements of a worker depend on its timing, so two drains of
        one release differ; each drain checks its own tasks instead."""
        result = dataclasses.replace(
            _control_result(
                _counts(),
                _counts(client_statements=401, write_transactions=900),
            ),
            scenario=_DRAIN,
            conditions=_conditions(
                _comparison_label(_DRAIN, control=True, telemetry=TelemetryLevel.OFF)
            ),
        )
        assert _control_disagreements(result) == []

    def test_it_has_no_contract_of_a_real_comparison(self) -> None:
        report = _contract_violations(
            scenario=_DRAIN,
            conditions=_conditions(
                _comparison_label(_DRAIN, control=True, telemetry=TelemetryLevel.OFF)
            ),
            baseline=Measurement(
                readings=StatementReadings(), samples_ms=[1.0], counts=_counts()
            ),
            candidate=Measurement(
                readings=StatementReadings(),
                samples_ms=[1.0],
                counts=_counts(client_statements=400),
            ),
            plans=None,
            control=True,
        )
        assert report == ()

    def test_it_takes_the_drain_counts(self) -> None:
        assert _minimum_observations(_DRAIN, 'gate') == perf_main.GATE_DRAINS
        assert _minimum_observations(_DRAIN, 'smoke') == perf_main.SMOKE_DRAINS

    def test_its_block_is_one_drain(self) -> None:
        assert _block_size_for(_DRAIN, 100) == 1

    @pytest.mark.parametrize('verdict', [Verdict.FAIL, Verdict.INCONCLUSIVE])
    def test_a_smoke_run_does_not_judge_its_throughput(self, verdict: Verdict) -> None:
        assert _exit_status('smoke', [(verdict, True)]) == 0

    @pytest.mark.parametrize('verdict', [Verdict.FAIL, Verdict.INCONCLUSIVE])
    def test_a_gate_needs_a_throughput_pass(self, verdict: Verdict) -> None:
        assert _exit_status('gate', [(verdict, True)]) == 1
