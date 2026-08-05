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

import pytest

from tests.perf.__main__ import (
    GATE_ENVIRONMENT,
    _control_disagreements,
    _exit_status,
    _gate_environment_violations,
    _minimum_observations,
    _observations_for,
    _select_scenarios,
)
from tests.perf.counters import Counts
from tests.perf.scenarios import scenario_by_name
from tests.perf.runner import (
    Conditions,
    Measurement,
    PlanEvidence,
    RunResult,
    _contract_violations,
)
from tests.perf.statistics import Verdict


def _conditions(comparison: str, observations: int = 200) -> Conditions:
    return Conditions(
        scenario='locked-completion',
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


def _control_result(
    baseline: Counts,
    candidate: Counts,
    verdict: Verdict = Verdict.PASS,
) -> RunResult:
    return RunResult(
        conditions=_conditions('existing statement against itself (harness control)'),
        baseline=Measurement(samples_ms=[1.0], counts=baseline),
        candidate=Measurement(samples_ms=[1.0], counts=candidate),
        plans=None,
        comparisons=[],
        throughput=None,
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
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(samples_ms=[1.0], counts=_counts()),
            candidate=Measurement(
                samples_ms=[1.0],
                counts=_counts(client_statements=100),
            ),
            plans=None,
            comparisons=[],
            throughput=None,
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
            scenario=scenario_by_name('locked-completion'),
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(samples_ms=[1.0], counts=baseline),
            candidate=Measurement(samples_ms=[1.0], counts=candidate),
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
            scenario=scenario_by_name('locked-completion'),
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(samples_ms=[1.0], counts=_counts()),
            candidate=Measurement(samples_ms=[1.0], counts=_counts()),
            plans=None,
            control=False,
        )
        assert 'instrumented plan evidence is missing' in report


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
        assert selected
        assert all(scenario.candidate is not None for scenario in selected)
        assert {scenario.name for scenario in selected} == {
            scenario.name
            for scenario in _select_scenarios('all')
            if scenario.candidate is not None
        }


class TestObservationCount:
    def test_default_uses_the_declared_gate_minimum(self) -> None:
        scenario = scenario_by_name('locked-completion')
        assert _observations_for(scenario, 'gate', None) == 10_000
        assert _minimum_observations(scenario, 'gate') == 10_000

    def test_override_supports_an_inconclusive_retry(self) -> None:
        scenario = scenario_by_name('locked-completion')
        assert _observations_for(scenario, 'gate', 20_000) == 20_000
