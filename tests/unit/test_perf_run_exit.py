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

from tests.perf.__main__ import _control_disagreements, _exit_status
from tests.perf.counters import Counts
from tests.perf.runner import Conditions, Measurement, RunResult
from tests.perf.statistics import Verdict


def _conditions(comparison: str, observations: int = 200) -> Conditions:
    return Conditions(
        scenario='locked-completion',
        description='a scenario built for these tests',
        server_version='PostgreSQL 16.4',
        fsync='off',
        synchronous_commit='off',
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
    rows_affected: int = 200,
    write_transactions: int = 200,
) -> Counts:
    return Counts(
        client_statements=client_statements,
        nested_statements=0,
        rows_affected=rows_affected,
        wal_records=1_000,
        wal_bytes=100_000,
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
        comparisons=[],
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
        self, verdict: Verdict, is_control: bool,
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

    def test_rows_affected_disagreement_is_reported(self) -> None:
        result = _control_result(_counts(), _counts(rows_affected=199))
        report = _control_disagreements(result)
        assert len(report) == 1
        assert 'rows affected 200 vs 199' in report[0]

    def test_count_disagreement_is_reported_even_when_latency_passed(self) -> None:
        result = _control_result(
            _counts(), _counts(client_statements=400), verdict=Verdict.PASS,
        )
        assert _control_disagreements(result) != []

    def test_non_control_comparison_is_not_held_to_count_equality(self) -> None:
        result = RunResult(
            conditions=_conditions('existing statement against function'),
            baseline=Measurement(samples_ms=[1.0], counts=_counts()),
            candidate=Measurement(
                samples_ms=[1.0], counts=_counts(client_statements=100),
            ),
            comparisons=[],
            verdict=Verdict.PASS,
        )
        assert _control_disagreements(result) == []
