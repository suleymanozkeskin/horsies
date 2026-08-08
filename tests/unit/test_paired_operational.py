"""Operational rows report intervals separately, and say where each came from."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_operational import (
    ClassifiedObservation,
    CompetingReader,
    OperationalInterval,
    OperationalReport,
    OperationalReportError,
    ServerEventSample,
    classify,
    classify_observations,
    judge_interval,
)


def _sample(
    *, checkpoints: int = 10, autovacuum: int = 0, buffers: int = 500
) -> ServerEventSample:
    return ServerEventSample(
        checkpoints=checkpoints,
        autovacuum_workers=autovacuum,
        relation_buffers=buffers,
    )


def test_a_checkpoint_during_the_observation_names_the_interval() -> None:
    assert (
        classify(_sample(checkpoints=10), _sample(checkpoints=11))
        is OperationalInterval.CHECKPOINT
    )


def test_an_autovacuum_worker_names_the_interval() -> None:
    assert (
        classify(_sample(autovacuum=1), _sample())
        is OperationalInterval.AUTOVACUUM
    )
    assert (
        classify(_sample(), _sample(autovacuum=1))
        is OperationalInterval.AUTOVACUUM
    )


def test_a_checkpoint_outranks_a_concurrent_autovacuum() -> None:
    """The larger, rarer event is the one the budget separates for."""
    assert (
        classify(
            _sample(checkpoints=10, autovacuum=1),
            _sample(checkpoints=11, autovacuum=1),
        )
        is OperationalInterval.CHECKPOINT
    )


def test_nothing_resident_at_the_start_is_a_first_run() -> None:
    """Decided by residency, not by position in the run."""
    assert (
        classify(_sample(buffers=0), _sample(buffers=300))
        is OperationalInterval.FIRST_RUN
    )


def test_a_relation_still_filling_is_warm_cache() -> None:
    assert (
        classify(_sample(buffers=300), _sample(buffers=420))
        is OperationalInterval.WARM_CACHE
    )


def test_resident_and_not_filling_is_steady_state() -> None:
    """The narrow case, which is what makes it worth separating."""
    assert (
        classify(_sample(buffers=500), _sample(buffers=500))
        is OperationalInterval.STEADY_STATE
    )


def test_residency_falling_is_unclassified_not_steady_state() -> None:
    """Something evicted the relation underneath the observation.

    That is none of the five intervals, and folding it into the healthy one is
    the flattering direction.
    """
    assert (
        classify(_sample(buffers=500), _sample(buffers=120))
        is OperationalInterval.UNCLASSIFIED
    )


def test_every_observation_carries_the_evidence_that_placed_it() -> None:
    durations = [1.0, 2.0]
    samples = [
        (_sample(buffers=0), _sample(buffers=100)),
        (_sample(buffers=500), _sample(buffers=500)),
    ]
    classified = classify_observations(durations, samples)
    assert [entry.interval for entry in classified] == [
        OperationalInterval.FIRST_RUN,
        OperationalInterval.STEADY_STATE,
    ]
    conditions = classified[0].as_conditions()
    assert conditions['before']['relation_buffers'] == 0
    assert conditions['after']['relation_buffers'] == 100


def test_observations_without_evidence_cannot_be_placed() -> None:
    with pytest.raises(OperationalReportError, match='cannot be placed'):
        classify_observations([1.0, 2.0], [(_sample(), _sample())])


def test_a_row_with_no_observations_is_refused() -> None:
    with pytest.raises(OperationalReportError, match='needs observations'):
        classify_observations([], [])


def _report(pairs: list[tuple[float, OperationalInterval]]) -> OperationalReport:
    by_interval = {
        OperationalInterval.FIRST_RUN: (_sample(buffers=0), _sample(buffers=10)),
        OperationalInterval.WARM_CACHE: (_sample(buffers=10), _sample(buffers=50)),
        OperationalInterval.STEADY_STATE: (_sample(), _sample()),
        OperationalInterval.CHECKPOINT: (
            _sample(checkpoints=1),
            _sample(checkpoints=2),
        ),
        OperationalInterval.AUTOVACUUM: (_sample(autovacuum=1), _sample()),
        OperationalInterval.UNCLASSIFIED: (
            _sample(buffers=500),
            _sample(buffers=1),
        ),
    }
    return OperationalReport(
        row='health-pass',
        observations=tuple(
            ClassifiedObservation(
                index=index,
                duration_ms=duration,
                interval=interval,
                before=by_interval[interval][0],
                after=by_interval[interval][1],
            )
            for index, (duration, interval) in enumerate(pairs)
        ),
    )


def test_intervals_are_reported_separately() -> None:
    report = _report(
        [(1.0, OperationalInterval.STEADY_STATE)] * 4
        + [(90.0, OperationalInterval.CHECKPOINT)] * 2
    )
    conditions = report.as_conditions()
    assert set(conditions['intervals']) == {'steady-state', 'checkpoint'}
    assert conditions['intervals']['steady-state']['count'] == 4
    assert conditions['intervals']['checkpoint']['count'] == 2
    assert conditions['intervals']['checkpoint']['p50'] == 90.0


def test_the_report_offers_no_pooled_statistic() -> None:
    """The way to keep a forbidden number out is to give no way to make it."""
    report = _report(
        [(1.0, OperationalInterval.STEADY_STATE), (90.0, OperationalInterval.CHECKPOINT)]
    )
    assert not hasattr(report, 'p99')
    assert not hasattr(report, 'pooled')
    assert 'does not average them' in report.as_conditions()['pooled_statistic']


def test_unclassified_observations_are_reported_as_their_own_bucket() -> None:
    report = _report(
        [(1.0, OperationalInterval.STEADY_STATE)] * 3
        + [(50.0, OperationalInterval.UNCLASSIFIED)] * 2
    )
    conditions = report.as_conditions()
    assert conditions['unclassified'] == 2
    assert conditions['intervals']['unclassified']['count'] == 2
    assert conditions['intervals']['steady-state']['count'] == 3


def test_a_budget_is_judged_against_a_named_interval() -> None:
    """Two seconds p99 for a health pass is not a promise about a checkpoint."""
    report = _report(
        [(1.0, OperationalInterval.STEADY_STATE)] * 10
        + [(9000.0, OperationalInterval.CHECKPOINT)] * 2
    )
    verdict = judge_interval(
        report,
        interval=OperationalInterval.STEADY_STATE,
        statistic='p99',
        limit_ms=2000.0,
    )
    assert verdict['interval'] == 'steady-state'
    assert verdict['within_limit']
    assert verdict['count'] == 10


def test_an_interval_with_no_observations_cannot_be_judged() -> None:
    """A row that was not measured is not a row that passed."""
    report = _report([(1.0, OperationalInterval.STEADY_STATE)])
    with pytest.raises(OperationalReportError, match='not a row that passed'):
        judge_interval(
            report,
            interval=OperationalInterval.CHECKPOINT,
            statistic='p99',
            limit_ms=2000.0,
        )


def test_an_unknown_statistic_is_refused() -> None:
    report = _report([(1.0, OperationalInterval.STEADY_STATE)])
    with pytest.raises(OperationalReportError, match='no statistic named'):
        judge_interval(
            report,
            interval=OperationalInterval.STEADY_STATE,
            statistic='mean',
            limit_ms=2000.0,
        )


def test_a_competing_reader_declares_what_it_runs() -> None:
    reader = CompetingReader(
        statement='SELECT count(*) FROM horsies_tasks',
        cadence_ms=50,
        relations=('horsies_tasks',),
        backend_label='qual-long-reader',
    )
    conditions = reader.as_conditions()
    assert conditions['cadence_ms'] == 50
    assert conditions['relations'] == ['horsies_tasks']
    assert conditions['statement'].startswith('SELECT')


def test_a_reader_against_unnamed_relations_is_refused() -> None:
    """It cannot be shown to compete with the operation under test."""
    with pytest.raises(OperationalReportError, match='name the relations'):
        CompetingReader(
            statement='SELECT 1',
            cadence_ms=50,
            relations=(),
            backend_label='x',
        )


def test_a_reader_with_no_cadence_is_refused() -> None:
    with pytest.raises(OperationalReportError, match='at least 1 ms'):
        CompetingReader(
            statement='SELECT 1',
            cadence_ms=0,
            relations=('horsies_tasks',),
            backend_label='x',
        )


def test_a_report_with_no_observations_is_refused() -> None:
    """An empty report would answer every interval query with an absence."""
    with pytest.raises(OperationalReportError, match='no observations to report'):
        OperationalReport(row='health-pass', observations=())
