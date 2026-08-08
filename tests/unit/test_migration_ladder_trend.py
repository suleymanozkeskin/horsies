"""The per-batch trend gate, checked against the shapes that motivated it."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from horsies.core.history.cutover.ladder import BatchCommit
from tests.task_history_prototypes.migration_ladder_evidence import (
    PER_BATCH_TREND_GATE_FRACTION,
    RESIDUAL_TREND_GATE_FRACTION,
    LadderProgressSnapshot,
    RungMeasurementError,
    Trajectory,
    compute_per_batch_trend,
    fit_trajectory,
)
from tests.task_history_prototypes.qualification_io import (
    AtomicEvidenceWriter,
)

BATCH_SIZE = 10_000


def _trajectory(stage: str, durations: list[float]) -> Trajectory:
    """Build a trajectory whose per-batch durations are exactly `durations`."""
    commits: list[BatchCommit] = []
    elapsed = 0.0
    for index, duration in enumerate(durations, start=1):
        elapsed += duration
        commits.append(
            BatchCommit(
                cumulative_rows=index * BATCH_SIZE,
                elapsed_seconds=elapsed,
            )
        )
    return Trajectory(
        stage=stage,
        batch_size=BATCH_SIZE,
        batches=len(durations),
        rows=len(durations) * BATCH_SIZE,
        seconds=elapsed,
        commits=tuple(commits),
    )


def _linear(count: int, *, start: float, step: float) -> list[float]:
    return [start + step * index for index in range(count)]


def test_flat_stage_passes_the_gate() -> None:
    trend = compute_per_batch_trend(
        _trajectory('relocation', [1.1] * 100)
    )
    assert trend.within_gate
    assert trend.trend_seconds_per_batch == pytest.approx(0.0, abs=1e-12)


def test_measured_relocation_shape_passes() -> None:
    """The copy stage as actually measured: mean 1.12s, drifting -1.3%."""
    trend = compute_per_batch_trend(
        _trajectory(
            'relocation', _linear(100, start=1.1297, step=-0.000147)
        )
    )
    assert trend.drift_fraction_of_mean == pytest.approx(-0.0131, abs=5e-4)
    assert trend.within_gate


def test_measured_preparation_shape_fails() -> None:
    """The counterfactual the gate exists for.

    Preparation as measured on the first rung: a mean batch near 6.1s with
    per-batch cost rising 13.5ms per batch, because the batch selection
    rescanned rows it had already prepared. Extrapolating that stage's
    averaged slope is what cost the next rung five hours.
    """
    trend = compute_per_batch_trend(
        _trajectory('preparation', _linear(100, start=5.4335, step=0.0135))
    )
    assert trend.trend_seconds_per_batch == pytest.approx(0.0135, abs=1e-5)
    assert trend.drift_fraction_of_mean == pytest.approx(0.22, abs=0.01)
    assert not trend.within_gate


def test_gate_is_symmetric() -> None:
    """A stage getting faster is also not a constant per-row cost."""
    rising = compute_per_batch_trend(
        _trajectory('preparation', _linear(100, start=5.4335, step=0.0135))
    )
    falling = compute_per_batch_trend(
        _trajectory('preparation', _linear(100, start=6.7835, step=-0.0135))
    )
    assert not falling.within_gate
    assert falling.drift_fraction_of_mean == pytest.approx(
        -rising.drift_fraction_of_mean, abs=0.01
    )


@pytest.mark.parametrize(
    ('fraction_of_gate', 'expected_within'),
    [(0.99, True), (1.01, False)],
)
def test_gate_decides_on_either_side_of_the_threshold(
    fraction_of_gate: float, expected_within: bool
) -> None:
    """Just inside passes, just outside fails.

    The exact boundary is not asserted: a drift constructed to land on it
    arrives a float epsilon above or below, so a test pinning it would be
    testing arithmetic noise rather than the gate.
    """
    count = 100
    mean = 1.0
    target = PER_BATCH_TREND_GATE_FRACTION * fraction_of_gate
    step = target * mean / count
    trend = compute_per_batch_trend(
        _trajectory(
            'relocation',
            _linear(count, start=mean - step * (count - 1) / 2, step=step),
        )
    )
    assert trend.drift_fraction_of_mean == pytest.approx(target, rel=1e-6)
    assert trend.within_gate is expected_within


def test_single_batch_cannot_show_drift() -> None:
    with pytest.raises(RungMeasurementError, match='two committed batches'):
        compute_per_batch_trend(_trajectory('relocation', [1.1]))


def test_declared_intercept_gates_the_residual_not_the_raw_series() -> None:
    """A stage fitted with an intercept is judged on model conformance.

    The warm-up shape: per-batch cost decays over the first batches, then
    holds. Raw, it drifts far outside the raw gate; against its own declared
    model the residual is inside the residual gate. Rejecting it raw would
    reject the stage for having exactly the shape it was fitted with.
    """
    durations = [
        1.7 + 0.85 * (0.86**index) for index in range(100)
    ]
    trajectory = _trajectory('preparation', durations)

    raw = compute_per_batch_trend(trajectory)
    residual = compute_per_batch_trend(
        trajectory, declared_fit=fit_trajectory(trajectory)
    )

    assert raw.mode == 'raw'
    assert not raw.within_gate
    assert residual.mode == 'residual'
    assert residual.gate_fraction == RESIDUAL_TREND_GATE_FRACTION
    assert abs(residual.drift_fraction_of_mean) < abs(
        raw.drift_fraction_of_mean
    )
    assert residual.within_gate


def test_growth_still_fails_against_its_own_declared_model() -> None:
    """The counterfactual survives the looser residual threshold.

    Raising the gate from 5% to 8% for the residual measurand must not buy
    the candidate its pass by giving up the detection the gate exists for.
    The quadratic shape — per-batch cost rising 13.5ms per batch — is fitted
    with an intercept and still fails, because an intercept cannot absorb
    growth that never stops.
    """
    trajectory = _trajectory(
        'preparation', _linear(100, start=5.4335, step=0.0135)
    )
    declared = fit_trajectory(trajectory)

    residual = compute_per_batch_trend(trajectory, declared_fit=declared)

    assert residual.drift_fraction_of_mean == pytest.approx(0.108, abs=0.01)
    assert residual.drift_fraction_of_mean > RESIDUAL_TREND_GATE_FRACTION
    assert not residual.within_gate
    # The fit itself reports the mismatch: absorbing rising cost drives the
    # head cost negative, which is not a quantity a stage can have.
    assert declared.intercept_seconds < 0.0


def test_progress_snapshot_survives_the_evidence_writer(
    tmp_path: Path,
) -> None:
    """The flush path is executed, not merely constructed.

    The writer serializes through `asdict`, which accepts dataclass
    instances and nothing else. A snapshot built as a plain mapping raises
    at the first flush — which is at the first stage boundary, so the run
    dies immediately and the flush that exists to preserve evidence
    destroys the run instead.
    """
    destination = tmp_path / 'partial.json'
    snapshot = LadderProgressSnapshot(
        status='in_progress',
        scenario='migration-ladder',
        last_stage='preparation',
        batches_committed=2,
        commits=_trajectory('preparation', [6.0, 6.1]).commits,
        workload={
            'rung_rows': 1_000_000,
            'attempts_per_task': 1,
            'batch_size': BATCH_SIZE,
            'next_rung_rows': 10_000_000,
        },
    )

    AtomicEvidenceWriter(destination).write(snapshot)

    written = json.loads(destination.read_text(encoding='utf-8'))
    assert written['status'] == 'in_progress'
    assert written['last_stage'] == 'preparation'
    assert len(written['commits']) == 2
    assert written['commits'][1]['cumulative_rows'] == 2 * BATCH_SIZE
    assert written['workload']['rung_rows'] == 1_000_000
    # A snapshot must not be readable as a finished measurement.
    assert 'measurement' not in written
    assert 'footprint' not in written


def test_disabled_writer_accepts_the_snapshot() -> None:
    """A run without a checkpoint path flushes into nothing, not into a crash."""
    AtomicEvidenceWriter(None).write(
        LadderProgressSnapshot(
            status='in_progress',
            scenario='migration-ladder',
            last_stage='drain',
            batches_committed=0,
            commits=(),
            workload={'rung_rows': 1},
        )
    )
