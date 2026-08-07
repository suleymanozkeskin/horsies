"""Stage 1: the laddered dry-run campaign — hooks and the refit.

Three rungs on disposable databases: one million rows, one plus ten
million, and the hundred-million rung that runs only if the second
disproves nothing. Each rung REFITS the coefficients from its own
measured run and re-emits the estimate with its ceiling; a rung whose
measured time busts the ceiling STOPS the ladder — the estimate is
disproven, not overridden, and the coefficients must be refit before
any further rung or the real run.
"""

from __future__ import annotations

from dataclasses import dataclass

from .preflight import (
    CutoverEstimate,
    RelocationCoefficients,
    estimate_relocation,
)


@dataclass(frozen=True, slots=True)
class LadderRung:
    name: str
    rows: int
    contingent: bool


LADDER: tuple[LadderRung, ...] = (
    LadderRung(name='one-million', rows=1_000_000, contingent=False),
    LadderRung(name='eleven-million', rows=11_000_000, contingent=False),
    LadderRung(name='hundred-million', rows=100_000_000, contingent=True),
)


@dataclass(frozen=True, slots=True)
class MeasuredRun:
    rows: int
    seconds: float


@dataclass(frozen=True, slots=True)
class RungPassed:
    rung: LadderRung
    estimate: CutoverEstimate
    measured_seconds: float
    refit: RelocationCoefficients


@dataclass(frozen=True, slots=True)
class RungBusted:
    """The measured run exceeded the ceiling: the estimate is
    disproven. The ladder stops here."""

    rung: LadderRung
    estimate: CutoverEstimate
    measured_seconds: float


def fit_coefficients(
    runs: tuple[MeasuredRun, ...],
) -> RelocationCoefficients:
    """Least-squares fit of the two-coefficient linear model.

    One run pins the slope through the origin offset; two or more fit
    both coefficients. Rows are scaled to millions so the slope is the
    field the estimate carries.
    """
    if not runs:
        raise ValueError('cannot fit coefficients from zero runs')
    if len(runs) == 1:
        only = runs[0]
        return RelocationCoefficients(
            seconds_per_million_rows=(
                only.seconds / (only.rows / 1_000_000)
            ),
            fixed_seconds=0.0,
        )
    n = len(runs)
    xs = [run.rows / 1_000_000 for run in runs]
    ys = [run.seconds for run in runs]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        raise ValueError('cannot fit a slope from identical row counts')
    slope = (
        sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        / denominator
    )
    intercept = mean_y - slope * mean_x
    return RelocationCoefficients(
        seconds_per_million_rows=slope,
        fixed_seconds=max(intercept, 0.0),
    )


def evaluate_rung(
    rung: LadderRung,
    *,
    coefficients: RelocationCoefficients,
    measured: MeasuredRun,
    prior_runs: tuple[MeasuredRun, ...] = (),
) -> RungPassed | RungBusted:
    """Judge one measured rung against the estimate it was given."""
    estimate = estimate_relocation(coefficients, rows=rung.rows)
    if measured.seconds > estimate.ceiling_seconds:
        return RungBusted(
            rung=rung,
            estimate=estimate,
            measured_seconds=measured.seconds,
        )
    return RungPassed(
        rung=rung,
        estimate=estimate,
        measured_seconds=measured.seconds,
        refit=fit_coefficients((*prior_runs, measured)),
    )
