"""Stage 1: the laddered dry-run campaign — hooks and the fit.

Three rungs on disposable databases: one million rows, ten million,
and the hundred-million rung that runs only if the second disproves
nothing. Each rung refits the model from its own measured run and a
rung whose measured time falls outside EITHER declared bound stops
the ladder — over the 5/4 ceiling or under the 7/10 floor, the
estimate is disproven, not overridden. The floor exists because an
over-predicting estimator is as wrong as an under-predicting one:
without it, an inflated slope sails under the ceiling forever and the
adopter inherits the inflation invisibly.

The fit separates the model's two terms by MEASUREMENT, not by
inference from one total: the fixed term is measured directly at the
stage boundaries (setup before the first batch commit plus
finalization after the last), and the slope comes from an intra-run
least-squares regression over the relocation's committed batches
(constant batch size, cumulative elapsed vs cumulative rows). The
regression's own intercept is REPORTED BESIDE the measured fixed
term: the two estimate the same quantity by independent routes, and
a large disagreement means the model's decomposition is wrong — the
seam the one-total fit could never show.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .preflight import (
    CutoverEstimate,
    RelocationCoefficients,
    estimate_relocation,
)

RUNG_FLOOR_NUMERATOR: Final = 7
RUNG_FLOOR_DENOMINATOR: Final = 10


@dataclass(frozen=True, slots=True)
class LadderRung:
    name: str
    rows: int
    contingent: bool


LADDER: tuple[LadderRung, ...] = (
    LadderRung(name='one-million', rows=1_000_000, contingent=False),
    LadderRung(name='ten-million', rows=10_000_000, contingent=False),
    LadderRung(name='hundred-million', rows=100_000_000, contingent=True),
)


@dataclass(frozen=True, slots=True)
class BatchCommit:
    """One committed relocation batch: the intra-run observation."""

    cumulative_rows: int
    elapsed_seconds: float


@dataclass(frozen=True, slots=True)
class MeasuredRun:
    """One rung's measurements, with the model's terms kept apart.

    `fixed_seconds` is measured directly at the stage boundaries —
    never inferred from the total — and `commits` carries the
    per-batch trajectory the slope regression runs over.
    """

    rows: int
    seconds: float
    fixed_seconds: float
    commits: tuple[BatchCommit, ...]


@dataclass(frozen=True, slots=True)
class FittedRun:
    """The refit, with the decomposition showing its seam.

    `coefficients.fixed_seconds` is the MEASURED fixed term;
    `regression_intercept_seconds` is the intra-run regression's own
    intercept — an independent estimate of the same quantity. They
    are reported side by side so a wrong decomposition is visible,
    never absorbed into the slope.
    """

    coefficients: RelocationCoefficients
    regression_intercept_seconds: float


@dataclass(frozen=True, slots=True)
class RungPassed:
    rung: LadderRung
    estimate: CutoverEstimate
    measured_seconds: float
    refit: FittedRun


@dataclass(frozen=True, slots=True)
class RungBusted:
    """Measured time exceeded the ceiling: the estimate is disproven
    from above. The ladder stops here."""

    rung: LadderRung
    estimate: CutoverEstimate
    measured_seconds: float


@dataclass(frozen=True, slots=True)
class RungOverpredicted:
    """Measured time fell under the floor: the estimate is disproven
    from below — a structurally over-predicting model whose inflation
    the ceiling alone would never catch. The ladder stops here."""

    rung: LadderRung
    estimate: CutoverEstimate
    measured_seconds: float


def fit_run(run: MeasuredRun) -> FittedRun:
    """Slope by intra-run regression; fixed term carried as measured.

    Least squares of cumulative elapsed seconds against cumulative
    rows over the committed batches. Refuses fewer than two distinct
    commit points — one point cannot separate a slope from an
    intercept, which is the one-observation failure this fit exists
    to prevent.
    """
    if len({commit.cumulative_rows for commit in run.commits}) < 2:
        raise ValueError(
            'the slope regression requires at least two distinct '
            'batch-commit points'
        )
    n = len(run.commits)
    xs = [commit.cumulative_rows / 1_000_000 for commit in run.commits]
    ys = [commit.elapsed_seconds for commit in run.commits]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denominator = sum((x - mean_x) ** 2 for x in xs)
    slope = (
        sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        / denominator
    )
    intercept = mean_y - slope * mean_x
    return FittedRun(
        coefficients=RelocationCoefficients(
            seconds_per_million_rows=slope,
            fixed_seconds=run.fixed_seconds,
        ),
        regression_intercept_seconds=intercept,
    )


def evaluate_rung(
    rung: LadderRung,
    *,
    coefficients: RelocationCoefficients,
    measured: MeasuredRun,
) -> RungPassed | RungBusted | RungOverpredicted:
    """Judge one measured rung against BOTH declared bounds."""
    estimate = estimate_relocation(coefficients, rows=rung.rows)
    if measured.seconds > estimate.ceiling_seconds:
        return RungBusted(
            rung=rung,
            estimate=estimate,
            measured_seconds=measured.seconds,
        )
    floor_seconds = (
        estimate.estimated_seconds
        * RUNG_FLOOR_NUMERATOR
        / RUNG_FLOOR_DENOMINATOR
    )
    if measured.seconds < floor_seconds:
        return RungOverpredicted(
            rung=rung,
            estimate=estimate,
            measured_seconds=measured.seconds,
        )
    return RungPassed(
        rung=rung,
        estimate=estimate,
        measured_seconds=measured.seconds,
        refit=fit_run(measured),
    )
