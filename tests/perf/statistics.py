"""Turning two sets of timings into a verdict, without assuming a shape.

Latency distributions are right-skewed and heavy-tailed: a handful of samples
land many multiples above the median because a page was cold or a checkpoint
landed. The difference between two such distributions is not normal either, so
a t-interval on the mean would produce a confidence interval that looks precise
and is wrong in a direction nobody can see. The bootstrap makes no distribution
assumption — it resamples the observations actually collected — so the interval
it reports is honest about how little a tail is pinned down.

The verdict rule follows from the interval rather than the point estimate. A
budget is only passed when the whole interval is inside it; an interval that
straddles the budget means the samples cannot tell which side of it the truth
is on, which is inconclusive rather than pass. That asymmetry is deliberate:
absent evidence does not clear a gate.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum


class Verdict(Enum):
    """What a measurement establishes about its budget."""

    PASS = 'PASS'
    FAIL = 'FAIL'
    INCONCLUSIVE = 'INCONCLUSIVE'


@dataclass(frozen=True, slots=True)
class Budget:
    """A tolerance stated as the looser of a fraction and a floor.

    The floor exists because a percentage of a sub-millisecond operation
    measures scheduling noise rather than the operation.
    """

    fraction: float
    floor_ms: float

    def limit_ms(self, baseline_ms: float) -> float:
        return max(baseline_ms * self.fraction, self.floor_ms)


@dataclass(frozen=True, slots=True)
class Comparison:
    """One percentile of one path, measured on both sides."""

    percentile: float
    baseline_ms: float
    candidate_ms: float
    delta_ms: float
    ci_low_ms: float
    ci_high_ms: float
    limit_ms: float
    verdict: Verdict


def percentile_ms(samples: list[float], percentile: float) -> float:
    """Nearest-rank percentile of already-collected samples.

    Nearest-rank rather than interpolated: an interpolated p99 invents a value
    between two observations, and at the tail those neighbours can be far
    apart, so the reported number would be one nothing measured.
    """
    if not samples:
        raise ValueError('percentile of no samples')
    ordered = sorted(samples)
    rank = max(1, min(len(ordered), round(percentile / 100 * len(ordered))))
    return ordered[rank - 1]


def compare(
    *,
    baseline: list[float],
    candidate: list[float],
    percentile: float,
    budget: Budget,
    resamples: int,
    seed: int,
) -> Comparison:
    """Bootstrap the difference of a percentile and judge it against a budget.

    Each side is resampled independently with replacement, because the two
    sides are not paired observation by observation — they are interleaved
    blocks under the same conditions, which pairs them at the block level and
    leaves the observations exchangeable within a side.
    """
    if resamples < 1:
        raise ValueError('bootstrap needs at least one resample')
    baseline_ms = percentile_ms(baseline, percentile)
    candidate_ms = percentile_ms(candidate, percentile)
    limit_ms = budget.limit_ms(baseline_ms)

    rng = random.Random(seed)
    deltas: list[float] = []
    for _ in range(resamples):
        drawn_baseline = rng.choices(baseline, k=len(baseline))
        drawn_candidate = rng.choices(candidate, k=len(candidate))
        deltas.append(
            percentile_ms(drawn_candidate, percentile)
            - percentile_ms(drawn_baseline, percentile)
        )
    deltas.sort()
    ci_low_ms = deltas[max(0, round(0.025 * len(deltas)) - 1)]
    ci_high_ms = deltas[min(len(deltas) - 1, round(0.975 * len(deltas)) - 1)]

    return Comparison(
        percentile=percentile,
        baseline_ms=baseline_ms,
        candidate_ms=candidate_ms,
        delta_ms=candidate_ms - baseline_ms,
        ci_low_ms=ci_low_ms,
        ci_high_ms=ci_high_ms,
        limit_ms=limit_ms,
        verdict=_verdict(ci_low_ms, ci_high_ms, limit_ms),
    )


def _verdict(ci_low_ms: float, ci_high_ms: float, limit_ms: float) -> Verdict:
    """Pass on the whole interval, fail on the whole interval, else neither."""
    if ci_high_ms <= limit_ms:
        return Verdict.PASS
    if ci_low_ms > limit_ms:
        return Verdict.FAIL
    return Verdict.INCONCLUSIVE


def worst(verdicts: list[Verdict]) -> Verdict:
    """The verdict a run reports overall.

    A failure anywhere is a failure; otherwise an inconclusive anywhere leaves
    the whole run inconclusive, since the run has not established what it set
    out to establish.
    """
    if Verdict.FAIL in verdicts:
        return Verdict.FAIL
    if Verdict.INCONCLUSIVE in verdicts:
        return Verdict.INCONCLUSIVE
    return Verdict.PASS
