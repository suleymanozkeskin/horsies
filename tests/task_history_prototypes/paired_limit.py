"""Judging a paired row against its limit, and saying which arm decided it.

Group A's limits are two-armed: ``max(+5%, +0.20 ms)`` allows whichever of the
two is larger. Which one that is depends entirely on how fast the operation
is, and at these latencies it is not the percentage. Five per cent of a 1.6 ms
enqueue is 0.082 ms, well under the 0.20 ms arm, so the absolute arm is the
allowance and the percentage arm never binds.

That matters because the two arms are not equally resolvable here. Across
repeated passes the baseline's own p50 moves by more than five per cent, so a
verdict resting on the percentage arm would be reporting the machine. The
absolute arm is far wider than that movement, so it is decidable. A row that
does not say which arm governed leaves the reader to assume the resolvable one
was used.

**Confirm on repeat.** One pass is a reading, not a result. A row is quotable
only when at least two full passes agree on its verdict, and every pass is
kept — a first reading is never erased by a second, and passes that disagree
are recorded as a disagreement rather than resolved by preferring one.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from .paired_cell import SampleUnit


class LimitError(Exception):
    """A row cannot be judged as stated."""


class LimitArm(StrEnum):
    """Which half of a two-armed limit supplied the allowance."""

    RELATIVE = 'relative'
    ABSOLUTE = 'absolute'


class ControlKind(StrEnum):
    """What a row is measured against.

    Nine of group A's ten rows compare the candidate build against the released
    baseline. Keyed enqueue compares keyed against *ordinary* enqueue on the
    same build — a different control from its neighbours, and so the one most
    likely to be wired against the wrong thing.
    """

    BASELINE_BUILD = 'baseline-build'
    SIBLING_OPERATION = 'sibling-operation'


@dataclass(frozen=True, slots=True)
class TwoArmedLimit:
    """``max(relative x control, absolute)``, as the budget states it."""

    statistic: str
    relative: float
    absolute: float
    unit: SampleUnit
    control: ControlKind

    def __post_init__(self) -> None:
        if self.relative <= 0.0:
            raise LimitError(
                f'{self.statistic}: the relative arm must be positive, got '
                f'{self.relative}'
            )
        if self.absolute <= 0.0:
            raise LimitError(
                f'{self.statistic}: the absolute arm must be positive, got '
                f'{self.absolute}'
            )

    def allowance(self, control_value: float) -> tuple[float, LimitArm]:
        """The allowance and the arm that supplied it.

        Ties go to the absolute arm: at the crossover the two allowances are
        the same number, and naming the arm that stays resolvable as the
        operation gets faster describes the row more usefully.
        """
        relative_allowance = self.relative * control_value
        if relative_allowance > self.absolute:
            return relative_allowance, LimitArm.RELATIVE
        return self.absolute, LimitArm.ABSOLUTE

    def as_conditions(self) -> dict[str, Any]:
        return {
            'statistic': self.statistic,
            'relative': self.relative,
            'absolute': self.absolute,
            'unit': self.unit.value,
            'control': self.control.value,
        }


@dataclass(frozen=True, slots=True)
class PassReading:
    """One pass's reading of one statistic. Never overwritten by a later pass."""

    pass_index: int
    control_value: float
    candidate_value: float
    limit: TwoArmedLimit

    @property
    def delta(self) -> float:
        return self.candidate_value - self.control_value

    @property
    def relative_delta(self) -> float:
        if self.control_value == 0.0:
            raise LimitError(
                'the control measured zero, so a relative delta is undefined'
            )
        return self.delta / self.control_value

    @property
    def allowance(self) -> float:
        return self.limit.allowance(self.control_value)[0]

    @property
    def governing_arm(self) -> LimitArm:
        return self.limit.allowance(self.control_value)[1]

    @property
    def within_limit(self) -> bool:
        return self.delta <= self.allowance

    def as_conditions(self) -> dict[str, Any]:
        """Everything a reader needs to check the verdict rather than trust it.

        Both arms are recorded, not only the one that governed, so the reader
        can see how far from the crossover the row sat.
        """
        return {
            'pass': self.pass_index,
            'control': self.control_value,
            'candidate': self.candidate_value,
            'delta': self.delta,
            'relative_delta': self.relative_delta,
            'relative_arm_allowance': self.limit.relative * self.control_value,
            'absolute_arm_allowance': self.limit.absolute,
            'governing_arm': self.governing_arm.value,
            'allowance': self.allowance,
            'within_limit': self.within_limit,
        }


class RowOutcome(StrEnum):
    """What a set of passes says about a row."""

    CONFIRMED_WITHIN = 'confirmed-within'
    CONFIRMED_EXCEEDED = 'confirmed-exceeded'
    DISAGREED = 'disagreed'


@dataclass(frozen=True, slots=True)
class ConfirmedRow:
    """One budget row, judged over every pass that was run.

    Two passes minimum. A single pass is a reading: the instrument's own
    between-pass movement at these latencies is comparable to the relative arm,
    so one number cannot separate the build from the machine.
    """

    name: str
    limit: TwoArmedLimit
    readings: tuple[PassReading, ...]

    def __post_init__(self) -> None:
        if len(self.readings) < 2:
            raise LimitError(
                f'{self.name}: {len(self.readings)} pass(es); a row is quotable '
                'only on at least two, because one reading cannot separate the '
                'build from the machine'
            )
        for position, reading in enumerate(self.readings):
            if reading.pass_index != position:
                raise LimitError(
                    f'{self.name}: reading at position {position} reports pass '
                    f'{reading.pass_index}; passes are recorded in the order '
                    'they ran and none is dropped'
                )
            if reading.limit != self.limit:
                raise LimitError(
                    f'{self.name}: pass {position} was judged against a '
                    'different limit from the row it belongs to'
                )

    @property
    def outcome(self) -> RowOutcome:
        verdicts = {reading.within_limit for reading in self.readings}
        match verdicts:
            case s if s == {True}:
                return RowOutcome.CONFIRMED_WITHIN
            case s if s == {False}:
                return RowOutcome.CONFIRMED_EXCEEDED
            case _:
                return RowOutcome.DISAGREED

    @property
    def quotable(self) -> bool:
        """A disagreement is a result about the instrument, not about the build."""
        return self.outcome is not RowOutcome.DISAGREED

    def as_conditions(self) -> dict[str, Any]:
        arms = {reading.governing_arm for reading in self.readings}
        return {
            'row': self.name,
            'limit': self.limit.as_conditions(),
            'outcome': self.outcome.value,
            'quotable': self.quotable,
            'passes': [reading.as_conditions() for reading in self.readings],
            'governing_arm': (
                arms.pop().value
                if len(arms) == 1
                else sorted(arm.value for arm in arms)
            ),
            'sign_flipped': len(
                {reading.delta >= 0.0 for reading in self.readings}
            )
            > 1,
        }


def percentile(values: Sequence[float], fraction: float) -> float:
    """Nearest-rank percentile, and a refusal when it would be the maximum.

    A percentile read from a sample too small to contain it is the maximum
    wearing a percentile's name: one noise event sets it, and it moves by an
    order of magnitude between passes. Measured on this harness, p99 over 50
    observations per side ranged 13-24 ms while the same cell at 4,000 per side
    held 2.427-2.612 ms.
    """
    if not values:
        raise LimitError('a percentile needs at least one observation')
    if not 0.0 < fraction < 1.0:
        raise LimitError(f'fraction must be in (0, 1), got {fraction}')
    ordered = sorted(values)
    rank = int(round(fraction * (len(ordered) - 1)))
    if rank >= len(ordered) - 1 and fraction < 1.0:
        raise LimitError(
            f'p{fraction * 100:g} over {len(ordered)} observations is the '
            'maximum of the sample, not a percentile; one outlier sets it and '
            'it moves by an order of magnitude between passes'
        )
    return ordered[rank]


def median(values: Sequence[float]) -> float:
    if not values:
        raise LimitError('a median needs at least one observation')
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


# Section 2.1: bootstrap comparisons use at least 1,000 resamples and a
# recorded seed. Both are properties of the result, not of the run that
# produced it: a reader who has the seed and the samples gets the same interval
# back, and a reader who has neither is being asked to trust an interval.
BOOTSTRAP_RESAMPLES: Final = 1_000
BOOTSTRAP_SEED: Final = 20260808


@dataclass(frozen=True, slots=True)
class BootstrapInterval:
    """A percentile interval for the paired delta, with what produced it.

    The two sides are resampled INDEPENDENTLY, because they are independent
    samples: the ordering pairs them in position, not observation by
    observation, so there is no pairing to preserve when resampling.
    """

    statistic: str
    point: float
    low: float
    high: float
    confidence: float
    resamples: int
    seed: int

    @property
    def excludes_zero(self) -> bool:
        """Whether the interval says a difference exists at all."""
        return self.low > 0.0 or self.high < 0.0

    def as_conditions(self) -> dict[str, Any]:
        return {
            'statistic': self.statistic,
            'point': self.point,
            'low': self.low,
            'high': self.high,
            'confidence': self.confidence,
            'resamples': self.resamples,
            'seed': self.seed,
            'excludes_zero': self.excludes_zero,
        }


def _statistic_of(values: Sequence[float], statistic: str) -> float:
    match statistic:
        case 'p50':
            return median(values)
        case 'p99':
            return percentile(values, 0.99)
        case _:
            raise LimitError(f'no bootstrap defined for statistic {statistic}')


def bootstrap_delta(
    control: Sequence[float],
    candidate: Sequence[float],
    *,
    statistic: str,
    confidence: float = 0.95,
    resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = BOOTSTRAP_SEED,
) -> BootstrapInterval:
    """Resample both sides to put an interval around the delta.

    A point delta says where the two sides landed; it does not say how much of
    that is the sample. The interval is what lets a reader see whether a delta
    inside its limit is a delta at all.
    """
    if resamples < BOOTSTRAP_RESAMPLES:
        raise LimitError(
            f'{resamples} resamples, the mode requires at least '
            f'{BOOTSTRAP_RESAMPLES}'
        )
    if not 0.0 < confidence < 1.0:
        raise LimitError(f'confidence must be in (0, 1), got {confidence}')
    if not control or not candidate:
        raise LimitError('both sides need observations to be resampled')
    generator = random.Random(seed)
    deltas: list[float] = []
    control_size = len(control)
    candidate_size = len(candidate)
    for _ in range(resamples):
        control_sample = [
            control[generator.randrange(control_size)]
            for _ in range(control_size)
        ]
        candidate_sample = [
            candidate[generator.randrange(candidate_size)]
            for _ in range(candidate_size)
        ]
        deltas.append(
            _statistic_of(candidate_sample, statistic)
            - _statistic_of(control_sample, statistic)
        )
    deltas.sort()
    tail = (1.0 - confidence) / 2.0
    low_index = int(round(tail * (resamples - 1)))
    high_index = int(round((1.0 - tail) * (resamples - 1)))
    return BootstrapInterval(
        statistic=statistic,
        point=_statistic_of(candidate, statistic)
        - _statistic_of(control, statistic),
        low=deltas[low_index],
        high=deltas[high_index],
        confidence=confidence,
        resamples=resamples,
        seed=seed,
    )


class IntervalVerdict(StrEnum):
    """What an interval says about the bound it was judged against."""

    WITHIN = 'within'
    EXCEEDED = 'exceeded'
    UNDECIDED = 'undecided'


def judge_containment(
    interval: BootstrapInterval,
    *,
    bound: float,
) -> IntervalVerdict:
    """Decide a one-sided budget by where the WHOLE interval falls.

    A point estimate below the bound is not a verdict: the interval it came
    from may reach well past the bound, in which case the run did not measure
    the answer. The interval decides only when every value it admits agrees.
    Straddling the bound is not a near miss to be rounded — it is the run
    saying it cannot tell, and it is recorded as such.

    This gate carries no threshold and needs no derivation. Applied to the four
    no-blocking intervals it reproduces every verdict reached by hand,
    including the p99 at 100 observations per arm that was withheld as
    unconstrained.
    """
    if bound <= 0.0:
        raise LimitError(
            f'a one-sided budget bound must be positive, got {bound}'
        )
    match (interval.high < bound, interval.low > bound):
        case (True, False):
            return IntervalVerdict.WITHIN
        case (False, True):
            return IntervalVerdict.EXCEEDED
        case _:
            return IntervalVerdict.UNDECIDED


# An interval wider than half its bound cannot fix a magnitude at the
# resolution the question is asked at.
#
# Sited on sixteen measured ratios rather than chosen. Sorted, they run
# 0.039 0.096 0.113 0.147 0.235 0.266 0.412 | 0.860 0.902 0.925 1.186 1.292
# 2.635 3.178 12.388 18.193. The widest empty band in the region that matters
# is (0.412, 0.860) — a factor of 2.1 holding no observation — and 0.50 sits
# inside it with nothing within 19% either side, so the threshold is stable
# against re-measurement.
#
# 0.25 was the intended number and the distribution refused it: the gap from
# 0.235 to 0.266 is thirteen per cent, so a threshold there sits inside a
# cluster and flips on noise. It would refuse two more rows — but those two
# are thin-population rows whose defect is COUNT, not width, and refusing them
# here would be the right refusal for the wrong reason, leaving the coverage
# gap open while the width gate appeared to cover it. Each guard refuses for
# its own reason; the count floor is separate and still to be derived.
#
# 0.50 refuses the specimen that motivated the gate: a p99 at 0.86x that
# decided WITHIN while unable to resolve its own delta's sign.
WIDTH_THRESHOLD_RATIO: Final[float] = 0.50


@dataclass(frozen=True, slots=True)
class MagnitudeJudgement:
    """Whether the delta may be quoted as a NUMBER, apart from its verdict.

    A verdict and a magnitude are separate claims resting on the same
    interval. ``high < bound`` can settle 'within budget' while the interval
    still spans zero — the budget question is answered and the size of the
    effect is not. Quoting the point estimate there reports a number whose
    sign the run never established.
    """

    sign_resolved: bool
    width: float
    width_ratio: float
    width_threshold: float | None
    width_within_threshold: bool

    @property
    def quotable(self) -> bool:
        return self.sign_resolved and self.width_within_threshold

    def as_conditions(self) -> dict[str, Any]:
        return {
            'quotable': self.quotable,
            'sign_resolved': self.sign_resolved,
            'width': self.width,
            'width_ratio': self.width_ratio,
            'width_threshold': self.width_threshold,
            'width_enforced': self.width_threshold is not None,
            'width_within_threshold': self.width_within_threshold,
        }


def judge_magnitude(
    interval: BootstrapInterval,
    *,
    bound: float,
    width_threshold: float | None = WIDTH_THRESHOLD_RATIO,
) -> MagnitudeJudgement:
    """Judge the delta's quotability as a number.

    Two conditions, one of them derived and one not. Sign resolution is
    thresholdless and enforced now: an interval containing zero has not
    established that the delta has a direction. The width ratio — interval
    width over the bound the row is judged by, so that the same width is fatal
    for a 100 ms bound and irrelevant for a 5 s one — is recorded always and
    enforced only once ``width_threshold`` carries a derived number. Supplying
    one here enforces it immediately; nothing further has to be wired.
    """
    if bound <= 0.0:
        raise LimitError(
            f'a one-sided budget bound must be positive, got {bound}'
        )
    width = interval.high - interval.low
    match width_threshold:
        case None:
            within = True
        case threshold if threshold <= 0.0:
            raise LimitError(
                f'a width threshold must be positive, got {threshold}'
            )
        case threshold:
            within = width / bound <= threshold
    return MagnitudeJudgement(
        sign_resolved=interval.excludes_zero,
        width=width,
        width_ratio=width / bound,
        width_threshold=width_threshold,
        width_within_threshold=within,
    )
