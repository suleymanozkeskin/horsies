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

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

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
