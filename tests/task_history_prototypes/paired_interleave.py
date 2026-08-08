"""The order the two sides are observed in, and what that order costs.

Interleaving exists for one reason: a machine drifts during a window — caches
fill, another process wakes, the disk gets busy — and the drift must land on
both sides rather than on one. Running every baseline observation and then
every candidate observation produces a clean-looking result in which the whole
drift has been renamed "the build".

**The order is judged by the gap it leaves, not by looking interleaved.** For a
drift that grows monotonically with observation order, the delta it induces
between the sides is the drift rate multiplied by the difference between the
two sides' mean positions in that order. That difference is a property of the
schedule, computable before anything runs, so it is computed, asserted against
the schedule that was actually built, and recorded with the cell:

    induced delta  =  drift rate  ×  mean-position gap

Sequential ordering puts the gap at half the run. Simple alternation leaves a
gap of exactly one block. Counterbalanced ordering — ABBA, each pair of blocks
mirrored — leaves a gap of exactly zero, which is why it is the default here:
it removes the residual rather than declaring it small.

Warm-up blocks are discarded whole. Discarding a partial unit would reintroduce
the gap the ordering was chosen to remove, so the count is required to be a
whole number of units.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from .paired_sides import PairedSide


class InterleaveError(Exception):
    """The observation order would attribute drift to the build."""


class BlockOrder(StrEnum):
    """How the blocks of the two sides are laid out.

    ``ALTERNATING`` is A B A B. ``COUNTERBALANCED`` is A B B A, repeated: each
    pair of blocks is mirrored, so a side that ran early in one unit runs late
    in the next and a monotonic drift cancels exactly.
    """

    ALTERNATING = 'alternating'
    COUNTERBALANCED = 'counterbalanced'


def _unit_blocks(order: BlockOrder) -> int:
    """How many blocks make one repeatable, balanced unit of this order."""
    match order:
        case BlockOrder.ALTERNATING:
            return 2
        case BlockOrder.COUNTERBALANCED:
            return 4


def _other(side: PairedSide) -> PairedSide:
    match side:
        case PairedSide.BASELINE:
            return PairedSide.CANDIDATE
        case PairedSide.CANDIDATE:
            return PairedSide.BASELINE


@dataclass(frozen=True, slots=True)
class InterleaveSpec:
    """The block structure, declared before the window opens.

    Recorded with the cell in full: a reader who knows the block structure can
    recompute the drift bound, and one who does not cannot judge the number.
    """

    blocks: int
    block_size: int
    warmup_blocks: int
    order: BlockOrder = BlockOrder.COUNTERBALANCED
    first: PairedSide = PairedSide.BASELINE

    def __post_init__(self) -> None:
        unit = _unit_blocks(self.order)
        if self.block_size < 1:
            raise InterleaveError(
                f'block_size must be at least 1, got {self.block_size}'
            )
        if self.warmup_blocks < 1:
            raise InterleaveError(
                'warmup_blocks must be at least one unit; ordering and warm-up '
                'discard answer different failures — ordering cancels a drift '
                'that grows with position, warm-up removes a transient that is '
                'neither fixed-length nor linear — and neither substitutes for '
                'the other'
            )
        if self.blocks < unit or self.blocks % unit:
            raise InterleaveError(
                f'{self.order} needs a whole number of {unit}-block units, '
                f'got {self.blocks} blocks; a partial unit leaves one side '
                'sitting later in the run than the other, which is the '
                'asymmetry the ordering exists to remove'
            )
        if self.warmup_blocks < 0 or self.warmup_blocks % unit:
            raise InterleaveError(
                f'warmup_blocks must be a whole number of {unit}-block units, '
                f'got {self.warmup_blocks}; discarding a partial unit '
                'reintroduces the position gap into the measured half'
            )
        if self.warmup_blocks >= self.blocks:
            raise InterleaveError(
                f'warmup_blocks {self.warmup_blocks} leaves no measured '
                f'blocks out of {self.blocks}'
            )

    def measured_blocks(self) -> int:
        return self.blocks - self.warmup_blocks

    def measured_observations_per_side(self) -> int:
        return self.measured_blocks() * self.block_size // 2

    def as_conditions(self) -> dict[str, Any]:
        return {
            'blocks': self.blocks,
            'block_size': self.block_size,
            'warmup_blocks': self.warmup_blocks,
            'order': self.order.value,
            'first': self.first.value,
        }


@dataclass(frozen=True, slots=True)
class ScheduledObservation:
    """One observation's place in the run."""

    global_index: int
    block: int
    side: PairedSide
    warmup: bool


def block_sides(spec: InterleaveSpec) -> tuple[PairedSide, ...]:
    """The side each block belongs to, in order."""
    first = spec.first
    second = _other(first)
    match spec.order:
        case BlockOrder.ALTERNATING:
            unit = (first, second)
        case BlockOrder.COUNTERBALANCED:
            unit = (first, second, second, first)
    repeats = spec.blocks // len(unit)
    return tuple(side for _ in range(repeats) for side in unit)


def interleave_schedule(spec: InterleaveSpec) -> tuple[ScheduledObservation, ...]:
    """Every observation the window will take, in the order it takes them."""
    schedule: list[ScheduledObservation] = []
    for block_index, side in enumerate(block_sides(spec)):
        for _ in range(spec.block_size):
            schedule.append(
                ScheduledObservation(
                    global_index=len(schedule),
                    block=block_index,
                    side=side,
                    warmup=block_index < spec.warmup_blocks,
                )
            )
    return tuple(schedule)


def measured(
    schedule: Sequence[ScheduledObservation],
) -> tuple[ScheduledObservation, ...]:
    """The observations that count, warm-up removed."""
    return tuple(entry for entry in schedule if not entry.warmup)


def mean_position_gap(schedule: Sequence[ScheduledObservation]) -> float:
    """Candidate mean position minus baseline mean position, over measured work.

    This is the whole quantity the ordering controls. Multiplied by a drift
    rate it gives the delta that drift alone would produce, so a schedule with
    a gap of zero cannot manufacture a delta out of a monotonic drift no
    matter how steep that drift is.
    """
    counted = measured(schedule)
    positions: dict[PairedSide, list[int]] = {
        PairedSide.BASELINE: [],
        PairedSide.CANDIDATE: [],
    }
    for entry in counted:
        positions[entry.side].append(entry.global_index)
    for side, values in positions.items():
        if not values:
            raise InterleaveError(
                f'the measured half of the schedule contains no {side} '
                'observations, so there is nothing to compare against'
            )
    baseline_mean = sum(positions[PairedSide.BASELINE]) / len(
        positions[PairedSide.BASELINE]
    )
    candidate_mean = sum(positions[PairedSide.CANDIDATE]) / len(
        positions[PairedSide.CANDIDATE]
    )
    return candidate_mean - baseline_mean


def expected_position_gap(spec: InterleaveSpec) -> float:
    """What the gap must be if the schedule is the one this spec describes.

    Derived from the ordering rather than measured from the schedule, so the
    two can be compared: a schedule whose gap does not match its own model is
    not the schedule anybody reasoned about.
    """
    match spec.order:
        case BlockOrder.ALTERNATING:
            sign = 1.0 if spec.first is PairedSide.BASELINE else -1.0
            return sign * float(spec.block_size)
        case BlockOrder.COUNTERBALANCED:
            return 0.0


def assert_schedule_matches_its_model(
    spec: InterleaveSpec, schedule: Sequence[ScheduledObservation]
) -> None:
    """The built schedule is the one the spec describes.

    Checked to a tolerance well below one observation: the gaps here are exact
    integers or exactly zero by construction, so anything in between means the
    schedule was built by something other than this spec.
    """
    counted = measured(schedule)
    per_side = spec.measured_observations_per_side()
    for side in (PairedSide.BASELINE, PairedSide.CANDIDATE):
        observed = sum(1 for entry in counted if entry.side is side)
        if observed != per_side:
            raise InterleaveError(
                f'the measured half gives {side} {observed} observations, the '
                f'spec describes {per_side}; unequal sides mean one build was '
                'sampled more thoroughly than the other'
            )
    expected = expected_position_gap(spec)
    actual = mean_position_gap(schedule)
    if abs(actual - expected) > 1e-9:
        raise InterleaveError(
            f'the schedule mean-position gap is {actual}, its spec implies '
            f'{expected}; the run order is not the one the drift bound was '
            'computed from'
        )


def permitted_run_observations(spec: InterleaveSpec) -> int:
    """The longest stretch one side may hold, in observations.

    Counted in observations rather than in blocks. A block is whatever the
    schedule says it is, so a sequential run written as one oversized block per
    side has a block run-length of one and would satisfy any block-counting
    check while being the exact failure that check exists for.
    """
    match spec.order:
        case BlockOrder.ALTERNATING:
            return spec.block_size
        case BlockOrder.COUNTERBALANCED:
            return spec.block_size * 2


def assert_no_long_runs(
    spec: InterleaveSpec, schedule: Sequence[ScheduledObservation]
) -> None:
    """No side may hold the machine longer than its ordering allows.

    The failure this exists for is the whole of one side followed by the whole
    of the other. That schedule looks orderly, runs without complaint, and
    hands every drift in the window to the build.
    """
    maximum = permitted_run_observations(spec)
    run_side: PairedSide | None = None
    run_length = 0
    for entry in schedule:
        if entry.side is run_side:
            run_length += 1
        else:
            run_side, run_length = entry.side, 1
        if run_length > maximum:
            raise InterleaveError(
                f'{entry.side} holds {run_length} consecutive observations, '
                f'more than the {maximum} this ordering permits; a run that '
                'long lets the drift within it be attributed to the build'
            )


# What the bound is a bound under. Named in the conditions because a bound of
# zero, unlabelled, reads as "nothing in the window can decide this cell",
# which is a much larger claim than the one being made.
DRIFT_MODEL: Final = 'monotonic-linear-in-observation-order'

DRIFT_MODEL_EXCLUDES: Final = (
    'a warm-up transient, which is neither fixed-length nor linear and is '
    'answered by discarding warm-up blocks rather than by the ordering'
)


@dataclass(frozen=True, slots=True)
class DriftAttributionBound:
    """How much of a cell's delta the observation order could have produced.

    Carries its model with it. A bare number here would be read as a bound on
    every way a window can move, and it is a bound on exactly one: a drift that
    grows linearly with position in the run. Under any other shape — a warm-up
    transient above all — this says nothing, which is why the warm-up discard
    is a separate and equally mandatory mechanism.
    """

    model: str
    excludes: str
    mean_position_gap: float
    drift_per_observation: float
    bound: float

    def as_conditions(self) -> dict[str, Any]:
        return {
            'model': self.model,
            'excludes': self.excludes,
            'mean_position_gap': self.mean_position_gap,
            'drift_per_observation': self.drift_per_observation,
            'bound': self.bound,
        }


def drift_attribution_bound(
    spec: InterleaveSpec, *, drift_per_observation: float
) -> DriftAttributionBound:
    """The delta a linear drift of this rate could contribute, in sample units.

    Quoted with the cell so a reader can compare it against the limit the cell
    is judged by. A bound larger than the limit means the window, not the
    build, decided the verdict.
    """
    gap = expected_position_gap(spec)
    return DriftAttributionBound(
        model=DRIFT_MODEL,
        excludes=DRIFT_MODEL_EXCLUDES,
        mean_position_gap=gap,
        drift_per_observation=drift_per_observation,
        bound=abs(gap) * abs(drift_per_observation),
    )


def partition_samples(
    schedule: Sequence[ScheduledObservation], samples: Sequence[float]
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    """Split observations back onto their sides, warm-up discarded.

    The samples must be in schedule order and complete. A short or reordered
    sequence would silently pair each side with the other's numbers.
    """
    if len(samples) != len(schedule):
        raise InterleaveError(
            f'{len(samples)} samples for {len(schedule)} scheduled '
            'observations; the samples cannot be placed on their sides'
        )
    baseline: list[float] = []
    candidate: list[float] = []
    for entry, value in zip(schedule, samples, strict=True):
        if entry.warmup:
            continue
        match entry.side:
            case PairedSide.BASELINE:
                baseline.append(value)
            case PairedSide.CANDIDATE:
                candidate.append(value)
    return tuple(baseline), tuple(candidate)


def observed_drift_per_observation(
    schedule: Sequence[ScheduledObservation], samples: Sequence[float]
) -> float:
    """The drift rate the run itself shows, as a least-squares slope.

    Measured rather than assumed, so the bound quoted with a cell is derived
    from that cell's own window. The two sides are pooled: under an ordering
    whose mean-position gap is zero, a genuine build difference contributes
    equally at both ends of the run and does not tilt the fit. Under an
    ordering with a non-zero gap it does tilt it, which inflates the rate and
    therefore the bound — the safe direction.
    """
    if len(samples) != len(schedule):
        raise InterleaveError(
            f'{len(samples)} samples for {len(schedule)} scheduled '
            'observations; the drift rate cannot be fitted'
        )
    points = [
        (float(entry.global_index), value)
        for entry, value in zip(schedule, samples, strict=True)
        if not entry.warmup
    ]
    if len(points) < 2:
        raise InterleaveError(
            'a drift rate needs at least two measured observations'
        )
    mean_index = sum(index for index, _ in points) / len(points)
    mean_value = sum(value for _, value in points) / len(points)
    covariance = sum(
        (index - mean_index) * (value - mean_value) for index, value in points
    )
    variance = sum((index - mean_index) ** 2 for index, _ in points)
    if variance == 0.0:
        raise InterleaveError(
            'every measured observation sits at the same position; there is '
            'no run order to fit a drift against'
        )
    return covariance / variance


def interleave_conditions(
    spec: InterleaveSpec,
    schedule: Sequence[ScheduledObservation],
    *,
    drift_per_observation: float,
) -> dict[str, Any]:
    """What the artifact records about the order the numbers were taken in.

    The drift rate is required rather than optional. A cell that records its
    ordering without the bound that ordering buys leaves the reader to assume
    the bound is small, and the whole point of computing it is that assuming
    is what goes wrong.
    """
    return {
        'spec': spec.as_conditions(),
        'total_observations': len(schedule),
        'discarded_warmup_observations': spec.warmup_blocks * spec.block_size,
        'measured_observations_per_side': spec.measured_observations_per_side(),
        'mean_position_gap': mean_position_gap(schedule),
        'block_sides': [side.value for side in block_sides(spec)],
        'drift_attribution': drift_attribution_bound(
            spec, drift_per_observation=drift_per_observation
        ).as_conditions(),
    }
