"""A paired cell that cannot exist without saying which builds it compared.

Routing every cell through the identity check is a rule someone has to
remember. This makes it a property of the type instead: a cell result carries
both side identities and both sides' equivalence facts as required fields, so
a cell without them is unrepresentable rather than merely forbidden.

The checks live in ``__post_init__``, so constructing the type directly runs
them too. A helper that validates is discipline wearing a type's clothes: it
holds until someone calls the constructor.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .paired_sides import (
    PairedSide,
    SideIdentity,
    assert_side_identity,
    assert_sides_differ,
    side_conditions,
)

class SampleUnit(StrEnum):
    """What a cell's numbers are.

    Required, because a quoted measurement without a unit is dimensionless
    and a reader supplies the missing one from expectation.
    """

    MILLISECONDS = 'ms'
    SECONDS = 's'
    ROWS_PER_SECOND = 'rows/s'
    BYTES = 'bytes'


class EquivalenceError(Exception):
    """The two sides were not fed equivalent work."""


@dataclass(frozen=True, slots=True)
class EquivalenceFacts:
    """What was actually seeded into one side, measured after seeding.

    Declared rather than assumed: the budget requires equal live working set
    and payload shape, and a requirement nobody measured is a hope.
    """

    side: PairedSide
    rows: int
    payload_bytes_total: int
    payload_size_histogram: tuple[tuple[int, int], ...]
    status_mix: tuple[tuple[str, int], ...]

    def as_conditions(self) -> dict[str, Any]:
        return {
            'rows': self.rows,
            'payload_bytes_total': self.payload_bytes_total,
            'payload_size_histogram': [
                list(entry) for entry in self.payload_size_histogram
            ],
            'status_mix': [list(entry) for entry in self.status_mix],
        }


def assert_equivalent_inputs(
    baseline: EquivalenceFacts, candidate: EquivalenceFacts
) -> None:
    """Equivalence is at the input boundary, never at storage.

    The two sides run different schema versions, so their stored bytes are
    expected to differ — that difference IS the product difference and forcing
    it away would measure something neither build does. What must match is the
    work fed in: the same logical rows, the same payload bytes, the same
    status mix.
    """
    if baseline.rows != candidate.rows:
        raise EquivalenceError(
            f'row counts differ: baseline {baseline.rows}, candidate '
            f'{candidate.rows}. The sides were fed different work, so any '
            'delta between them measures the difference in work'
        )
    if baseline.payload_bytes_total != candidate.payload_bytes_total:
        raise EquivalenceError(
            'payload byte totals differ: baseline '
            f'{baseline.payload_bytes_total}, candidate '
            f'{candidate.payload_bytes_total}'
        )
    if baseline.payload_size_histogram != candidate.payload_size_histogram:
        raise EquivalenceError(
            'payload size distributions differ; equal totals can hide '
            'different shapes, and payload shape is a declared condition'
        )
    if baseline.status_mix != candidate.status_mix:
        raise EquivalenceError(
            f'status mixes differ: baseline {baseline.status_mix}, candidate '
            f'{candidate.status_mix}'
        )


@dataclass(frozen=True, slots=True)
class PairedCell:
    """One paired measurement, with everything needed to trust it.

    Every field is required. A cell missing its identities or its equivalence
    facts cannot be constructed, so the checks cannot be skipped by a caller
    who forgot them.
    """

    name: str
    unit: SampleUnit
    baseline_identity: SideIdentity
    candidate_identity: SideIdentity
    baseline_equivalence: EquivalenceFacts
    candidate_equivalence: EquivalenceFacts
    baseline_samples: tuple[float, ...]
    candidate_samples: tuple[float, ...]

    def __post_init__(self) -> None:
        if self.baseline_identity.side is not PairedSide.BASELINE:
            raise EquivalenceError(
                f'cell {self.name}: the baseline identity reports side '
                f'{self.baseline_identity.side}'
            )
        if self.candidate_identity.side is not PairedSide.CANDIDATE:
            raise EquivalenceError(
                f'cell {self.name}: the candidate identity reports side '
                f'{self.candidate_identity.side}'
            )
        assert_side_identity(self.baseline_identity)
        assert_side_identity(self.candidate_identity)
        assert_sides_differ(self.baseline_identity, self.candidate_identity)
        if self.baseline_equivalence.side is not PairedSide.BASELINE:
            raise EquivalenceError(
                f'cell {self.name}: baseline equivalence facts report side '
                f'{self.baseline_equivalence.side}'
            )
        if self.candidate_equivalence.side is not PairedSide.CANDIDATE:
            raise EquivalenceError(
                f'cell {self.name}: candidate equivalence facts report side '
                f'{self.candidate_equivalence.side}'
            )
        assert_equivalent_inputs(
            self.baseline_equivalence, self.candidate_equivalence
        )
        if not self.baseline_samples or not self.candidate_samples:
            raise EquivalenceError(
                f'cell {self.name}: a side produced no observations; an '
                'empty side yields a delta against nothing'
            )

    def conditions(self) -> dict[str, Any]:
        """Everything a reader needs to judge whether this cell means what it says."""
        return {
            'cell': self.name,
            'unit': self.unit.value,
            'sides': side_conditions(
                self.baseline_identity, self.candidate_identity
            ),
            'equivalence': {
                'baseline': self.baseline_equivalence.as_conditions(),
                'candidate': self.candidate_equivalence.as_conditions(),
            },
            'observations': {
                'baseline': len(self.baseline_samples),
                'candidate': len(self.candidate_samples),
            },
        }


def paired_cell(
    name: str,
    *,
    unit: SampleUnit,
    baseline_identity: SideIdentity,
    candidate_identity: SideIdentity,
    baseline_equivalence: EquivalenceFacts,
    candidate_equivalence: EquivalenceFacts,
    baseline_samples: Sequence[float],
    candidate_samples: Sequence[float],
) -> PairedCell:
    """Documented entrypoint; normalizes the sample sequences.

    Every check lives in the type, so this is a convenience and not the
    enforcement. Constructing `PairedCell` directly is equally safe, which is
    what makes the guarantee a property of the type rather than of the caller
    choosing the polite door.
    """
    return PairedCell(
        name=name,
        unit=unit,
        baseline_identity=baseline_identity,
        candidate_identity=candidate_identity,
        baseline_equivalence=baseline_equivalence,
        candidate_equivalence=candidate_equivalence,
        baseline_samples=tuple(baseline_samples),
        candidate_samples=tuple(candidate_samples),
    )
