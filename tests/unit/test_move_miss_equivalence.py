"""The miss classifier receives full equivalence classes, by derivation.

Every move-family operation delegates its miss to the one classifier
with a rendered text-array of kinds. That list has one owner —
``EQUIVALENCE_CLASSES`` — and is derived at render time; a
hand-written single-member list here once made same-class replays
(locked completion over a fused row) report a foreign conflict instead
of already-applied.
"""

from __future__ import annotations

import re

import pytest

from horsies.core.history.cutover.program import installation_fragments
from horsies.core.lifecycle.operations import (
    TerminalizationKind,
    equivalence_class_of,
)

pytestmark = [pytest.mark.unit]

_ARRAY_PATTERN = re.compile(r"ARRAY\[((?:'[A-Z_]+'(?:,\s*)?)+)\]::text\[\]")


def _rendered_kind_arrays() -> set[tuple[str, ...]]:
    arrays: set[tuple[str, ...]] = set()
    for fragment in installation_fragments():
        for match in _ARRAY_PATTERN.finditer(fragment):
            members = tuple(
                part.strip().strip("'")
                for part in match.group(1).split(',')
            )
            if all(member in TerminalizationKind.__members__ for member in members):
                arrays.add(members)
    return arrays


class TestMissClassifierReceivesFullClasses:
    def test_every_rendered_kind_array_is_a_complete_class(self) -> None:
        """No rendered kind array may be a strict subset of its class."""
        arrays = _rendered_kind_arrays()
        assert arrays, 'no kind arrays rendered — the pattern went stale'
        for members in arrays:
            first = TerminalizationKind(members[0])
            expected = tuple(
                sorted(
                    member.value for member in equivalence_class_of(first)
                )
            )
            assert tuple(sorted(members)) == expected, (
                f'rendered {members} is not the full class of '
                f'{first.value}: {expected}'
            )

    def test_the_multi_member_classes_are_rendered_somewhere(self) -> None:
        """The presence half: the completion pair must actually appear,
        so the subset assertion above cannot pass vacuously."""
        arrays = _rendered_kind_arrays()
        assert ('COMPLETE_FUSED', 'COMPLETE_LOCKED') in {
            tuple(sorted(members)) for members in arrays
        }
