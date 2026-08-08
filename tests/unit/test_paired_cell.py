"""A cell cannot be built without saying which builds it compared."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_cell import (
    EquivalenceError,
    EquivalenceFacts,
    assert_equivalent_inputs,
    paired_cell,
)
from tests.task_history_prototypes.paired_sides import (
    BASELINE_SCHEMA_VERSION,
    CANDIDATE_SCHEMA_VERSION,
    PairedSide,
    SideIdentity,
    SideIdentityError,
)

VENV = '/tmp/v047'
CHECKOUT = '/repo'


def _identity(side: PairedSide) -> SideIdentity:
    if side is PairedSide.BASELINE:
        return SideIdentity(
            side=side,
            interpreter=f'{VENV}/bin/python',
            module_path=f'{VENV}/lib/horsies/__init__.py',
            schema_version=BASELINE_SCHEMA_VERSION,
            expected_root=VENV,
            expected_schema_version=BASELINE_SCHEMA_VERSION,
        )
    return SideIdentity(
        side=side,
        interpreter=f'{CHECKOUT}/.venv/bin/python',
        module_path=f'{CHECKOUT}/horsies/__init__.py',
        schema_version=CANDIDATE_SCHEMA_VERSION,
        expected_root=CHECKOUT,
        expected_schema_version=CANDIDATE_SCHEMA_VERSION,
    )


def _facts(side: PairedSide, *, rows: int = 1000) -> EquivalenceFacts:
    return EquivalenceFacts(
        side=side,
        rows=rows,
        payload_bytes_total=200 * rows,
        payload_size_histogram=((200, rows),),
        status_mix=(('COMPLETED', rows),),
    )


def _cell(**overrides: object):
    arguments: dict[str, object] = {
        'baseline_identity': _identity(PairedSide.BASELINE),
        'candidate_identity': _identity(PairedSide.CANDIDATE),
        'baseline_equivalence': _facts(PairedSide.BASELINE),
        'candidate_equivalence': _facts(PairedSide.CANDIDATE),
        'baseline_samples': (1.0, 1.1),
        'candidate_samples': (1.0, 1.2),
    }
    arguments.update(overrides)
    return paired_cell('claim', **arguments)  # type: ignore[arg-type]


def test_a_well_formed_cell_records_both_builds() -> None:
    cell = _cell()
    conditions = cell.conditions()
    assert conditions['sides']['baseline']['schema_version'] == (
        BASELINE_SCHEMA_VERSION
    )
    assert conditions['sides']['candidate']['schema_version'] == (
        CANDIDATE_SCHEMA_VERSION
    )
    assert conditions['equivalence']['baseline']['rows'] == 1000
    assert conditions['observations']['candidate'] == 2


def test_a_shadowed_side_cannot_reach_a_cell() -> None:
    """The identity check is not a step the caller may skip."""
    shadowed = SideIdentity(
        side=PairedSide.BASELINE,
        interpreter=f'{VENV}/bin/python',
        module_path=f'{CHECKOUT}/horsies/__init__.py',
        schema_version=CANDIDATE_SCHEMA_VERSION,
        expected_root=VENV,
        expected_schema_version=BASELINE_SCHEMA_VERSION,
    )
    with pytest.raises(SideIdentityError):
        _cell(baseline_identity=shadowed)


def test_unequal_work_cannot_reach_a_cell() -> None:
    with pytest.raises(EquivalenceError, match='row counts differ'):
        _cell(candidate_equivalence=_facts(PairedSide.CANDIDATE, rows=999))


def test_equal_totals_with_different_shapes_are_refused() -> None:
    """Equal byte totals can hide different payload shapes."""
    skewed = EquivalenceFacts(
        side=PairedSide.CANDIDATE,
        rows=1000,
        payload_bytes_total=200 * 1000,
        payload_size_histogram=((100, 500), (300, 500)),
        status_mix=(('COMPLETED', 1000),),
    )
    with pytest.raises(EquivalenceError, match='size distributions differ'):
        _cell(candidate_equivalence=skewed)


def test_swapped_sides_are_refused() -> None:
    with pytest.raises(EquivalenceError, match='reports side'):
        _cell(baseline_identity=_identity(PairedSide.CANDIDATE))


def test_an_empty_side_is_refused() -> None:
    with pytest.raises(EquivalenceError, match='no observations'):
        _cell(candidate_samples=())


def test_equivalence_compares_inputs_not_storage() -> None:
    """Different schema versions are expected; different inputs are not."""
    assert_equivalent_inputs(
        _facts(PairedSide.BASELINE), _facts(PairedSide.CANDIDATE)
    )
