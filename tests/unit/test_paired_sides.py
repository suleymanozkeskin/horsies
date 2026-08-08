"""A paired cell must prove it compared two builds, not one build twice."""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.paired_sides import (
    BASELINE_SCHEMA_VERSION,
    CANDIDATE_SCHEMA_VERSION,
    PairedSide,
    SideIdentity,
    SideIdentityError,
    assert_side_identity,
    assert_sides_differ,
    side_conditions,
)

VENV = '/tmp/v047'
CHECKOUT = '/repo'


def _baseline(
    *, module_path: str = f'{VENV}/lib/python3.13/site-packages/horsies/__init__.py',
    schema_version: int = BASELINE_SCHEMA_VERSION,
) -> SideIdentity:
    return SideIdentity(
        side=PairedSide.BASELINE,
        interpreter=f'{VENV}/bin/python',
        module_path=module_path,
        schema_version=schema_version,
        expected_root=VENV,
        expected_schema_version=BASELINE_SCHEMA_VERSION,
    )


def _candidate(
    *, module_path: str = f'{CHECKOUT}/horsies/__init__.py',
    schema_version: int = CANDIDATE_SCHEMA_VERSION,
) -> SideIdentity:
    return SideIdentity(
        side=PairedSide.CANDIDATE,
        interpreter=f'{CHECKOUT}/.venv/bin/python',
        module_path=module_path,
        schema_version=schema_version,
        expected_root=CHECKOUT,
        expected_schema_version=CANDIDATE_SCHEMA_VERSION,
    )


def test_correctly_resolved_sides_are_accepted() -> None:
    baseline, candidate = _baseline(), _candidate()
    assert_side_identity(baseline)
    assert_side_identity(candidate)
    assert_sides_differ(baseline, candidate)


def test_shadowed_baseline_is_refused() -> None:
    """The measured trap: the baseline imports the checkout.

    Reproduced from a real probe — a 0.4.7 interpreter invoked from the
    repository root without `PYTHONSAFEPATH` imports the checkout's package
    and reports the checkout's schema version, while its distribution
    metadata still says 0.4.7.
    """
    with pytest.raises(SideIdentityError, match='not under'):
        assert_side_identity(
            _baseline(
                module_path=f'{CHECKOUT}/horsies/__init__.py',
                schema_version=CANDIDATE_SCHEMA_VERSION,
            )
        )


def test_shadowed_candidate_is_refused() -> None:
    """The symmetric failure, which is exactly as silent.

    A guard that only watches the direction its author thought of is a guard
    against that direction.
    """
    with pytest.raises(SideIdentityError, match='not under'):
        assert_side_identity(
            _candidate(
                module_path=(
                    f'{VENV}/lib/python3.13/site-packages/horsies/__init__.py'
                ),
                schema_version=BASELINE_SCHEMA_VERSION,
            )
        )


@pytest.mark.parametrize('side', list(PairedSide))
def test_right_path_wrong_build_is_refused(side: PairedSide) -> None:
    """The path can be right while the build is not.

    Two builds can occupy similarly-shaped paths, so the path alone does not
    identify a build; only one of them declares each schema version.
    """
    identity = (
        _baseline(schema_version=99)
        if side is PairedSide.BASELINE
        else _candidate(schema_version=99)
    )
    with pytest.raises(SideIdentityError, match='schema version'):
        assert_side_identity(identity)


def test_both_sides_resolving_to_one_build_is_refused() -> None:
    """Each half can pass while the pair is still one build twice.

    If both expected roots were configured to the same place, both sides
    satisfy their own assertion and the comparison measures a build against
    itself — deltas near zero, every limit passed.
    """
    same = f'{CHECKOUT}/horsies/__init__.py'
    baseline = SideIdentity(
        side=PairedSide.BASELINE,
        interpreter=f'{CHECKOUT}/.venv/bin/python',
        module_path=same,
        schema_version=CANDIDATE_SCHEMA_VERSION,
        expected_root=CHECKOUT,
        expected_schema_version=CANDIDATE_SCHEMA_VERSION,
    )
    candidate = _candidate()
    assert_side_identity(baseline)
    assert_side_identity(candidate)
    with pytest.raises(SideIdentityError, match='same module'):
        assert_sides_differ(baseline, candidate)


def test_equal_schema_versions_are_refused() -> None:
    with pytest.raises(SideIdentityError, match='both sides declare'):
        assert_sides_differ(
            _baseline(schema_version=CANDIDATE_SCHEMA_VERSION),
            _candidate(),
        )


def test_conditions_record_what_each_side_imported() -> None:
    recorded = side_conditions(_baseline(), _candidate())
    assert recorded['baseline']['schema_version'] == BASELINE_SCHEMA_VERSION
    assert recorded['candidate']['schema_version'] == CANDIDATE_SCHEMA_VERSION
    assert recorded['baseline']['module_path'] != (
        recorded['candidate']['module_path']
    )
