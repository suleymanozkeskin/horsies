"""The leaf lifecycle outcome unions: exhaustive, frozen, and evidence-carrying.

The inspection union is the vocabulary every maintenance decision matches
over; these tests pin its membership so a new state has to justify itself
here before an executor can return it, and an exhaustive match elsewhere
learns about it from the type checker rather than a runtime fallthrough.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import get_args

import pytest

from horsies.core.history.outcomes import (
    CatalogConflictKind,
    ClassCoverage,
    ClassIntervalMismatch,
    CoverageBelowFloor,
    DetachAwaitingFinalize,
    DropRefusedLoaderReferences,
    ForeverClassLeaf,
    HealthFault,
    LeafAlreadyConformant,
    LeafAttachment,
    LeafCatalogConflict,
    LeafCreated,
    LeafCreation,
    LeafDetachable,
    LeafDetached,
    LeafDetachInterrupted,
    LeafDropped,
    LeafIndexRepaired,
    LeafInspection,
    LeafMissing,
    LeafNonconformant,
    LeafNotExpired,
    LeafPendingBlocked,
    MissingDdlPrivilege,
    PartitionHealthReport,
    RetentionClassAbsent,
)

pytestmark = [pytest.mark.unit]


NOW = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)


class TestInspectionUnion:
    def test_union_members_are_exactly_the_lifecycle_states(self) -> None:
        assert set(get_args(LeafInspection.__value__)) == {
            LeafDetachable,
            LeafNotExpired,
            LeafPendingBlocked,
            LeafDetachInterrupted,
            LeafDetached,
            LeafDropped,
            LeafMissing,
            RetentionClassAbsent,
            ForeverClassLeaf,
            LeafCatalogConflict,
        }

    def test_every_variant_is_frozen(self) -> None:
        for variant in get_args(LeafInspection.__value__):
            params = getattr(variant, '__dataclass_params__')
            assert params.frozen, f'{variant.__name__} must be frozen'

    def test_blocked_state_names_its_attachment(self) -> None:
        fields = {field.name for field in dataclasses.fields(LeafPendingBlocked)}
        assert fields == {
            'leaf_name',
            'blocker_count',
            'expires_at',
            'attachment',
        }
        assert {member.value for member in LeafAttachment} == {
            'ATTACHED',
            'DETACH_INTERRUPTED',
            'DETACHED',
        }


class TestCreationUnion:
    def test_union_members_are_exactly_the_creation_outcomes(self) -> None:
        assert set(get_args(LeafCreation.__value__)) == {
            LeafCreated,
            LeafAlreadyConformant,
            LeafIndexRepaired,
            RetentionClassAbsent,
            ForeverClassLeaf,
            ClassIntervalMismatch,
            LeafCatalogConflict,
        }


class TestHealthReport:
    def make_report(
        self, faults: tuple[HealthFault, ...]
    ) -> PartitionHealthReport:
        return PartitionHealthReport(
            class_key='finite_30d_v1',
            checked_at=NOW,
            coverage=ClassCoverage(
                class_key='finite_30d_v1',
                attached_leaf_count=8,
                coverage_until=NOW,
                complete_future_intervals=3,
                detachable_leaf_count=1,
                pending_blocked_leaf_count=0,
            ),
            faults=faults,
        )

    def test_no_faults_is_healthy(self) -> None:
        assert self.make_report(()).is_healthy

    @pytest.mark.parametrize(
        'fault',
        [
            CoverageBelowFloor(
                class_key='finite_30d_v1',
                complete_future_intervals=1,
                coverage_until=NOW,
            ),
            MissingDdlPrivilege(schema_create=False, owns_parent=True),
            LeafNonconformant(
                leaf_name='leaf',
                kind=CatalogConflictKind.PHYSICAL_NONCONFORMANT,
                detail='bound mismatch',
            ),
            DetachAwaitingFinalize(leaf_name='leaf'),
            RetentionClassAbsent(class_key='finite_30d_v1'),
        ],
    )
    def test_any_fault_is_unhealthy(self, fault: HealthFault) -> None:
        assert not self.make_report((fault,)).is_healthy

    def test_fault_union_members_are_exactly_the_fault_kinds(self) -> None:
        assert set(get_args(HealthFault.__value__)) == {
            CoverageBelowFloor,
            MissingDdlPrivilege,
            LeafNonconformant,
            DetachAwaitingFinalize,
            RetentionClassAbsent,
        }


class TestDropRefusal:
    def test_refusal_is_frozen_and_names_the_leaf(self) -> None:
        refusal = DropRefusedLoaderReferences(leaf_name='leaf')
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(refusal, 'leaf_name', 'other')
