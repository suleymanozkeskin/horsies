"""The outcome types of the inspection states of the 0006 pruning functions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.outcomes import (
    CatalogConflictKind,
    LeafAttachment,
    LeafCatalogConflict,
    LeafDetachable,
    LeafDetached,
    LeafDetachInterrupted,
    LeafDropped,
    LeafMissing,
    LeafNotExpired,
    LeafPendingBlocked,
)
from horsies.core.history.partitions.retirement import InspectionColumns, inspection_of

pytestmark = [pytest.mark.unit]

LEAF: Final = 'horsies_task_history_standard_30d_2026_08_06'
EXPIRES_AT: Final = datetime(2026, 9, 6, tzinfo=UTC)


def _columns(
    *,
    expires_at: object = EXPIRES_AT,
    blocker_count: object = None,
    attachment: object = None,
    conflict_kind: object = None,
) -> InspectionColumns:
    return InspectionColumns(
        expires_at=expires_at,
        blocker_count=blocker_count,
        attachment=attachment,
        conflict_kind=conflict_kind,
    )


class TestStates:
    def test_not_cataloged_is_an_uncataloged_missing_leaf(self) -> None:
        assert inspection_of(LEAF, _columns(expires_at=None), 'NOT_CATALOGED') == LeafMissing(
            leaf_name=LEAF, cataloged=False, expires_at=None
        )

    def test_missing_is_a_cataloged_missing_leaf(self) -> None:
        assert inspection_of(LEAF, _columns(), 'MISSING') == LeafMissing(
            leaf_name=LEAF, cataloged=True, expires_at=EXPIRES_AT
        )

    def test_already_dropped(self) -> None:
        assert inspection_of(LEAF, _columns(), 'ALREADY_DROPPED') == LeafDropped(leaf_name=LEAF)

    def test_pending_blocked_carries_the_count_and_the_attachment(self) -> None:
        columns = _columns(blocker_count=2, attachment='DETACH_PENDING')
        assert inspection_of(LEAF, columns, 'PENDING_BLOCKED') == LeafPendingBlocked(
            leaf_name=LEAF,
            blocker_count=2,
            expires_at=EXPIRES_AT,
            attachment=LeafAttachment.DETACH_INTERRUPTED,
        )

    def test_detached(self) -> None:
        assert inspection_of(LEAF, _columns(), 'DETACHED') == LeafDetached(
            leaf_name=LEAF, expires_at=EXPIRES_AT
        )

    def test_detach_interrupted(self) -> None:
        assert inspection_of(LEAF, _columns(), 'DETACH_INTERRUPTED') == LeafDetachInterrupted(
            leaf_name=LEAF, expires_at=EXPIRES_AT
        )

    def test_not_expired(self) -> None:
        assert inspection_of(LEAF, _columns(), 'NOT_EXPIRED') == LeafNotExpired(
            leaf_name=LEAF, expires_at=EXPIRES_AT
        )

    def test_detachable(self) -> None:
        assert inspection_of(LEAF, _columns(), 'DETACHABLE') == LeafDetachable(
            leaf_name=LEAF, expires_at=EXPIRES_AT
        )

    @pytest.mark.parametrize(
        'kind',
        [
            CatalogConflictKind.RELATION_WITHOUT_CATALOG,
            CatalogConflictKind.OPEN_LOWER_BOUND,
            CatalogConflictKind.DROPPED_RELATION_EXISTS,
            CatalogConflictKind.FOREIGN_PARENT,
            CatalogConflictKind.DETACH_RECORDED_WHILE_ATTACHED,
            CatalogConflictKind.PHYSICAL_NONCONFORMANT,
        ],
    )
    def test_every_conflict_kind_of_the_inspection_maps_to_its_member(
        self, kind: CatalogConflictKind
    ) -> None:
        outcome = inspection_of(LEAF, _columns(conflict_kind=kind.value), 'CATALOG_CONFLICT')
        assert isinstance(outcome, LeafCatalogConflict)
        assert outcome.kind is kind


class TestRefusedValues:
    def test_an_unknown_state_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='inspection state'):
            inspection_of(LEAF, _columns(), 'RETIRED')

    def test_an_unknown_conflict_kind_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='conflict_kind'):
            inspection_of(LEAF, _columns(conflict_kind='SHADOWED'), 'CATALOG_CONFLICT')

    @pytest.mark.parametrize('count', [0, None, True, '2'])
    def test_a_blocker_count_that_is_not_a_positive_integer_raises(self, count: object) -> None:
        with pytest.raises(HistoryContractError, match='blocker_count'):
            inspection_of(
                LEAF, _columns(blocker_count=count, attachment='ATTACHED'), 'PENDING_BLOCKED'
            )

    def test_an_unknown_attachment_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='attachment'):
            inspection_of(
                LEAF, _columns(blocker_count=1, attachment='MISSING'), 'PENDING_BLOCKED'
            )

    def test_a_naive_expiry_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='expires_at'):
            inspection_of(LEAF, _columns(expires_at=datetime(2026, 9, 6)), 'DETACHABLE')
