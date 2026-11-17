"""The coverage pass's reading of the 0008 creation rows and its declared day counts."""

from __future__ import annotations

from datetime import timedelta
from typing import Final

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.maintenance.coverage import declared_days, leaf_creation_of
from horsies.core.history.names import FOREVER_LEAF
from horsies.core.history.outcomes import (
    CatalogConflictKind,
    CoveredByOpenEndedLeaf,
    LeafAlreadyConformant,
    LeafCatalogConflict,
    LeafCreated,
    LeafIndexRepaired,
    LeafMaintenanceBusy,
    RetentionClassAbsent,
)

pytestmark = [pytest.mark.unit]

LEAF: Final = 'horsies_task_history_standard_30d_2026_08_06'
HEARTBEAT_LEAF: Final = 'horsies_heartbeats_2026_08_06_13'


class TestLeafCreation:
    def test_created_names_the_task_index(self) -> None:
        assert leaf_creation_of('standard_30d', LEAF, 'CREATED', None) == LeafCreated(
            leaf_name=LEAF, id_index_name=f'{LEAF}_task_idx'
        )

    def test_a_created_heartbeat_leaf_names_the_probe_index(self) -> None:
        assert leaf_creation_of('heartbeats', HEARTBEAT_LEAF, 'CREATED', None) == LeafCreated(
            leaf_name=HEARTBEAT_LEAF, id_index_name=f'{HEARTBEAT_LEAF}_probe_idx'
        )

    def test_index_repaired(self) -> None:
        assert leaf_creation_of('standard_30d', LEAF, 'INDEX_REPAIRED', None) == (
            LeafIndexRepaired(leaf_name=LEAF, id_index_name=f'{LEAF}_task_idx')
        )

    def test_already_conformant(self) -> None:
        assert leaf_creation_of('standard_30d', LEAF, 'ALREADY_CONFORMANT', None) == (
            LeafAlreadyConformant(leaf_name=LEAF)
        )

    def test_covered_by_the_static_forever_leaf(self) -> None:
        name = 'horsies_task_history_forever_2026_08_06'
        assert leaf_creation_of('forever', name, 'COVERED_BY_OPEN_ENDED', None) == (
            CoveredByOpenEndedLeaf(leaf_name=name, open_ended_leaf_name=FOREVER_LEAF)
        )

    def test_busy(self) -> None:
        assert leaf_creation_of('standard_30d', LEAF, 'BUSY', None) == LeafMaintenanceBusy(
            leaf_name=LEAF
        )

    def test_catalog_conflict_carries_its_kind(self) -> None:
        outcome = leaf_creation_of('standard_30d', LEAF, 'CATALOG_CONFLICT', 'FOREIGN_INDEX')
        assert isinstance(outcome, LeafCatalogConflict)
        assert outcome.kind is CatalogConflictKind.FOREIGN_INDEX

    def test_class_absent_has_no_leaf_name(self) -> None:
        assert leaf_creation_of('standard_9d', None, 'CLASS_ABSENT', None) == (
            RetentionClassAbsent(class_key='standard_9d')
        )

    def test_an_unknown_outcome_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='returned'):
            leaf_creation_of('standard_30d', LEAF, 'CREATED_TWICE', None)

    def test_an_outcome_without_a_leaf_name_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='leaf name'):
            leaf_creation_of('standard_30d', None, 'CREATED', None)

    def test_an_unknown_conflict_kind_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='conflict_kind'):
            leaf_creation_of('standard_30d', LEAF, 'CATALOG_CONFLICT', 'SHADOWED')


class TestDeclaredDays:
    def test_the_day_counts_of_the_declared_classes(self) -> None:
        assert declared_days(
            (('standard_7d', timedelta(days=7)), ('standard_90d', timedelta(days=90)))
        ) == [7, 90]

    def test_a_fractional_period_is_refused(self) -> None:
        with pytest.raises(ValueError, match='whole number of days'):
            declared_days((('standard_1d', timedelta(days=1, hours=1)),))

    def test_a_key_that_is_not_the_key_of_its_period_is_refused(self) -> None:
        with pytest.raises(ValueError, match='not the class of its period'):
            declared_days((('nightly', timedelta(days=7)),))
