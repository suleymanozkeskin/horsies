"""The coverage pass's reading of the 0008 probe and creation rows, and its declared day counts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final, cast

import pytest
from sqlalchemy.engine import Row

from horsies.core.history.commands import ForeverLeafRef
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.maintenance.coverage import (
    CoveredForever,
    CoveredUntil,
    _coverage_probe_of,  # pyright: ignore[reportPrivateUsage]
    declared_days,
    history_coverage_payload,
    leaf_creation_of,
)
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


_CHECKED_AT: Final = datetime(2026, 11, 17, 13, 20, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class _ProbeRow:
    """The columns of one ``horsies_coverage_probe`` row that the decoder reads."""

    leaf_kind: str
    class_key: str
    leaf_name: str
    lower_bound_kind: str
    lower_anchor: datetime | None
    upper_bound_kind: str
    upper_anchor: datetime | None
    conformant: bool
    checked_at: datetime = _CHECKED_AT
    registration_healthy: bool = True
    publication_healthy: bool = True
    absent_leaves: tuple[str, ...] = ()


def _at(hour: int, day: int = 17) -> datetime:
    return datetime(2026, 11, day, hour, tzinfo=timezone.utc)


def _forever_row(*, conformant: bool = True) -> _ProbeRow:
    return _ProbeRow(
        leaf_kind='history',
        class_key='forever',
        leaf_name=FOREVER_LEAF,
        lower_bound_kind='OPEN',
        lower_anchor=None,
        upper_bound_kind='OPEN',
        upper_anchor=None,
        conformant=conformant,
    )


def _daily_row(class_key: str, day: int, *, conformant: bool = True) -> _ProbeRow:
    return _ProbeRow(
        leaf_kind='history',
        class_key=class_key,
        leaf_name=f'horsies_task_history_{class_key}_2026_11_{day}',
        lower_bound_kind='AT',
        lower_anchor=_at(0, day),
        upper_bound_kind='AT',
        upper_anchor=_at(0, day + 1),
        conformant=conformant,
    )


def _hourly_row(hour: int) -> _ProbeRow:
    return _ProbeRow(
        leaf_kind='heartbeat',
        class_key='heartbeats',
        leaf_name=f'horsies_heartbeats_2026_11_17_{hour}',
        lower_bound_kind='AT',
        lower_anchor=_at(hour),
        upper_bound_kind='AT',
        upper_anchor=_at(hour + 1),
        conformant=True,
    )


def _rows(*rows: _ProbeRow) -> Sequence[Row[Any]]:
    return cast(Sequence[Row[Any]], rows)


class TestCoverageProbeRows:
    def test_history_coverage_is_the_earliest_newest_end_over_the_classes(self) -> None:
        """Each class reaches the end of its newest leaf; the probe reports the earliest.

        The forever leaf has no end and does not move it; a damaged leaf
        counts as an expected leaf.
        """
        probe = _coverage_probe_of(
            _rows(
                _hourly_row(13),
                _hourly_row(14),
                _forever_row(),
                _daily_row('standard_30d', 17),
                _daily_row('standard_30d', 18),
                _daily_row('standard_30d', 19, conformant=False),
                _daily_row('standard_7d', 17),
                _daily_row('standard_7d', 18),
            ),
        )

        assert probe.history_covered_through == CoveredUntil(at=_at(0, 19))
        assert probe.heartbeats_covered_through == _at(15)
        assert probe.heartbeat_covered_now

    def test_history_coverage_is_forever_when_only_the_forever_leaf_is_expected(self) -> None:
        """A fresh install has no period class yet: the forever leaf is the only history row."""
        probe = _coverage_probe_of(_rows(_hourly_row(13), _forever_row()))

        assert probe.history_covered_through == CoveredForever()

    def test_the_forever_row_decodes_with_open_bounds_and_no_anchors(self) -> None:
        probe = _coverage_probe_of(_rows(_hourly_row(13), _forever_row(conformant=False)))

        assert probe.damaged_history == (
            ForeverLeafRef(leaf_name=FOREVER_LEAF, class_key='forever'),
        )

    @pytest.mark.parametrize(
        ('lower_bound_kind', 'upper_bound_kind'),
        [('OPEN', 'AT'), ('AT', 'OPEN'), ('AT', 'MAXVALUE')],
    )
    def test_a_row_whose_bound_kinds_are_not_a_probe_pair_is_refused(
        self,
        lower_bound_kind: str,
        upper_bound_kind: str,
    ) -> None:
        mixed = _ProbeRow(
            leaf_kind='history',
            class_key='standard_30d',
            leaf_name=LEAF,
            lower_bound_kind=lower_bound_kind,
            lower_anchor=_at(0),
            upper_bound_kind=upper_bound_kind,
            upper_anchor=_at(0, 18),
            conformant=True,
        )

        with pytest.raises(AssertionError, match='are not a pair of the probe'):
            _coverage_probe_of(_rows(_hourly_row(13), mixed))

    def test_the_health_payload_names_the_kind_and_the_instant(self) -> None:
        assert history_coverage_payload(CoveredForever()) == {'kind': 'FOREVER'}
        assert history_coverage_payload(CoveredUntil(at=_at(0, 19))) == {
            'kind': 'UNTIL',
            'at': '2026-11-19T00:00:00+00:00',
        }
