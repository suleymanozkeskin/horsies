"""Values the leaf catalog derives instead of storing."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from horsies.core.history.commands import AnchoredLowerBound, OpenLowerBound
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.partitions.catalog import (
    decode_lower_bound,
    leaf_parent_name,
    leaf_task_index_name,
    render_partition_bound,
)

UTC = timezone.utc
OPEN_END = datetime(9999, 1, 1, tzinfo=UTC)


class TestRenderPartitionBound:
    def test_open_lower_bound_renders_minvalue(self) -> None:
        assert render_partition_bound(OpenLowerBound(), OPEN_END) == (
            "FOR VALUES FROM (MINVALUE) TO ('9999-01-01 00:00:00+00')"
        )

    def test_anchored_bounds_render_as_utc_literals(self) -> None:
        lower = datetime(2026, 11, 16, tzinfo=UTC)
        upper = datetime(2026, 11, 17, tzinfo=UTC)
        assert render_partition_bound(AnchoredLowerBound(lower), upper) == (
            "FOR VALUES FROM ('2026-11-16 00:00:00+00') TO ('2026-11-17 00:00:00+00')"
        )

    def test_fractional_seconds_drop_trailing_zeros(self) -> None:
        lower = datetime(2026, 11, 16, 1, 2, 3, 500000, tzinfo=UTC)
        upper = datetime(2026, 11, 16, 2, tzinfo=UTC)
        assert render_partition_bound(AnchoredLowerBound(lower), upper) == (
            "FOR VALUES FROM ('2026-11-16 01:02:03.5+00') TO ('2026-11-16 02:00:00+00')"
        )


class TestDecodeLowerBound:
    def test_open_kind_without_anchor(self) -> None:
        assert decode_lower_bound('OPEN', None) == OpenLowerBound()

    def test_at_kind_with_anchor(self) -> None:
        anchor = datetime(2026, 11, 16, tzinfo=UTC)
        assert decode_lower_bound('AT', anchor) == AnchoredLowerBound(anchor)

    @pytest.mark.parametrize(
        ('kind', 'anchor'),
        [
            ('OPEN', datetime(2026, 11, 16, tzinfo=UTC)),
            ('AT', None),
            ('ON', None),
        ],
    )
    def test_other_pairs_are_a_contract_error(
        self, kind: str, anchor: datetime | None
    ) -> None:
        with pytest.raises(HistoryContractError, match='lower bound pair'):
            decode_lower_bound(kind, anchor)


class TestDerivedNames:
    @pytest.mark.parametrize(
        ('class_key', 'parent'),
        [
            ('forever', 'horsies_task_history_forever'),
            ('heartbeats', 'horsies_heartbeats'),
            ('standard_30d', 'horsies_task_history_standard_30d'),
        ],
    )
    def test_parent_by_class(self, class_key: str, parent: str) -> None:
        assert leaf_parent_name(class_key) == parent

    def test_task_index_by_class(self) -> None:
        assert (
            leaf_task_index_name('standard_30d', 'leaf_a') == 'leaf_a_task_idx'
        )
        assert leaf_task_index_name('heartbeats', 'leaf_b') == 'leaf_b_probe_idx'
