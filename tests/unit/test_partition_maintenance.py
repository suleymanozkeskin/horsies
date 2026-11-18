"""The maintenance owner's shape pins.

The runtime behavior (leaves land, workers start or refuse, one gate
holder) is proven by integration suites; these pins hold the structural
facts those suites assume: the config bounds refuse out-of-range
horizons by name, the ensure pass runs its stages in dependency order,
and the stamp check names every function that republication renders.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import ValidationError

from horsies.core.models.retention import RetentionConfig

pytestmark = [pytest.mark.unit]


class TestConfigBounds:
    @pytest.mark.parametrize(
        ('field', 'value'),
        [
            ('history_leaf_horizon_days', 1),
            ('history_leaf_horizon_days', 15),
            ('heartbeat_leaf_horizon_hours', 1),
            ('heartbeat_leaf_horizon_hours', 49),
            ('partition_maintenance_interval_s', 59),
            ('partition_maintenance_interval_s', 3_601),
        ],
    )
    def test_out_of_range_values_are_refused(
        self, field: str, value: int
    ) -> None:
        with pytest.raises(ValidationError, match=field):
            RetentionConfig(**cast('dict[str, Any]', {field: value}))

    def test_defaults_hold_the_ratified_values(self) -> None:
        config = RetentionConfig()
        assert config.history_leaf_horizon_days == 3
        assert config.heartbeat_leaf_horizon_hours == 6
        assert config.partition_maintenance_interval_s == 900

    def test_any_in_bounds_interval_refreshes_before_the_horizon_floor(
        self,
    ) -> None:
        """The interval ceiling must sit far inside the horizon floors.

        The heartbeat floor is 2 complete future hourly leaves; a tick
        every 3600 s leaves at least one full covered hour ahead at all
        times, so no in-bounds configuration can reach the cliff.
        """
        interval_ceiling_s = 3_600
        heartbeat_floor_s = 2 * 3_600
        assert interval_ceiling_s < heartbeat_floor_s


class TestPruneCandidateKinds:
    """The pass hands heartbeat leaves to the heartbeat sweep and period
    leaves to the history sweep, in candidate order; neither sweep sees the
    other's leaves."""

    def test_each_sweep_gets_only_its_kind(self) -> None:
        from datetime import UTC, datetime, timedelta

        from horsies.core.history.commands import LeafBounds, LeafRef
        from horsies.core.history.outcomes import PruneReach
        from horsies.core.history.partitions.retirement import (
            CandidateAttachment,
            PruneCandidate,
            PruneCandidates,
            RetiredClassKind,
        )

        def candidate(name: str, kind: RetiredClassKind) -> PruneCandidate:
            lower = datetime(2021, 1, 1, tzinfo=UTC)
            return PruneCandidate(
                leaf=LeafRef(
                    leaf_name=name,
                    class_key='standard_1d',
                    bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
                ),
                class_kind=kind,
                attachment=CandidateAttachment.ATTACHED,
            )

        candidates = PruneCandidates(
            leaves=(
                candidate('history_a', RetiredClassKind.PERIOD),
                candidate('heartbeat_a', RetiredClassKind.HEARTBEAT),
                candidate('history_b', RetiredClassKind.PERIOD),
            ),
            reach=PruneReach.WITHIN_BOUNDS,
        )

        assert [leaf.leaf_name for leaf in candidates.of_kind(RetiredClassKind.PERIOD)] == [
            'history_a', 'history_b',
        ]
        assert [
            leaf.leaf_name for leaf in candidates.of_kind(RetiredClassKind.HEARTBEAT)
        ] == ['heartbeat_a']
