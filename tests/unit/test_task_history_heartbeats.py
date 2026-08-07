"""Heartbeat partition module: derivations, vocabulary, structural pins.

The horizon is derived from staleness thresholds, never a constant; the
parent is RANGE on `sent_at` with no LIST tier and no primary key; the
per-leaf index is the stale probe's shape; and the probe's recency bound
is computed from the passed parameters inside the capture statement.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from horsies.core.history.heartbeats.partitioning import (
    HEARTBEATS_PARTITIONED_DDL,
    CreateHourlyHeartbeatLeaf,
    EnsureHeartbeatCoverage,
    heartbeat_horizon,
    hourly_leaf_name,
    hourly_leaf_ref,
    probe_index_name,
)
from horsies.core.history.names import HEARTBEAT_CLASS_KEY
from horsies.core.history.terminalization.move import (
    failure_family_fragments,
)

pytestmark = [pytest.mark.unit]

HOUR = datetime(2026, 8, 7, 13, 0, tzinfo=timezone.utc)


class TestHorizonDerivation:
    def test_larger_threshold_times_safety_factor(self) -> None:
        horizon = heartbeat_horizon(
            stale_after=timedelta(minutes=10),
            finalizing_stale_after=timedelta(minutes=45),
            safety_factor=4,
        )
        assert horizon == timedelta(hours=3)

    def test_one_hour_floor(self) -> None:
        horizon = heartbeat_horizon(
            stale_after=timedelta(seconds=30),
            finalizing_stale_after=timedelta(seconds=45),
            safety_factor=2,
        )
        assert horizon == timedelta(hours=1)

    def test_rejects_non_positive_thresholds(self) -> None:
        with pytest.raises(ValueError, match='must be positive'):
            heartbeat_horizon(
                stale_after=timedelta(0),
                finalizing_stale_after=timedelta(minutes=1),
                safety_factor=2,
            )

    def test_rejects_safety_factor_below_one(self) -> None:
        with pytest.raises(ValueError, match='at least 1'):
            heartbeat_horizon(
                stale_after=timedelta(minutes=1),
                finalizing_stale_after=timedelta(minutes=1),
                safety_factor=0,
            )


class TestVocabulary:
    def test_hourly_leaf_name_embeds_the_hour(self) -> None:
        assert hourly_leaf_name(HOUR) == 'horsies_heartbeats_2026_08_07_13'

    def test_probe_index_name_derives_from_leaf(self) -> None:
        assert (
            probe_index_name('horsies_heartbeats_2026_08_07_13')
            == 'horsies_heartbeats_2026_08_07_13_probe_idx'
        )

    def test_hourly_ref_carries_the_reserved_class(self) -> None:
        ref = hourly_leaf_ref(HOUR)
        assert ref.class_key == HEARTBEAT_CLASS_KEY
        assert ref.bounds.upper - ref.bounds.lower == timedelta(hours=1)


class TestCommands:
    def test_hour_span_is_required(self) -> None:
        ref = hourly_leaf_ref(HOUR)
        CreateHourlyHeartbeatLeaf(leaf=ref)
        from horsies.core.history.commands import LeafBounds, LeafRef

        daily = LeafRef(
            leaf_name=ref.leaf_name,
            class_key=HEARTBEAT_CLASS_KEY,
            bounds=LeafBounds(lower=HOUR, upper=HOUR + timedelta(days=1)),
        )
        with pytest.raises(ValueError, match='exactly one hour'):
            CreateHourlyHeartbeatLeaf(leaf=daily)

    def test_reserved_class_is_required(self) -> None:
        from horsies.core.history.commands import LeafBounds, LeafRef

        foreign = LeafRef(
            leaf_name='horsies_heartbeats_2026_08_07_13',
            class_key='not_heartbeats',
            bounds=LeafBounds(lower=HOUR, upper=HOUR + timedelta(hours=1)),
        )
        with pytest.raises(ValueError, match='reserved class key'):
            CreateHourlyHeartbeatLeaf(leaf=foreign)

    def test_coverage_floor_is_two_future_leaves(self) -> None:
        EnsureHeartbeatCoverage(horizon_hours=2)
        with pytest.raises(ValueError, match='red line'):
            EnsureHeartbeatCoverage(horizon_hours=1)


class TestParentShape:
    def test_range_on_sent_at_with_no_list_tier(self) -> None:
        assert 'PARTITION BY RANGE (sent_at)' in HEARTBEATS_PARTITIONED_DDL
        assert 'PARTITION BY LIST' not in HEARTBEATS_PARTITIONED_DDL

    def test_no_primary_key_and_no_retention_index(self) -> None:
        assert 'PRIMARY KEY' not in HEARTBEATS_PARTITIONED_DDL
        assert 'idx_horsies_heartbeats_sent_at' not in (
            HEARTBEATS_PARTITIONED_DDL
        )


class TestStaleProbeRecencyBound:
    def test_bound_is_computed_from_the_passed_parameters(self) -> None:
        """The recency bound must derive from the call's own thresholds —
        a constant bound could silently undercut a configured stale_after
        and change a verdict."""
        stale_fragment = next(
            fragment
            for fragment in failure_family_fragments()
            if 'horsies_fail_stale_task' in fragment
            and 'CREATE FUNCTION' in fragment
        )
        probe = stale_fragment.index("h.role = 'runner'")
        bound_region = stale_fragment[probe:probe + 400]
        assert 'h.sent_at >= NOW() - make_interval(' in bound_region
        assert 'GREATEST(' in bound_region
        assert 'p_stale_after_ms' in bound_region
        assert 'p_finalizing_stale_after_ms' in bound_region

    def test_bound_sits_inside_the_capture_subselect(self) -> None:
        stale_fragment = next(
            fragment
            for fragment in failure_family_fragments()
            if 'horsies_fail_stale_task' in fragment
            and 'CREATE FUNCTION' in fragment
        )
        capture = stale_fragment.index('ORDER BY h.sent_at DESC')
        bound = stale_fragment.index('h.sent_at >= NOW() - make_interval(')
        assert bound < capture
