"""The maintenance owner's shape pins.

The runtime behavior (leaves land, workers start or refuse, one gate
holder) is proven by integration suites; these pins hold the structural
facts those suites assume: the config bounds refuse out-of-range
horizons by name, the ensure pass runs its stages in dependency order,
and the stamp check names every function that republication renders.
"""

from __future__ import annotations

import inspect
import re
from typing import Any, cast

import pytest
from pydantic import ValidationError

from horsies.core.history.maintenance.coverage import ensure_partition_coverage
from horsies.core.history.reads import lookup_generation
from horsies.core.history.reads.lookup_generation import LookupManifest
from horsies.core.history.reads.publisher import (
    RENDERED_READER_SIGNATURES,
    StagedLoaderPublisher,
)
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


class TestEnsureSequence:
    def test_stages_run_in_dependency_order(self) -> None:
        """Registration precedes coverage; coverage precedes publication."""
        source = inspect.getsource(ensure_partition_coverage)
        positions = [
            source.index('register_heartbeat_class('),
            source.index('register_finite_retention_class('),
            source.index('ensure_leaf_coverage('),
            source.index('ensure_heartbeat_coverage('),
            source.index('republish('),
        ]
        assert positions == sorted(positions)


class TestRenderedReadersAreStamped:
    def test_the_stamped_signatures_are_the_rendered_functions(self) -> None:
        """A function that republication renders but does not stamp would
        never count as stale, so the two lists must name the same set."""
        republish_source = inspect.getsource(StagedLoaderPublisher.republish)
        renderers = set(
            re.findall(r'render_staged_\w+_function', republish_source)
        )
        empty = LookupManifest(leaves=(), birth_floor=None)
        rendered: set[str] = set()
        for renderer in renderers:
            source = getattr(lookup_generation, renderer)(empty)
            match = re.search(
                r'CREATE OR REPLACE FUNCTION (\w+)\(p_task_id uuid'
                r'(, p_include_live boolean)?',
                source,
            )
            assert match is not None, renderer
            parameters = 'uuid, boolean' if match.group(2) else 'uuid'
            rendered.add(f'{match.group(1)}({parameters})')
        assert len(renderers) == len(RENDERED_READER_SIGNATURES)
        assert rendered == set(RENDERED_READER_SIGNATURES)


class TestPruningWiring:
    def test_maintenance_pass_prunes_after_coverage(self) -> None:
        """The tick runs coverage, then pruning, then reschedules.

        This pin is the defect's own shape: the pruning mechanism once
        existed fully tested with no production caller, so configured
        retention durations were silently unenforced. It goes red if
        the pruning call ever leaves the reaper pass.
        """
        from horsies.core.worker.reaper import ReaperMixin

        source = inspect.getsource(ReaperMixin._run_reaper_pass)
        positions = [
            source.index('maintain_partition_coverage('),
            source.index('prune_expired_partitions('),
            source.index('state.next_partition_maintenance_at = ('),
        ]
        assert positions == sorted(positions)

    def test_driver_finalizes_before_either_sweep(self) -> None:
        """One pending finalization blocks every further detach on its
        parent, so the finalize phase must precede both sweeps."""
        from horsies.core.history.maintenance.pruning import (
            prune_expired_partitions,
        )

        source = inspect.getsource(prune_expired_partitions)
        positions = [
            source.index('_finalize_interrupted_detaches('),
            source.index('sweep_expired_heartbeat_leaves('),
            source.index('sweep_expired_history_leaves('),
        ]
        assert positions == sorted(positions)


class TestPruningDiscoveryFilters:
    def test_forever_is_structurally_excluded(self) -> None:
        """Both discovery statements require a non-NULL duration; the
        forever class carries NULL and can never become a candidate,
        by filter rather than by NULL interval arithmetic."""
        from horsies.core.history.maintenance.pruning import (
            EXPIRED_FINITE_LEAVES_SQL,
            EXPIRED_HISTORY_LEAVES_SQL,
        )

        for statement in (
            EXPIRED_FINITE_LEAVES_SQL,
            EXPIRED_HISTORY_LEAVES_SQL,
        ):
            assert 'r.duration IS NOT NULL' in statement
            assert 'c.dropped_at IS NULL' in statement

    def test_history_discovery_excludes_the_heartbeat_class(self) -> None:
        """Heartbeat leaves are the heartbeat sweep's; the history sweep
        must not race it for the same leaves."""
        from horsies.core.history.maintenance.pruning import (
            EXPIRED_HISTORY_LEAVES_SQL,
        )

        assert (
            'c.class_key <> :heartbeat_class_key'
            in EXPIRED_HISTORY_LEAVES_SQL
        )
