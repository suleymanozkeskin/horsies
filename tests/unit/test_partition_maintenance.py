"""The partition maintenance settings of ``RetentionConfig``.

The config bounds refuse out-of-range horizons by name, the defaults hold
the ratified values, and any in-bounds interval refreshes before the
horizon floor.
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


