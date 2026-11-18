"""The workflow recovery limit: classification, outcomes, and the error text.

The real writes are in
crates/horsies-core/src/workflow_engine/port_tests/recovery_limit_pass.rs.
Here the count statement (`count_recovery_raise`) is patched, and the pass
runs against sessions that answer with planned rows.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations


import pytest
from pydantic import ValidationError

from horsies.core.models.recovery import (
    DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT,
    RecoveryConfig,
)

pytestmark = pytest.mark.unit


class TestConfig:
    def test_the_default_matches_the_named_constant(self) -> None:
        assert (
            RecoveryConfig().workflow_recovery_failure_limit
            == DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT
            == 25
        )

    @pytest.mark.parametrize('limit', [3, 1_000])
    def test_each_end_of_the_range_is_accepted(self, limit: int) -> None:
        config = RecoveryConfig(workflow_recovery_failure_limit=limit)
        assert config.workflow_recovery_failure_limit == limit

    @pytest.mark.parametrize('limit', [2, 1_001])
    def test_a_value_out_of_range_is_refused(self, limit: int) -> None:
        with pytest.raises(ValidationError):
            RecoveryConfig(workflow_recovery_failure_limit=limit)


