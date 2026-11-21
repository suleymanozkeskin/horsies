"""Shape pins for the phase-2 attempt bound.

The runtime behavior (attempts climbing, the quarantine transition) is
proven by integration suites; these pins hold the structural facts those
suites assume: the bound's config validation refuses out-of-range values
by name, and discovery excludes rows at the bound.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import ValidationError

from horsies.core.models.recovery import RecoveryConfig

pytestmark = [pytest.mark.unit]


class TestQuarantineBoundConfig:
    @pytest.mark.parametrize('value', [2, 1_001])
    def test_out_of_range_bounds_are_refused(self, value: int) -> None:
        with pytest.raises(
            ValidationError, match='phase2_quarantine_after_attempts'
        ):
            RecoveryConfig(
                **cast(
                    'dict[str, Any]',
                    {'phase2_quarantine_after_attempts': value},
                )
            )

    def test_the_default_holds_the_derived_value(self) -> None:
        assert RecoveryConfig().phase2_quarantine_after_attempts == 25


