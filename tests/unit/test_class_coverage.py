"""The pure functions of the fleet check and the class report.

Each outcome of each function has a test: a stream is served, served
without capacity, or unserved; a stream waits or has no due row; a live
snapshot reads or is unreadable; a worker's rows fit its budgets or do not.
"""

from __future__ import annotations

import pytest

from horsies.core.models.class_coverage import (
    BudgetLoad,
)
from horsies.core.models.tasks import Execution

ISOLATED = Execution.ISOLATED


@pytest.mark.unit
class TestBudgetLoads:


    def test_free_is_never_below_zero(self) -> None:
        """A prefetch buffer holds more rows than the capacity."""
        assert BudgetLoad(ISOLATED, 2, (ISOLATED,), in_flight=5).free == 0
