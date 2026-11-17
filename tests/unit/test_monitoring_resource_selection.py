"""Python's monitoring reads returned EVENT_LOOP_LIMIT when the broker's
resources were in use by another event loop; nothing ran.

Since the monitoring switch every read and the cancel action run in the
core and select no resource set, so they have no such answer. The case runs
on Python's body before the switch, the dual-run reference
(``tests/dualrun/reference/monitoring_queries.py``).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from horsies.core.types.result import is_err
from horsies.monitoring.errors import MonitoringQueryEventLoopLimit
from tests.dualrun.reference.monitoring_queries import list_schedules
from tests.broker_resources_support import attach_loop_limit

pytestmark = [pytest.mark.unit]


@pytest.mark.asyncio
async def test_query_maps_loop_limit_to_monitoring_loop_limit() -> None:
    broker = MagicMock()
    loop_limit = attach_loop_limit(broker)

    result = await list_schedules(broker)

    assert is_err(result)
    error = result.err_value
    assert isinstance(error, MonitoringQueryEventLoopLimit)
    assert error.retryable is False
    assert error.loop_limit is loop_limit
    assert error.message == f'schedule state query: {loop_limit.describe()}'
    broker.select_resources.assert_awaited_once()
