"""A capture error of a pair's Python case fails its core case with that error.

The core case does not capture or compare: no snapshot of the Python case
exists, so the pair can never compare equal. No database: the record of the
Python case is put in place by hand.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

import pytest

from tests.dualrun.capture import NotificationListener, TableRowLimitExceeded
from tests.dualrun.operations.plugin import (
    PairCase,
    _after_core_body,  # pyright: ignore[reportPrivateUsage]
    _references,  # pyright: ignore[reportPrivateUsage]
)
from tests.dualrun.operations.rebinding import CallCounts
from tests.dualrun.operations.table import Implementation, WholeSnapshot
from tests.dualrun.pair_outcomes import PairKey, ReferenceCaptureFailed

pytestmark = [pytest.mark.unit]


@dataclass
class _Config:
    """The part of ``pytest.Config`` that the pair records use."""

    stash: pytest.Stash = field(default_factory=pytest.Stash)


def test_a_core_case_whose_python_capture_failed_fails_with_that_error() -> None:
    config = cast(pytest.Config, _Config())
    key = PairKey(case_name='tests/integration/test_x.py::test_y')
    _references(config)[key] = ReferenceCaptureFailed(
        error=TableRowLimitExceeded(table='horsies_tasks', limit=10),
    )
    case = PairCase(
        key=key,
        implementation=Implementation.CORE,
        counts=CallCounts(),
        url='not a database',
        start=datetime(2026, 11, 18, tzinfo=UTC),
        listener=cast(NotificationListener, None),
        row_limit=10,
        compare=WholeSnapshot(),
    )

    with pytest.raises(pytest.fail.Exception) as failed:
        _after_core_body(config, case)

    assert str(failed.value) == (
        'DualRunCaptureFailed: the capture after the python case failed: '
        'horsies_tasks: more than 10 rows; nothing to compare'
    )
    assert key not in _references(config)
