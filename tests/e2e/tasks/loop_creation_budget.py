"""A child start hook that makes event loop creation fail on demand.

While the budget file named by ``HORSIES_E2E_LOOP_FAILURES`` holds a count
above 0, a call of ``asyncio.new_event_loop`` lowers the count by one and
raises ``OSError(EMFILE)``, as a process at its file descriptor limit does.
With no such variable the hook changes nothing. The hook runs in each child
during its boot, before the child creates its task loop.
"""

from __future__ import annotations

import asyncio
import errno
import os
from typing import Final

LOOP_FAILURES_ENV: Final = 'HORSIES_E2E_LOOP_FAILURES'


def _take_one_failure(budget_path: str) -> bool:
    """Lower the budget by one when it is above 0. Return whether it was."""
    with open(budget_path, 'r+', encoding='ascii') as budget:
        left = int(budget.read() or '0')
        if left <= 0:
            return False
        budget.seek(0)
        budget.truncate()
        budget.write(str(left - 1))
        return True


def fail_loop_creation_on_demand() -> None:
    """Wrap ``asyncio.new_event_loop`` in this child, when the variable is set."""
    budget_path = os.environ.get(LOOP_FAILURES_ENV)
    if budget_path is None:
        return
    real_new_event_loop = asyncio.new_event_loop

    def new_event_loop() -> asyncio.AbstractEventLoop:
        if _take_one_failure(budget_path):
            raise OSError(errno.EMFILE, 'Too many open files')
        return real_new_event_loop()

    asyncio.new_event_loop = new_event_loop
