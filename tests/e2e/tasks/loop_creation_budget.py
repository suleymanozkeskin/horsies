"""A child start hook that makes event loop creation fail on demand.

While the budget file named by ``HORSIES_E2E_LOOP_FAILURES`` holds a count
above 0, a call of ``asyncio.new_event_loop`` lowers the count by one and
raises ``OSError(EMFILE)``, as a process at its file descriptor limit does.
When ``HORSIES_E2E_LOOP_FAILS_ON_SLOT`` names a slot, every call in the child
of that slot raises it. With neither variable the hook changes nothing. The
hook runs in each child during its boot, before the child creates its task
loop.
"""

from __future__ import annotations

import asyncio
import errno
import multiprocessing
import os
from typing import Final

LOOP_FAILURES_ENV: Final = 'HORSIES_E2E_LOOP_FAILURES'
LOOP_FAILS_ON_SLOT_ENV: Final = 'HORSIES_E2E_LOOP_FAILS_ON_SLOT'


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


def _this_slot_always_fails() -> bool:
    """Whether this child serves the slot that the variable names.

    The worker names each child ``horsies-child-<slot>`` before its boot.
    """
    slot = os.environ.get(LOOP_FAILS_ON_SLOT_ENV)
    name = multiprocessing.current_process().name
    return slot is not None and name == f'horsies-child-{slot}'


def fail_loop_creation_on_demand() -> None:
    """Wrap ``asyncio.new_event_loop`` in this child, when a variable is set."""
    if _this_slot_always_fails():

        def no_descriptors() -> asyncio.AbstractEventLoop:
            raise OSError(errno.EMFILE, 'Too many open files')

        asyncio.new_event_loop = no_descriptors
        return
    budget_path = os.environ.get(LOOP_FAILURES_ENV)
    if budget_path is None:
        return
    real_new_event_loop = asyncio.new_event_loop

    def new_event_loop() -> asyncio.AbstractEventLoop:
        if _take_one_failure(budget_path):
            raise OSError(errno.EMFILE, 'Too many open files')
        return real_new_event_loop()

    asyncio.new_event_loop = new_event_loop
