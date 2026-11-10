"""The verdict of each run of a shared async child that the worker killed."""

from __future__ import annotations

import pytest

from horsies.core.worker.shared_stop import (
    BesideBlockedLoop,
    BlockedLoopKill,
    BlockedTheLoop,
    ChildKill,
    CrashPath,
    DeadlineState,
    KilledRunVerdict,
    LoopHeldBy,
    LoopHolderUnknown,
    NoNamedKill,
    RetryPlacement,
    StopState,
    StuckRunsKill,
    TimedOutAtKill,
    retry_placement,
    verdict_at_kill,
)

pytestmark = [pytest.mark.unit]

RUN = 4
HELD_BY_RUN = BlockedLoopKill(
    holder=LoopHeldBy(dispatch=RUN, task_id='task-4'),
    blocked_ms=5100,
    where='app.py:9 in fetch',
)
HELD_BY_NEIGHBOUR = BlockedLoopKill(
    holder=LoopHeldBy(dispatch=9, task_id='task-9'),
    blocked_ms=5100,
    where='app.py:9 in fetch',
)
HELD_BY_NO_RUN = BlockedLoopKill(holder=LoopHolderUnknown(), blocked_ms=5100, where='x')

PASSED = DeadlineState.PASSED
PENDING = DeadlineState.PENDING
STUCK = StopState.STUCK
NOT_STUCK = StopState.NOT_STUCK


@pytest.mark.parametrize(
    ('kill', 'deadline', 'stop', 'verdict'),
    [
        # A run past its own deadline times out; a stuck run retries isolated.
        (StuckRunsKill(), PASSED, STUCK, TimedOutAtKill(RetryPlacement.ISOLATED)),
        (NoNamedKill(), PASSED, NOT_STUCK, TimedOutAtKill(RetryPlacement.SAME_CLASS)),
        (NoNamedKill(), PASSED, STUCK, TimedOutAtKill(RetryPlacement.ISOLATED)),
        (
            HELD_BY_NEIGHBOUR,
            PASSED,
            NOT_STUCK,
            TimedOutAtKill(RetryPlacement.SAME_CLASS),
        ),
        (HELD_BY_NO_RUN, PASSED, NOT_STUCK, TimedOutAtKill(RetryPlacement.SAME_CLASS)),
        # The run that held the loop retries isolated, past its deadline or not.
        (HELD_BY_RUN, PASSED, NOT_STUCK, TimedOutAtKill(RetryPlacement.ISOLATED)),
        (HELD_BY_RUN, PENDING, NOT_STUCK, BlockedTheLoop(5100, 'app.py:9 in fetch')),
        # A neighbour of a named holder gets its own code.
        (HELD_BY_NEIGHBOUR, PENDING, NOT_STUCK, BesideBlockedLoop('task-9', 5100)),
        # No named culprit and time left: the crash path, nobody isolated.
        (HELD_BY_NO_RUN, PENDING, NOT_STUCK, CrashPath()),
        (NoNamedKill(), PENDING, NOT_STUCK, CrashPath()),
        (StuckRunsKill(), PENDING, NOT_STUCK, CrashPath()),
    ],
)
def test_the_verdict_of_a_run_at_a_kill(
    kill: ChildKill,
    deadline: DeadlineState,
    stop: StopState,
    verdict: KilledRunVerdict | CrashPath,
) -> None:
    assert verdict_at_kill(kill, RUN, deadline, stop) == verdict


@pytest.mark.parametrize(
    ('stop', 'placement'),
    [(STUCK, RetryPlacement.ISOLATED), (NOT_STUCK, RetryPlacement.SAME_CLASS)],
)
def test_only_a_stuck_run_retries_isolated(
    stop: StopState,
    placement: RetryPlacement,
) -> None:
    assert retry_placement(stop) is placement
