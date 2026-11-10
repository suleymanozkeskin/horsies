"""The verdict of each run of a shared child that the worker killed."""

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
    TimedOutAtKill,
    culprit_retry,
    retry_placement,
    verdict_at_kill,
)
from horsies.core.worker.thread_slots import RunCulprit

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
IN_CLASS = TimedOutAtKill(RetryPlacement.SAME_CLASS)


@pytest.mark.parametrize(
    ('kill', 'deadline', 'verdict'),
    [
        # A run past its own deadline times out, in its class.
        (NoNamedKill(), PASSED, IN_CLASS),
        (HELD_BY_NEIGHBOUR, PASSED, IN_CLASS),
        (HELD_BY_NO_RUN, PASSED, IN_CLASS),
        # The run that held the loop retries isolated, past its deadline or not.
        (HELD_BY_RUN, PASSED, TimedOutAtKill(RetryPlacement.ISOLATED)),
        (HELD_BY_RUN, PENDING, BlockedTheLoop(5100, 'app.py:9 in fetch')),
        # A neighbour of a named holder gets its own code.
        (HELD_BY_NEIGHBOUR, PENDING, BesideBlockedLoop('task-9', 5100)),
        # No named culprit and time left: the crash path, nobody isolated.
        (HELD_BY_NO_RUN, PENDING, CrashPath()),
        (NoNamedKill(), PENDING, CrashPath()),
    ],
)
def test_the_verdict_of_a_run_at_a_kill(
    kill: ChildKill,
    deadline: DeadlineState,
    verdict: KilledRunVerdict | CrashPath,
) -> None:
    assert verdict_at_kill(kill, RUN, deadline) == verdict


@pytest.mark.parametrize(
    ('culprit_dispatch', 'placement'),
    [(RUN, RetryPlacement.ISOLATED), (9, RetryPlacement.SAME_CLASS)],
)
def test_only_the_culprit_of_a_kill_with_no_time_left_retries_isolated(
    culprit_dispatch: int,
    placement: RetryPlacement,
) -> None:
    culprit = RunCulprit(dispatch=culprit_dispatch, task_id='t')
    assert culprit_retry(culprit, RUN) is placement


@pytest.mark.parametrize(
    ('stop', 'placement'),
    [
        (StopState.STUCK, RetryPlacement.ISOLATED),
        (StopState.NOT_STUCK, RetryPlacement.SAME_CLASS),
    ],
)
def test_a_run_that_ends_after_it_was_stuck_retries_isolated(
    stop: StopState,
    placement: RetryPlacement,
) -> None:
    assert retry_placement(stop) is placement
