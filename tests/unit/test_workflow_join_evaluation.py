"""The join of a PENDING dependent, as the batched promotion evaluates it."""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import pytest

from horsies.core.workflows.engine import _evaluate_join, _JoinVerdict

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    (
        'join_type',
        'min_success',
        'completed',
        'failed',
        'skipped',
        'running',
        'verdict',
    ),
    [
        ('all', None, 2, 0, 0, 0, _JoinVerdict.MET),
        ('all', None, 1, 1, 1, 0, _JoinVerdict.MET),
        ('all', None, 2, 0, 0, 1, _JoinVerdict.WAITING),
        ('any', None, 1, 0, 0, 2, _JoinVerdict.MET),
        ('any', None, 0, 2, 1, 0, _JoinVerdict.IMPOSSIBLE),
        ('any', None, 0, 1, 0, 2, _JoinVerdict.WAITING),
        ('quorum', 2, 2, 0, 0, 1, _JoinVerdict.MET),
        ('quorum', 2, 1, 2, 0, 0, _JoinVerdict.IMPOSSIBLE),
        ('quorum', 2, 1, 1, 0, 1, _JoinVerdict.WAITING),
        ('quorum', None, 1, 0, 0, 2, _JoinVerdict.MET),
        ('unknown', None, 3, 0, 0, 0, _JoinVerdict.WAITING),
    ],
    ids=[
        'all-every-dependency-completed',
        'all-every-dependency-terminal',
        'all-one-running',
        'any-first-success',
        'any-all-terminal-no-success',
        'any-waits',
        'quorum-threshold-reached',
        'quorum-threshold-out-of-reach',
        'quorum-waits',
        'quorum-unset-threshold-is-one',
        'unknown-join-waits',
    ],
)
def test_join_verdict(
    join_type: str,
    min_success: int | None,
    completed: int,
    failed: int,
    skipped: int,
    running: int,
    verdict: _JoinVerdict,
) -> None:
    """``running`` counts the dependencies that are not terminal yet."""
    assert (
        _evaluate_join(
            join_type,
            min_success,
            total_deps=completed + failed + skipped + running,
            completed=completed,
            failed=failed,
            skipped=skipped,
        )
        is verdict
    )
