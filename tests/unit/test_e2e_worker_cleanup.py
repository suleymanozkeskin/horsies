"""The e2e cleanup of stale workers kills only this checkout's workers.

Stand-in processes carry ``horsies worker`` in their command line, as a real
worker does. Only the one whose environment names this checkout as its owner
may be killed; a worker of another checkout, or with no owner, must survive.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator

import pytest

from tests.e2e.helpers.worker import (
    REPO_ROOT,
    WORKER_OWNER_ENV,
    kill_stale_workers,
    own_stale_worker_pids,
    worker_env,
)

pytestmark = [pytest.mark.unit]

STAND_IN_LIFETIME_S = 60
EXIT_WAIT_S = 10.0


def _stand_in(owner: str | None) -> subprocess.Popen[bytes]:
    """A process whose command line reads ``... horsies worker``."""
    env = os.environ.copy()
    env.pop(WORKER_OWNER_ENV, None)
    if owner is not None:
        env[WORKER_OWNER_ENV] = owner
    return subprocess.Popen(
        [
            sys.executable,
            '-c',
            f'import time; time.sleep({STAND_IN_LIFETIME_S})',
            'horsies',
            'worker',
        ],
        env=env,
    )


@pytest.fixture
def stand_ins() -> Iterator[list[subprocess.Popen[bytes]]]:
    started: list[subprocess.Popen[bytes]] = []
    yield started
    for process in started:
        if process.poll() is None:
            process.kill()
        process.wait(EXIT_WAIT_S)


def test_only_this_checkouts_worker_is_killed(
    stand_ins: list[subprocess.Popen[bytes]],
) -> None:
    own = _stand_in(REPO_ROOT)
    other_checkout = _stand_in('/elsewhere/another-checkout')
    no_owner = _stand_in(None)
    stand_ins.extend([own, other_checkout, no_owner])

    kill_stale_workers()

    assert own.wait(EXIT_WAIT_S) is not None
    assert other_checkout.poll() is None
    assert no_owner.poll() is None


def test_the_scan_names_only_this_checkouts_workers(
    stand_ins: list[subprocess.Popen[bytes]],
) -> None:
    own = _stand_in(REPO_ROOT)
    other_checkout = _stand_in('/elsewhere/another-checkout')
    stand_ins.extend([own, other_checkout])

    pids = own_stale_worker_pids()

    assert own.pid in pids
    assert other_checkout.pid not in pids


def test_started_workers_carry_this_checkouts_marker() -> None:
    env = worker_env()
    assert env[WORKER_OWNER_ENV] == REPO_ROOT
    assert env['PYTHONPATH'] == REPO_ROOT
