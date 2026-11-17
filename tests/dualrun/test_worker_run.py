"""Whole-run pairs of the worker (`tests/dualrun/worker_run.py`).

Each test runs one scenario on the reference worker and on the candidate
worker, each with a fresh database, and requires an empty snapshot diff, an
equal task report and a clean exit on both sides. The scenarios cover the
plain successes, the task errors with and without a retry, the isolated
timeout and crash, and the shared async timeout, on the profile of each child
kind.

Run: `pytest tests/dualrun/test_worker_run.py -m e2e` with a test server
(`HORSIES_TEST_DATABASE_URL`) and a built `horsies._core`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.apps import whole_run_app as whole
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.worker_run import (
    Scenario,
    StopEnd,
    Terminal,
    build_scenario,
    run_pair,
)

pytestmark = [pytest.mark.e2e]

MakeDatabase = Callable[[], DualRunDatabase]

BATCH: int = 64
"""The plain successes of one scenario: more than one claim batch."""


@dataclass(frozen=True, slots=True)
class _Case:
    """A scenario and the terminal status of each of its sends."""

    build: Callable[[], Scenario]
    statuses: tuple[str, ...]


def _value(value: int) -> str:
    return f'{{"value":{value}}}'


def _successes(name: str, profile: str, task: object) -> _Case:
    return _Case(
        build=lambda: build_scenario(
            name, profile, [(task, _value(value)) for value in range(BATCH)]
        ),
        statuses=('COMPLETED',) * BATCH,
    )


CASES: dict[str, _Case] = {
    'isolated-successes': _successes(
        'isolated-successes', whole.ISOLATED, whole.double
    ),
    'isolated-task-errors': _Case(
        build=lambda: build_scenario(
            'isolated-task-errors',
            whole.ISOLATED,
            [
                (whole.refuse, _value(1)),
                (whole.transient, _value(2)),
                (whole.refuse, _value(3)),
                (whole.transient, _value(4)),
            ],
        ),
        statuses=('FAILED',) * 4,
    ),
    'isolated-timeout-and-crash': _Case(
        build=lambda: build_scenario(
            'isolated-timeout-and-crash',
            whole.ISOLATED,
            [
                (whole.sleep_past_deadline, None),
                (whole.crash, None),
                (whole.double, _value(5)),
            ],
        ),
        statuses=('FAILED', 'FAILED', 'COMPLETED'),
    ),
    'shared-async-successes': _successes(
        'shared-async-successes', whole.SHARED_ASYNC, whole.double_async
    ),
    'shared-async-timeout': _Case(
        build=lambda: build_scenario(
            'shared-async-timeout',
            whole.SHARED_ASYNC,
            [(whole.async_past_deadline, None), (whole.double_async, _value(6))],
        ),
        statuses=('FAILED', 'COMPLETED'),
    ),
    'shared-thread-successes': _successes(
        'shared-thread-successes', whole.SHARED_THREAD, whole.double_thread
    ),
}


def _installed(make_database: MakeDatabase) -> DualRunDatabase:
    database = make_database()
    match install_baseline_schema(database):
        case Ok():
            return database
        case Err(failed):
            pytest.fail(f'{failed.step.value} {failed.database}: {failed.cause}')


@pytest.mark.parametrize('name', sorted(CASES))
def test_the_core_worker_leaves_what_the_python_worker_leaves(
    empty_database: MakeDatabase,
    name: str,
) -> None:
    case = CASES[name]
    scenario = case.build()
    match run_pair(scenario, _installed(empty_database), _installed(empty_database)):
        case Ok(pair):
            pass
        case Err(failed):
            pytest.fail(f'{name}: {failed!r}')
    for side in (pair.reference, pair.candidate):
        assert side.worker.end is StopEnd.EXITED, side.worker.log_tail
        assert side.worker.exit_code == 0, side.worker.log_tail
    assert not pair.snapshot_diff.entries, render_diff(pair.snapshot_diff)
    assert not pair.report_differences, pair.report_differences
    statuses = tuple(
        report.state.status
        if isinstance(report.state, Terminal)
        else repr(report.state)
        for report in (pair.reference.report[send.task_id] for send in scenario.sends)
    )
    assert statuses == case.statuses
