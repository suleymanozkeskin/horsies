"""Whole-run pairs of the worker (`tests/dualrun/worker_run.py`).

Each test runs one scenario on the reference worker and on the candidate
worker, each with a fresh database, and requires an empty snapshot diff, an
equal report and a clean exit on both sides. The scenarios cover, on the
profiles where each applies:
- plain successes on each child kind;
- task errors with and without a retry;
- the isolated timeout and crash, the shared async timeout, the shared
  thread deadline;
- a blocked loop in a shared async child (the culprit retries isolated, the
  neighbour fails);
- left work of an isolated async task;
- stored options without a timeout on a shared row;
- a stored class that does not fit the registered task (the row goes back
  as isolated);
- a workflow chain with a sub-workflow that the worker builds;
- a pause of a workflow while its first node runs;
- recycling by task count;
- a drain with tasks in flight.

Run: `pytest tests/dualrun/test_worker_run.py -m e2e` with a test server
(`HORSIES_TEST_DATABASE_URL`) and a built `horsies._core`.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from dataclasses import dataclass

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.apps import whole_run_app as whole
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.worker_run import (
    DropOptionKey,
    PauseWhenRunning,
    ReportEntry,
    ReportKey,
    Scenario,
    SetStoredClass,
    StopEnd,
    StopWhenRunning,
    TaskReport,
    Terminal,
    WorkflowKey,
    WorkflowReport,
    build_scenario,
    run_pair,
)

pytestmark = [pytest.mark.e2e]

MakeDatabase = Callable[[], DualRunDatabase]

BATCH: int = 64
"""The plain successes of one scenario: more than one claim batch."""


@dataclass(frozen=True, slots=True)
class _Case:
    """A scenario, the terminal status of each of its sends, and the status
    of each of its workflows."""

    build: Callable[[], Scenario]
    statuses: tuple[str, ...]
    workflow_statuses: tuple[str, ...] = ()


def _pause_scenario() -> Scenario:
    """The pause chain, paused once its first node runs."""
    scenario = build_scenario(
        'workflow-pause-during-a-run',
        whole.ISOLATED,
        [],
        workflows=[whole.pause_chain],
    )
    return build_scenario_with_stop(scenario)


def build_scenario_with_stop(scenario: Scenario) -> Scenario:
    """``scenario`` with the pause of its one workflow as its stop rule."""
    (start,) = scenario.workflows
    return dataclasses.replace(
        scenario,
        stop=PauseWhenRunning(
            workflow_id=start.workflow_id,
            running=whole.FIRST_NODE_RUNNING,
            release=whole.FIRST_NODE_RELEASED,
        ),
    )


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
    'shared-async-blocked-loop': _Case(
        build=lambda: build_scenario(
            'shared-async-blocked-loop',
            whole.SPLIT,
            [(whole.async_neighbour, None), (whole.blocks_loop_once, None)],
        ),
        statuses=('FAILED', 'COMPLETED'),
    ),
    'isolated-left-work': _Case(
        build=lambda: build_scenario(
            'isolated-left-work',
            whole.ISOLATED,
            [(whole.leaves_work, _value(7)), (whole.double, _value(8))],
        ),
        statuses=('COMPLETED', 'COMPLETED'),
    ),
    'shared-async-stored-options-refused': _Case(
        build=lambda: build_scenario(
            'shared-async-stored-options-refused',
            whole.SHARED_ASYNC,
            [(whole.double_async, _value(9)), (whole.double_async, _value(10))],
            row_edits=[DropOptionKey(send=0, key='timeout_ms')],
        ),
        statuses=('FAILED', 'COMPLETED'),
    ),
    'stored-class-does-not-fit': _Case(
        build=lambda: build_scenario(
            'stored-class-does-not-fit',
            whole.SPLIT,
            [(whole.double, _value(11))],
            row_edits=[SetStoredClass(send=0, execution='SHARED_ASYNC')],
        ),
        statuses=('COMPLETED',),
    ),
    'workflow-chain-with-subworkflow': _Case(
        build=lambda: build_scenario(
            'workflow-chain-with-subworkflow',
            whole.ISOLATED,
            [],
            workflows=[whole.chain_with_subworkflow],
        ),
        statuses=(),
        workflow_statuses=('COMPLETED',),
    ),
    'workflow-pause-during-a-run': _Case(
        build=_pause_scenario,
        statuses=(),
        workflow_statuses=('PAUSED',),
    ),
    'isolated-recycling-by-task-count': _Case(
        build=lambda: build_scenario(
            'isolated-recycling-by-task-count',
            whole.ISOLATED_RECYCLED,
            [(whole.double, _value(value)) for value in range(6)],
        ),
        statuses=('COMPLETED',) * 6,
    ),
    'shared-async-drain-with-tasks-in-flight': _Case(
        build=lambda: build_scenario(
            'shared-async-drain-with-tasks-in-flight',
            whole.SHARED_ASYNC,
            [(whole.sleep_then_double, _value(value)) for value in range(4)],
            stop=StopWhenRunning(),
        ),
        statuses=('COMPLETED',) * 4,
    ),
    'shared-thread-deadline': _Case(
        build=lambda: build_scenario(
            'shared-thread-deadline',
            whole.SHARED_THREAD,
            [(whole.thread_past_deadline, None), (whole.double_thread, _value(12))],
        ),
        statuses=('FAILED', 'COMPLETED'),
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
    assert _send_statuses(pair.reference.report, scenario) == case.statuses
    assert _workflow_statuses(pair.reference.report, scenario) == case.workflow_statuses


def _send_statuses(
    report: dict[ReportKey, ReportEntry],
    scenario: Scenario,
) -> tuple[str, ...]:
    statuses: list[str] = []
    for send in scenario.sends:
        match report[send.task_id]:
            case TaskReport(state=Terminal(status=status)):
                statuses.append(status)
            case other:
                statuses.append(repr(other))
    return tuple(statuses)


def _workflow_statuses(
    report: dict[ReportKey, ReportEntry],
    scenario: Scenario,
) -> tuple[str, ...]:
    statuses: list[str] = []
    for start in scenario.workflows:
        match report[WorkflowKey(workflow_id=start.workflow_id)]:
            case WorkflowReport(status=status):
                statuses.append(status)
            case other:
                statuses.append(repr(other))
    return tuple(statuses)
