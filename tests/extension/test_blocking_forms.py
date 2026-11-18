"""Each blocking form of ``CoreHandle`` that the sync facades call gives the
outcome of its awaitable form, on a real database and on one that refuses
the connect (``programs/blocking_forms.py``)."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pytest

from tests.extension.fresh_interpreter import (
    Database,
    NoDatabase,
    report_at,
    run_case,
)
from tests.extension.programs.report import JsonValue, Report

pytestmark = [pytest.mark.integration]

PROGRAM = 'blocking_forms'

LIFE: Final[list[tuple[str, str]]] = [
    ('start', 'WorkflowStarted'),
    ('start_again', 'WorkflowExists'),
    ('start_other_name', 'WorkflowIdTaken'),
    ('status', 'WorkflowStatusRead'),
    ('wait_no_time', 'WaitTimedOut'),
    ('node_result', 'NodeResultNotStored'),
    ('node_results', 'WorkflowNodeResultsRead'),
    ('nodes', 'WorkflowNodesRead'),
    ('pause', 'WorkflowPaused'),
    ('pause_again', 'WorkflowNotRunning'),
    ('wait_paused', 'WorkflowPausedRead'),
    ('resume', 'WorkflowResumed'),
    ('resume_again', 'WorkflowNotPaused'),
    ('cancel', 'WorkflowCancelled'),
    ('cancel_again', 'WorkflowNotCancellable'),
    ('wait_no_limit', 'WorkflowEndedRead'),
]
"""The record class of each step of a workflow's life, in both runs."""

ABSENT: Final[list[tuple[str, str]]] = [
    ('status', 'WorkflowAbsent'),
    ('wait', 'WorkflowAbsent'),
    ('node_result', 'NodeResultNotStored'),
    ('node_results', 'WorkflowAbsent'),
    ('nodes', 'WorkflowAbsent'),
    ('pause', 'WorkflowAbsent'),
    ('resume', 'WorkflowAbsent'),
    ('cancel', 'WorkflowAbsent'),
]
"""The record class of each operation on an id that no workflow has."""


def _runs(report: Report) -> tuple[JsonValue, JsonValue]:
    return report['blocking'], report['awaited']


@pytest.fixture(scope='module')
def workflow_report(
    tmp_path_factory: pytest.TempPathFactory, extension_database: Database,
) -> Report:
    return run_case(
        PROGRAM, 'workflow_operations', tmp_path_factory.mktemp('workflow'), extension_database,
    ).report


def test_each_blocking_workflow_form_gives_the_outcome_of_its_awaitable_form(
    workflow_report: Report,
) -> None:
    blocking, awaited = _runs(report_at(workflow_report, 'lives'))

    assert blocking == awaited
    pairs = _pairs(blocking)
    assert [(step, _class_of(outcome)) for step, outcome in pairs] == LIFE
    outcomes = dict(pairs)
    assert outcomes['status'] == ['WorkflowStatusRead', 'RUNNING']
    assert outcomes['start_other_name'] == ['WorkflowIdTaken', 'tests.extension.blocking_forms']
    assert outcomes['node_results'] == ['WorkflowNodeResultsRead', []]
    # A cancel writes no error.
    assert outcomes['wait_no_limit'] == ['WorkflowEndedRead', 'CANCELLED', True]


def test_each_blocking_form_on_an_absent_workflow_gives_the_awaitable_outcome(
    workflow_report: Report,
) -> None:
    blocking, awaited = _runs(report_at(workflow_report, 'absent'))

    assert blocking == awaited
    assert [(step, _class_of(outcome)) for step, outcome in _pairs(blocking)] == ABSENT


def test_each_blocking_monitoring_form_gives_the_outcome_of_its_awaitable_form(
    tmp_path: Path, extension_database: Database,
) -> None:
    report = run_case(PROGRAM, 'monitoring_operations', tmp_path, extension_database).report
    blocking, awaited = _runs(report)

    assert blocking == awaited
    outcomes = dict(_pairs(blocking))
    assert outcomes['worker_state'] == ['WorkerStateAbsent']
    assert outcomes['worker_state_history'] == ['WorkerStatesRecord', 0]
    assert outcomes['worker_state_history_refused'] == ['BrokerFailure', 'INVALID_INPUT', False]
    assert outcomes['ping'] == ['WorkerPongsRecord', 0]
    assert outcomes['ping_refused'] == ['BrokerFailure', 'INVALID_INPUT', False]
    assert _class_of(outcomes['worker_states']) == 'WorkerStatesRecord'
    assert _class_of(outcomes['class_coverage']) == 'ClassCoverageRecord'
    assert _class_of(outcomes['class_report']) == 'ClassReportRecord'


def test_each_blocking_form_on_an_unreachable_database_gives_the_awaitable_failure(
    tmp_path: Path,
) -> None:
    report = run_case(PROGRAM, 'unreachable_database', tmp_path, NoDatabase()).report
    blocking, awaited = _runs(report)

    assert blocking == awaited
    outcomes = dict(_pairs(blocking))
    assert _class_of(outcomes['start']) == 'WorkflowStartFailed'
    for read in ('status', 'wait', 'node_result', 'node_results', 'nodes', 'pause', 'resume', 'cancel'):
        assert _class_and_code(outcomes[read]) == (
            'HandleDbOperationFailed', 'DB_OPERATION_FAILED',
        ), read
    for read in ('worker_states', 'worker_state', 'worker_state_history', 'class_coverage', 'class_report'):
        assert _class_and_code(outcomes[read]) == ('BrokerFailure', 'MONITORING_QUERY_FAILED'), read
    # Refused before any core call: the same answer as on a real database.
    assert outcomes['worker_state_history_refused'] == ['BrokerFailure', 'INVALID_INPUT', False]
    assert outcomes['ping_refused'] == ['BrokerFailure', 'INVALID_INPUT', False]
    assert _class_of(outcomes['ping']) == 'BrokerFailure'


def _pairs(run: JsonValue) -> list[tuple[str, JsonValue]]:
    """The (step, outcome) pairs of one run of a report."""
    match run:
        case list() as steps:
            pairs: list[tuple[str, JsonValue]] = []
            for step in steps:
                match step:
                    case [str() as name, outcome]:
                        pairs.append((name, outcome))
                    case _:
                        raise AssertionError(f'a step is [name, outcome]: {step!r}')
            return pairs
        case _:
            raise AssertionError(f'a run is a list of steps: {run!r}')


def _class_of(outcome: JsonValue) -> JsonValue:
    """The record class of a described outcome."""
    match outcome:
        case [name, *_]:
            return name
        case _:
            raise AssertionError(f'an outcome starts with its record class: {outcome!r}')


def _class_and_code(outcome: JsonValue) -> tuple[JsonValue, JsonValue]:
    """The record class and the code of a described failure."""
    match outcome:
        case [name, code, *_]:
            return name, code
        case _:
            raise AssertionError(f'a failure is [class, code, ...]: {outcome!r}')
