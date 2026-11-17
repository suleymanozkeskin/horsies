"""Dual-run of the workflow and schedule switch: the client statements against the functions.

Reference: ``tests/dualrun/monitoring_workflow_reference.py``. Candidate:
one call of ``horsies_monitoring_workflow_names``,
``horsies_monitoring_workflow_runs``, ``horsies_monitoring_workflow_run`` and
``horsies_monitoring_schedules``, Python's bodies before the monitoring
switch (``tests/dualrun/reference/monitoring_queries.py``). The seed has
root runs of several names and statuses (three created at one time), a
child run with nodes, nodes with dependencies (one on an index the run does
not have), sub-workflow nodes with and without a child run, and schedules
(three due at one time). Each read must give equal answers; a second seed
puts each bounded list past its bound.

The caller switch pair of the monitoring switch: on the same seeds, Python's
reads against the production reads, which the core runs. The reports must
be equal; an open run's ``wall_s`` and a RUNNING node's ``exec_s`` may be up
to ``COUNTING_SPAN_TOLERANCE_S`` longer on the core, which reads second.
"""

from __future__ import annotations

import typing
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Final

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.monitoring import (
    WorkflowRunsLimit,
    get_workflow_run,
    list_schedules,
    list_workflow_names,
    list_workflow_runs,
)
from horsies.monitoring.queries import LIST_ROW_BOUND
from horsies.core.types.result import Ok
from tests.dualrun import monitoring_workflow_reference as reference
from tests.dualrun.monitoring_reads import (
    MakeDatabase,
    ReadPair,
    differing,
    differing_reports,
    read_dual_run,
)
from tests.dualrun.reference import monitoring_queries as python_reads
from tests.integration.monitoring_seeding import (
    LastRun,
    NodeRow,
    ScheduleRow,
    SeedRow,
    SubworkflowNode,
    TaskNode,
    WorkflowParent,
    WorkflowRow,
)

pytestmark = [pytest.mark.integration]

_NOW: Final = datetime.now(UTC)
_TIE: Final = _NOW - timedelta(seconds=300)

_ROOT: Final = str(uuid.uuid4())
_CHILD: Final = str(uuid.uuid4())
_OTHER_ROOTS: Final = tuple(str(uuid.uuid4()) for _ in range(5))


def _run(
    run_id: str,
    name: str,
    *,
    status: str,
    created_at: datetime,
    parent: WorkflowParent | None = None,
    completed_at: datetime | None = None,
) -> WorkflowRow:
    return WorkflowRow(
        id=run_id,
        name=name,
        status=status,
        on_error='fail',
        definition_key=None if name == 'gamma.flow' else f'{name}.v1',
        parent=parent,
        sent_at=created_at,
        created_at=created_at,
        completed_at=completed_at,
    )


def _node(
    workflow_id: str,
    task_index: int,
    *,
    status: str,
    dependencies: tuple[int, ...] = (),
    kind: TaskNode | SubworkflowNode = TaskNode(task_id=None),
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> NodeRow:
    return NodeRow(
        id=str(uuid.uuid4()),
        workflow_id=workflow_id,
        task_index=task_index,
        node_id=f'node-{task_index}',
        task_name='workflow.task',
        status=status,
        dependencies=dependencies,
        allow_failed_deps=task_index == 3,
        kind=kind,
        result=None,
        created_at=_NOW - timedelta(seconds=400),
        started_at=started_at,
        completed_at=completed_at,
    )


def _seed() -> tuple[SeedRow, ...]:
    runs = (
        _run(_ROOT, 'alpha.flow', status='RUNNING', created_at=_NOW - timedelta(seconds=400)),
        _run(_OTHER_ROOTS[0], 'beta.flow', status='COMPLETED', created_at=_TIE, completed_at=_NOW),
        _run(_OTHER_ROOTS[1], 'beta.flow', status='FAILED', created_at=_TIE, completed_at=_NOW),
        _run(_OTHER_ROOTS[2], 'gamma.flow', status='RUNNING', created_at=_TIE),
        _run(_OTHER_ROOTS[3], 'alpha.flow', status='PAUSED', created_at=_NOW - timedelta(seconds=100)),
        _run(_OTHER_ROOTS[4], 'Delta.flow', status='CANCELLED', created_at=_NOW - timedelta(seconds=50)),
        _run(
            _CHILD,
            'child.flow',
            status='FAILED',
            created_at=_NOW - timedelta(seconds=200),
            parent=WorkflowParent(workflow_id=_ROOT, task_index=2, root_workflow_id=_ROOT),
            completed_at=_NOW - timedelta(seconds=10),
        ),
    )
    nodes = (
        _node(_ROOT, 0, status='COMPLETED', started_at=_NOW - timedelta(seconds=390), completed_at=_NOW - timedelta(seconds=380)),
        _node(_ROOT, 1, status='FAILED', dependencies=(0,), started_at=_NOW - timedelta(seconds=370), completed_at=_NOW - timedelta(seconds=360)),
        _node(_ROOT, 2, status='RUNNING', dependencies=(0, 1), kind=SubworkflowNode(sub_workflow_id=_CHILD), started_at=_NOW - timedelta(seconds=200)),
        _node(_ROOT, 3, status='PENDING', dependencies=(2, 9)),
        _node(_ROOT, 4, status='PENDING', dependencies=(3,), kind=SubworkflowNode(sub_workflow_id=None)),
        _node(_CHILD, 0, status='COMPLETED', started_at=_NOW - timedelta(seconds=190), completed_at=_NOW - timedelta(seconds=180)),
        _node(_CHILD, 1, status='FAILED', dependencies=(0,), started_at=_NOW - timedelta(seconds=170), completed_at=_NOW - timedelta(seconds=160)),
        _node(_CHILD, 2, status='SKIPPED', dependencies=(1,)),
    )
    schedules = (
        ScheduleRow(schedule_name='nightly', next_run_at=_NOW + timedelta(hours=8), last_run=None),
        ScheduleRow(
            schedule_name='zeta',
            next_run_at=_NOW + timedelta(minutes=5),
            last_run=LastRun(at=_NOW - timedelta(minutes=55), task_id=str(uuid.uuid4()), run_count=11),
        ),
        ScheduleRow(schedule_name='alpha', next_run_at=_NOW + timedelta(minutes=5), last_run=None),
        ScheduleRow(schedule_name='Beta', next_run_at=_NOW + timedelta(minutes=5), last_run=None),
    )
    return runs + nodes + schedules


type _Read = Callable[[PostgresBroker], Awaitable[object]]


def _row_count(answer: object) -> int:
    """The number of rows of an ``Ok`` list answer."""
    assert isinstance(answer, Ok), answer
    value: object = typing.cast(Ok[object], answer).ok_value
    assert isinstance(value, list), value
    return len(typing.cast(list[object], value))


def _pair(name: str, reference_read: _Read, candidate: _Read) -> ReadPair[object]:
    return ReadPair(name=name, reference=reference_read, candidate=candidate)


def _three_way(
    name: str, statements: _Read, python: _Read, core: _Read, *, on_core: bool,
) -> ReadPair[object]:
    """The SQL redesign pair (the client statements against Python's read on
    the function), or with ``on_core`` the caller switch pair (Python's read
    against the production read on the core)."""
    match on_core:
        case False:
            return _pair(name, statements, python)
        case True:
            return _pair(name, python, core)


def _names_pair(*, on_core: bool = False) -> ReadPair[object]:
    async def statements(broker: PostgresBroker) -> object:
        return await reference.list_workflow_names(broker)

    async def python(broker: PostgresBroker) -> object:
        return await python_reads.list_workflow_names(broker)

    async def core(broker: PostgresBroker) -> object:
        return await list_workflow_names(broker)

    return _three_way('names', statements, python, core, on_core=on_core)


def _schedules_pair(*, on_core: bool = False) -> ReadPair[object]:
    async def statements(broker: PostgresBroker) -> object:
        return await reference.list_schedules(broker)

    async def python(broker: PostgresBroker) -> object:
        return await python_reads.list_schedules(broker)

    async def core(broker: PostgresBroker) -> object:
        return await list_schedules(broker)

    return _three_way('schedules', statements, python, core, on_core=on_core)


def _runs_pair(
    name: str | None, status: str | None, limit: int, *, on_core: bool = False,
) -> ReadPair[object]:
    runs_limit = WorkflowRunsLimit(value=limit)

    async def statements(broker: PostgresBroker) -> object:
        return await reference.list_workflow_runs(broker, name=name, status=status, limit=runs_limit)

    async def python(broker: PostgresBroker) -> object:
        return await python_reads.list_workflow_runs(broker, name=name, status=status, limit=runs_limit)

    async def core(broker: PostgresBroker) -> object:
        return await list_workflow_runs(broker, name=name, status=status, limit=runs_limit)

    return _three_way(
        f'runs name={name} status={status} limit={limit}', statements, python, core, on_core=on_core,
    )


def _run_pair(workflow_id: str, *, on_core: bool = False) -> ReadPair[object]:
    async def statements(broker: PostgresBroker) -> object:
        return await reference.get_workflow_run(broker, workflow_id)

    async def python(broker: PostgresBroker) -> object:
        return await python_reads.get_workflow_run(broker, workflow_id)

    async def core(broker: PostgresBroker) -> object:
        return await get_workflow_run(broker, workflow_id)

    return _three_way(f'run {workflow_id}', statements, python, core, on_core=on_core)


def _pairs(*, on_core: bool) -> list[ReadPair[object]]:
    pairs = [_names_pair(on_core=on_core), _schedules_pair(on_core=on_core)]
    pairs += [
        _runs_pair(name, status, limit, on_core=on_core)
        for name in (None, 'alpha.flow', 'beta.flow', 'child.flow')
        for status in (None, 'RUNNING', 'FAILED', 'NOT_A_STATUS')
        for limit in (1, 2, 200)
    ]
    pairs += [
        _run_pair(workflow_id, on_core=on_core)
        for workflow_id in (_ROOT, _CHILD, _OTHER_ROOTS[0], str(uuid.uuid4()), 'not-a-run-id')
    ]
    return pairs


def test_the_functions_answer_what_the_client_statements_answered(
    empty_database: MakeDatabase,
) -> None:
    pairs = _pairs(on_core=False)

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert len(answers) == 2 + 48 + 5
    assert differing(answers) == []
    assert [each.name for each in answers if not isinstance(each.candidate, Ok)] == []
    # The tie of three root runs, newest first, then by id highest first.
    unfiltered = next(each for each in answers if each.name == 'runs name=None status=None limit=200')
    assert _row_count(unfiltered.candidate) == 6


_PAST_THE_BOUND: Final = (
    'INSERT INTO horsies_schedule_state ('
    'schedule_name, next_run_at, last_run_at, last_task_id, run_count, config_hash, updated_at'
    ") SELECT 'bound_' || lpad(n::text, 6, '0'), now() + interval '1 day', NULL, NULL, 0, "
    'sha256(n::text::bytea), now() FROM generate_series(1, 10001) AS n',
    'INSERT INTO horsies_workflows ('
    'id, name, definition_key, status, on_error, output_task_index, success_policy, result, '
    'error, parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, '
    'created_at, started_at, completed_at, updated_at'
    ") SELECT run.id, 'bound_' || lpad(run.n::text, 6, '0'), NULL, 'RUNNING', 'fail', NULL, "
    'NULL, NULL, NULL, NULL, NULL, run.id, 0, now(), now(), NULL, NULL, now() '
    'FROM (SELECT horsies_uuidv7() AS id, n FROM generate_series(1, 10001) AS n) AS run',
)


def test_the_bounded_lists_cut_at_one_row(empty_database: MakeDatabase) -> None:
    answers = read_dual_run(
        empty_database,
        _seed(),
        [_names_pair(), _schedules_pair()],
        seed_statements=_PAST_THE_BOUND,
    )

    assert differing(answers) == []
    assert [_row_count(each.candidate) for each in answers] == [LIST_ROW_BOUND] * 2


def test_the_comparison_sees_a_candidate_that_reads_another_run(
    empty_database: MakeDatabase,
) -> None:
    pair = _run_pair(_ROOT)

    async def wrong(broker: PostgresBroker) -> object:
        return await get_workflow_run(broker, _CHILD)

    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=wrong)],
    )

    assert differing(answers) == [pair.name]


def test_the_core_answers_what_python_answered(empty_database: MakeDatabase) -> None:
    """The caller switch: Python's workflow and schedule reads against the production reads on the core."""
    pairs = _pairs(on_core=True)

    answers = read_dual_run(empty_database, _seed(), pairs)

    assert len(answers) == 2 + 48 + 5
    assert [each.name for each in answers if not isinstance(each.candidate, Ok)] == []
    assert differing_reports(answers) == {}


def test_the_core_cuts_the_bounded_lists_at_the_rows_python_cut(empty_database: MakeDatabase) -> None:
    answers = read_dual_run(
        empty_database,
        _seed(),
        [_names_pair(on_core=True), _schedules_pair(on_core=True)],
        seed_statements=_PAST_THE_BOUND,
    )

    assert differing_reports(answers) == {}
    assert [_row_count(each.candidate) for each in answers] == [LIST_ROW_BOUND] * 2


def test_the_core_comparison_sees_a_candidate_that_reads_another_run(
    empty_database: MakeDatabase,
) -> None:
    pair = _run_pair(_ROOT, on_core=True)

    async def wrong(broker: PostgresBroker) -> object:
        return await get_workflow_run(broker, _CHILD)

    answers = read_dual_run(
        empty_database,
        _seed(),
        [ReadPair(name=pair.name, reference=pair.reference, candidate=wrong)],
    )

    assert list(differing_reports(answers)) == [pair.name]
