"""Dual-run of the start switch: the session statements against ``horsies_workflow_start``.

Reference: ``tests/dualrun/reference/workflow_start.py``. Candidate:
``start_workflow_async`` (``core/workflows/lifecycle.py``). Each side starts,
on an empty baseline with one seeded workflow:

- a tree under ``on_error`` fail: two task roots, a dependent of one, a
  sub-workflow root whose child builds (a task root and its dependent), a
  sub-workflow root whose child build raises, a task dependent of that root
  (skipped), and a sub-workflow dependent that allows a failed dependency
  (its child start is owed in the start's transaction);
- a tree under ``on_error`` pause: a task root, a sub-workflow root whose
  child build raises, and a later sub-workflow root whose child builds (the
  pause leaves it READY with no child);
- two trees under ``on_error`` pause where the order of the roots decides
  the rows: a root whose child builds before a root whose build raises (the
  child and the task row of its root go in, then the pause pauses them); two
  roots whose builds raise (the second stays READY with no error);
- a spec with the id and the name of the seeded workflow, and a spec with
  its id and another name.

The snapshots compare equal.

The caller pair runs the same starts with the Python caller before the
switch (``tests/dualrun/reference/workflow_start_caller.py``) against the
caller on the core (``CoreHandle.start_workflow_async``), with one fixed
``sent_at``. Each side also reports how many times ``build_with`` ran per
definition key: the core runs it as often as Python.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, assert_never

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.registry import register_workflow_definition
from horsies.core.workflows.start_types import (
    WorkflowStartIdConflict,
    WorkflowStartResult,
)
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationSide, dual_run
from tests.dualrun.reference import workflow_start as reference
from tests.dualrun.reference import workflow_start_caller as caller_reference
from tests.dualrun.workflow_seeds import (
    OnError as SeedOnError,
    TopLevel,
    WorkflowSeed,
    run_or_fail,
    start_tree,
    task_node,
)

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://dualrun@127.0.0.1:1/unused'),
        ),
    ),
)


_BUILDS: Counter[str] = Counter()
"""The runs of ``build_with`` per definition key, since the last reset."""


@APP.task(task_name='dualrun.start.value')
def value_task(*, value: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=value)


class BuiltChild(WorkflowDefinition[int]):
    """A child whose build gives a task root and its dependent."""

    name = 'dualrun.start.child'
    definition_key = 'dualrun.start.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        _BUILDS[cls.definition_key] += 1
        root = TaskNode(fn=value_task, kwargs={'value': 20})
        dependent = TaskNode(fn=value_task, kwargs={'value': 21}, waits_for=[root])
        return app.workflow(
            cls.name, [root, dependent], output=dependent, definition_key=cls.definition_key,
        )


class BrokenChild(WorkflowDefinition[int]):
    """A child whose build raises."""

    name = 'dualrun.start.broken'
    definition_key = 'dualrun.start.broken.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (app, args, params)
        _BUILDS[cls.definition_key] += 1
        raise RuntimeError('the child cannot be built')


@pytest.fixture(autouse=True)
def registered_definitions() -> None:
    """The child definitions, registered again: other tests clear the
    definition registry."""
    for definition in (BuiltChild, BrokenChild):
        register_workflow_definition(definition)


def _failing_tree() -> WorkflowSpec[Any]:
    first = TaskNode(fn=value_task, kwargs={'value': 1})
    second = TaskNode(fn=value_task, kwargs={'value': 2})
    after_first = TaskNode(fn=value_task, kwargs={'value': 3}, waits_for=[first])
    built = SubWorkflowNode(workflow_def=BuiltChild)
    broken = SubWorkflowNode(workflow_def=BrokenChild)
    after_broken = TaskNode(fn=value_task, kwargs={'value': 4}, waits_for=[broken])
    started_after_broken = SubWorkflowNode(
        workflow_def=BuiltChild, waits_for=[broken], allow_failed_deps=True,
    )
    return APP.workflow(
        'dualrun.start.fail_tree',
        [first, second, after_first, built, broken, after_broken, started_after_broken],
        on_error=OnError.FAIL,
        definition_key='dualrun.start.fail_tree.v1',
    )


def _pausing_tree() -> WorkflowSpec[Any]:
    root = TaskNode(fn=value_task, kwargs={'value': 5})
    broken = SubWorkflowNode(workflow_def=BrokenChild)
    built_after_broken = SubWorkflowNode(workflow_def=BuiltChild)
    return APP.workflow(
        'dualrun.start.pause_tree',
        [root, broken, built_after_broken],
        on_error=OnError.PAUSE,
        definition_key='dualrun.start.pause_tree.v1',
    )


def _built_then_broken_tree() -> WorkflowSpec[Any]:
    built = SubWorkflowNode(workflow_def=BuiltChild)
    broken = SubWorkflowNode(workflow_def=BrokenChild)
    return APP.workflow(
        'dualrun.start.built_then_broken',
        [built, broken],
        on_error=OnError.PAUSE,
        definition_key='dualrun.start.built_then_broken.v1',
    )


def _broken_twice_tree() -> WorkflowSpec[Any]:
    first = SubWorkflowNode(workflow_def=BrokenChild)
    second = SubWorkflowNode(workflow_def=BrokenChild)
    return APP.workflow(
        'dualrun.start.broken_twice',
        [first, second],
        on_error=OnError.PAUSE,
        definition_key='dualrun.start.broken_twice.v1',
    )


_EXISTING_NAME = 'dualrun.start.existing'


def _named(name: str) -> WorkflowSpec[Any]:
    root = TaskNode(fn=value_task, kwargs={'value': 6})
    return APP.workflow(name, [root], definition_key=f'{name}.v1')


@dataclass(frozen=True, slots=True)
class _Seed:
    failing: uuid.UUID
    pausing: uuid.UUID
    built_then_broken: uuid.UUID
    broken_twice: uuid.UUID
    existing: uuid.UUID


def _seed(connection: Connection) -> _Seed:
    """One RUNNING workflow, whose id the last two starts reuse."""
    existing = uuid.uuid4()
    start_tree(
        connection,
        [
            WorkflowSeed(
                workflow_id=existing,
                name=_EXISTING_NAME,
                on_error=SeedOnError.FAIL,
                parent=TopLevel(),
                nodes=(task_node(0, 'dualrun.start.value'),),
            ),
        ],
    )
    return _Seed(
        failing=uuid.uuid4(),
        pausing=uuid.uuid4(),
        built_then_broken=uuid.uuid4(),
        broken_twice=uuid.uuid4(),
        existing=existing,
    )


def _starts(seed: _Seed) -> tuple[tuple[WorkflowSpec[Any], uuid.UUID], ...]:
    return (
        (_failing_tree(), seed.failing),
        (_pausing_tree(), seed.pausing),
        (_built_then_broken_tree(), seed.built_then_broken),
        (_broken_twice_tree(), seed.broken_twice),
        (_named(_EXISTING_NAME), seed.existing),
        (_named('dualrun.start.other_name'), seed.existing),
    )


def _attach(broker: PostgresBroker) -> None:
    broker.app = APP
    APP._broker = broker  # pyright: ignore[reportPrivateUsage]


async def _reference_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.StartReport, ...]:
    async with open_broker(database) as broker:
        _attach(broker)
        match await broker.select_resources():
            case Err(limit):
                raise AssertionError(f'no resources: {limit}')
            case Ok(resources):
                pass
        sent_at = datetime.now(timezone.utc)
        return tuple([
            await reference.start(
                resources.session_factory, broker, spec, str(workflow_id), sent_at,
            )
            for spec, workflow_id in _starts(seed)
        ])


def _reference(database: DualRunDatabase, seed: _Seed) -> tuple[reference.StartReport, ...]:
    return asyncio.run(_reference_async(database, seed))


def _report(result: WorkflowStartResult[Any]) -> reference.StartReport:
    match result:
        case Ok():
            return reference.Started()
        case Err(WorkflowStartIdConflict(existing_workflow_name=name)):
            return reference.IdConflict(existing_name=name)
        case Err(other):
            raise AssertionError(f'the start answered {other!r}')
        case _ as unreachable:
            assert_never(unreachable)


async def _candidate_async(
    database: DualRunDatabase, seed: _Seed,
) -> tuple[reference.StartReport, ...]:
    async with open_broker(database) as broker:
        _attach(broker)
        sent_at = datetime.now(timezone.utc)
        return tuple([
            _report(await start_workflow_async(spec, broker, str(workflow_id), sent_at))
            for spec, workflow_id in _starts(seed)
        ])


def _candidate(database: DualRunDatabase, seed: _Seed) -> tuple[reference.StartReport, ...]:
    return asyncio.run(_candidate_async(database, seed))


def test_start_writes_what_the_session_statements_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='session statements', act=_reference),
            OperationSide(name='horsies_workflow_start', act=_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == (
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.Existing(),
        reference.IdConflict(existing_name=_EXISTING_NAME),
    )
    # The candidate maps an existing workflow of the same name to its handle.
    assert run.candidate.report == (
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.IdConflict(existing_name=_EXISTING_NAME),
    )


_CALLER_SENT_AT = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)
"""The one start time of both sides of the caller pair."""


@dataclass(frozen=True, slots=True)
class _CallerReport:
    starts: tuple[reference.StartReport, ...]
    builds: tuple[tuple[str, int], ...]
    """The runs of ``build_with`` per definition key, sorted by key."""


StartFunction = Callable[..., Any]


async def _caller_async(
    database: DualRunDatabase, seed: _Seed, start: StartFunction,
) -> _CallerReport:
    _BUILDS.clear()
    async with open_broker(database) as broker:
        _attach(broker)
        starts = tuple([
            _report(await start(spec, broker, str(workflow_id), _CALLER_SENT_AT))
            for spec, workflow_id in _starts(seed)
        ])
    return _CallerReport(starts=starts, builds=tuple(sorted(_BUILDS.items())))


def _caller_reference(database: DualRunDatabase, seed: _Seed) -> _CallerReport:
    return asyncio.run(_caller_async(database, seed, caller_reference.start_workflow_async))


def _caller_candidate(database: DualRunDatabase, seed: _Seed) -> _CallerReport:
    return asyncio.run(_caller_async(database, seed, start_workflow_async))


def test_the_start_on_the_core_writes_what_the_python_caller_wrote(
    empty_database: MakeDatabase,
) -> None:
    run = run_or_fail(
        dual_run(
            empty_database,
            _seed,
            OperationSide(name='python caller', act=_caller_reference),
            OperationSide(name='core caller', act=_caller_candidate),
        )
    )

    assert run.diff.entries == (), render_diff(run.diff)
    expected = (
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.Started(),
        reference.IdConflict(existing_name=_EXISTING_NAME),
    )
    assert run.reference.report.starts == expected
    assert run.candidate.report.starts == expected
    assert run.candidate.report.builds == run.reference.report.builds
