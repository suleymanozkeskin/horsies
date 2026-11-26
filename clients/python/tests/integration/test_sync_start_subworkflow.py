"""A sync start of a workflow whose root is a sub-workflow node runs the
child's ``build_with``.

The sync start (``WorkflowSpec.start``, ``start_workflow``) calls
``CoreHandle.start_workflow`` on the calling thread with the GIL released.
The core builds the pre-built child of the sub-workflow root through the
broker's child spec source, which runs ``build_with`` on the core's callback
thread while the calling thread waits. The start runs on a thread with no
event loop, as in a sync application, and is bounded by
``START_BOUND_SECONDS``: a calling thread that held the GIL would never let
the build run.
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Final

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.types.result import Ok, Result, is_ok
from horsies.core.workflows.registry import (
    register_workflow_definition,
    unregister_workflow_definition,
)
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

START_BOUND_SECONDS: Final = 60.0
"""The longest wait for the sync start on its thread."""

CHILD_VALUE: Final = 7
"""The node input of the sub-workflow root, passed to ``build_with``."""


@dataclass(frozen=True)
class BuildSeen:
    """One run of the child's ``build_with``: its input and its thread."""

    value: int
    thread_id: int


SEEN: list[BuildSeen] = []
SEEN_LOCK = threading.Lock()

APP: Final = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url=SecretStr(DB_URL)),
    ),
)


@APP.task(name='sync_start_subworkflow.value')
def value_task(*, value: int) -> Result[int, TaskError]:
    return Ok(value)


class SyncStartChild(WorkflowDefinition[int]):
    """A child of one task node; its build records each run."""

    name = 'sync_start_subworkflow.child'
    definition_key = 'sync_start_subworkflow.child.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls, app: Horsies, *, value: int,
    ) -> WorkflowSpec[int]:
        with SEEN_LOCK:
            SEEN.append(BuildSeen(value=value, thread_id=threading.get_ident()))
        node: TaskNode[int] = TaskNode(function=value_task, kwargs={'value': value})
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


@pytest.fixture(scope='module')
def schema_ready() -> None:
    """Create the schema with a default broker, then close that broker."""

    async def initialize() -> None:
        broker = PostgresBroker(PostgresConfig(database_url=SecretStr(DB_URL)))
        initialized = await broker.ensure_schema_initialized()
        closed = await broker.close_async()
        assert initialized.is_ok(), initialized
        assert closed.is_ok(), closed

    asyncio.run(initialize())


@pytest.fixture
def registered_child(schema_ready: None) -> Generator[None, None, None]:
    """The child definition, registered for the test; the build records start empty."""
    with SEEN_LOCK:
        SEEN.clear()
    register_workflow_definition(SyncStartChild)
    yield
    unregister_workflow_definition(SyncStartChild.definition_key)
    closed = APP.get_broker().close()
    assert closed.is_ok(), closed


def _on_a_thread[T](call: Callable[[], T]) -> tuple[T, int]:
    """Run ``call`` on a new thread with no event loop; fail if it does not
    return within ``START_BOUND_SECONDS``. Returns its value and the thread id."""
    box: list[tuple[T, int]] = []
    thread = threading.Thread(
        target=lambda: box.append((call(), threading.get_ident())), daemon=True,
    )
    thread.start()
    thread.join(START_BOUND_SECONDS)
    match box:
        case [outcome]:
            return outcome
        case _:
            pytest.fail(f'the sync start did not return within {START_BOUND_SECONDS} s')


def test_a_sync_start_with_a_sub_workflow_root_runs_build_with(
    registered_child: None,
) -> None:
    root: SubWorkflowNode[int] = SubWorkflowNode(
        workflow_def=SyncStartChild, kwargs={'value': CHILD_VALUE},
    )
    spec = APP.workflow(
        'sync_start_subworkflow.parent',
        [root],
        output=root,
        definition_key='sync_start_subworkflow.parent.v1',
    )
    workflow_id = str(uuid.uuid4())

    started, caller_thread_id = _on_a_thread(lambda: spec.start(workflow_id))

    assert is_ok(started), started
    handle = started.ok_value
    assert handle.workflow_id == uuid.UUID(workflow_id)
    with SEEN_LOCK:
        seen = list(SEEN)
    # build_with ran once, with the node input, on a thread other than the
    # caller's: the core's callback thread, while the caller waited.
    assert [build.value for build in seen] == [CHILD_VALUE]
    assert seen[0].thread_id != caller_thread_id
    # The start wrote the pre-built child of the root.
    tasks, _ = _on_a_thread(handle.tasks)
    assert is_ok(tasks), tasks
    [node] = tasks.ok_value
    assert node.sub_workflow_id is not None
    cancelled, _ = _on_a_thread(handle.cancel)
    assert is_ok(cancelled), cancelled
