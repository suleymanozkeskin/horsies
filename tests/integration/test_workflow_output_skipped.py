"""A COMPLETED workflow whose output node has no result, through the real engine.

A success case that does not require the output node lets a workflow be
COMPLETED while that node is SKIPPED or FAILED. These tests run that path
against a database, with each reader of the stored result: ``get()``, the
parent node, a downstream node, ``result_for()``, ``results()``, ``tasks()``
and the workflow context.

A ``SubWorkflowNode`` is COMPLETED only when the child's output is ok. An err
output, or a stored child result that does not parse, makes a FAILED node, so
``allow_failed_deps=False``, ``join`` and ``SuccessCase`` hold by status. For a
result that does not parse, the node holds an envelope that names the cause, and
the raw text stays on the child row.
"""

from __future__ import annotations

import json
from typing import Any, TypeVar

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    SuccessCase,
    SuccessPolicy,
    TaskNode,
    WorkflowContext,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.workflows.engine import (
    drain_parent_propagations_in_session,
    on_subworkflow_complete,
    on_workflow_task_complete,
    pop_pending_parent_propagations,
)

from .conftest import start_ok

pytestmark = [pytest.mark.integration]

ValueT = TypeVar('ValueT')

OK_RESULT: TaskResult[int, TaskError] = TaskResult(ok=7)
FAILED_RESULT: TaskResult[int, TaskError] = TaskResult(
    err=TaskError(error_code='DELIBERATE_FAIL', message='node failed'),
)
FAILED_CODE = 'DELIBERATE_FAIL'


def _register_tasks(app: Horsies) -> tuple[Any, Any, Any]:
    """The tasks of a child workflow. The driver supplies each result."""

    @app.task(task_name='output_skipped_gate')
    def gate() -> TaskResult[int, TaskError]:
        return TaskResult(ok=1)

    @app.task(task_name='output_skipped_out')
    def out() -> TaskResult[int, TaskError]:
        return TaskResult(ok=2)

    @app.task(task_name='output_skipped_fallback')
    def fallback() -> TaskResult[int, TaskError]:
        return TaskResult(ok=3)

    return gate, out, fallback


class SkippedOutputChild(WorkflowDefinition[int]):
    """``gate`` fails, so ``out`` is SKIPPED. ``fallback`` satisfies the case.

    Node indexes: gate 0, out 1, fallback 2.
    """

    name = 'output_skipped_child'
    definition_key = 'tests.integration.output_skipped_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        gate_fn, out_fn, fallback_fn = _register_tasks(app)
        gate = TaskNode(fn=gate_fn)
        out = TaskNode(fn=out_fn, waits_for=[gate])
        fallback = TaskNode(fn=fallback_fn)
        return app.workflow(
            name=cls.name,
            tasks=[gate, out, fallback],
            output=out,
            on_error=OnError.FAIL,
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[fallback])]),
        )


class FailedOutputChild(WorkflowDefinition[int]):
    """``out`` fails. ``fallback`` satisfies the case.

    Node indexes: out 0, fallback 1.
    """

    name = 'output_failed_child'
    definition_key = 'tests.integration.output_failed_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        _, out_fn, fallback_fn = _register_tasks(app)
        out = TaskNode(fn=out_fn)
        fallback = TaskNode(fn=fallback_fn)
        return app.workflow(
            name=cls.name,
            tasks=[out, fallback],
            output=out,
            on_error=OnError.FAIL,
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[fallback])]),
        )


class MidOverSkippedChild(WorkflowDefinition[int]):
    """A workflow whose only node and output node is ``SkippedOutputChild``."""

    name = 'output_skipped_mid'
    definition_key = 'tests.integration.output_skipped_mid.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        node: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        return app.workflow(
            name=cls.name, tasks=[node], output=node, on_error=OnError.FAIL,
        )


class MidOverSkippedChildWithCase(WorkflowDefinition[int]):
    """The mid workflow's only case does not require the child node.

    Node indexes: child 0, spare 1.
    """

    name = 'output_skipped_mid_with_case'
    definition_key = 'tests.integration.output_skipped_mid_with_case.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='output_skipped_mid_spare')
        def spare_fn() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        node: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        spare = TaskNode(fn=spare_fn)
        return app.workflow(
            name=cls.name,
            tasks=[node, spare],
            output=node,
            on_error=OnError.FAIL,
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[spare])]),
        )


def _handle_value(result: Result[ValueT, Any]) -> ValueT:
    """The value of a handle call, or a test failure that names the error."""
    match result:
        case Ok(value):
            return value
        case Err(error):
            pytest.fail(f'handle call failed: {error}')


def _code(result: TaskResult[Any, TaskError]) -> str:
    """The error code of ``result`` as text: a built-in code or a user code."""
    assert result.is_err(), result
    code = result.err_value.error_code
    match code:
        case OutcomeCode() | OperationalErrorCode():
            return code.value
        case _:
            return str(code)


@pytest.mark.asyncio(loop_scope='function')
class TestOutputNodeUnderSuccessPolicy:
    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, '
                'horsies_tasks CASCADE',
            ),
        )
        await session.commit()
        return session, broker, app

    @staticmethod
    async def _node(session: AsyncSession, workflow_id: str, index: int) -> Any:
        rows = await session.execute(
            text("""
                SELECT status, result, task_id, task_name, sub_workflow_id,
                       sub_workflow_summary
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': index},
        )
        return rows.one()

    @staticmethod
    async def _workflow_row(session: AsyncSession, workflow_id: str) -> Any:
        rows = await session.execute(
            text('SELECT status, result FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': workflow_id},
        )
        return rows.one()

    async def _finish(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        workflow_id: str,
        index: int,
        result: TaskResult[int, TaskError],
    ) -> None:
        """Complete one node as the worker does, with parent propagation."""
        node = await self._node(session, workflow_id, index)
        await on_workflow_task_complete(
            session, node.task_id, result, broker, task_name=node.task_name,
        )
        await drain_parent_propagations_in_session(session, broker)
        await session.commit()

    async def _run_skipped_child(
        self, session: AsyncSession, broker: PostgresBroker, child_id: str,
    ) -> None:
        await self._finish(session, broker, child_id, 0, FAILED_RESULT)
        await self._finish(session, broker, child_id, 2, OK_RESULT)

    async def _run_failed_child(
        self, session: AsyncSession, broker: PostgresBroker, child_id: str,
    ) -> None:
        await self._finish(session, broker, child_id, 0, FAILED_RESULT)
        await self._finish(session, broker, child_id, 1, OK_RESULT)

    async def _run_child(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        child_def: type[WorkflowDefinition[int]],
        child_id: str,
    ) -> None:
        if child_def is SkippedOutputChild:
            await self._run_skipped_child(session, broker, child_id)
            return
        await self._run_failed_child(session, broker, child_id)

    async def _child_of(
        self, session: AsyncSession, workflow_id: str, index: int,
    ) -> str:
        child_id = (await self._node(session, workflow_id, index)).sub_workflow_id
        assert isinstance(child_id, str)
        return child_id

    async def test_skipped_output_gives_the_named_outcome(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        spec: WorkflowSpec[int] = SkippedOutputChild.build_with(app)
        handle = await start_ok(spec, broker)

        await self._run_skipped_child(session, broker, handle.workflow_id)

        row = await self._workflow_row(session, handle.workflow_id)
        assert row.status == 'COMPLETED'
        assert (await self._node(session, handle.workflow_id, 1)).status == 'SKIPPED'
        got = await handle.get_async(timeout_ms=2000)
        assert got.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert got.err_value.data == {
            'workflow_id': handle.workflow_id,
            'output_node_id': f'{SkippedOutputChild.name}:1',
            'output_task_index': 1,
            'output_task_name': 'output_skipped_out',
        }

    async def test_failed_output_outside_the_required_set_keeps_its_error(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        spec: WorkflowSpec[int] = FailedOutputChild.build_with(app)
        handle = await start_ok(spec, broker)

        await self._run_failed_child(session, broker, handle.workflow_id)

        row = await self._workflow_row(session, handle.workflow_id)
        node = await self._node(session, handle.workflow_id, 0)
        assert row.status == 'COMPLETED'
        assert node.status == 'FAILED'
        assert row.result == node.result
        got = await handle.get_async(timeout_ms=2000)
        assert got.err_value.error_code == FAILED_CODE

    @pytest.mark.parametrize(
        ('child_def', 'expected_code'),
        [
            (SkippedOutputChild, OutcomeCode.WORKFLOW_OUTPUT_SKIPPED.value),
            (FailedOutputChild, FAILED_CODE),
        ],
        ids=['skipped-output', 'failed-output'],
    )
    async def test_err_output_fails_the_node_and_skips_a_default_dependent(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
        child_def: type[WorkflowDefinition[int]],
        expected_code: str,
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='output_skipped_consume')
        def consume(
            *, upstream: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return upstream

        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=child_def)
        dependent = TaskNode(
            fn=consume, waits_for=[sub], args_from={'upstream': sub},
        )
        spec = app.workflow(
            name='output_skipped_parent',
            tasks=[sub, dependent],
            output=dependent,
            definition_key='tests.integration.output_skipped_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._run_child(
            session, broker, child_def,
            await self._child_of(session, handle.workflow_id, 0),
        )

        node = await self._node(session, handle.workflow_id, 0)
        assert node.status == 'FAILED'
        summary = json.loads(node.sub_workflow_summary)
        assert summary['status'] == 'COMPLETED'
        assert summary['output'] is None

        skipped = await self._node(session, handle.workflow_id, 1)
        assert skipped.status == 'SKIPPED'
        assert skipped.task_id is None

        row = await self._workflow_row(session, handle.workflow_id)
        assert row.status == 'FAILED'
        assert row.result is None
        assert _code(await handle.get_async(timeout_ms=2000)) == expected_code
        assert _code(await handle.result_for_async(sub.key())) == expected_code
        results = _handle_value(await handle.results_async())
        assert _code(results[f'{spec.name}:0']) == expected_code
        tasks = _handle_value(await handle.tasks_async())
        node_result = tasks[0].result
        assert node_result is not None
        assert _code(node_result) == expected_code

    @pytest.mark.parametrize(
        ('child_def', 'expected_code'),
        [
            (SkippedOutputChild, OutcomeCode.WORKFLOW_OUTPUT_SKIPPED.value),
            (FailedOutputChild, FAILED_CODE),
        ],
        ids=['skipped-output', 'failed-output'],
    )
    async def test_allow_failed_deps_dependent_receives_the_err(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
        child_def: type[WorkflowDefinition[int]],
        expected_code: str,
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='output_skipped_recover')
        def recover(
            *, upstream: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return upstream

        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=child_def)
        dependent = TaskNode(
            fn=recover,
            waits_for=[sub],
            args_from={'upstream': sub},
            allow_failed_deps=True,
        )
        spec = app.workflow(
            name='output_skipped_recover_parent',
            tasks=[sub, dependent],
            output=dependent,
            definition_key='tests.integration.output_skipped_recover_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._run_child(
            session, broker, child_def,
            await self._child_of(session, handle.workflow_id, 0),
        )

        waiting = await self._node(session, handle.workflow_id, 1)
        assert waiting.status == 'ENQUEUED'
        task_row = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :id'),
            {'id': waiting.task_id},
        )
        injected = json.loads(task_row.one().kwargs)['upstream']
        assert _code(decode_task_result(injected['inner'], int)) == expected_code

    async def test_workflow_context_holds_the_named_outcome(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='output_skipped_ctx_consume')
        def consume_ctx(
            *, workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        dependent = TaskNode(
            fn=consume_ctx,
            waits_for=[sub],
            workflow_ctx_from=[sub],
            allow_failed_deps=True,
        )
        spec = app.workflow(
            name='output_skipped_ctx_parent',
            tasks=[sub, dependent],
            output=dependent,
            definition_key='tests.integration.output_skipped_ctx_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._run_skipped_child(
            session, broker, await self._child_of(session, handle.workflow_id, 0),
        )

        waiting = await self._node(session, handle.workflow_id, 1)
        task_row = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :id'),
            {'id': waiting.task_id},
        )
        context = json.loads(task_row.one().kwargs)['__h_workflow_ctx__']
        entry = context['results_by_id'][f'{spec.name}:0']
        assert _code(decode_task_result(entry, int)) == (
            OutcomeCode.WORKFLOW_OUTPUT_SKIPPED.value
        )

    async def test_join_any_does_not_count_the_failed_node(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='output_skipped_sibling')
        def sibling_fn() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task(task_name='output_skipped_after_any')
        def after_any() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        sibling = TaskNode(fn=sibling_fn)
        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        dependent = TaskNode(fn=after_any, waits_for=[sibling, sub], join='any')
        spec = app.workflow(
            name='output_skipped_any_parent',
            tasks=[sibling, sub, dependent],
            output=dependent,
            definition_key='tests.integration.output_skipped_any_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._finish(session, broker, handle.workflow_id, 0, FAILED_RESULT)
        await self._run_skipped_child(
            session, broker, await self._child_of(session, handle.workflow_id, 1),
        )

        assert (await self._node(session, handle.workflow_id, 2)).status == 'SKIPPED'

    async def test_join_quorum_does_not_count_the_failed_node(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='output_skipped_quorum_other')
        def other_fn() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task(task_name='output_skipped_after_quorum')
        def after_quorum() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        other = TaskNode(fn=other_fn)
        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        dependent = TaskNode(
            fn=after_quorum, waits_for=[other, sub], join='quorum', min_success=2,
        )
        spec = app.workflow(
            name='output_skipped_quorum_parent',
            tasks=[other, sub, dependent],
            output=dependent,
            definition_key='tests.integration.output_skipped_quorum_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._finish(session, broker, handle.workflow_id, 0, OK_RESULT)
        await self._run_skipped_child(
            session, broker, await self._child_of(session, handle.workflow_id, 1),
        )

        assert (await self._node(session, handle.workflow_id, 2)).status == 'SKIPPED'

    async def test_parent_success_case_is_not_met_and_get_names_the_cause(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        spec = app.workflow(
            name='output_skipped_policy_parent',
            tasks=[sub],
            output=sub,
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[sub])]),
            definition_key='tests.integration.output_skipped_policy_parent.v1',
        )
        handle = await start_ok(spec, broker)
        child_id = await self._child_of(session, handle.workflow_id, 0)

        await self._run_skipped_child(session, broker, child_id)

        row = await self._workflow_row(session, handle.workflow_id)
        assert row.status == 'FAILED'
        got = await handle.get_async(timeout_ms=2000)
        assert got.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert (got.err_value.data or {})['workflow_id'] == child_id

    async def test_policy_parent_with_the_node_in_no_case_keeps_its_error(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """The node is FAILED, ``on_error`` runs, and the case decides COMPLETED."""
        session, broker, app = setup

        @app.task(task_name='output_skipped_spare')
        def spare_fn() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        spare = TaskNode(fn=spare_fn)
        spec = app.workflow(
            name='output_skipped_no_case_parent',
            tasks=[sub, spare],
            output=spare,
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[spare])]),
            definition_key='tests.integration.output_skipped_no_case_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._run_skipped_child(
            session, broker, await self._child_of(session, handle.workflow_id, 0),
        )
        before = await self._workflow_row(session, handle.workflow_id)
        assert before.status == 'RUNNING'
        await self._finish(session, broker, handle.workflow_id, 1, OK_RESULT)

        row = await self._workflow_row(session, handle.workflow_id)
        assert row.status == 'COMPLETED'
        stored_error = await session.execute(
            text('SELECT error FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert 'WORKFLOW_OUTPUT_SKIPPED' in stored_error.scalar_one()
        assert (await handle.get_async(timeout_ms=2000)).ok == 7

    async def test_pause_policy_pauses_the_parent_and_resume_finishes_it(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='output_skipped_after_pause')
        def after_pause() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        dependent = TaskNode(fn=after_pause, waits_for=[sub])
        spec = app.workflow(
            name='output_skipped_pause_parent',
            tasks=[sub, dependent],
            output=dependent,
            on_error=OnError.PAUSE,
            definition_key='tests.integration.output_skipped_pause_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._run_skipped_child(
            session, broker, await self._child_of(session, handle.workflow_id, 0),
        )

        assert (await self._workflow_row(session, handle.workflow_id)).status == 'PAUSED'
        assert (await self._node(session, handle.workflow_id, 1)).status == 'PENDING'

        _handle_value(await handle.resume_async())
        await session.commit()

        assert (await self._node(session, handle.workflow_id, 1)).status == 'SKIPPED'
        assert (await self._workflow_row(session, handle.workflow_id)).status == 'FAILED'
        got = await handle.get_async(timeout_ms=2000)
        assert got.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    async def test_two_levels_with_default_policy_give_subworkflow_failed(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=MidOverSkippedChild)
        spec = app.workflow(
            name='output_skipped_grand',
            tasks=[sub],
            output=sub,
            definition_key='tests.integration.output_skipped_grand.v1',
        )
        handle = await start_ok(spec, broker)
        mid_id = await self._child_of(session, handle.workflow_id, 0)
        child_id = await self._child_of(session, mid_id, 0)

        await self._run_skipped_child(session, broker, child_id)

        assert (await self._node(session, mid_id, 0)).status == 'FAILED'
        assert (await self._workflow_row(session, mid_id)).status == 'FAILED'
        assert (await self._node(session, handle.workflow_id, 0)).status == 'FAILED'
        row = await self._workflow_row(session, handle.workflow_id)
        assert row.status == 'FAILED'
        got = await handle.get_async(timeout_ms=2000)
        assert got.err_value.error_code is OutcomeCode.SUBWORKFLOW_FAILED

    async def test_two_levels_with_a_mid_case_keep_the_named_outcome(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        sub: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=MidOverSkippedChildWithCase,
        )
        spec = app.workflow(
            name='output_skipped_grand_with_case',
            tasks=[sub],
            output=sub,
            definition_key='tests.integration.output_skipped_grand_with_case.v1',
        )
        handle = await start_ok(spec, broker)
        mid_id = await self._child_of(session, handle.workflow_id, 0)
        child_id = await self._child_of(session, mid_id, 0)

        await self._finish(session, broker, mid_id, 1, OK_RESULT)
        await self._run_skipped_child(session, broker, child_id)

        assert (await self._node(session, mid_id, 0)).status == 'FAILED'
        assert (await self._workflow_row(session, mid_id)).status == 'COMPLETED'
        assert (await self._node(session, handle.workflow_id, 0)).status == 'FAILED'
        assert (await self._workflow_row(session, handle.workflow_id)).status == 'FAILED'
        got = await handle.get_async(timeout_ms=2000)
        assert got.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert (got.err_value.data or {})['workflow_id'] == child_id

    async def test_outputless_parent_entry_holds_the_named_outcome(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        @app.task(task_name='output_skipped_outputless_spare')
        def spare_fn() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        spare = TaskNode(fn=spare_fn)
        spec = app.workflow(
            name='output_skipped_outputless_parent',
            tasks=[sub, spare],
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[spare])]),
            definition_key='tests.integration.output_skipped_outputless_parent.v1',
        )
        handle = await start_ok(spec, broker)

        await self._finish(session, broker, handle.workflow_id, 1, OK_RESULT)
        await self._run_skipped_child(
            session, broker, await self._child_of(session, handle.workflow_id, 0),
        )

        got = await handle.get_async(timeout_ms=2000)
        assert _code(got.ok_value[f'{spec.name}:0']) == (
            OutcomeCode.WORKFLOW_OUTPUT_SKIPPED.value
        )
        assert got.ok_value[f'{spec.name}:1'].ok == 7

    async def _complete_child_with_text(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        child_id: str,
        stored: str | None,
    ) -> None:
        """Complete a child, replace its stored result, then advance the parent."""
        for index, result in ((0, FAILED_RESULT), (2, OK_RESULT)):
            node = await self._node(session, child_id, index)
            await on_workflow_task_complete(
                session, node.task_id, result, broker, task_name=node.task_name,
            )
        _ = pop_pending_parent_propagations(session)
        await session.execute(
            text('UPDATE horsies_workflows SET result = :result WHERE id = :wf_id'),
            {'result': stored, 'wf_id': child_id},
        )
        await session.commit()
        await on_subworkflow_complete(session, child_id, broker)
        await drain_parent_propagations_in_session(session, broker)
        await session.commit()

    async def _parent_with_a_recovering_dependent(
        self, app: Horsies, broker: PostgresBroker,
    ) -> tuple[Any, SubWorkflowNode[int]]:
        @app.task(task_name='output_skipped_stored_data_recover')
        def recover(
            *, upstream: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return upstream

        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        dependent = TaskNode(
            fn=recover,
            waits_for=[sub],
            args_from={'upstream': sub},
            allow_failed_deps=True,
        )
        spec = app.workflow(
            name='output_skipped_stored_data_parent',
            tasks=[sub, dependent],
            output=dependent,
            definition_key='tests.integration.output_skipped_stored_data_parent.v1',
        )
        return await start_ok(spec, broker), sub

    async def _injected_code(
        self, session: AsyncSession, workflow_id: str,
    ) -> str:
        waiting = await self._node(session, workflow_id, 1)
        assert waiting.status == 'ENQUEUED'
        task_row = await session.execute(
            text('SELECT kwargs FROM horsies_tasks WHERE id = :id'),
            {'id': waiting.task_id},
        )
        injected = json.loads(task_row.one().kwargs)['upstream']
        return _code(decode_task_result(injected['inner'], int))

    @pytest.mark.parametrize(
        'stored',
        ['null', 'not{json', '{"ok": 99}', None, ''],
        ids=['text-null', 'not-json', 'not-an-envelope', 'sql-null', 'empty'],
    )
    async def test_child_text_that_does_not_parse_fails_the_node(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
        stored: str | None,
    ) -> None:
        session, broker, app = setup
        handle, sub = await self._parent_with_a_recovering_dependent(app, broker)
        child_id = await self._child_of(session, handle.workflow_id, 0)

        await self._complete_child_with_text(session, broker, child_id, stored)

        node = await self._node(session, handle.workflow_id, 0)
        assert node.status == 'FAILED'
        assert node.result != stored
        assert child_id in node.result
        assert (await self._workflow_row(session, child_id)).result == stored
        code = OperationalErrorCode.RESULT_DESERIALIZATION_ERROR.value
        assert await self._injected_code(session, handle.workflow_id) == code
        result_for = await handle.result_for_async(sub.key())
        assert result_for.err_value.error_code is (
            OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )

    @pytest.mark.parametrize(
        'stored', ['null', 'not{json', None], ids=['text-null', 'not-json', 'sql-null'],
    )
    async def test_required_unreadable_node_reports_the_cause_at_get(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
        stored: str | None,
    ) -> None:
        session, broker, app = setup
        sub: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=SkippedOutputChild)
        spec = app.workflow(
            name='output_skipped_required_unreadable_parent',
            tasks=[sub],
            output=sub,
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[sub])]),
            definition_key='tests.integration.output_skipped_required_unreadable.v1',
        )
        handle = await start_ok(spec, broker)
        child_id = await self._child_of(session, handle.workflow_id, 0)

        await self._complete_child_with_text(session, broker, child_id, stored)

        row = await self._workflow_row(session, handle.workflow_id)
        assert row.status == 'FAILED'
        got = await handle.get_async(timeout_ms=2000)
        assert got.err_value.error_code is (
            OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )
        assert (got.err_value.data or {})['workflow_id'] == child_id

    async def test_outputless_skipped_terminal_holds_the_named_outcome(
        self, setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup
        gate_fn, out_fn, fallback_fn = _register_tasks(app)
        gate = TaskNode(fn=gate_fn)
        terminal = TaskNode(fn=out_fn, waits_for=[gate])
        fallback = TaskNode(fn=fallback_fn)
        spec = app.workflow(
            name='output_skipped_outputless',
            tasks=[gate, terminal, fallback],
            success_policy=SuccessPolicy(cases=[SuccessCase(required=[fallback])]),
            definition_key='tests.integration.output_skipped_outputless.v1',
        )
        handle = await start_ok(spec, broker)

        await self._run_skipped_child(session, broker, handle.workflow_id)

        got = await handle.get_async(timeout_ms=2000)
        entry = got.ok_value[f'{spec.name}:1']
        assert entry.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert got.ok_value[f'{spec.name}:2'].ok == 7
