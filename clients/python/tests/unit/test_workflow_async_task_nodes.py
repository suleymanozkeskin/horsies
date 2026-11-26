"""Workflow nodes that hold an `async def` task.

The spec checks read the signature of the task function. They must read an
`async def` task the same way as a `def` task.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.outcome import Failed, Succeeded, TaskOutcome
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import TaskNode, WorkflowContext
from horsies.core.models.workflow.nodes import stored_kwargs
from horsies.core.task_decorator import from_node
from horsies.core.types.result import Err, Ok, Result

pytestmark = [pytest.mark.unit]


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr(
                    'postgresql://user:password@localhost/unused'
                ),
            ),
        ),
    )


class TestAsyncTaskNodes:
    def test_a_node_of_an_async_task_stores_its_wire_form(self) -> None:
        app = _app()

        @app.task('async_double')
        async def double(*, value: int) -> Result[int, TaskError]:
            return Ok(value * 2)

        spec = app.workflow(
            name='async_node',
            nodes=[double.node()(value=3)],
            definition_key='async_node.v1',
        )

        node = spec.nodes[0]
        assert isinstance(node, TaskNode)
        assert node.fn is double
        assert stored_kwargs(node) == {'value': 3}

    def test_args_from_flows_between_def_and_async_def_tasks(self) -> None:
        app = _app()

        @app.task('sync_source')
        def source() -> Result[int, TaskError]:
            return Ok(1)

        @app.task('async_middle')
        async def middle(
            *, upstream: TaskOutcome[int, TaskError]
        ) -> Result[int, TaskError]:
            match upstream:
                case Succeeded(value):
                    return Ok(value + 1)
                case Failed(error):
                    return Err(error)

        @app.task('sync_sink')
        def sink(*, upstream: TaskOutcome[int, TaskError]) -> Result[int, TaskError]:
            match upstream:
                case Succeeded(value):
                    return Ok(value + 1)
                case Failed(error):
                    return Err(error)

        first = source.node()()
        second = middle.node()(upstream=from_node(first))
        third = sink.node()(upstream=from_node(second))
        spec = app.workflow(
            name='mixed_chain',
            nodes=[first, second, third],
            output=third,
            definition_key='mixed_chain.v1',
        )

        assert [node.index for node in spec.nodes] == [0, 1, 2]

    def test_an_args_from_type_mismatch_into_an_async_task_is_refused(self) -> None:
        app = _app()

        @app.task('sync_text_source')
        def text_source() -> Result[str, TaskError]:
            return Ok('text')

        @app.task('async_needs_int')
        async def needs_int(
            *, upstream: TaskOutcome[int, TaskError]
        ) -> Result[int, TaskError]:
            match upstream:
                case Succeeded(value):
                    return Ok(value)
                case Failed(error):
                    return Err(error)

        first = text_source.node()()
        second = needs_int.node()(upstream=from_node(first))

        with pytest.raises(WorkflowValidationError) as exc_info:
            app.workflow(
                name='mismatch',
                nodes=[first, second],
                definition_key='mismatch.v1',
            )

        assert exc_info.value.code == ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH

    def test_workflow_context_from_needs_the_parameter_on_an_async_task(self) -> None:
        app = _app()

        @app.task('sync_first')
        def first_task() -> Result[int, TaskError]:
            return Ok(1)

        @app.task('async_without_ctx')
        async def without_ctx() -> Result[int, TaskError]:
            return Ok(1)

        first = first_task.node()()
        second = without_ctx.node(waits_for=[first], workflow_context_from=[first])()

        with pytest.raises(WorkflowValidationError) as exc_info:
            app.workflow(
                name='ctx_missing',
                nodes=[first, second],
                definition_key='ctx_missing.v1',
            )

        assert exc_info.value.code == ErrorCode.WORKFLOW_CTX_PARAM_MISSING

    def test_workflow_context_from_is_accepted_on_an_async_task_that_declares_it(
        self,
    ) -> None:
        app = _app()

        @app.task('sync_first')
        def first_task() -> Result[int, TaskError]:
            return Ok(1)

        @app.task('async_with_ctx')
        async def with_ctx(
            *,
            workflow_ctx: WorkflowContext | None = None,
        ) -> Result[int, TaskError]:
            return Ok(1)

        first = first_task.node()()
        second = with_ctx.node(waits_for=[first], workflow_context_from=[first])()
        spec = app.workflow(
            name='ctx_present',
            nodes=[first, second],
            definition_key='ctx_present.v1',
        )

        node = spec.nodes[1]
        assert isinstance(node, TaskNode)
        assert node.fn is with_ctx
        assert node.workflow_context_from is not None
        assert len(node.workflow_context_from) == 1
