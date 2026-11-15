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
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode, WorkflowContext
from horsies.core.models.workflow.nodes import stored_kwargs
from horsies.core.task_decorator import from_node

pytestmark = [pytest.mark.unit]


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr(
                    'postgresql+psycopg://user:password@localhost/unused'
                ),
            ),
        ),
    )


class TestAsyncTaskNodes:
    def test_a_node_of_an_async_task_stores_its_wire_form(self) -> None:
        app = _app()

        @app.task('async_double')
        async def double(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value * 2)

        spec = app.workflow(
            name='async_node',
            tasks=[double.node()(value=3)],
            definition_key='async_node.v1',
        )

        node = spec.tasks[0]
        assert isinstance(node, TaskNode)
        assert node.fn is double
        assert stored_kwargs(node) == {'value': 3}

    def test_args_from_flows_between_def_and_async_def_tasks(self) -> None:
        app = _app()

        @app.task('sync_source')
        def source() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('async_middle')
        async def middle(
            *, upstream: TaskResult[int, TaskError]
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=upstream.ok_value + 1)

        @app.task('sync_sink')
        def sink(*, upstream: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=upstream.ok_value + 1)

        first = source.node()()
        second = middle.node()(upstream=from_node(first))
        third = sink.node()(upstream=from_node(second))
        spec = app.workflow(
            name='mixed_chain',
            tasks=[first, second, third],
            output=third,
            definition_key='mixed_chain.v1',
        )

        assert [node.index for node in spec.tasks] == [0, 1, 2]

    def test_an_args_from_type_mismatch_into_an_async_task_is_refused(self) -> None:
        app = _app()

        @app.task('sync_text_source')
        def text_source() -> TaskResult[str, TaskError]:
            return TaskResult(ok='text')

        @app.task('async_needs_int')
        async def needs_int(
            *, upstream: TaskResult[int, TaskError]
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=upstream.ok_value)

        first = text_source.node()()
        second = needs_int.node()(upstream=from_node(first))

        with pytest.raises(WorkflowValidationError) as exc_info:
            app.workflow(
                name='mismatch',
                tasks=[first, second],
                definition_key='mismatch.v1',
            )

        assert exc_info.value.code == ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH

    def test_workflow_ctx_from_needs_the_parameter_on_an_async_task(self) -> None:
        app = _app()

        @app.task('sync_first')
        def first_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('async_without_ctx')
        async def without_ctx() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        first = first_task.node()()
        second = without_ctx.node(waits_for=[first], workflow_ctx_from=[first])()

        with pytest.raises(WorkflowValidationError) as exc_info:
            app.workflow(
                name='ctx_missing',
                tasks=[first, second],
                definition_key='ctx_missing.v1',
            )

        assert exc_info.value.code == ErrorCode.WORKFLOW_CTX_PARAM_MISSING

    def test_workflow_ctx_from_is_accepted_on_an_async_task_that_declares_it(
        self,
    ) -> None:
        app = _app()

        @app.task('sync_first')
        def first_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('async_with_ctx')
        async def with_ctx(
            *,
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        first = first_task.node()()
        second = with_ctx.node(waits_for=[first], workflow_ctx_from=[first])()
        spec = app.workflow(
            name='ctx_present',
            tasks=[first, second],
            definition_key='ctx_present.v1',
        )

        node = spec.tasks[1]
        assert isinstance(node, TaskNode)
        assert node.fn is with_ctx
        assert node.workflow_ctx_from is not None
        assert len(node.workflow_ctx_from) == 1
