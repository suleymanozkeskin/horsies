"""Unit tests for the workflow node limit and the nesting findings of check."""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.models.workflow.limits import MAX_WORKFLOW_NODES

pytestmark = pytest.mark.unit

QUEUE = 'work'
LEVEL_WHERE_THE_NESTING_ENDS = 3


def _app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql+psycopg://user:pass@localhost/db'),
            ),
            queue_mode=QueueMode.CUSTOM,
            custom_queues=[CustomQueueConfig(name=QUEUE, priority=10)],
        ),
    )


def _task_fn() -> Any:
    return mock.MagicMock(task_name='leaf_task', task_queue_name=QUEUE)


def _wide_spec(node_count: int) -> WorkflowSpec[Any]:
    fn = _task_fn()
    return WorkflowSpec(
        name='wide',
        tasks=[TaskNode(fn=fn, node_id=f'node_{index}') for index in range(node_count)],
    )


class TestNodeLimit:
    def test_a_spec_with_the_limit_of_nodes_builds(self) -> None:
        spec = _wide_spec(MAX_WORKFLOW_NODES)

        assert len(spec.tasks) == MAX_WORKFLOW_NODES

    def test_a_spec_over_the_limit_raises_hrs_038(self) -> None:
        with pytest.raises(WorkflowValidationError) as caught:
            _wide_spec(MAX_WORKFLOW_NODES + 1)

        assert caught.value.code == ErrorCode.WORKFLOW_TOO_MANY_NODES


class TestNestingFindingsInCheck:
    def test_a_nesting_past_the_depth_limit_is_reported_once(self) -> None:
        app = _app()

        class Deeper(WorkflowDefinition[Any]):
            name = 'deeper'
            definition_key = 'tests.limits.deeper.v1'

            @classmethod
            def build_with(
                cls, app: Horsies, level: int = 0, *_args: Any, **_kwargs: Any
            ) -> WorkflowSpec[Any]:
                child = SubWorkflowNode(
                    workflow_def=Deeper, kwargs={'level': level + 1}
                )
                return WorkflowSpec(name=cls.name, tasks=[child])

        def root() -> WorkflowSpec[Any]:
            return app.workflow(
                'deeper_root',
                [SubWorkflowNode(workflow_def=Deeper, kwargs={'level': 0})],
                definition_key='tests.limits.deeper_root.v1',
            )

        app.workflow_builder()(root)

        errors = app.check(live=False)

        assert [error.code for error in errors] == [
            ErrorCode.WORKFLOW_SUBWORKFLOW_DEPTH_LIMIT
        ]

    def test_a_subworkflow_that_includes_itself_with_the_same_kwargs_is_reported(
        self,
    ) -> None:
        app = _app()

        class Same(WorkflowDefinition[Any]):
            name = 'same'
            definition_key = 'tests.limits.same.v1'

            @classmethod
            def build_with(
                cls, app: Horsies, level: int = 0, *_args: Any, **_kwargs: Any
            ) -> WorkflowSpec[Any]:
                child = SubWorkflowNode(workflow_def=Same, kwargs={'level': level})
                return WorkflowSpec(name=cls.name, tasks=[child])

        def root() -> WorkflowSpec[Any]:
            return app.workflow(
                'same_root',
                [SubWorkflowNode(workflow_def=Same, kwargs={'level': 0})],
                definition_key='tests.limits.same_root.v1',
            )

        app.workflow_builder()(root)

        errors = app.check(live=False)

        assert [error.code for error in errors] == [
            ErrorCode.WORKFLOW_SUBWORKFLOW_SELF_NESTING
        ]

    def test_a_nesting_that_ends_before_the_limit_passes(self) -> None:
        app = _app()

        class Ends(WorkflowDefinition[Any]):
            name = 'ends'
            definition_key = 'tests.limits.ends.v1'

            @classmethod
            def build_with(
                cls, app: Horsies, level: int = 0, *_args: Any, **_kwargs: Any
            ) -> WorkflowSpec[Any]:
                if level >= LEVEL_WHERE_THE_NESTING_ENDS:
                    leaf = TaskNode(fn=_task_fn(), node_id='leaf')
                    return WorkflowSpec(name=cls.name, tasks=[leaf], output=leaf)
                child = SubWorkflowNode(workflow_def=Ends, kwargs={'level': level + 1})
                return WorkflowSpec(name=cls.name, tasks=[child])

        def root() -> WorkflowSpec[Any]:
            return app.workflow(
                'ends_root',
                [SubWorkflowNode(workflow_def=Ends, kwargs={'level': 0})],
                definition_key='tests.limits.ends_root.v1',
            )

        app.workflow_builder()(root)

        assert app.check(live=False) == []
