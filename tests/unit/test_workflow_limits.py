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
from horsies.core.models.workflow.limits import (
    MAX_SUBWORKFLOW_DEPTH,
    MAX_WORKFLOW_NODES,
)
from horsies.core.models.workflow.nesting_check import (
    ChildStart,
    NestingPath,
    NestingStep,
    StopWithFinding,
    WalkChild,
    decide_nesting,
)

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


class TestNestingBehindADependency:
    """Results at run time can end a nesting behind a node with waits_for."""

    def test_a_self_nesting_behind_a_dependency_passes(self) -> None:
        app = _app()

        class Gated(WorkflowDefinition[Any]):
            name = 'gated'
            definition_key = 'tests.limits.gated.v1'

            @classmethod
            def build_with(
                cls, app: Horsies, level: int = 0, *_args: Any, **_kwargs: Any
            ) -> WorkflowSpec[Any]:
                gate = TaskNode(fn=_task_fn(), node_id='gate')
                child = SubWorkflowNode(
                    workflow_def=Gated, kwargs={'level': level}, waits_for=[gate],
                )
                return WorkflowSpec(name=cls.name, tasks=[gate, child])

        def root() -> WorkflowSpec[Any]:
            return app.workflow(
                'gated_root',
                [SubWorkflowNode(workflow_def=Gated, kwargs={'level': 0})],
                definition_key='tests.limits.gated_root.v1',
            )

        app.workflow_builder()(root)

        assert app.check(live=False) == []

    def test_a_nesting_past_the_limit_behind_dependencies_passes(self) -> None:
        app = _app()

        class GatedDeeper(WorkflowDefinition[Any]):
            name = 'gated_deeper'
            definition_key = 'tests.limits.gated_deeper.v1'

            @classmethod
            def build_with(
                cls, app: Horsies, level: int = 0, *_args: Any, **_kwargs: Any
            ) -> WorkflowSpec[Any]:
                gate = TaskNode(fn=_task_fn(), node_id='gate')
                child = SubWorkflowNode(
                    workflow_def=GatedDeeper,
                    kwargs={'level': level + 1},
                    waits_for=[gate],
                )
                return WorkflowSpec(name=cls.name, tasks=[gate, child])

        def root() -> WorkflowSpec[Any]:
            return app.workflow(
                'gated_deeper_root',
                [SubWorkflowNode(workflow_def=GatedDeeper, kwargs={'level': 0})],
                definition_key='tests.limits.gated_deeper_root.v1',
            )

        app.workflow_builder()(root)

        assert app.check(live=False) == []

    def test_a_dependency_before_a_self_nesting_does_not_hide_it(self) -> None:
        """Once the definition starts, nothing between its two occurrences waits."""
        app = _app()

        class Same(WorkflowDefinition[Any]):
            name = 'same_after_gate'
            definition_key = 'tests.limits.same_after_gate.v1'

            @classmethod
            def build_with(
                cls, app: Horsies, level: int = 0, *_args: Any, **_kwargs: Any
            ) -> WorkflowSpec[Any]:
                child = SubWorkflowNode(workflow_def=Same, kwargs={'level': level})
                return WorkflowSpec(name=cls.name, tasks=[child])

        def root() -> WorkflowSpec[Any]:
            gate = TaskNode(fn=_task_fn(), node_id='gate')
            child = SubWorkflowNode(
                workflow_def=Same, kwargs={'level': 0}, waits_for=[gate],
            )
            return app.workflow(
                'same_after_gate_root',
                [gate, child],
                definition_key='tests.limits.same_after_gate_root.v1',
            )

        app.workflow_builder()(root)

        errors = app.check(live=False)

        assert [error.code for error in errors] == [
            ErrorCode.WORKFLOW_SUBWORKFLOW_SELF_NESTING
        ]

    def test_a_dependency_before_a_nesting_past_the_limit_hides_it(self) -> None:
        """The depth that the chain reaches depends on the result of the gate."""
        app = _app()

        class Deeper(WorkflowDefinition[Any]):
            name = 'deeper_after_gate'
            definition_key = 'tests.limits.deeper_after_gate.v1'

            @classmethod
            def build_with(
                cls, app: Horsies, level: int = 0, *_args: Any, **_kwargs: Any
            ) -> WorkflowSpec[Any]:
                child = SubWorkflowNode(
                    workflow_def=Deeper, kwargs={'level': level + 1}
                )
                return WorkflowSpec(name=cls.name, tasks=[child])

        def root() -> WorkflowSpec[Any]:
            gate = TaskNode(fn=_task_fn(), node_id='gate')
            child = SubWorkflowNode(
                workflow_def=Deeper, kwargs={'level': 0}, waits_for=[gate],
            )
            return app.workflow(
                'deeper_after_gate_root',
                [gate, child],
                definition_key='tests.limits.deeper_after_gate_root.v1',
            )

        app.workflow_builder()(root)

        assert app.check(live=False) == []


def _chain(length: int) -> NestingPath:
    return tuple(
        NestingStep(
            definition_key='tests.limits.chain.v1',
            kwargs_signature=str(level),
            start=ChildStart.WITH_ITS_WORKFLOW,
        )
        for level in range(length)
    )


def _next_step(level: int) -> NestingStep:
    return NestingStep(
        definition_key='tests.limits.chain.v1',
        kwargs_signature=str(level),
        start=ChildStart.WITH_ITS_WORKFLOW,
    )


class TestNestingDecisionAtTheLimit:
    """The check walks a child at the depth that the engine still starts."""

    def test_a_child_at_the_limit_is_walked(self) -> None:
        decision = decide_nesting(
            _chain(MAX_SUBWORKFLOW_DEPTH - 1),
            'node',
            _next_step(MAX_SUBWORKFLOW_DEPTH - 1),
        )

        assert isinstance(decision, WalkChild)
        assert len(decision.path) == MAX_SUBWORKFLOW_DEPTH

    def test_a_child_past_the_limit_is_reported(self) -> None:
        decision = decide_nesting(
            _chain(MAX_SUBWORKFLOW_DEPTH),
            'node',
            _next_step(MAX_SUBWORKFLOW_DEPTH),
        )

        assert isinstance(decision, StopWithFinding)
        assert decision.finding.code == ErrorCode.WORKFLOW_SUBWORKFLOW_DEPTH_LIMIT
