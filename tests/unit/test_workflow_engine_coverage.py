"""Unit tests for engine.py uncovered error/edge-case branches.

Each test class targets a specific engine function, mocking AsyncSession
to exercise defensive branches that integration tests don't reach.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.models.tasks import OperationalErrorCode, TaskError, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.workflows import engine
from horsies.core.workflows.engine import (
    DependencyResults,
    _build_workflow_context_data,
    _deser_json,
    _handle_workflow_task_failure,
    _load_workflow_def_from_key,
    _resolve_workflow_def_nodes,
    _ser,
    _validate_args_from_map,
    check_workflow_completion,
    enqueue_subworkflow_task,
    enqueue_workflow_task,
    evaluate_workflow_success,
    get_dependency_results,
    get_dependency_results_with_names,
    get_workflow_failure_error,
    get_workflow_final_result,
    on_subworkflow_complete,
    on_workflow_task_complete,
    try_make_ready_and_enqueue,
)
from horsies.core.workflows.sql import (
    ENQUEUE_SUBWORKFLOW_TASK_SQL,
    ENQUEUE_WORKFLOW_TASK_SQL,
    GET_CHILD_WORKFLOW_INFO_SQL,
    GET_DEPENDENCY_RESULTS_SQL,
    GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL,
    GET_FIRST_FAILED_TASK_RESULT_SQL,
    GET_FIRST_FAILED_REQUIRED_TASK_SQL,
    GET_SUBWORKFLOW_SUMMARIES_SQL,
    GET_TASK_CONFIG_SQL,
    GET_TASK_STATUSES_SQL,
    GET_TERMINAL_TASK_RESULTS_SQL,
    GET_WORKFLOW_COMPLETION_STATUS_SQL,
    GET_WORKFLOW_NAME_SQL,
    GET_WORKFLOW_ON_ERROR_SQL,
    GET_WORKFLOW_OUTPUT_INDEX_SQL,
    GET_WORKFLOW_STATUS_SQL,
    GET_WORKFLOW_TASK_BY_TASK_ID_SQL,
    LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
    MARK_TASK_READY_SQL,
    UPDATE_WORKFLOW_TASK_RESULT_SQL,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _one_result(row: Any) -> MagicMock:
    """Mock a session.execute result that returns a single row."""
    result = MagicMock()
    result.fetchone.return_value = row
    return result


def _rows_result(rows: list[Any]) -> MagicMock:
    """Mock a session.execute result that returns multiple rows."""
    result = MagicMock()
    result.fetchall.return_value = rows
    return result


def _empty_result() -> MagicMock:
    """Mock a session.execute result that returns no rows."""
    result = MagicMock()
    result.fetchone.return_value = None
    result.fetchall.return_value = []
    return result


# ── 1. _ser ──────────────────────────────────────────────────────────


@pytest.mark.unit
class TestSer:
    def test_err_result_returns_fallback(self) -> None:
        err = Err(Exception('boom'))
        assert _ser(err, 'test-ctx') is None

    def test_err_result_returns_custom_fallback(self) -> None:
        err = Err(Exception('boom'))
        assert _ser(err, 'test-ctx', fallback='default') == 'default'

    def test_ok_result_returns_value(self) -> None:
        ok = Ok('hello')
        assert _ser(ok, 'test-ctx') == 'hello'


# ── 2. _deser_json ──────────────────────────────────────────────────


@pytest.mark.unit
class TestDeserJson:
    def test_none_raw_returns_fallback(self) -> None:
        assert _deser_json(None, 'ctx') is None

    def test_empty_string_returns_fallback(self) -> None:
        assert _deser_json('', 'ctx') is None

    def test_empty_string_returns_custom_fallback(self) -> None:
        assert _deser_json('', 'ctx', fallback={}) == {}

    def test_invalid_json_returns_fallback(self) -> None:
        assert _deser_json('not{json', 'ctx') is None

    def test_valid_json_returns_parsed(self) -> None:
        assert _deser_json('{"a": 1}', 'ctx') == {'a': 1}


# ── 2b. _validate_args_from_map ─────────────────────────────────────


@pytest.mark.unit
class TestValidateArgsFromMap:
    def test_valid_map_returns_typed(self) -> None:
        assert _validate_args_from_map({'x': 0, 'y': 1}) == {'x': 0, 'y': 1}

    def test_empty_map_returns_empty(self) -> None:
        assert _validate_args_from_map({}) == {}

    def test_non_int_value_returns_none(self) -> None:
        assert _validate_args_from_map({'x': 'not_int'}) is None

    def test_float_value_returns_none(self) -> None:
        assert _validate_args_from_map({'x': 1.5}) is None

    def test_none_value_returns_none(self) -> None:
        assert _validate_args_from_map({'x': None}) is None

    def test_mixed_valid_and_invalid_returns_none(self) -> None:
        assert _validate_args_from_map({'x': 0, 'y': 'bad'}) is None


# ── 3. enqueue_workflow_task ─────────────────────────────────────────


@pytest.mark.unit
class TestEnqueueWorkflowTask:
    def _base_row(self, **overrides: Any) -> SimpleNamespace:
        defaults = dict(
            id='wt-1',
            task_name='my_task',
            task_args='[]',
            task_kwargs=None,
            queue_name='default',
            priority=100,
            args_from=None,
            workflow_ctx_from=None,
            task_options=None,
        )
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    @pytest.mark.asyncio
    async def test_no_row_returns_none(self) -> None:
        session = AsyncMock()
        session.execute = AsyncMock(return_value=_empty_result())
        result = await enqueue_workflow_task(session, 'wf-1', 0, {})
        assert result is None

    @pytest.mark.asyncio
    async def test_good_until_invalid_iso_silently_continues(self) -> None:
        row = self._base_row(
            task_options='{"good_until":"not-a-date"}',
        )

        call_count = 0

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await enqueue_workflow_task(session, 'wf-1', 0, {})
        # Should succeed (task_id returned) despite bad ISO
        assert result is not None

    @pytest.mark.asyncio
    async def test_corrupt_kwargs_json_fails_task(self) -> None:
        row = self._base_row(task_kwargs='broken{json')

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await enqueue_workflow_task(session, 'wf-1', 0, {})
        assert result is None

    @pytest.mark.asyncio
    async def test_corrupt_task_options_json_fails_task(self) -> None:
        """Regression for #10: corrupt task_options must fail the task instead
        of silently falling back to {} (which loses retry policy / good_until)."""
        row = self._base_row(task_options='not{json')

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ) as mock_fail:
            result = await enqueue_workflow_task(session, 'wf-1', 0, {})

        assert result is None
        mock_fail.assert_called_once()
        call_kwargs = mock_fail.call_args
        assert call_kwargs[1].get('error_code') == OperationalErrorCode.WORKER_SERIALIZATION_ERROR

    @pytest.mark.asyncio
    async def test_task_options_non_dict_fails_task(self) -> None:
        """Regression for #10: valid JSON but non-object task_options must fail
        (e.g. a bare string or array is not a valid options payload)."""
        row = self._base_row(task_options='"just_a_string"')

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ) as mock_fail:
            result = await enqueue_workflow_task(session, 'wf-1', 0, {})

        assert result is None
        mock_fail.assert_called_once()
        call_kwargs = mock_fail.call_args
        assert call_kwargs[1].get('error_code') == OperationalErrorCode.WORKER_SERIALIZATION_ERROR

    @pytest.mark.asyncio
    async def test_corrupt_args_from_string_fails_task(self) -> None:
        row = self._base_row(args_from='{broken')

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await enqueue_workflow_task(session, 'wf-1', 0, {})
        assert result is None

    @pytest.mark.asyncio
    async def test_args_from_non_int_values_fails_task(self) -> None:
        """Regression: args_from with non-int values must fail early, not propagate."""
        row = self._base_row(args_from={'x': 'not_an_int'})

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await enqueue_workflow_task(session, 'wf-1', 0, {})
        assert result is None

    @pytest.mark.asyncio
    async def test_args_from_key_conflicts_static_kwarg(self) -> None:
        row = self._base_row(
            task_kwargs='{"x": 1}',
            args_from={'x': 0},
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        dep_results = {0: TaskResult(ok='val')}
        result = await enqueue_workflow_task(session, 'wf-1', 0, dep_results)
        assert result is None

    @pytest.mark.asyncio
    async def test_dep_result_serialization_fails(self) -> None:
        row = self._base_row(args_from={'x': 0})

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        dep_results = {0: TaskResult(ok='val')}

        with patch(
            'horsies.core.workflows.engine.dumps_json',
            return_value=Err(Exception('ser fail')),
        ):
            result = await enqueue_workflow_task(session, 'wf-1', 0, dep_results)
        assert result is None

    @pytest.mark.asyncio
    async def test_final_kwargs_serialization_fails(self) -> None:
        row = self._base_row()

        call_count = 0

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if stmt is ENQUEUE_WORKFLOW_TASK_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        # First dumps_json call succeeds (for kwargs parsing),
        # but the final one for kwargs serialization fails
        original_dumps = engine.dumps_json
        call_idx = 0

        def _dumps_side_effect(value: Any) -> Any:
            nonlocal call_idx
            call_idx += 1
            # The kwargs serialization is the first dumps_json call
            # in enqueue_workflow_task (for the final kwargs)
            return Err(Exception('kwargs ser fail'))

        with patch(
            'horsies.core.workflows.engine.dumps_json',
            side_effect=_dumps_side_effect,
        ):
            result = await enqueue_workflow_task(session, 'wf-1', 0, {})
        assert result is None


# ── 4. enqueue_subworkflow_task ──────────────────────────────────────


@pytest.mark.unit
class TestEnqueueSubworkflowTask:
    def _base_row(self, **overrides: Any) -> SimpleNamespace:
        defaults = dict(
            id='wt-1',
            sub_workflow_name='child_wf',
            task_args='[]',
            task_kwargs=None,
            args_from=None,
            node_id='node-0',
            sub_definition_key='test.child.v1',
        )
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    @pytest.mark.asyncio
    async def test_no_row_returns_none(self) -> None:
        session = AsyncMock()
        session.execute = AsyncMock(return_value=_empty_result())
        broker = MagicMock()
        result = await enqueue_subworkflow_task(
            session, broker, 'wf-1', 0, {}, 0, 'root-wf',
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_workflow_name_not_found_fails(self) -> None:
        row = self._base_row()

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()
        with patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_corrupt_kwargs_json_fails(self) -> None:
        row = self._base_row(task_kwargs='broken{json')

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=MagicMock(),
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_corrupt_args_from_string_fails(self) -> None:
        row = self._base_row(args_from='{broken')

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=MagicMock(),
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_args_from_non_int_values_fails_task(self) -> None:
        """Regression: args_from with non-int values must fail early in subworkflow path."""
        row = self._base_row(args_from={'x': 'not_an_int'})

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=MagicMock(),
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_args_from_key_conflicts_static_kwarg(self) -> None:
        row = self._base_row(
            task_kwargs='{"x": 1}',
            args_from={'x': 0},
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=MagicMock(),
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {0: TaskResult(ok='val')}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_dep_result_ser_fails(self) -> None:
        row = self._base_row(args_from={'x': 0})

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=MagicMock(),
        ), patch(
            'horsies.core.workflows.engine.dumps_json',
            return_value=Err(Exception('ser fail')),
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {0: TaskResult(ok='val')}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_build_with_not_overridden_with_args_raises(self) -> None:
        row = self._base_row(task_args='["positional"]', task_kwargs='{}')

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()
        broker.app = MagicMock()

        # Create a mock workflow_def that uses the default build_with
        from horsies.core.models.workflow import WorkflowDefinition

        mock_wf_def = MagicMock(spec=type)
        mock_wf_def.build_with = WorkflowDefinition.build_with
        mock_wf_def.name = 'TestWF'

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=mock_wf_def,
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_child_subworkflow_guard_no_positional_args_fails(self) -> None:
        row = self._base_row()

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()
        broker.app = MagicMock()

        from horsies.core.models.workflow import SubWorkflowNode

        # Create a child SubWorkflowNode with positional args
        child_sub_node = MagicMock(spec=SubWorkflowNode)
        child_sub_node.name = 'child_sub'
        child_sub_node.args = ('positional',)
        child_sub_node.waits_for = []
        child_sub_node.args_from = {}
        child_sub_node.workflow_ctx_from = None
        child_sub_node.index = 0
        child_sub_node.node_id = 'child-0'
        # Make isinstance check succeed
        child_sub_node.__class__ = SubWorkflowNode

        mock_spec = MagicMock()
        mock_spec.tasks = [child_sub_node]
        mock_spec.success_policy = None
        mock_spec.output = None
        mock_spec.on_error = MagicMock(value='fail')
        mock_spec.definition_key = 'test.child.v1'
        mock_spec.name = 'child_wf'

        mock_wf_def = MagicMock()
        mock_wf_def.build_with.return_value = mock_spec
        mock_wf_def.name = 'TestWF'
        # Ensure it's not the default build_with
        mock_wf_def._original_build_with = mock_wf_def.build_with

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=mock_wf_def,
        ), patch(
            'horsies.core.workflows.engine.validate_workflow_generic_output_match',
        ), patch(
            'horsies.core.workflows.engine.guard_no_positional_args',
            side_effect=ValueError('has positional args'),
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_child_subworkflow_kwargs_ser_fails(self) -> None:
        row = self._base_row()

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()
        broker.app = MagicMock()

        from horsies.core.models.workflow import SubWorkflowNode

        child_sub_node = MagicMock(spec=SubWorkflowNode)
        child_sub_node.name = 'child_sub'
        child_sub_node.args = ()
        child_sub_node.kwargs = {'k': 'v'}
        child_sub_node.waits_for = []
        child_sub_node.args_from = {}
        child_sub_node.workflow_ctx_from = None
        child_sub_node.index = 0
        child_sub_node.node_id = 'child-0'
        child_sub_node.allow_failed_deps = False
        child_sub_node.join = 'all'
        child_sub_node.min_success = None
        child_sub_node.__class__ = SubWorkflowNode

        mock_spec = MagicMock()
        mock_spec.tasks = [child_sub_node]
        mock_spec.success_policy = None
        mock_spec.output = None
        mock_spec.on_error = MagicMock(value='fail')
        mock_spec.definition_key = 'test.child.v1'
        mock_spec.name = 'child_wf'

        mock_wf_def = MagicMock()
        mock_wf_def.build_with.return_value = mock_spec
        mock_wf_def.name = 'TestWF'
        mock_wf_def._original_build_with = mock_wf_def.build_with

        # dumps_json returns Err for the child kwargs serialization
        original_dumps = engine.dumps_json

        def _dumps_failing(value: Any) -> Any:
            # Fail on child sub kwargs (dict with 'k')
            if isinstance(value, dict) and 'k' in value:
                return Err(Exception('child kwargs ser fail'))
            return original_dumps(value)

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=mock_wf_def,
        ), patch(
            'horsies.core.workflows.engine.validate_workflow_generic_output_match',
        ), patch(
            'horsies.core.workflows.engine.guard_no_positional_args',
        ), patch(
            'horsies.core.workflows.engine.dumps_json',
            side_effect=_dumps_failing,
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_child_task_guard_no_positional_args_fails(self) -> None:
        row = self._base_row()

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()
        broker.app = MagicMock()

        # Regular TaskNode child (not SubWorkflowNode)
        child_task = MagicMock()
        child_task.name = 'child_task'
        child_task.args = ('positional',)
        child_task.waits_for = []
        child_task.args_from = {}
        child_task.workflow_ctx_from = None
        child_task.index = 0
        child_task.node_id = 'child-0'
        # Not a SubWorkflowNode
        child_task.__class__ = type('TaskNode', (), {})

        mock_spec = MagicMock()
        mock_spec.tasks = [child_task]
        mock_spec.success_policy = None
        mock_spec.output = None
        mock_spec.on_error = MagicMock(value='fail')
        mock_spec.definition_key = 'test.child.v1'
        mock_spec.name = 'child_wf'

        mock_wf_def = MagicMock()
        mock_wf_def.build_with.return_value = mock_spec
        mock_wf_def.name = 'TestWF'
        mock_wf_def._original_build_with = mock_wf_def.build_with

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=mock_wf_def,
        ), patch(
            'horsies.core.workflows.engine.validate_workflow_generic_output_match',
        ), patch(
            'horsies.core.workflows.engine.guard_no_positional_args',
            side_effect=ValueError('has positional args'),
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_child_task_good_until_options_merge(self) -> None:
        """Child task with good_until merges into task_options."""
        from datetime import datetime, timezone

        row = self._base_row()

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()
        broker.app = MagicMock()

        # Regular TaskNode with good_until set
        child_task = MagicMock()
        child_task.name = 'child_task'
        child_task.args = ()
        child_task.kwargs = {}
        child_task.waits_for = []
        child_task.args_from = {}
        child_task.workflow_ctx_from = None
        child_task.index = 0
        child_task.node_id = 'child-0'
        child_task.good_until = datetime(2030, 1, 1, tzinfo=timezone.utc)
        child_task.fn = MagicMock()
        child_task.fn.task_options_json = '{"retry_policy": {"max_retries": 2}}'
        child_task.fn.task_queue_name = 'default'
        child_task.allow_failed_deps = False
        child_task.join = 'all'
        child_task.min_success = None
        child_task.queue = None
        child_task.priority = None
        child_task.__class__ = type('TaskNode', (), {})

        mock_spec = MagicMock()
        mock_spec.tasks = [child_task]
        mock_spec.success_policy = None
        mock_spec.output = None
        mock_spec.on_error = MagicMock(value='fail')
        mock_spec.definition_key = 'test.child.v1'
        mock_spec.name = 'child_wf'

        mock_wf_def = MagicMock()
        mock_wf_def.build_with.return_value = mock_spec
        mock_wf_def.name = 'TestWF'
        mock_wf_def._original_build_with = mock_wf_def.build_with

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=mock_wf_def,
        ), patch(
            'horsies.core.workflows.engine.validate_workflow_generic_output_match',
        ), patch(
            'horsies.core.workflows.engine.guard_no_positional_args',
        ), patch(
            'horsies.core.workflows.engine.enqueue_workflow_task',
            new=AsyncMock(return_value='child-task-id'),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        # Should succeed — child workflow created
        assert result is not None

    @pytest.mark.asyncio
    async def test_child_task_kwargs_ser_fails(self) -> None:
        row = self._base_row()

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is ENQUEUE_SUBWORKFLOW_TASK_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_NAME_SQL:
                return _one_result(SimpleNamespace(name='test_wf'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        broker = MagicMock()
        broker.app = MagicMock()

        child_task = MagicMock()
        child_task.name = 'child_task'
        child_task.args = ()
        child_task.kwargs = {'data': 'value'}
        child_task.waits_for = []
        child_task.args_from = {}
        child_task.workflow_ctx_from = None
        child_task.index = 0
        child_task.node_id = 'child-0'
        child_task.good_until = None
        child_task.fn = MagicMock()
        child_task.fn.task_options_json = None
        child_task.fn.task_queue_name = 'default'
        child_task.allow_failed_deps = False
        child_task.join = 'all'
        child_task.min_success = None
        child_task.queue = None
        child_task.priority = None
        child_task.__class__ = type('TaskNode', (), {})

        mock_spec = MagicMock()
        mock_spec.tasks = [child_task]
        mock_spec.success_policy = None
        mock_spec.output = None
        mock_spec.on_error = MagicMock(value='fail')
        mock_spec.definition_key = 'test.child.v1'
        mock_spec.name = 'child_wf'

        mock_wf_def = MagicMock()
        mock_wf_def.build_with.return_value = mock_spec
        mock_wf_def.name = 'TestWF'
        mock_wf_def._original_build_with = mock_wf_def.build_with

        original_dumps = engine.dumps_json

        def _dumps_failing(value: Any) -> Any:
            # Fail on child task kwargs
            if isinstance(value, dict) and 'data' in value:
                return Err(Exception('child task kwargs ser fail'))
            return original_dumps(value)

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=mock_wf_def,
        ), patch(
            'horsies.core.workflows.engine.validate_workflow_generic_output_match',
        ), patch(
            'horsies.core.workflows.engine.guard_no_positional_args',
        ), patch(
            'horsies.core.workflows.engine.dumps_json',
            side_effect=_dumps_failing,
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=AsyncMock(),
        ):
            result = await enqueue_subworkflow_task(
                session, broker, 'wf-1', 0, {}, 0, 'root-wf',
            )
        assert result is None


# ── 5. on_workflow_task_complete ─────────────────────────────────────


@pytest.mark.unit
class TestOnWorkflowTaskComplete:
    @pytest.mark.asyncio
    async def test_task_not_found_returns_early(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_WORKFLOW_TASK_BY_TASK_ID_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = TaskResult(ok='done')
        await on_workflow_task_complete(session, 'task-1', result)
        # Should return without error

    @pytest.mark.asyncio
    async def test_lock_acquisition_fails_returns_early(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_WORKFLOW_TASK_BY_TASK_ID_SQL:
                return _one_result(SimpleNamespace(workflow_id='wf-1', task_index=0))
            if stmt is LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = TaskResult(ok='done')
        await on_workflow_task_complete(session, 'task-1', result)

    @pytest.mark.asyncio
    async def test_already_terminal_skips(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_WORKFLOW_TASK_BY_TASK_ID_SQL:
                return _one_result(SimpleNamespace(workflow_id='wf-1', task_index=0))
            if stmt is LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL:
                return _one_result(SimpleNamespace(id='wf-1'))
            if stmt is UPDATE_WORKFLOW_TASK_RESULT_SQL:
                return _one_result(None)  # CAS failed
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = TaskResult(ok='done')
        await on_workflow_task_complete(session, 'task-1', result)


# ── 6. try_make_ready_and_enqueue ────────────────────────────────────


@pytest.mark.unit
class TestCheckAndPromoteTask:
    @pytest.mark.asyncio
    async def test_config_row_not_found(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_TASK_CONFIG_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        await try_make_ready_and_enqueue(session, None, 'wf-1', 0)

    @pytest.mark.asyncio
    async def test_no_dependencies_returns(self) -> None:
        config_row = SimpleNamespace(
            status='PENDING',
            dependencies=[],
            allow_failed_deps=False,
            join_type='all',
            min_success=None,
            workflow_ctx_from=None,
            is_subworkflow=False,
            wf_status='RUNNING',
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_TASK_CONFIG_SQL:
                return _one_result(config_row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        await try_make_ready_and_enqueue(session, None, 'wf-1', 0)

    @pytest.mark.asyncio
    async def test_mark_ready_race_returns(self) -> None:
        config_row = SimpleNamespace(
            status='PENDING',
            dependencies=[1],
            allow_failed_deps=False,
            join_type='all',
            min_success=None,
            workflow_ctx_from=None,
            is_subworkflow=False,
            wf_status='RUNNING',
        )
        dep_status_row = SimpleNamespace(status='COMPLETED', cnt=1)

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_TASK_CONFIG_SQL:
                return _one_result(config_row)
            # dep status counts
            if 'GROUP BY' in str(getattr(stmt, 'text', '')):
                return _rows_result([dep_status_row])
            if stmt is MARK_TASK_READY_SQL:
                return _one_result(None)  # Race: already processed
            return _rows_result([dep_status_row])

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        await try_make_ready_and_enqueue(session, None, 'wf-1', 0)


# ── 7. get_dependency_results ────────────────────────────────────────


@pytest.mark.unit
class TestGetDependencyResults:
    @pytest.mark.asyncio
    async def test_empty_indices_returns_empty(self) -> None:
        session = AsyncMock()
        result = await get_dependency_results(session, 'wf-1', [])
        assert result == {}

    @pytest.mark.asyncio
    async def test_corrupt_result_json_injects_deser_error(self) -> None:
        row = SimpleNamespace(
            task_index=0, status='COMPLETED', result='not{json',
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            return _rows_result([row])

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        results = await get_dependency_results(session, 'wf-1', [0])
        assert 0 in results
        assert results[0].is_err()
        assert results[0].err.error_code == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR

    @pytest.mark.asyncio
    async def test_task_result_from_json_fails_injects_deser_error(self) -> None:
        # Valid JSON but not a TaskResult shape
        row = SimpleNamespace(
            task_index=0, status='COMPLETED', result='{"unexpected": "shape"}',
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            return _rows_result([row])

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine.task_result_from_json',
            return_value=Err('parse failed'),
        ):
            results = await get_dependency_results(session, 'wf-1', [0])
        assert 0 in results
        assert results[0].is_err()
        assert results[0].err.error_code == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR


# ── 8. get_dependency_results_with_names ─────────────────────────────


@pytest.mark.unit
class TestGetDependencyResultsWithNames:
    @pytest.mark.asyncio
    async def test_empty_node_ids_returns_empty(self) -> None:
        session = AsyncMock()
        result = await get_dependency_results_with_names(session, 'wf-1', [])
        assert isinstance(result, DependencyResults)
        assert result.by_index == {}

    @pytest.mark.asyncio
    async def test_corrupt_result_json(self) -> None:
        row = SimpleNamespace(
            task_index=0, task_name='task_a', node_id='node-0',
            status='COMPLETED', result='not{json',
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            return _rows_result([row])

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        results = await get_dependency_results_with_names(session, 'wf-1', ['node-0'])
        assert 0 in results.by_index
        assert results.by_index[0].is_err()
        assert results.by_index[0].err.error_code == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR

    @pytest.mark.asyncio
    async def test_task_result_from_json_fails(self) -> None:
        row = SimpleNamespace(
            task_index=0, task_name='task_a', node_id='node-0',
            status='COMPLETED', result='{"valid": "json"}',
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            return _rows_result([row])

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine.task_result_from_json',
            return_value=Err('parse failed'),
        ):
            results = await get_dependency_results_with_names(session, 'wf-1', ['node-0'])
        assert 0 in results.by_index
        assert results.by_index[0].is_err()

    @pytest.mark.asyncio
    async def test_no_stored_result_skipped(self) -> None:
        row = SimpleNamespace(
            task_index=0, task_name='task_a', node_id='node-0',
            status='COMPLETED', result=None,
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            return _rows_result([row])

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        results = await get_dependency_results_with_names(session, 'wf-1', ['node-0'])
        # result=None with status COMPLETED → continue (skip), not stored
        assert 0 not in results.by_index


# ── 9. evaluate_workflow_success ─────────────────────────────────────


@pytest.mark.unit
class TestEvaluateWorkflowSuccess:
    @pytest.mark.asyncio
    async def test_string_policy_parsed(self) -> None:
        """success_policy_data as string → parsed, evaluated."""
        policy_str = '{"cases": [{"required_indices": [0]}]}'

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_TASK_STATUSES_SQL:
                return _rows_result([
                    SimpleNamespace(task_index=0, status='COMPLETED'),
                ])
            return _empty_result()

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await evaluate_workflow_success(
            session, 'wf-1', policy_str, False, 0,
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_corrupt_policy_falls_back(self) -> None:
        """Policy parses to non-dict → default behavior."""
        session = AsyncMock()
        session.execute = AsyncMock(return_value=_empty_result())
        # String that parses to a list, not dict
        result = await evaluate_workflow_success(
            session, 'wf-1', '[1,2,3]', False, 0,
        )
        # Default: not has_error and failed == 0 → True
        assert result is True

    @pytest.mark.asyncio
    async def test_empty_required_indices_skipped(self) -> None:
        """Case with empty required_indices → skip case."""
        policy = {'cases': [{'required_indices': []}]}

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_TASK_STATUSES_SQL:
                return _rows_result([])
            return _empty_result()

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await evaluate_workflow_success(
            session, 'wf-1', policy, False, 0,
        )
        # Empty required_indices → skipped → no case satisfied → False
        assert result is False


# ── 10. get_workflow_failure_error ───────────────────────────────────


@pytest.mark.unit
class TestGetWorkflowFailureError:
    @pytest.mark.asyncio
    async def test_no_policy_extracts_first_failed_error(self) -> None:
        """No success_policy → extract first failed task's error."""
        error = TaskError(error_code='TEST_ERR', message='test failure')
        tr = TaskResult(err=error)
        # Serialize the TaskResult as it would be stored in DB
        from horsies.core.codec.serde import dumps_json as real_dumps
        ser = real_dumps(tr)
        stored = ser.ok_value if not ser.is_err() else None

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_FIRST_FAILED_TASK_RESULT_SQL:
                return _one_result(SimpleNamespace(result=stored))
            return _empty_result()

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await get_workflow_failure_error(session, 'wf-1', None)
        assert result is not None

    @pytest.mark.asyncio
    async def test_no_policy_no_failed_returns_none(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_FIRST_FAILED_TASK_RESULT_SQL:
                return _one_result(None)
            return _empty_result()

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await get_workflow_failure_error(session, 'wf-1', None)
        assert result is None

    @pytest.mark.asyncio
    async def test_string_policy_parsed(self) -> None:
        policy_str = '{"cases": [{"required_indices": [0]}]}'

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_FIRST_FAILED_REQUIRED_TASK_SQL:
                return _one_result(None)
            return _empty_result()

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await get_workflow_failure_error(session, 'wf-1', policy_str)
        # No failed required task → WORKFLOW_SUCCESS_CASE_NOT_MET sentinel
        assert result is not None

    @pytest.mark.asyncio
    async def test_corrupt_policy_returns_none(self) -> None:
        session = AsyncMock()
        session.execute = AsyncMock(return_value=_empty_result())
        # Parses to list, not dict
        result = await get_workflow_failure_error(session, 'wf-1', '[1,2]')
        assert result is None


# ── 11. get_workflow_final_result ────────────────────────────────────


@pytest.mark.unit
class TestGetWorkflowFinalResult:
    @pytest.mark.asyncio
    async def test_non_string_node_id_skipped(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_WORKFLOW_OUTPUT_INDEX_SQL:
                return _one_result(SimpleNamespace(output_task_index=None))
            if stmt is GET_TERMINAL_TASK_RESULTS_SQL:
                return _rows_result([
                    SimpleNamespace(node_id=123, task_index=0, result='null'),
                ])
            return _empty_result()

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result = await get_workflow_final_result(session, 'wf-1')
        # int node_id → skipped, result dict should be empty
        assert result is not None

    @pytest.mark.asyncio
    async def test_task_result_parse_fails_stores_none(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_WORKFLOW_OUTPUT_INDEX_SQL:
                return _one_result(SimpleNamespace(output_task_index=None))
            if stmt is GET_TERMINAL_TASK_RESULTS_SQL:
                return _rows_result([
                    SimpleNamespace(
                        node_id='node-0', task_index=0,
                        result='{"valid": "but_not_taskresult"}',
                    ),
                ])
            return _empty_result()

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine.task_result_from_json',
            return_value=Err('parse failed'),
        ):
            result = await get_workflow_final_result(session, 'wf-1')
        assert result is not None


# ── 12. _handle_workflow_task_failure ─────────────────────────────────


@pytest.mark.unit
class TestHandleWorkflowTaskFailure:
    @pytest.mark.asyncio
    async def test_workflow_not_found_returns_true(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_WORKFLOW_ON_ERROR_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result_tr = TaskResult(err=TaskError(error_code='ERR', message='fail'))
        result = await _handle_workflow_task_failure(session, 'wf-1', 0, result_tr)
        assert result is True

    @pytest.mark.asyncio
    async def test_unknown_on_error_returns_true(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_WORKFLOW_ON_ERROR_SQL:
                return _one_result(SimpleNamespace(on_error='unknown_value'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        result_tr = TaskResult(err=TaskError(error_code='ERR', message='fail'))
        result = await _handle_workflow_task_failure(session, 'wf-1', 0, result_tr)
        assert result is True


# ── 13. check_workflow_completion ────────────────────────────────────


@pytest.mark.unit
class TestCheckWorkflowCompletion:
    @pytest.mark.asyncio
    async def test_lock_fails_returns(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        await check_workflow_completion(session, 'wf-1')

    @pytest.mark.asyncio
    async def test_workflow_not_found_returns(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL:
                return _one_result(SimpleNamespace(id='wf-1'))
            if stmt is GET_WORKFLOW_COMPLETION_STATUS_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        await check_workflow_completion(session, 'wf-1')


# ── 14. on_subworkflow_complete ──────────────────────────────────────


@pytest.mark.unit
class TestOnChildWorkflowComplete:
    @pytest.mark.asyncio
    async def test_child_not_found_returns(self) -> None:
        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_CHILD_WORKFLOW_INFO_SQL:
                return _one_result(None)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        await on_subworkflow_complete(session, 'child-1')

    @pytest.mark.asyncio
    async def test_not_a_child_workflow_returns(self) -> None:
        row = SimpleNamespace(
            status='COMPLETED',
            result=None,
            error=None,
            parent_workflow_id=None,
            parent_task_index=None,
            total=2, completed=2, failed=0, skipped=0,
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_CHILD_WORKFLOW_INFO_SQL:
                return _one_result(row)
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)
        await on_subworkflow_complete(session, 'child-1')

    @pytest.mark.asyncio
    async def test_error_deser_none_falls_back_to_raw_string(self) -> None:
        """When error JSON deser returns None → uses raw string[:200]."""
        row = SimpleNamespace(
            status='FAILED',
            result=None,
            error='not-valid-json{{{',
            parent_workflow_id='wf-parent',
            parent_task_index=0,
            total=2, completed=1, failed=1, skipped=0,
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_CHILD_WORKFLOW_INFO_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_ON_ERROR_SQL:
                return _one_result(SimpleNamespace(on_error='fail'))
            if stmt is GET_WORKFLOW_STATUS_SQL:
                return _one_result(SimpleNamespace(status='RUNNING'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine._process_dependents',
            new=AsyncMock(),
        ), patch(
            'horsies.core.workflows.engine.check_workflow_completion',
            new=AsyncMock(),
        ):
            await on_subworkflow_complete(session, 'child-1')
        # Test passes if no crash (error_summary = raw string)

    @pytest.mark.asyncio
    async def test_parent_paused_stops_propagation(self) -> None:
        row = SimpleNamespace(
            status='COMPLETED',
            result=None,
            error=None,
            parent_workflow_id='wf-parent',
            parent_task_index=0,
            total=2, completed=2, failed=0, skipped=0,
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_CHILD_WORKFLOW_INFO_SQL:
                return _one_result(row)
            if stmt is GET_WORKFLOW_STATUS_SQL:
                return _one_result(SimpleNamespace(status='PAUSED'))
            return _one_result(SimpleNamespace())

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine._process_dependents',
            new=AsyncMock(),
        ) as mock_proc:
            await on_subworkflow_complete(session, 'child-1')
        # _process_dependents should NOT have been called
        mock_proc.assert_not_awaited()


# ── 15. _load_workflow_def_from_key ──────────────────────────────────


@pytest.mark.unit
class TestLoadWorkflowDefFromKey:
    def test_registry_miss_returns_none(self) -> None:
        with patch(
            'horsies.core.workflows.registry.get_workflow_definition',
            return_value=None,
        ):
            result = _load_workflow_def_from_key('missing.workflow.v1')
        assert result is None

    def test_registry_hit_returns_workflow_def(self) -> None:
        workflow_def = MagicMock()
        with patch(
            'horsies.core.workflows.registry.get_workflow_definition',
            return_value=workflow_def,
        ):
            result = _load_workflow_def_from_key('some.workflow.v1')
        assert result is workflow_def


# ── 16. _resolve_workflow_def_nodes ──────────────────────────────────


@pytest.mark.unit
class TestResolveWorkflowDefNodes:
    def test_assigns_node_id_from_attr_name(self) -> None:
        """Node with node_id=None → assigned attr_name."""
        mock_node = MagicMock()
        mock_node.index = None
        mock_node.node_id = None
        mock_node.workflow_ctx_from = None
        mock_node._frozen = False

        mock_wf_def = MagicMock()
        mock_wf_def.get_workflow_nodes.return_value = [('my_task', mock_node)]

        result = _resolve_workflow_def_nodes(mock_wf_def)
        assert 0 in result
        assert result[0].node_id == 'my_task'
        assert result[0].index == 0

    def test_empty_nodes_returns_empty(self) -> None:
        mock_wf_def = MagicMock()
        mock_wf_def.get_workflow_nodes.return_value = []
        result = _resolve_workflow_def_nodes(mock_wf_def)
        assert result == {}


# ── 17. _build_workflow_context_data ─────────────────────────────────


@pytest.mark.unit
class TestBuildWorkflowContextData:
    @pytest.mark.asyncio
    async def test_fetches_subworkflow_summaries(self) -> None:
        """ctx_from_ids present → summaries populated from SQL."""
        summary_row = SimpleNamespace(
            node_id='sub-node', sub_workflow_summary='{"status":"COMPLETED"}',
        )

        async def _dispatch(stmt: Any, params: Any) -> MagicMock:
            if stmt is GET_SUBWORKFLOW_SUMMARIES_SQL:
                return _rows_result([summary_row])
            return _rows_result([])

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=_dispatch)

        with patch(
            'horsies.core.workflows.engine.get_dependency_results_with_names',
            new=AsyncMock(return_value=DependencyResults()),
        ):
            result = await _build_workflow_context_data(
                session, 'wf-1', 0, 'my_task', ['sub-node'],
            )
        assert result['summaries_by_id'] == {'sub-node': '{"status":"COMPLETED"}'}

    @pytest.mark.asyncio
    async def test_empty_ctx_from_ids_no_summaries(self) -> None:
        """Empty ctx_from_ids → no summary fetch."""
        session = AsyncMock()
        session.execute = AsyncMock(return_value=_rows_result([]))

        with patch(
            'horsies.core.workflows.engine.get_dependency_results_with_names',
            new=AsyncMock(return_value=DependencyResults()),
        ):
            result = await _build_workflow_context_data(
                session, 'wf-1', 0, 'my_task', [],
            )
        assert result['summaries_by_id'] == {}
