"""The typed read of ``horsies_workflow_step_row`` and the drive of parent advances."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.workflows import steps
from horsies.core.workflows.sql import SubWorkflowParams
from horsies.core.workflows.steps import (
    MAX_PARENT_ADVANCES,
    ChildStartSource,
    ChildStartDisposition,
    NoParentAdvance,
    ParentAdvanceOwed,
    ParentChainTooLong,
    SourceStatus,
    StartDisposition,
    add_parent_advance,
    advance_parents,
    decode_step_rows,
    require_no_child_starts,
)

pytestmark = [pytest.mark.unit]

_PARENT = uuid.uuid4()
_CHILD = uuid.uuid4()

_COLUMNS = (
    'row_kind', 'disposition', 'workflow_id', 'workflow_status', 'advance_child_workflow_id',
    'detail', 'task_index', 'node_input', 'args_from', 'sub_workflow_name',
    'sub_definition_key', 'sub_workflow_params', 'parent_depth', 'root_workflow_id',
    'source_task_index', 'source_node_id', 'source_status', 'source_task_name',
    'source_definition_key', 'source_result', 'source_summary', 'workflow_name',
)


def _row(**values: Any) -> SimpleNamespace:
    return SimpleNamespace(**{column: values.get(column) for column in _COLUMNS})


def _outcome(disposition: str = 'STARTED', **values: Any) -> SimpleNamespace:
    return _row(row_kind='OUTCOME', disposition=disposition, workflow_id=_PARENT, **values)


def _child_start(task_index: int = 2, **values: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        'row_kind': 'CHILD_START',
        'workflow_id': _PARENT,
        'workflow_name': 'parent',
        'task_index': task_index,
        'node_input': '{}',
        'sub_workflow_name': 'child',
        'sub_definition_key': 'child.v1',
        'sub_workflow_params': 'NOT_GIVEN',
        'parent_depth': 0,
        'root_workflow_id': _PARENT,
    }
    return _row(**{**defaults, **values})


def _source(task_index: int = 2, source_task_index: int = 0, **values: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        'row_kind': 'SOURCE',
        'workflow_id': _PARENT,
        'task_index': task_index,
        'source_task_index': source_task_index,
        'source_node_id': 'parent:0',
        'source_status': 'COMPLETED',
        'source_task_name': 'source_task',
        'source_result': '{"__h_task_result__":true,"ok":1,"err":null}',
    }
    return _row(**{**defaults, **values})


class TestDecodeStepRows:
    def test_an_outcome_row_alone(self) -> None:
        step = decode_step_rows(
            [_outcome(workflow_status='RUNNING')], 'f', StartDisposition,
        )

        assert step.outcome.disposition is StartDisposition.STARTED
        assert step.outcome.workflow_status == 'RUNNING'
        assert step.outcome.parent_advance == NoParentAdvance()
        assert step.owed_child_starts == ()

    def test_an_advance_names_the_child_workflow(self) -> None:
        step = decode_step_rows(
            [_outcome(advance_child_workflow_id=_CHILD)], 'f', StartDisposition,
        )

        assert step.outcome.parent_advance == ParentAdvanceOwed(child_workflow_id=str(_CHILD))

    def test_source_rows_attach_to_their_child_start_in_any_order(self) -> None:
        step = decode_step_rows(
            [_source(source_task_index=1), _outcome(), _child_start(), _source()],
            'f',
            StartDisposition,
        )

        (owed,) = step.owed_child_starts
        assert owed.workflow_id == str(_PARENT)
        assert owed.workflow_name == 'parent'
        assert owed.sub_workflow_params is SubWorkflowParams.NOT_GIVEN
        assert [source.task_index for source in owed.sources] == [1, 0]
        assert owed.sources[1] == ChildStartSource(
            task_index=0,
            node_id='parent:0',
            status=SourceStatus.COMPLETED,
            task_name='source_task',
            definition_key=None,
            result='{"__h_task_result__":true,"ok":1,"err":null}',
        )

    @pytest.mark.parametrize('count', [0, 2])
    def test_not_one_outcome_row_is_refused(self, count: int) -> None:
        with pytest.raises(OutcomeDecodeError, match=f'{count} OUTCOME rows'):
            decode_step_rows([_outcome() for _ in range(count)], 'f', StartDisposition)

    def test_an_unknown_row_kind_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="row_kind 'OTHER'"):
            decode_step_rows([_outcome(), _row(row_kind='OTHER')], 'f', StartDisposition)

    def test_a_disposition_outside_the_function_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match="disposition 'APPLIED'"):
            decode_step_rows([_outcome('APPLIED')], 'f', StartDisposition)

    def test_a_source_without_its_child_start_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='has no CHILD_START row'):
            decode_step_rows([_outcome(), _source()], 'f', StartDisposition)

    def test_a_params_choice_outside_the_contract_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='sub_workflow_params'):
            decode_step_rows(
                [_outcome(), _child_start(sub_workflow_params='MAYBE')], 'f', StartDisposition,
            )

    def test_the_name_of_a_workflow_the_call_inserted_comes_from_its_tree(self) -> None:
        step = decode_step_rows(
            [_outcome(), _child_start(workflow_name=None)],
            'f',
            StartDisposition,
            {str(_PARENT): 'inserted parent'},
        )

        assert step.owed_child_starts[0].workflow_name == 'inserted parent'

    def test_a_child_start_without_the_workflow_name_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='workflow_name'):
            decode_step_rows(
                [_outcome(), _child_start(workflow_name=None)], 'f', StartDisposition,
            )

    def test_a_source_status_that_is_not_terminal_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='source_status'):
            decode_step_rows(
                [_outcome(), _child_start(), _source(source_status='RUNNING')],
                'f',
                StartDisposition,
            )


class TestRequireNoChildStarts:
    def test_a_step_with_no_owed_start_gives_its_outcome(self) -> None:
        step = decode_step_rows([_outcome()], 'f', StartDisposition)

        assert require_no_child_starts(step, 'f') is step.outcome

    def test_an_owed_start_of_an_autocommit_step_is_refused(self) -> None:
        step = decode_step_rows([_outcome(), _child_start()], 'f', StartDisposition)

        with pytest.raises(OutcomeDecodeError, match='1 owed child starts'):
            require_no_child_starts(step, 'f')


class TestAddParentAdvance:
    def test_each_child_is_named_once(self) -> None:
        advances: list[str] = []

        add_parent_advance(advances, ParentAdvanceOwed(child_workflow_id='a'))
        add_parent_advance(advances, NoParentAdvance())
        add_parent_advance(advances, ParentAdvanceOwed(child_workflow_id='a'))
        add_parent_advance(advances, ParentAdvanceOwed(child_workflow_id='b'))

        assert advances == ['a', 'b']


class TestChildStartDisposition:
    @pytest.mark.parametrize('value', [member.value for member in ChildStartDisposition])
    def test_each_disposition_decodes(self, value: str) -> None:
        step = decode_step_rows([_outcome(value)], 'f', ChildStartDisposition)

        assert step.outcome.disposition.value == value


@pytest.mark.asyncio
class TestAdvanceParents:
    async def test_the_advances_a_level_owes_run_before_the_rest(self) -> None:
        owed_by_level = {'a': ('a1',), 'a1': (), 'b': ()}
        ran: list[str] = []

        async def level(_broker: Any, _factory: Any, child: str) -> tuple[str, ...]:
            ran.append(child)
            return owed_by_level[child]

        with patch.object(steps, 'advance_parent', new=AsyncMock(side_effect=level)):
            failure = await advance_parents(MagicMock(), MagicMock(), ['a', 'b'])

        assert failure is None
        assert ran == ['a', 'a1', 'b']

    async def test_a_failed_level_names_what_is_still_owed(self) -> None:
        cause = RuntimeError('connection lost')

        async def level(_broker: Any, _factory: Any, child: str) -> tuple[str, ...]:
            if child == 'b':
                raise cause
            return ()

        with patch.object(steps, 'advance_parent', new=AsyncMock(side_effect=level)):
            failure = await advance_parents(MagicMock(), MagicMock(), ['a', 'b', 'c'])

        assert failure is not None
        assert failure.failed_child_workflow_id == 'b'
        assert failure.still_owed == ('b', 'c')
        assert failure.cause is cause

    async def test_a_chain_past_the_bound_fails_closed(self) -> None:
        async def level(_broker: Any, _factory: Any, child: str) -> tuple[str, ...]:
            return (f'{child}+',)

        with patch.object(steps, 'advance_parent', new=AsyncMock(side_effect=level)):
            failure = await advance_parents(MagicMock(), MagicMock(), ['a'])

        assert failure is not None
        assert isinstance(failure.cause, ParentChainTooLong)
        assert failure.failed_child_workflow_id == 'a' + '+' * MAX_PARENT_ADVANCES
