"""A COMPLETED workflow with an explicit output node always stores an envelope.

A success case that does not require the output node lets a workflow be
COMPLETED while that node is SKIPPED or FAILED. A SKIPPED output node stores an
error envelope with ``WORKFLOW_OUTPUT_SKIPPED``. A FAILED output node stores its
own error.

A ``SubWorkflowNode`` is COMPLETED only when the child's output is ok, as a task
node is. An err output makes a FAILED node that holds the child's text. A stored
child result that does not parse makes a FAILED node with
``RESULT_DESERIALIZATION_ERROR``, so a COMPLETED node always holds an ok.

An outputless workflow has no output node, so its terminal nodes are its output.
A SKIPPED terminal gets the same code in its entry. The shape of that envelope
and the ``None`` entry of a row that does not parse stay as they are.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import SerializationError
from horsies.core.codec.typed import (
    decode_task_error,
    decode_task_result,
    encode_task_error,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    BUILTIN_CODE_REGISTRY,
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import (
    SubWorkflowSummary,
    WorkflowDefinition,
    WorkflowStatus,
    WorkflowTaskStatus,
)
from horsies.core.models.workflow.handle import (
    _OUTPUTLESS_TERMINALS,
    _decode_per_task_envelope,
    _decode_workflow_envelope,
)
from horsies.core.types.result import Err
from horsies.core.workflows.engine import (
    _ChildOutputAbsent,
    _ChildOutputError,
    _ChildOutputless,
    _ChildOutputUnreadable,
    _ChildOutputValue,
    _decode_err_only,
    _decode_stored_task_result,
    _parse_child_output,
    _unreadable_child_error,
    check_workflow_completion,
    get_dependency_results,
    get_dependency_results_with_names,
    get_workflow_final_result,
    on_subworkflow_complete,
)
from horsies.core.workflows.registry import (
    register_workflow_definition,
    unregister_workflow_definition,
)
from horsies.core.workflows.sql import (
    GET_CHILD_WORKFLOW_INFO_SQL,
    GET_DEPENDENCY_RESULTS_SQL,
    GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL,
    GET_FIRST_FAILED_TASK_RESULT_SQL,
    GET_OUTPUT_TASK_RESULT_SQL,
    GET_TASK_RESULTS_BY_INDEXES_SQL,
    GET_TASK_STATUSES_SQL,
    GET_WORKFLOW_COMPLETION_STATUS_SQL,
    GET_WORKFLOW_DAG_EDGES_SQL,
    GET_WORKFLOW_ON_ERROR_SQL,
    GET_WORKFLOW_STATUS_SQL,
    LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
    MARK_WORKFLOW_COMPLETED_SQL,
    MARK_WORKFLOW_FAILED_SQL,
    PAUSE_WORKFLOW_ON_ERROR_SQL,
    SET_WORKFLOW_ERROR_SQL,
    UPDATE_PARENT_NODE_RESULT_SQL,
)
from horsies.monitoring.models import ErrorCategory
from horsies.monitoring.queries import categorize_error_code

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '11111111-1111-7111-8111-111111111111'
OUTPUT_INDEX = 2
OUTPUT_NODE_ID = 'shipping:2'
OUTPUT_TASK_NAME = 'label_parcel'
CHILD_KEY = 'tests.unit.workflow_output_skipped.child.v1'
DUMPS_JSON = 'horsies.core.workflows.engine.dumps_json'
OK_RESULT_TEXT = '{"__h_task_result__":true,"ok":7,"err":null}'
ERR_RESULT_TEXT = serialize_error_payload(
    TaskResult(err=TaskError(error_code='LABEL_REFUSED', message='refused')),
)
OUTPUTLESS_RESULT_TEXT = (
    '{"__h_task_result__":true,"__h_outputless_terminals__":true,'
    '"ok":{"results_by_id":{},"task_name_by_id":{},'
    '"definition_key_by_id":{}},"err":null}'
)
UNREADABLE_TEXTS = {
    'text-null': 'null',
    'not-json': 'not{json',
    'not-an-envelope': '{"ok": 99}',
    'both-slots': '{"__h_task_result__":true,"ok":1,"err":{"message":"x"}}',
    'err-slot-refused': '{"__h_task_result__":true,"ok":null,"err":7}',
}
LIVE_STATUSES = [
    status for status in WorkflowTaskStatus if not status.is_terminal
]


class _Session:
    """An ``AsyncSession`` stand-in. ``rows`` maps a statement to its rows."""

    def __init__(self, rows: dict[Any, list[Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[Any, dict[str, Any]]] = []
        self.session = AsyncMock()
        self.session.execute = AsyncMock(side_effect=self._dispatch)
        # The engine queues parent propagations in ``session.info``.
        self.session.info = {}

    async def _dispatch(
        self, statement: Any, params: dict[str, Any] | None = None,
    ) -> MagicMock:
        self.calls.append((statement, params or {}))
        rows = self.rows.get(statement, [])
        result = MagicMock()
        result.fetchone.return_value = rows[0] if rows else None
        result.fetchall.return_value = rows
        return result

    def executed(self, statement: Any) -> bool:
        return any(called is statement for called, _ in self.calls)

    def params_of(self, statement: Any) -> dict[str, Any]:
        found = [params for called, params in self.calls if called is statement]
        assert len(found) == 1, f'expected one execution, got {len(found)}'
        return found[0]


class _ChildDefinition(WorkflowDefinition[int]):
    """The child workflow that the parent node of these tests runs."""

    name = 'tests_unit_output_skipped_child'
    definition_key = CHILD_KEY

    @classmethod
    def build_with(cls, app: Any, *args: Any, **params: Any) -> Any:
        raise AssertionError('these tests do not start the child')


@pytest.fixture
def registered_child() -> Iterator[None]:
    register_workflow_definition(_ChildDefinition)
    yield
    unregister_workflow_definition(CHILD_KEY)


@pytest.fixture
def app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql+psycopg://u:p@localhost:5432/db'),
            ),
        ),
    )


def _output_row(status: str, result: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        node_id=OUTPUT_NODE_ID,
        task_name=OUTPUT_TASK_NAME,
        status=status,
        result=result,
    )


async def _final_result(row: SimpleNamespace | None) -> str:
    recorder = _Session({GET_OUTPUT_TASK_RESULT_SQL: [] if row is None else [row]})
    return await get_workflow_final_result(
        recorder.session, WORKFLOW_ID, OUTPUT_INDEX,
    )


async def _skipped_output_text() -> str:
    return await _final_result(_output_row('SKIPPED', None))


def _stored_err(text: str) -> TaskError:
    result = decode_task_result(json.loads(text), int)
    assert result.is_err()
    return result.err_value


class TestExplicitOutputResult:
    """Each outcome of the explicit-output branch."""

    @pytest.mark.asyncio
    async def test_skipped_output_stores_the_named_outcome(self) -> None:
        stored = await _skipped_output_text()

        envelope = json.loads(stored)
        assert set(envelope) == {'__h_task_result__', 'ok', 'err'}
        assert envelope['__h_task_result__'] is True
        assert envelope['ok'] is None
        err = _stored_err(stored)
        assert err.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert err.data == {
            'workflow_id': WORKFLOW_ID,
            'output_node_id': OUTPUT_NODE_ID,
            'output_task_index': OUTPUT_INDEX,
            'output_task_name': OUTPUT_TASK_NAME,
        }
        assert WORKFLOW_ID in (err.message or '')
        assert OUTPUT_NODE_ID in (err.message or '')

    @pytest.mark.asyncio
    async def test_skipped_status_decides_over_a_stored_result(self) -> None:
        """No skip writer stores a result. The status names the state."""
        stored = await _final_result(_output_row('SKIPPED', OK_RESULT_TEXT))

        assert _stored_err(stored).error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status', 'stored'),
        [('COMPLETED', OK_RESULT_TEXT), ('FAILED', ERR_RESULT_TEXT)],
        ids=['completed', 'failed-outside-the-required-set'],
    )
    async def test_stored_result_is_returned_unchanged(
        self, status: str, stored: str,
    ) -> None:
        assert await _final_result(_output_row(status, stored)) == stored

    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', ['COMPLETED', 'FAILED'])
    @pytest.mark.parametrize('stored', [None, ''], ids=['null', 'empty'])
    async def test_node_without_a_result_is_stored_data_that_does_not_parse(
        self, status: str, stored: str | None,
    ) -> None:
        text = await _final_result(_output_row(status, stored))

        err = _stored_err(text)
        assert err.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert status in (err.message or '')
        assert OUTPUT_NODE_ID in (err.message or '')

    @pytest.mark.asyncio
    async def test_absent_row_raises_with_subject_and_cause(self) -> None:
        with pytest.raises(AssertionError) as raised:
            await _final_result(None)

        message = str(raised.value)
        assert WORKFLOW_ID in message
        assert f'output_task_index {OUTPUT_INDEX}' in message

    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', LIVE_STATUSES, ids=lambda s: s.value)
    async def test_live_status_raises_with_subject_and_cause(
        self, status: WorkflowTaskStatus,
    ) -> None:
        with pytest.raises(AssertionError) as raised:
            await _final_result(_output_row(status.value, None))

        message = str(raised.value)
        assert WORKFLOW_ID in message
        assert OUTPUT_NODE_ID in message
        assert status.value in message

    @pytest.mark.asyncio
    async def test_unknown_status_raises_with_subject_and_cause(self) -> None:
        with pytest.raises(AssertionError) as raised:
            await _final_result(_output_row('GARBAGE', OK_RESULT_TEXT))

        assert WORKFLOW_ID in str(raised.value)
        assert 'GARBAGE' in str(raised.value)

    @pytest.mark.asyncio
    async def test_envelope_that_does_not_serialize_raises(self) -> None:
        refused = Err(SerializationError('forced'))

        with patch(DUMPS_JSON, return_value=refused):
            with pytest.raises(SerializationError) as raised:
                await _skipped_output_text()

        assert f'output result of workflow {WORKFLOW_ID}' in str(raised.value)

    def test_statement_reads_what_the_branch_matches_on(self) -> None:
        """Only this pin sees the statement: the fakes supply every column."""
        statement = str(GET_OUTPUT_TASK_RESULT_SQL)

        assert 'SELECT node_id, task_name, status, result' in statement


class TestCompletionCheck:
    """The branch runs from its real caller, under a real policy evaluation."""

    @staticmethod
    def _session(
        *, statuses: dict[int, str], output: SimpleNamespace | None,
    ) -> _Session:
        return _Session({
            GET_WORKFLOW_COMPLETION_STATUS_SQL: [
                SimpleNamespace(
                    status='RUNNING',
                    completed_at=None,
                    error='{"message": "first failure"}',
                    success_policy={'cases': [{'required_indices': [0]}]},
                    name='shipping',
                    output_task_index=OUTPUT_INDEX,
                    parent_workflow_id=None,
                    incomplete=0,
                    failed=1,
                    completed=1,
                    total=len(statuses),
                ),
            ],
            GET_TASK_STATUSES_SQL: [
                SimpleNamespace(task_index=index, status=status)
                for index, status in statuses.items()
            ],
            GET_OUTPUT_TASK_RESULT_SQL: [] if output is None else [output],
        })

    @pytest.mark.asyncio
    async def test_success_case_without_the_output_stores_the_named_outcome(
        self,
    ) -> None:
        recorder = self._session(
            statuses={0: 'COMPLETED', 1: 'FAILED', 2: 'SKIPPED'},
            output=_output_row('SKIPPED', None),
        )

        await check_workflow_completion(
            recorder.session, WORKFLOW_ID, lock_held=True,
        )

        stored = recorder.params_of(MARK_WORKFLOW_COMPLETED_SQL)['result']
        assert _stored_err(stored).error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert not recorder.executed(MARK_WORKFLOW_FAILED_SQL)

    @pytest.mark.asyncio
    async def test_failed_output_outside_the_required_set_stores_its_error(
        self,
    ) -> None:
        recorder = self._session(
            statuses={0: 'COMPLETED', 1: 'SKIPPED', 2: 'FAILED'},
            output=_output_row('FAILED', ERR_RESULT_TEXT),
        )

        await check_workflow_completion(
            recorder.session, WORKFLOW_ID, lock_held=True,
        )

        params = recorder.params_of(MARK_WORKFLOW_COMPLETED_SQL)
        assert params['result'] == ERR_RESULT_TEXT

    @pytest.mark.asyncio
    async def test_absent_output_row_writes_no_completion(self) -> None:
        recorder = self._session(statuses={0: 'COMPLETED'}, output=None)

        with pytest.raises(AssertionError):
            await check_workflow_completion(
                recorder.session, WORKFLOW_ID, lock_held=True,
            )

        assert not recorder.executed(MARK_WORKFLOW_COMPLETED_SQL)


class TestOutputlessBranch:
    """A SKIPPED terminal is the one row that gets a named entry."""

    @staticmethod
    async def _stored(status: str, result: str | None) -> dict[str, Any]:
        recorder = _Session({
            GET_WORKFLOW_DAG_EDGES_SQL: [
                SimpleNamespace(task_index=3, dependencies=[]),
            ],
            GET_TASK_RESULTS_BY_INDEXES_SQL: [
                SimpleNamespace(
                    node_id='terminal',
                    task_index=3,
                    task_name='terminal_task',
                    status=status,
                    result=result,
                    sub_definition_key=None,
                ),
            ],
        })

        stored = await get_workflow_final_result(
            recorder.session, WORKFLOW_ID, None,
        )

        assert not recorder.executed(GET_OUTPUT_TASK_RESULT_SQL)
        return json.loads(stored)

    @pytest.mark.asyncio
    async def test_skipped_terminal_holds_the_named_outcome(self) -> None:
        stored = await self._stored('SKIPPED', None)

        entry = stored['ok']['results_by_id']['terminal']
        err = decode_task_result(entry, int).err_value
        assert err.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert err.data == {
            'workflow_id': WORKFLOW_ID,
            'output_node_id': 'terminal',
            'output_task_index': 3,
            'output_task_name': 'terminal_task',
        }
        assert 'terminal' in (err.message or '')

    @pytest.mark.asyncio
    async def test_outer_shape_and_markers_do_not_change(self) -> None:
        stored = await self._stored('SKIPPED', None)

        del stored['ok']['results_by_id']['terminal']
        assert stored == {
            '__h_task_result__': True,
            '__h_outputless_terminals__': True,
            'ok': {
                'results_by_id': {},
                'task_name_by_id': {'terminal': 'terminal_task'},
                'definition_key_by_id': {'terminal': None},
            },
            'err': None,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', ['COMPLETED', 'FAILED'])
    @pytest.mark.parametrize(
        'result',
        [None, '', 'not{json', '{"valid": "but_not_taskresult"}'],
        ids=['null', 'empty', 'not-json', 'not-an-envelope'],
    )
    async def test_row_that_does_not_parse_keeps_its_none_entry(
        self, status: str, result: str | None,
    ) -> None:
        stored = await self._stored(status, result)

        assert stored['ok']['results_by_id'] == {'terminal': None}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status', 'result'),
        [('COMPLETED', OK_RESULT_TEXT), ('FAILED', ERR_RESULT_TEXT)],
        ids=['completed', 'failed'],
    )
    async def test_stored_envelope_is_the_entry(
        self, status: str, result: str,
    ) -> None:
        stored = await self._stored(status, result)

        assert stored['ok']['results_by_id'] == {'terminal': json.loads(result)}

    @pytest.mark.asyncio
    async def test_failed_serialization_keeps_the_fallback_text(self) -> None:
        recorder = _Session({
            GET_WORKFLOW_DAG_EDGES_SQL: [
                SimpleNamespace(task_index=0, dependencies=[]),
            ],
        })

        with patch(DUMPS_JSON, return_value=Err(SerializationError('forced'))):
            stored = await get_workflow_final_result(
                recorder.session, WORKFLOW_ID, None,
            )

        assert stored == 'null'

    @pytest.mark.asyncio
    async def test_get_decodes_the_entry(self, app: Horsies) -> None:
        @app.task(task_name='terminal_task')
        def terminal_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        _ = terminal_task
        stored = await self._stored('SKIPPED', None)

        decoded = _decode_workflow_envelope(
            stored, _OUTPUTLESS_TERMINALS, app=app, workflow_id=WORKFLOW_ID,
        )

        entry = decoded.ok_value['terminal']
        assert entry.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    def test_statement_reads_the_status(self) -> None:
        """Only this pin sees the statement: the fakes supply every column."""
        statement = str(GET_TASK_RESULTS_BY_INDEXES_SQL)

        assert 'SELECT node_id, task_index, task_name, status, result' in statement


class TestReaders:
    """Each decoder of a stored result gives the named outcome."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize('out_type', [int, None], ids=['typed', 'no-out-type'])
    async def test_get_decodes_the_workflow_result(self, out_type: Any) -> None:
        stored = await _skipped_output_text()

        decoded = _decode_workflow_envelope(
            json.loads(stored), out_type, app=None, workflow_id=WORKFLOW_ID,
        )

        assert decoded.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    @pytest.mark.asyncio
    async def test_result_for_decodes_the_node_result(
        self, app: Horsies, registered_child: None,
    ) -> None:
        stored = await _skipped_output_text()

        decoded = _decode_per_task_envelope(
            json.loads(stored),
            app=app,
            task_name=_ChildDefinition.name,
            node_id='parent:0',
            sub_definition_key=CHILD_KEY,
        )

        assert decoded.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    @pytest.mark.asyncio
    async def test_downstream_decode_is_not_corrupt(
        self, app: Horsies, registered_child: None,
    ) -> None:
        stored = await _skipped_output_text()

        decoded = _decode_stored_task_result(
            stored,
            app=app,
            task_name=_ChildDefinition.name,
            context={'workflow_id': WORKFLOW_ID},
            sub_definition_key=CHILD_KEY,
        )

        assert decoded.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    @pytest.mark.asyncio
    async def test_err_only_decode(self) -> None:
        decoded = _decode_err_only(await _skipped_output_text())

        assert decoded is not None
        assert decoded.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    @staticmethod
    async def _dependency_row(*, node_id: str | None = None) -> SimpleNamespace:
        return SimpleNamespace(
            task_index=0,
            task_name=_ChildDefinition.name,
            node_id=node_id,
            status='FAILED',
            result=await _skipped_output_text(),
            sub_definition_key=CHILD_KEY,
        )

    @pytest.mark.asyncio
    async def test_dependency_results_of_a_failed_node(
        self, app: Horsies, registered_child: None,
    ) -> None:
        recorder = _Session({
            GET_DEPENDENCY_RESULTS_SQL: [await self._dependency_row()],
        })

        results, _, _ = await get_dependency_results(
            recorder.session, WORKFLOW_ID, [0], app=app,
        )

        assert results[0].err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED

    @pytest.mark.asyncio
    async def test_dependency_results_with_names_of_a_failed_node(
        self, app: Horsies, registered_child: None,
    ) -> None:
        recorder = _Session({
            GET_DEPENDENCY_RESULTS_WITH_NAMES_SQL: [
                await self._dependency_row(node_id='parent:0'),
            ],
        })

        results = await get_dependency_results_with_names(
            recorder.session, WORKFLOW_ID, ['parent:0'], app=app,
        )

        by_id = results.by_id['parent:0']
        assert by_id.err_value.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED


class TestChildOutputParse:
    """Each stored form of a child result is one variant."""

    def test_ok_envelope(self) -> None:
        parsed = _parse_child_output(OK_RESULT_TEXT)

        assert parsed == _ChildOutputValue(value=7, text=OK_RESULT_TEXT)

    def test_err_envelope(self) -> None:
        parsed = _parse_child_output(ERR_RESULT_TEXT)

        assert isinstance(parsed, _ChildOutputError)
        assert parsed.error.error_code == 'LABEL_REFUSED'
        assert parsed.text == ERR_RESULT_TEXT

    def test_outputless_envelope(self) -> None:
        assert _parse_child_output(OUTPUTLESS_RESULT_TEXT) == _ChildOutputless()

    @pytest.mark.parametrize('stored', [None, ''], ids=['null', 'empty'])
    def test_no_text(self, stored: str | None) -> None:
        assert _parse_child_output(stored) == _ChildOutputAbsent()

    @pytest.mark.parametrize(
        'stored', UNREADABLE_TEXTS.values(), ids=UNREADABLE_TEXTS.keys(),
    )
    def test_text_that_does_not_parse(self, stored: str) -> None:
        parsed = _parse_child_output(stored)

        assert isinstance(parsed, _ChildOutputUnreadable)
        assert parsed.text == stored
        assert parsed.cause


class TestParentNode:
    """A node is COMPLETED only when the output of its COMPLETED child is ok."""

    @staticmethod
    def _session(
        child_result: str | None, *, on_error: str = 'fail',
    ) -> _Session:
        return _Session({
            GET_CHILD_WORKFLOW_INFO_SQL: [
                SimpleNamespace(
                    status='COMPLETED',
                    result=child_result,
                    error=None,
                    parent_workflow_id='wf-parent',
                    parent_task_index=0,
                    total=3, completed=1, failed=1, skipped=1,
                ),
            ],
            LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL: [
                SimpleNamespace(id='wf-parent'),
            ],
            UPDATE_PARENT_NODE_RESULT_SQL: [SimpleNamespace(task_index=0)],
            GET_WORKFLOW_ON_ERROR_SQL: [SimpleNamespace(on_error=on_error)],
            PAUSE_WORKFLOW_ON_ERROR_SQL: [SimpleNamespace(id='wf-parent')],
            GET_WORKFLOW_STATUS_SQL: [SimpleNamespace(status='RUNNING')],
        })

    @staticmethod
    async def _complete_child(recorder: _Session) -> tuple[AsyncMock, AsyncMock]:
        with patch(
            'horsies.core.workflows.engine._process_dependents',
            new=AsyncMock(),
        ) as dependents, patch(
            'horsies.core.workflows.engine.check_workflow_completion',
            new=AsyncMock(),
        ) as completion, patch(
            'horsies.core.workflows.engine.cascade_pause_to_children',
            new=AsyncMock(),
        ):
            await on_subworkflow_complete(recorder.session, 'child-1')
        return dependents, completion

    @staticmethod
    def _workflow_error(recorder: _Session) -> TaskError:
        stored = recorder.params_of(SET_WORKFLOW_ERROR_SQL)['error']
        return decode_task_error(json.loads(stored))

    @pytest.mark.asyncio
    async def test_ok_output_makes_a_completed_node(self) -> None:
        recorder = self._session(OK_RESULT_TEXT)

        dependents, completion = await self._complete_child(recorder)

        params = recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)
        assert params['status'] == 'COMPLETED'
        assert params['result'] == OK_RESULT_TEXT
        assert not recorder.executed(SET_WORKFLOW_ERROR_SQL)
        dependents.assert_awaited_once()
        completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_outputless_child_makes_a_completed_node(self) -> None:
        recorder = self._session(OUTPUTLESS_RESULT_TEXT)

        await self._complete_child(recorder)

        params = recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)
        assert params['status'] == 'COMPLETED'
        assert decode_task_result(json.loads(params['result']), type(None)).is_ok()
        assert not recorder.executed(SET_WORKFLOW_ERROR_SQL)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('child', ['skipped-output', 'failed-output'])
    async def test_err_output_makes_a_failed_node_with_the_child_text(
        self, child: str,
    ) -> None:
        child_result, expected_code = (
            (await _skipped_output_text(), OutcomeCode.WORKFLOW_OUTPUT_SKIPPED)
            if child == 'skipped-output'
            else (ERR_RESULT_TEXT, 'LABEL_REFUSED')
        )
        recorder = self._session(child_result)

        dependents, completion = await self._complete_child(recorder)

        params = recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)
        assert params['status'] == 'FAILED'
        assert params['result'] == child_result
        assert self._workflow_error(recorder).error_code == expected_code
        dependents.assert_awaited_once()
        completion.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failure_policy_uses_the_stored_node_error(self) -> None:
        """The handler selects the first failed node: its text is the child's."""
        recorder = self._session(ERR_RESULT_TEXT)
        recorder.rows[GET_FIRST_FAILED_TASK_RESULT_SQL] = [
            SimpleNamespace(task_name='child', result=ERR_RESULT_TEXT),
        ]

        await self._complete_child(recorder)

        assert self._workflow_error(recorder).error_code == 'LABEL_REFUSED'

    @pytest.mark.asyncio
    async def test_pause_policy_pauses_the_parent(self) -> None:
        recorder = self._session(ERR_RESULT_TEXT, on_error='pause')

        dependents, completion = await self._complete_child(recorder)

        stored = recorder.params_of(PAUSE_WORKFLOW_ON_ERROR_SQL)['error']
        assert decode_task_error(json.loads(stored)).error_code == 'LABEL_REFUSED'
        dependents.assert_not_awaited()
        completion.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'stored', UNREADABLE_TEXTS.values(), ids=UNREADABLE_TEXTS.keys(),
    )
    async def test_text_that_does_not_parse_makes_a_failed_node(
        self, stored: str,
    ) -> None:
        recorder = self._session(stored)

        await self._complete_child(recorder)

        params = recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)
        assert params['status'] == 'FAILED'
        assert params['result'] == stored
        err = self._workflow_error(recorder)
        assert err.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert 'child-1' in (err.message or '')
        assert err.data == {'workflow_id': 'child-1'}

    @pytest.mark.asyncio
    @pytest.mark.parametrize('stored', [None, ''], ids=['null', 'empty'])
    async def test_no_text_makes_a_failed_node_with_an_envelope(
        self, stored: str | None,
    ) -> None:
        """A node without a result gives its readers no entry."""
        recorder = self._session(stored)

        await self._complete_child(recorder)

        params = recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)
        assert params['status'] == 'FAILED'
        err = _stored_err(params['result'])
        assert err.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert err.data == {'workflow_id': 'child-1'}
        assert self._workflow_error(recorder).message == err.message

    def test_cause_with_a_lone_surrogate_is_escaped(self) -> None:
        """The failure handler encodes this error and raises for a surrogate."""
        err = _unreadable_child_error('child-1', f'bad {chr(0xdc80)} text')

        assert '\\udc80' in (err.message or '')
        assert encode_task_error(err)

    @pytest.mark.asyncio
    async def test_summary_of_a_failed_node_has_the_child_status(self) -> None:
        recorder = self._session(await _skipped_output_text())

        await self._complete_child(recorder)

        summary = SubWorkflowSummary.from_json(
            json.loads(recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)['summary']),
        )
        assert summary.status is WorkflowStatus.COMPLETED
        assert summary.output is None
        assert summary.skipped_tasks == 1

    @pytest.mark.asyncio
    async def test_summary_of_an_ok_output_holds_the_value(self) -> None:
        recorder = self._session(OK_RESULT_TEXT)

        await self._complete_child(recorder)

        summary = SubWorkflowSummary.from_json(
            json.loads(recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)['summary']),
        )
        assert summary.output == 7

    @pytest.mark.asyncio
    async def test_two_levels_keep_the_code_when_a_case_lets_the_level_complete(
        self,
    ) -> None:
        """Each level's output node is the FAILED node of the level below."""
        stored = await _skipped_output_text()

        for _ in range(2):
            recorder = self._session(stored)
            await self._complete_child(recorder)
            node_result = recorder.params_of(UPDATE_PARENT_NODE_RESULT_SQL)['result']
            stored = await _final_result(_output_row('FAILED', node_result))

        err = _stored_err(stored)
        assert err.error_code is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        # The text names the workflow that holds the skipped node.
        assert (err.data or {})['workflow_id'] == WORKFLOW_ID


class TestOutcomeCode:
    def test_code_is_registered_as_a_built_in_outcome(self) -> None:
        name = 'WORKFLOW_OUTPUT_SKIPPED'

        assert BUILTIN_CODE_REGISTRY[name] is OutcomeCode.WORKFLOW_OUTPUT_SKIPPED
        assert categorize_error_code(name) is ErrorCategory.OUTCOME
