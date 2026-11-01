"""A stored ``TaskError`` that does not parse, at the decode and at each reader.

``decode_task_error`` is the one decode of an err slot. Each test enters from
the reader that a caller uses, so a test fails when a reader stops folding the
refusal or drops its cause.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr, ValidationError

from horsies.core.app import Horsies
from horsies.core.brokers.result_types import (
    BrokerInvalidJsonPayload,
    RawResultRecord,
)
from horsies.core.codec.error_payload import FALLBACK_ERROR_JSON
from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import (
    Json,
    decode_task_error,
    decode_task_result,
    encode_task_error,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.lifecycle.classify import (
    ChildReport,
    FinalizeContext,
    FinalizeDecision,
    FinalizeNoOpReason,
    NoTerminalAction,
    ResultUndecodable,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    OperationalErrorCode,
    SubWorkflowError,
    TaskError,
    TaskInfo,
    TaskResult,
)
from horsies.core.models.workflow.context import SubWorkflowSummary
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import (
    _decode_per_task_envelope,  # pyright: ignore[reportPrivateUsage]
    _decode_stored_workflow_error,  # pyright: ignore[reportPrivateUsage]
    _decode_workflow_envelope,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import Ok, is_err
from horsies.core.types.status import TaskStatus
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from horsies.core.workflows.engine import (
    _decode_err_only,  # pyright: ignore[reportPrivateUsage]
    _decode_stored_task_result,  # pyright: ignore[reportPrivateUsage]
    get_workflow_failure_error,
)
from horsies.core.workflows.phase2_recovery import (
    _recovered_failure_result,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.workflows.sql import (
    GET_FIRST_FAILED_REQUIRED_TASK_SQL,
    GET_FIRST_FAILED_TASK_RESULT_SQL,
)

pytestmark = [pytest.mark.unit]

_DECLARED: dict[str, Json] = {'error_code': 'MY_CODE', 'message': 'm'}

_NOT_A_TASK_ERROR: list[tuple[dict[str, Json], str]] = [
    ({'nope': 1}, 'nope'),
    ({**_DECLARED, 'nope': 1}, 'nope'),
]
"""An err slot that does not parse, with a word of the cause that it gives."""

_NOT_A_TASK_ERROR_IDS = [
    'only-an-unknown-key',
    'an-unknown-key-beside-valid-fields',
]

_SUMMARY: dict[str, Json] = {
    'status': 'FAILED',
    'output': None,
    'total_tasks': 1,
    'completed_tasks': 0,
    'failed_tasks': 1,
    'skipped_tasks': 0,
    'error_summary': None,
}

_REFUSED = pytest.mark.parametrize(
    ('err_slot', 'cause'), _NOT_A_TASK_ERROR, ids=_NOT_A_TASK_ERROR_IDS,
)


def _envelope(err_slot: Json) -> dict[str, Json]:
    return {'__h_task_result__': True, 'ok': None, 'err': err_slot}


def _stored(err_slot: Json) -> str:
    return dumps_json(_envelope(err_slot)).unwrap()


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql+psycopg://u:p@localhost/db'),
            ),
        ),
    )


def _make_worker() -> Worker:
    return Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(
            dsn='postgresql+psycopg://u:p@localhost/db',
            psycopg_dsn='postgresql://u:p@localhost/db',
            queues=['default'],
        ),
    )


def _assert_stored_data_error(result: TaskResult[Any, TaskError], cause: str) -> None:
    assert result.is_err()
    error = result.unwrap_err()
    assert error.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
    assert error.message is not None
    assert cause in error.message


class TestDecodeTaskError:
    """The decode of one err slot."""

    @_REFUSED
    def test_a_value_that_is_not_a_task_error_is_refused_with_the_cause(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        with pytest.raises(ValueValidationError) as refusal:
            decode_task_error(err_slot)
        assert cause in str(refusal.value)

    def test_the_declared_fields_decode(self) -> None:
        decoded = decode_task_error({
            'error_code': 'MY_CODE',
            'message': 'm',
            'data': {'k': 1},
            'exception': None,
        })
        assert type(decoded) is TaskError
        assert decoded.error_code == 'MY_CODE'
        assert decoded.message == 'm'
        assert decoded.data == {'k': 1}

    def test_the_built_in_discriminator_stays_valid_inside_error_code(self) -> None:
        decoded = decode_task_error({
            'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
            'message': 'm',
        })
        assert decoded.error_code is OperationalErrorCode.BROKER_ERROR

    def test_the_discriminator_key_is_refused_as_a_field_of_the_error(self) -> None:
        with pytest.raises(ValueValidationError, match='extra_forbidden'):
            decode_task_error({**_DECLARED, '__builtin_task_code__': 'BROKER_ERROR'})

    def test_the_fields_of_a_subworkflow_error_decode(self) -> None:
        decoded = decode_task_error({
            **_DECLARED,
            'sub_workflow_id': 'wf-child',
            'sub_workflow_summary': _SUMMARY,
        })
        assert isinstance(decoded, SubWorkflowError)
        assert decoded.sub_workflow_id == 'wf-child'
        assert decoded.sub_workflow_summary.failed_tasks == 1

    def test_a_subworkflow_error_refuses_an_unknown_key(self) -> None:
        with pytest.raises(ValueValidationError, match='nope'):
            decode_task_error({
                **_DECLARED,
                'sub_workflow_id': 'wf-child',
                'sub_workflow_summary': _SUMMARY,
                'nope': 1,
            })

    def test_a_subworkflow_field_without_the_id_is_refused(self) -> None:
        with pytest.raises(ValueValidationError, match='sub_workflow_summary'):
            decode_task_error({**_DECLARED, 'sub_workflow_summary': _SUMMARY})

    def test_the_engine_dump_of_a_subworkflow_error_decodes(self) -> None:
        """The engine stores this dump in a parent node, not ``encode_task_error``."""
        error = SubWorkflowError(
            error_code='MY_CODE',
            message='m',
            sub_workflow_id='wf-child',
            sub_workflow_summary=SubWorkflowSummary(
                status=WorkflowStatus.FAILED,
                output=None,
                total_tasks=1,
                completed_tasks=0,
                failed_tasks=1,
                skipped_tasks=0,
                error_summary=None,
            ),
        )
        decoded = decode_task_error(cast(Json, error.model_dump(mode='json')))
        assert isinstance(decoded, SubWorkflowError)

    @pytest.mark.parametrize(
        'error',
        [
            TaskError(error_code='MY_CODE', message='m'),
            TaskError(
                error_code=OperationalErrorCode.BROKER_ERROR,
                message='m',
                data={'k': 1},
            ),
        ],
        ids=['user-code', 'built-in-code'],
    )
    def test_each_key_that_the_encoder_writes_decodes(self, error: TaskError) -> None:
        encoded = encode_task_error(error)
        assert isinstance(encoded, dict)
        assert set(encoded) == set(TaskError.model_fields)
        assert decode_task_error(encoded) == error

    def test_a_key_with_a_lone_surrogate_is_refused_without_the_key(self) -> None:
        key = f'key{chr(0xd800)}'
        with pytest.raises((ValueValidationError, StrictJsonError)) as refusal:
            decode_task_error({**_DECLARED, key: 1})
        assert key not in str(refusal.value)
        str(refusal.value).encode('utf-8')

    def test_the_constructor_refuses_an_unknown_key(self) -> None:
        with pytest.raises(ValidationError, match='extra_forbidden'):
            TaskError.model_validate({**_DECLARED, 'nope': 1})

    def test_the_fallback_error_json_decodes(self) -> None:
        decoded = decode_task_result(
            loads_json(FALLBACK_ERROR_JSON).unwrap(), type(None),
        )
        assert decoded.is_err()
        error = decoded.unwrap_err()
        assert error.error_code is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        assert error.message == 'secondary serialization failure'


class TestClientReaders:
    """The readers that return the stored error to a caller."""

    @_REFUSED
    @pytest.mark.asyncio
    async def test_task_handle_get_gives_the_stored_data_error(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=Ok(
                RawResultRecord(
                    task_id='task-1',
                    task_name='t',
                    status=TaskStatus.FAILED,
                    raw_result=_envelope(err_slot),
                ),
            ),
        )
        app = MagicMock()
        app.get_broker = MagicMock(return_value=broker)
        handle: TaskHandle[int] = TaskHandle(
            task_id='task-1', app=app, broker_mode=True, ok_type=int,
        )

        _assert_stored_data_error(await handle.get_async(), cause)

    @_REFUSED
    @pytest.mark.asyncio
    async def test_app_get_result_gives_the_stored_data_variant(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=Ok(
                RawResultRecord(
                    task_id='task-1',
                    task_name='t',
                    status=TaskStatus.FAILED,
                    raw_result=_envelope(err_slot),
                ),
            ),
        )
        app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]

        result = await app.get_result_async('task-1')

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidJsonPayload)
        assert cause in result.err_value.message

    @_REFUSED
    @pytest.mark.asyncio
    async def test_app_get_task_info_gives_the_stored_data_variant(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        app = _make_app()
        broker = MagicMock()
        info = MagicMock(spec=TaskInfo)
        info.task_name = 't'
        info.raw_result = _envelope(err_slot)
        broker.get_task_info_async = AsyncMock(return_value=Ok(info))
        app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]

        result = await app.get_task_info_async('task-1', include_result=True)

        assert is_err(result)
        assert isinstance(result.err_value, BrokerInvalidJsonPayload)
        assert cause in result.err_value.message

    @_REFUSED
    def test_a_stored_workflow_error_gives_the_stored_data_error(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        result = _decode_stored_workflow_error(
            'wf-1', WorkflowStatus.FAILED, dumps_json(err_slot).unwrap(),
        )
        _assert_stored_data_error(result, cause)

    @_REFUSED
    def test_a_stored_workflow_envelope_gives_the_stored_data_error(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        result = _decode_workflow_envelope(
            _envelope(err_slot), int, app=None, workflow_id='wf-1',
        )
        _assert_stored_data_error(result, cause)

    @_REFUSED
    def test_a_per_node_envelope_gives_the_stored_data_error(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        app = MagicMock()
        app.tasks = {'t': SimpleNamespace(task_ok_type=int)}
        result = _decode_per_task_envelope(
            _envelope(err_slot), app=app, task_name='t', node_id='n1',
        )
        _assert_stored_data_error(result, cause)


class TestEngineReaders:
    """The readers that the workflow engine uses for a stored task row."""

    @_REFUSED
    def test_a_dependency_result_gives_the_stored_data_error(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        app = cast(
            Horsies,
            SimpleNamespace(tasks={'t': SimpleNamespace(task_ok_type=int)}),
        )
        result = _decode_stored_task_result(
            _stored(err_slot), app=app, task_name='t', context={},
        )
        _assert_stored_data_error(result, cause)

    @_REFUSED
    def test_decode_err_only_gives_the_stored_data_error(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        result = _decode_err_only(_stored(err_slot))
        assert result is not None
        _assert_stored_data_error(result, cause)

    def test_decode_err_only_escapes_a_lone_surrogate_in_the_cause(self) -> None:
        """The caller encodes this error, and the encode refuses a surrogate."""
        refusal = ValueValidationError(f'bad {chr(0xd800)}')
        with patch(
            'horsies.core.workflows.engine.decode_task_error',
            side_effect=refusal,
        ):
            result = _decode_err_only(_stored(_DECLARED))

        assert result is not None
        encode_task_error(result.unwrap_err())

    @_REFUSED
    @pytest.mark.asyncio
    async def test_the_workflow_error_of_a_failed_task_names_the_cause(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=MagicMock(
                fetchone=MagicMock(
                    return_value=SimpleNamespace(result=_stored(err_slot)),
                ),
            ),
        )

        stored_error = await get_workflow_failure_error(session, 'wf-1', None)

        assert session.execute.await_args.args[0] is GET_FIRST_FAILED_TASK_RESULT_SQL
        assert stored_error is not None
        decoded = decode_task_error(loads_json(stored_error).unwrap())
        assert decoded.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert cause in (decoded.message or '')

    @_REFUSED
    @pytest.mark.asyncio
    async def test_the_workflow_error_of_a_required_task_names_the_cause(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        session = AsyncMock()
        session.execute = AsyncMock(
            return_value=MagicMock(
                fetchone=MagicMock(
                    return_value=SimpleNamespace(result=_stored(err_slot)),
                ),
            ),
        )

        stored_error = await get_workflow_failure_error(
            session, 'wf-1', {'cases': [{'required_indices': [0]}]},
        )

        assert session.execute.await_args.args[0] is GET_FIRST_FAILED_REQUIRED_TASK_SQL
        assert stored_error is not None
        decoded = decode_task_error(loads_json(stored_error).unwrap())
        assert decoded.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert cause in (decoded.message or '')


class TestRecoveryReaders:
    """The readers of a recovery path: a stored value must not raise out."""

    @_REFUSED
    def test_phase2_recovery_gives_the_stored_data_error(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        result = _recovered_failure_result('FAILED', _stored(err_slot))
        _assert_stored_data_error(result, cause)
        assert result.unwrap_err().data == {'task_status': 'FAILED'}

    @_REFUSED
    def test_the_recovered_error_encodes_for_the_failure_handler(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        result = _recovered_failure_result('FAILED', _stored(err_slot))
        encoded = encode_task_error(result.unwrap_err())
        assert isinstance(encoded, dict)
        assert cause in str(encoded['message'])

    def test_phase2_recovery_escapes_a_lone_surrogate_in_the_cause(self) -> None:
        """An encode refusal in the failure handler repeats on each pass."""
        refusal = ValueValidationError(f'bad {chr(0xd800)}')
        with patch(
            'horsies.core.workflows.phase2_recovery.decode_task_error',
            side_effect=refusal,
        ):
            result = _recovered_failure_result('FAILED', _stored(_DECLARED))

        encode_task_error(result.unwrap_err())

    @_REFUSED
    @pytest.mark.asyncio
    async def test_finalize_records_a_refused_result_as_undecodable(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        reports: list[ChildReport] = []

        def _classify(report: ChildReport, _: FinalizeContext) -> FinalizeDecision:
            reports.append(report)
            return NoTerminalAction(reason=FinalizeNoOpReason.CLAIM_LOST)

        worker = _make_worker()
        with patch('horsies.core.worker.finalize.classify', side_effect=_classify):
            outcome = await worker._persist_task_terminal_state(  # pyright: ignore[reportPrivateUsage]
                task_id='task-1',
                now=datetime.now(timezone.utc),
                ok=True,
                result_json_str=_stored(err_slot),
                failed_reason=None,
                task_name='t',
                queue_name='default',
                is_workflow_task=False,
            )

        assert outcome == Ok(None)
        match reports:
            case [ResultUndecodable(detail=detail)]:
                assert cause in detail
            case _:
                pytest.fail(f'unexpected reports: {reports!r}')

    @_REFUSED
    @pytest.mark.asyncio
    async def test_phase2_replay_ends_without_a_retry(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        worker = _make_worker()
        with patch.object(
            worker,
            '_read_persisted_task_row',
            AsyncMock(
                return_value=SimpleNamespace(
                    status='FAILED',
                    task_name='t',
                    queue_name='default',
                    is_workflow_task=True,
                    result=_stored(err_slot),
                ),
            ),
        ):
            outcome = await worker._read_persisted_task_result(  # pyright: ignore[reportPrivateUsage]
                'task-1', MagicMock(),
            )

        assert is_err(outcome)
        assert outcome.err_value.retryable is False
        assert cause in outcome.err_value.message
