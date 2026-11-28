"""A stored ``TaskError`` that does not parse, at the decode and at each reader.

``decode_task_error`` is the one decode of an err slot. Each test enters from
the reader that a caller uses, so a test fails when a reader stops folding the
refusal or drops its cause.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr, ValidationError

from horsies.core.app import Horsies
from horsies.core.brokers.result_types import (
    DatabaseInvalidJsonPayload,
    StoredResult,
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
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    OperationalCode,
    OutcomeCode,
    ResultSlotStored,
    SubWorkflowFailure,
    TaskError,
    TaskInfo,
)
from horsies.core.models.outcome import (
    Failed,
    ReadResult,
    ReadResultDeserializationError,
    Succeeded,
    TaskOutcome,
)
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import (
    _decode_per_task_envelope,  # pyright: ignore[reportPrivateUsage]
    _decode_stored_workflow_error,  # pyright: ignore[reportPrivateUsage]
    _decode_workflow_envelope,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.types.status import TaskStatus

pytestmark = [pytest.mark.unit]


_DECLARED: dict[str, Json] = {'error_code': 'MY_CODE', 'message': 'm'}

_NOT_A_TASK_ERROR: list[tuple[dict[str, Json], str]] = [
    ({'nope': 1}, 'nope'),
    ({**_DECLARED, 'nope': 1}, 'nope'),
    ({}, 'code: Field required'),
    ({'message': 'm'}, 'code: Field required'),
    (
        {'error_code': None, 'message': 'm', 'data': None, 'exception': None},
        'code must not be null',
    ),
]
"""An err slot that does not parse, with a word of the cause that it gives."""

_STORED_KEYS = ('exception', 'error_code', 'data', 'message')
"""The keys of a stored plain task error, in the order of the core's writer."""

_NOT_A_TASK_ERROR_IDS = [
    'only-an-unknown-key',
    'an-unknown-key-beside-valid-fields',
    'empty',
    'no-code',
    'older-row-with-a-null-code',
]

_SUMMARY: dict[str, Json] = {
    'status': 'FAILED',
    'output': None,
    'total_nodes': 1,
    'completed_nodes': 0,
    'failed_nodes': 1,
    'skipped_nodes': 0,
    'error_summary': None,
}

_REFUSED = pytest.mark.parametrize(
    ('err_slot', 'cause'), _NOT_A_TASK_ERROR, ids=_NOT_A_TASK_ERROR_IDS,
)

_NO_MESSAGE = pytest.mark.parametrize(
    'err_slot',
    [
        {
            'error_code': 'RATE_LIMITED',
            'message': None,
            'data': None,
            'exception': None,
        },
        {'error_code': 'RATE_LIMITED', 'message': None},
        {'error_code': 'RATE_LIMITED'},
    ],
    ids=['row-of-a-code-only-error', 'null-message', 'no-message-key'],
)
"""An err slot with a code and no message: an error, not stored junk."""

"""Stored text that fails before the err slot, with a word of the cause."""


def _envelope(err_slot: Json) -> dict[str, Json]:
    return {'__h_task_result__': True, 'ok': None, 'err': err_slot}


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            database=PostgresConfig(
                database_url=SecretStr('postgresql://u:p@localhost/db'),
            ),
        ),
    )


def _assert_stored_data_error(read: ReadResult[Any, TaskError], cause: str) -> None:
    match read:
        case Err(err_value=ReadResultDeserializationError(message=message)):
            assert cause in message
        case _:
            pytest.fail(f'expected ReadResultDeserializationError, got {read!r}')


def _failed_task_error(read: Result[TaskOutcome[Any, TaskError], object]) -> TaskError:
    """The task error of a failed outcome; a test failure for any other read."""
    match read:
        case Ok(ok_value=Failed(error=error)):
            return error
        case _:
            pytest.fail(f'expected a failed outcome, got {read!r}')


def _assert_code_without_message(error: TaskError) -> None:
    assert error.code == 'RATE_LIMITED'
    assert error.message is None


class TestRequiredFields:
    """The code is required, and the other three fields are not.

    The stored key of the code is ``error_code``, and the field is ``code``.
    """

    def test_a_null_code_gives_one_cause(self) -> None:
        with pytest.raises(ValueValidationError) as refusal:
            decode_task_error({'error_code': None, 'message': 'm'})
        assert 'code must not be null' in str(refusal.value)
        assert 'UNHANDLED_EXCEPTION' not in str(refusal.value)

    def test_an_empty_value_names_the_code_only(self) -> None:
        with pytest.raises(ValueValidationError) as refusal:
            decode_task_error({})
        assert 'code: Field required' in str(refusal.value)
        assert 'message' not in str(refusal.value)

    def test_a_bad_code_shape_does_not_offer_null(self) -> None:
        with pytest.raises(StrictJsonError) as refusal:
            decode_task_error({'error_code': {}, 'message': 'm'})
        assert 'expected a plain string' in str(refusal.value)
        assert 'None' not in str(refusal.value)

    def test_data_and_exception_stay_optional(self) -> None:
        decoded = decode_task_error(_DECLARED)
        assert decoded.data is None
        assert decoded.exception is None

    def test_a_custom_code_stays_a_free_string(self) -> None:
        decoded = decode_task_error({'error_code': 'any text at all', 'message': 'm'})
        assert decoded.code == 'any text at all'
        assert type(decoded.code) is str

    def test_the_constructor_requires_a_code(self) -> None:
        with pytest.raises(ValidationError, match='code'):
            TaskError(message='m')  # pyright: ignore[reportCallIssue]

    def test_a_code_only_error_builds(self) -> None:
        error = TaskError(code='MY_CODE')
        assert error.code == 'MY_CODE'
        assert error.message is None

    def test_a_code_only_error_writes_a_null_message(self) -> None:
        encoded = encode_task_error(TaskError(code='MY_CODE'))
        assert isinstance(encoded, dict)
        assert encoded['message'] is None

    def test_a_task_that_builds_no_code_ends_unhandled_with_the_cause(self) -> None:
        app = _make_app()

        @app.task('builds_no_code')
        def builds_no_code() -> Result[int, TaskError]:
            return Err(
                TaskError(message='m'),  # pyright: ignore[reportCallIssue]
            )

        error = builds_no_code().unwrap_err()

        assert error.code is OperationalCode.UNHANDLED_EXCEPTION
        assert error.message is not None
        assert 'code: Field required' in error.message

    def test_a_task_that_builds_no_message_keeps_its_code(self) -> None:
        app = _make_app()

        @app.task('builds_no_message')
        def builds_no_message() -> Result[int, TaskError]:
            return Err(TaskError(code='RATE_LIMITED'))

        _assert_code_without_message(builds_no_message().unwrap_err())


class TestErrorWithNoMessage:
    """A code with no message is an error: each reader returns it as stored.

    ``message`` is for a human, and no library decision branches on it. Earlier
    versions stored this form, and the docs taught it.
    """

    @_NO_MESSAGE
    def test_the_decode_keeps_the_code(self, err_slot: dict[str, Json]) -> None:
        decoded = decode_task_error(err_slot)
        assert decoded.code == 'RATE_LIMITED'
        assert decoded.message is None

    @_NO_MESSAGE
    @pytest.mark.asyncio
    async def test_task_handle_get_returns_the_error(
        self, err_slot: dict[str, Json],
    ) -> None:
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=Ok(
                StoredResult(
                    task_id='task-1',
                    task_name='t',
                    status=TaskStatus.FAILED,
                    envelope=_envelope(err_slot),
                ),
            ),
        )
        app = MagicMock()
        app.get_broker = MagicMock(return_value=broker)
        handle: TaskHandle[int] = TaskHandle(
            task_id='task-1', app=app, broker_mode=True, ok_type=int,
        )

        _assert_code_without_message(_failed_task_error(await handle.get_async()))

    @_NO_MESSAGE
    @pytest.mark.asyncio
    async def test_app_get_result_returns_the_error(
        self, err_slot: dict[str, Json],
    ) -> None:
        app = _make_app()
        broker = MagicMock()
        broker.get_raw_result_record_async = AsyncMock(
            return_value=Ok(
                StoredResult(
                    task_id='task-1',
                    task_name='t',
                    status=TaskStatus.FAILED,
                    envelope=_envelope(err_slot),
                ),
            ),
        )
        app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]

        result = await app.get_result_async('task-1')

        _assert_code_without_message(_failed_task_error(result))

    @_NO_MESSAGE
    def test_a_stored_workflow_error_returns_the_error(
        self, err_slot: dict[str, Json],
    ) -> None:
        _assert_code_without_message(_failed_task_error(
            _decode_stored_workflow_error(
                'wf-1', WorkflowStatus.FAILED, dumps_json(err_slot).unwrap(),
            ),
        ))

    @_NO_MESSAGE
    def test_a_stored_workflow_envelope_returns_the_error(
        self, err_slot: dict[str, Json],
    ) -> None:
        _assert_code_without_message(_failed_task_error(
            _decode_workflow_envelope(
                _envelope(err_slot), int, app=None, workflow_id='wf-1',
            ),
        ))

    @_NO_MESSAGE
    def test_a_per_node_envelope_returns_the_error(
        self, err_slot: dict[str, Json],
    ) -> None:
        app = MagicMock()
        app.tasks = {'t': SimpleNamespace(task_ok_type=int)}
        _assert_code_without_message(_failed_task_error(
            _decode_per_task_envelope(
                _envelope(err_slot), app=app, task_name='t', node_id='n1',
            ),
        ))


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
        assert decoded.code == 'MY_CODE'
        assert decoded.message == 'm'
        assert decoded.data == {'k': 1}

    def test_the_built_in_discriminator_stays_valid_inside_error_code(self) -> None:
        decoded = decode_task_error({
            'error_code': {'__builtin_task_code__': 'DATABASE_ERROR'},
            'message': 'm',
        })
        assert decoded.code is OperationalCode.DATABASE_ERROR

    def test_the_discriminator_key_is_refused_as_a_field_of_the_error(self) -> None:
        with pytest.raises(ValueValidationError, match='extra_forbidden'):
            decode_task_error({**_DECLARED, '__builtin_task_code__': 'DATABASE_ERROR'})

    def test_the_fields_of_a_subworkflow_error_decode(self) -> None:
        decoded = decode_task_error({
            **_DECLARED,
            'sub_workflow_id': 'wf-child',
            'sub_workflow_summary': _SUMMARY,
        })
        assert isinstance(decoded, SubWorkflowFailure)
        assert decoded.sub_workflow_id == 'wf-child'
        assert decoded.sub_workflow_summary.failed_nodes == 1

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

    def test_the_engine_form_of_a_subworkflow_error_decodes(self) -> None:
        """The core engine stores a sub-workflow error in a parent node with
        the key ``error_code`` and the two sub-workflow fields after the four
        keys of every error."""
        decoded = decode_task_error({
            'exception': None,
            'error_code': 'MY_CODE',
            'data': None,
            'message': 'm',
            'sub_workflow_id': 'wf-child',
            'sub_workflow_summary': _SUMMARY,
        })
        assert isinstance(decoded, SubWorkflowFailure)
        assert decoded.code == 'MY_CODE'

    @pytest.mark.parametrize(
        'error',
        [
            TaskError(code='MY_CODE', message='m'),
            TaskError(
                code=OperationalCode.DATABASE_ERROR,
                message='m',
                data={'k': 1},
            ),
        ],
        ids=['custom-code', 'built-in-code'],
    )
    def test_each_key_that_the_encoder_writes_decodes(self, error: TaskError) -> None:
        encoded = encode_task_error(error)
        assert isinstance(encoded, dict)
        assert list(encoded) == list(_STORED_KEYS)
        assert decode_task_error(encoded) == error

    def test_a_subclass_with_its_own_field_is_refused(self) -> None:
        class WithOwnField(TaskError):
            retry_hint: int

        encoded = encode_task_error(
            WithOwnField(code='MY_CODE', message='m', retry_hint=1),
        )

        with pytest.raises(ValueValidationError, match='retry_hint'):
            decode_task_error(encoded)

    def test_a_key_with_a_lone_surrogate_is_refused_without_the_key(self) -> None:
        key = f'key{chr(0xd800)}'
        with pytest.raises((ValueValidationError, StrictJsonError)) as refusal:
            decode_task_error({**_DECLARED, key: 1})
        assert key not in str(refusal.value)
        str(refusal.value).encode('utf-8')

    def test_the_constructor_refuses_an_unknown_key(self) -> None:
        with pytest.raises(ValidationError, match='extra_forbidden'):
            TaskError.model_validate({'code': 'MY_CODE', 'message': 'm', 'nope': 1})

    def test_the_fallback_error_json_decodes(self) -> None:
        decoded = decode_task_result(
            loads_json(FALLBACK_ERROR_JSON).unwrap(), type(None),
        )
        match decoded:
            case Failed(error=error):
                assert error.code is OperationalCode.WORKER_SERIALIZATION_ERROR
                assert error.message == 'secondary serialization failure'
            case Succeeded():
                pytest.fail(f'expected a failed outcome, got {decoded!r}')


class TestStoredCodeKey:
    """The field ``code`` is stored under the key ``error_code``, the field
    name of the core's ``TaskError``."""

    def test_the_encode_writes_error_code_at_the_place_of_the_field(self) -> None:
        encoded = encode_task_error(
            TaskError(code=OperationalCode.DATABASE_ERROR, message='m'),
        )
        assert isinstance(encoded, dict)
        assert list(encoded) == list(_STORED_KEYS)
        assert encoded['error_code'] == {'__builtin_task_code__': 'DATABASE_ERROR'}
        assert 'code' not in encoded

    def test_the_encode_of_a_subworkflow_error_keeps_the_key_order(self) -> None:
        stored: dict[str, Json] = {
            'exception': None,
            'error_code': 'MY_CODE',
            'data': None,
            'message': 'm',
            'sub_workflow_id': 'wf-child',
            'sub_workflow_summary': _SUMMARY,
        }
        encoded = encode_task_error(decode_task_error(stored))
        assert isinstance(encoded, dict)
        assert list(encoded) == list(stored)

    def test_the_decode_reads_error_code_as_the_code(self) -> None:
        decoded = decode_task_error({
            'exception': None,
            'error_code': {'__builtin_task_code__': 'TASK_TIMEOUT'},
            'data': None,
            'message': 'm',
        })
        assert decoded.code is OutcomeCode.TASK_TIMEOUT

    @pytest.mark.parametrize(
        'err_slot',
        [
            {'code': 'MY_CODE', 'message': 'm'},
            {'error_code': 'MY_CODE', 'code': 'MY_CODE', 'message': 'm'},
        ],
        ids=['only-the-field-name', 'field-name-beside-the-stored-key'],
    )
    def test_a_stored_key_code_is_refused(self, err_slot: dict[str, Json]) -> None:
        with pytest.raises(ValueValidationError) as refusal:
            decode_task_error(err_slot)
        assert "unexpected key 'code'" in str(refusal.value)
        assert "'error_code'" in str(refusal.value)

    def test_the_constructor_refuses_the_stored_key(self) -> None:
        with pytest.raises(ValidationError, match='extra_forbidden'):
            TaskError(error_code='MY_CODE')  # pyright: ignore[reportCallIssue]


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
                StoredResult(
                    task_id='task-1',
                    task_name='t',
                    status=TaskStatus.FAILED,
                    envelope=_envelope(err_slot),
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
                StoredResult(
                    task_id='task-1',
                    task_name='t',
                    status=TaskStatus.FAILED,
                    envelope=_envelope(err_slot),
                ),
            ),
        )
        app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]

        result = await app.get_result_async('task-1')

        _assert_stored_data_error(result, cause)

    @_REFUSED
    @pytest.mark.asyncio
    async def test_app_get_task_info_gives_the_stored_data_variant(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        app = _make_app()
        broker = MagicMock()
        info = MagicMock(spec=TaskInfo)
        info.task_name = 't'
        info.result = ResultSlotStored(_envelope(err_slot))
        broker.get_task_info_async = AsyncMock(return_value=Ok(info))
        app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]

        result = await app.get_task_info_async('task-1', include_result=True)

        assert is_err(result)
        assert isinstance(result.err_value, DatabaseInvalidJsonPayload)
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
        match result:
            case Err(err_value=unreadable):
                assert cause in unreadable.message
                assert (
                    unreadable.as_outcome().error.code
                    is OperationalCode.RESULT_DESERIALIZATION_ERROR
                )
            case Ok():
                pytest.fail(f'expected an unreadable node, got {result!r}')


class TestUnregisteredSource:
    """A node of a source that this process does not register.

    An err-only envelope needs no ok type, so it reads as ``Failed`` with the
    stored code and message. Only an ok slot needs the registered type.
    """

    @staticmethod
    def _app_without_the_task() -> MagicMock:
        app = MagicMock()
        app.tasks = {}
        return app

    def test_an_err_only_envelope_gives_the_stored_error(self) -> None:
        result = _decode_per_task_envelope(
            _envelope({'error_code': 'MY_CODE', 'message': 'broke'}),
            app=self._app_without_the_task(),
            task_name='gone',
            node_id='n1',
        )

        error = _failed_task_error(result)
        assert error.code == 'MY_CODE'
        assert error.message == 'broke'

    def test_an_ok_envelope_is_unreadable_and_names_the_source(self) -> None:
        result = _decode_per_task_envelope(
            {'__h_task_result__': True, 'ok': 5, 'err': None},
            app=self._app_without_the_task(),
            task_name='gone',
            node_id='n1',
        )

        match result:
            case Err(err_value=unreadable):
                assert 'not registered' in unreadable.message
            case Ok():
                pytest.fail(f'expected an unreadable node, got {result!r}')

    @_REFUSED
    def test_an_err_slot_that_does_not_decode_is_unreadable(
        self, err_slot: dict[str, Json], cause: str,
    ) -> None:
        result = _decode_per_task_envelope(
            _envelope(err_slot),
            app=self._app_without_the_task(),
            task_name='gone',
            node_id='n1',
        )

        match result:
            case Err(err_value=unreadable):
                assert cause in unreadable.message
            case Ok():
                pytest.fail(f'expected an unreadable node, got {result!r}')

    def test_an_envelope_with_no_marker_is_unreadable(self) -> None:
        result = _decode_per_task_envelope(
            {'ok': None, 'err': {'error_code': 'MY_CODE'}},
            app=self._app_without_the_task(),
            task_name='gone',
            node_id='n1',
        )

        match result:
            case Err(err_value=unreadable):
                assert 'envelope invalid' in unreadable.message
            case Ok():
                pytest.fail(f'expected an unreadable node, got {result!r}')


class TestStoredWorkflowStoppedCode:
    """``WORKFLOW_STOPPED`` is a reserved name and no task code: a stored err
    slot with it does not decode, so a finalize fails the task instead of
    reading the error as a stop and leaving the row RUNNING."""


    def test_the_err_slot_does_not_decode(self) -> None:
        err_slot: Json = {
            'exception': None,
            'error_code': 'WORKFLOW_STOPPED',
            'data': None,
            'message': 'stop',
        }
        with pytest.raises(ValueValidationError, match='reserved error code name'):
            decode_task_error(err_slot)
