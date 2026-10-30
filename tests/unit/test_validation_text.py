"""Validation failure text holds no input values.

Each test puts a marker string into the refused value and checks that the
marker is not in the text that the library writes: error messages, stored
``TaskError`` fields, flattened exceptions and logs.
"""

from __future__ import annotations

import json
import logging
import string
import traceback
import uuid
from datetime import datetime
from types import SimpleNamespace
from typing import Annotated, Any, Literal, TypedDict, Union, cast
from unittest.mock import MagicMock

import pytest
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    SecretStr,
    ValidationError,
    field_serializer,
)
from pydantic_core import PydanticCustomError
from pydantic_core._pydantic_core import list_all_errors  # pyright: ignore[reportPrivateImportUsage]

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import (
    PostgresBroker,
    _broker_err,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.brokers.result_types import BrokerErrorCode
from horsies.core.codec.error_payload import MAX_EXCEPTION_CHAIN_LINKS, flatten_exception
from horsies.core.codec.json_io import SerializationError
from horsies.core.codec.kwargs import encode_subworkflow_kwargs
from horsies.core.codec.typed import decode_value, encode_task_result, encode_value
from horsies.core.codec.validation_errors import (
    INPUT_PLACEHOLDER,
    MAX_VALIDATION_ERRORS_IN_TEXT,
    SCHEMA_CONTEXT_KEYS,
    ValueValidationError,
    exception_text,
    validation_error_entries,
    validation_summary,
    without_validation_input,
)
from horsies.core.errors import ErrorCode, TaskDefinitionError
from horsies.core.logging import get_logger
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendValidationFailed
from horsies.core.models.tasks import ContractCode, TaskError, TaskResult
from horsies.core.models.workflow import WorkflowDefinition, WorkflowSpec
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.result import is_err
from horsies.core.worker.health import HealthMixin
from horsies.core.workflows.lifecycle import (
    _encode_node_kwargs_or_raise,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = pytest.mark.unit

MARKER = 'marker-5c1e9d'

# Placeholders whose values come from the input. A new pydantic placeholder
# fails ``test_every_builtin_placeholder_is_classified`` until it is put into
# ``SCHEMA_CONTEXT_KEYS`` or here.
INPUT_CONTEXT_KEYS = frozenset({'attribute', 'encoding_error', 'error', 'tag', 'tz_actual'})


def _refusal(adapter: TypeAdapter[Any], value: object) -> ValidationError:
    try:
        adapter.validate_python(value)
    except ValidationError as error:
        return error
    raise AssertionError('the value was accepted')


def _raised_from(make: Any) -> BaseException:
    try:
        make()
    except BaseException as exc:  # noqa: BLE001 - the test builds the chain
        return exc
    raise AssertionError('nothing was raised')


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid')
    a: int


class _Cat(BaseModel):
    kind: Literal['cat']
    lives: int


class _Dog(BaseModel):
    kind: Literal['dog']
    bark: int


_Pet = Annotated[Union[_Cat, _Dog], Field(discriminator='kind')]


class _Pair(TypedDict):
    a: int


class _Many(BaseModel):
    values: list[int]


def _refuse_with_marker(value: str) -> str:
    raise ValueError(f'bad value {value}')


def _refuse_with_custom_error(value: str) -> str:
    raise PydanticCustomError('my_code', 'refused {thing}', {'thing': value})


def _refuse_with_builtin_name(value: str) -> str:
    raise PydanticCustomError('greater_than', 'refused {thing}', {'thing': value})


class TestValidationSummary:
    def test_the_value_is_not_in_the_text(self) -> None:
        summary = validation_summary(_refusal(TypeAdapter(_Pair), {'a': MARKER}))

        assert MARKER not in summary
        assert summary == 'a: Input should be a valid integer, unable to parse string as an integer [int_parsing]'

    def test_a_root_entry_has_no_location(self) -> None:
        summary = validation_summary(_refusal(TypeAdapter(int), MARKER))

        assert summary.startswith('Input should be a valid integer')
        assert summary.endswith('[int_parsing]')

    def test_a_schema_template_keeps_its_text(self) -> None:
        adapter: TypeAdapter[Any] = TypeAdapter(Annotated[int, Field(ge=10)])

        summary = validation_summary(_refusal(adapter, 3))

        assert summary == 'Input should be greater than or equal to 10 [greater_than_equal]'

    def test_a_union_tag_from_the_input_is_replaced(self) -> None:
        summary = validation_summary(_refusal(TypeAdapter(_Pet), {'kind': MARKER}))

        assert MARKER not in summary
        assert f"Input tag '{INPUT_PLACEHOLDER}' found using 'kind'" in summary
        assert "'cat', 'dog'" in summary

    def test_an_input_character_in_a_parse_error_is_replaced(self) -> None:
        summary = validation_summary(_refusal(TypeAdapter(uuid.UUID), 'zz' + MARKER))

        assert summary == f'Input should be a valid UUID, {INPUT_PLACEHOLDER} [uuid_parsing]'

    def test_a_validator_message_is_kept_as_written(self) -> None:
        adapter: TypeAdapter[Any] = TypeAdapter(Annotated[str, AfterValidator(_refuse_with_marker)])

        summary = validation_summary(_refusal(adapter, MARKER))

        assert summary == f'Value error, bad value {MARKER} [value_error]'

    def test_a_custom_error_type_is_kept_as_written(self) -> None:
        adapter: TypeAdapter[Any] = TypeAdapter(Annotated[str, AfterValidator(_refuse_with_custom_error)])

        summary = validation_summary(_refusal(adapter, MARKER))

        assert summary == f'refused {MARKER} [my_code]'

    def test_a_builtin_type_without_its_context_is_withheld(self) -> None:
        adapter: TypeAdapter[Any] = TypeAdapter(Annotated[str, AfterValidator(_refuse_with_builtin_name)])

        summary = validation_summary(_refusal(adapter, MARKER))

        assert summary == f'{INPUT_PLACEHOLDER} [greater_than]'

    def test_a_dict_key_stays_in_the_location(self) -> None:
        summary = validation_summary(_refusal(TypeAdapter(dict[str, int]), {MARKER: 'x'}))

        assert summary.startswith(f'{MARKER}: ')

    def test_a_refused_extra_key_stays_in_the_location(self) -> None:
        summary = validation_summary(_refusal(TypeAdapter(_Strict), {'a': 1, MARKER: 2}))

        assert summary == f'{MARKER}: Extra inputs are not permitted [extra_forbidden]'

    def test_the_text_is_bounded(self) -> None:
        count = MAX_VALIDATION_ERRORS_IN_TEXT + 2
        error = _refusal(TypeAdapter(_Many), {'values': ['x'] * count})

        summary = validation_summary(error)

        assert summary.count('[int_parsing]') == MAX_VALIDATION_ERRORS_IN_TEXT
        assert summary.endswith('; (+2 more)')

    def test_every_builtin_placeholder_is_classified(self) -> None:
        placeholders = {
            field
            for info in list_all_errors()
            for _, field, _, _ in string.Formatter().parse(info['message_template_python'])
            if field is not None
        }

        assert placeholders - SCHEMA_CONTEXT_KEYS - INPUT_CONTEXT_KEYS == set()
        assert SCHEMA_CONTEXT_KEYS & INPUT_CONTEXT_KEYS == set()


class TestValidationErrorEntries:
    def test_entries_hold_location_message_and_type_only(self) -> None:
        entries = validation_error_entries(_refusal(TypeAdapter(_Pet), {'kind': MARKER}))

        assert MARKER not in json.dumps(entries)
        assert entries == [{
            'loc': [],
            'msg': (
                f"Input tag '{INPUT_PLACEHOLDER}' found using 'kind' does not match "
                "any of the expected tags: 'cat', 'dog'"
            ),
            'type': 'union_tag_invalid',
        }]

    def test_entries_are_bounded(self) -> None:
        count = MAX_VALIDATION_ERRORS_IN_TEXT + 5
        error = _refusal(TypeAdapter(_Many), {'values': ['x'] * count})

        assert len(validation_error_entries(error)) == MAX_VALIDATION_ERRORS_IN_TEXT


class TestHelpers:
    def test_exception_text_of_a_validation_error_is_the_summary(self) -> None:
        error = _refusal(TypeAdapter(int), MARKER)

        assert exception_text(error) == validation_summary(error)

    def test_exception_text_of_another_exception_is_its_str(self) -> None:
        assert exception_text(KeyError(MARKER)) == str(KeyError(MARKER))

    def test_without_validation_input_replaces_a_validation_error(self) -> None:
        cause = without_validation_input(_refusal(TypeAdapter(int), MARKER))

        assert isinstance(cause, ValueValidationError)
        assert MARKER not in str(cause)
        assert cause.__context__ is None

    def test_without_validation_input_keeps_another_exception(self) -> None:
        original = ConnectionError('db gone')

        assert without_validation_input(original) is original


class TestCodecBoundary:
    def test_an_encode_refusal_holds_no_input(self) -> None:
        with pytest.raises(ValueValidationError) as caught:
            encode_value({'a': MARKER}, _Pair)

        assert MARKER not in str(caught.value)
        assert caught.value.__context__ is None
        assert caught.value.__cause__ is None

    def test_a_decode_refusal_holds_no_input(self) -> None:
        with pytest.raises(ValueValidationError) as caught:
            decode_value({'a': MARKER}, _Pair)

        assert MARKER not in str(caught.value)
        assert caught.value.__context__ is None

    def test_the_refusal_is_a_serialization_error_and_a_value_error(self) -> None:
        with pytest.raises(ValueValidationError) as caught:
            encode_value(MARKER, int)

        assert isinstance(caught.value, SerializationError)
        assert isinstance(caught.value, ValueError)

    def test_a_serializer_failure_is_converted(self) -> None:
        class _Failing(BaseModel):
            a: int

            @field_serializer('a')
            def _refuse(self, value: int) -> int:  # pyright: ignore[reportUnusedFunction]
                raise ValueError('serializer refused')

        with pytest.raises(ValueValidationError, match='serialization failed') as caught:
            encode_value(_Failing(a=1), _Failing)

        assert caught.value.__context__ is None


def _validation_error_with_marker() -> None:
    TypeAdapter(int).validate_python(MARKER)


def _caused_by_validation_error() -> None:
    try:
        _validation_error_with_marker()
    except ValidationError as error:
        raise RuntimeError('outer') from error


def _during_validation_error() -> None:
    try:
        _validation_error_with_marker()
    except ValidationError:
        raise KeyError('during')


def _suppressed_validation_error() -> None:
    try:
        _validation_error_with_marker()
    except ValidationError:
        raise RuntimeError('replaced') from None


def _group_with_validation_error() -> None:
    raise ExceptionGroup('many', [_refusal(TypeAdapter(int), MARKER), ValueError('plain')])


def _plain_chain() -> None:
    try:
        raise ValueError('inner')
    except ValueError as error:
        raise RuntimeError('outer') from error


def _long_chain_with_validation_error_at_the_oldest_end() -> BaseException:
    oldest = _raised_from(_validation_error_with_marker)
    current = oldest
    for index in range(MAX_EXCEPTION_CHAIN_LINKS + 5):
        newer = RuntimeError(f'link {index}')
        newer.__cause__ = current
        current = newer
    return current


def _flattened_text(exc: BaseException) -> str:
    flat = flatten_exception(exc)
    return flat['message'] + flat['repr'] + flat['traceback']


class TestFlattenException:
    def test_a_validation_error_uses_the_summary(self) -> None:
        exc = _raised_from(_validation_error_with_marker)
        assert isinstance(exc, ValidationError)

        flat = flatten_exception(exc)

        assert MARKER not in _flattened_text(exc)
        assert flat['message'] == validation_summary(exc)
        assert flat['traceback'].endswith(
            f'pydantic_core._pydantic_core.ValidationError: {validation_summary(exc)}\n',
        )

    def test_a_validation_error_cause_is_replaced_in_the_traceback(self) -> None:
        exc = _raised_from(_caused_by_validation_error)

        flat = flatten_exception(exc)

        assert MARKER not in _flattened_text(exc)
        assert 'The above exception was the direct cause' in flat['traceback']
        assert flat['traceback'].endswith('RuntimeError: outer\n')

    def test_a_validation_error_context_is_replaced_in_the_traceback(self) -> None:
        exc = _raised_from(_during_validation_error)

        flat = flatten_exception(exc)

        assert MARKER not in _flattened_text(exc)
        assert 'During handling of the above exception' in flat['traceback']

    def test_a_suppressed_context_keeps_the_standard_traceback(self) -> None:
        exc = _raised_from(_suppressed_validation_error)

        assert flatten_exception(exc)['traceback'] == ''.join(traceback.format_exception(exc))
        assert MARKER not in _flattened_text(exc)

    def test_a_group_member_is_not_shown(self) -> None:
        exc = _raised_from(_group_with_validation_error)

        flat = flatten_exception(exc)

        assert MARKER not in _flattened_text(exc)
        assert 'sub-exceptions not shown' in flat['traceback']

    def test_a_chain_without_validation_errors_keeps_the_standard_traceback(self) -> None:
        exc = _raised_from(_plain_chain)

        flat = flatten_exception(exc)

        assert flat['traceback'] == ''.join(traceback.format_exception(exc))
        assert flat['message'] == 'outer'
        assert flat['repr'] == "RuntimeError('outer')"

    def test_a_chain_past_the_bound_fails_closed(self) -> None:
        exc = _long_chain_with_validation_error_at_the_oldest_end()

        flat = flatten_exception(exc)

        assert MARKER not in _flattened_text(exc)
        assert 'older chained exceptions not shown' in flat['traceback']


def _make_app() -> Any:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.config.exception_mapper = {}
    app.config.default_unhandled_error_code = None
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    return app


def _stored_text(result: TaskResult[Any, TaskError]) -> str:
    return json.dumps(encode_task_result(result, int))


class TestReturnTypeMismatch:
    def test_the_output_value_is_not_stored(self) -> None:
        def returns_wrong_type(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok={'secret': MARKER})  # type: ignore[arg-type]

        wrapper = create_task_wrapper(returns_wrong_type, _make_app(), 'test.returns_wrong_type')

        result = wrapper(x=1)

        assert result.is_err()
        err = result.err_value
        assert err.error_code == ContractCode.RETURN_TYPE_MISMATCH
        assert err.exception is None
        assert MARKER not in (err.message or '')
        assert MARKER not in json.dumps(err.data)
        assert MARKER not in _stored_text(result)
        assert 'TaskResult(ok=<dict>)' in (err.message or '')
        data = cast(dict[str, Any], err.data)
        assert data['actual_type'] == 'dict'
        assert data['validation_error_count'] == 1
        assert data['validation_errors'][0]['type'] == 'int_type'
        assert 'actual_value' not in data


class TestTaskRaisesValidationError:
    def test_the_refused_input_is_not_stored(self) -> None:
        def validates_its_input(*, x: int) -> TaskResult[int, TaskError]:
            TypeAdapter(int).validate_python(MARKER)
            return TaskResult(ok=x)

        wrapper = create_task_wrapper(validates_its_input, _make_app(), 'test.validates_its_input')

        result = wrapper(x=1)

        assert result.is_err()
        assert MARKER not in (result.err_value.message or '')
        assert MARKER not in _stored_text(result)


class TestSendOptions:
    def test_a_refused_send_option_gives_a_cause_without_input(self) -> None:
        naive_deadline = datetime(2031, 7, 9, 11, 22, 33)

        def task_fn(*, x: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=x)

        wrapper = create_task_wrapper(task_fn, _make_app(), 'test.send_options')

        result = wrapper.with_options(good_until=naive_deadline).send(x=1)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, TaskSendValidationFailed)
        assert isinstance(err.exception, ValueValidationError)
        assert '2031' not in err.message
        assert '2031' not in str(err.exception)
        assert 'good_until must be timezone-aware' in err.message


class TestWorkflowNodeKwargs:
    def test_a_refused_node_kwarg_holds_no_input(self) -> None:
        def node_fn(*, count: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=count)

        node = SimpleNamespace(fn=node_fn, kwargs={'count': MARKER}, name='node-a')

        with pytest.raises(SerializationError) as caught:
            _encode_node_kwargs_or_raise(node)

        assert MARKER not in str(caught.value)
        assert MARKER not in ''.join(traceback.format_exception(caught.value))


class _TypedSubworkflow(WorkflowDefinition[int]):
    """build_with(cls, app, *, count: int) — typed kwarg-only."""

    name = 'validation_text_typed_subworkflow'
    definition_key = 'tests.unit.validation_text.typed_subworkflow.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls, app: Horsies, *, count: int,  # pyright: ignore[reportUnusedParameter]
    ) -> WorkflowSpec[int]:
        raise NotImplementedError


class TestSubworkflowKwargs:
    def test_a_refused_subworkflow_kwarg_holds_no_input(self) -> None:
        with pytest.raises(ValueValidationError) as caught:
            encode_subworkflow_kwargs(_TypedSubworkflow, {'count': MARKER})

        assert MARKER not in str(caught.value)


class TestBrokerErrorCause:
    def test_a_validation_error_cause_is_stored_without_input(self) -> None:
        error = _refusal(TypeAdapter(int), MARKER)

        result = _broker_err(BrokerErrorCode.WORKER_PING_FAILED, 'ping failed', error)

        cause = result.err_value.exception
        assert isinstance(cause, ValueValidationError)
        assert MARKER not in str(cause)
        assert result.err_value.retryable is False


class TestDefinitionTimeOptions:
    def test_a_refused_decorator_option_holds_no_value(self) -> None:
        app = object.__new__(Horsies)

        def task_fn() -> TaskResult[str, TaskError]:
            return TaskResult(ok='x')

        with pytest.raises(TaskDefinitionError) as caught:
            refused_options: dict[str, Any] = {'timeout_ms': MARKER}
            app.task(task_name='test.bad_timeout', **refused_options)(task_fn)

        notes = ' '.join(caught.value.notes)
        assert caught.value.code == ErrorCode.TASK_INVALID_OPTIONS
        assert MARKER not in notes
        assert 'timeout_ms: Input should be a valid integer' in notes

    def test_refused_retry_policy_metadata_holds_no_value(self) -> None:
        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url=SecretStr('postgresql+psycopg://x/y')),
            ),
        )
        app.tasks['test.metadata'] = cast(Any, SimpleNamespace(
            task_options_json=json.dumps({
                'retry_policy': {'max_retries': MARKER, 'auto_retry_for': ['TRANSIENT']},
            }),
        ))

        errors = app._check_runtime_policy_safety()  # pyright: ignore[reportPrivateUsage]

        refused = [e for e in errors if e.message == 'invalid retry_policy metadata']
        assert len(refused) == 1
        notes = ' '.join(refused[0].notes)
        assert MARKER not in notes
        assert 'max_retries: Input should be a valid integer' in notes


class TestHealthPayloadLogs:
    @pytest.mark.asyncio
    async def test_a_refused_ping_payload_is_logged_without_values(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        payload = json.dumps({'correlation_id': 'c-1', 'reply_channel': {'secret': MARKER}})

        with caplog.at_level(logging.WARNING):
            await HealthMixin._handle_ping(cast(Any, MagicMock()), payload)  # pyright: ignore[reportPrivateUsage]

        assert 'Discarding invalid ping payload' in caplog.text
        assert MARKER not in caplog.text

    def test_a_refused_pong_payload_is_logged_without_values(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        payload = json.dumps({
            'correlation_id': 'c-1', 'worker_id': 'w-1', 'hostname': 'h', 'pid': MARKER,
        })
        broker_like = cast(Any, SimpleNamespace(logger=get_logger('broker')))

        with caplog.at_level(logging.WARNING):
            pong = PostgresBroker._decode_pong(broker_like, payload, 'c-1', 0.0)  # pyright: ignore[reportPrivateUsage]

        assert pong is None
        assert 'Discarding invalid pong payload' in caplog.text
        assert MARKER not in caplog.text
