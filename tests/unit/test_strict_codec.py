"""The strict codec: input, read-back, lost fields, and the error path.

- A wrong value is refused, not converted.
- A value that a reader cannot read is refused before it is stored.
- Data that the declared type cannot carry is refused, not dropped.
- A bad error keeps its code, so a retry rule still matches.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import json
import uuid
from typing import Annotated, Any, Literal, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    computed_field,
    model_serializer,
)

from horsies import AppConfig, Horsies, PostgresConfig, TaskError, TaskResult
from horsies.core.codec.json_io import SerializationError, dumps_json, loads_json
from horsies.core.codec.json_value import (
    MAX_JSON_DEPTH,
    MAX_TYPED_JSON_DEPTH,
    StrictJsonError,
)
from horsies.core.codec.kwargs import encode_kwargs, encode_kwargs_read_back
from horsies.core.codec.typed import (
    EncodeRefused,
    FieldsLost,
    ReadBackRefused,
    decode_task_error,
    decode_task_result,
    decode_value,
    encode_and_read_back,
    encode_returned_result,
    encode_task_error,
    encode_task_result,
    encode_value,
    reportable_error,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.errors import ErrorCode
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.schedule import IntervalSchedule, TaskSchedule
from horsies.core.models.task_send_types import TaskSendValidationFailed
from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    SubWorkflowError,
)
from horsies.core.models.workflow import WorkflowStatus
from horsies.core.models.workflow.context import SubWorkflowSummary
from horsies.core.scheduler.validation import _encode_schedule_kwargs
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.result import Err, Ok, is_err, is_ok


class Color(enum.Enum):
    RED = 'red'


class Inner(BaseModel):
    n: int = 1


class Note(BaseModel):
    n: int
    note: str = 'default'


class Sub(Note):
    secret: str = 's'


class SameSub(Note):
    pass


class Holder(BaseModel):
    items: list[Note]
    table: dict[str, Note] = {}
    maybe: Optional[Note] = None
    pair: tuple[Note, ...] = ()


class One(BaseModel):
    item: Note


class Ignore(BaseModel):
    model_config = ConfigDict(extra='ignore')
    n: int


class OpenSub(Note):
    model_config = ConfigDict(extra='allow')


class Revalidated(BaseModel):
    model_config = ConfigDict(revalidate_instances='always')
    n: int


class RevalidatedSub(Revalidated):
    secret: str = 's'


@dataclasses.dataclass
class Point:
    n: int


@dataclasses.dataclass
class PointSub(Point):
    extra: int = 0


@dataclasses.dataclass
class Named:
    item: Note


class StrictDate(BaseModel):
    model_config = ConfigDict(strict=True)
    when: datetime.datetime
    span: tuple[int, ...] = ()


class Excluded(BaseModel):
    n: int = 1
    secret: str = Field(default='d', exclude=True)


class Drops(BaseModel):
    n: int = 1
    s: str = 'd'

    @model_serializer
    def dump(self) -> dict[str, int]:
        return {'n': self.n}


class Factory(BaseModel):
    items: list[int] = Field(default_factory=list)


class WithComputed(BaseModel):
    n: int = 1

    @computed_field
    @property
    def double(self) -> int:
        return self.n * 2


Upper = Annotated[str, BeforeValidator(str.upper), PlainSerializer(str.upper, return_type=str)]


class Transforming(BaseModel):
    s: Upper = 'D'


def _read_renamed(value: object) -> object:
    if isinstance(value, dict) and 'value' in value:
        return {'n': value['value']}
    return value


Renamed = Annotated[
    Inner,
    BeforeValidator(_read_renamed),
    PlainSerializer(lambda model: {'value': model.n}, return_type=dict),
]
NOW = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


@pytest.mark.unit
class TestStrictInput:
    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            ('7', int),
            (7.0, int),
            (True, int),
            ('yes', bool),
            (1, bool),
            ('3.5', float),
            (True, float),
            (decimal.Decimal('7'), int),
            (1700000000, datetime.datetime),
            ('2026-01-01T00:00:00Z', datetime.datetime),
            ('red', Color),
            ('00000000-0000-0000-0000-000000000001', uuid.UUID),
            ('1.5', decimal.Decimal),
            ((1, 2), list[int]),
            ({1, 2}, list[int]),
            (['1'], list[int]),
            ([1, 2], tuple[int, ...]),
            (b'abc', str),
            ({'n': '7'}, Inner),
        ],
    )
    def test_a_conversion_is_refused(
        self,
        value: object,
        expected_type: Any,
    ) -> None:
        with pytest.raises(ValueValidationError):
            encode_value(value, expected_type)

    @pytest.mark.parametrize(
        ('value', 'expected_type', 'wire'),
        [
            (7, int, 7),
            (1, float, 1.0),
            (True, bool, True),
            ('x', str, 'x'),
            (decimal.Decimal('1.50'), decimal.Decimal, '1.50'),
            (NOW, datetime.datetime, '2026-01-01T00:00:00Z'),
            (uuid.UUID(int=1), uuid.UUID, '00000000-0000-0000-0000-000000000001'),
            (Color.RED, Color, 'red'),
            ((1, 2), tuple[int, ...], [1, 2]),
            ([1], list[int], [1]),
            ({'a': 1}, dict[str, int], {'a': 1}),
            (None, Optional[int], None),
            ('a', Literal['a'], 'a'),
            (2**70, int, 2**70),
            ({'n': 1}, Inner, {'n': 1}),
            (Inner(n=2), Inner, {'n': 2}),
        ],
    )
    def test_a_right_value_keeps_its_wire_form(
        self,
        value: object,
        expected_type: Any,
        wire: object,
    ) -> None:
        assert encode_value(value, expected_type) == wire

    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            ({'n': 1, 'bogus': 2}, Note),
            ({'item': {'n': 1, 'bogus': 2}}, One),
            ([{'n': 1, 'bogus': 2}], list[Note]),
            ({'items': [{'n': 1, 'bogus': 2}]}, Holder),
            ({'k': {'n': 1, 'bogus': 2}}, dict[str, Note]),
            ({'items': [], 'maybe': {'n': 1, 'bogus': 2}}, Holder),
            ({'items': [], 'pair': ({'n': 1, 'bogus': 2},)}, Holder),
            ({'n': 1, 'bogus': 2}, Ignore),
        ],
    )
    def test_an_unknown_key_is_refused_at_any_depth(
        self,
        value: object,
        expected_type: Any,
    ) -> None:
        with pytest.raises(ValueValidationError, match='extra_forbidden'):
            encode_value(value, expected_type)

    def test_a_revalidated_subclass_field_is_refused(self) -> None:
        with pytest.raises(ValueValidationError, match='extra_forbidden'):
            encode_value(RevalidatedSub(n=1, secret='keep'), Revalidated)


@pytest.mark.unit
class TestStrictRead:
    @pytest.mark.parametrize(
        ('stored', 'expected_type', 'read'),
        [
            (1, float, 1.0),
            ('2026-01-01T00:00:00Z', datetime.datetime, NOW),
            ('1.50', decimal.Decimal, decimal.Decimal('1.50')),
            ('red', Color, Color.RED),
            ([1, 2], tuple[int, ...], (1, 2)),
            ({'n': 1}, Inner, Inner(n=1)),
            ({'n': 1}, Point, Point(n=1)),
        ],
    )
    def test_a_stored_form_reads(
        self,
        stored: Any,
        expected_type: Any,
        read: object,
    ) -> None:
        assert decode_value(stored, expected_type) == read

    @pytest.mark.parametrize(
        ('stored', 'expected_type'),
        [
            (7.0, int),
            ('7', int),
            (1, bool),
            ('not a date', datetime.datetime),
        ],
    )
    def test_a_value_that_does_not_fit_is_refused(
        self,
        stored: Any,
        expected_type: Any,
    ) -> None:
        with pytest.raises(ValueValidationError):
            decode_value(stored, expected_type)

    def test_an_unknown_key_is_ignored_on_read(self) -> None:
        assert decode_value({'n': 1, 'gone': 2}, Note) == Note(n=1)

    def test_a_strict_model_reads_its_datetime_and_tuple(self) -> None:
        wire = encode_value(StrictDate(when=NOW, span=(1,)), StrictDate)

        assert decode_value(wire, StrictDate) == StrictDate(when=NOW, span=(1,))


@pytest.mark.unit
class TestDepth:
    def _nested(self, levels: int) -> Any:
        value: Any = 0
        for _ in range(levels):
            value = [value]
        return value

    def test_the_limit_is_below_the_json_bound(self) -> None:
        assert MAX_TYPED_JSON_DEPTH < MAX_JSON_DEPTH

    def test_the_constant_is_the_limit_of_the_read(self) -> None:
        deepest = self._nested(MAX_TYPED_JSON_DEPTH)

        assert decode_value(deepest, Any) == deepest

    def test_one_more_level_is_refused_with_the_constant(self) -> None:
        with pytest.raises(ValueValidationError, match=str(MAX_TYPED_JSON_DEPTH)):
            decode_value(self._nested(MAX_TYPED_JSON_DEPTH + 1), Any)

    @pytest.mark.parametrize('levels', [254, 255, 300])
    def test_past_the_dump_limit_is_refused_not_raised(self, levels: int) -> None:
        with pytest.raises((ValueValidationError, StrictJsonError)):
            decode_value(self._nested(levels), Any)


@pytest.mark.unit
class TestReadBack:
    def test_a_good_value_gives_both_forms(self) -> None:
        result = encode_and_read_back(Inner(n=1), Inner)

        assert is_ok(result)
        assert result.ok_value.encoded == {'n': 1}
        assert result.ok_value.decoded == Inner(n=1)

    def test_a_wrong_value_is_an_encode_refusal(self) -> None:
        result = encode_and_read_back('7', int)

        assert is_err(result)
        assert isinstance(result.err_value, EncodeRefused)

    @pytest.mark.parametrize(
        'value',
        [
            Note.model_construct(n='x'),
            Note.model_construct(n='7'),
        ],
        ids=['text', 'numeric-text'],
    )
    def test_a_value_built_past_validation_is_a_read_back_refusal(
        self,
        value: Note,
    ) -> None:
        result = encode_and_read_back(value, Note)

        assert is_err(result)
        assert isinstance(result.err_value, ReadBackRefused)

    def test_a_mutated_instance_is_refused(self) -> None:
        value = Note(n=1)
        value.n = '7'  # type: ignore[assignment]

        result = encode_and_read_back(value, Note)

        assert is_err(result)
        assert isinstance(result.err_value, ReadBackRefused)

    def test_a_dataclass_with_a_wrong_field_is_refused(self) -> None:
        result = encode_and_read_back(Point(n='x'), Point)  # type: ignore[arg-type]

        assert is_err(result)
        assert isinstance(result.err_value, ReadBackRefused)

    def test_the_location_is_in_the_refusal(self) -> None:
        result = encode_and_read_back(Point(n='x'), Point, location=('point',))  # type: ignore[arg-type]

        assert is_err(result)
        assert isinstance(result.err_value, ReadBackRefused)
        assert 'point' in str(result.err_value.cause)


@pytest.mark.unit
class TestLostFields:
    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            (Sub(n=1, secret='keep'), Note),
            (One(item=Sub(n=1)), One),
            (Holder(items=[Sub(n=1)]), Holder),
            (Holder(items=[], maybe=Sub(n=1)), Holder),
            (Holder(items=[], table={'k': Sub(n=1)}), Holder),
            (Holder(items=[], pair=(Sub(n=1),)), Holder),
            ([Sub(n=1)], list[Note]),
            ({'k': Sub(n=1)}, dict[str, Note]),
            ((Sub(n=1), 1), tuple[Note, int]),
            ({'item': Sub(n=1)}, One),
            (PointSub(n=1), Point),
            (Named(item=Sub(n=1)), Named),
            (OpenSub(n=1, keep='me'), Note),
            (Holder(items=[OpenSub(n=1, keep='me')]), Holder),
            (Excluded(secret='keep'), Excluded),
            ([Excluded(secret='keep')], list[Excluded]),
            (Drops(s='keep'), Drops),
        ],
    )
    def test_found(self, value: object, expected_type: Any) -> None:
        result = encode_and_read_back(value, expected_type)

        assert is_err(result)
        assert isinstance(result.err_value, FieldsLost)

    def test_the_detail_holds_no_value(self) -> None:
        result = encode_and_read_back(Sub(n=1, secret='secret-marker'), Note)

        assert is_err(result)
        assert isinstance(result.err_value, FieldsLost)
        assert 'secret-marker' not in result.err_value.detail
        assert 'secret' in result.err_value.detail

    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            (Note(n=1), Note),
            (SameSub(n=1), Note),
            (OpenSub(n=1), Note),
            (Excluded(), Excluded),
            (Factory(), Factory),
            (Factory(items=[1]), Factory),
            (WithComputed(), WithComputed),
            (Transforming(s='a'), Transforming),
            (Inner(n=1), Renamed),
        ],
    )
    def test_not_found(self, value: object, expected_type: Any) -> None:
        assert is_ok(encode_and_read_back(value, expected_type))

    def test_the_walk_is_skipped_on_plain_data(self) -> None:
        with patch('horsies.core.codec.typed._lost_field') as walk:
            assert is_ok(encode_and_read_back([{'a': 1}], list[dict[str, int]]))

        walk.assert_not_called()

    def test_the_walk_runs_on_a_type_that_holds_a_record(self) -> None:
        with patch(
            'horsies.core.codec.typed._lost_field',
            return_value=None,
        ) as walk:
            assert is_ok(encode_and_read_back([Inner()], list[Inner]))

        walk.assert_called_once()


def _mock_app() -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.config.exception_mapper = {}
    app.config.default_unhandled_error_code = 'UNHANDLED_EXCEPTION'
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    return app


@pytest.mark.unit
class TestEntryPoints:
    def test_send_refuses_a_value_the_worker_cannot_read(self) -> None:
        def task(*, order: Note) -> TaskResult[int, TaskError]:
            return TaskResult(ok=order.n)

        app = _mock_app()
        value = Note(n=1)
        value.n = 'x'  # type: ignore[assignment]

        result = create_task_wrapper(task, app, 'test.task').send(order=value)

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        app.get_broker.assert_not_called()

    def test_send_refuses_a_typo_in_a_nested_key(self) -> None:
        def task(*, order: Note) -> TaskResult[int, TaskError]:
            return TaskResult(ok=order.n)

        app = _mock_app()

        result = create_task_wrapper(task, app, 'test.task').send(
            order={'n': 1, 'noet': 'high'},
        )

        assert is_err(result)
        app.get_broker.assert_not_called()

    def test_send_refuses_a_subclass_that_loses_fields(self) -> None:
        def task(*, order: Note) -> TaskResult[int, TaskError]:
            return TaskResult(ok=order.n)

        app = _mock_app()

        result = create_task_wrapper(task, app, 'test.task').send(order=Sub(n=1))

        assert is_err(result)
        assert 'secret' in result.err_value.message
        app.get_broker.assert_not_called()

    def test_a_return_with_an_unknown_key_is_refused(self) -> None:
        def task() -> TaskResult[Note, TaskError]:
            return TaskResult(ok={'n': 1, 'bogus': 2})  # type: ignore[arg-type]

        result = create_task_wrapper(task, _mock_app(), 'test.task')()

        assert result.is_err()
        assert result.err_value.error_code == ContractCode.RETURN_TYPE_MISMATCH

    def test_a_schedule_refuses_a_value_the_worker_cannot_read(self) -> None:
        app = Horsies(
            AppConfig(
                broker=PostgresConfig(
                    database_url='postgresql+psycopg://user:password@localhost/unused',
                ),
            ),
        )

        @app.task('schedule_read_back')
        def task(*, order: Note) -> TaskResult[int, TaskError]:
            return TaskResult(ok=order.n)

        value = Note(n=1)
        value.n = 'x'  # type: ignore[assignment]
        schedule = TaskSchedule(
            name='nightly',
            task_name='schedule_read_back',
            pattern=IntervalSchedule(hours=1),
            kwargs={'order': value},
        )

        result = _encode_schedule_kwargs(app, schedule)

        assert is_err(result)
        assert result.err_value.code is ErrorCode.CONFIG_INVALID_SCHEDULE

    def test_the_checked_entry_keeps_the_wire_form(self) -> None:
        def task(*, order: Note) -> TaskResult[int, TaskError]:
            return TaskResult(ok=order.n)

        assert encode_kwargs_read_back(task, {'order': Note(n=1)}) == {
            'order': {'n': 1, 'note': 'default'},
        }

    def test_encode_kwargs_runs_no_read_back(self) -> None:
        def task(*, order: Note) -> TaskResult[int, TaskError]:
            return TaskResult(ok=order.n)

        with patch('horsies.core.codec.typed.decode_value') as decode:
            assert encode_kwargs(task, {'order': Note(n=1)})

        decode.assert_not_called()

    def test_a_returned_result_is_proved_at_the_wire(self) -> None:
        with pytest.raises(ValueValidationError, match='secret'):
            encode_returned_result(TaskResult(ok=Sub(n=1)), Note)

    def test_a_good_returned_result_keeps_its_envelope(self) -> None:
        assert encode_returned_result(TaskResult(ok=1), int) == encode_task_result(
            TaskResult(ok=1),
            int,
        )


def _summary(**fields: Any) -> SubWorkflowSummary[Any]:
    base: dict[str, Any] = {
        'status': WorkflowStatus.FAILED,
        'output': None,
        'error_summary': None,
        'total_tasks': 2,
        'completed_tasks': 1,
        'failed_tasks': 1,
        'skipped_tasks': 0,
    }
    return SubWorkflowSummary(**{**base, **fields})


def _deep(levels: int) -> Any:
    value: Any = 0
    for _ in range(levels):
        value = [value]
    return value


class _Opaque:
    pass


class _RateLimited(TaskError):
    retry_after: float = 1.5


class _WithObject(TaskError):
    holds: object = None


class _RaisingDict(dict[str, str]):
    def items(self) -> Any:
        raise RuntimeError('a dict that cannot be read')


class _Evil(ValueError):
    def __str__(self) -> str:
        raise RuntimeError('no')


def _stored(err: TaskError) -> TaskError:
    """The full path of a stored error."""
    wire = encode_task_result(TaskResult(err=err), type(None), 'task')
    text = dumps_json(wire)
    assert is_ok(text)
    parsed = loads_json(text.ok_value)
    assert is_ok(parsed)
    return decode_task_result(parsed.ok_value, type(None)).err_value


@pytest.mark.unit
class TestReportableError:
    @pytest.mark.parametrize(
        'data',
        [
            (1, 2),
            {1, 2},
            decimal.Decimal('1.5'),
            {'retry_after': decimal.Decimal('1.5')},
            NOW,
            uuid.UUID(int=1),
            Color.RED,
            Inner(),
            _Opaque(),
            float('nan'),
            {'x': float('inf')},
            {'__h_x': 1},
            '\ud800',
            b'abc',
            {1: 'a'},
            ValueError('boom'),
            _deep(250),
        ],
        ids=lambda data: type(data).__name__,
    )
    def test_bad_data_keeps_the_code_and_the_message(self, data: object) -> None:
        read = _stored(TaskError(error_code='RATE_LIMITED', message='m', data=data))

        assert read.error_code == 'RATE_LIMITED'
        assert read.message == 'm'
        assert 'data_refused' in read.data

    def test_an_int_too_long_for_json_keeps_the_code(self) -> None:
        read = _stored(TaskError(error_code='RATE_LIMITED', data=10**5000))

        assert read.error_code == 'RATE_LIMITED'
        assert 'data_refused' in read.data

    def test_a_self_reference_keeps_the_code(self) -> None:
        data: dict[str, Any] = {}
        data['self'] = data

        read = _stored(TaskError(error_code='RATE_LIMITED', data=data))

        assert read.error_code == 'RATE_LIMITED'

    @pytest.mark.parametrize('data', [{'a': 1.5}, None, 'x', [1, 'y', None], 5])
    def test_good_data_is_unchanged(self, data: object) -> None:
        err = TaskError(error_code='RATE_LIMITED', message='m', data=data)

        assert _stored(err).data == data

    def test_a_lone_surrogate_in_the_message_is_escaped(self) -> None:
        read = _stored(TaskError(error_code='RATE_LIMITED', message='bad \ud800'))

        assert read.error_code == 'RATE_LIMITED'
        assert read.message == 'bad \\ud800'

    def test_a_lone_surrogate_in_the_exception_is_escaped(self) -> None:
        read = _stored(
            TaskError(error_code='RATE_LIMITED', exception=ValueError('bad \ud800')),
        )

        assert read.error_code == 'RATE_LIMITED'

    def test_a_lone_surrogate_in_the_code_is_escaped(self) -> None:
        read = _stored(TaskError(error_code='RATE\ud800'))

        assert read.error_code == 'RATE\\ud800'

    def test_an_exception_whose_str_raises_keeps_the_code(self) -> None:
        read = _stored(TaskError(error_code='RATE_LIMITED', exception=_Evil()))

        assert read.error_code == 'RATE_LIMITED'

    @pytest.mark.parametrize(
        'err',
        [
            _RateLimited(error_code='RATE_LIMITED'),
            _WithObject(error_code='RATE_LIMITED', holds=_Opaque()),
        ],
        ids=['new-field', 'object-field'],
    )
    def test_a_subclass_comes_back_plain_with_its_code(self, err: TaskError) -> None:
        read = _stored(err)

        assert type(read) is TaskError
        assert read.error_code == 'RATE_LIMITED'
        assert 'data_refused' in read.data

    def test_a_built_in_code_keeps_its_type(self) -> None:
        read = _stored(
            TaskError(error_code=OperationalErrorCode.BROKER_ERROR, data=_Opaque()),
        )

        assert read.error_code is OperationalErrorCode.BROKER_ERROR

    @pytest.mark.parametrize(
        'summary',
        [
            _summary(output=object(), error_summary='failed'),
            _summary(error_summary='bad \ud800'),
            _summary(output=_deep(250)),
        ],
        ids=['object-output', 'surrogate-summary', 'deep-output'],
    )
    def test_a_bad_sub_workflow_error_keeps_its_class(
        self,
        summary: SubWorkflowSummary[Any],
    ) -> None:
        read = _stored(
            SubWorkflowError(
                error_code='X',
                sub_workflow_id='wf-1',
                sub_workflow_summary=summary,
            ),
        )

        assert isinstance(read, SubWorkflowError)
        assert read.sub_workflow_id == 'wf-1'
        assert read.sub_workflow_summary.failed_tasks == 1

    def test_a_surrogate_in_the_id_keeps_the_class(self) -> None:
        read = _stored(
            SubWorkflowError(
                error_code='X',
                sub_workflow_id='wf-\ud800',
                sub_workflow_summary=_summary(),
            ),
        )

        assert isinstance(read, SubWorkflowError)
        assert read.sub_workflow_id == 'wf-\\ud800'

    def test_bad_data_keeps_a_good_output(self) -> None:
        read = _stored(
            SubWorkflowError(
                error_code='X',
                data=_Opaque(),
                sub_workflow_id='wf-1',
                sub_workflow_summary=_summary(output={'a': 1}),
            ),
        )

        assert isinstance(read, SubWorkflowError)
        assert read.sub_workflow_summary.output == {'a': 1}

    def test_a_good_sub_workflow_error_stays(self) -> None:
        err = SubWorkflowError(
            error_code='X',
            sub_workflow_id='wf-1',
            sub_workflow_summary=_summary(output={'a': 1}),
        )

        read = _stored(err)

        assert isinstance(read, SubWorkflowError)
        assert read.data is None

    def test_damaged_counts_fall_back_to_a_plain_error(self) -> None:
        err = SubWorkflowError(
            error_code='X',
            sub_workflow_id='wf-1',
            sub_workflow_summary=_summary(failed_tasks='many'),
        )

        read = _stored(err)

        assert type(read) is TaskError
        assert read.error_code == 'X'

    def test_a_constructed_error_keeps_its_code(self) -> None:
        err = SubWorkflowError.model_construct(error_code='X')

        read = _stored(err)

        assert read.error_code == 'X'

    def test_the_check_runs_the_real_writer(self) -> None:
        with patch(
            'horsies.core.codec.typed.dumps_json',
            side_effect=[Err(SerializationError('cannot write')), Ok('{}')],
        ):
            kept = reportable_error(TaskError(error_code='X', data={'a': 1}), 'task')

        assert kept.data == {'data_refused': 'SerializationError'}

    def test_the_cause_holds_no_value(self) -> None:
        kept = reportable_error(
            TaskError(error_code='X', data={'secret-marker': _Opaque()}),
            'task',
        )

        assert 'secret-marker' not in str(kept.data)

    def test_a_refusal_logs_with_its_origin(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level('ERROR', logger='horsies.codec'):
            reportable_error(TaskError(error_code='X', data=_Opaque()), 'task')

        record = next(item for item in caplog.records if item.name == 'horsies.codec')
        assert 'X' in record.getMessage()
        assert record.refusal_origin == 'task'  # type: ignore[attr-defined]

    @pytest.mark.allow_library_data_refusal
    def test_a_library_refusal_is_marked(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level('ERROR', logger='horsies.codec'):
            encode_task_result(
                TaskResult(err=TaskError(error_code='X', data=_Opaque())),
                type(None),
            )

        record = next(item for item in caplog.records if item.name == 'horsies.codec')
        assert record.refusal_origin == 'library'  # type: ignore[attr-defined]

    @pytest.mark.parametrize(
        ('field', 'bad'),
        [
            ('message', 5),
            ('exception', 5),
            ('exception', {'type': 1}),
            ('exception', {'type': 'V', 'module': 'm', 'message': 1, 'repr': 'r', 'traceback': ''}),
        ],
        ids=['message_int', 'exception_int', 'exception_bad_dict', 'exception_bad_field'],
    )
    def test_a_field_assigned_after_construction_keeps_the_code(
        self, field: str, bad: object,
    ) -> None:
        err = TaskError(error_code='RATE_LIMITED', data={'a': 1})
        setattr(err, field, bad)

        kept = _stored(reportable_error(err, 'task'))

        assert kept.error_code == 'RATE_LIMITED'
        assert isinstance(kept.data, dict) and 'data_refused' in kept.data

    def test_a_good_message_stays_when_the_exception_is_dropped(self) -> None:
        err = TaskError(error_code='RATE_LIMITED', message='slow down')
        err.exception = 5  # pyright: ignore[reportAttributeAccessIssue]

        kept = _stored(reportable_error(err, 'task'))

        assert kept.message == 'slow down'
        assert kept.exception is None

    def test_a_good_exception_stays_when_the_data_is_bad(self) -> None:
        err = TaskError(
            error_code='RATE_LIMITED',
            exception=ValueError('boom'),
            data=cast('Any', object()),
        )
        err.message = 5  # pyright: ignore[reportAttributeAccessIssue]

        kept = _stored(reportable_error(err, 'task'))

        assert isinstance(kept.exception, dict)
        assert kept.message is None

    def test_a_built_in_code_stays_when_a_field_is_bad(self) -> None:
        err = TaskError(error_code=OperationalErrorCode.BROKER_ERROR)
        err.message = 5  # pyright: ignore[reportAttributeAccessIssue]

        assert _stored(reportable_error(err, 'task')).error_code is OperationalErrorCode.BROKER_ERROR

    def test_a_lone_surrogate_with_a_damaged_exception_keeps_the_code(self) -> None:
        err = TaskError(error_code='RATE_LIMITED', message='bad \ud800')
        err.exception = _RaisingDict({'type': 'V'})  # pyright: ignore[reportAttributeAccessIssue]

        kept = _stored(reportable_error(err, 'task'))

        assert kept.error_code == 'RATE_LIMITED'
        assert kept.message is None

    @pytest.mark.parametrize('code', [5, True, None, 'BROKER_ERROR'])
    def test_a_code_that_cannot_be_kept_is_a_catalog_error(self, code: object) -> None:
        err = TaskError(error_code='RATE_LIMITED')
        err.error_code = cast('Any', code)

        with pytest.raises(ValueValidationError, match='error_code'):
            reportable_error(err, 'task')

    def test_a_missing_code_is_a_catalog_error(self) -> None:
        err = TaskError.model_construct(message='no code')

        with pytest.raises(ValueValidationError, match='NoneType'):
            reportable_error(err, 'task')

    def test_a_good_error_logs_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level('ERROR', logger='horsies.codec'):
            encode_task_error(reportable_error(TaskError(error_code='X', data={'a': 1})))

        assert caplog.records == []

    def test_an_error_slot_decodes_with_the_helper(self) -> None:
        wire = encode_task_error(TaskError(error_code='X', data={'a': [1]}))

        assert decode_task_error(wire).data == {'a': [1]}


@pytest.mark.unit
def test_json_text_of_a_refused_error_is_valid() -> None:
    wire = encode_task_result(
        TaskResult(err=TaskError(error_code='X', data=_Opaque())),
        type(None),
        'task',
    )

    assert json.loads(json.dumps(wire))['err']['error_code'] == 'X'
