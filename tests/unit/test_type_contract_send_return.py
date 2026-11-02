"""The type contract at send, at return, in workflow wiring and at the receiver.

- `.send()` refuses a call that cannot run.
- A return that is not a `TaskResult` is a named refusal, not an exception.
- `Annotated` metadata on the return type holds at run time.
- Wiring compares types only, at every depth.
- A receiver of `args_from` gets the type that it declares.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, ForwardRef, Literal, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from horsies import AppConfig, Horsies, PostgresConfig, TaskError, TaskResult
from horsies.core.codec.kwargs import (
    MissingKwargs,
    declared_taskresult_ok_type,
    missing_required_kwargs,
)
from horsies.core.codec.signature_check import _DOCS_URL
from horsies.core.codec.typed import conform_to_target, encode_task_result
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.errors import ErrorCode, TaskDefinitionError
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendValidationFailed
from horsies.core.models.tasks import ContractCode, OperationalErrorCode
from horsies.core.models.workflow import TaskNode, WorkflowContext, WorkflowSpec
from horsies.core.models.workflow.typing_utils import _is_ok_type_compatible
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.worker import child_runner

Positive = Annotated[int, Field(gt=0)]
Upper = Annotated[str, AfterValidator(str.upper)]


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


def _real_app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://user:password@localhost/unused',
            ),
        ),
    )


@pytest.mark.unit
class TestMissingRequiredKwargs:
    def test_a_missing_parameter_is_refused_with_nothing_sent(self) -> None:
        def charge(*, amount: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=amount)

        app = _mock_app()
        result = create_task_wrapper(charge, app, 'test.charge').send()

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        assert result.err_value.exception == NoCause()
        assert 'amount' in result.err_value.message
        app.get_broker.assert_not_called()

    def test_a_partial_call_is_refused(self) -> None:
        def charge(*, amount: int, currency: str) -> TaskResult[int, TaskError]:
            return TaskResult(ok=amount)

        app = _mock_app()
        result = create_task_wrapper(charge, app, 'test.charge').send(amount=1)

        assert is_err(result)
        assert 'currency' in result.err_value.message
        assert 'amount' not in result.err_value.message.split('[')[1]
        app.get_broker.assert_not_called()

    def test_a_parameter_with_a_default_may_be_omitted(self) -> None:
        def charge(*, amount: int = 1) -> TaskResult[int, TaskError]:
            return TaskResult(ok=amount)

        assert missing_required_kwargs(charge, ()) == MissingKwargs((), ())

    def test_a_required_workflow_ctx_needs_a_workflow(self) -> None:
        def step(*, workflow_ctx: WorkflowContext) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        app = _mock_app()
        result = create_task_wrapper(step, app, 'test.step').send()

        assert is_err(result)
        assert 'workflow_ctx' in result.err_value.message
        assert 'workflow node' in result.err_value.message
        app.get_broker.assert_not_called()

    def test_a_required_args_from_slot_needs_a_workflow(self) -> None:
        def step(
            *,
            value: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            return value

        assert missing_required_kwargs(step, ()) == MissingKwargs((), ('value',))

    def test_an_optional_args_from_slot_may_be_omitted(self) -> None:
        def step(
            *,
            value: TaskResult[int, TaskError] | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        assert missing_required_kwargs(step, ()) == MissingKwargs((), ())

    def test_a_callable_with_no_hints_passes(self) -> None:
        assert missing_required_kwargs(lambda value: value, ()) == MissingKwargs((), ())


class _NotAResult:
    pass


@pytest.mark.unit
class TestReturnThatIsNotATaskResult:
    @pytest.mark.parametrize(
        ('returned', 'name'),
        [
            (5, 'int'),
            ('secret-marker', 'str'),
            ({'ok': 1}, 'dict'),
            (None, 'None'),
            (TaskResult, 'type'),
            (MagicMock(), 'MagicMock'),
        ],
        ids=['int', 'str', 'dict', 'none', 'the-class', 'mock'],
    )
    def test_each_wrong_return_is_a_named_refusal(
        self,
        returned: object,
        name: str,
    ) -> None:
        def task() -> TaskResult[int, TaskError]:
            return returned  # type: ignore[return-value]

        result = create_task_wrapper(task, _mock_app(), 'test.task')()

        assert result.is_err()
        err = result.err_value
        assert err.error_code == ContractCode.RETURN_TYPE_MISMATCH
        assert err.data is not None
        assert err.data['variant'] == 'not_a_task_result'
        assert err.data['actual_type'] == name
        assert err.data['validation_errors'] == []
        assert err.data['validation_error_count'] == 0
        assert 'secret-marker' not in (err.message or '')

    def test_a_returned_task_error_names_its_code(self) -> None:
        def task() -> TaskResult[int, TaskError]:
            return TaskError(error_code='RATE_LIMITED')  # type: ignore[return-value]

        result = create_task_wrapper(task, _mock_app(), 'test.task')()

        assert result.err_value.error_code == ContractCode.RETURN_TYPE_MISMATCH
        assert 'RATE_LIMITED' in (result.err_value.message or '')

    def test_a_copy_of_the_class_is_named_with_its_module(self) -> None:
        copy = type('TaskResult', (), {})

        def task() -> TaskResult[int, TaskError]:
            return copy()  # type: ignore[return-value]

        result = create_task_wrapper(task, _mock_app(), 'test.task')()

        assert result.err_value.data['actual_type'] == f'{__name__}.TaskResult'

    def test_a_task_result_subclass_passes(self) -> None:
        class Kept(TaskResult[int, TaskError]):
            pass

        def task() -> TaskResult[int, TaskError]:
            return Kept(ok=1)

        result = create_task_wrapper(task, _mock_app(), 'test.task')()

        assert result.is_ok()

    def test_an_exception_mapper_does_not_apply(self) -> None:
        app = _mock_app()
        app.config.exception_mapper = {AttributeError: 'MAPPED'}

        def task() -> TaskResult[int, TaskError]:
            return 5  # type: ignore[return-value]

        result = create_task_wrapper(task, app, 'test.task')()

        assert result.err_value.error_code == ContractCode.RETURN_TYPE_MISMATCH

    def test_the_default_unhandled_code_does_not_apply(self) -> None:
        app = _mock_app()
        app.config.default_unhandled_error_code = 'MY_DEFAULT'

        def task() -> TaskResult[int, TaskError]:
            return 5  # type: ignore[return-value]

        result = create_task_wrapper(task, app, 'test.task')()

        assert result.err_value.error_code == ContractCode.RETURN_TYPE_MISMATCH

    def test_an_exception_from_the_body_still_maps(self) -> None:
        app = _mock_app()
        app.config.exception_mapper = {ValueError: 'MAPPED'}

        def task() -> TaskResult[int, TaskError]:
            raise ValueError('boom')

        result = create_task_wrapper(task, app, 'test.task')()

        assert result.err_value.error_code == 'MAPPED'

    def test_a_generator_function_is_refused_at_definition(self) -> None:
        def task() -> TaskResult[int, TaskError]:
            yield 1  # type: ignore[misc]

        with pytest.raises(TaskDefinitionError) as refused:
            create_task_wrapper(task, _mock_app(), 'test.task')

        assert refused.value.code == ErrorCode.TASK_INVALID_RETURN_TYPE


class _KindA(BaseModel):
    kind: Literal['a']
    n: int


class _KindB(BaseModel):
    kind: Literal['b']
    n: int


_Tagged = Annotated[_KindA | _KindB, Field(discriminator='kind')]


@pytest.mark.unit
class TestAnnotatedReturn:
    def test_a_constraint_on_the_return_is_enforced(self) -> None:
        def task(*, n: int) -> TaskResult[Positive, TaskError]:
            return TaskResult(ok=n)

        wrapper = create_task_wrapper(task, _mock_app(), 'test.task')

        assert wrapper(n=5).ok_value == 5
        refused = wrapper(n=-1)
        assert refused.is_err()
        assert refused.err_value.error_code == ContractCode.RETURN_TYPE_MISMATCH

    def test_a_validator_on_the_return_runs(self) -> None:
        def task() -> TaskResult[Upper, TaskError]:
            return TaskResult(ok='abc')

        assert create_task_wrapper(task, _mock_app(), 'test.task')().ok_value == 'ABC'

    def test_the_ok_type_keeps_the_discriminator(self) -> None:
        def task() -> TaskResult[_Tagged, TaskError]:
            return TaskResult(ok=_KindA(kind='a', n=1))

        wrapper = create_task_wrapper(task, _mock_app(), 'test.task')

        assert wrapper.task_ok_type == _Tagged
        envelope = encode_task_result(wrapper(), wrapper.task_ok_type)
        assert envelope['ok'] == {'kind': 'a', 'n': 1}

    def test_annotated_around_task_result_stays_refused(self) -> None:
        def task() -> Annotated[TaskResult[int, TaskError], 'note']:
            return TaskResult(ok=1)

        with pytest.raises(TaskDefinitionError) as refused:
            create_task_wrapper(task, _mock_app(), 'test.task')

        assert refused.value.code == ErrorCode.TASK_INVALID_RETURN_TYPE


@pytest.mark.unit
class TestWiringCompare:
    @pytest.mark.parametrize(
        ('source', 'target'),
        [
            (Positive, int),
            (int, Positive),
            (list[Positive], list[int]),
            (dict[str, Positive], dict[str, int]),
            (Optional[Positive], Optional[int]),
            (tuple[Positive, ...], tuple[int, ...]),
            (_Tagged, _Tagged),
            (_Tagged, _KindA | _KindB),
            (int, Any),
            (int, Optional[int]),
        ],
    )
    def test_compatible_types(self, source: object, target: object) -> None:
        assert _is_ok_type_compatible(source, target) is True

    @pytest.mark.parametrize(
        ('source', 'target'),
        [
            (Positive, str),
            (list[Positive], list[str]),
            (bool, int),
            (bool, Positive),
            (bool, Optional[int]),
            (int, bool),
            (ForwardRef('NotDefinedAnywhere'), int),
        ],
    )
    def test_incompatible_types(self, source: object, target: object) -> None:
        assert _is_ok_type_compatible(source, target) is False

    def test_a_spec_accepts_a_constrained_source(self) -> None:
        app = _real_app()

        @app.task('wiring_source')
        def source() -> TaskResult[Positive, TaskError]:
            return TaskResult(ok=1)

        @app.task('wiring_target')
        def target(*, value: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return value

        first = TaskNode(fn=source)
        second = TaskNode(fn=target, waits_for=[first], args_from={'value': first})

        spec = WorkflowSpec(name='wiring', tasks=[first, second])

        assert len(spec.tasks) == 2


class _Base(BaseModel):
    n: int


class _Sub(_Base):
    secret: str = 's'


class _Closed(BaseModel):
    model_config = ConfigDict(extra='forbid')
    n: int


class _ClosedSub(_Closed):
    secret: str = 's'


@pytest.mark.unit
class TestConformToTarget:
    def test_a_constraint_refuses_the_value(self) -> None:
        match conform_to_target(TaskResult(ok=-1), Positive):
            case Err(err_value=refusal):
                assert isinstance(refusal, ValueValidationError)
                assert '-1' not in str(refusal)
            case Ok():
                pytest.fail('a value outside the constraint passed')

    def test_a_nested_constraint_refuses_the_value(self) -> None:
        assert is_err(conform_to_target(TaskResult(ok=[1, -1]), list[Positive]))

    def test_the_receiver_gets_the_transformed_value(self) -> None:
        result = conform_to_target(TaskResult(ok='abc'), Upper)

        assert is_ok(result)
        assert result.ok_value.ok_value == 'ABC'

    def test_a_subclass_stays_a_subclass(self) -> None:
        result = conform_to_target(TaskResult(ok=_Sub(n=1, secret='keep')), _Base)

        assert is_ok(result)
        assert isinstance(result.ok_value.ok_value, _Sub)

    def test_a_forbid_target_takes_a_subclass_that_the_wiring_accepts(self) -> None:
        assert is_ok(conform_to_target(TaskResult(ok=_ClosedSub(n=1)), _Closed))

    def test_validation_is_strict(self) -> None:
        assert is_err(conform_to_target(TaskResult(ok='7'), int))

    def test_an_error_result_passes_unchanged(self) -> None:
        failed: TaskResult[Any, TaskError] = TaskResult(
            err=TaskError(error_code='FAILED'),
        )

        result = conform_to_target(failed, Positive)

        assert is_ok(result)
        assert result.ok_value is failed


@pytest.mark.unit
class TestDeclaredTaskResultOkType:
    def test_it_keeps_annotated(self) -> None:
        def target(
            *,
            value: TaskResult[Positive, TaskError],
        ) -> TaskResult[int, TaskError]:
            return value

        assert declared_taskresult_ok_type(target, 'value') == Positive

    def test_it_reads_through_none(self) -> None:
        def target(
            *,
            value: TaskResult[Positive, TaskError] | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        assert declared_taskresult_ok_type(target, 'value') == Positive

    def test_a_plain_parameter_has_none(self) -> None:
        def target(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        assert declared_taskresult_ok_type(target, 'value') is None


def _run_child(
    app: Horsies,
    task_name: str,
    source_name: str,
    inner: dict[str, Any],
) -> tuple[bool, str, str | None]:
    kwargs = {
        'x': {
            '__h_taskresult_envelope__': True,
            'source_task_name': source_name,
            'inner': inner,
        },
    }
    with (
        patch.object(
            child_runner,
            '_confirm_ownership_and_set_running',
            MagicMock(return_value=None),
        ),
        patch.object(child_runner, '_start_heartbeat_thread', MagicMock()),
        patch.object(child_runner, '_mark_task_finalizing', MagicMock()),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=app)),
    ):
        return child_runner._run_task_entry(
            task_name=task_name,
            args_json='[]',
            kwargs_json=json.dumps(kwargs),
            task_id='task-id',
            database_url='postgresql://unused/unused',
            master_worker_id='worker',
            runner_heartbeat_interval_ms=100,
            is_workflow_task=True,
        )


@pytest.mark.unit
class TestReceiverOfArgsFrom:
    def _app(self, seen: list[object]) -> Horsies:
        app = _real_app()

        @app.task('source_int')
        def source_int() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('source_str')
        def source_str() -> TaskResult[str, TaskError]:
            return TaskResult(ok='abc')

        @app.task('target_positive')
        def target_positive(
            *,
            x: TaskResult[Positive, TaskError],
        ) -> TaskResult[int, TaskError]:
            seen.append(x.ok_value)
            return TaskResult(ok=1)

        @app.task('target_optional')
        def target_optional(
            *,
            x: TaskResult[Positive, TaskError] | None = None,
        ) -> TaskResult[int, TaskError]:
            seen.append(x.ok_value if x is not None else None)
            return TaskResult(ok=1)

        @app.task('target_upper')
        def target_upper(
            *,
            x: TaskResult[Upper, TaskError],
        ) -> TaskResult[int, TaskError]:
            seen.append(x.ok_value)
            return TaskResult(ok=1)

        @app.task('target_same')
        def target_same(
            *,
            x: TaskResult[int, TaskError],
        ) -> TaskResult[int, TaskError]:
            seen.append(x.ok_value)
            return TaskResult(ok=1)

        return app

    @pytest.mark.parametrize('target', ['target_positive', 'target_optional'])
    def test_a_value_outside_the_target_constraint_stops_the_body(
        self,
        target: str,
    ) -> None:
        seen: list[object] = []
        inner = encode_task_result(TaskResult(ok=-1), int)

        _, payload, reason = _run_child(self._app(seen), target, 'source_int', inner)

        assert seen == []
        assert reason is not None and 'SerializationError' in reason
        code = json.loads(payload)['err']['error_code']
        assert code == {
            '__builtin_task_code__': OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value,
        }

    def test_a_value_inside_the_constraint_runs(self) -> None:
        seen: list[object] = []
        inner = encode_task_result(TaskResult(ok=5), int)

        _run_child(self._app(seen), 'target_positive', 'source_int', inner)

        assert seen == [5]

    def test_a_transforming_target_gets_the_transformed_value(self) -> None:
        seen: list[object] = []
        inner = encode_task_result(TaskResult(ok='abc'), str)

        _run_child(self._app(seen), 'target_upper', 'source_str', inner)

        assert seen == ['ABC']

    def test_equal_types_run_no_second_validation(self) -> None:
        seen: list[object] = []
        inner = encode_task_result(TaskResult(ok=5), int)

        with patch.object(child_runner, 'conform_to_target') as conform:
            _run_child(self._app(seen), 'target_same', 'source_int', inner)

        conform.assert_not_called()
        assert seen == [5]


@pytest.mark.unit
def test_the_docs_link_names_the_serialization_page() -> None:
    assert _DOCS_URL.endswith('/internals/serialization/')
