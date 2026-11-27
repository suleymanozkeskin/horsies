"""Unit tests for strict retry/error-code validation in task decorators."""

from __future__ import annotations

import json
from enum import Enum
from functools import wraps
import pytest

from horsies.core.app import Horsies
from horsies.core.codec.task_options import serialize_task_options
from pydantic import ValidationError
from horsies.core.errors import (
    HorsiesErrorCode,
    MultipleValidationErrors,
    TaskDefinitionError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    OperationalCode,
    OutcomeCode,
    RetryPolicy,
    TaskError,
    TaskOptions,
)
from horsies.core.types.result import Ok, Result


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url='postgresql://user:pass@localhost/db',
            ),
        )
    )


class _TaskCode(str, Enum):
    """The codes that the tasks of these tests declare."""

    TRANSIENT = 'TRANSIENT'
    RATE_LIMITED = 'RATE_LIMITED'


class _OtherCode(str, Enum):
    """Codes of an Enum class that no task here declares."""

    RATE_LIMITED = 'RATE_LIMITED'


@pytest.mark.unit
class TestRetryPolicyValidation:
    """Tests for RetryPolicy model-level validation."""

    def test_empty_auto_retry_for_rejected(self) -> None:
        """RetryPolicy rejects empty auto_retry_for list."""
        with pytest.raises(ValueError, match='at least 1 item'):
            RetryPolicy.fixed([5], auto_retry_for=[])

    def test_exponential_accepts_max_delay_seconds(self) -> None:
        """RetryPolicy supports an opt-in maximum delay for exponential backoff."""
        policy = RetryPolicy.exponential(
            base_seconds=60,
            max_retries=5,
            max_delay_seconds=600,
            auto_retry_for=[_TaskCode.TRANSIENT],
            jitter=False,
        )

        assert policy.max_delay_seconds == 600
        assert policy.model_dump(mode='json')['max_delay_seconds'] == 600

    def test_max_delay_seconds_must_be_positive(self) -> None:
        """RetryPolicy rejects non-positive max_delay_seconds values."""
        with pytest.raises(ValueError, match='greater than 0'):
            RetryPolicy.exponential(
                base_seconds=60,
                max_retries=5,
                max_delay_seconds=0,
                auto_retry_for=[_TaskCode.TRANSIENT],
            )

    def test_max_delay_seconds_serializes_only_when_set(self) -> None:
        """TaskOptions serialization omits a null max delay but preserves an explicit cap."""
        uncapped = RetryPolicy.exponential(
            base_seconds=60,
            max_retries=5,
            auto_retry_for=[_TaskCode.TRANSIENT],
        )
        capped = RetryPolicy.exponential(
            base_seconds=60,
            max_retries=5,
            max_delay_seconds=600,
            auto_retry_for=[_TaskCode.TRANSIENT],
        )

        uncapped_json = serialize_task_options(
            TaskOptions(task_name='uncapped', retry_policy=uncapped)
        )
        capped_json = serialize_task_options(
            TaskOptions(task_name='capped', retry_policy=capped)
        )

        assert uncapped_json.is_ok()
        assert capped_json.is_ok()
        uncapped_payload = json.loads(uncapped_json.ok_value)
        capped_payload = json.loads(capped_json.ok_value)
        assert 'max_delay_seconds' not in uncapped_payload['retry_policy']
        assert capped_payload['retry_policy']['max_delay_seconds'] == 600

    def test_appconfig_rejects_mutation_after_construction(self) -> None:
        """AppConfig frozen=True rejects attribute assignment."""
        app = _make_app()
        with pytest.raises(ValidationError):
            app.config.exception_mapper = None  # type: ignore[assignment]


@pytest.mark.unit
class TestTaskDecoratorValidation:
    """Tests for @app.task() definition-time validation."""

    def test_top_level_auto_retry_for_rejected(self) -> None:
        """Top-level auto_retry_for is rejected with TASK_INVALID_OPTIONS."""
        app = _make_app()
        with pytest.raises(TaskDefinitionError, match='auto_retry_for') as exc_info:

            @app.task(
                'deprecated_auto_retry_for',
                auto_retry_for=['RATE_LIMITED'],  # type: ignore[call-arg]
            )
            def deprecated_auto_retry_for() -> Result[str, TaskError]:
                return Ok('ok')

        assert exc_info.value.code == HorsiesErrorCode.TASK_INVALID_OPTIONS

    def test_unknown_task_option_rejected(self) -> None:
        """Unknown task option is rejected with TASK_INVALID_OPTIONS."""
        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:

            @app.task('bad_option', queue_nam='default')  # type: ignore[call-arg]
            def bad_option() -> Result[str, TaskError]:
                return Ok('ok')

        assert exc_info.value.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
        assert 'bad_option' not in app.list_tasks()

    def test_default_unhandled_error_code_exception_name_rejected(self) -> None:
        """Exception class name as default_unhandled_error_code is rejected."""
        app = _make_app()
        with pytest.raises(
            TaskDefinitionError, match='default_unhandled_error_code',
        ) as exc_info:

            @app.task(
                'bad_default_error_code',
                default_unhandled_error_code='TimeoutError',
            )
            def bad_default_error_code() -> Result[str, TaskError]:
                return Ok('ok')

        assert exc_info.value.code == HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_exception_mapper_must_be_mapping(self) -> None:
        """Non-mapping exception_mapper is rejected with CONFIG_INVALID_EXCEPTION_MAPPER."""
        app = _make_app()
        with pytest.raises(
            TaskDefinitionError, match='exception_mapper',
        ) as exc_info:

            @app.task(
                'bad_mapper_type',
                exception_mapper=['not', 'a', 'mapping'],  # type: ignore[arg-type]
            )
            def bad_mapper_type() -> Result[str, TaskError]:
                return Ok('ok')

        assert exc_info.value.code == HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

    def test_inner_decorator_with_wraps_rejected(self) -> None:
        """Task pre-decorated with @wraps wrapper is rejected."""
        app = _make_app()

        def passthrough(fn):  # type: ignore[no-untyped-def]
            @wraps(fn)
            def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
                return fn(*args, **kwargs)

            return wrapper

        with pytest.raises(TaskDefinitionError) as exc_info:

            @app.task('predecorated_with_wraps')
            @passthrough
            def predecorated_with_wraps() -> Result[str, TaskError]:
                return Ok('ok')

        assert exc_info.value.code == HorsiesErrorCode.TASK_PREDECORATED_NOT_SUPPORTED

    def test_async_task_function_registers_with_its_options(self) -> None:
        """`@app.task()` registers an `async def` function like a `def` one."""
        app = _make_app()

        @app.task('async_task_function', timeout_ms=5_000)
        async def async_task_function() -> Result[str, TaskError]:
            return Ok('ok')

        assert app.tasks['async_task_function'] is async_task_function
        options_json = async_task_function.task_options_json
        assert options_json is not None
        assert json.loads(options_json)['timeout_ms'] == 5_000

    def test_inner_decorator_without_wraps_missing_return_type_rejected(self) -> None:
        """A no-wraps wrapper is rejected if it no longer satisfies the task contract."""
        app = _make_app()

        def passthrough(fn):  # type: ignore[no-untyped-def]
            def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
                return fn(*args, **kwargs)

            return wrapper

        with pytest.raises(TaskDefinitionError) as exc_info:

            @app.task('predecorated_no_wraps')
            @passthrough
            def predecorated_no_wraps() -> Result[str, TaskError]:
                return Ok('ok')

        assert exc_info.value.code == HorsiesErrorCode.TASK_NO_RETURN_TYPE


@pytest.mark.unit
class TestRetryCodeMembers:
    """`@app.task()` takes code members in `auto_retry_for`: members of the
    built-in code enums and of the `Enum` class of `error_codes=`."""

    def test_built_in_and_declared_members_register(self) -> None:
        """A built-in member and a declared member register; the stored
        policy holds their texts, the texts that a failure with each code
        stores."""
        app = _make_app()

        @app.task(
            'retries_on_members',
            error_codes=_TaskCode,
            retry_policy=RetryPolicy.fixed(
                [1, 2],
                auto_retry_for=[OperationalCode.WORKER_CRASHED, _TaskCode.RATE_LIMITED],
            ),
        )
        def retries_on_members() -> Result[str, TaskError]:
            return Ok('ok')

        options_json = retries_on_members.task_options_json
        assert options_json is not None
        stored = json.loads(options_json)['retry_policy']['auto_retry_for']
        assert stored == ['WORKER_CRASHED', 'RATE_LIMITED']
        failure = TaskError(error_code=_TaskCode.RATE_LIMITED).model_dump(mode='json')
        assert failure['error_code'] == stored[1]

    def test_plain_text_entry_refused(self) -> None:
        """A plain `str` entry is TASK_INVALID_OPTIONS; the notes name the
        task and the entry, and nothing is registered."""
        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:

            @app.task(
                'plain_text_retry',
                retry_policy=RetryPolicy.fixed([1], auto_retry_for=['BROKR_ERROR']),  # pyright: ignore[reportArgumentType]
            )
            def plain_text_retry() -> Result[str, TaskError]:
                return Ok('ok')

        error = exc_info.value
        assert error.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
        assert error.message == 'auto_retry_for[0] is not a code member'
        assert error.notes == [
            "task 'plain_text_retry'",
            "auto_retry_for[0]='BROKR_ERROR' is a str, not a code member",
            'the task declares no Enum class with error_codes=',
        ]
        assert 'plain_text_retry' not in app.list_tasks()

    def test_member_of_another_enum_refused(self) -> None:
        """A member of an Enum class that the task does not declare is
        TASK_INVALID_OPTIONS, also when its value is a declared code."""
        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:

            @app.task(
                'other_enum_retry',
                error_codes=_TaskCode,
                retry_policy=RetryPolicy.fixed([1], auto_retry_for=[_OtherCode.RATE_LIMITED]),
            )
            def other_enum_retry() -> Result[str, TaskError]:
                return Ok('ok')

        error = exc_info.value
        assert error.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
        assert error.message == 'auto_retry_for[0] is not a code of the task'
        assert error.notes == [
            "task 'other_enum_retry'",
            'auto_retry_for[0]=_OtherCode.RATE_LIMITED is neither a built-in '
            'code nor a member of the declared Enum class',
            'the task declares error_codes=_TaskCode',
        ]
        assert 'other_enum_retry' not in app.list_tasks()

    def test_custom_member_without_an_enum_declaration_refused(self) -> None:
        """A custom member of a task that declares no Enum class is
        TASK_INVALID_OPTIONS: a tuple declaration gives no code members."""
        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:

            @app.task(
                'tuple_declared_retry',
                error_codes=('RATE_LIMITED',),
                retry_policy=RetryPolicy.fixed([1], auto_retry_for=[_TaskCode.RATE_LIMITED]),
            )
            def tuple_declared_retry() -> Result[str, TaskError]:
                return Ok('ok')

        error = exc_info.value
        assert error.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
        assert error.notes[2] == 'the task declares no Enum class with error_codes='

    def test_every_refused_entry_is_reported(self) -> None:
        """Two refused entries give one error each, in list order, in a
        MultipleValidationErrors."""
        app = _make_app()
        with pytest.raises(MultipleValidationErrors) as exc_info:

            @app.task(
                'two_refused_entries',
                retry_policy=RetryPolicy.fixed(
                    [1],
                    auto_retry_for=['TRANSIENT', OutcomeCode.TASK_TIMEOUT, _OtherCode.RATE_LIMITED],  # pyright: ignore[reportArgumentType]
                ),
            )
            def two_refused_entries() -> Result[str, TaskError]:
                return Ok('ok')

        messages = [error.message for error in exc_info.value.report.errors]
        assert messages == [
            'auto_retry_for[0] is not a code member',
            'auto_retry_for[2] is not a code of the task',
        ]
        assert 'two_refused_entries' not in app.list_tasks()


@pytest.mark.unit
class TestCheckRuntimePolicyValidation:
    """Tests for app.check(live=False) runtime policy validation."""

    # --- task_options_json shape validation ---

    def test_rejects_malformed_task_options_json(self) -> None:
        """Malformed JSON in task_options_json is rejected."""
        app = _make_app()

        @app.task('malformed_task_options')
        def malformed_task_options() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(app.tasks['malformed_task_options'], 'task_options_json', '{')

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'invalid task_options_json' in e.message
            for e in errors
        )

    def test_rejects_non_object_task_options_json(self) -> None:
        """Non-object task_options_json is rejected."""
        app = _make_app()

        @app.task('non_object_task_options')
        def non_object_task_options() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['non_object_task_options'],
            'task_options_json',
            json.dumps(['not-an-object']),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'task_options_json must be an object' in e.message
            for e in errors
        )

    # --- retry_policy shape validation ---

    def test_rejects_non_dict_retry_policy(self) -> None:
        """Non-dict retry_policy is rejected."""
        app = _make_app()

        @app.task('bad_retry_policy_shape')
        def bad_retry_policy_shape() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['bad_retry_policy_shape'],
            'task_options_json',
            json.dumps({'retry_policy': 'bad-shape'}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'retry_policy must be an object' in e.message
            for e in errors
        )

    # --- auto_retry_for field validation ---

    def test_rejects_missing_auto_retry_for(self) -> None:
        """Missing auto_retry_for in retry_policy is rejected."""
        app = _make_app()

        @app.task('missing_auto_retry_for')
        def missing_auto_retry_for() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['missing_auto_retry_for'],
            'task_options_json',
            json.dumps({
                'retry_policy': {
                    'max_retries': 1,
                    'intervals': [1],
                    'backoff_strategy': 'fixed',
                },
            }),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'retry_policy.auto_retry_for is required' in e.message
            for e in errors
        )

    def test_rejects_non_list_auto_retry_for(self) -> None:
        """Non-list auto_retry_for is rejected."""
        app = _make_app()

        @app.task('bad_auto_retry_for_shape')
        def bad_auto_retry_for_shape() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['bad_auto_retry_for_shape'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': 'NOT_A_LIST'}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'auto_retry_for must be a list' in e.message
            for e in errors
        )

    def test_rejects_empty_auto_retry_for(self) -> None:
        """Empty auto_retry_for is rejected."""
        app = _make_app()

        @app.task('empty_auto_retry_for')
        def empty_auto_retry_for() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['empty_auto_retry_for'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': []}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'auto_retry_for must not be empty' in e.message
            for e in errors
        )

    # --- auto_retry_for entry validation ---

    def test_rejects_non_string_auto_retry_entries(self) -> None:
        """Non-string entries in auto_retry_for are rejected."""
        app = _make_app()

        @app.task('non_string_auto_retry')
        def non_string_auto_retry() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['non_string_auto_retry'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': ['RETRY_ME', 123]}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'auto_retry_for entries must be strings' in e.message
            for e in errors
        )

    def test_rejects_exception_name_in_auto_retry_for(self) -> None:
        """Exception class names in auto_retry_for are rejected at check time."""
        app = _make_app()

        @app.task('exc_name_retry')
        def exc_name_retry() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['exc_name_retry'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': ['TimeoutError']}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'looks like an exception class name' in e.message
            for e in errors
        )

    def test_rejects_invalid_format_in_auto_retry_for(self) -> None:
        """Non-UPPER_SNAKE_CASE entries in auto_retry_for are rejected at check time."""
        app = _make_app()

        @app.task('bad_format_retry')
        def bad_format_retry() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['bad_format_retry'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': ['lower_case']}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'is invalid' in e.message
            for e in errors
        )

    # --- model_validate failure ---

    def test_rejects_structurally_invalid_retry_policy(self) -> None:
        """Valid entries but structurally wrong policy fails model_validate."""
        app = _make_app()

        @app.task('bad_policy_structure')
        def bad_policy_structure() -> Result[str, TaskError]:
            return Ok('ok')

        # intervals length (1) != max_retries (3) for fixed strategy
        setattr(
            app.tasks['bad_policy_structure'],
            'task_options_json',
            json.dumps({
                'retry_policy': {
                    'max_retries': 3,
                    'intervals': [5],
                    'backoff_strategy': 'fixed',
                    'auto_retry_for': ['SOME_CODE'],
                },
            }),
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.TASK_INVALID_OPTIONS
            and 'invalid retry_policy metadata' in e.message
            for e in errors
        )

    # --- global config re-validation (defensive) ---

    def test_rejects_mutated_global_exception_mapper(self) -> None:
        """check() catches corrupted global exception_mapper via object.__setattr__."""
        app = _make_app()
        # Bypass Pydantic frozen guard to simulate corruption
        object.__setattr__(app.config, 'exception_mapper', 'not_a_mapping')

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER
            and 'exception_mapper must be a mapping' in e.message
            for e in errors
        )

    def test_rejects_mutated_global_default_error_code(self) -> None:
        """check() catches corrupted global default_unhandled_error_code."""
        app = _make_app()
        # Bypass Pydantic frozen guard to simulate corruption
        object.__setattr__(
            app.config, 'default_unhandled_error_code', 'TimeoutError',
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER
            and 'looks like an exception class name' in e.message
            for e in errors
        )

    # --- per-task default_unhandled_error_code validation ---

    def test_rejects_invalid_per_task_default_error_code(self) -> None:
        """check() catches invalid per-task default_unhandled_error_code."""
        app = _make_app()

        @app.task('bad_task_default')
        def bad_task_default() -> Result[str, TaskError]:
            return Ok('ok')

        setattr(
            app.tasks['bad_task_default'],
            'default_unhandled_error_code',
            'TimeoutError',
        )

        errors = app.check(live=False)

        assert any(
            e.code == HorsiesErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER
            and 'looks like an exception class name' in e.message
            for e in errors
        )
