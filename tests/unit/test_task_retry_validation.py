"""Unit tests for strict retry/error-code validation in task decorators."""

from __future__ import annotations

import json
import pytest

from horsies.core.app import Horsies
from pydantic import ValidationError
from horsies.core.errors import TaskDefinitionError, ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult, RetryPolicy


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://user:pass@localhost/db',
            ),
        )
    )


@pytest.mark.unit
class TestRetryPolicyValidation:
    """Tests for RetryPolicy model-level validation."""

    def test_exception_name_rejected_in_auto_retry_for(self) -> None:
        """RetryPolicy rejects exception class names in auto_retry_for."""
        with pytest.raises(ValueError, match='auto_retry_for'):
            RetryPolicy.fixed([5], auto_retry_for=['TimeoutError'])

    def test_empty_auto_retry_for_rejected(self) -> None:
        """RetryPolicy rejects empty auto_retry_for list."""
        with pytest.raises(ValueError, match='at least 1 item'):
            RetryPolicy.fixed([5], auto_retry_for=[])

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
            def deprecated_auto_retry_for() -> TaskResult[str, TaskError]:
                return TaskResult(ok='ok')

        assert exc_info.value.code == ErrorCode.TASK_INVALID_OPTIONS

    def test_unknown_task_option_rejected(self) -> None:
        """Unknown task option is rejected with TASK_INVALID_OPTIONS."""
        app = _make_app()
        with pytest.raises(TaskDefinitionError) as exc_info:

            @app.task('bad_option', queue_nam='default')  # type: ignore[call-arg]
            def bad_option() -> TaskResult[str, TaskError]:
                return TaskResult(ok='ok')

        assert exc_info.value.code == ErrorCode.TASK_INVALID_OPTIONS
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
            def bad_default_error_code() -> TaskResult[str, TaskError]:
                return TaskResult(ok='ok')

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER

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
            def bad_mapper_type() -> TaskResult[str, TaskError]:
                return TaskResult(ok='ok')

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER


@pytest.mark.unit
class TestCheckRuntimePolicyValidation:
    """Tests for app.check(live=False) runtime policy validation."""

    # --- task_options_json shape validation ---

    def test_rejects_malformed_task_options_json(self) -> None:
        """Malformed JSON in task_options_json is rejected."""
        app = _make_app()

        @app.task('malformed_task_options')
        def malformed_task_options() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(app.tasks['malformed_task_options'], 'task_options_json', '{')

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'invalid task_options_json' in e.message
            for e in errors
        )

    def test_rejects_non_object_task_options_json(self) -> None:
        """Non-object task_options_json is rejected."""
        app = _make_app()

        @app.task('non_object_task_options')
        def non_object_task_options() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['non_object_task_options'],
            'task_options_json',
            json.dumps(['not-an-object']),
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'task_options_json must be an object' in e.message
            for e in errors
        )

    # --- retry_policy shape validation ---

    def test_rejects_non_dict_retry_policy(self) -> None:
        """Non-dict retry_policy is rejected."""
        app = _make_app()

        @app.task('bad_retry_policy_shape')
        def bad_retry_policy_shape() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['bad_retry_policy_shape'],
            'task_options_json',
            json.dumps({'retry_policy': 'bad-shape'}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'retry_policy must be an object' in e.message
            for e in errors
        )

    # --- auto_retry_for field validation ---

    def test_rejects_missing_auto_retry_for(self) -> None:
        """Missing auto_retry_for in retry_policy is rejected."""
        app = _make_app()

        @app.task('missing_auto_retry_for')
        def missing_auto_retry_for() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

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
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'retry_policy.auto_retry_for is required' in e.message
            for e in errors
        )

    def test_rejects_non_list_auto_retry_for(self) -> None:
        """Non-list auto_retry_for is rejected."""
        app = _make_app()

        @app.task('bad_auto_retry_for_shape')
        def bad_auto_retry_for_shape() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['bad_auto_retry_for_shape'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': 'NOT_A_LIST'}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'auto_retry_for must be a list' in e.message
            for e in errors
        )

    def test_rejects_empty_auto_retry_for(self) -> None:
        """Empty auto_retry_for is rejected."""
        app = _make_app()

        @app.task('empty_auto_retry_for')
        def empty_auto_retry_for() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['empty_auto_retry_for'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': []}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'auto_retry_for must not be empty' in e.message
            for e in errors
        )

    # --- auto_retry_for entry validation ---

    def test_rejects_non_string_auto_retry_entries(self) -> None:
        """Non-string entries in auto_retry_for are rejected."""
        app = _make_app()

        @app.task('non_string_auto_retry')
        def non_string_auto_retry() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['non_string_auto_retry'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': ['RETRY_ME', 123]}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'auto_retry_for entries must be strings' in e.message
            for e in errors
        )

    def test_rejects_exception_name_in_auto_retry_for(self) -> None:
        """Exception class names in auto_retry_for are rejected at check time."""
        app = _make_app()

        @app.task('exc_name_retry')
        def exc_name_retry() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['exc_name_retry'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': ['TimeoutError']}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'looks like an exception class name' in e.message
            for e in errors
        )

    def test_rejects_invalid_format_in_auto_retry_for(self) -> None:
        """Non-UPPER_SNAKE_CASE entries in auto_retry_for are rejected at check time."""
        app = _make_app()

        @app.task('bad_format_retry')
        def bad_format_retry() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['bad_format_retry'],
            'task_options_json',
            json.dumps({'retry_policy': {'auto_retry_for': ['lower_case']}}),
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.TASK_INVALID_OPTIONS
            and 'is invalid' in e.message
            for e in errors
        )

    # --- model_validate failure ---

    def test_rejects_structurally_invalid_retry_policy(self) -> None:
        """Valid entries but structurally wrong policy fails model_validate."""
        app = _make_app()

        @app.task('bad_policy_structure')
        def bad_policy_structure() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

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
            e.code == ErrorCode.TASK_INVALID_OPTIONS
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
            e.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER
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
            e.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER
            and 'looks like an exception class name' in e.message
            for e in errors
        )

    # --- per-task default_unhandled_error_code validation ---

    def test_rejects_invalid_per_task_default_error_code(self) -> None:
        """check() catches invalid per-task default_unhandled_error_code."""
        app = _make_app()

        @app.task('bad_task_default')
        def bad_task_default() -> TaskResult[str, TaskError]:
            return TaskResult(ok='ok')

        setattr(
            app.tasks['bad_task_default'],
            'default_unhandled_error_code',
            'TimeoutError',
        )

        errors = app.check(live=False)

        assert any(
            e.code == ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER
            and 'looks like an exception class name' in e.message
            for e in errors
        )
