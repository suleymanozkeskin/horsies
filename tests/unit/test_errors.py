"""Unit tests for Rust-style error formatting."""

from __future__ import annotations

import inspect
import os
import sys
import tempfile
from collections.abc import Iterator
from io import StringIO
from unittest import mock

import pytest

from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    MultipleValidationErrors,
    RegistryError,
    SourceLocation,
    TaskDefinitionError,
    ValidationReport,
    WorkflowValidationError,
    _find_user_frame,
    _horsies_excepthook,
    _should_use_colors,
    _should_show_verbose,
    _should_use_plain_errors,
    install_error_handler,
    raise_collected,
    task_definition_error,
    uninstall_error_handler,
    workflow_validation_error,
)

pytestmark = pytest.mark.unit


# =============================================================================
# SourceLocation Tests
# =============================================================================


class TestSourceLocation:
    """Tests for SourceLocation."""

    def test_format_short_without_column(self) -> None:
        loc = SourceLocation(file='/path/to/file.py', line=42)
        assert loc.format_short() == '/path/to/file.py:42'

    def test_format_short_with_column(self) -> None:
        loc = SourceLocation(file='/path/to/file.py', line=42, column=10)
        assert loc.format_short() == '/path/to/file.py:42:10'

    def test_get_source_line_existing_file(self) -> None:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('line 1\n')
            f.write('line 2\n')
            f.write('line 3\n')
            temp_path = f.name

        try:
            loc = SourceLocation(file=temp_path, line=2)
            assert loc.get_source_line() == 'line 2'
        finally:
            os.unlink(temp_path)

    def test_get_source_line_nonexistent_file(self) -> None:
        loc = SourceLocation(file='/nonexistent/path.py', line=1)
        assert loc.get_source_line() is None

    def test_get_source_line_out_of_range(self) -> None:
        """Line number beyond file length returns None."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('only line\n')
            temp_path = f.name

        try:
            loc = SourceLocation(file=temp_path, line=999)
            assert loc.get_source_line() is None
        finally:
            os.unlink(temp_path)

    def test_from_function(self) -> None:
        def sample_function() -> None:
            pass

        loc = SourceLocation.from_function(sample_function)
        assert loc is not None
        assert loc.file.endswith('test_errors.py')
        assert loc.line > 0

    def test_from_function_returns_none_for_non_function(self) -> None:
        class MockCallable:
            def __call__(self) -> None:
                pass

        loc = SourceLocation.from_function(MockCallable())
        assert loc is None

    def test_from_frame(self) -> None:
        """from_frame extracts file and line from a real frame."""
        frame = inspect.currentframe()
        assert frame is not None
        loc = SourceLocation.from_frame(frame)
        assert loc.file.endswith('test_errors.py')
        assert loc.line > 0
        assert loc.column is None

    def test_end_column_stored(self) -> None:
        """end_column field is stored and accessible."""
        loc = SourceLocation(file='f.py', line=1, column=5, end_column=15)
        assert loc.end_column == 15


# =============================================================================
# HorsiesError Tests
# =============================================================================


class TestHorsiesError:
    """Tests for HorsiesError base class."""

    def test_basic_creation(self) -> None:
        err = HorsiesError(message='something went wrong')
        assert err.message == 'something went wrong'
        assert err.code is None
        assert err.notes == []
        assert err.help_text is None

    def test_creation_with_code(self) -> None:
        err = HorsiesError(
            message='invalid node id',
            code=ErrorCode.WORKFLOW_INVALID_NODE_ID,
        )
        assert err.code == ErrorCode.WORKFLOW_INVALID_NODE_ID

    def test_creation_with_notes_and_help(self) -> None:
        err = HorsiesError(
            message='cycle detected',
            code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
            notes=['task A waits for B', 'task B waits for A'],
            help_text='remove circular dependency',
        )
        assert len(err.notes) == 2
        assert err.help_text == 'remove circular dependency'

    def test_fluent_api(self) -> None:
        err = (
            HorsiesError(message='error')
            .with_note('note 1')
            .with_note('note 2')
            .with_help('help text')
        )
        assert err.notes == ['note 1', 'note 2']
        assert err.help_text == 'help text'

    def test_with_location_fluent(self) -> None:
        loc = SourceLocation(file='test.py', line=10)
        err = HorsiesError(message='err').with_location(loc)
        assert err.location is loc

    def test_with_location_from_frame_fluent(self) -> None:
        frame = inspect.currentframe()
        assert frame is not None
        err = HorsiesError(message='err').with_location_from_frame(frame)
        assert err.location is not None
        assert err.location.file.endswith('test_errors.py')

    def test_auto_location_detection(self) -> None:
        """__post_init__ auto-detects location from call stack when not provided."""
        err = HorsiesError(message='auto-located')
        assert err.location is not None

    def test_is_exception(self) -> None:
        """HorsiesError is a proper Exception subclass."""
        err = HorsiesError(message='exc')
        assert isinstance(err, Exception)

    def test_exception_args_contains_message(self) -> None:
        """Exception.__init__ is called with the message."""
        err = HorsiesError(message='msg')
        assert err.args == ('msg',)

    def test_format_rust_style_basic(self) -> None:
        err = HorsiesError(message='something went wrong')
        formatted = err.format_rust_style(use_colors=False)
        assert 'error: something went wrong' in formatted

    def test_format_rust_style_with_code(self) -> None:
        err = HorsiesError(
            message='cycle detected',
            code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
        )
        formatted = err.format_rust_style(use_colors=False)
        assert 'error[E007]:' in formatted
        assert 'cycle detected' in formatted

    def test_format_rust_style_with_location(self) -> None:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('x = 1\n')
            f.write('raise SomeError()\n')
            f.write('y = 2\n')
            temp_path = f.name

        try:
            err = HorsiesError(
                message='error occurred',
                location=SourceLocation(file=temp_path, line=2),
            )
            formatted = err.format_rust_style(use_colors=False)
            assert '-->' in formatted
            assert temp_path in formatted
            assert 'raise SomeError()' in formatted
            assert '^^^' in formatted  # Underline
        finally:
            os.unlink(temp_path)

    def test_format_rust_style_column_underline(self) -> None:
        """Column-specific underline uses start/end column range."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('some_variable = bad_call()\n')
            temp_path = f.name

        try:
            err = HorsiesError(
                message='bad call',
                location=SourceLocation(
                    file=temp_path, line=1, column=16, end_column=24,
                ),
            )
            formatted = err.format_rust_style(use_colors=False)
            # 24 - 16 = 8 carets
            assert '^^^^^^^^' in formatted
            assert 'some_variable = bad_call()' in formatted
        finally:
            os.unlink(temp_path)

    def test_format_rust_style_column_without_end_column(self) -> None:
        """Column set but end_column is None produces a single caret."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write('x = 1\n')
            temp_path = f.name

        try:
            err = HorsiesError(
                message='err',
                location=SourceLocation(file=temp_path, line=1, column=4),
            )
            formatted = err.format_rust_style(use_colors=False)
            # width = max(1, (4+1) - 4) = 1 → single caret at column 4
            assert '^' in formatted
        finally:
            os.unlink(temp_path)

    def test_format_rust_style_location_without_source_line(self) -> None:
        """Location exists but source file is missing — arrow shown, no snippet."""
        err = HorsiesError(
            message='gone',
            location=SourceLocation(file='/nonexistent/deleted.py', line=10),
        )
        formatted = err.format_rust_style(use_colors=False)
        assert '-->' in formatted
        assert '/nonexistent/deleted.py:10' in formatted
        # No source line or underline should appear
        assert '^^^' not in formatted

    def test_format_rust_style_with_notes(self) -> None:
        err = HorsiesError(
            message='error',
            notes=['first note', 'second note'],
        )
        formatted = err.format_rust_style(use_colors=False)
        assert '= note: first note' in formatted
        assert '= note: second note' in formatted

    def test_format_rust_style_multiline_notes(self) -> None:
        """Multi-line notes get continuation indentation."""
        err = HorsiesError(
            message='error',
            notes=['line 1\nline 2\nline 3'],
        )
        formatted = err.format_rust_style(use_colors=False)
        assert '= note: line 1' in formatted
        assert 'line 2' in formatted
        assert 'line 3' in formatted

    def test_format_rust_style_with_help(self) -> None:
        err = HorsiesError(
            message='error',
            help_text='try doing this instead',
        )
        formatted = err.format_rust_style(use_colors=False)
        assert '= help:' in formatted
        assert 'try doing this instead' in formatted

    def test_format_rust_style_multiline_help(self) -> None:
        err = HorsiesError(
            message='error',
            help_text='line 1\nline 2\nline 3',
        )
        formatted = err.format_rust_style(use_colors=False)
        assert '= help:' in formatted
        assert 'line 1' in formatted
        assert 'line 2' in formatted
        assert 'line 3' in formatted

    def test_format_rust_style_with_colors(self) -> None:
        """use_colors=True emits ANSI escape codes."""
        err = HorsiesError(message='colored error')
        formatted = err.format_rust_style(use_colors=True)
        assert '\033[' in formatted
        assert 'colored error' in formatted

    def test_format_rust_style_default_colors_auto_detects(self) -> None:
        """use_colors=None delegates to _should_use_colors()."""
        err = HorsiesError(message='auto')
        with mock.patch('horsies.core.errors._should_use_colors', return_value=False):
            formatted = err.format_rust_style(use_colors=None)
        assert '\033[' not in formatted

    def test_str_uses_rust_format(self) -> None:
        err = HorsiesError(message='test')
        assert 'error: test' in str(err)


# =============================================================================
# WorkflowValidationError Tests
# =============================================================================


class TestWorkflowValidationError:
    """Tests for WorkflowValidationError."""

    def test_inherits_from_horsies_error(self) -> None:
        err = WorkflowValidationError(message='invalid workflow')
        assert isinstance(err, HorsiesError)
        assert isinstance(err, Exception)

    def test_backward_compatible_creation(self) -> None:
        # Should work with positional message arg
        err = WorkflowValidationError('simple message')
        assert err.message == 'simple message'

    def test_full_error_format(self) -> None:
        err = WorkflowValidationError(
            message='args_from references task not in waits_for',
            code=ErrorCode.WORKFLOW_INVALID_ARGS_FROM,
            notes=[
                "task 'process' args_from['data'] references 'fetch'",
                "'fetch' must be in waits_for to inject its result",
            ],
            help_text="add 'fetch' to waits_for list",
        )
        formatted = err.format_rust_style(use_colors=False)

        assert 'error[E008]:' in formatted
        assert 'args_from references task not in waits_for' in formatted
        assert "= note: task 'process'" in formatted
        assert '= help:' in formatted
        assert "add 'fetch'" in formatted


# =============================================================================
# TaskDefinitionError Tests
# =============================================================================


class TestTaskDefinitionError:
    """Tests for TaskDefinitionError."""

    def test_inherits_from_horsies_error(self) -> None:
        err = TaskDefinitionError(message='invalid task')
        assert isinstance(err, HorsiesError)
        assert isinstance(err, Exception)

    def test_basic_creation(self) -> None:
        err = TaskDefinitionError(
            message='missing return type',
            code=ErrorCode.TASK_NO_RETURN_TYPE,
        )
        assert err.message == 'missing return type'
        assert err.code == ErrorCode.TASK_NO_RETURN_TYPE

    def test_format_rust_style(self) -> None:
        err = TaskDefinitionError(
            message='invalid return type',
            code=ErrorCode.TASK_INVALID_RETURN_TYPE,
        )
        formatted = err.format_rust_style(use_colors=False)
        assert 'error[E101]:' in formatted
        assert 'invalid return type' in formatted


# =============================================================================
# RegistryError Tests
# =============================================================================


class TestRegistryError:
    """Tests for RegistryError."""

    def test_inherits_from_horsies_error(self) -> None:
        err = RegistryError(message='not registered')
        assert isinstance(err, HorsiesError)
        assert isinstance(err, Exception)

    def test_basic_creation(self) -> None:
        err = RegistryError(
            message='task not found',
            code=ErrorCode.TASK_NOT_REGISTERED,
        )
        assert err.message == 'task not found'
        assert err.code == ErrorCode.TASK_NOT_REGISTERED

    def test_format_rust_style(self) -> None:
        err = RegistryError(
            message='duplicate name',
            code=ErrorCode.TASK_DUPLICATE_NAME,
        )
        formatted = err.format_rust_style(use_colors=False)
        assert 'error[E301]:' in formatted
        assert 'duplicate name' in formatted


# =============================================================================
# ConfigurationError Tests
# =============================================================================


class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_inherits_from_horsies_error(self) -> None:
        err = ConfigurationError(message='bad config')
        assert isinstance(err, HorsiesError)
        assert isinstance(err, Exception)

    def test_basic_creation(self) -> None:
        err = ConfigurationError(
            message='invalid broker URL',
            code=ErrorCode.BROKER_INVALID_URL,
        )
        assert err.message == 'invalid broker URL'
        assert err.code == ErrorCode.BROKER_INVALID_URL


# =============================================================================
# Environment Variable Tests
# =============================================================================


class TestEnvironmentVariables:
    """Tests for environment variable handling."""

    def test_should_use_colors_force_enabled(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_FORCE_COLOR': '1'}):
            assert _should_use_colors() is True

    def test_should_use_colors_force_enabled_true_string(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_FORCE_COLOR': 'true'}):
            assert _should_use_colors() is True

    def test_should_use_colors_force_enabled_yes_string(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_FORCE_COLOR': 'yes'}):
            assert _should_use_colors() is True

    def test_should_use_colors_no_color_disables(self) -> None:
        """NO_COLOR disables colors when FORCE_COLOR is not set."""
        with mock.patch.dict(
            os.environ, {'NO_COLOR': '1', 'HORSIES_FORCE_COLOR': ''},
        ):
            assert _should_use_colors() is False

    def test_should_use_colors_force_color_beats_no_color(self) -> None:
        """FORCE_COLOR takes precedence over NO_COLOR (checked first)."""
        with mock.patch.dict(
            os.environ, {'HORSIES_FORCE_COLOR': '1', 'NO_COLOR': '1'},
        ):
            assert _should_use_colors() is True

    def test_should_use_colors_tty_fallback_true(self) -> None:
        """Falls back to stderr.isatty() when no env vars are set."""
        cleaned_env = {
            k: v for k, v in os.environ.items()
            if k not in ('HORSIES_FORCE_COLOR', 'NO_COLOR')
        }
        with mock.patch.dict(os.environ, cleaned_env, clear=True):
            with mock.patch.object(sys.stderr, 'isatty', return_value=True):
                assert _should_use_colors() is True

    def test_should_use_colors_tty_fallback_false(self) -> None:
        """Non-TTY stderr returns False when no env vars override."""
        cleaned_env = {
            k: v for k, v in os.environ.items()
            if k not in ('HORSIES_FORCE_COLOR', 'NO_COLOR')
        }
        with mock.patch.dict(os.environ, cleaned_env, clear=True):
            with mock.patch.object(sys.stderr, 'isatty', return_value=False):
                assert _should_use_colors() is False

    def test_should_show_verbose_enabled(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_VERBOSE': '1'}):
            assert _should_show_verbose() is True

    def test_should_show_verbose_true_string(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_VERBOSE': 'true'}):
            assert _should_show_verbose() is True

    def test_should_show_verbose_yes_string(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_VERBOSE': 'yes'}):
            assert _should_show_verbose() is True

    def test_should_show_verbose_disabled(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_VERBOSE': ''}):
            assert _should_show_verbose() is False

    def test_should_use_plain_errors_enabled(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_PLAIN_ERRORS': '1'}):
            assert _should_use_plain_errors() is True

    def test_should_use_plain_errors_true_string(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_PLAIN_ERRORS': 'true'}):
            assert _should_use_plain_errors() is True

    def test_should_use_plain_errors_yes_string(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_PLAIN_ERRORS': 'yes'}):
            assert _should_use_plain_errors() is True

    def test_should_use_plain_errors_disabled(self) -> None:
        with mock.patch.dict(os.environ, {'HORSIES_PLAIN_ERRORS': ''}):
            assert _should_use_plain_errors() is False


# =============================================================================
# Exception Hook Tests
# =============================================================================


class TestExceptionHook:
    """Tests for custom exception hook."""

    @pytest.fixture(autouse=True)
    def _restore_excepthook(self) -> Iterator[None]:
        """Restore sys.excepthook after each test to prevent state leaks."""
        original = sys.excepthook
        yield
        sys.excepthook = original

    def test_install_and_uninstall(self) -> None:
        from horsies.core.errors import _horsies_excepthook, _original_excepthook

        # Ensure hook is installed
        install_error_handler()
        assert sys.excepthook == _horsies_excepthook

        # Uninstall
        uninstall_error_handler()
        assert sys.excepthook == _original_excepthook

        # Re-install
        install_error_handler()
        assert sys.excepthook == _horsies_excepthook

    def test_horsies_error_prints_to_stderr(self) -> None:
        """Hook prints Rust-style format for HorsiesError to stderr."""
        err = HorsiesError(message='hook test error')
        fake_stderr = StringIO()
        with mock.patch('sys.stderr', fake_stderr):
            with mock.patch.dict(os.environ, {
                'HORSIES_PLAIN_ERRORS': '',
                'HORSIES_VERBOSE': '',
                'HORSIES_FORCE_COLOR': '',
            }):
                _horsies_excepthook(type(err), err, err.__traceback__)
        output = fake_stderr.getvalue()
        assert 'error: hook test error' in output

    def test_non_horsies_error_delegates_to_original(self) -> None:
        """Non-HorsiesError exceptions delegate to _original_excepthook."""
        err = ValueError('not a horsies error')
        with mock.patch('horsies.core.errors._original_excepthook') as mock_hook:
            with mock.patch.dict(os.environ, {'HORSIES_PLAIN_ERRORS': ''}):
                _horsies_excepthook(type(err), err, err.__traceback__)
            mock_hook.assert_called_once_with(type(err), err, err.__traceback__)

    def test_plain_errors_bypasses_custom_formatting(self) -> None:
        """HORSIES_PLAIN_ERRORS=1 delegates to original hook for all errors."""
        err = HorsiesError(message='plain mode')
        with mock.patch('horsies.core.errors._original_excepthook') as mock_hook:
            with mock.patch.dict(os.environ, {'HORSIES_PLAIN_ERRORS': '1'}):
                _horsies_excepthook(type(err), err, err.__traceback__)
            mock_hook.assert_called_once_with(type(err), err, err.__traceback__)

    def test_verbose_mode_shows_traceback(self) -> None:
        """HORSIES_VERBOSE=1 appends full traceback after Rust-style output."""
        err = HorsiesError(message='verbose test')
        fake_stderr = StringIO()
        with mock.patch('sys.stderr', fake_stderr):
            with mock.patch.dict(os.environ, {
                'HORSIES_VERBOSE': '1',
                'HORSIES_PLAIN_ERRORS': '',
                'HORSIES_FORCE_COLOR': '',
            }):
                _horsies_excepthook(type(err), err, err.__traceback__)
        output = fake_stderr.getvalue()
        assert 'error: verbose test' in output
        assert 'Full traceback (HORSIES_VERBOSE=1):' in output


# =============================================================================
# Error Code Tests
# =============================================================================


class TestErrorCode:
    """Tests for ErrorCode enum."""

    def test_workflow_error_codes_in_range(self) -> None:
        workflow_codes = [
            ErrorCode.WORKFLOW_NO_NAME,
            ErrorCode.WORKFLOW_NO_NODES,
            ErrorCode.WORKFLOW_INVALID_NODE_ID,
            ErrorCode.WORKFLOW_DUPLICATE_NODE_ID,
            ErrorCode.WORKFLOW_NO_ROOT_TASKS,
            ErrorCode.WORKFLOW_INVALID_DEPENDENCY,
            ErrorCode.WORKFLOW_CYCLE_DETECTED,
            ErrorCode.WORKFLOW_INVALID_ARGS_FROM,
            ErrorCode.WORKFLOW_INVALID_CTX_FROM,
            ErrorCode.WORKFLOW_CTX_PARAM_MISSING,
            ErrorCode.WORKFLOW_INVALID_OUTPUT,
            ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY,
            ErrorCode.WORKFLOW_INVALID_JOIN,
            ErrorCode.WORKFLOW_UNRESOLVED_QUEUE,
            ErrorCode.WORKFLOW_UNRESOLVED_PRIORITY,
            ErrorCode.WORKFLOW_ARGS_WITH_INJECTION,
            ErrorCode.WORKFLOW_INVALID_SUBWORKFLOW_RETRY_MODE,
            ErrorCode.WORKFLOW_SUBWORKFLOW_APP_MISSING,
            ErrorCode.WORKFLOW_INVALID_KWARG_KEY,
            ErrorCode.WORKFLOW_MISSING_REQUIRED_PARAMS,
            ErrorCode.WORKFLOW_KWARGS_ARGS_FROM_OVERLAP,
            ErrorCode.WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH,
            ErrorCode.WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING,
            ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH,
            ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH,
            ErrorCode.WORKFLOW_EXCESS_POSITIONAL_ARGS,
            ErrorCode.WORKFLOW_CHECK_CASES_REQUIRED,
            ErrorCode.WORKFLOW_CHECK_CASE_INVALID,
            ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
            ErrorCode.WORKFLOW_CHECK_UNDECORATED_BUILDER,
        ]
        for code in workflow_codes:
            # E001-E099 range
            num = int(code.value[1:])
            assert 1 <= num <= 99, f'{code} should be in E001-E099 range'

    def test_task_error_codes_in_range(self) -> None:
        task_codes = [
            ErrorCode.TASK_NO_RETURN_TYPE,
            ErrorCode.TASK_INVALID_RETURN_TYPE,
            ErrorCode.TASK_INVALID_OPTIONS,
            ErrorCode.TASK_INVALID_QUEUE,
        ]
        for code in task_codes:
            # E100-E199 range
            num = int(code.value[1:])
            assert 100 <= num <= 199, f'{code} should be in E100-E199 range'

    def test_config_broker_error_codes_in_range(self) -> None:
        config_codes = [
            ErrorCode.CONFIG_INVALID_QUEUE_MODE,
            ErrorCode.CONFIG_INVALID_CLUSTER_CAP,
            ErrorCode.CONFIG_INVALID_PREFETCH,
            ErrorCode.BROKER_INVALID_URL,
            ErrorCode.CONFIG_INVALID_RECOVERY,
            ErrorCode.CONFIG_INVALID_SCHEDULE,
            ErrorCode.CLI_INVALID_ARGS,
            ErrorCode.WORKER_INVALID_LOCATOR,
            ErrorCode.CONFIG_INVALID_RESILIENCE,
            ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
        ]
        for code in config_codes:
            # E200-E299 range
            num = int(code.value[1:])
            assert 200 <= num <= 299, f'{code} should be in E200-E299 range'

    def test_registry_error_codes_in_range(self) -> None:
        registry_codes = [
            ErrorCode.TASK_NOT_REGISTERED,
            ErrorCode.TASK_DUPLICATE_NAME,
        ]
        for code in registry_codes:
            # E300-E399 range
            num = int(code.value[1:])
            assert 300 <= num <= 399, f'{code} should be in E300-E399 range'

    def test_error_code_is_string_enum(self) -> None:
        """ErrorCode values are strings like 'E001'."""
        for code in ErrorCode:
            assert isinstance(code.value, str)
            assert code.value[0] == 'E'
            assert code.value[1:].isdigit()


# =============================================================================
# ValidationReport Tests
# =============================================================================


class TestValidationReport:
    """Tests for ValidationReport error collection."""

    def test_empty_report(self) -> None:
        report = ValidationReport('test_phase')
        assert not report.has_errors()
        assert len(report.errors) == 0
        assert report.phase_name == 'test_phase'

    def test_add_single_error(self) -> None:
        report = ValidationReport('config')
        report.add(ConfigurationError(message='bad config'))
        assert report.has_errors()
        assert len(report.errors) == 1

    def test_add_multiple_errors(self) -> None:
        report = ValidationReport('config')
        report.add(ConfigurationError(message='error 1'))
        report.add(ConfigurationError(message='error 2'))
        report.add(ConfigurationError(message='error 3'))
        assert len(report.errors) == 3

    def test_format_rust_style_summary(self) -> None:
        report = ValidationReport('test')
        report.add(ConfigurationError(message='first error'))
        report.add(ConfigurationError(message='second error'))
        formatted = report.format_rust_style(use_colors=False)
        assert 'first error' in formatted
        assert 'second error' in formatted
        assert 'aborting due to 2 previous errors' in formatted

    def test_format_rust_style_empty_report(self) -> None:
        """Formatting an empty report still produces the summary line."""
        report = ValidationReport('empty')
        formatted = report.format_rust_style(use_colors=False)
        assert 'aborting due to 0 previous errors' in formatted

    def test_format_rust_style_with_colors(self) -> None:
        """Colored output contains ANSI escape codes."""
        report = ValidationReport('test')
        report.add(ConfigurationError(message='err'))
        formatted = report.format_rust_style(use_colors=True)
        assert '\033[' in formatted

    def test_str_delegates_to_format(self) -> None:
        report = ValidationReport('test')
        report.add(ConfigurationError(message='an error'))
        text = str(report)
        assert 'an error' in text
        assert 'aborting due to 1 previous errors' in text


# =============================================================================
# MultipleValidationErrors Tests
# =============================================================================


class TestMultipleValidationErrors:
    """Tests for MultipleValidationErrors wrapper."""

    def test_inherits_from_horsies_error(self) -> None:
        report = ValidationReport('test')
        report.add(ConfigurationError(message='err1'))
        report.add(ConfigurationError(message='err2'))
        exc = MultipleValidationErrors(
            message='aborting due to 2 previous errors', report=report,
        )
        assert isinstance(exc, HorsiesError)
        assert isinstance(exc, Exception)

    def test_format_delegates_to_report(self) -> None:
        report = ValidationReport('test')
        report.add(ConfigurationError(message='err1'))
        report.add(ConfigurationError(message='err2'))
        exc = MultipleValidationErrors(
            message='aborting due to 2 previous errors', report=report,
        )
        formatted = exc.format_rust_style(use_colors=False)
        assert 'err1' in formatted
        assert 'err2' in formatted
        assert 'aborting due to 2 previous errors' in formatted

    def test_str_output(self) -> None:
        report = ValidationReport('test')
        report.add(ConfigurationError(message='err1'))
        report.add(ConfigurationError(message='err2'))
        exc = MultipleValidationErrors(
            message='aborting due to 2 previous errors', report=report,
        )
        text = str(exc)
        assert 'err1' in text
        assert 'err2' in text

    def test_auto_message_from_report(self) -> None:
        """Empty message auto-generates from report error count."""
        report = ValidationReport('test')
        report.add(ConfigurationError(message='a'))
        report.add(ConfigurationError(message='b'))
        report.add(ConfigurationError(message='c'))
        exc = MultipleValidationErrors(message='', report=report)
        assert exc.message == 'aborting due to 3 previous errors'


# =============================================================================
# raise_collected Tests
# =============================================================================


class TestRaiseCollected:
    """Tests for the raise_collected backward-compat helper."""

    def test_zero_errors_no_raise(self) -> None:
        report = ValidationReport('test')
        # Should not raise
        raise_collected(report)

    def test_single_error_raises_original_type(self) -> None:
        report = ValidationReport('test')
        report.add(WorkflowValidationError(message='single error'))
        with pytest.raises(WorkflowValidationError, match='single error'):
            raise_collected(report)

    def test_single_config_error_raises_original_type(self) -> None:
        report = ValidationReport('config')
        report.add(ConfigurationError(message='config error'))
        with pytest.raises(ConfigurationError, match='config error'):
            raise_collected(report)

    def test_single_error_preserves_code(self) -> None:
        """Single error re-raised preserves its error code."""
        report = ValidationReport('test')
        report.add(
            WorkflowValidationError(
                message='err',
                code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
            ),
        )
        with pytest.raises(WorkflowValidationError) as exc_info:
            raise_collected(report)
        assert exc_info.value.code == ErrorCode.WORKFLOW_CYCLE_DETECTED

    def test_two_errors_raises_multiple(self) -> None:
        report = ValidationReport('test')
        report.add(WorkflowValidationError(message='err1'))
        report.add(WorkflowValidationError(message='err2'))
        with pytest.raises(MultipleValidationErrors) as exc_info:
            raise_collected(report)
        assert len(exc_info.value.report.errors) == 2

    def test_multiple_errors_is_horsies_error(self) -> None:
        report = ValidationReport('test')
        report.add(WorkflowValidationError(message='err1'))
        report.add(WorkflowValidationError(message='err2'))
        with pytest.raises(HorsiesError):
            raise_collected(report)

    def test_mixed_error_types_raises_multiple(self) -> None:
        report = ValidationReport('test')
        report.add(WorkflowValidationError(message='workflow err'))
        report.add(ConfigurationError(message='config err'))
        with pytest.raises(MultipleValidationErrors) as exc_info:
            raise_collected(report)
        errors = exc_info.value.report.errors
        assert isinstance(errors[0], WorkflowValidationError)
        assert isinstance(errors[1], ConfigurationError)

    def test_multiple_errors_message_contains_count(self) -> None:
        """MultipleValidationErrors message includes the error count."""
        report = ValidationReport('test')
        report.add(WorkflowValidationError(message='a'))
        report.add(WorkflowValidationError(message='b'))
        report.add(WorkflowValidationError(message='c'))
        with pytest.raises(MultipleValidationErrors) as exc_info:
            raise_collected(report)
        assert '3 previous errors' in exc_info.value.message


# =============================================================================
# Helper Factory Tests
# =============================================================================


class TestHelperFactories:
    """Tests for workflow_validation_error() and task_definition_error() factories."""

    def test_workflow_validation_error_basic(self) -> None:
        err = workflow_validation_error('bad workflow')
        assert isinstance(err, WorkflowValidationError)
        assert err.message == 'bad workflow'

    def test_workflow_validation_error_with_all_fields(self) -> None:
        err = workflow_validation_error(
            'cycle',
            code=ErrorCode.WORKFLOW_CYCLE_DETECTED,
            notes=['A->B', 'B->A'],
            help_text='remove cycle',
        )
        assert err.code == ErrorCode.WORKFLOW_CYCLE_DETECTED
        assert err.notes == ['A->B', 'B->A']
        assert err.help_text == 'remove cycle'

    def test_workflow_validation_error_explicit_location(self) -> None:
        loc = SourceLocation(file='wf.py', line=5)
        err = workflow_validation_error('err', location=loc)
        assert err.location is loc

    def test_workflow_validation_error_auto_location(self) -> None:
        """Auto-detects location when not explicitly provided."""
        err = workflow_validation_error('auto')
        # Function attempts frame-based auto-detection; should not raise
        assert isinstance(err, WorkflowValidationError)

    def test_task_definition_error_basic(self) -> None:
        err = task_definition_error('bad task')
        assert isinstance(err, TaskDefinitionError)
        assert err.message == 'bad task'

    def test_task_definition_error_with_function_location(self) -> None:
        def sample_task() -> int:
            return 1

        err = task_definition_error(
            'no return type',
            code=ErrorCode.TASK_NO_RETURN_TYPE,
            fn=sample_task,
        )
        assert err.code == ErrorCode.TASK_NO_RETURN_TYPE
        assert err.location is not None
        assert err.location.file.endswith('test_errors.py')

    def test_task_definition_error_without_fn(self) -> None:
        """No fn means no location derived from function; auto-detection fills in."""
        err = task_definition_error('err', fn=None)
        # location is auto-detected from call stack (not from a function object)
        assert isinstance(err, TaskDefinitionError)
        assert err.message == 'err'

    def test_task_definition_error_with_notes_and_help(self) -> None:
        err = task_definition_error(
            'invalid',
            notes=['detail 1'],
            help_text='fix it',
        )
        assert err.notes == ['detail 1']
        assert err.help_text == 'fix it'


# =============================================================================
# _find_user_frame Tests
# =============================================================================


class TestFindUserFrame:
    """Tests for _find_user_frame helper."""

    def test_returns_frame_outside_horsies(self) -> None:
        """Calling from test code returns a frame (not under /horsies/)."""
        frame = _find_user_frame()
        assert frame is not None

    def test_returns_none_when_no_frame(self) -> None:
        """Returns None when inspect.currentframe() returns None."""
        with mock.patch('horsies.core.errors.inspect.currentframe', return_value=None):
            result = _find_user_frame()
        assert result is None
