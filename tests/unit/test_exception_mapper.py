"""Unit tests for exception-to-error-code mapper (exact-class matching)."""

from __future__ import annotations

import pytest

from horsies.core.exception_mapper import (
    ExceptionMapper,
    validate_error_code_string,
    resolve_exception_error_code,
    validate_exception_mapper,
)


# ── Test exception hierarchies ──────────────────────────────────────────────


class NetworkError(Exception):
    pass


class TimeoutError_(NetworkError):
    pass


class ConnectionError_(NetworkError):
    pass


class DNSError(ConnectionError_):
    pass


class FileError(Exception):
    pass


class PermissionError_(FileError):
    pass


# ── TestExactLookup ─────────────────────────────────────────────────────────


@pytest.mark.unit
class TestExactLookup:
    """Tests for exact-class mapper lookup."""

    def test_exact_match(self) -> None:
        mapper: ExceptionMapper = {NetworkError: 'NETWORK_ERROR'}
        code = resolve_exception_error_code(
            exc=NetworkError(),
            task_mapper=None,
            global_mapper=mapper,
            task_default=None,
            global_default='UNHANDLED',
        )
        assert code == 'NETWORK_ERROR'

    def test_no_match_returns_default(self) -> None:
        mapper: ExceptionMapper = {FileError: 'FILE_ERROR'}
        code = resolve_exception_error_code(
            exc=NetworkError(),
            task_mapper=None,
            global_mapper=mapper,
            task_default=None,
            global_default='UNHANDLED',
        )
        assert code == 'UNHANDLED'

    def test_exact_match_only_no_subclass(self) -> None:
        """Subclass does NOT match parent entry — exact matching only."""
        mapper: ExceptionMapper = {NetworkError: 'NETWORK_ERROR'}
        code = resolve_exception_error_code(
            exc=TimeoutError_(),
            task_mapper=None,
            global_mapper=mapper,
            task_default=None,
            global_default='UNHANDLED',
        )
        assert code == 'UNHANDLED'

    def test_empty_mapper_returns_default(self) -> None:
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper=None,
            global_mapper={},
            task_default=None,
            global_default='UNHANDLED',
        )
        assert code == 'UNHANDLED'

    def test_base_exception_subclass_matched(self) -> None:
        """BaseException subclasses (e.g. KeyboardInterrupt) are supported."""
        mapper: ExceptionMapper = {KeyboardInterrupt: 'INTERRUPTED'}
        code = resolve_exception_error_code(
            exc=KeyboardInterrupt(),
            task_mapper=None,
            global_mapper=mapper,
            task_default=None,
            global_default='UNHANDLED',
        )
        assert code == 'INTERRUPTED'

    def test_non_string_value_in_mapper_ignored_at_lookup(self) -> None:
        """Mapper with non-string value silently falls through to default."""
        mapper = {ValueError: 42}  # type: ignore[dict-item]
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper=None,
            global_mapper=mapper,  # type: ignore[arg-type]
            task_default=None,
            global_default='UNHANDLED',
        )
        assert code == 'UNHANDLED'


# ── TestResolveExceptionErrorCode ────────────────────────────────────────────


@pytest.mark.unit
class TestResolveExceptionErrorCode:
    """Tests for the full resolution chain."""

    def test_task_mapper_wins_over_global(self) -> None:
        task_mapper: ExceptionMapper = {ValueError: 'TASK_VAL_ERROR'}
        global_mapper: ExceptionMapper = {ValueError: 'GLOBAL_VAL_ERROR'}
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper=task_mapper,
            global_mapper=global_mapper,
            task_default=None,
            global_default='UNHANDLED_EXCEPTION',
        )
        assert code == 'TASK_VAL_ERROR'

    def test_global_fallback(self) -> None:
        """Exception only in global mapper."""
        global_mapper: ExceptionMapper = {ValueError: 'GLOBAL_VAL_ERROR'}
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper=None,
            global_mapper=global_mapper,
            task_default=None,
            global_default='UNHANDLED_EXCEPTION',
        )
        assert code == 'GLOBAL_VAL_ERROR'

    def test_task_default_over_global_default(self) -> None:
        """No mapper match, task default used."""
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper=None,
            global_mapper={},
            task_default='TASK_DEFAULT',
            global_default='UNHANDLED_EXCEPTION',
        )
        assert code == 'TASK_DEFAULT'

    def test_global_default_fallback(self) -> None:
        """No mapper match, no task default, global default used."""
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper=None,
            global_mapper={},
            task_default=None,
            global_default='UNHANDLED_EXCEPTION',
        )
        assert code == 'UNHANDLED_EXCEPTION'

    def test_full_chain_task_mapper_first(self) -> None:
        """Task mapper hit stops the chain."""
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper={ValueError: 'FROM_TASK'},
            global_mapper={ValueError: 'FROM_GLOBAL'},
            task_default='TASK_DEFAULT',
            global_default='GLOBAL_DEFAULT',
        )
        assert code == 'FROM_TASK'

    def test_full_chain_global_mapper_second(self) -> None:
        """Task mapper miss, global mapper hit."""
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper={TypeError: 'WRONG'},
            global_mapper={ValueError: 'FROM_GLOBAL'},
            task_default='TASK_DEFAULT',
            global_default='GLOBAL_DEFAULT',
        )
        assert code == 'FROM_GLOBAL'

    def test_full_chain_task_default_third(self) -> None:
        """Both mappers miss, task default used."""
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper={TypeError: 'WRONG'},
            global_mapper={TypeError: 'ALSO_WRONG'},
            task_default='TASK_DEFAULT',
            global_default='GLOBAL_DEFAULT',
        )
        assert code == 'TASK_DEFAULT'

    def test_full_chain_global_default_last(self) -> None:
        """Everything misses, global default used."""
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper={TypeError: 'WRONG'},
            global_mapper={TypeError: 'ALSO_WRONG'},
            task_default=None,
            global_default='GLOBAL_DEFAULT',
        )
        assert code == 'GLOBAL_DEFAULT'

    def test_empty_task_mapper_treated_as_none(self) -> None:
        """Empty dict task mapper falls through to global."""
        global_mapper: ExceptionMapper = {ValueError: 'GLOBAL_VAL_ERROR'}
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper={},
            global_mapper=global_mapper,
            task_default=None,
            global_default='UNHANDLED_EXCEPTION',
        )
        assert code == 'GLOBAL_VAL_ERROR'

    def test_invalid_global_mapper_falls_back_to_defaults(self) -> None:
        """Invalid mapper object should not crash resolution."""
        code = resolve_exception_error_code(
            exc=ValueError(),
            task_mapper=None,
            global_mapper=None,  # type: ignore[arg-type]
            task_default=None,
            global_default='UNHANDLED_EXCEPTION',
        )
        assert code == 'UNHANDLED_EXCEPTION'



# ── TestValidateExceptionMapper ──────────────────────────────────────────────


@pytest.mark.unit
class TestValidateExceptionMapper:
    """Tests for startup validation of mapper entries."""

    def test_valid_mapper_no_errors(self) -> None:
        mapper: ExceptionMapper = {
            ValueError: 'VAL_ERROR',
            TypeError: 'TYPE_ERROR',
        }
        errors = validate_exception_mapper(mapper)
        assert errors == []

    def test_non_exception_key_rejected(self) -> None:
        mapper = {str: 'BAD'}  # type: ignore[dict-item]
        errors = validate_exception_mapper(mapper)  # type: ignore[arg-type]
        assert len(errors) == 1
        assert 'not a BaseException subclass' in errors[0]

    def test_empty_string_value_rejected(self) -> None:
        mapper: ExceptionMapper = {ValueError: ''}
        errors = validate_exception_mapper(mapper)
        assert len(errors) == 1
        assert 'non-empty string' in errors[0]

    def test_non_string_value_rejected(self) -> None:
        mapper = {ValueError: 42}  # type: ignore[dict-item]
        errors = validate_exception_mapper(mapper)  # type: ignore[arg-type]
        assert len(errors) == 1
        assert 'non-empty string' in errors[0]

    def test_empty_mapper_valid(self) -> None:
        errors = validate_exception_mapper({})
        assert errors == []

    def test_multiple_errors_collected(self) -> None:
        mapper = {str: '', ValueError: 123}  # type: ignore[dict-item]
        errors = validate_exception_mapper(mapper)  # type: ignore[arg-type]
        # str key: not BaseException subclass (1) + empty string value (2)
        # ValueError key: non-string value (3)
        assert len(errors) == 3

    def test_non_mapping_rejected(self) -> None:
        errors = validate_exception_mapper(['bad'])  # type: ignore[arg-type]
        assert len(errors) == 1
        assert 'must be a mapping' in errors[0]

    def test_exception_class_name_as_value_rejected(self) -> None:
        """Common user mistake: using exception name instead of error code."""
        mapper: ExceptionMapper = {ValueError: 'TimeoutError'}
        errors = validate_exception_mapper(mapper)
        assert len(errors) == 1
        assert 'looks like an exception class name' in errors[0]


@pytest.mark.unit
class TestErrorCodeValidation:
    def test_exception_like_name_rejected(self) -> None:
        err = validate_error_code_string('TimeoutError', field_name='auto_retry_for')
        assert err is not None
        assert 'looks like an exception class name' in err

    def test_exception_suffix_variant_rejected(self) -> None:
        """Both *Error and *Exception suffixes are caught by the regex."""
        err = validate_error_code_string('NetworkException', field_name='test')
        assert err is not None
        assert 'looks like an exception class name' in err

    def test_non_string_input_rejected(self) -> None:
        err = validate_error_code_string(123, field_name='test')
        assert err is not None
        assert 'non-empty string' in err

    def test_empty_string_rejected(self) -> None:
        err = validate_error_code_string('', field_name='test')
        assert err is not None
        assert 'non-empty string' in err

    def test_lowercase_format_rejected(self) -> None:
        """Not empty, not exception-like, but fails UPPER_SNAKE_CASE regex."""
        err = validate_error_code_string('some_code', field_name='test')
        assert err is not None
        assert 'UPPER_SNAKE_CASE' in err

    def test_upper_snake_case_accepted(self) -> None:
        err = validate_error_code_string('RATE_LIMITED', field_name='auto_retry_for')
        assert err is None

