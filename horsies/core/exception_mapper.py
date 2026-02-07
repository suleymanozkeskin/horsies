"""Exact-class exception-to-error-code mapper.

Resolves unhandled exceptions to user-defined error codes by exact class
match (`type(exc) in mapper`). Supports per-task and global mappers.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import cast

ExceptionMapper = dict[type[BaseException], str]
ERROR_CODE_RE = re.compile(r'^[A-Z][A-Z0-9_]*$')
EXCEPTION_NAME_RE = re.compile(r'^[A-Z][A-Za-z0-9_]*(Error|Exception)$')


def resolve_exception_error_code(
    exc: BaseException,
    task_mapper: Mapping[type[BaseException], str] | None,
    global_mapper: Mapping[type[BaseException], str] | None,
    task_default: str | None,
    global_default: str,
) -> str:
    """Resolve an exception to an error code using the full resolution chain.

    Resolution order:
        1. task_mapper (exact class lookup)
        2. global_mapper (exact class lookup)
        3. task_default
        4. global_default
    """
    if task_mapper:
        code = _exact_lookup(exc, task_mapper)
        if code is not None:
            return code

    code = _exact_lookup(exc, global_mapper)
    if code is not None:
        return code

    if task_default is not None:
        return task_default
    return global_default


def _exact_lookup(
    exc: BaseException,
    mapper: Mapping[type[BaseException], str] | None,
) -> str | None:
    """Return the mapped error code for the exact class of exc, or None."""
    if not isinstance(mapper, Mapping):
        return None
    code = mapper.get(type(exc))
    return code if isinstance(code, str) else None


def validate_error_code_string(
    value: object,
    *,
    field_name: str,
) -> str | None:
    """Validate normalized error-code format."""
    if not isinstance(value, str) or not value:
        return f"{field_name} must be a non-empty string, got {value!r}"
    if EXCEPTION_NAME_RE.fullmatch(value) is not None:
        return (
            f"{field_name} '{value}' looks like an exception class name; "
            "retry matching is error-code-only, use UPPER_SNAKE_CASE code names"
        )
    if ERROR_CODE_RE.fullmatch(value) is None:
        return (
            f"{field_name} '{value}' is invalid; expected UPPER_SNAKE_CASE "
            "(e.g. RATE_LIMITED)"
        )
    return None


def validate_exception_mapper(
    mapper: object,
) -> list[str]:
    """Validate mapper entries. Returns error messages (empty = valid)."""
    errors: list[str] = []
    if not isinstance(mapper, Mapping):
        return [
            (
                'exception_mapper must be a mapping of '
                '{ExceptionClass: "ERROR_CODE"} entries'
            )
        ]

    exception_code_map = cast(Mapping[object, object], mapper)
    for key, value in exception_code_map.items():
        key_label = key.__name__ if isinstance(key, type) else repr(key)
        if not isinstance(key, type) or not issubclass(key, BaseException):
            errors.append(
                f"Mapper key {key!r} is not a BaseException subclass"
            )
        value_error = validate_error_code_string(
            value,
            field_name=f"Mapper value for {key_label}",
        )
        if value_error is not None:
            errors.append(value_error)
    return errors
