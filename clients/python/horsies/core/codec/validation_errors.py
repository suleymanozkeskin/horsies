"""Text for pydantic validation failures that holds no input values.

``str(ValidationError)`` and ``ValidationError.errors()`` carry the refused
input. Library error text, logs and stored task errors use the functions
here instead. Each entry names the location, the message and the error type.

A message keeps its text when every placeholder in its template is a schema
value (a limit, a pattern, the expected values). A placeholder whose value
comes from the input is written as ``INPUT_PLACEHOLDER``. ``value_error``,
``assertion_error`` and custom error types keep the text that the
validator wrote.

A ``loc`` part is a key from the input in two cases. pydantic does not mark
which parts are field names, so these keys stay in the text:

- the validated type is a dict or a mapping, or contains one;
- a model, ``TypedDict`` or dataclass with ``extra='forbid'`` refuses a key.
"""

from __future__ import annotations

from string import Formatter
from typing import Any, Final, cast

from pydantic import ValidationError
from pydantic_core import ErrorDetails, PydanticKnownError
from pydantic_core.core_schema import ErrorType

from horsies.core.codec.json_io import Json, SerializationError


__all__ = [
    'AUTHOR_TEXT_ERROR_TYPES',
    'INPUT_PLACEHOLDER',
    'MAX_VALIDATION_ERRORS_IN_TEXT',
    'SCHEMA_CONTEXT_KEYS',
    'ValueValidationError',
    'exception_text',
    'validation_error_entries',
    'validation_summary',
    'without_validation_input',
]


MAX_VALIDATION_ERRORS_IN_TEXT: Final = 10
"""Entries written per validation failure; the text then adds ``(+N more)``."""

INPUT_PLACEHOLDER: Final = '<input>'
"""Written in place of a template placeholder whose value comes from the input."""

SCHEMA_CONTEXT_KEYS: Final = frozenset({
    'actual_length',
    'class',
    'class_name',
    'decimal_places',
    'discriminator',
    'encoding',
    'expected',
    'expected_plural',
    'expected_schemes',
    'expected_tags',
    'expected_version',
    'field_type',
    'ge',
    'gt',
    'le',
    'lt',
    'max_digits',
    'max_length',
    'method_name',
    'min_length',
    'multiple_of',
    'pattern',
    'tz_expected',
    'whole_digits',
})
"""Template placeholders whose values come from the schema. ``actual_length``
is the length of the input, not its content."""

AUTHOR_TEXT_ERROR_TYPES: Final = frozenset({'value_error', 'assertion_error'})
"""Error types whose message is the text of an exception that a validator raised."""


class ValueValidationError(SerializationError, ValueError):
    """A value failed pydantic validation or serialization.

    The text names each failing location, the message and the error type. It
    holds no input value and no reference to the pydantic exception. It is a
    ``ValueError``, as the pydantic exceptions that it replaces.
    """


def validation_summary(
    error: ValidationError,
    location: tuple[str, ...] = (),
) -> str:
    """The failing entries as ``<loc>: <msg> [<type>]``, joined by ``'; '``.

    ``location`` goes in front of each entry's ``loc``, for example the name
    of the kwarg that was validated alone. At most
    ``MAX_VALIDATION_ERRORS_IN_TEXT`` entries are written; ``(+N more)``
    counts the rest. An entry with an empty location has no ``<loc>:`` part.
    """
    details = error.errors(include_url=False, include_input=False)
    shown = [
        _entry_text(detail, location)
        for detail in details[:MAX_VALIDATION_ERRORS_IN_TEXT]
    ]
    hidden_count = len(details) - len(shown)
    if hidden_count > 0:
        shown.append(f'(+{hidden_count} more)')
    return '; '.join(shown)


def validation_error_entries(error: ValidationError) -> list[Json]:
    """The failing entries as ``{'loc', 'msg', 'type'}`` objects.

    At most ``MAX_VALIDATION_ERRORS_IN_TEXT`` entries; ``error.error_count()``
    gives the full count.
    """
    details = error.errors(include_url=False, include_input=False)
    entries: list[Json] = []
    for detail in details[:MAX_VALIDATION_ERRORS_IN_TEXT]:
        location: list[Json] = [part for part in detail['loc']]
        entries.append({
            'loc': location,
            'msg': _safe_message(detail),
            'type': detail['type'],
        })
    return entries


def exception_text(exc: BaseException) -> str:
    """``str(exc)``, with the summary in place of a pydantic validation text."""
    match exc:
        case ValidationError():
            return validation_summary(exc)
        case _:
            return str(exc)


def without_validation_input(exc: BaseException) -> BaseException:
    """``exc``, or a ``ValueValidationError`` with the summary when ``exc`` is a
    pydantic ``ValidationError``. The result holds no reference to ``exc``."""
    match exc:
        case ValidationError():
            return ValueValidationError(validation_summary(exc))
        case _:
            return exc


def _entry_text(detail: ErrorDetails, prefix: tuple[str, ...]) -> str:
    location = '.'.join(str(part) for part in (*prefix, *detail['loc']))
    message = _safe_message(detail)
    match location:
        case '':
            return f'{message} [{detail["type"]}]'
        case _:
            return f'{location}: {message} [{detail["type"]}]'


def _safe_message(detail: ErrorDetails) -> str:
    error_type = detail['type']
    if error_type in AUTHOR_TEXT_ERROR_TYPES:
        return detail['msg']
    context = detail.get('ctx')
    try:
        # A custom error type is not an ``ErrorType`` member; pydantic refuses
        # it with KeyError.
        template = PydanticKnownError(cast(ErrorType, error_type), context).message_template
    except KeyError:
        return detail['msg']
    except TypeError:
        # A built-in type without the context that its template needs.
        return INPUT_PLACEHOLDER
    placeholders = [
        field for _, field, _, _ in Formatter().parse(template) if field is not None
    ]
    if all(field in SCHEMA_CONTEXT_KEYS for field in placeholders):
        return detail['msg']
    return _render_without_input(template, context)


def _render_without_input(template: str, context: dict[str, Any] | None) -> str:
    parts: list[str] = []
    for literal, field, _, _ in Formatter().parse(template):
        parts.append(literal)
        match field:
            case None:
                pass
            case str() as name if (
                name in SCHEMA_CONTEXT_KEYS and context is not None and name in context
            ):
                parts.append(str(context[name]))
            case _:
                parts.append(INPUT_PLACEHOLDER)
    return ''.join(parts)
