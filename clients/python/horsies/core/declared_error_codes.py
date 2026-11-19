"""The user error codes that a task declares: `@app.task(..., error_codes=...)`.

The `error_code` metric label of one task has every built-in code by its
value, each code that the task declares, and `undeclared`. A user code that
the task does not declare counts as `undeclared` in that label only; events,
traces, logs and the history row keep the exact code. Declaring is
optional, and no count limits a set.

`error_codes=` takes an `Enum` class, whose member values are the codes, or a
tuple of code texts. `parse_declared_error_codes` applies the rules of the
core's `DeclaredErrorCodes::parse`, in its order for each entry:

1. the code-text rule (`validate_error_code_string`): a non-empty
   `UPPER_SNAKE_CASE` string that is not an exception class name
   (`HRS-112`), and not the reserved name `WORKFLOW_STOPPED` (`HRS-212`);
2. not the name of a built-in code (`HRS-212`);
3. not the text of an earlier accepted entry (`HRS-113`).

A value that is neither an `Enum` class nor a tuple is `HRS-102`.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final, cast

from horsies.core.exception_mapper import (
    RESERVED_NON_TASK_CODES,
    validate_error_code_string,
)
from horsies.core.models.tasks import BUILTIN_CODE_REGISTRY
from horsies.core.types.result import Err, Ok, Result

NO_ERROR_CODES: Final[tuple[str, ...]] = ()
"""The default of `error_codes=`: the task declares no code."""

ERROR_CODES_FIELD: Final = 'error_codes'
"""The field name that a refusal text names."""


@dataclass(frozen=True, slots=True)
class CodeSetFormRefused:
    """`error_codes` is neither an `Enum` class nor a tuple (`HRS-102`).
    `given` is the type name of the value."""

    given: str


@dataclass(frozen=True, slots=True)
class CodeTextRefused:
    """An entry fails the code-text rule: it is not a non-empty string, it
    is an exception class name, or it is not `UPPER_SNAKE_CASE` (`HRS-112`).
    `detail` names the entry and the rule, as `validate_error_code_string`
    words it."""

    detail: str


@dataclass(frozen=True, slots=True)
class ReservedNameRefused:
    """An entry is the name of a built-in code or a reserved name
    (`HRS-212`). `detail` names the code that it collides with."""

    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class RepeatedCodeRefused:
    """An entry is the same text as an earlier accepted entry (`HRS-113`)."""

    code: str


type CodeRefusal = CodeTextRefused | ReservedNameRefused | RepeatedCodeRefused
"""Why one entry of `error_codes` is refused."""


@dataclass(frozen=True, slots=True)
class CodesRefused:
    """Every refused entry, in the order of the input; at least one."""

    refusals: tuple[CodeRefusal, ...]


type DeclaredCodesRefusal = CodeSetFormRefused | CodesRefused
"""Why an `error_codes` value is refused."""


def parse_declared_error_codes(
    value: object,
) -> Result[tuple[str, ...], DeclaredCodesRefusal]:
    """The codes that the `error_codes` value `value` declares, in the order
    of the declaration. Pure.

    Ok: the codes, each once; `()` for an empty tuple or an `Enum` class
    without members.

    Err: `CodeSetFormRefused` when `value` is neither an `Enum` class nor a
    tuple; `CodesRefused` with one refusal per refused entry, from the first
    rule that refuses it (see the module doc).
    """
    match _entries_of(value):
        case Ok(entries):
            pass
        case Err(form_refused):
            return Err(form_refused)
    accepted: list[str] = []
    refusals: list[CodeRefusal] = []
    for entry in entries:
        match _checked_entry(entry, accepted):
            case Ok(code):
                accepted.append(code)
            case Err(refusal):
                refusals.append(refusal)
    match refusals:
        case []:
            return Ok(tuple(accepted))
        case _:
            return Err(CodesRefused(refusals=tuple(refusals)))


def _entries_of(value: object) -> Result[tuple[object, ...], CodeSetFormRefused]:
    """The entries of an `error_codes` value: the member values of an
    `Enum` class (aliases once, as the class iterates them), or the items of
    a tuple."""
    match value:
        case type() if issubclass(value, Enum):
            return Ok(tuple(member.value for member in value))
        case tuple():
            return Ok(cast('tuple[object, ...]', value))
        case _:
            return Err(CodeSetFormRefused(given=type(value).__name__))


def _checked_entry(
    entry: object,
    accepted: Sequence[str],
) -> Result[str, CodeRefusal]:
    """The code of `entry`, given the entries accepted before it."""
    match entry:
        case str() as text:
            pass
        case _:
            return Err(
                CodeTextRefused(
                    detail=(
                        f'{ERROR_CODES_FIELD} must hold non-empty strings, '
                        f'got {entry!r}'
                    ),
                ),
            )
    match validate_error_code_string(text, field_name=ERROR_CODES_FIELD):
        case None:
            pass
        case str() as reserved_detail if text in RESERVED_NON_TASK_CODES:
            return Err(ReservedNameRefused(code=text, detail=reserved_detail))
        case str() as text_detail:
            return Err(CodeTextRefused(detail=text_detail))
    match BUILTIN_CODE_REGISTRY.get(text):
        case None:
            pass
        case member:
            return Err(
                ReservedNameRefused(
                    code=text,
                    detail=(
                        f"{ERROR_CODES_FIELD} value '{text}' collides with "
                        f'reserved built-in {type(member).__name__}.{member.name}'
                    ),
                ),
            )
    match text in accepted:
        case True:
            return Err(RepeatedCodeRefused(code=text))
        case False:
            return Ok(text)

