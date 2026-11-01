"""Strict JsonValue boundary type and validators.

Phase 0 spike for the strict-serde redesign
(see `ignored-content/design/strict-serde.md` §3 and §12 phase 0).

Defines:

- `JsonValue` — PEP 695 alias; the sanctioned "untyped boundary" type.
- `StrictJsonError` — raised on any strict-JSON violation.
- `_validate_json_native(value)` — producer-side walker that rejects
  non-JSON-native Python values (bytes, Decimal, tuple, set, non-finite
  floats, non-string dict keys, ...) before they reach
  `TypeAdapter(JsonValue)`, which silently coerces.
- `_reject_nonstandard_json_constant(constant)` — `parse_constant` hook
  for every raw `json.loads(s, parse_constant=...)` site; rejects
  `NaN`, `Infinity`, `-Infinity` (Python lenient extensions, not RFC 8259).
- `_parse_finite_float(literal)` — `parse_float` hook for the same sites;
  rejects a numeric literal outside the finite binary64 range.
- `_reject_lone_surrogate(text, parsed)` — post-parse check for the same
  sites; rejects a string that UTF-8 cannot encode.
"""

from __future__ import annotations

import math
from typing import NoReturn, cast


__all__ = [
    'JsonValue',
    'StrictJsonError',
    '_validate_json_native',
    '_reject_nonstandard_json_constant',
    '_parse_finite_float',
    '_reject_lone_surrogate',
]


type JsonValue = (
    None
    | bool
    | int
    | float
    | str
    | list['JsonValue']
    | dict[str, 'JsonValue']
)


class StrictJsonError(Exception):
    """Raised when a value fails strict JSON-native validation.

    Producer-side via `_validate_json_native`; decode-side via
    `_reject_nonstandard_json_constant`, `_parse_finite_float` and
    `_reject_lone_surrogate`.
    """


def _validate_json_native(value: object) -> None:
    """Reject anything that isn't a JSON-native Python value, recursively.

    Producer-side fence. Required because `TypeAdapter(JsonValue)` silently
    coerces non-JSON-native values (bytes -> str, Decimal -> float, ...).
    Decode-side hardening lives separately in
    `_reject_nonstandard_json_constant`.

    Args:
        value: Any Python value; walked recursively for list / dict.

    Raises:
        StrictJsonError: on bytes, Decimal, tuple, set, non-finite float,
            non-string dict key, or any other non-JSON-native value at any
            depth.
    """
    # bool BEFORE int — `True isinstance int` is True; check the narrower type first.
    if isinstance(value, bool) or value is None or isinstance(value, (int, str)):
        return
    if isinstance(value, float):
        # RFC 8259 has no NaN / +Inf / -Inf; reject at the boundary.
        if math.isnan(value) or math.isinf(value):
            raise StrictJsonError(f'non-RFC-8259 float: {value!r}')
        return
    if isinstance(value, list):
        # Tuples are NOT `list` and fall through to the final raise — JSON has
        # no tuple shape; TypeAdapter would coerce silently.
        for item in cast(list[object], value):
            _validate_json_native(item)
        return
    if isinstance(value, dict):
        for key, item in cast(dict[object, object], value).items():
            if not isinstance(key, str):
                raise StrictJsonError(
                    f'dict key must be str, got {type(key).__name__}',
                )
            _validate_json_native(item)
        return
    raise StrictJsonError(f'non-JSON-native value: {type(value).__name__}')


def _reject_nonstandard_json_constant(constant: str) -> NoReturn:
    """`parse_constant` hook for every raw `json.loads(s, ...)` site in horsies.

    Python's `json.loads` accepts `NaN`, `Infinity`, `-Infinity` by default
    (lenient extension, not RFC 8259). Cross-language / cross-version
    producers, or manual writes to the result column, could smuggle them in.
    Reject at load so the strictness contract holds end-to-end.

    Args:
        constant: Constant name handed back by the json parser.

    Raises:
        StrictJsonError: always; this hook is reject-only.
    """
    raise StrictJsonError(
        f'Non-standard JSON constant is not allowed: {constant}',
    )


def _parse_finite_float(literal: str) -> float:
    """`parse_float` hook that rejects a literal outside finite binary64.

    `parse_constant` sees only the tokens `NaN`, `Infinity` and `-Infinity`.
    A numeric literal goes through `parse_float`, and `float('1e9999')` is
    `inf`. A literal that underflows, such as `1e-9999`, gives `0.0`: the
    value is finite and the text is valid JSON, so it stays accepted.

    Args:
        literal: Numeric literal handed over by the json parser. It holds
            task input, so the error text does not repeat it.

    Returns:
        The parsed finite float.

    Raises:
        StrictJsonError: when the literal overflows binary64.
    """
    value = float(literal)
    if math.isinf(value):
        raise StrictJsonError('JSON number is outside the finite binary64 range')
    return value


def _encodes_as_utf8(text: str) -> bool:
    if text.isascii():
        return True
    try:
        text.encode('utf-8')
    except UnicodeEncodeError:
        return False
    return True


def _reject_lone_surrogate(text: str, parsed: JsonValue) -> None:
    """Reject a parsed document that holds a lone UTF-16 surrogate.

    `json.loads` decodes the escape `\\ud800` to a `str` that UTF-8 cannot
    encode, and it has no hook for strings. The failure would come later and
    far from the parse: an envelope encode or a fingerprint raises
    `UnicodeEncodeError`, and Postgres cannot store the value.

    A surrogate pair such as `\\ud83d\\ude00` decodes to one valid character,
    and an escaped backslash such as `\\\\ud800` is plain text. Both stay
    accepted. The walk reads the decoded strings, so it needs no escape
    grammar, and it runs only when the text can hold a lone surrogate.
    `dumps_json` emits non-ASCII text unescaped, so its output skips the walk.

    Args:
        text: The JSON text that `parsed` came from.
        parsed: The parsed document. It is finite, so the walk ends.

    Raises:
        StrictJsonError: when a string or key holds a lone surrogate. The
            text names no input value.
    """
    may_hold_escape = '\\u' in text and ('\\ud' in text or '\\uD' in text)
    if not may_hold_escape and _encodes_as_utf8(text):
        return
    stack: list[JsonValue] = [parsed]
    while stack:
        match stack.pop():
            case str() as item:
                if not _encodes_as_utf8(item):
                    raise StrictJsonError('JSON string holds a lone surrogate')
            case list() as items:
                stack.extend(items)
            case dict() as mapping:
                stack.extend(mapping.keys())
                stack.extend(mapping.values())
            case _:
                pass
