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
"""

from __future__ import annotations

import math
from typing import NoReturn, cast


__all__ = [
    'JsonValue',
    'StrictJsonError',
    '_validate_json_native',
    '_reject_nonstandard_json_constant',
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
    `_reject_nonstandard_json_constant`.
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
