"""Strict JSON I/O — the sanctioned serialize/deserialize boundary.

Replaces the legacy ``codec/serde.py``. ``dumps_json`` is strict: it runs the
producer-side ``_validate_json_native`` fence (rejecting tuple, set, bytes,
Decimal, non-finite floats, non-string dict keys, ...) before ``json.dumps``,
so non-JSON-native values fail closed instead of being silently coerced
(``json.dumps`` would turn tuples into arrays and int keys into strings).

No class-tag serialization lives here — typed Python values are encoded via
``codec/typed.py`` (``encode_value`` / ``encode_task_result`` / ...) into a
plain JSON-native shape first, then handed to ``dumps_json``.
"""

from __future__ import annotations

import json
from typing import Dict, List, Optional, Union

from horsies.core.codec.json_value import (
    StrictJsonError,
    _reject_nonstandard_json_constant,
    _validate_json_native,
)
from horsies.core.types.result import Err, Ok, Result


Json = Union[None, bool, int, float, str, List['Json'], Dict[str, 'Json']]
"""Union type for JSON-serializable values."""


class SerializationError(Exception):
    """Raised when a value cannot be serialized to or deserialized from JSON."""


type SerdeResult[T] = Result[T, SerializationError]


def dumps_json(value: object) -> SerdeResult[str]:
    """Serialize a Python value to a strict JSON string.

    Fails closed (Err) on any non-JSON-native value — tuple, set, bytes,
    Decimal, non-finite float, non-string dict key — rather than coercing it.
    Emits no class tags; encode typed values via ``codec/typed.py`` first.
    """
    try:
        _validate_json_native(value)
    except StrictJsonError as exc:
        return Err(SerializationError(f'value is not JSON-native: {exc}'))
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(',', ':'),
            allow_nan=False,
        )
    except (ValueError, TypeError) as exc:
        return Err(SerializationError(f'json.dumps failed: {exc}'))
    # ensure_ascii=False emits lone UTF-16 surrogates verbatim; they are not
    # UTF-8 encodable and would otherwise fail far downstream in the Postgres
    # TEXT insert. Fail closed here at the producer-side encode boundary.
    try:
        encoded.encode('utf-8')
    except UnicodeEncodeError as exc:
        return Err(SerializationError(
            f'serialized value is not valid UTF-8 (lone surrogate?): {exc}'
        ))
    return Ok(encoded)


def loads_json(s: Optional[str]) -> SerdeResult[Json]:
    """Deserialize a JSON string.

    Returns Ok(None) for empty/None input. Wraps json.JSONDecodeError as
    SerializationError so callers handle a single error type.

    Routes through ``parse_constant=_reject_nonstandard_json_constant`` so
    Python's lenient acceptance of ``NaN`` / ``Infinity`` / ``-Infinity``
    (not RFC 8259) fails closed at every raw-load site instead of smuggling
    non-finite floats past the producer-side strict fence.
    """
    if not s:
        return Ok(None)
    try:
        return Ok(json.loads(s, parse_constant=_reject_nonstandard_json_constant))
    except StrictJsonError as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))
    except (json.JSONDecodeError, ValueError) as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))
