"""Public strict codec primitives: `encode_value`, `decode_value`.

Phase 2 of the strict-serde redesign — see
`ignored-content/design/strict-serde.md` §3, §8, §12 phase 2.

Wraps `pydantic.TypeAdapter`, runs `_scan_wire_json` and
`_scan_reserved_keys` after every encode, and special-cases JsonValue at
the producer-side fence so silent coercions are rejected.

The primitives are NOT yet wired into producers / workers / handles —
that lands in phases 3-6.
"""

from __future__ import annotations

import math
from typing import Any, cast

from pydantic import TypeAdapter

from horsies.core.codec.json_value import (
    JsonValue,
    StrictJsonError,
    _validate_json_native,
)


__all__ = [
    'Json',
    'TypeAnnotation',
    'decode_value',
    'encode_value',
]


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------


type Json = (
    None
    | bool
    | int
    | float
    | str
    | list['Json']
    | dict[str, 'Json']
)
"""A value shaped to fit RFC 8259 JSON (post-TypeAdapter-dump, pre-stringify).

Structurally identical to `JsonValue`, but `JsonValue` is the *user-facing*
boundary type while `Json` is the *internal wire-shape* type. Kept distinct
so docs / signatures can express intent."""


TypeAnnotation = object
"""Any value `pydantic.TypeAdapter` accepts.

Not narrowed to `type` because `list[X]`, `Annotated[...]`, `Union[...]`,
`TaskResult[X, Y]`, etc. are not plain `type` instances at runtime. See
design-doc §0.
"""


# ---------------------------------------------------------------------------
# Reserved-key invariant (§2 / §8)
# ---------------------------------------------------------------------------


_RESERVED_KEY_PREFIX = '__h_'
_RESERVED_DISCRIMINATOR = '__builtin_task_code__'


# ---------------------------------------------------------------------------
# TypeAdapter cache
# ---------------------------------------------------------------------------


_adapter_cache: dict[Any, TypeAdapter[Any]] = {}


def _get_adapter(expected_type: TypeAnnotation) -> TypeAdapter[Any]:
    """Look up or construct a `TypeAdapter` for `expected_type`.

    Caches by identity. Unhashable annotations skip the cache.
    """
    try:
        cached = _adapter_cache.get(expected_type)
        if cached is not None:
            return cached
    except TypeError:
        return TypeAdapter(expected_type)
    adapter: TypeAdapter[Any] = TypeAdapter(expected_type)
    try:
        _adapter_cache[expected_type] = adapter
    except TypeError:
        pass
    return adapter


# ---------------------------------------------------------------------------
# Public primitives
# ---------------------------------------------------------------------------


def encode_value(value: object, expected_type: TypeAnnotation) -> Json:
    """Encode `value` to a JSON-shaped Python value under `expected_type`.

    Pipeline:
    1. If `expected_type is JsonValue`, run the producer-side
       `_validate_json_native` fence first to catch the silent coercions
       `TypeAdapter(JsonValue)` would let through (bytes -> str,
       Decimal -> float, ...).
    2. `TypeAdapter(expected_type).dump_python(value, mode='json')`.
    3. `_scan_wire_json` rejects non-finite floats at any depth — TypeAdapter
       preserves NaN/Inf even with `mode='json'`.
    4. `_scan_reserved_keys` rejects user-originated reserved-namespace keys
       (`__h_*` and `__builtin_task_code__`) at any depth.

    Args:
        value: The Python value to encode.
        expected_type: The declared type (Pydantic-acceptable annotation).

    Returns:
        A JSON-shaped Python value (a `Json`).

    Raises:
        StrictJsonError: on JsonValue fence rejection, non-finite floats,
            or reserved-key smuggling.
        pydantic.ValidationError: when `value` doesn't satisfy
            `expected_type`.
    """
    if expected_type is JsonValue:
        _validate_json_native(value)
    adapter = _get_adapter(expected_type)
    dumped = adapter.dump_python(value, mode='json')
    dumped_json = cast(Json, dumped)
    _scan_wire_json(dumped_json)
    _scan_reserved_keys(dumped_json)
    return dumped_json


def decode_value(json_value: Json, expected_type: TypeAnnotation) -> object:
    """Decode a JSON-shaped value into a typed Python value.

    Assumes `json_value` came from `json.loads(s,
    parse_constant=_reject_nonstandard_json_constant)` so non-RFC-8259
    constants were rejected upstream.

    Args:
        json_value: The JSON-shaped input.
        expected_type: The declared type.

    Returns:
        The decoded Python value.

    Raises:
        pydantic.ValidationError: when `json_value` doesn't satisfy
            `expected_type`.
    """
    adapter = _get_adapter(expected_type)
    return adapter.validate_python(json_value)


# ---------------------------------------------------------------------------
# Scans
# ---------------------------------------------------------------------------


def _scan_wire_json(dumped_json: Json) -> None:
    """Reject non-finite floats anywhere in dumped JSON.

    `TypeAdapter(float).dump_python(float('nan'), mode='json')` preserves
    `nan`; we walk every dump output and reject `math.isnan()` /
    `math.isinf()` floats before the final `json.dumps(...,
    allow_nan=False)` guard.

    Raises:
        StrictJsonError: on the first non-finite float encountered.
    """
    # bool BEFORE int — bool is a subclass of int, and we don't want True
    # to fall into the float-checking branch via the int matcher.
    match dumped_json:
        case None | bool() | int() | str():
            return
        case float() as fval:
            if math.isnan(fval) or math.isinf(fval):
                raise StrictJsonError(
                    f'non-RFC-8259 float in encoded wire value: {fval!r}',
                )
            return
        case list() as items:
            for item in cast('list[Json]', items):
                _scan_wire_json(item)
            return
        case dict() as mapping:
            for sub_value in cast('dict[str, Json]', mapping).values():
                _scan_wire_json(sub_value)
            return


def _scan_reserved_keys(dumped_json: Json) -> None:
    """Reject reserved-namespace keys at any depth in user-originated dump.

    Reserved namespace per §2:
    - `__h_*` — engine transport keys.
    - `__builtin_task_code__` — TaskError's internal Pydantic discriminator.

    The TaskError-discriminator allowance is path-aware and lives in
    `encode_task_result` (phase 2.5+); this generic scan rejects every
    reserved key it sees so user types with collisions fail loudly.

    Raises:
        StrictJsonError: on the first reserved-key collision.
    """
    match dumped_json:
        case None | bool() | int() | float() | str():
            return
        case list() as items:
            for item in cast('list[Json]', items):
                _scan_reserved_keys(item)
            return
        case dict() as mapping:
            for key, sub_value in cast('dict[str, Json]', mapping).items():
                if (
                    key.startswith(_RESERVED_KEY_PREFIX)
                    or key == _RESERVED_DISCRIMINATOR
                ):
                    raise StrictJsonError(
                        f'reserved key {key!r} in user-originated data',
                    )
                _scan_reserved_keys(sub_value)
            return
