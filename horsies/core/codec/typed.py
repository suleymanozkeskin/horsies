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

import dataclasses
import math
import types
from typing import Any, Union, cast, get_args, get_origin

from pydantic import BaseModel, TypeAdapter

from horsies.core.codec.json_value import (
    JsonValue,
    StrictJsonError,
    _validate_json_native,
)
from horsies.core.models.tasks import TaskError


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


# Reserved namespace per §2 + the legacy `__horsies_*` prefix still in
# active engine use (workflow_ctx / workflow_meta / taskresult transport
# keys at `workflows/engine.py`). Rejecting `__horsies_*` in user data
# closes the smuggle path through `child_runner.py`'s args_from envelope
# handling — that path keys off `__horsies_taskresult__` inside a kwarg
# dict and routes to legacy `task_result_from_json` / `rehydrate_value`,
# which would still run the old class-identity importer. Engine-internal
# uses are encode-side direct dict construction (not via `encode_value`),
# so this restriction only fires for user-originated data.
_RESERVED_KEY_PREFIXES: tuple[str, ...] = ('__h_', '__horsies_')
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
    1. Run the producer-side `_validate_json_native` fence at every
       JsonValue position in `expected_type` (top-level, container
       elements, `Optional[...]`, BaseModel / dataclass fields). The
       fence catches silent coercions `TypeAdapter(JsonValue)` would let
       through (bytes -> str, Decimal -> float, ...) at boundary
       positions and at all the JsonValue-derivative positions §3
       documents (`dict[str, JsonValue]`, `list[JsonValue]`, ...).
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
    _apply_json_value_fence(value, expected_type)
    adapter = _get_adapter(expected_type)
    dumped = adapter.dump_python(value, mode='json')
    dumped_json = cast(Json, dumped)
    _scan_wire_json(dumped_json)
    if _is_task_error_type(expected_type):
        # TaskError's own `error_code` field legitimately emits
        # `{"__builtin_task_code__": "..."}` for built-in codes.
        # Path-aware allowance: scan only the user-controlled fields
        # (`data`, `exception`, `message`) and skip `error_code`.
        _scan_task_error_user_fields(dumped_json)
    else:
        _scan_reserved_keys(dumped_json)
    return dumped_json


def decode_value(json_value: Json, expected_type: TypeAnnotation) -> object:
    """Decode a JSON-shaped value into a typed Python value.

    Assumes `json_value` came from `json.loads(s,
    parse_constant=_reject_nonstandard_json_constant)` so non-RFC-8259
    constants were rejected upstream.

    Reserved-key invariant is enforced symmetrically with `encode_value`:
    payloads carrying `__h_*` or `__builtin_task_code__` at user-controlled
    positions are rejected before TypeAdapter validation. Without this,
    cross-version / cross-language producers (the same threat model the
    decode-side `_reject_nonstandard_json_constant` hook exists for)
    could smuggle reserved keys past a strict in-process producer.

    Args:
        json_value: The JSON-shaped input.
        expected_type: The declared type.

    Returns:
        The decoded Python value.

    Raises:
        StrictJsonError: on reserved-key collision in user-positioned data.
        pydantic.ValidationError: when `json_value` doesn't satisfy
            `expected_type`.
    """
    if _is_task_error_type(expected_type):
        _scan_task_error_user_fields(json_value)
    else:
        _scan_reserved_keys(json_value)
    adapter = _get_adapter(expected_type)
    return adapter.validate_python(json_value)


def _is_task_error_type(expected_type: TypeAnnotation) -> bool:
    return isinstance(expected_type, type) and issubclass(expected_type, TaskError)


def _scan_task_error_user_fields(dumped_json: Json) -> None:
    """Scan only the user-controlled fields of a TaskError dump.

    TaskError's `error_code` serializer emits
    `{"__builtin_task_code__": "..."}` for built-in codes; that's the
    one path-specific allowance. `data` is user-controlled and gets
    full strict scan; `exception` and `message` likewise.
    """
    if not isinstance(dumped_json, dict):
        return
    d = cast('dict[str, Json]', dumped_json)
    for key, sub_value in d.items():
        if key == 'error_code':
            continue
        _scan_reserved_keys(sub_value)


# ---------------------------------------------------------------------------
# Producer-side JsonValue fence: applied at *every* JsonValue position
# ---------------------------------------------------------------------------


def _apply_json_value_fence(
    value: object,
    expected_type: TypeAnnotation,
    visited: frozenset[type] = frozenset(),
) -> None:
    """Walk `(value, expected_type)` and run `_validate_json_native` at
    each JsonValue position.

    This is the encode-time analogue of `signature_check._walk_model_fields`:
    it descends through the declared type's container / union / model
    structure and applies the strict producer-side fence wherever the
    annotation is JsonValue. For non-JsonValue subtrees the walk is a
    no-op; TypeAdapter's normal coercion within their declared shape is
    fine.

    Required because §3's JsonValue boundary positions include
    `dict[str, JsonValue]`, `list[JsonValue]`, `Optional[JsonValue]`,
    and JsonValue inside BaseModel / dataclass fields — TypeAdapter
    silently coerces `bytes -> str` / `Decimal -> float` in all of those
    positions, breaking the "raw JSON only" contract that the literal
    `JsonValue` boundary already enforces.
    """
    if expected_type is JsonValue:
        _validate_json_native(value)
        return
    if value is None:
        return

    origin = get_origin(expected_type)
    args = get_args(expected_type)

    # Annotated[T, meta...] — recurse into the underlying.
    if origin is not None and hasattr(expected_type, '__metadata__'):
        if args:
            _apply_json_value_fence(value, args[0], visited)
        return

    # Optional / Union — JsonValue can appear as an alternative.
    if origin is Union or origin is types.UnionType:
        non_none = tuple(a for a in args if a is not type(None))
        if len(non_none) == 1:
            _apply_json_value_fence(value, non_none[0], visited)
            return
        # Mixed primitive unions etc. can't contain JsonValue (the
        # signature validator already forbids cross-category unions), so
        # nothing to fence here.
        return

    if origin is list and isinstance(value, list):
        if not args:
            return
        item_t = args[0]
        for item in cast('list[object]', value):
            _apply_json_value_fence(item, item_t, visited)
        return

    if origin is dict and isinstance(value, dict):
        if len(args) < 2:
            return
        val_t = args[1]
        for v in cast('dict[str, object]', value).values():
            _apply_json_value_fence(v, val_t, visited)
        return

    if origin is tuple and isinstance(value, tuple):
        if not args:
            return
        items = cast('tuple[object, ...]', value)
        if len(args) == 2 and args[1] is Ellipsis:
            for item in items:
                _apply_json_value_fence(item, args[0], visited)
        else:
            for item, arg_t in zip(items, args, strict=False):
                _apply_json_value_fence(item, arg_t, visited)
        return

    # BaseModel — walk fields that mention JsonValue. Bounded by `visited`
    # so self-referential models don't blow the stack here either.
    if isinstance(expected_type, type) and issubclass(expected_type, BaseModel):
        if expected_type in visited:
            return
        if not isinstance(value, expected_type):
            return
        next_visited = visited | {expected_type}
        for field_name, field_info in expected_type.model_fields.items():
            field_type = field_info.annotation
            if field_type is None:
                continue
            _apply_json_value_fence(
                getattr(value, field_name),
                field_type,
                next_visited,
            )
        return

    # Parameterized BaseModel generic that didn't get caught above (e.g.
    # Box[int] where annot is itself a ModelMetaclass — usually the
    # branch above catches it, but the generic-alias path is here as a
    # safety net using `model_fields` on the parameterized form).
    if (
        origin is not None
        and isinstance(origin, type)
        and issubclass(origin, BaseModel)
        and isinstance(value, origin)
    ):
        target = (
            expected_type
            if isinstance(expected_type, type)
            and issubclass(expected_type, BaseModel)
            else origin
        )
        if target in visited:
            return
        next_visited = visited | {target}
        for field_name, field_info in target.model_fields.items():
            field_type = field_info.annotation
            if field_type is None:
                continue
            _apply_json_value_fence(
                getattr(value, field_name),
                field_type,
                next_visited,
            )
        return

    # Dataclass — walk fields. Forward refs / generic substitutions in
    # dataclasses are weaker than Pydantic's; use `get_type_hints` and
    # accept that fully-generic dataclass parameterization isn't
    # introspectable here.
    if isinstance(expected_type, type) and dataclasses.is_dataclass(expected_type):
        if expected_type in visited:
            return
        if not isinstance(value, expected_type):
            return
        try:
            hints = _dataclass_hints(expected_type)
        except Exception:  # noqa: BLE001 — forward-ref resolution failures
            return
        next_visited = visited | {expected_type}
        for field_name, field_type in hints.items():
            if field_name.startswith('_'):
                continue
            if not hasattr(value, field_name):
                continue
            _apply_json_value_fence(
                getattr(value, field_name),
                field_type,
                next_visited,
            )
        return


def _dataclass_hints(dc: type) -> dict[str, object]:
    """Resolve dataclass field type hints to concrete annotations.

    Pulled out so the fence walker can swallow forward-ref resolution
    errors without losing structured type-hint extraction.
    """
    from typing import get_type_hints  # local: avoid module import cycle

    return get_type_hints(dc, include_extras=True)


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
                    any(key.startswith(p) for p in _RESERVED_KEY_PREFIXES)
                    or key == _RESERVED_DISCRIMINATOR
                ):
                    raise StrictJsonError(
                        f'reserved key {key!r} in user-originated data',
                    )
                _scan_reserved_keys(sub_value)
            return
