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
from horsies.core.models.tasks import SubWorkflowError, TaskError, TaskResult


__all__ = [
    'Json',
    'TypeAnnotation',
    'decode_task_error',
    'decode_task_result',
    'decode_value',
    'encode_task_result',
    'encode_value',
    'validate_task_result_envelope',
]


# Wire envelope key for the typed `TaskResult` payload (§5 / §8). The
# only reserved-namespace key the codec layer emits directly; user-
# originated dumps that try to inject this key are rejected by
# `_scan_reserved_keys`.
_TASK_RESULT_ENVELOPE_KEY = '__h_task_result__'


# Reserved internal `__h_*` keys that survive strict-serde phase 6.
# These never appear in user-originated dumps — the engine emits them
# directly (bypassing `encode_value`) for engine-internal transport
# and storage. User data carrying any of these is rejected by
# `_scan_reserved_keys` via the `__h_` prefix.
#
# Surviving set:
# - `__h_task_result__`        — TaskResult envelope marker.
# - `__h_workflow_ctx__`       — engine-injected ``workflow_ctx`` kwarg.
# - `__h_workflow_meta__`      — engine-injected ``workflow_meta`` kwarg.
# - `__h_taskresult_envelope__` — args_from envelope wrapping a
#   ``__h_task_result__`` with ``source_task_name`` metadata.
# - `__h_outputless_terminals__` — flag on the workflow's final-result
#   envelope when the workflow has no explicit output node; signals
#   the handle's outputless decode path which walks per-node entries
#   via the embedded ``task_name_by_id`` map.
#
# Phase 7 reviews and tightens this list further.
_RESERVED_INTERNAL_KEYS: frozenset[str] = frozenset({
    '__h_task_result__',
    '__h_workflow_ctx__',
    '__h_workflow_meta__',
    '__h_taskresult_envelope__',
    '__h_outputless_terminals__',
})


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


# Reserved namespace per §2 + the legacy `__horsies_*` prefix retained
# for defense-in-depth even after strict-serde phase 6 renamed every
# engine emitter to ``__h_*``. `__horsies_*` in user data still indicates
# a smuggle attempt against the pre-phase-6 envelope shape; rejecting
# at any depth keeps the codec strict against cross-version producers.
# Engine-internal emits don't route through `encode_value`, so this
# restriction only fires for user-originated data.
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


def encode_task_result(
    result: TaskResult[Any, TaskError],
    ok_type: TypeAnnotation,
) -> dict[str, Json]:
    """Encode a `TaskResult[OkT, TaskError]` into the wire envelope.

    Envelope shape:
        ``{"__h_task_result__": True, "ok": <encoded ok> | None,
           "err": <encoded TaskError dict> | None}``

    Exactly one slot carries data (matching `TaskResult`'s constructor
    invariant). The ok slot may legitimately be `None` when
    `ok_type` permits it (e.g. `TaskResult[None, TaskError]`); decode
    discriminates on the err slot — `err is None` → ok path,
    `err is not None` → err path.

    The ok slot routes through `encode_value(value, ok_type)` and inherits
    the JsonValue fence, wire scan, and reserved-key scan. The err slot
    routes through `encode_value(value, TaskError)` and picks up the
    path-aware TaskError scan (built-in code discriminator allowed under
    `error_code`; smuggle attempts in `data` rejected).

    Args:
        result: The task result to encode.
        ok_type: Declared `OkT` for the ok-slot encoding. Required even
            when the result is an err so the envelope shape is uniform.

    Returns:
        The wire-shaped envelope dict.

    Raises:
        StrictJsonError: on JsonValue fence rejection, non-finite floats,
            or reserved-key smuggling inside ok / err payloads.
        pydantic.ValidationError: when the carried value doesn't satisfy
            its declared shape.
    """
    if result.is_err():
        encoded_err = encode_value(result.err_value, TaskError)
        return {
            _TASK_RESULT_ENVELOPE_KEY: True,
            'ok': None,
            'err': encoded_err,
        }
    encoded_ok = encode_value(result.ok_value, ok_type)
    return {
        _TASK_RESULT_ENVELOPE_KEY: True,
        'ok': encoded_ok,
        'err': None,
    }


def decode_task_error(err_slot: Json) -> TaskError:
    """Decode an err-slot payload into the right concrete TaskError class.

    Public shared helper used by every err-slot decode site (typed
    codec, task handle err-fast-path, app err-fast-path, workflow
    handle err slot). Single implementation so the discriminator
    rules can never drift between callers.

    The strict codec uses ``TypeAdapter(TaskError)`` which drops fields
    declared only on subclasses (notably ``SubWorkflowError``'s
    ``sub_workflow_id`` / ``sub_workflow_summary``). The engine emits
    subworkflow failures via ``SubWorkflowError.model_dump(...)`` at
    ``engine.py::on_subworkflow_complete``; without polymorphic decode
    those subclass fields would be silently lost when downstream
    consumers read the parent node's TaskResult.

    Discriminator: inspect the payload's keys. SubWorkflowError-specific
    fields (``sub_workflow_id``) identify the subclass; everything else
    decodes as plain ``TaskError``. The reserved-key scan
    (``_scan_task_error_user_fields``) still applies in both branches
    via ``decode_value``.

    Args:
        err_slot: The raw err-slot value from a TaskResult envelope.

    Returns:
        A ``TaskError`` (or ``SubWorkflowError``) instance.

    Raises:
        StrictJsonError: on reserved-key smuggling inside the err
            payload.
        pydantic.ValidationError: when the payload doesn't satisfy the
            chosen concrete class.
    """
    if (
        isinstance(err_slot, dict)
        and 'sub_workflow_id' in cast('dict[str, Json]', err_slot)
    ):
        decoded = decode_value(err_slot, SubWorkflowError)
        if not isinstance(decoded, SubWorkflowError):
            # Defensive: TypeAdapter should always return the concrete
            # class. If it ever doesn't, refuse to silently downgrade.
            raise StrictJsonError(
                f'SubWorkflowError decode returned '
                f'{type(decoded).__name__!r}',
            )
        return decoded
    decoded_base = decode_value(err_slot, TaskError)
    if not isinstance(decoded_base, TaskError):
        raise StrictJsonError(
            f'TaskError decode returned {type(decoded_base).__name__!r}',
        )
    return decoded_base


def validate_task_result_envelope(raw: Json) -> dict[str, Json]:
    """Validate the wire shape of a ``__h_task_result__`` envelope.

    Returns the envelope as ``dict[str, Json]`` on success; raises
    ``StrictJsonError`` otherwise. Used by ``decode_task_result`` and
    by err-fast-path callers (handle / app ``get_result``) that decode
    only the err slot but still must reject malformed envelopes —
    a payload carrying both ok and err, missing the marker, or
    carrying extras would otherwise slip past the fast path.

    Strict-serde §5: the envelope must carry exactly the keys
    ``{__h_task_result__, ok, err}``, ``__h_task_result__: True``, and
    at most one of ``ok`` / ``err`` populated.
    """
    if not isinstance(raw, dict):
        raise StrictJsonError(
            f'TaskResult envelope must be a JSON object; got '
            f'{type(raw).__name__}',
        )
    envelope = cast('dict[str, Json]', raw)
    marker = envelope.get(_TASK_RESULT_ENVELOPE_KEY)
    if marker is not True:
        raise StrictJsonError(
            f'TaskResult envelope missing {_TASK_RESULT_ENVELOPE_KEY!r} '
            f'marker (or marker is not True); got keys '
            f'{sorted(envelope.keys())}',
        )
    keys = set(envelope.keys())
    expected = {_TASK_RESULT_ENVELOPE_KEY, 'ok', 'err'}
    missing = expected - keys
    if missing:
        raise StrictJsonError(
            f'TaskResult envelope missing required keys '
            f'{sorted(missing)}; got {sorted(keys)}',
        )
    extra = keys - expected
    if extra:
        raise StrictJsonError(
            f'unexpected keys in TaskResult envelope: {sorted(extra)}',
        )
    ok_slot = envelope.get('ok')
    err_slot = envelope.get('err')
    if err_slot is not None and ok_slot is not None:
        raise StrictJsonError(
            'TaskResult envelope has both ok and err populated; exactly '
            'one must be set',
        )
    return envelope


def decode_task_result(
    raw: Json,
    ok_type: TypeAnnotation,
) -> TaskResult[Any, TaskError]:
    """Decode a wire envelope back to `TaskResult[OkT, TaskError]`.

    Validates the envelope shape strictly (must be a dict carrying
    `__h_task_result__: True`, with exactly the keys
    `{__h_task_result__, ok, err}`). Discriminates on the err slot —
    `err is None` → ok path, `err is not None` → err path. Both slots
    populated, or the envelope marker missing, raises
    ``StrictJsonError``; legacy envelopes (`__task_result__`,
    `__task_error__`, …) fall under the same rejection because they
    don't carry the `__h_task_result__` marker.

    The ok slot routes through `decode_value(slot, ok_type)`; the err
    slot routes through `decode_value(slot, TaskError)`.

    Args:
        raw: The JSON-shaped envelope, typically from
            `json.loads(...)` on the broker's stored payload.
        ok_type: Declared `OkT` for the ok-slot decoding.

    Returns:
        The reconstructed `TaskResult`.

    Raises:
        StrictJsonError: on envelope shape failure or reserved-key
            smuggling inside ok / err payloads.
        pydantic.ValidationError: when the payload doesn't satisfy the
            declared shape.
    """
    envelope = validate_task_result_envelope(raw)
    ok_slot = envelope.get('ok')
    err_slot = envelope.get('err')
    if err_slot is not None:
        # Polymorphic decode preserves SubWorkflowError subclass fields
        # (``sub_workflow_id`` / ``sub_workflow_summary``). The engine
        # emits these at ``on_subworkflow_complete``; without subclass
        # routing TypeAdapter(TaskError) would drop them.
        return TaskResult(err=decode_task_error(err_slot))
    decoded_ok = decode_value(ok_slot, ok_type)
    return TaskResult(ok=decoded_ok)


def _is_task_error_type(expected_type: TypeAnnotation) -> bool:
    return isinstance(expected_type, type) and issubclass(expected_type, TaskError)


def _scan_task_error_user_fields(dumped_json: Json) -> None:
    """Scan a TaskError dump with the path-specific allowance for
    `error_code`'s built-in-code discriminator.

    TaskError's `error_code` serializer emits exactly
    `{"__builtin_task_code__": "<str>"}` for built-in codes and a
    plain string (or None) for user codes. Anything else under
    `error_code` — additional keys, other reserved keys alongside the
    discriminator — is rejected as smuggled. `data`, `exception`, and
    `message` are user-controlled and get the full strict scan.
    """
    if not isinstance(dumped_json, dict):
        return
    d = cast('dict[str, Json]', dumped_json)
    for key, sub_value in d.items():
        if key == 'error_code':
            _scan_task_error_error_code(sub_value)
            continue
        _scan_reserved_keys(sub_value)


def _scan_task_error_error_code(value: Json) -> None:
    """Path-aware scan for TaskError.error_code.

    The TaskError serializer emits one of exactly three shapes:
    - `None` (no error code set)
    - `"some_user_code"` (plain string for user codes)
    - `{"__builtin_task_code__": "BUILTIN_NAME"}` (single-key dict for
      built-in codes)

    Any other dict shape under `error_code` is malformed and must be
    rejected. The earlier version of this scan relied on TypeAdapter
    to surface a shape error for the
    `{"__builtin_task_code__": "X", "other": "y"}` case, but Pydantic
    silently drops unknown keys during error_code validation — so an
    extra non-reserved key would pass through unobserved. Reject any
    deviation from the documented three shapes here.

    When the deviation includes a reserved key (e.g.
    `{"__builtin_task_code__": "X", "__h_extra__": 1}`), prefer the
    reserved-key message: it tells operators they're looking at a
    smuggle attempt, not just a shape bug.
    """
    if value is None:
        return
    if isinstance(value, str):
        return
    if not isinstance(value, dict):
        # Unexpected non-dict; let TypeAdapter surface a clearer error.
        return
    d = cast('dict[str, Json]', value)
    is_exact_discriminator = (
        len(d) == 1
        and '__builtin_task_code__' in d
        and isinstance(d['__builtin_task_code__'], str)
    )
    if is_exact_discriminator:
        return
    # Not the exact discriminator shape. If any non-discriminator key
    # is reserved, name it first (smuggle attempt — actionable). If
    # all non-discriminator keys are unreserved, raise a shape error
    # (TypeAdapter would silently drop them otherwise).
    for key in d:
        if key == _RESERVED_DISCRIMINATOR:
            continue
        if any(key.startswith(p) for p in _RESERVED_KEY_PREFIXES):
            raise StrictJsonError(
                f'reserved key {key!r} in user-originated data',
            )
    raise StrictJsonError(
        f'invalid TaskError.error_code shape: expected None, a plain '
        f'string, or {{"__builtin_task_code__": "<str>"}}; got dict '
        f'with keys {sorted(d.keys())}',
    )


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
        except (NameError, TypeError):
            # Forward-ref resolution failure (NameError: missing globalns
            # entry; TypeError: unsupported annotation form). The strict
            # signature validator at registration already accepted this
            # dataclass, so reaching here means the fence couldn't
            # introspect a field — skip the JsonValue walk for it rather
            # than raise; TypeAdapter's later dump will still surface a
            # concrete error if the field is genuinely malformed.
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
