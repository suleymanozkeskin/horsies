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
import re
import types
from collections import deque
from collections.abc import Iterable
from typing import Any, Final, Literal, Union, cast, get_args, get_origin

from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic_core import PydanticSerializationError

from horsies.core.codec.json_value import (
    JsonValue,
    StrictJsonError,
    _validate_json_native,
)
from horsies.core.codec.validation_errors import (
    ValueValidationError,
    validation_summary,
)
from horsies.core.models.tasks import SubWorkflowError, TaskError, TaskResult


__all__ = [
    'Json',
    'TypeAnnotation',
    'ValueValidationError',
    'decode_task_error',
    'decode_task_result',
    'decode_value',
    'encode_task_error',
    'encode_task_result',
    'encode_value',
    'lone_surrogate_text',
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

_REPLACEMENT_CHAR: Final = '\ufffd'
"""What `dump_python(mode='json')` writes in a dict key for a lone surrogate."""

_NO_JSON_KEY_TEXT: Final = re.compile(r'`\w+` not valid as object key')
"""The text of the plain `TypeError` from `dump_python(mode='json')` for a dict
key without a JSON form. Its one placeholder is a type name, so it holds no
input value. `_dump` matches all of it: any other `TypeError` is a defect."""

_NOT_SERIALIZABLE_VALUE_TEXTS: Final = frozenset({
    'Circular reference detected (id repeated)',
    'Circular reference detected (depth exceeded)',
})
"""The texts of the plain `ValueError` from `dump_python` for a value that holds
itself, and for a value that is nested too deep. The second text says circular,
but the value can be finite. `_dump` compares all of each text: any other
`ValueError` propagates."""


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


def encode_value(
    value: object,
    expected_type: TypeAnnotation,
    *,
    location: tuple[str, ...] = (),
) -> Json:
    """Encode `value` to a JSON-shaped Python value under `expected_type`.

    Pipeline:
    1. Run the producer-side `_validate_json_native` fence at every
       JsonValue position in `expected_type` (top-level, container
       elements, `Optional[...]`, BaseModel / dataclass fields). The
       fence catches silent coercions `TypeAdapter(JsonValue)` would let
       through (bytes -> str, Decimal -> float, ...) at boundary
       positions and at all the JsonValue-derivative positions §3
       documents (`dict[str, JsonValue]`, `list[JsonValue]`, ...).
    2. `TypeAdapter(expected_type).validate_python(value)` rejects values
       that do not satisfy `expected_type`. `dump_python` alone only
       serializes — it coerces/passes a mistyped value (`dict` for a
       `str` param, etc.) with a `PydanticSerializationUnexpectedValue`
       warning instead of raising. Validating first makes the producer
       fail closed, symmetric with `decode_value` on the consumer.
    3. `TypeAdapter(expected_type).dump_python(validated, mode='json')`.
       A dict key with a lone surrogate raises at a position without a
       declared key type; `_dump` makes that a `StrictJsonError`. A dict key
       without a JSON form, a value that holds itself and a value that is
       nested too deep raise too; `_dump` makes those a
       `ValueValidationError`.
    4. `_scan_wire_json` rejects non-finite floats and strings with a lone
       surrogate at any depth, so `encode_value` refuses each string that
       `dumps_json` refuses. TypeAdapter preserves NaN/Inf and lone
       surrogates even with `mode='json'`.
    5. When a dumped dict key holds U+FFFD, `_scan_wire_json` runs again on
       the `mode='python'` dump. `mode='json'` writes U+FFFD in place of a
       lone surrogate in a key with a declared type, so the first scan
       cannot see that surrogate. `mode='python'` keeps the key. U+FFFD is
       a valid character, so a key that truly holds it is accepted. A key
       text that only the `mode='json'` dump produces, for example from a
       `field_serializer(when_used='json')`, stays accepted with U+FFFD:
       the surrogate is in code, not in runtime data.
    6. `_scan_reserved_keys` rejects user-originated reserved-namespace keys
       (`__h_*` and `__builtin_task_code__`) at any depth.

    Args:
        value: The Python value to encode.
        expected_type: The declared type (Pydantic-acceptable annotation).
        location: Written in front of each failing location in the
            refusal text, for example the kwarg name.

    Returns:
        A JSON-shaped Python value (a `Json`).

    Raises:
        StrictJsonError: on JsonValue fence rejection, non-finite floats,
            a lone surrogate in a string, a dict key or the text of a
            validator, or reserved-key smuggling.
        ValueValidationError: when `value` doesn't satisfy `expected_type`,
            or pydantic cannot serialize the validated value. The text holds
            no input value.
    """
    _apply_json_value_fence(value, expected_type)
    adapter = _get_adapter(expected_type)
    validated = _validate(adapter, value, location)
    dumped_json = cast(Json, _dump(adapter, validated, 'json', location))
    if _scan_wire_json(dumped_json, location):
        _ = _scan_wire_json(
            _dump(adapter, validated, 'python', location), location,
        )
    if _is_task_error_type(expected_type):
        # TaskError's own `error_code` field legitimately emits
        # `{"__builtin_task_code__": "..."}` for built-in codes.
        # Path-aware allowance: scan only the user-controlled fields
        # (`data`, `exception`, `message`) and skip `error_code`.
        _scan_task_error_user_fields(dumped_json)
    else:
        _scan_reserved_keys(dumped_json)
    return dumped_json


def decode_value(
    json_value: Json,
    expected_type: TypeAnnotation,
    *,
    location: tuple[str, ...] = (),
) -> object:
    """Decode a JSON-shaped value into a typed Python value.

    Assumes `json_value` came from `loads_json`, so non-RFC-8259 constants
    and numbers outside the finite binary64 range were rejected upstream.

    Reserved-key invariant is enforced symmetrically with `encode_value`:
    payloads carrying `__h_*` or `__builtin_task_code__` at user-controlled
    positions are rejected before TypeAdapter validation. Without this,
    cross-version / cross-language producers (the same threat model the
    decode-side `_reject_nonstandard_json_constant` hook exists for)
    could smuggle reserved keys past a strict in-process producer.

    Args:
        json_value: The JSON-shaped input.
        expected_type: The declared type.
        location: Written in front of each failing location in the
            refusal text, for example the kwarg name.

    Returns:
        The decoded Python value.

    Raises:
        StrictJsonError: on reserved-key collision in user-positioned data,
            or a lone surrogate in the text of a validator.
        ValueValidationError: when `json_value` doesn't satisfy
            `expected_type`. The text holds no input value.
    """
    if _is_task_error_type(expected_type):
        _scan_task_error_user_fields(json_value)
    else:
        _scan_reserved_keys(json_value)
    adapter = _get_adapter(expected_type)
    return _validate(adapter, json_value, location)


def _validate(
    adapter: TypeAdapter[Any],
    value: object,
    location: tuple[str, ...],
) -> object:
    """`adapter.validate_python(value)`; a refusal raises `ValueValidationError`.

    A validator can write a lone surrogate in its error text, for example
    when it echoes the value. Pydantic then cannot build the error text and
    raises `UnicodeEncodeError`: `validate_python` does for the text of a
    `ValueError` or an `AssertionError`, and `ValidationError.errors()` does
    for the context of a `PydanticCustomError`. A validator cannot raise that
    error itself, because pydantic reads each `ValueError` as a refusal. So
    the arm catches only this case, and it raises `StrictJsonError`.

    The error is raised outside the `except` blocks, so it has no
    `__context__` that holds the pydantic error and its input.
    """
    failure: StrictJsonError | ValueValidationError
    try:
        try:
            return adapter.validate_python(value)
        except ValidationError as error:
            failure = ValueValidationError(validation_summary(error, location))
    except UnicodeEncodeError:
        failure = StrictJsonError(
            lone_surrogate_text('a validation error', location),
        )
    raise failure


def _dump(
    adapter: TypeAdapter[Any],
    validated: object,
    mode: Literal['json', 'python'],
    location: tuple[str, ...],
) -> object:
    """`adapter.dump_python(validated, mode=mode)`; a failure raises
    `ValueValidationError` with the pydantic text. That text names types;
    text that a custom serializer raises is kept as written.

    Pydantic raises `UnicodeEncodeError` for a dict key with a lone surrogate
    at a position without a declared key type (`Any`, a bare `dict`). That is
    a `StrictJsonError`, raised outside the `except` block, because the
    `UnicodeEncodeError` holds the input.

    Pydantic raises a plain `TypeError` for a dict key without a JSON form:
    a `frozenset`, or an Enum member whose value is a list, a dict or a set.
    Only the text tells that from a defect, for example a `TypeError` from a
    computed field or a generator, so the text must match in full. Any other
    `TypeError` propagates. A pydantic that changes the text gives the raw
    `TypeError` again, not a wrong refusal.

    Pydantic raises a plain `ValueError` for a value that holds itself or
    that is nested too deep. The same rule applies: only the full text is
    this refusal. Any other `ValueError` propagates, for example from a
    computed field. The arm comes after the arms for
    `PydanticSerializationError` and `UnicodeEncodeError`, because both are a
    `ValueError`.
    """
    failure: StrictJsonError | ValueValidationError
    try:
        return adapter.dump_python(validated, mode=mode)
    except PydanticSerializationError as error:
        failure = ValueValidationError(f'serialization failed: {error}')
    except UnicodeEncodeError:
        failure = StrictJsonError(lone_surrogate_text('a dict key', location))
    except TypeError as error:
        if _NO_JSON_KEY_TEXT.fullmatch(str(error)) is None:
            raise
        failure = ValueValidationError(
            _with_location(f'serialization failed: {error}', location),
        )
    except ValueError as error:
        if str(error) not in _NOT_SERIALIZABLE_VALUE_TEXTS:
            raise
        failure = ValueValidationError(
            _with_location(f'serialization failed: {error}', location),
        )
    raise failure


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

    ``TaskError.exception`` is declared as ``BaseException |
    FlattenedException | None``: the in-memory form lets producers
    (task_decorator's catch blocks, library error builders) attach a
    live exception for local diagnostics. Pydantic can't serialize a
    raw ``BaseException``, so the err branch flattens it to the
    structured ``FlattenedException`` shape via ``model_copy`` before
    invoking ``encode_value``. This is the single source of truth for
    the live-exception → flattened invariant; every wire emit
    (worker finalize, engine args_from envelope, broker reaper /
    expiry, library error payloads) goes through this function.

    Args:
        result: The task result to encode.
        ok_type: Declared `OkT` for the ok-slot encoding. Required even
            when the result is an err so the envelope shape is uniform.

    Returns:
        The wire-shaped envelope dict.

    Raises:
        StrictJsonError: on JsonValue fence rejection, non-finite floats,
            a lone surrogate, or reserved-key smuggling inside ok / err
            payloads.
        ValueValidationError: when the carried value doesn't satisfy
            its declared shape.
    """
    if result.is_err():
        err = result.err_value
        if isinstance(err.exception, BaseException):
            from horsies.core.codec.error_payload import flatten_exception
            err = err.model_copy(
                update={'exception': flatten_exception(err.exception)},
            )
        encoded_err = encode_task_error(err)
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


def encode_task_error(err: TaskError) -> Json:
    """Encode a TaskError (or subclass) preserving its concrete type.

    Symmetric counterpart to ``decode_task_error``. Routes the encode
    through ``TypeAdapter(type(err))`` so subclass-only fields (notably
    ``SubWorkflowError.sub_workflow_id`` / ``sub_workflow_summary``)
    survive the wire round-trip. ``encode_value(err, TaskError)`` would
    silently strip them.

    Args:
        err: A ``TaskError`` instance — may be the base class or any
            library-known subclass such as ``SubWorkflowError``.

    Returns:
        The encoded JSON payload carrying every field present on the
        concrete runtime type.

    Raises:
        StrictJsonError / ValueValidationError: on reserved-key
            smuggling inside ``err.data`` or schema violations
            (same surfaces as ``encode_value``).
    """
    return encode_value(err, type(err))


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
        ValueValidationError: when the payload doesn't satisfy the
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
        ValueValidationError: when the payload doesn't satisfy the
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


def _with_location(text: str, location: tuple[str, ...]) -> str:
    """`text` with `location` in front of it, for example the kwarg name."""
    match location:
        case ():
            return text
        case _:
            return f'{".".join(location)}: {text}'


def lone_surrogate_text(
    subject: Literal['a string', 'a dict key', 'a validation error'],
    location: tuple[str, ...],
) -> str:
    """The refusal text for a lone surrogate. It holds no input value."""
    return _with_location(
        f'{subject} holds a lone surrogate, which UTF-8 cannot encode',
        location,
    )


def _scan_text(
    text: str,
    subject: Literal['a string', 'a dict key'],
    location: tuple[str, ...],
) -> None:
    """Refuse `text` when UTF-8 cannot encode it, as `dumps_json` does.

    The refusal is raised outside the `except` block, because the
    `UnicodeEncodeError` holds the input.

    Raises:
        StrictJsonError: `text` holds a lone surrogate.
    """
    try:
        text.encode('utf-8')
    except UnicodeEncodeError:
        failure = StrictJsonError(lone_surrogate_text(subject, location))
    else:
        return
    raise failure


def _scan_wire_json(
    dumped: object,
    location: tuple[str, ...] = (),
) -> bool:
    """Reject non-finite floats and lone surrogates anywhere in a dump.

    `TypeAdapter(float).dump_python(float('nan'), mode='json')` preserves
    `nan`; we walk every dump output and reject `math.isnan()` /
    `math.isinf()` floats before the final `json.dumps(...,
    allow_nan=False)` guard. A string with a lone surrogate also stays in the
    dump. `dumps_json` refuses it, so this refuses it first, with a
    `StrictJsonError` that encode callers already handle.

    `str.isascii()` comes first: it reads a flag, and ASCII text cannot hold
    a surrogate.

    A dict key with a lone surrogate does not stay in a `mode='json'` dump:
    pydantic writes U+FFFD in place of each byte of the surrogate, so
    `{'a\\ud800': 1}` becomes `{'a\\ufffd\\ufffd\\ufffd': 1}`, and two keys
    can become one. So a dumped key that holds U+FFFD is the only sign, and
    U+FFFD is also a valid character. The caller then runs this scan on the
    `mode='python'` dump, which keeps each key. That dump can also hold
    tuples, sets, deques and other leaf types.

    Args:
        dumped: A `mode='json'` or a `mode='python'` dump.
        location: Written in front of the lone surrogate refusal text.

    Returns:
        True when a dict key holds U+FFFD. Each container in `dumped` was
        scanned to its end.

    Raises:
        StrictJsonError: on the first non-finite float, or the first string
            or dict key with a lone surrogate. The text holds no input value.
    """
    # bool BEFORE int — bool is a subclass of int, and we don't want True
    # to fall into the float-checking branch via the int matcher.
    match dumped:
        case None | bool() | int():
            return False
        case str() as text:
            if not text.isascii():
                _scan_text(text, 'a string', location)
            return False
        case float() as fval:
            if math.isnan(fval) or math.isinf(fval):
                raise StrictJsonError(
                    f'non-RFC-8259 float in encoded wire value: {fval!r}',
                )
            return False
        case dict():
            key_holds_replacement_char = False
            for key, sub_value in cast('dict[object, object]', dumped).items():
                if isinstance(key, str) and not key.isascii():
                    _scan_text(key, 'a dict key', location)
                    key_holds_replacement_char |= _REPLACEMENT_CHAR in key
                key_holds_replacement_char |= _scan_wire_json(sub_value, location)
            return key_holds_replacement_char
        case list() | tuple() | set() | frozenset() | deque():
            key_holds_replacement_char = False
            for item in cast('Iterable[object]', dumped):
                key_holds_replacement_char |= _scan_wire_json(item, location)
            return key_holds_replacement_char
        case _:
            # A leaf of a `mode='python'` dump, for example a datetime.
            return False


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
