"""Strict JSON I/O — the sanctioned serialize/deserialize boundary.

Replaces the legacy ``codec/serde.py``. ``dumps_json`` is strict: it runs the
producer-side ``_validate_json_native`` fence (rejecting tuple, set, bytes,
Decimal, non-finite floats, non-string dict keys, ...) before ``json.dumps``,
so non-JSON-native values fail closed instead of being silently coerced
(``json.dumps`` would turn tuples into arrays and int keys into strings).

No class-tag serialization lives here — typed Python values are encoded via
``codec/typed.py`` (``encode_value`` / ``encode_task_result`` / ...) into a
plain JSON-native shape first, then handed to ``dumps_json``.

``validate_core_model_json`` reads the JSON text that the core
(``horsies._core``) writes from its model of a monitoring read into the
Pydantic type of that read.
"""

from __future__ import annotations

import json
from typing import Dict, Final, List, Union

from pydantic import TypeAdapter

from horsies.core.codec.json_value import (
    JSON_TEXT_TOO_DEEP,
    StrictJsonError,
    _object_without_repeated_key,
    _parse_finite_float,
    _reject_deep_nesting,
    _reject_lone_surrogate,
    _reject_nonstandard_json_constant,
    _validate_json_native,
)
from horsies.core.types.result import Err, Ok, Result


Json = Union[None, bool, int, float, str, List['Json'], Dict[str, 'Json']]
"""Union type for JSON-serializable values."""


class SerializationError(Exception):
    """Raised when a value cannot be serialized to or deserialized from JSON."""


class EmptyJsonTextError(SerializationError):
    """The text or payload to read is empty. An empty text is not JSON.

    ``loads_json`` and ``loads_json_bytes`` return it. It is a
    ``SerializationError``, so a caller that handles that type handles it too.
    """


EMPTY_JSON_TEXT: Final = 'JSON parse failed: the text is empty'
"""The message of ``EmptyJsonTextError``."""


type SerdeResult[T] = Result[T, SerializationError]


def dumps_json(value: object) -> SerdeResult[str]:
    """Serialize a Python value to a strict JSON string.

    Fails closed (Err) on any non-JSON-native value — tuple, set, bytes,
    Decimal, non-finite float, non-string dict key — rather than coercing it.
    A value nested deeper than ``MAX_JSON_DEPTH``, or a value that holds
    itself, is also an Err.
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


def dumps_canonical_json(value: object) -> str:
    """Serialize a JSON-native value to its one canonical text.

    Compact separators, sorted keys, no ASCII escaping. Callers store or hash
    this text, so equal content always gives equal text.

    Callers pass values that ``loads_json`` parsed or that the library built,
    so a non-finite float is a code defect: ``allow_nan=False`` raises
    ``ValueError``, and no caller handles it. Unlike ``dumps_json`` this does
    not walk the value first.
    """
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(',', ':'),
        sort_keys=True,
        allow_nan=False,
    )


def loads_json(s: str) -> SerdeResult[Json]:
    """Deserialize a JSON string.

    An empty text is ``Err(EmptyJsonTextError)``: it is not JSON. The JSON
    text ``null`` is ``Ok(None)``. The input is never ``None``: a caller with
    a nullable column checks NULL before the call. Wraps
    json.JSONDecodeError as SerializationError so callers handle a single
    error type.

    Routes through ``parse_constant=_reject_nonstandard_json_constant`` so
    Python's lenient acceptance of ``NaN`` / ``Infinity`` / ``-Infinity``
    (not RFC 8259) fails closed at every raw-load site instead of smuggling
    non-finite floats past the producer-side strict fence. Routes through
    ``parse_float=_parse_finite_float`` for the same reason: a numeric literal
    such as ``1e9999`` never reaches ``parse_constant``, and ``float()`` would
    turn it into ``inf``. Routes through
    ``object_pairs_hook=_object_without_repeated_key``: an object that repeats
    a key is an Err, where ``json.loads`` keeps the last value.
    ``_reject_lone_surrogate`` then refuses a string that UTF-8 cannot encode,
    which ``dumps_json`` also refuses.

    A document nested deeper than ``MAX_JSON_DEPTH`` is an Err, so each walker
    that gets the parsed value stays inside the recursion limit. A text that
    is too deep for ``json.loads`` itself gets the same Err from the
    ``RecursionError`` arm.
    """
    if s == '':
        return Err(EmptyJsonTextError(EMPTY_JSON_TEXT))
    try:
        parsed: Json = json.loads(
            s,
            parse_constant=_reject_nonstandard_json_constant,
            parse_float=_parse_finite_float,
            object_pairs_hook=_object_without_repeated_key,
        )
        _reject_lone_surrogate(s, parsed)
        _reject_deep_nesting(s, parsed)
    except StrictJsonError as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))
    except RecursionError:
        return Err(SerializationError(f'JSON parse failed: {JSON_TEXT_TOO_DEEP}'))
    except (json.JSONDecodeError, ValueError) as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))
    return Ok(parsed)


def loads_json_bytes(payload: bytes) -> SerdeResult[Json]:
    """Deserialize a stored payload whose codec declares UTF-8 JSON.

    The bytes are decoded as UTF-8, then read by ``loads_json``, so empty
    bytes are ``Err(EmptyJsonTextError)`` as an empty text is. Bytes that are
    not UTF-8 are an Err: ``json.loads`` on bytes detects UTF-16 and UTF-32
    and skips a byte order mark, so it accepts formats that the codec does
    not declare.

    The error text names a byte offset, not a byte: the payload is task input.
    """
    try:
        text = payload.decode('utf-8')
    except UnicodeDecodeError as exc:
        return Err(SerializationError(
            f'JSON parse failed: payload is not UTF-8 at byte {exc.start}'
        ))
    return loads_json(text)


def validate_core_model_json[T](adapter: TypeAdapter[T], text: str) -> T:
    """Validate the JSON text of a core model into the type of ``adapter``.

    The core (``horsies._core``) writes ``text`` from its own model of a
    monitoring read. The monitoring reads validate that text only here, so
    the codec holds their one parse site.

    This is ``adapter.validate_json(text)`` in the adapter's own mode. A
    ``TypeAdapter`` of a ``BaseModel`` uses the validator of the model, so the
    value and the error are those of ``Model.model_validate_json(text)``.

    It does not run the hooks of ``loads_json``. Use it only for text that the
    core writes, never for task input or a stored payload. Pure: no I/O.

    Raises:
        pydantic.ValidationError: the text does not match the type. The core
            and the Python type disagree, which is a defect. The monitoring
            reads do not catch it.
    """
    return adapter.validate_json(text)
