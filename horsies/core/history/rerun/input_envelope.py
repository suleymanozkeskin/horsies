"""Input-envelope content v1: the bytes inside the frozen physics.

Gate 2 froze the envelope's physical contract — columns, the 64 KiB
inline bound, the digest, copy-never-reserialize — and left the payload
bytes opaque. This module owns what is inside them:
`prepared_rerun_input_version` 1 declares one JSON object
`{args, kwargs, options}` produced by ONE canonical serializer, because
the digest covers bytes and two serializations of equal content must
never diverge. `options: null` means the request ran on defaults;
options ABSENT means an unknown or foreign content shape and fails
closed — absence is never guessed as defaults. Decode verifies the
digest before parsing anything.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Final, cast

INPUT_ENVELOPE_VERSION: Final = 1
INPUT_ENVELOPE_CODEC: Final = 'json-utf8'
INPUT_ENVELOPE_CONTENT_TYPE: Final = 'application/json'
INPUT_ENVELOPE_INLINE_MAX_BYTES: Final = 65536


@dataclass(frozen=True, slots=True)
class ReconstructedInput:
    """The replayable request payload, decoded and digest-verified."""

    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    options: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class InputEnvelopeCorrupt:
    """Digest mismatch, malformed bytes, or a foreign content shape."""

    detail: str


@dataclass(frozen=True, slots=True)
class InputEnvelopeVersionUnknown:
    """A content version this build does not carry a decoder for."""

    version: int


type InputDecode = (
    ReconstructedInput | InputEnvelopeCorrupt | InputEnvelopeVersionUnknown
)


def encode_input_envelope_v1(
    *,
    args: tuple[Any, ...] | list[Any],
    kwargs: dict[str, Any],
    options: dict[str, Any] | None,
) -> bytes:
    """Serialize content v1 canonically: compact, sorted keys, UTF-8.

    The same content always produces the same bytes; the digest the
    caller stores is `sha256` over exactly these bytes.
    """
    return json.dumps(
        {'args': list(args), 'kwargs': kwargs, 'options': options},
        separators=(',', ':'),
        sort_keys=True,
        ensure_ascii=False,
    ).encode('utf-8')


def decode_input_envelope(
    *,
    version: int,
    payload: bytes,
    digest: bytes,
) -> InputDecode:
    """Verify the digest, then parse and shape-check content v1."""
    if version != INPUT_ENVELOPE_VERSION:
        return InputEnvelopeVersionUnknown(version=version)
    if sha256(payload).digest() != digest:
        return InputEnvelopeCorrupt(
            detail='payload digest disagrees with the stored digest'
        )
    try:
        parsed: Any = json.loads(payload.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return InputEnvelopeCorrupt(detail=f'payload is not JSON: {error}')
    if not isinstance(parsed, dict):
        return InputEnvelopeCorrupt(detail='content is not an object')
    content = cast(dict[str, Any], parsed)
    if set(content) != {'args', 'kwargs', 'options'}:
        # An absent options key is a foreign shape, never defaults.
        return InputEnvelopeCorrupt(
            detail=f'content keys are {sorted(content)!r}, '
            "not ['args', 'kwargs', 'options']"
        )
    args: Any = content['args']
    kwargs: Any = content['kwargs']
    options: Any = content['options']
    if not isinstance(args, list):
        return InputEnvelopeCorrupt(detail='args is not a list')
    if not isinstance(kwargs, dict):
        return InputEnvelopeCorrupt(detail='kwargs is not an object')
    if options is not None and not isinstance(options, dict):
        return InputEnvelopeCorrupt(
            detail='options is neither an object nor null'
        )
    return ReconstructedInput(
        args=tuple(cast(list[Any], args)),
        kwargs=cast(dict[str, Any], kwargs),
        options=(
            cast(dict[str, Any], options) if options is not None else None
        ),
    )
