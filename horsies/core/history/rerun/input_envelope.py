"""The rerun input envelope, version 1: the bytes the move stores.

The move to history frames the source row's stored ``input`` and
``task_options`` texts byte for byte (``horsies_encode_rerun_input``)::

    {"input":<input>,"options":<task_options, or null>}

The digest covers those bytes. A rerun reads the two texts back exactly
as stored and writes them unchanged into the new row, so the rerun's own
envelope has the same bytes and digest. Decode verifies the digest before
it parses anything, parses through ``loads_json_bytes`` so stored bytes
obey the strict JSON rules of every other read, and refuses any other
framing: an absent ``options`` member is a foreign shape, never defaults.

``encode_input_envelope_v1`` writes a different form,
``{"args","kwargs","options"}``: the enqueue-time prepared envelope that
the broker's ``_cutover_enqueue_values`` still builds. No baseline column
stores it and no reader decodes it.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Final, cast

from horsies.core.codec.json_io import (
    dumps_canonical_json,
    loads_json,
    loads_json_bytes,
)
from horsies.core.types.result import Err, Ok, Result, is_err

INPUT_ENVELOPE_VERSION: Final = 1
INPUT_ENVELOPE_CODEC: Final = 'json-utf8'
INPUT_ENVELOPE_CONTENT_TYPE: Final = 'application/json'
INPUT_ENVELOPE_INLINE_MAX_BYTES: Final = 65536
"""The move stores an envelope inline up to this many bytes; a longer one
is OVER_BOUND and is not stored."""

_ENVELOPE_PREFIX: Final = '{"input":'
_OPTIONS_SEPARATOR: Final = ',"options":'
_ENVELOPE_SUFFIX: Final = '}'
_FRAMING_REFUSED: Final = (
    'content is not framed as {"input":<input>,"options":<options>}'
)


@dataclass(frozen=True, slots=True)
class StoredTaskOptions:
    """The source request's task options: the stored text and its object."""

    text: str
    value: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReconstructedInput:
    """The source request's stored input and options, digest-verified.

    ``input_json`` is the exact ``input`` text of the source row; it is a
    JSON object. ``options`` is None when the source ran on default task
    options.
    """

    input_json: str
    options: StoredTaskOptions | None


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
    """Serialize the prepared enqueue envelope canonically.

    Compact, sorted keys, UTF-8: the same content always produces the same
    bytes. A non-finite float is a code defect: ``dumps_canonical_json``
    raises ``ValueError`` instead of returning bytes that are not JSON.
    """
    return dumps_canonical_json(
        {'args': list(args), 'kwargs': kwargs, 'options': options}
    ).encode('utf-8')


def decode_input_envelope(
    *,
    version: int,
    payload: bytes,
    digest: bytes,
) -> InputDecode:
    """Verify the digest, then split and parse the stored envelope.

    Returns the exact stored texts on success. Every refusal is
    ``InputEnvelopeCorrupt`` with its cause, or
    ``InputEnvelopeVersionUnknown``. Reads nothing else and writes nothing.
    """
    if version != INPUT_ENVELOPE_VERSION:
        return InputEnvelopeVersionUnknown(version=version)
    if sha256(payload).digest() != digest:
        return InputEnvelopeCorrupt(
            detail='payload digest disagrees with the stored digest'
        )
    loaded = loads_json_bytes(payload)
    if is_err(loaded):
        return InputEnvelopeCorrupt(
            detail=f'payload is not JSON: {loaded.err_value}'
        )
    # The strict parse accepted the bytes, so they are UTF-8.
    match _split_frame(payload.decode('utf-8')):
        case InputEnvelopeCorrupt() as refused:
            return refused
        case (input_json, options_json):
            pass
    match _parse_member(input_json):
        case Err(err_value=refused):
            return refused
        case Ok(ok_value=parsed_input):
            pass
    if not isinstance(parsed_input, dict):
        return InputEnvelopeCorrupt(detail='input is not an object')
    match _parse_member(options_json):
        case Err(err_value=refused):
            return refused
        case Ok(ok_value=options):
            pass
    match options:
        case None:
            return ReconstructedInput(input_json=input_json, options=None)
        case dict():
            return ReconstructedInput(
                input_json=input_json,
                options=StoredTaskOptions(
                    text=options_json,
                    value=cast(dict[str, Any], options),
                ),
            )
        case _:
            return InputEnvelopeCorrupt(
                detail='options is neither an object nor null'
            )


def _split_frame(text: str) -> tuple[str, str] | InputEnvelopeCorrupt:
    """The input and options texts of a framed envelope, exactly as stored.

    The separator ``,"options":`` can also occur inside the input, as a
    member of a nested object; a quote inside a JSON string is escaped, so
    it never occurs inside a string. A complete JSON value followed by more
    text is not one value, and a proper prefix of an object is not one
    either, so exactly one occurrence leaves an input text that parses: the
    first that does is the boundary. Each try starts after the previous
    occurrence, so the scan ends within the length of ``text``.
    """
    if not (
        text.startswith(_ENVELOPE_PREFIX) and text.endswith(_ENVELOPE_SUFFIX)
    ):
        return InputEnvelopeCorrupt(detail=_FRAMING_REFUSED)
    input_start = len(_ENVELOPE_PREFIX)
    options_end = len(text) - len(_ENVELOPE_SUFFIX)
    separator = text.find(_OPTIONS_SEPARATOR, input_start, options_end)
    while separator != -1:
        input_json = text[input_start:separator]
        if input_json and not is_err(loads_json(input_json)):
            options_start = separator + len(_OPTIONS_SEPARATOR)
            return input_json, text[options_start:options_end]
        separator = text.find(_OPTIONS_SEPARATOR, separator + 1, options_end)
    return InputEnvelopeCorrupt(detail=_FRAMING_REFUSED)


def _parse_member(member_json: str) -> Result[Any, InputEnvelopeCorrupt]:
    """One member text of the envelope, parsed with the strict rules.

    A member that is not one JSON value means the bytes between the frame
    markers hold more than the frame allows, for example an extra member
    after ``options``.
    """
    match loads_json(member_json):
        case Ok(ok_value=value):
            return Ok(value)
        case Err():
            return Err(InputEnvelopeCorrupt(detail=_FRAMING_REFUSED))
