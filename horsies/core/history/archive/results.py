"""Result-envelope codec, version 1.

A history row stores at most one result-domain value: the canonical result,
or — only for administrative cancellation — the prior result the cancelled
task had produced. Both travel under the same version, codec, content-type,
and digest columns, so the decoder is one function regardless of which
column held the bytes; which column it was is the caller's fact, not the
payload's.

The SQL terminalization functions write these bytes, and this module decodes
them.
"""

from __future__ import annotations

from typing import Any

from horsies.core.codec.json_io import loads_json_bytes
from horsies.core.types.result import is_err

from .versions import (
    ArchiveDecodeResult,
    ArchiveDomain,
    CorruptArchiveValue,
    DecodedArchiveValue,
    validate_envelope_contract,
    verify_payload_digest,
)


def decode_result_envelope(
    *,
    version: int,
    codec: str,
    content_type: str,
    payload: bytes,
    digest: bytes,
) -> ArchiveDecodeResult[Any]:
    """Decode one stored result payload to its JSON value, failing closed."""
    contract_failure = validate_envelope_contract(
        domain=ArchiveDomain.RESULT,
        version=version,
        codec=codec,
        content_type=content_type,
    )
    if contract_failure is not None:
        return contract_failure
    digest_failure = verify_payload_digest(
        domain=ArchiveDomain.RESULT, payload=payload, digest=digest
    )
    if digest_failure is not None:
        return digest_failure
    loaded = loads_json_bytes(payload)
    if is_err(loaded):
        return CorruptArchiveValue(
            domain=ArchiveDomain.RESULT, detail='invalid_json'
        )
    return DecodedArchiveValue(loaded.ok_value)

