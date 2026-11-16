"""Result-envelope codec, version 1.

A history row stores at most one result: its payload under the row's
result envelope version and digest. An administrative cancellation stores
none.

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
    validate_archive_version,
    verify_payload_digest,
)


def decode_result_envelope(
    *,
    version: int,
    payload: bytes,
    digest: bytes,
) -> ArchiveDecodeResult[Any]:
    """Decode one stored result payload to its JSON value, failing closed."""
    version_failure = validate_archive_version(
        domain=ArchiveDomain.RESULT,
        version=version,
    )
    if version_failure is not None:
        return version_failure
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

