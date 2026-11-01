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

from dataclasses import dataclass
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


# ---------------------------------------------------------------------------
# Result-domain column classification
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CanonicalResultPayload:
    """The row's result is the task's own terminal result."""

    payload: bytes


@dataclass(frozen=True, slots=True)
class AdministrativePriorResult:
    """Administrative cancellation: canonical result is null, prior retained.

    Exposed only through task detail, never lists or facets — the caller
    owning that projection rule receives the distinction as a type.
    """

    payload: bytes


@dataclass(frozen=True, slots=True)
class NoResultPayload:
    """The row carries no result-domain value."""


type ResultPayloadSelection = (
    CanonicalResultPayload
    | AdministrativePriorResult
    | NoResultPayload
    | CorruptArchiveValue
)


def select_result_payload(
    *,
    canonical: bytes | None,
    prior: bytes | None,
) -> ResultPayloadSelection:
    """Classify which result-domain value a row carries.

    Both columns populated violates the mutual-exclusion CHECK and decodes
    as corruption: shared envelope metadata cannot describe two payloads,
    so no reader may pick one and continue.
    """
    match canonical, prior:
        case None, None:
            return NoResultPayload()
        case bytes() as value, None:
            return CanonicalResultPayload(payload=value)
        case None, bytes() as value:
            return AdministrativePriorResult(payload=value)
        case _:
            return CorruptArchiveValue(
                domain=ArchiveDomain.RESULT,
                detail='canonical_and_prior_both_present',
            )
