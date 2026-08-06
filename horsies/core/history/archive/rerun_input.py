"""Rerun-input codec, version 1: one exhaustive stored disposition.

Every eligible terminal history row states what happened to its rerun
input through a single non-null discriminant:

- ``INLINE`` and ``REFERENCE`` are the available states and own the
  versioned envelope fields;
- ``DECLINED_BY_POLICY``, ``OVER_BOUND``, and ``NEVER_ELIGIBLE`` are the
  unavailable states and require every envelope field to be null.

The stored value is a union — an unavailable state that carries a payload
cannot be constructed, and the writer's column values are derived from the
variant rather than assembled by hand. The public unavailable result adds
``MISSING_OBJECT``, which is a reference-resolution failure at read time,
never a stored disposition.

Unknown or corrupt disposition and envelope values are typed decode
failures. They are never reported as unavailable input: unavailable is a
recorded fact about the request, corruption is a broken row.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from .versions import (
    ARCHIVE_VERSION_1,
    ArchiveDecodeResult,
    ArchiveDomain,
    CorruptArchiveValue,
    DIGEST_LENGTH_BYTES,
    DecodedArchiveValue,
    JSON_CONTENT_TYPE,
    JSON_UTF8_CODEC,
    archive_digest,
    validate_envelope_contract,
    verify_payload_digest,
)


RERUN_INPUT_INLINE_MAX_BYTES: Final = 65_536
"""Inclusive inline bound: exactly 65,536 bytes encodes, 65,537 is refused."""

RERUN_INPUT_REFERENCE_MAX_BYTES: Final = 2_048
"""UTF-8 byte bound of a content-addressed reference."""


class RerunInputDisposition(StrEnum):
    """The exhaustive stored discriminant."""

    INLINE = 'INLINE'
    REFERENCE = 'REFERENCE'
    DECLINED_BY_POLICY = 'DECLINED_BY_POLICY'
    OVER_BOUND = 'OVER_BOUND'
    NEVER_ELIGIBLE = 'NEVER_ELIGIBLE'


class RerunInputUnavailability(StrEnum):
    """The three stored unavailable dispositions."""

    DECLINED_BY_POLICY = 'DECLINED_BY_POLICY'
    OVER_BOUND = 'OVER_BOUND'
    NEVER_ELIGIBLE = 'NEVER_ELIGIBLE'


class RerunInputUnavailableReason(StrEnum):
    """What a rerun request is told when input cannot be served.

    Three reasons restate stored dispositions; ``MISSING_OBJECT`` arises
    only when resolving a ``REFERENCE`` whose object no longer exists.
    """

    DECLINED_BY_POLICY = 'DECLINED_BY_POLICY'
    OVER_BOUND = 'OVER_BOUND'
    NEVER_ELIGIBLE = 'NEVER_ELIGIBLE'
    MISSING_OBJECT = 'MISSING_OBJECT'


# ---------------------------------------------------------------------------
# Stored value: states that cannot occur cannot be written down
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InlineRerunInputStored:
    """Available input retained inline, within the inclusive bound."""

    version: int
    codec: str
    content_type: str
    payload: bytes
    digest: bytes


@dataclass(frozen=True, slots=True)
class ReferencedRerunInputStored:
    """Available input retained behind a content-addressed reference."""

    version: int
    codec: str
    content_type: str
    reference: str
    digest: bytes


@dataclass(frozen=True, slots=True)
class UnavailableRerunInputStored:
    """No retained input; the disposition records why."""

    unavailability: RerunInputUnavailability


type StoredRerunInput = (
    InlineRerunInputStored
    | ReferencedRerunInputStored
    | UnavailableRerunInputStored
)


def store_inline_rerun_input(payload: bytes) -> InlineRerunInputStored:
    """Encode input for inline retention; the bound is inclusive.

    Enforced before any stored state exists: an over-bound payload is the
    enqueue classifier's ``OVER_BOUND`` disposition, never a truncated
    inline value.
    """
    if len(payload) > RERUN_INPUT_INLINE_MAX_BYTES:
        raise ValueError(
            f'inline rerun input is {len(payload)} bytes; the inclusive '
            f'bound is {RERUN_INPUT_INLINE_MAX_BYTES}'
        )
    return InlineRerunInputStored(
        version=ARCHIVE_VERSION_1,
        codec=JSON_UTF8_CODEC,
        content_type=JSON_CONTENT_TYPE,
        payload=payload,
        digest=archive_digest(payload),
    )


def store_referenced_rerun_input(
    *,
    reference: str,
    digest: bytes,
) -> ReferencedRerunInputStored:
    """Encode a reference envelope for input retained externally."""
    if not reference:
        raise ValueError('reference must be non-empty')
    if len(reference.encode()) > RERUN_INPUT_REFERENCE_MAX_BYTES:
        raise ValueError(
            f'reference exceeds {RERUN_INPUT_REFERENCE_MAX_BYTES} UTF-8 bytes'
        )
    if len(digest) != DIGEST_LENGTH_BYTES:
        raise ValueError('reference digest must be 32 bytes')
    return ReferencedRerunInputStored(
        version=ARCHIVE_VERSION_1,
        codec=JSON_UTF8_CODEC,
        content_type=JSON_CONTENT_TYPE,
        reference=reference,
        digest=digest,
    )


def store_unavailable_rerun_input(
    unavailability: RerunInputUnavailability,
) -> UnavailableRerunInputStored:
    """Record an unavailable disposition; every envelope field stays null."""
    return UnavailableRerunInputStored(unavailability=unavailability)


def disposition_of(stored: StoredRerunInput) -> RerunInputDisposition:
    """The discriminant column value a writer stores for this variant."""
    match stored:
        case InlineRerunInputStored():
            return RerunInputDisposition.INLINE
        case ReferencedRerunInputStored():
            return RerunInputDisposition.REFERENCE
        case UnavailableRerunInputStored(unavailability=unavailability):
            return RerunInputDisposition(unavailability.value)


# ---------------------------------------------------------------------------
# Decode
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AvailableInlineInput:
    """Decoded inline input, digest-verified."""

    payload: bytes
    digest: bytes


@dataclass(frozen=True, slots=True)
class AvailableReferencedInput:
    """A decoded reference envelope; resolution happens elsewhere.

    Resolving the reference may still yield ``MISSING_OBJECT``; this value
    only proves the stored envelope is well-formed.
    """

    reference: str
    digest: bytes


@dataclass(frozen=True, slots=True)
class UnavailableRerunInput:
    """The request's input cannot be served, for a recorded reason."""

    reason: RerunInputUnavailableReason


type DecodedRerunInput = (
    AvailableInlineInput | AvailableReferencedInput | UnavailableRerunInput
)


def decode_rerun_input(
    *,
    disposition: str,
    version: int | None,
    codec: str | None,
    content_type: str | None,
    digest: bytes | None,
    inline_payload: bytes | None,
    reference: str | None,
) -> ArchiveDecodeResult[DecodedRerunInput]:
    """Decode the stored rerun-input columns, failing closed.

    Unavailable dispositions require every envelope column null; available
    dispositions require exactly their own envelope columns. Any other
    combination — including an unknown discriminant — is corruption, not
    unavailability.
    """
    try:
        stored_disposition = RerunInputDisposition(disposition)
    except ValueError:
        return _corrupt('unknown_disposition')

    match stored_disposition:
        case (
            RerunInputDisposition.DECLINED_BY_POLICY
            | RerunInputDisposition.OVER_BOUND
            | RerunInputDisposition.NEVER_ELIGIBLE
        ):
            if (
                version is not None
                or codec is not None
                or content_type is not None
                or digest is not None
                or inline_payload is not None
                or reference is not None
            ):
                return _corrupt('unavailable_with_envelope_fields')
            return DecodedArchiveValue(
                UnavailableRerunInput(
                    reason=RerunInputUnavailableReason(stored_disposition.value)
                )
            )
        case RerunInputDisposition.INLINE:
            if (
                version is None
                or codec is None
                or content_type is None
                or digest is None
                or inline_payload is None
                or reference is not None
            ):
                return _corrupt('invalid_inline_envelope')
            contract_failure = validate_envelope_contract(
                domain=ArchiveDomain.RERUN_INPUT,
                version=version,
                codec=codec,
                content_type=content_type,
            )
            if contract_failure is not None:
                return contract_failure
            if len(inline_payload) > RERUN_INPUT_INLINE_MAX_BYTES:
                return _corrupt('inline_over_bound')
            digest_failure = verify_payload_digest(
                domain=ArchiveDomain.RERUN_INPUT,
                payload=inline_payload,
                digest=digest,
            )
            if digest_failure is not None:
                return digest_failure
            return DecodedArchiveValue(
                AvailableInlineInput(payload=inline_payload, digest=digest)
            )
        case RerunInputDisposition.REFERENCE:
            if (
                version is None
                or codec is None
                or content_type is None
                or digest is None
                or reference is None
                or not reference
                or inline_payload is not None
            ):
                return _corrupt('invalid_reference_envelope')
            contract_failure = validate_envelope_contract(
                domain=ArchiveDomain.RERUN_INPUT,
                version=version,
                codec=codec,
                content_type=content_type,
            )
            if contract_failure is not None:
                return contract_failure
            if len(digest) != DIGEST_LENGTH_BYTES:
                return _corrupt('invalid_reference_digest')
            return DecodedArchiveValue(
                AvailableReferencedInput(reference=reference, digest=digest)
            )


def _corrupt(detail: str) -> CorruptArchiveValue:
    return CorruptArchiveValue(domain=ArchiveDomain.RERUN_INPUT, detail=detail)
