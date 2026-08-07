"""The four archive version domains and their shared decode vocabulary.

Each domain versions independently: a result-envelope change must not
falsely version attempts, rerun input, or the row projection. Version 1 is
the sole retained version of every domain at the 0.5.0 cutover; the
vocabulary here is what lets a later version join one domain without
touching the others.

Codec and content-type discriminants are constrained text rather than
database enums, so introducing a codec never rewrites an enum type. The
decode failure union is shared by every domain codec: unknown version,
unknown codec, unknown content type, corrupt payload, digest mismatch —
and nothing else, so a reader matching over it is exhaustive by
construction.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Final


ARCHIVE_VERSION_1: Final = 1
JSON_UTF8_CODEC: Final = 'json-utf8'
JSON_CONTENT_TYPE: Final = 'application/json'

DIGEST_LENGTH_BYTES: Final = 32
"""Every archive digest is a 32-byte SHA-256 over the stored payload bytes."""


class ArchiveDomain(StrEnum):
    """The four independently versioned archive domains."""

    HISTORY_ROW = 'history_row'
    RESULT = 'result'
    ATTEMPTS = 'attempts'
    RERUN_INPUT = 'rerun_input'


@dataclass(frozen=True, slots=True)
class UnknownArchiveVersion:
    """The stored version is not retained for this domain."""

    domain: ArchiveDomain
    version: int


@dataclass(frozen=True, slots=True)
class UnknownArchiveCodec:
    """The stored codec identifier is not one this version defines."""

    domain: ArchiveDomain
    codec: str


@dataclass(frozen=True, slots=True)
class UnknownArchiveContentType:
    """The stored content type is not one this version defines."""

    domain: ArchiveDomain
    content_type: str


@dataclass(frozen=True, slots=True)
class CorruptArchiveValue:
    """The stored value does not satisfy its own declared format."""

    domain: ArchiveDomain
    detail: str


@dataclass(frozen=True, slots=True)
class ArchiveDigestMismatch:
    """The stored payload does not hash to its stored digest."""

    domain: ArchiveDomain


type ArchiveDecodeFailure = (
    UnknownArchiveVersion
    | UnknownArchiveCodec
    | UnknownArchiveContentType
    | CorruptArchiveValue
    | ArchiveDigestMismatch
)
"""Every way an archive decode fails. Decode failure is never absence."""


@dataclass(frozen=True, slots=True)
class DecodedArchiveValue[T]:
    """A successful decode, wrapped so failure cannot be mistaken for it."""

    value: T


type ArchiveDecodeResult[T] = DecodedArchiveValue[T] | ArchiveDecodeFailure


def archive_digest(payload: bytes) -> bytes:
    """The canonical 32-byte SHA-256 digest of stored payload bytes."""
    return hashlib.sha256(payload).digest()


def decode_history_row_version(version: int) -> ArchiveDecodeResult[int]:
    """Validate a stored history-row schema version."""
    match version:
        case 1:
            return DecodedArchiveValue(version)
        case _:
            return UnknownArchiveVersion(ArchiveDomain.HISTORY_ROW, version)


def validate_envelope_contract(
    *,
    domain: ArchiveDomain,
    version: int,
    codec: str,
    content_type: str,
) -> ArchiveDecodeFailure | None:
    """Check the version/codec/content-type triple of a stored envelope.

    Version is judged first: an unknown version cannot know which codecs it
    defines, so it reports as unknown version rather than unknown codec.
    """
    match version:
        case 1 if codec != JSON_UTF8_CODEC:
            return UnknownArchiveCodec(domain=domain, codec=codec)
        case 1:
            pass
        case _:
            return UnknownArchiveVersion(domain=domain, version=version)
    if content_type != JSON_CONTENT_TYPE:
        return UnknownArchiveContentType(domain=domain, content_type=content_type)
    return None


def verify_payload_digest(
    *,
    domain: ArchiveDomain,
    payload: bytes,
    digest: bytes,
) -> ArchiveDigestMismatch | None:
    """Compare stored payload bytes against their stored digest."""
    if archive_digest(payload) != digest:
        return ArchiveDigestMismatch(domain=domain)
    return None
