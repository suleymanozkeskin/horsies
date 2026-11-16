"""The four archive version domains and their shared decode vocabulary.

Each domain versions independently: a result-envelope change must not
falsely version attempts, rerun input, or the row projection. Version 1 is
the sole retained version of every domain; the vocabulary here is what
lets a later version join one domain without touching the others.

The version is the whole format contract. History stores no codec or
content-type column: version 1 of every domain is UTF-8 JSON. The decode
failure union is shared by every domain codec: unknown version, corrupt
payload, digest mismatch — and nothing else, so a reader matching over it
is exhaustive by construction.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum


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


def validate_archive_version(
    *,
    domain: ArchiveDomain,
    version: int,
) -> UnknownArchiveVersion | None:
    """Check that a stored envelope version is one this build decodes.

    Returns ``None`` for a retained version. Reads no payload.
    """
    match version:
        case 1:
            return None
        case _:
            return UnknownArchiveVersion(domain=domain, version=version)


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
