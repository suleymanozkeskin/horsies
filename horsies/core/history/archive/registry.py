"""The retained-version registry: one authority for what must decode.

Decoder retirement is an offline transcode concern, but its precondition
lives here: a version is retained until an explicit transcode proves zero
rows require it, and while retained it must decode. The transcode
inventory, the migration preflight, and the decode paths all consult this
mapping rather than hard-coding version literals, so retiring a version is
one change with one review.

At the 0.5.0 cutover every domain retains exactly version 1.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Final, Mapping

from .versions import ArchiveDomain


RETAINED_ARCHIVE_VERSIONS: Final[Mapping[ArchiveDomain, frozenset[int]]] = (
    MappingProxyType(
        {
            ArchiveDomain.HISTORY_ROW: frozenset({1}),
            ArchiveDomain.RESULT: frozenset({1}),
            ArchiveDomain.ATTEMPTS: frozenset({1}),
            ArchiveDomain.RERUN_INPUT: frozenset({1}),
        }
    )
)


def retained_versions(domain: ArchiveDomain) -> frozenset[int]:
    """Every version the named domain must currently decode."""
    return RETAINED_ARCHIVE_VERSIONS[domain]


def is_retained(domain: ArchiveDomain, version: int) -> bool:
    """Whether a stored version is retained for its domain."""
    return version in RETAINED_ARCHIVE_VERSIONS[domain]
