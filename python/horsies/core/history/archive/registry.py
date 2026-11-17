"""The retained-version registry: one authority for what must decode.

A retained version must decode. No mechanism rewrites stored rows to
another version, so a version that stored rows may hold stays retained.

Every domain retains exactly version 1.
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
