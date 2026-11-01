"""The retained-version registry: one authority for what must decode.

Decoder retirement is an offline transcode concern, but its precondition
lives here: a version is retained until an explicit transcode proves zero
rows require it, and while retained it must decode. The transcode decoder
set reads this mapping rather than hard-coding version literals, so retiring
a version is one change with one review.

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
