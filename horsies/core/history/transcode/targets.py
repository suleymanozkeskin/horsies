"""The transcode targets a decoder reads.

A transcode writes rows at its target version and codec. A target that no
decoder in this build reads would make every later read of the moved rows
fail, so planning accepts only targets inside a named decoder set.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..archive.registry import RETAINED_ARCHIVE_VERSIONS
from ..archive.versions import JSON_UTF8_CODEC, ArchiveDomain
from .outcomes import ArchiveComponent

HISTORY_ROW_V1_CODEC = 'row-v1'


@dataclass(frozen=True, slots=True)
class DecoderEntry:
    """One component, version, and codec that a decoder reads."""

    component: ArchiveComponent
    version: int
    codec: str


@dataclass(frozen=True, slots=True)
class DecoderSet:
    """The entries one decoder reads."""

    entries: frozenset[DecoderEntry]

    def reads(
        self,
        *,
        component: ArchiveComponent,
        version: int,
        codec: str,
    ) -> bool:
        return DecoderEntry(component, version, codec) in self.entries


def _domain(component: ArchiveComponent) -> ArchiveDomain:
    match component:
        case ArchiveComponent.HISTORY_ROW:
            return ArchiveDomain.HISTORY_ROW
        case ArchiveComponent.RESULT:
            return ArchiveDomain.RESULT
        case ArchiveComponent.ATTEMPTS:
            return ArchiveDomain.ATTEMPTS
        case ArchiveComponent.RERUN_INPUT:
            return ArchiveDomain.RERUN_INPUT


def _codec(component: ArchiveComponent, version: int) -> str:
    """The codec this build's decoder reads at one retained version."""
    match (component, version):
        case (ArchiveComponent.HISTORY_ROW, 1):
            return HISTORY_ROW_V1_CODEC
        case (_, 1):
            return JSON_UTF8_CODEC
        case _:
            raise AssertionError(
                f'retained {component.value} version {version} has no codec '
                'in this build'
            )


def current_decoder_set() -> DecoderSet:
    """The entries this build's archive decoder reads.

    Built from the retained-version registry, so retaining a version adds
    its entry here, and the codec mapping above must name its codec.
    """
    return DecoderSet(
        entries=frozenset(
            DecoderEntry(component, version, _codec(component, version))
            for component in ArchiveComponent
            for version in RETAINED_ARCHIVE_VERSIONS[_domain(component)]
        )
    )
