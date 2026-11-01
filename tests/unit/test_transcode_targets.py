"""The decoder set that transcode planning accepts targets from."""

from __future__ import annotations

import pytest

from horsies.core.history.archive.versions import (
    ArchiveDomain,
    JSON_CONTENT_TYPE,
    validate_envelope_contract,
)
from horsies.core.history.transcode.outcomes import ArchiveComponent
from horsies.core.history.transcode.targets import (
    HISTORY_ROW_V1_CODEC,
    DecoderEntry,
    DecoderSet,
    current_decoder_set,
)

pytestmark = [pytest.mark.unit]


def test_current_set_is_version_one_of_every_component() -> None:
    assert current_decoder_set().entries == frozenset({
        DecoderEntry(ArchiveComponent.HISTORY_ROW, 1, HISTORY_ROW_V1_CODEC),
        DecoderEntry(ArchiveComponent.RESULT, 1, 'json-utf8'),
        DecoderEntry(ArchiveComponent.ATTEMPTS, 1, 'json-utf8'),
        DecoderEntry(ArchiveComponent.RERUN_INPUT, 1, 'json-utf8'),
    })


def _component_name(entry: DecoderEntry) -> str:
    return entry.component.value


ENVELOPE_ENTRIES: list[DecoderEntry] = sorted(
    (
        entry
        for entry in current_decoder_set().entries
        if entry.component is not ArchiveComponent.HISTORY_ROW
    ),
    key=_component_name,
)
"""A history row has no envelope, and no read checks its version."""


@pytest.mark.parametrize('entry', ENVELOPE_ENTRIES, ids=_component_name)
def test_every_envelope_entry_decodes(entry: DecoderEntry) -> None:
    assert validate_envelope_contract(
        domain=ArchiveDomain[entry.component.value],
        version=entry.version,
        codec=entry.codec,
        content_type=JSON_CONTENT_TYPE,
    ) is None


@pytest.mark.parametrize(
    ('version', 'codec', 'expected'),
    [(1, 'json-utf8', True), (2, 'framed-v2', False), (1, 'framed-v2', False)],
    ids=['member', 'unknown-version', 'unknown-codec'],
)
def test_reads_names_membership(version: int, codec: str, expected: bool) -> None:
    decoders = DecoderSet(entries=frozenset({
        DecoderEntry(ArchiveComponent.RESULT, 1, 'json-utf8'),
    }))
    assert decoders.reads(
        component=ArchiveComponent.RESULT, version=version, codec=codec,
    ) is expected
