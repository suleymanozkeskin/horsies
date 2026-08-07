"""Transform-builder pins: the disposition delta and framing identity.

The rerun-input component must ride the ratified five-value
`rerun_input_disposition` column, never the prototype's
`rerun_input_form` — the presence half proves the ratified column is
real in the installed DDL, so the exclusion cannot go vacuous. The
copy and the mismatch counter render the expected target from the same
builders, which is why one pin over the builders covers both readers.
"""

from __future__ import annotations

import pytest

from horsies.core.history.ddl.conditional import (
    GatedFragment,
    gated_fragment,
)
from horsies.core.history.transcode.outcomes import ArchiveComponent
from horsies.core.history.transcode.transforms import (
    component_columns,
    component_source_condition,
    encoded_source_select,
    quoted_identifier,
    transformed_select,
)

pytestmark = [pytest.mark.unit]


class TestDispositionDelta:
    def test_presence_half_the_ratified_column_exists(self) -> None:
        installed = '\n'.join(
            gated_fragment(GatedFragment.RERUN_INPUT_COLUMNS)
        )
        assert 'rerun_input_disposition' in installed

    def test_rerun_input_rides_the_disposition_never_the_form(self) -> None:
        columns = component_columns(ArchiveComponent.RERUN_INPUT)
        condition = component_source_condition(
            ArchiveComponent.RERUN_INPUT,
            alias='source',
            source_version=1,
            source_codec='json-utf8',
        )
        for text_form in (columns.presence_predicate, condition):
            assert 'rerun_input_disposition' in text_form
            assert 'rerun_input_form' not in text_form


class TestFramingTransform:
    def test_forward_prefixes_and_backward_strips(self) -> None:
        forward = encoded_source_select(
            ArchiveComponent.ATTEMPTS,
            alias='source',
            source_version=1,
            source_codec='json-utf8',
            forward=True,
        )
        backward = encoded_source_select(
            ArchiveComponent.ATTEMPTS,
            alias='source',
            source_version=2,
            source_codec='framed-v2',
            forward=False,
        )
        assert "decode('4832', 'hex') ||" in forward
        assert 'substring(' in backward

    def test_metadata_component_projects_the_source_unchanged(self) -> None:
        select = encoded_source_select(
            ArchiveComponent.HISTORY_ROW,
            alias='source',
            source_version=1,
            source_codec='row-v1',
            forward=True,
        )
        assert select == 'source.*'

    def test_transformed_digest_recomputes_over_the_new_bytes(self) -> None:
        select = transformed_select(
            ('task_id', 'attempt_archive_version', 'attempt_snapshot_codec',
             'attempt_snapshot', 'attempt_snapshot_digest'),
            component=ArchiveComponent.ATTEMPTS,
            source_version=1,
            source_codec='json-utf8',
            target_version=2,
            target_codec='framed-v2',
            alias='source',
        )
        assert 'sha256(' in select
        assert 'archive_target_attempt_snapshot' in select


class TestIdentifierDiscipline:
    def test_unsafe_identifiers_are_refused(self) -> None:
        with pytest.raises(ValueError, match='unsafe'):
            quoted_identifier('bad; DROP TABLE x')
        with pytest.raises(ValueError, match='unsafe'):
            quoted_identifier('')
        assert quoted_identifier('attempt_snapshot') == '"attempt_snapshot"'
