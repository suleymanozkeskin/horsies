"""The paired transcode control is the candidate minus the transform."""

from __future__ import annotations

import pytest

from horsies.core.history.transcode import executor as production_executor
from horsies.core.history.transcode.outcomes import ArchiveComponent
from tests.task_history_prototypes import transcode_control
from tests.task_history_prototypes.transcode_control import (
    TranscodeControlError,
    assert_control_matches_candidate_shape,
    assert_control_removes_the_transform,
    plain_copy_and_hash_control,
    plain_copy_projection,
    plain_source_select,
)

COMPONENT_COLUMNS: dict[ArchiveComponent, tuple[str, ...]] = {
    ArchiveComponent.RESULT: (
        'task_id',
        'result_envelope_version',
        'result_codec',
        'result_payload',
        'prior_result_payload',
        'result_digest',
    ),
    ArchiveComponent.ATTEMPTS: (
        'task_id',
        'attempt_archive_version',
        'attempt_snapshot_codec',
        'attempt_snapshot',
        'attempt_snapshot_digest',
    ),
    ArchiveComponent.RERUN_INPUT: (
        'task_id',
        'rerun_input_version',
        'rerun_input_codec',
        'rerun_input_inline',
        'rerun_input_digest',
    ),
}

PAYLOAD_COMPONENTS = tuple(COMPONENT_COLUMNS)


def _kwargs(component: ArchiveComponent) -> dict[str, object]:
    return {
        'component': component,
        'source_version': 1,
        'source_codec': 'v1',
        'target_version': 2,
        'target_codec': 'v2',
        'alias': 'source',
    }


@pytest.mark.parametrize('component', PAYLOAD_COMPONENTS)
def test_control_is_the_candidate_minus_the_transform(
    component: ArchiveComponent,
) -> None:
    """The budget's definition, checked as an exact identity.

    Stripping the transform's generated-column prefix from the candidate
    projection must reproduce the control character for character. Anything
    else differing means the control is not the candidate minus the
    transform, and its ratio would not measure the transform's cost.
    """
    columns = COMPONENT_COLUMNS[component]
    assert_control_matches_candidate_shape(columns, **_kwargs(component))  # type: ignore[arg-type]


@pytest.mark.parametrize('component', PAYLOAD_COMPONENTS)
def test_control_copies_source_bytes_and_still_hashes(
    component: ArchiveComponent,
) -> None:
    columns = COMPONENT_COLUMNS[component]
    candidate = production_executor.transformed_select(
        columns, **_kwargs(component)  # type: ignore[arg-type]
    )
    control = plain_copy_projection(columns, **_kwargs(component))  # type: ignore[arg-type]

    assert 'archive_target_' in candidate, (
        'the candidate must read the transform output, or this component '
        'has no transform to control for'
    )
    assert 'archive_target_' not in control
    assert 'sha256(' in control, (
        'the budget retains hash computation in the control'
    )
    assert_control_removes_the_transform(columns, **_kwargs(component))  # type: ignore[arg-type]


def test_pass_through_source_select_removes_the_decode() -> None:
    for component in PAYLOAD_COMPONENTS:
        assert (
            plain_source_select(
                component,
                alias='source',
                source_version=1,
                source_codec='v1',
                forward=True,
            )
            == 'source.*'
        )


@pytest.mark.parametrize(
    'table_name',
    ['_VERSION_COLUMNS', '_CODEC_COLUMNS', '_DIGEST_COLUMNS'],
)
def test_a_stale_transcribed_column_name_is_caught(table_name: str) -> None:
    """The column names are transcribed from production literals.

    Transcription goes stale silently: a wrong version-column name simply
    stops that column advancing while the rest of the control keeps working,
    and the ratio still looks reasonable. Each table is corrupted in turn and
    the conformance check must reject it.
    """
    component = ArchiveComponent.RESULT
    table: dict[ArchiveComponent, str] = getattr(
        transcode_control, table_name
    )
    original = table[component]
    table[component] = f'stale_{original}'
    try:
        with pytest.raises(TranscodeControlError, match='minus the transform'):
            assert_control_matches_candidate_shape(
                COMPONENT_COLUMNS[component], **_kwargs(component)  # type: ignore[arg-type]
            )
    finally:
        table[component] = original


def test_the_control_context_substitutes_both_projections() -> None:
    """Both seams are patched, not just the visible one.

    The projection carries the transform's output; the source select carries
    the decode that produces it. Patching only the projection would leave the
    control paying for a decode whose result it discards, which understates
    the candidate's advantage rather than removing it.
    """
    before_select = production_executor.encoded_source_select
    before_projection = production_executor.transformed_select

    with plain_copy_and_hash_control():
        assert production_executor.encoded_source_select is plain_source_select
        assert production_executor.transformed_select is plain_copy_projection

    assert production_executor.encoded_source_select is before_select
    assert production_executor.transformed_select is before_projection
