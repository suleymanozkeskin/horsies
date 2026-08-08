"""The paired plain-copy-and-hash control for archive-transcode throughput.

The budget defines this control precisely: it performs the same batching,
durable commits, replacement relation writes, indexes, constraints, and
payload hash computation as the candidate, but copies the existing envelope
bytes without decoding or transforming them.

"Same" is taken literally. The control does not reimplement the copy loop —
it runs the production executor and substitutes the two projections that carry
the transform, so batching, the ctid cursor, the replacement relation and its
indexes and constraints, the commit rhythm, and every outcome type are the
production code path rather than a copy of it. A reimplemented control drifts
from the thing it controls, and the drift is invisible in the ratio it
produces.

Two functions carry the transform and nothing else does:

- ``encoded_source_select`` re-frames payload bytes for the target. The
  control replaces it with a pass-through, which is where the decoding cost
  is removed.
- ``transformed_select`` projects those re-framed bytes into the replacement
  row and hashes them. The control replaces it with a projection over the
  ORIGINAL payload columns, hashing the copied bytes with the same
  ``sha256`` the candidate uses.

The substitution is asserted rather than assumed: a control whose patch missed
would measure the candidate and report a ratio of one.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Final
from unittest.mock import patch

from horsies.core.history.transcode import executor as _executor
from horsies.core.history.transcode.transforms import (
    component_source_condition,
    quoted_identifier,
)
from horsies.core.history.transcode.outcomes import ArchiveComponent

# The marker the transform leaves in generated SQL. Its absence is how the
# control proves it removed the transform rather than merely intending to.
_TRANSFORM_MARKER: Final = 'archive_target_'

_PAYLOAD_COLUMNS: Final[dict[ArchiveComponent, tuple[str, ...]]] = {
    ArchiveComponent.RESULT: ('result_payload', 'prior_result_payload'),
    ArchiveComponent.ATTEMPTS: ('attempt_snapshot',),
    ArchiveComponent.RERUN_INPUT: ('rerun_input_inline',),
}

# Transcribed from the production projection's match arms, which hold them as
# literals rather than exposing them as data. Transcription is a drift risk, so
# `assert_control_matches_candidate_shape` proves the two projections still
# differ in exactly the payload and digest columns and nowhere else.
_DIGEST_COLUMNS: Final[dict[ArchiveComponent, str]] = {
    ArchiveComponent.RESULT: 'result_digest',
    ArchiveComponent.ATTEMPTS: 'attempt_snapshot_digest',
    ArchiveComponent.RERUN_INPUT: 'rerun_input_digest',
}

_VERSION_COLUMNS: Final[dict[ArchiveComponent, str]] = {
    ArchiveComponent.RESULT: 'result_envelope_version',
    ArchiveComponent.ATTEMPTS: 'attempt_archive_version',
    ArchiveComponent.RERUN_INPUT: 'rerun_input_version',
}

_CODEC_COLUMNS: Final[dict[ArchiveComponent, str]] = {
    ArchiveComponent.RESULT: 'result_codec',
    ArchiveComponent.ATTEMPTS: 'attempt_snapshot_codec',
    ArchiveComponent.RERUN_INPUT: 'rerun_input_codec',
}


class TranscodeControlError(Exception):
    """The control could not be established as the budget defines it."""


def plain_source_select(
    component: ArchiveComponent,
    *,
    alias: str,
    source_version: int,
    source_codec: str,
    forward: bool,
) -> str:
    """Pass the source through untouched: no re-framing, no decode.

    Signature-compatible with the production projection it replaces, so the
    executor's SQL is unchanged apart from what this returns.
    """
    del source_version, source_codec, forward
    if component not in _PAYLOAD_COLUMNS and (
        component is not ArchiveComponent.HISTORY_ROW
    ):
        raise TranscodeControlError(
            f'no control projection declared for {component}'
        )
    return f'{alias}.*'


def plain_copy_projection(
    columns: tuple[str, ...],
    *,
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    target_version: int,
    target_codec: str,
    alias: str,
) -> str:
    """Project the replacement row over the ORIGINAL payload bytes.

    Version and codec columns still advance and the digest is still computed,
    because the budget retains hash computation in the control; only the
    payload expression differs, and it is the source column itself.
    """
    if component is ArchiveComponent.HISTORY_ROW:
        return _executor.transformed_select(
            columns,
            component=component,
            source_version=source_version,
            source_codec=source_codec,
            target_version=target_version,
            target_codec=target_codec,
            alias=alias,
        )
    payload_columns = _PAYLOAD_COLUMNS.get(component)
    if payload_columns is None:
        raise TranscodeControlError(
            f'no control projection declared for {component}'
        )
    condition = component_source_condition(
        component,
        alias=alias,
        source_version=source_version,
        source_codec=source_codec,
    )
    expressions = {
        column: f'{alias}.{quoted_identifier(column)}' for column in columns
    }
    expressions[_VERSION_COLUMNS[component]] = (
        f'CASE WHEN {condition} THEN {target_version} '
        f'ELSE {alias}.'
        f'{quoted_identifier(_VERSION_COLUMNS[component])} END'
    )
    expressions[_CODEC_COLUMNS[component]] = (
        f"CASE WHEN {condition} THEN '{target_codec}' "
        f'ELSE {alias}.'
        f'{quoted_identifier(_CODEC_COLUMNS[component])} END'
    )
    copied = [
        f'{alias}.{quoted_identifier(column)}' for column in payload_columns
    ]
    payload = (
        copied[0] if len(copied) == 1 else 'COALESCE(' + ', '.join(copied) + ')'
    )
    digest_column = _DIGEST_COLUMNS[component]
    expressions[digest_column] = (
        f'CASE WHEN {condition} AND {payload} IS NOT NULL '
        f'THEN sha256({payload}) '
        f'ELSE {alias}.{quoted_identifier(digest_column)} END'
    )
    return ', '.join(expressions[column] for column in columns)


def assert_control_removes_the_transform(
    columns: tuple[str, ...],
    *,
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    target_version: int,
    target_codec: str,
    alias: str = 'source',
) -> None:
    """Prove the substitution took effect on generated SQL.

    A patch that missed leaves the candidate running under the control's name
    and reports a ratio of one, which reads as a pass. The transform's
    generated columns are named, so their absence is checkable rather than
    assumed.
    """
    if component is ArchiveComponent.HISTORY_ROW:
        return
    projection = plain_copy_projection(
        columns,
        component=component,
        source_version=source_version,
        source_codec=source_codec,
        target_version=target_version,
        target_codec=target_codec,
        alias=alias,
    )
    if _TRANSFORM_MARKER in projection:
        raise TranscodeControlError(
            'the control projection still references the transform output '
            f'({_TRANSFORM_MARKER}...); it is copying transformed bytes, '
            'not source bytes, and its ratio would be meaningless'
        )
    if 'sha256(' not in projection:
        raise TranscodeControlError(
            'the control projection computes no payload hash; the budget '
            'retains hash computation in the control, so a control without '
            'it would understate the candidate cost it is compared against'
        )


def assert_control_matches_candidate_shape(
    columns: tuple[str, ...],
    *,
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    target_version: int,
    target_codec: str,
    alias: str = 'source',
) -> None:
    """The control must differ from the candidate in the payload alone.

    The column names above are transcribed from literals inside the
    production projection, so they can go stale without anything failing —
    a stale version-column name would simply stop advancing that column
    while every other part of the control kept working, and the ratio would
    still look reasonable.

    This compares the two projections column by column and requires the set
    of differing columns to be exactly the payload columns plus the digest.
    Version and codec must be IDENTICAL, because the control advances them
    exactly as the candidate does.
    """
    if component is ArchiveComponent.HISTORY_ROW:
        return
    candidate = _executor.transformed_select(
        columns,
        component=component,
        source_version=source_version,
        source_codec=source_codec,
        target_version=target_version,
        target_codec=target_codec,
        alias=alias,
    )
    control = plain_copy_projection(
        columns,
        component=component,
        source_version=source_version,
        source_codec=source_codec,
        target_version=target_version,
        target_codec=target_codec,
        alias=alias,
    )
    # "The candidate minus the transform" stated exactly: the transform's only
    # footprint is that it reads its own generated columns, so stripping that
    # prefix from the candidate must reproduce the control character for
    # character. Any other difference — a stale version or codec name, a
    # dropped digest, an extra expression — breaks the identity.
    #
    # Compared whole rather than column by column on purpose: the projection is
    # a comma-joined string whose expressions contain commas of their own, so
    # splitting it compares misaligned fragments and reports agreement that is
    # not there.
    if candidate.replace(_TRANSFORM_MARKER, '') != control:
        raise TranscodeControlError(
            'the control is not the candidate minus the transform. With the '
            f'{_TRANSFORM_MARKER!r} prefix stripped, the candidate projection '
            'does not reproduce the control, so the two differ somewhere '
            'other than the payload bytes — most likely a column name '
            'transcribed from the production projection has gone stale'
        )


@contextmanager
def plain_copy_and_hash_control() -> Generator[None]:
    """Run the production executor with the transform substituted out.

    Everything the budget requires to be identical — batching, durable
    commits, replacement relation writes, indexes, constraints — is the
    production path, because this patches two projections rather than
    reimplementing the loop around them.
    """
    with (
        patch.object(
            _executor, 'encoded_source_select', plain_source_select
        ),
        patch.object(
            _executor, 'transformed_select', plain_copy_projection
        ),
    ):
        yield


def control_conditions() -> dict[str, Any]:
    """What the artifact records about how the control was formed."""
    return {
        'control': 'plain copy and hash',
        'substituted': (
            'encoded_source_select -> pass-through; '
            'transformed_select -> projection over source payload columns'
        ),
        'retained_from_production': (
            'batching, ctid cursor, replacement relation, indexes, '
            'constraints, durable commits, payload hash computation'
        ),
        'construction': (
            'production executor with two projections patched, not a '
            'reimplementation of the copy loop'
        ),
    }
