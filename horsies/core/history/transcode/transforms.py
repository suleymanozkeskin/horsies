"""Per-component transform SQL for the transcode copy and verification.

Pure statement builders: the copy stage and the mismatch counter both
render their SELECTs from here, so the expected-target definition is
one text in both places — verification compares against exactly what
the copy wrote, by construction. The rerun-input component carries the
ratified five-value `rerun_input_disposition` column, never the
prototype's pre-ratification `rerun_input_form`.
"""

from __future__ import annotations

from dataclasses import dataclass

from .outcomes import ArchiveComponent


@dataclass(frozen=True, slots=True)
class ComponentColumns:
    """The physical columns implementing one archive component."""

    version: str
    codec: str
    payload: str
    presence_predicate: str
    metadata_only: bool


def component_columns(component: ArchiveComponent) -> ComponentColumns:
    match component:
        case ArchiveComponent.HISTORY_ROW:
            return ComponentColumns(
                version='history_schema_version',
                codec=(
                    'CASE history_schema_version '
                    "WHEN 1 THEN 'row-v1' WHEN 2 THEN 'row-v2' END"
                ),
                payload='NULL::bytea',
                presence_predicate='TRUE',
                metadata_only=True,
            )
        case ArchiveComponent.RESULT:
            return ComponentColumns(
                version='result_envelope_version',
                codec='result_codec',
                payload='COALESCE(result_payload, prior_result_payload)',
                presence_predicate=(
                    'result_payload IS NOT NULL '
                    'OR prior_result_payload IS NOT NULL'
                ),
                metadata_only=False,
            )
        case ArchiveComponent.ATTEMPTS:
            return ComponentColumns(
                version='attempt_archive_version',
                codec='attempt_snapshot_codec',
                payload='attempt_snapshot',
                presence_predicate='attempt_snapshot IS NOT NULL',
                metadata_only=False,
            )
        case ArchiveComponent.RERUN_INPUT:
            return ComponentColumns(
                version='rerun_input_version',
                codec='rerun_input_codec',
                payload='rerun_input_inline',
                presence_predicate=(
                    "rerun_input_disposition IN ('INLINE', 'REFERENCE')"
                ),
                metadata_only=False,
            )


def quoted_identifier(value: str) -> str:
    """Quote one identifier, refusing anything unsafe."""
    if not value or len(value) > 63 or not value.replace('_', '').isalnum():
        raise ValueError(f'unsafe PostgreSQL identifier: {value!r}')
    return '"' + value + '"'


def column_list(columns: tuple[str, ...]) -> str:
    return ', '.join(quoted_identifier(column) for column in columns)


def encoded_payload_name(payload_column: str) -> str:
    return f'archive_target_{payload_column}'


def component_source_condition(
    component: ArchiveComponent,
    *,
    alias: str,
    source_version: int,
    source_codec: str,
) -> str:
    """The predicate selecting rows the transcode transforms."""
    if component is ArchiveComponent.HISTORY_ROW:
        return f'{alias}.history_schema_version = {source_version}'
    columns = component_columns(component)
    match component:
        case ArchiveComponent.RESULT:
            presence = (
                f'{alias}.result_payload IS NOT NULL '
                f'OR {alias}.prior_result_payload IS NOT NULL'
            )
        case ArchiveComponent.ATTEMPTS:
            presence = f'{alias}.attempt_snapshot IS NOT NULL'
        case ArchiveComponent.RERUN_INPUT:
            presence = (
                f"{alias}.rerun_input_disposition IN "
                "('INLINE', 'REFERENCE')"
            )
    return (
        f'{alias}.{columns.version} = {source_version} '
        f"AND {alias}.{columns.codec} = '{source_codec}' "
        f'AND ({presence})'
    )


def encoded_source_select(
    component: ArchiveComponent,
    *,
    alias: str,
    source_version: int,
    source_codec: str,
    forward: bool,
) -> str:
    """The source projection with payload bytes re-framed for the target.

    The framing transform is byte-level and deterministic: forward adds
    the two-byte frame prefix, backward strips it; rows outside the
    source condition pass through untouched.
    """
    condition = component_source_condition(
        component,
        alias=alias,
        source_version=source_version,
        source_codec=source_codec,
    )
    match component:
        case ArchiveComponent.HISTORY_ROW:
            return f'{alias}.*'
        case ArchiveComponent.RESULT:
            payload_columns = ('result_payload', 'prior_result_payload')
        case ArchiveComponent.ATTEMPTS:
            payload_columns = ('attempt_snapshot',)
        case ArchiveComponent.RERUN_INPUT:
            payload_columns = ('rerun_input_inline',)
    encoded = [f'{alias}.*']
    for payload_column in payload_columns:
        source = f'{alias}.{quoted_identifier(payload_column)}'
        transformed = (
            f"decode('4832', 'hex') || {source}"
            if forward
            else f'substring({source} FROM 3)'
        )
        encoded.append(
            f'CASE WHEN {condition} AND {source} IS NOT NULL '
            f'THEN {transformed} ELSE {source} END AS '
            f'{quoted_identifier(encoded_payload_name(payload_column))}'
        )
    return ', '.join(encoded)


def transformed_select(
    columns: tuple[str, ...],
    *,
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    target_version: int,
    target_codec: str,
    alias: str,
) -> str:
    """The full replacement-row projection over the encoded source."""
    condition = component_source_condition(
        component,
        alias=alias,
        source_version=source_version,
        source_codec=source_codec,
    )
    expressions = {
        column: f'{alias}.{quoted_identifier(column)}'
        for column in columns
    }
    match component:
        case ArchiveComponent.HISTORY_ROW:
            expressions['history_schema_version'] = (
                f'CASE WHEN {condition} THEN {target_version} '
                f'ELSE {alias}.history_schema_version END'
            )
        case ArchiveComponent.RESULT:
            _apply_payload_transform(
                expressions,
                condition=condition,
                alias=alias,
                version_column='result_envelope_version',
                codec_column='result_codec',
                payload_columns=('result_payload', 'prior_result_payload'),
                digest_column='result_digest',
                target_version=target_version,
                target_codec=target_codec,
            )
        case ArchiveComponent.ATTEMPTS:
            _apply_payload_transform(
                expressions,
                condition=condition,
                alias=alias,
                version_column='attempt_archive_version',
                codec_column='attempt_snapshot_codec',
                payload_columns=('attempt_snapshot',),
                digest_column='attempt_snapshot_digest',
                target_version=target_version,
                target_codec=target_codec,
            )
        case ArchiveComponent.RERUN_INPUT:
            _apply_payload_transform(
                expressions,
                condition=condition,
                alias=alias,
                version_column='rerun_input_version',
                codec_column='rerun_input_codec',
                payload_columns=('rerun_input_inline',),
                digest_column='rerun_input_digest',
                target_version=target_version,
                target_codec=target_codec,
            )
    return ', '.join(expressions[column] for column in columns)


def _apply_payload_transform(
    expressions: dict[str, str],
    *,
    condition: str,
    alias: str,
    version_column: str,
    codec_column: str,
    payload_columns: tuple[str, ...],
    digest_column: str,
    target_version: int,
    target_codec: str,
) -> None:
    expressions[version_column] = (
        f'CASE WHEN {condition} THEN {target_version} '
        f'ELSE {alias}.{quoted_identifier(version_column)} END'
    )
    expressions[codec_column] = (
        f"CASE WHEN {condition} THEN '{target_codec}' "
        f'ELSE {alias}.{quoted_identifier(codec_column)} END'
    )
    transformed_payloads: list[str] = []
    for payload_column in payload_columns:
        transformed = (
            f'{alias}.'
            f'{quoted_identifier(encoded_payload_name(payload_column))}'
        )
        expressions[payload_column] = transformed
        transformed_payloads.append(transformed)
    payload = (
        transformed_payloads[0]
        if len(transformed_payloads) == 1
        else 'COALESCE(' + ', '.join(transformed_payloads) + ')'
    )
    expressions[digest_column] = (
        f'CASE WHEN {condition} AND {payload} IS NOT NULL '
        f'THEN sha256({payload}) '
        f'ELSE {alias}.{quoted_identifier(digest_column)} END'
    )


def replacement_bound_name(job_id: str, relation_ordinal: int) -> str:
    suffix = job_id.replace('-', '')[:12]
    return f'archive_replacement_bound_{suffix}_{relation_ordinal}'


def replacement_index_name(job_id: str, relation_ordinal: int) -> str:
    suffix = job_id.replace('-', '')[:12]
    return f'archive_replacement_id_{suffix}_{relation_ordinal}'


def replacement_relation_name(job_id: str, relation_ordinal: int) -> str:
    suffix = job_id.replace('-', '')[:12]
    return f'archive_replacement_{suffix}_{relation_ordinal}'


def backup_relation_name(job_id: str, relation_ordinal: int) -> str:
    suffix = job_id.replace('-', '')[:12]
    return f'archive_replaced_{suffix}_{relation_ordinal}'
