"""Correctness gates for disposable task-history archive representations."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from tests.task_history_prototypes.archive import (
    ARCHIVE_CODEC,
    ARCHIVE_VERSION,
    DecodedArchiveValue,
    InlineRerunInput,
    ReferencedRerunInput,
    RerunInputForm,
    archive_digest,
    decode_attempts,
    decode_rerun_input,
    encode_attempts,
    encode_json_value,
    prototype_attempts,
    store_inline_rerun_input,
    store_referenced_rerun_input,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.measurements import (
    AdministrativeResultCandidate,
    RerunStorageCandidate,
    measure_administrative_result_candidate,
    measure_attempt_storage_candidates,
    measure_rerun_storage_candidate,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def archive_schema(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - installs the v26 base schema
) -> AsyncIterator[AsyncConnection]:
    schema = PrototypeSchema(f'history_archive_{uuid4().hex[:12]}')
    connection = await engine.connect()
    try:
        await install_archive_candidates(connection, schema)
        await connection.commit()
        connection.info['task_history_schema'] = schema
        yield connection
    finally:
        await connection.rollback()
        await remove_archive_candidates(connection, schema)
        await connection.commit()
        await connection.close()


def _schema(connection: AsyncConnection) -> PrototypeSchema:
    schema = connection.info.get('task_history_schema')
    assert isinstance(schema, PrototypeSchema)
    return schema


async def test_aggregate_history_projection_is_exact_and_bounded(
    archive_schema: AsyncConnection,
) -> None:
    schema = _schema(archive_schema)
    attributes = (
        await archive_schema.execute(
            text(
                """
                SELECT attribute.attname,
                       format_type(attribute.atttypid, attribute.atttypmod),
                       attribute.attnotnull
                FROM pg_attribute AS attribute
                JOIN pg_class AS relation
                  ON relation.oid = attribute.attrelid
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = :schema_name
                  AND relation.relname = 'history_aggregate'
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
                ORDER BY attribute.attnum
                """
            ),
            {'schema_name': schema.name},
        )
    ).all()
    expected = [
        ('task_id', 'character varying(36)', True),
        ('task_name', 'character varying(255)', True),
        ('queue_name', 'character varying(100)', True),
        ('priority', 'integer', True),
        ('command_fingerprint_version', 'smallint', True),
        ('command_fingerprint', 'bytea', True),
        ('status', 'text', True),
        ('terminalization_kind', 'character varying(32)', True),
        ('terminal_at', 'timestamp with time zone', True),
        ('retention_anchor_at', 'timestamp with time zone', True),
        ('retention_class_key', 'character varying(64)', True),
        ('sent_at', 'timestamp with time zone', False),
        ('enqueued_at', 'timestamp with time zone', True),
        ('claimed_at', 'timestamp with time zone', False),
        ('started_at', 'timestamp with time zone', False),
        ('created_at', 'timestamp with time zone', True),
        ('good_until', 'timestamp with time zone', False),
        ('retry_count', 'integer', True),
        ('max_retries', 'integer', True),
        ('last_claimed_worker_id', 'character varying(255)', False),
        ('last_worker_hostname', 'character varying(255)', False),
        ('last_worker_pid', 'integer', False),
        ('last_worker_process_name', 'character varying(255)', False),
        ('result_envelope_version', 'smallint', True),
        ('result_codec', 'character varying(64)', True),
        ('result_content_type', 'character varying(255)', True),
        ('result_payload', 'bytea', False),
        ('result_digest', 'bytea', False),
        ('error_code', 'text', False),
        ('final_failed_reason', 'text', False),
        ('prior_result_payload', 'bytea', False),
        ('rerun_of_task_id', 'character varying(36)', False),
        ('rerun_root_task_id', 'character varying(36)', False),
        ('input_digest', 'bytea', False),
        ('rerun_input_version', 'smallint', False),
        ('rerun_input_codec', 'character varying(64)', False),
        ('rerun_input_content_type', 'character varying(255)', False),
        ('rerun_input_form', 'character varying(16)', False),
        ('rerun_input_digest', 'bytea', False),
        ('rerun_input_inline', 'bytea', False),
        ('rerun_input_reference', 'character varying(2048)', False),
        ('workflow_id', 'character varying(36)', False),
        ('is_workflow_task', 'boolean', True),
        ('history_schema_version', 'smallint', True),
        ('attempt_archive_version', 'smallint', True),
        ('attempt_snapshot_codec', 'character varying(64)', True),
        ('attempt_snapshot_content_type', 'character varying(255)', True),
        ('attempt_snapshot', 'bytea', True),
        ('attempt_snapshot_digest', 'bytea', True),
    ]
    assert [tuple(attribute) for attribute in attributes] == expected
    defaults = (
        await archive_schema.execute(
            text(
                """
                SELECT attribute.attname
                FROM pg_attrdef AS default_value
                JOIN pg_class AS relation
                  ON relation.oid = default_value.adrelid
                JOIN pg_namespace AS namespace
                  ON namespace.oid = relation.relnamespace
                JOIN pg_attribute AS attribute
                  ON attribute.attrelid = default_value.adrelid
                 AND attribute.attnum = default_value.adnum
                WHERE namespace.nspname = :schema_name
                  AND relation.relname = 'history_aggregate'
                """
            ),
            {'schema_name': schema.name},
        )
    ).scalars()
    assert list(defaults) == []


def _history_parameters(
    *,
    task_id: str,
    encoded_attempts: bytes | None,
    attempts_digest: bytes | None,
    rerun_input_form: str | None = None,
    rerun_input_inline: bytes | None = None,
    rerun_input_reference: str | None = None,
    rerun_input_digest: bytes | None = None,
) -> dict[str, object]:
    terminal_at = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)
    result = encode_json_value({'ok': True})
    return {
        'task_id': task_id,
        'terminal_at': terminal_at,
        'result_payload': result.payload,
        'result_digest': result.digest,
        'attempt_snapshot': encoded_attempts,
        'attempt_snapshot_digest': attempts_digest,
        'rerun_input_version': ARCHIVE_VERSION if rerun_input_form else None,
        'rerun_input_codec': ARCHIVE_CODEC if rerun_input_form else None,
        'rerun_input_content_type': (
            'application/json' if rerun_input_form else None
        ),
        'rerun_input_form': rerun_input_form,
        'rerun_input_digest': rerun_input_digest,
        'rerun_input_inline': rerun_input_inline,
        'rerun_input_reference': rerun_input_reference,
    }


def _insert_history_sql(schema: PrototypeSchema, table: str, aggregate: bool) -> str:
    attempt_columns = (
        ', attempt_archive_version, attempt_snapshot_codec, '
        'attempt_snapshot_content_type, attempt_snapshot, '
        'attempt_snapshot_digest'
        if aggregate
        else ', attempt_archive_version'
    )
    attempt_values = (
        ", :archive_version, :archive_codec, 'application/json', "
        ':attempt_snapshot, '
        ':attempt_snapshot_digest'
        if aggregate
        else ', :archive_version'
    )
    return f"""
        INSERT INTO {schema.sql}.{table} (
            task_id, task_name, queue_name, priority,
            command_fingerprint_version, command_fingerprint, status,
            terminalization_kind, terminal_at, retention_anchor_at,
            retention_class_key, enqueued_at, created_at,
            result_envelope_version, result_codec, result_content_type,
            result_payload, result_digest, retry_count, max_retries,
            is_workflow_task,
            history_schema_version, rerun_input_version,
            rerun_input_codec, rerun_input_content_type, rerun_input_form,
            rerun_input_digest,
            rerun_input_inline, rerun_input_reference
            {attempt_columns}
        ) VALUES (
            :task_id, 'prototype.task', 'default', 100,
            1, decode(repeat('ab', 32), 'hex'), 'FAILED',
            'FAIL_RUNNING', :terminal_at, :terminal_at,
            'finite_30d_v1', :terminal_at, :terminal_at,
            :archive_version, :archive_codec, 'application/json',
            :result_payload, :result_digest, 20, 20, FALSE,
            :archive_version, :rerun_input_version,
            :rerun_input_codec, :rerun_input_content_type,
            :rerun_input_form, :rerun_input_digest,
            :rerun_input_inline, :rerun_input_reference
            {attempt_values}
        )
    """


async def test_aggregate_snapshot_preserves_complete_attempt_sequence(
    archive_schema: AsyncConnection,
) -> None:
    schema = _schema(archive_schema)
    attempts = prototype_attempts(21)
    snapshot = encode_attempts(attempts)
    parameters = _history_parameters(
        task_id=str(uuid4()),
        encoded_attempts=snapshot.payload,
        attempts_digest=snapshot.digest,
    )
    parameters.update(archive_version=ARCHIVE_VERSION, archive_codec=ARCHIVE_CODEC)

    await archive_schema.execute(
        text(_insert_history_sql(schema, 'history_aggregate', aggregate=True)),
        parameters,
    )
    stored = (
        await archive_schema.execute(
            text(
                f"""
                SELECT attempt_archive_version, attempt_snapshot_codec,
                       attempt_snapshot_content_type, attempt_snapshot,
                       attempt_snapshot_digest
                FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': parameters['task_id']},
        )
    ).one()

    decoded = decode_attempts(
        version=stored.attempt_archive_version,
        codec=stored.attempt_snapshot_codec,
        content_type=stored.attempt_snapshot_content_type,
        payload=bytes(stored.attempt_snapshot),
        digest=bytes(stored.attempt_snapshot_digest),
    )
    assert decoded == DecodedArchiveValue(attempts)


async def test_copartitioned_attempts_preserve_order_and_attribution(
    archive_schema: AsyncConnection,
) -> None:
    schema = _schema(archive_schema)
    task_id = str(uuid4())
    terminal_at = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)
    parameters = _history_parameters(
        task_id=task_id,
        encoded_attempts=None,
        attempts_digest=None,
    )
    parameters.update(archive_version=ARCHIVE_VERSION, archive_codec=ARCHIVE_CODEC)
    await archive_schema.execute(
        text(_insert_history_sql(schema, 'history_copartitioned', aggregate=False)),
        parameters,
    )

    for attempt in prototype_attempts(21):
        await archive_schema.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.attempts_copartitioned (
                    task_id, retention_class_key, retention_anchor_at,
                    attempt_archive_version, attempt, outcome, will_retry,
                    started_at, finished_at, error_code, error_message,
                    failed_reason, worker_id, worker_hostname, worker_pid,
                    worker_process_name
                ) VALUES (
                    :task_id, 'finite_30d_v1', :terminal_at,
                    :version, :attempt, :outcome, :will_retry,
                    :started_at, :finished_at, :error_code, :error_message,
                    :failed_reason, :worker_id, :worker_hostname, :worker_pid,
                    :worker_process_name
                )
                """
            ),
            {
                **asdict(attempt),
                'task_id': task_id,
                'terminal_at': terminal_at,
                'version': ARCHIVE_VERSION,
            },
        )

    rows = (
        await archive_schema.execute(
            text(
                f"""
                SELECT attempt, outcome, will_retry, failed_reason, worker_id
                FROM {schema.sql}.attempts_copartitioned
                WHERE task_id = :task_id
                ORDER BY attempt
                """
            ),
            {'task_id': task_id},
        )
    ).all()
    assert [row.attempt for row in rows] == list(range(1, 22))
    assert rows[0].failed_reason == 'worker failure'
    assert rows[0].worker_id == 'worker-1'
    assert rows[-1].outcome == 'COMPLETED'
    assert rows[-1].will_retry is False


@pytest.mark.parametrize('form', [RerunInputForm.INLINE, RerunInputForm.REFERENCE])
async def test_rerun_input_discriminant_round_trips(
    archive_schema: AsyncConnection,
    form: RerunInputForm,
) -> None:
    schema = _schema(archive_schema)
    payload = b'{"args":[],"kwargs":{"value":42}}'
    match form:
        case RerunInputForm.INLINE:
            stored = store_inline_rerun_input(payload)
        case RerunInputForm.REFERENCE:
            stored = store_referenced_rerun_input(
                reference='sha256://prototype/input',
                payload=payload,
            )
    attempts = encode_attempts(prototype_attempts(1))
    parameters = _history_parameters(
        task_id=str(uuid4()),
        encoded_attempts=attempts.payload,
        attempts_digest=attempts.digest,
        rerun_input_form=stored.form,
        rerun_input_inline=stored.inline_payload,
        rerun_input_reference=stored.reference,
        rerun_input_digest=stored.digest,
    )
    parameters.update(archive_version=ARCHIVE_VERSION, archive_codec=ARCHIVE_CODEC)
    await archive_schema.execute(
        text(_insert_history_sql(schema, 'history_aggregate', aggregate=True)),
        parameters,
    )
    row = (
        await archive_schema.execute(
            text(
                f"""
                SELECT rerun_input_version, rerun_input_codec,
                       rerun_input_content_type, rerun_input_form,
                       rerun_input_digest,
                       rerun_input_inline, rerun_input_reference
                FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': parameters['task_id']},
        )
    ).one()
    decoded = decode_rerun_input(
        version=row.rerun_input_version,
        codec=row.rerun_input_codec,
        content_type=row.rerun_input_content_type,
        form=row.rerun_input_form,
        digest=bytes(row.rerun_input_digest),
        inline_payload=(
            bytes(row.rerun_input_inline)
            if row.rerun_input_inline is not None
            else None
        ),
        reference=row.rerun_input_reference,
    )
    match form, decoded:
        case RerunInputForm.INLINE, DecodedArchiveValue(
            value=InlineRerunInput(payload=decoded_payload)
        ):
            assert decoded_payload == payload
        case RerunInputForm.REFERENCE, DecodedArchiveValue(
            value=ReferencedRerunInput(reference=decoded_reference)
        ):
            assert decoded_reference == 'sha256://prototype/input'
        case _:
            pytest.fail(f'unexpected decode result: {decoded!r}')


async def test_completed_and_workflow_rows_reject_rerun_input(
    archive_schema: AsyncConnection,
) -> None:
    schema = _schema(archive_schema)
    columns = """
        task_id, task_name, queue_name, priority,
        command_fingerprint_version, command_fingerprint, status,
        terminalization_kind, terminal_at, retention_anchor_at,
        retention_class_key, enqueued_at, created_at,
        result_envelope_version, result_codec, result_content_type,
        retry_count, max_retries, is_workflow_task, history_schema_version,
        rerun_input_version, rerun_input_codec, rerun_input_content_type,
        rerun_input_form,
        rerun_input_digest, rerun_input_inline,
        attempt_archive_version, attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot, attempt_snapshot_digest
    """
    values = """
        :task_id, 'prototype.task', 'default', 100,
        1, decode(repeat('ab', 32), 'hex'), :status,
        'COMPLETE_LOCKED', :terminal_at, :terminal_at,
        'finite_30d_v1', :terminal_at, :terminal_at,
        1, 'json-utf8', 'application/json', 0, 0,
        :is_workflow_task, 1,
        1, 'json-utf8', 'application/json', 'INLINE', :digest, :payload,
        1, 'json-utf8', 'application/json', :attempts, :attempts_digest
    """
    attempts = encode_attempts(prototype_attempts(1))
    for status, is_workflow_task in (
        ('COMPLETED', False),
        ('FAILED', True),
    ):
        with pytest.raises(IntegrityError, match='check constraint'):
            await archive_schema.execute(
                text(
                    f'INSERT INTO {schema.sql}.history_aggregate '
                    f'({columns}) VALUES ({values})'
                ),
                {
                    'task_id': str(uuid4()),
                    'status': status,
                    'terminal_at': datetime(2026, 8, 5, tzinfo=timezone.utc),
                    'is_workflow_task': is_workflow_task,
                    'digest': b'0' * 32,
                    'payload': b'{}',
                    'attempts': attempts.payload,
                    'attempts_digest': attempts.digest,
                },
            )
        await archive_schema.rollback()


async def test_administrative_cancel_cannot_present_prior_result_as_disposition(
    archive_schema: AsyncConnection,
) -> None:
    schema = _schema(archive_schema)
    with pytest.raises(IntegrityError, match='check constraint'):
        await archive_schema.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.history_aggregate (
                    task_id, task_name, queue_name, priority,
                    command_fingerprint_version, command_fingerprint, status,
                    terminalization_kind, terminal_at, retention_anchor_at,
                    retention_class_key, enqueued_at, created_at,
                    result_envelope_version, result_codec,
                    result_content_type, result_payload,
                    retry_count, max_retries, is_workflow_task,
                    history_schema_version,
                    attempt_archive_version, attempt_snapshot_codec,
                    attempt_snapshot_content_type,
                    attempt_snapshot, attempt_snapshot_digest
                ) VALUES (
                    :task_id, 'prototype.task', 'default', 100,
                    1, decode(repeat('ab', 32), 'hex'), 'CANCELLED',
                    'CANCEL_ADMIN', :terminal_at, :terminal_at,
                    'finite_30d_v1', :terminal_at, :terminal_at,
                    1, 'json-utf8', 'application/json', :prior_result,
                    0, 0, FALSE, 1,
                    1, 'json-utf8', 'application/json',
                    :attempts, :attempts_digest
                )
                """
            ),
            {
                'task_id': str(uuid4()),
                'terminal_at': datetime(2026, 8, 5, tzinfo=timezone.utc),
                'prior_result': b'{"old":"success"}',
                'attempts': b'[]',
                'attempts_digest': b'0' * 32,
            },
        )


async def test_named_prior_result_is_separate_from_cancel_disposition(
    archive_schema: AsyncConnection,
) -> None:
    schema = _schema(archive_schema)
    attempts = encode_attempts(prototype_attempts(1))
    task_id = str(uuid4())
    terminal_at = datetime(2026, 8, 5, tzinfo=timezone.utc)
    prior_result = b'{"old":"success"}'
    await archive_schema.execute(
        text(
            f"""
            INSERT INTO {schema.sql}.history_aggregate (
                task_id, task_name, queue_name, priority,
                command_fingerprint_version, command_fingerprint, status,
                terminalization_kind, terminal_at, retention_anchor_at,
                retention_class_key, enqueued_at, created_at,
                result_envelope_version, result_codec, result_content_type,
                result_payload,
                result_digest, prior_result_payload,
                retry_count, max_retries, is_workflow_task,
                history_schema_version, attempt_archive_version,
                attempt_snapshot_codec, attempt_snapshot_content_type,
                attempt_snapshot,
                attempt_snapshot_digest
            ) VALUES (
                :task_id, 'prototype.task', 'default', 100,
                1, decode(repeat('ab', 32), 'hex'), 'CANCELLED',
                'CANCEL_ADMIN', :terminal_at, :terminal_at,
                'finite_30d_v1', :terminal_at, :terminal_at,
                1, 'json-utf8', 'application/json', NULL,
                :prior_result_digest, :prior_result, 0, 0, FALSE,
                1, 1, 'json-utf8', 'application/json',
                :attempts, :attempts_digest
            )
            """
        ),
        {
            'task_id': task_id,
            'terminal_at': terminal_at,
            'prior_result': prior_result,
            'prior_result_digest': archive_digest(prior_result),
            'attempts': attempts.payload,
            'attempts_digest': attempts.digest,
        },
    )
    row = (
        await archive_schema.execute(
            text(
                f"""
                SELECT result_payload, prior_result_payload
                FROM {schema.sql}.history_aggregate
                WHERE task_id = :task_id
                """
            ),
            {'task_id': task_id},
        )
    ).one()
    assert row.result_payload is None
    assert bytes(row.prior_result_payload) == prior_result


async def test_catalog_has_no_default_partition_and_aligned_attempt_leaves(
    archive_schema: AsyncConnection,
) -> None:
    schema = _schema(archive_schema)
    partitions = (
        await archive_schema.execute(
            text(
                """
                SELECT child.relname,
                       pg_get_expr(child.relpartbound, child.oid) AS bound
                FROM pg_inherits inheritance
                JOIN pg_class child ON child.oid = inheritance.inhrelid
                JOIN pg_namespace namespace ON namespace.oid = child.relnamespace
                WHERE namespace.nspname = :schema
                ORDER BY child.relname
                """
            ),
            {'schema': schema.name},
        )
    ).all()
    bounds = {row.relname: row.bound for row in partitions}
    assert all(bound != 'DEFAULT' for bound in bounds.values())
    assert (
        bounds['history_copartitioned_finite_2026_08_05']
        == bounds['attempts_copartitioned_finite_2026_08_05']
    )

    id_indexes = (
        await archive_schema.execute(
            text(
                """
                SELECT tablename, indexdef
                FROM pg_indexes
                WHERE schemaname = :schema
                  AND tablename IN (
                      'history_aggregate_finite_2026_08_05',
                      'history_aggregate_forever',
                      'history_copartitioned_finite_2026_08_05',
                      'history_copartitioned_forever'
                  )
                """
            ),
            {'schema': schema.name},
        )
    ).all()
    assert len(id_indexes) == 4
    assert all('(task_id)' in row.indexdef for row in id_indexes)


async def test_storage_probe_measures_both_attempt_candidates(
    archive_schema: AsyncConnection,
) -> None:
    result = encode_json_value({'ok': 'x' * 200})
    attempts = encode_attempts(prototype_attempts(4))
    aggregate, copartitioned = await measure_attempt_storage_candidates(
        archive_schema,
        _schema(archive_schema),
        rows=100,
        result=result,
        attempts=attempts,
        attempts_per_task=4,
    )
    assert aggregate.candidate == 'aggregate_snapshot'
    assert aggregate.rows == copartitioned.rows == 100
    assert aggregate.attempts_per_task == copartitioned.attempts_per_task == 4
    assert aggregate.attempt_snapshot_bytes == len(attempts.payload)
    assert copartitioned.attempt_snapshot_bytes is None
    assert aggregate.logical_attempt_bytes == len(attempts.payload) * 100
    assert copartitioned.logical_attempt_bytes > 0
    assert aggregate.wal_bytes > 0
    assert copartitioned.wal_bytes > 0
    assert aggregate.footprint.heap_bytes > 0
    assert copartitioned.footprint.heap_bytes > 0
    assert aggregate.footprint.total_bytes >= aggregate.footprint.heap_bytes
    assert copartitioned.footprint.total_bytes >= copartitioned.footprint.heap_bytes
    assert aggregate.detail_latency.observations == 100
    assert copartitioned.detail_latency.observations == 100
    assert aggregate.detail_latency.ci_low_ms <= aggregate.detail_latency.ci_high_ms
    assert (
        copartitioned.detail_latency.ci_low_ms
        <= copartitioned.detail_latency.ci_high_ms
    )


@pytest.mark.parametrize('candidate', list(RerunStorageCandidate))
async def test_storage_probe_measures_each_rerun_input_form(
    archive_schema: AsyncConnection,
    candidate: RerunStorageCandidate,
) -> None:
    measurement = await measure_rerun_storage_candidate(
        archive_schema,
        _schema(archive_schema),
        rows=10,
        result=b'{"ok":true}',
        rerun_input=b'{"args":[],"kwargs":{"value":1}}',
        candidate=candidate,
    )
    assert measurement.candidate is candidate
    assert measurement.rows == 10
    assert measurement.payload_bytes == 32
    assert measurement.wal_bytes > 0
    assert measurement.footprint.total_bytes > 0


@pytest.mark.parametrize('candidate', list(AdministrativeResultCandidate))
async def test_storage_probe_measures_administrative_result_choices(
    archive_schema: AsyncConnection,
    candidate: AdministrativeResultCandidate,
) -> None:
    measurement = await measure_administrative_result_candidate(
        archive_schema,
        _schema(archive_schema),
        rows=10,
        prior_result=b'{"ok":{"prior":true}}',
        candidate=candidate,
    )
    assert measurement.candidate is candidate
    assert measurement.rows == 10
    assert measurement.prior_result_bytes == 21
    assert measurement.wal_bytes > 0
    assert measurement.footprint.total_bytes > 0


@pytest.mark.parametrize(
    ('measurement', 'message'),
    [
        ('rerun_rows', 'rows must be positive'),
        ('rerun_result', 'result must be non-empty'),
        ('rerun_input', 'rerun input must be non-empty'),
        ('admin_rows', 'rows must be positive'),
        ('admin_result', 'prior result must be non-empty'),
    ],
)
async def test_storage_probes_reject_invalid_workloads_before_mutation(
    archive_schema: AsyncConnection,
    measurement: str,
    message: str,
) -> None:
    schema = _schema(archive_schema)
    match measurement:
        case 'rerun_rows':
            operation = measure_rerun_storage_candidate(
                archive_schema,
                schema,
                rows=0,
                result=b'{}',
                rerun_input=b'{}',
                candidate=RerunStorageCandidate.INLINE,
            )
        case 'rerun_result':
            operation = measure_rerun_storage_candidate(
                archive_schema,
                schema,
                rows=1,
                result=b'',
                rerun_input=b'{}',
                candidate=RerunStorageCandidate.INLINE,
            )
        case 'rerun_input':
            operation = measure_rerun_storage_candidate(
                archive_schema,
                schema,
                rows=1,
                result=b'{}',
                rerun_input=b'',
                candidate=RerunStorageCandidate.INLINE,
            )
        case 'admin_rows':
            operation = measure_administrative_result_candidate(
                archive_schema,
                schema,
                rows=0,
                prior_result=b'{}',
                candidate=AdministrativeResultCandidate.EXCLUDE,
            )
        case 'admin_result':
            operation = measure_administrative_result_candidate(
                archive_schema,
                schema,
                rows=1,
                prior_result=b'',
                candidate=AdministrativeResultCandidate.EXCLUDE,
            )
        case _:
            pytest.fail(f'unrecognized measurement case: {measurement}')
    with pytest.raises(ValueError, match=message):
        await operation
