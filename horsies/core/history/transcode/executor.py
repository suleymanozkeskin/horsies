"""The five-stage replacement-transcode executor.

Plan inventories the component's relations through the partitioned
history parent, records a durable reversible job, and sets the WAL and
peak-disk budgets. Copy advances a ctid cursor in bounded committing
batches — the cursor compares and orders the qualified `tid`, never its
text rendering, because the text order silently duplicated boundary
rows in the rejected candidate. Verification proves full content BEFORE
any lock and captures the six-field identity token per relation; on the
zero-mismatch path the target-validity scan never runs. The swap takes
non-queuing locks in sorted order, re-checks the token AND the leaf
catalog's attachment inside the locked window — staleness and identity
only, never content — and rebinds by detach, rename, rename, attach,
where the replayed bound text is the SAFE deparse category. Finalize
drops guards and backups and records the WAL measurement.

Serialization against the partition manager: the copy holds ONE leaf
advisory lock at a time (taken for the batch's relation, released at
commit); the swap's window needs every relation at once, so its
advisory locks acquire in relation-ordinal order — ordinals are
assigned from sorted relation names at plan — per the sorted-order arm
of the posture rules.

The exhaustion diagnostic payload is new surface: blockers are captured
ONCE at the final failed attempt, best-effort inside their own guard,
query text truncated to the declared bound — a capture failure returns
the exhaustion outcome with the marker set, never replaced.
"""

from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from ..archive.versions import JSON_UTF8_CODEC
from ..names import TASK_HISTORY_PARENT
from ..partitions.locks import lock_leaf_for_transaction
from .jobs import (
    TRANSCODE_BATCHES,
    TRANSCODE_JOBS,
    TRANSCODE_MUTATION_FUNCTION,
    TRANSCODE_RELATIONS,
    RelationVerificationToken,
    TranscodeJobRow,
    TranscodeRelationRow,
    decode_relation_row,
)
from .maintenance import (
    active_maintenance_session,
    lock_archive_gate_row,
    lock_transcode_program,
)
from .outcomes import (
    BLOCKER_QUERY_TRUNCATION_CHARS,
    SWAP_LOCK_ATTEMPTS_MAXIMUM,
    SWAP_RETRY_BACKOFF_SECONDS,
    ArchiveComponent,
    SwapBlocker,
    SwapLockMode,
    TranscodeCopyBatch,
    TranscodeCopyOutcome,
    TranscodeCopyRejected,
    TranscodeCopyRejectionKind,
    TranscodeFinalized,
    TranscodeJobState,
    TranscodePlan,
    TranscodePlanOutcome,
    TranscodePlanRejected,
    TranscodeReadyForVerification,
    TranscodeSwap,
    TranscodeSwapBusy,
    TranscodeSwapExhausted,
    TranscodeSwapOutcome,
    TranscodeVerification,
)
from .signature import relation_schema_signature
from .transforms import (
    backup_relation_name,
    column_list,
    component_columns,
    encoded_source_select,
    quoted_identifier,
    replacement_bound_name,
    replacement_index_name,
    replacement_ordering_index_name,
    replacement_relation_name,
    transformed_select,
)

class TranscodeStateError(Exception):
    """A stage was invoked from a job state that cannot accept it."""


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


async def plan_transcode(
    connection: AsyncConnection,
    *,
    job_id: str,
    component: ArchiveComponent,
    source_version: int,
    target_version: int,
    source_codec: str,
    target_codec: str,
) -> TranscodePlanOutcome:
    """Inventory, preflight, and record one reversible job."""
    if abs(target_version - source_version) != 1:
        return TranscodePlanRejected(
            component=component,
            reason='unsupported transcode direction',
            affected_rows=0,
        )
    await lock_transcode_program(connection)
    await lock_archive_gate_row(connection)
    session_id = await active_maintenance_session(connection)
    if session_id is None:
        return TranscodePlanRejected(
            component=component,
            reason='archive maintenance is required',
            affected_rows=0,
        )
    active = (
        await connection.execute(
            text(
                f"SELECT count(*) FROM {TRANSCODE_JOBS} "
                "WHERE state <> 'COMPLETE'"
            )
        )
    ).scalar_one()
    if active:
        return TranscodePlanRejected(
            component=component,
            reason='another replacement job is active',
            affected_rows=int(active),
        )

    corrupt = await _invalid_component_rows(
        connection,
        relation=TASK_HISTORY_PARENT,
        component=component,
        version=source_version,
        codec=source_codec,
    )
    if corrupt:
        return TranscodePlanRejected(
            component=component,
            reason='source rows fail component validity',
            affected_rows=corrupt,
        )

    columns = component_columns(component)
    inventory = (
        await connection.execute(
            text(
                f"""
                SELECT history.tableoid::oid::bigint AS relation_oid,
                       child.relname AS relation_name,
                       parent.oid::bigint AS parent_oid,
                       parent.relname AS parent_name,
                       pg_get_expr(child.relpartbound, child.oid)
                           AS partition_bound,
                       pg_get_partition_constraintdef(child.oid)
                           AS partition_constraint,
                       count(*) AS row_count,
                       count(*) FILTER (
                           WHERE {columns.version} = :source_version
                             AND {columns.codec} = :source_codec
                             AND ({columns.presence_predicate})
                       ) AS transformed_rows,
                       count({columns.payload}) FILTER (
                           WHERE {columns.version} = :source_version
                             AND {columns.codec} = :source_codec
                             AND ({columns.presence_predicate})
                       ) AS payload_rows,
                       COALESCE(sum(octet_length({columns.payload})) FILTER (
                           WHERE {columns.version} = :source_version
                             AND {columns.codec} = :source_codec
                             AND ({columns.presence_predicate})
                       ), 0) AS payload_bytes,
                       pg_total_relation_size(history.tableoid)
                           AS relation_bytes,
                       count(DISTINCT task_id) AS distinct_task_ids
                FROM {TASK_HISTORY_PARENT} AS history
                JOIN pg_class AS child ON child.oid = history.tableoid
                JOIN pg_inherits AS inheritance
                  ON inheritance.inhrelid = child.oid
                JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
                GROUP BY history.tableoid, child.relname,
                         parent.oid, parent.relname, child.oid
                HAVING count(*) FILTER (
                    WHERE {columns.version} = :source_version
                      AND {columns.codec} = :source_codec
                      AND ({columns.presence_predicate})
                ) > 0
                ORDER BY child.relname
                """
            ),
            {
                'source_version': source_version,
                'source_codec': source_codec,
            },
        )
    ).all()
    duplicate_identities = sum(
        row.row_count - row.distinct_task_ids
        for row in inventory
        if row.row_count != row.distinct_task_ids
    )
    if duplicate_identities:
        return TranscodePlanRejected(
            component=component,
            reason='source relations carry duplicate task identities',
            affected_rows=int(duplicate_identities),
        )

    transformed_rows = sum(int(row.transformed_rows) for row in inventory)
    copied_rows = sum(int(row.row_count) for row in inventory)
    payload_rows = sum(int(row.payload_rows) for row in inventory)
    payload_bytes = sum(int(row.payload_bytes) for row in inventory)
    relation_bytes = sum(int(row.relation_bytes) for row in inventory)
    forward = target_version > source_version
    projected = (
        payload_bytes + 2 * payload_rows
        if forward
        else payload_bytes - 2 * payload_rows
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {TRANSCODE_JOBS} (
                job_id, maintenance_session_id, component,
                source_version, target_version,
                source_codec, target_codec, state,
                transformed_rows, copied_rows_total,
                copied_rows_completed, payload_rows,
                payload_bytes_before, projected_payload_bytes,
                affected_relation_bytes, started_at, start_lsn
            ) VALUES (
                CAST(:job_id AS uuid), CAST(:session_id AS uuid),
                :component, :source_version, :target_version,
                :source_codec, :target_codec, 'PLANNED',
                :transformed_rows, :copied_rows, 0, :payload_rows,
                :payload_bytes, :projected, :relation_bytes,
                statement_timestamp(), pg_current_wal_insert_lsn()
            )
            """
        ),
        {
            'job_id': job_id,
            'session_id': session_id,
            'component': component.value,
            'source_version': source_version,
            'target_version': target_version,
            'source_codec': source_codec,
            'target_codec': target_codec,
            'transformed_rows': transformed_rows,
            'copied_rows': copied_rows,
            'payload_rows': payload_rows,
            'payload_bytes': payload_bytes,
            'projected': max(projected, 0),
            'relation_bytes': relation_bytes,
        },
    )
    for ordinal, row in enumerate(inventory, start=1):
        await connection.execute(
            text(
                f"""
                INSERT INTO {TRANSCODE_RELATIONS} (
                    job_id, relation_ordinal, source_relation_oid,
                    source_relation_name, parent_relation_oid,
                    parent_relation_name, partition_bound,
                    partition_constraint, replacement_relation_name,
                    backup_relation_name, state, row_count,
                    transformed_rows, rows_copied, relation_bytes
                ) VALUES (
                    CAST(:job_id AS uuid), :ordinal, :source_oid,
                    :source_name, :parent_oid, :parent_name,
                    :partition_bound, :partition_constraint,
                    :replacement_name, :backup_name, 'PLANNED',
                    :row_count, :transformed_rows, 0, :relation_bytes
                )
                """
            ),
            {
                'job_id': job_id,
                'ordinal': ordinal,
                'source_oid': int(row.relation_oid),
                'source_name': str(row.relation_name),
                'parent_oid': int(row.parent_oid),
                'parent_name': str(row.parent_name),
                'partition_bound': str(row.partition_bound),
                'partition_constraint': str(row.partition_constraint),
                'replacement_name': replacement_relation_name(
                    job_id, ordinal
                ),
                'backup_name': backup_relation_name(job_id, ordinal),
                'row_count': int(row.row_count),
                'transformed_rows': int(row.transformed_rows),
                'relation_bytes': int(row.relation_bytes),
            },
        )
    return TranscodePlan(
        job_id=job_id,
        component=component,
        source_version=source_version,
        target_version=target_version,
        transformed_rows=transformed_rows,
        copied_rows=copied_rows,
        payload_bytes=payload_bytes,
        projected_payload_bytes=max(projected, 0),
        affected_relation_bytes=relation_bytes,
        relation_count=len(inventory),
        peak_additional_disk_budget_bytes=_ratio_ceiling(
            relation_bytes, numerator=5, denominator=4
        ),
        wal_budget_bytes=_ratio_ceiling(
            relation_bytes, numerator=3, denominator=2
        ),
        rollback_wal_budget_bytes=_ratio_ceiling(
            relation_bytes, numerator=3, denominator=2
        ),
        rollback_peak_additional_disk_budget_bytes=_ratio_ceiling(
            relation_bytes, numerator=5, denominator=4
        ),
        reversible=True,
    )


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------


async def run_copy_batch(
    connection: AsyncConnection,
    *,
    job_id: str,
    batch_size: int,
) -> TranscodeCopyOutcome:
    """Advance one relation's cursor by one committed batch.

    The batch's relation holds its leaf advisory lock for exactly this
    transaction — one leaf at a time, released at commit.
    """
    if batch_size <= 0:
        raise ValueError('batch size must be positive')
    await lock_transcode_program(connection)
    await lock_archive_gate_row(connection)
    job = await _lock_job(connection, job_id)
    state = TranscodeJobState(job.state)
    if state not in {TranscodeJobState.PLANNED, TranscodeJobState.COPYING}:
        if state is TranscodeJobState.COPIED:
            return TranscodeReadyForVerification(
                job_id=job_id, copied_rows_total=job.copied_rows_total
            )
        raise TranscodeStateError(
            'replacement copy is not mutable in this job state'
        )
    await _require_job_maintenance(connection, job)
    relation_mapping = (
        await connection.execute(
            text(
                f"""
                SELECT * FROM {TRANSCODE_RELATIONS}
                WHERE job_id = CAST(:job_id AS uuid)
                  AND state IN ('PLANNED', 'COPYING')
                ORDER BY relation_ordinal
                LIMIT 1
                FOR UPDATE
                """
            ),
            {'job_id': job_id},
        )
    ).mappings().one_or_none()
    if relation_mapping is None:
        await connection.execute(
            text(
                f"UPDATE {TRANSCODE_JOBS} SET state = 'COPIED', "
                'copied_at = statement_timestamp() '
                'WHERE job_id = CAST(:job_id AS uuid)'
            ),
            {'job_id': job_id},
        )
        return TranscodeReadyForVerification(
            job_id=job_id, copied_rows_total=job.copied_rows_total
        )
    relation = decode_relation_row(relation_mapping)
    await _lock_relation_leaf(connection, relation)

    if relation.state == 'PLANNED':
        rejection = await _prepare_replacement_relation(
            connection, job=job, relation=relation
        )
        if rejection is not None:
            return rejection
        relation = decode_relation_row(
            (
                await connection.execute(
                    text(
                        f'SELECT * FROM {TRANSCODE_RELATIONS} '
                        'WHERE job_id = CAST(:job_id AS uuid) '
                        'AND relation_ordinal = :ordinal FOR UPDATE'
                    ),
                    {'job_id': job_id, 'ordinal': relation.relation_ordinal},
                )
            ).mappings().one()
        )

    source = quoted_identifier(relation.source_relation_name)
    replacement = quoted_identifier(relation.replacement_relation_name)
    component = ArchiveComponent(job.component)
    columns = await _relation_columns(
        connection, relation.source_relation_oid
    )
    inserted = (
        await connection.execute(
            text(
                f"""
                WITH source_batch AS MATERIALIZED (
                    SELECT ctid AS source_ctid, source_table.*
                    FROM {source} AS source_table
                    WHERE (
                        CAST(:last_source_ctid AS tid) IS NULL
                        OR ctid > CAST(:last_source_ctid AS tid)
                    )
                    ORDER BY ctid
                    LIMIT :batch_size
                ), encoded AS MATERIALIZED (
                    SELECT {encoded_source_select(
                        component,
                        alias='source',
                        source_version=job.source_version,
                        source_codec=job.source_codec,
                        forward=job.target_version > job.source_version,
                    )}
                    FROM source_batch AS source
                ), inserted AS (
                    INSERT INTO {replacement} ({column_list(columns)})
                    SELECT {transformed_select(
                        columns,
                        component=component,
                        source_version=job.source_version,
                        source_codec=job.source_codec,
                        target_version=job.target_version,
                        target_codec=job.target_codec,
                        alias='source',
                    )}
                    FROM encoded AS source
                    RETURNING task_id
                )
                SELECT count(*) AS rows_copied,
                       (
                           SELECT source_ctid::text
                           FROM source_batch
                           ORDER BY source_batch.source_ctid DESC
                           LIMIT 1
                       ) AS last_source_ctid
                FROM inserted
                """
            ),
            {
                'last_source_ctid': relation.last_source_ctid,
                'batch_size': batch_size,
            },
        )
    ).one()
    rows_copied = relation.rows_copied + int(inserted.rows_copied)
    if (
        inserted.rows_copied == 0
        and relation.rows_copied < relation.row_count
    ) or rows_copied > relation.row_count:
        return TranscodeCopyRejected(
            job_id=job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=TranscodeCopyRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=rows_copied,
        )
    relation_complete = rows_copied == relation.row_count
    if relation_complete:
        await connection.execute(
            text(
                f'CREATE INDEX {quoted_identifier(replacement_index_name(job_id, relation.relation_ordinal))} '
                f'ON {replacement} (task_id)'
            )
        )
        await connection.execute(
            text(
                f'CREATE INDEX {quoted_identifier(replacement_ordering_index_name(job_id, relation.relation_ordinal))} '
                f'ON {replacement} (enqueued_at)'
            )
        )
    batch_number = (
        await connection.execute(
            text(
                f'SELECT COALESCE(max(batch_number), 0) + 1 '
                f'FROM {TRANSCODE_BATCHES} '
                'WHERE job_id = CAST(:job_id AS uuid)'
            ),
            {'job_id': job_id},
        )
    ).scalar_one()
    await connection.execute(
        text(
            f"""
            INSERT INTO {TRANSCODE_BATCHES} (
                job_id, batch_number, relation_ordinal,
                rows_copied, committed_at
            ) VALUES (
                CAST(:job_id AS uuid), :batch_number, :ordinal,
                :rows_copied, statement_timestamp()
            )
            """
        ),
        {
            'job_id': job_id,
            'batch_number': batch_number,
            'ordinal': relation.relation_ordinal,
            'rows_copied': int(inserted.rows_copied),
        },
    )
    await connection.execute(
        text(
            f"""
            UPDATE {TRANSCODE_RELATIONS}
            SET state = :state, rows_copied = :rows_copied,
                last_source_ctid = CAST(:last_source_ctid AS tid),
                copied_at = CASE WHEN :state = 'COPIED'
                                 THEN statement_timestamp()
                                 ELSE copied_at END
            WHERE job_id = CAST(:job_id AS uuid)
              AND relation_ordinal = :ordinal
            """
        ),
        {
            'state': 'COPIED' if relation_complete else 'COPYING',
            'rows_copied': rows_copied,
            'last_source_ctid': inserted.last_source_ctid,
            'job_id': job_id,
            'ordinal': relation.relation_ordinal,
        },
    )
    completed = job.copied_rows_completed + int(inserted.rows_copied)
    all_copied = completed == job.copied_rows_total
    await connection.execute(
        text(
            f"""
            UPDATE {TRANSCODE_JOBS}
            SET state = :state,
                copied_rows_completed = :completed,
                last_batch_at = statement_timestamp(),
                copied_at = CASE WHEN :state = 'COPIED'
                                 THEN statement_timestamp()
                                 ELSE copied_at END
            WHERE job_id = CAST(:job_id AS uuid)
            """
        ),
        {
            'state': 'COPIED' if all_copied else 'COPYING',
            'completed': completed,
            'job_id': job_id,
        },
    )
    return TranscodeCopyBatch(
        job_id=job_id,
        relation_ordinal=relation.relation_ordinal,
        batch_number=int(batch_number),
        rows_copied=int(inserted.rows_copied),
        copied_rows_completed=completed,
        copied_rows_total=job.copied_rows_total,
    )


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------


async def verify_transcode(
    connection: AsyncConnection,
    *,
    job_id: str,
) -> TranscodeVerification:
    """Full content verification, committed before any lock."""
    await lock_transcode_program(connection)
    await lock_archive_gate_row(connection)
    job = await _lock_job(connection, job_id)
    state = TranscodeJobState(job.state)
    if state not in {TranscodeJobState.COPIED, TranscodeJobState.VERIFIED}:
        raise TranscodeStateError(
            'replacement relations are not ready for verification'
        )
    await _require_job_maintenance(connection, job)
    component = ArchiveComponent(job.component)

    changed = 0
    mismatches = 0
    invalid_targets = 0
    for relation in await _job_relations(connection, job_id):
        await _lock_relation_leaf(connection, relation)
        initial_token = await _verification_token(connection, relation)
        if initial_token is None or not await _bindings_match(
            connection, relation
        ):
            changed += 1
            await _clear_verification(connection, relation)
            continue
        source = quoted_identifier(relation.source_relation_name)
        observed = (
            await connection.execute(
                text(f'SELECT count(*) FROM {source}')
            )
        ).scalar_one()
        if observed != relation.row_count:
            changed += 1
            await _clear_verification(connection, relation)
            continue
        columns = await _relation_columns(
            connection, relation.source_relation_oid
        )
        mismatch = await _mismatch_count(
            connection,
            relation=relation,
            columns=columns,
            component=component,
            source_version=job.source_version,
            source_codec=job.source_codec,
            target_version=job.target_version,
            target_codec=job.target_codec,
        )
        mismatches += mismatch
        relation_invalid = 0
        if mismatch:
            # The validity scan runs ONLY on the mismatch path.
            relation_invalid = await _invalid_component_rows(
                connection,
                relation=quoted_identifier(
                    relation.replacement_relation_name
                ),
                component=component,
                version=job.target_version,
                codec=job.target_codec,
            )
        invalid_targets += relation_invalid
        final_token = await _verification_token(
            connection, relation, lock_record=True
        )
        stable = final_token is not None and final_token == initial_token
        if not stable:
            changed += 1
        if (
            final_token is not None
            and mismatch == 0
            and relation_invalid == 0
            and stable
        ):
            await _record_verification(connection, relation, final_token)
        else:
            await _clear_verification(connection, relation)

    verified = changed == 0 and mismatches == 0 and invalid_targets == 0
    await connection.execute(
        text(
            f'UPDATE {TRANSCODE_JOBS} SET state = :state, '
            'verified_at = CASE WHEN :verified '
            'THEN statement_timestamp() ELSE NULL END '
            'WHERE job_id = CAST(:job_id AS uuid)'
        ),
        {
            'state': 'VERIFIED' if verified else 'COPIED',
            'verified': verified,
            'job_id': job_id,
        },
    )
    return TranscodeVerification(
        job_id=job_id,
        verified=verified,
        source_relations_changed=changed,
        replacement_row_mismatches=mismatches,
        invalid_target_rows=invalid_targets,
        copied_rows_total=job.copied_rows_total,
        wal_bytes=None,
    )


# ---------------------------------------------------------------------------
# Swap
# ---------------------------------------------------------------------------


async def swap_transcode(
    connection: AsyncConnection,
    *,
    job_id: str,
) -> TranscodeSwapOutcome:
    """One non-queuing swap attempt; busy is an outcome, never a wait."""
    await lock_transcode_program(connection)
    await lock_archive_gate_row(connection)
    job = await _lock_job(connection, job_id)
    state = TranscodeJobState(job.state)
    if state in {TranscodeJobState.SWAPPED, TranscodeJobState.COMPLETE}:
        return TranscodeSwap(
            job_id=job_id, relations_swapped=job.relation_count
        )
    if state is not TranscodeJobState.VERIFIED:
        raise TranscodeStateError(
            'replacement relations must be verified before binding swap'
        )
    await _require_job_maintenance(connection, job)
    relations = await _job_relations(connection, job_id)
    # Sorted-order arm: the window needs every relation, so the leaf
    # advisory locks acquire in ordinal order (assigned from sorted
    # relation names at plan) before the table locks.
    for relation in relations:
        await _lock_relation_leaf(connection, relation)
    busy = await _try_swap_locks(connection, job_id=job_id, relations=relations)
    if busy is not None:
        return busy
    for relation in relations:
        if not await _verified_token_matches(connection, relation):
            raise TranscodeStateError(
                'replacement verification changed before binding swap'
            )
        if not await _catalog_attachment_holds(connection, relation):
            raise TranscodeStateError(
                'leaf catalog attachment changed before binding swap'
            )
    for relation in relations:
        source = quoted_identifier(relation.source_relation_name)
        parent = quoted_identifier(relation.parent_relation_name)
        replacement = quoted_identifier(
            relation.replacement_relation_name
        )
        await connection.execute(
            text(f'ALTER TABLE {parent} DETACH PARTITION {source}')
        )
        await connection.execute(
            text(
                f'ALTER TABLE {source} RENAME TO '
                f'{quoted_identifier(relation.backup_relation_name)}'
            )
        )
        await connection.execute(
            text(
                f'ALTER TABLE {replacement} RENAME TO '
                f'{quoted_identifier(relation.source_relation_name)}'
            )
        )
        await connection.execute(
            text(
                f'ALTER TABLE {parent} ATTACH PARTITION '
                f'{quoted_identifier(relation.source_relation_name)} '
                f'{relation.partition_bound}'
            )
        )
        await connection.execute(
            text(
                f"UPDATE {TRANSCODE_RELATIONS} SET state = 'SWAPPED', "
                'swapped_at = statement_timestamp() '
                'WHERE job_id = CAST(:job_id AS uuid) '
                'AND relation_ordinal = :ordinal'
            ),
            {'job_id': job_id, 'ordinal': relation.relation_ordinal},
        )
    await connection.execute(
        text(
            f"UPDATE {TRANSCODE_JOBS} SET state = 'SWAPPED', "
            'swapped_at = statement_timestamp() '
            'WHERE job_id = CAST(:job_id AS uuid)'
        ),
        {'job_id': job_id},
    )
    return TranscodeSwap(job_id=job_id, relations_swapped=len(relations))


async def swap_with_retries(
    engine: AsyncEngine,
    *,
    job_id: str,
) -> TranscodeSwapOutcome:
    """Drive swap attempts to the qualified ceilings.

    Each attempt is its own transaction; busy sleeps the fixed backoff.
    Exhaustion captures the blocking sessions ONCE, after the final
    failed attempt, best-effort — a capture failure returns the
    exhaustion outcome with the marker set.
    """
    attempts = 0
    last_busy: TranscodeSwapBusy | None = None
    while attempts < SWAP_LOCK_ATTEMPTS_MAXIMUM:
        attempts += 1
        async with engine.begin() as connection:
            outcome = await swap_transcode(connection, job_id=job_id)
        match outcome:
            case TranscodeSwapBusy():
                last_busy = outcome
                if attempts < SWAP_LOCK_ATTEMPTS_MAXIMUM:
                    await asyncio.sleep(SWAP_RETRY_BACKOFF_SECONDS)
            case _:
                return outcome
    assert last_busy is not None
    blockers: tuple[SwapBlocker, ...] = ()
    capture_failed = False
    try:
        async with engine.connect() as connection:
            blockers = await _capture_swap_blockers(
                connection,
                lock_mode=last_busy.lock_mode,
                relation_names=last_busy.relation_names,
            )
    except Exception:
        capture_failed = True
    return TranscodeSwapExhausted(
        job_id=job_id,
        lock_mode=last_busy.lock_mode,
        relation_names=last_busy.relation_names,
        attempts=attempts,
        retry_sleep_seconds=round(
            (attempts - 1) * SWAP_RETRY_BACKOFF_SECONDS, 6
        ),
        blockers=blockers,
        blocker_capture_failed=capture_failed,
    )


# ---------------------------------------------------------------------------
# Finalize
# ---------------------------------------------------------------------------


async def finalize_transcode(
    connection: AsyncConnection,
    *,
    job_id: str,
) -> TranscodeFinalized:
    """Drop guards and backups; record the WAL measurement."""
    await lock_transcode_program(connection)
    await lock_archive_gate_row(connection)
    job = await _lock_job(connection, job_id)
    state = TranscodeJobState(job.state)
    if state is TranscodeJobState.COMPLETE:
        return TranscodeFinalized(
            job_id=job_id,
            retired_source_version=job.source_version,
            decoder_retirement_ready=True,
        )
    if state is not TranscodeJobState.SWAPPED:
        raise TranscodeStateError(
            'replacement partitions have not been swapped'
        )
    await _require_job_maintenance(connection, job)
    remaining = (
        await connection.execute(
            text(
                f'SELECT count(*) FROM {TASK_HISTORY_PARENT} '
                f'WHERE {component_columns(ArchiveComponent(job.component)).version} '
                '= :source_version '
                f'AND ({component_columns(ArchiveComponent(job.component)).presence_predicate})'
            ),
            {'source_version': job.source_version},
        )
    ).scalar_one()
    if remaining:
        raise TranscodeStateError(
            f'{remaining} source-version rows remain after swap'
        )
    for relation in await _job_relations(connection, job_id):
        await connection.execute(
            text(
                'DROP TRIGGER archive_replacement_target_guard ON '
                f'{quoted_identifier(relation.source_relation_name)}'
            )
        )
        await connection.execute(
            text(
                'DROP TABLE '
                f'{quoted_identifier(relation.backup_relation_name)}'
            )
        )
        await connection.execute(
            text(
                f"UPDATE {TRANSCODE_RELATIONS} SET state = 'COMPLETE', "
                'completed_at = statement_timestamp() '
                'WHERE job_id = CAST(:job_id AS uuid) '
                'AND relation_ordinal = :ordinal'
            ),
            {'job_id': job_id, 'ordinal': relation.relation_ordinal},
        )
    wal_bytes = (
        await connection.execute(
            text(
                'SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '
                'CAST(:start_lsn AS pg_lsn))'
            ),
            {'start_lsn': job.start_lsn},
        )
    ).scalar_one()
    await connection.execute(
        text(
            f"UPDATE {TRANSCODE_JOBS} SET state = 'COMPLETE', "
            'completed_at = statement_timestamp(), wal_bytes = :wal_bytes '
            'WHERE job_id = CAST(:job_id AS uuid)'
        ),
        {'job_id': job_id, 'wal_bytes': int(wal_bytes)},
    )
    return TranscodeFinalized(
        job_id=job_id,
        retired_source_version=job.source_version,
        decoder_retirement_ready=True,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ratio_ceiling(value: int, *, numerator: int, denominator: int) -> int:
    return -(-value * numerator // denominator)


async def _invalid_component_rows(
    connection: AsyncConnection,
    *,
    relation: str,
    component: ArchiveComponent,
    version: int,
    codec: str,
) -> int:
    """Component validity the frozen CHECKs cannot express.

    The frozen and gated DDL already reject malformed digest lengths and
    version signs; what remains is the version/codec pairing (owned by
    the archive versions module — version 1 pairs with its imported
    codec constant) and digest-over-payload equality, which is pure SQL.
    Metadata-only components have no payload to check.
    """
    columns = component_columns(component)
    if columns.metadata_only:
        return 0
    expected_codec = JSON_UTF8_CODEC if version == 1 else codec
    return int(
        (
            await connection.execute(
                text(
                    f"""
                    SELECT count(*)
                    FROM {relation}
                    WHERE {columns.version} = :version
                      AND ({columns.presence_predicate})
                      AND (
                          {columns.codec} <> :expected_codec
                          OR sha256({columns.payload})
                             IS DISTINCT FROM {_digest_column(component)}
                      )
                    """
                ),
                {'version': version, 'expected_codec': expected_codec},
            )
        ).scalar_one()
    )


def _digest_column(component: ArchiveComponent) -> str:
    match component:
        case ArchiveComponent.HISTORY_ROW:
            return 'NULL::bytea'
        case ArchiveComponent.RESULT:
            return 'result_digest'
        case ArchiveComponent.ATTEMPTS:
            return 'attempt_snapshot_digest'
        case ArchiveComponent.RERUN_INPUT:
            return 'rerun_input_digest'


async def _lock_job(
    connection: AsyncConnection, job_id: str
) -> TranscodeJobRow:
    mapping = (
        await connection.execute(
            text(
                f"""
                SELECT jobs.*, (
                    SELECT count(*) FROM {TRANSCODE_RELATIONS}
                    WHERE job_id = jobs.job_id
                ) AS relation_count
                FROM {TRANSCODE_JOBS} AS jobs
                WHERE job_id = CAST(:job_id AS uuid)
                FOR UPDATE
                """
            ),
            {'job_id': job_id},
        )
    ).mappings().one_or_none()
    if mapping is None:
        raise TranscodeStateError('unknown replacement transcode job')
    return TranscodeJobRow(
        job_id=str(mapping['job_id']),
        maintenance_session_id=str(mapping['maintenance_session_id']),
        component=str(mapping['component']),
        source_version=int(mapping['source_version']),
        target_version=int(mapping['target_version']),
        source_codec=str(mapping['source_codec']),
        target_codec=str(mapping['target_codec']),
        state=str(mapping['state']),
        transformed_rows=int(mapping['transformed_rows']),
        copied_rows_total=int(mapping['copied_rows_total']),
        copied_rows_completed=int(mapping['copied_rows_completed']),
        relation_count=int(mapping['relation_count']),
        start_lsn=str(mapping['start_lsn']),
        wal_bytes=(
            int(mapping['wal_bytes'])
            if mapping['wal_bytes'] is not None
            else None
        ),
    )


async def _require_job_maintenance(
    connection: AsyncConnection, job: TranscodeJobRow
) -> None:
    session_id = await active_maintenance_session(connection)
    if session_id != job.maintenance_session_id:
        raise TranscodeStateError(
            'the job\'s maintenance session is not active'
        )


async def _job_relations(
    connection: AsyncConnection, job_id: str
) -> tuple[TranscodeRelationRow, ...]:
    mappings = (
        await connection.execute(
            text(
                f'SELECT * FROM {TRANSCODE_RELATIONS} '
                'WHERE job_id = CAST(:job_id AS uuid) '
                'ORDER BY relation_ordinal'
            ),
            {'job_id': job_id},
        )
    ).mappings().all()
    return tuple(decode_relation_row(mapping) for mapping in mappings)


async def _lock_relation_leaf(
    connection: AsyncConnection, relation: TranscodeRelationRow
) -> None:
    """The cataloged leaf lock serializing against partition maintenance."""
    row = (
        await connection.execute(
            text(
                'SELECT class_key, lower_anchor '
                'FROM horsies_task_history_leaf_catalog '
                'WHERE leaf_name = :leaf_name'
            ),
            {'leaf_name': relation.source_relation_name},
        )
    ).one_or_none()
    if row is None:
        raise TranscodeStateError(
            f'source relation {relation.source_relation_name!r} '
            'has no leaf catalog row'
        )
    await lock_leaf_for_transaction(
        connection, class_key=str(row.class_key), anchor=row.lower_anchor
    )


async def _catalog_attachment_holds(
    connection: AsyncConnection, relation: TranscodeRelationRow
) -> bool:
    """The in-window catalog check: the leaf is still attached and not
    detached or dropped — verify-before-lock applied to the catalog
    dimension."""
    row = (
        await connection.execute(
            text(
                'SELECT detached_at IS NULL AND dropped_at IS NULL '
                'AS attached '
                'FROM horsies_task_history_leaf_catalog '
                'WHERE leaf_name = :leaf_name'
            ),
            {'leaf_name': relation.source_relation_name},
        )
    ).one_or_none()
    return row is not None and bool(row.attached)


async def _prepare_replacement_relation(
    connection: AsyncConnection,
    *,
    job: TranscodeJobRow,
    relation: TranscodeRelationRow,
) -> TranscodeCopyRejected | None:
    source = quoted_identifier(relation.source_relation_name)
    if not await _bindings_match(connection, relation, source_only=True):
        return TranscodeCopyRejected(
            job_id=job.job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=TranscodeCopyRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=0,
        )
    observed = (
        await connection.execute(
            text(
                f'SELECT count(*) AS row_count, '
                'count(DISTINCT task_id) AS distinct_task_ids '
                f'FROM {source}'
            )
        )
    ).one()
    if observed.row_count != relation.row_count or (
        observed.distinct_task_ids != relation.row_count
    ):
        return TranscodeCopyRejected(
            job_id=job.job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=TranscodeCopyRejectionKind.SOURCE_SET_CHANGED,
            observed_rows=int(observed.row_count),
        )
    invalid = await _invalid_component_rows(
        connection,
        relation=source,
        component=ArchiveComponent(job.component),
        version=job.source_version,
        codec=job.source_codec,
    )
    if invalid:
        return TranscodeCopyRejected(
            job_id=job.job_id,
            relation_ordinal=relation.relation_ordinal,
            kind=TranscodeCopyRejectionKind.SOURCE_CORRUPT,
            observed_rows=invalid,
        )
    for trigger_ddl in (
        f"""
        CREATE TRIGGER archive_replacement_source_row_guard
        AFTER INSERT OR UPDATE OR DELETE ON {source}
        FOR EACH ROW EXECUTE FUNCTION {TRANSCODE_MUTATION_FUNCTION}()
        """,
        f"""
        CREATE TRIGGER archive_replacement_source_truncate_guard
        AFTER TRUNCATE ON {source}
        FOR EACH STATEMENT EXECUTE FUNCTION {TRANSCODE_MUTATION_FUNCTION}()
        """,
    ):
        await connection.execute(text(trigger_ddl))
    replacement = quoted_identifier(relation.replacement_relation_name)
    # INCLUDING ALL copies CHECK constraints as attno-mapped trees, so
    # attach-time equality against the parent's constraints holds by
    # construction. A deparse-and-replay copy does not: varchar IN-list
    # constraints fail pg_get_constraintdef round-trips, and ATTACH
    # refuses same-name constraints with differing trees.
    await connection.execute(
        text(
            f'CREATE TABLE {replacement} '
            f'(LIKE {source} INCLUDING ALL EXCLUDING INDEXES)'
        )
    )
    replacement_oid = (
        await connection.execute(
            text('SELECT CAST(:name AS regclass)::oid::bigint'),
            {'name': relation.replacement_relation_name},
        )
    ).scalar_one()
    await connection.execute(
        text(
            f'UPDATE {TRANSCODE_RELATIONS} '
            'SET replacement_relation_oid = :oid '
            'WHERE job_id = CAST(:job_id AS uuid) '
            'AND relation_ordinal = :ordinal'
        ),
        {
            'oid': int(replacement_oid),
            'job_id': job.job_id,
            'ordinal': relation.relation_ordinal,
        },
    )
    await connection.execute(
        text(
            f"""
            CREATE TRIGGER archive_replacement_target_guard
            AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {replacement}
            FOR EACH STATEMENT
            EXECUTE FUNCTION {TRANSCODE_MUTATION_FUNCTION}()
            """
        )
    )
    await connection.execute(
        text(
            f'ALTER TABLE {replacement} ADD CONSTRAINT '
            f'{quoted_identifier(replacement_bound_name(job.job_id, relation.relation_ordinal))} '
            f'CHECK ({relation.partition_constraint})'
        )
    )
    await connection.execute(
        text(
            f"UPDATE {TRANSCODE_RELATIONS} SET state = 'COPYING', "
            'prepared_at = statement_timestamp() '
            'WHERE job_id = CAST(:job_id AS uuid) '
            'AND relation_ordinal = :ordinal'
        ),
        {'job_id': job.job_id, 'ordinal': relation.relation_ordinal},
    )
    return None


async def _relation_columns(
    connection: AsyncConnection, relation_oid: int
) -> tuple[str, ...]:
    rows = (
        await connection.execute(
            text(
                """
                SELECT attname FROM pg_attribute
                WHERE attrelid = CAST(:oid AS oid)
                  AND attnum > 0 AND NOT attisdropped
                ORDER BY attnum
                """
            ),
            {'oid': relation_oid},
        )
    ).scalars().all()
    columns = tuple(str(row) for row in rows)
    if not columns:
        raise TranscodeStateError(
            'replacement source relation has no visible columns'
        )
    return columns


async def _bindings_match(
    connection: AsyncConnection,
    relation: TranscodeRelationRow,
    *,
    source_only: bool = False,
) -> bool:
    source_ok = (
        await connection.execute(
            text(
                'SELECT to_regclass(:name)::oid::bigint = :oid '
                'AND EXISTS ('
                '  SELECT 1 FROM pg_inherits '
                '  WHERE inhrelid = CAST(:oid AS oid) '
                '    AND inhparent = CAST(:parent_oid AS oid))'
            ),
            {
                'name': relation.source_relation_name,
                'oid': relation.source_relation_oid,
                'parent_oid': relation.parent_relation_oid,
            },
        )
    ).scalar_one()
    if not bool(source_ok):
        return False
    if source_only or relation.replacement_relation_oid is None:
        return True
    replacement_ok = (
        await connection.execute(
            text('SELECT to_regclass(:name)::oid::bigint = :oid'),
            {
                'name': relation.replacement_relation_name,
                'oid': relation.replacement_relation_oid,
            },
        )
    ).scalar_one()
    return bool(replacement_ok)


async def _verification_token(
    connection: AsyncConnection,
    relation: TranscodeRelationRow,
    *,
    lock_record: bool = False,
) -> RelationVerificationToken | None:
    if relation.replacement_relation_oid is None:
        return None
    lock_clause = 'FOR UPDATE' if lock_record else ''
    row = (
        await connection.execute(
            text(
                f"""
                SELECT source_mutation_generation,
                       replacement_mutation_generation,
                       pg_relation_filenode(
                           CAST(source_relation_oid AS oid)
                       )::bigint AS source_filenode,
                       pg_relation_filenode(
                           CAST(replacement_relation_oid AS oid)
                       )::bigint AS replacement_filenode
                FROM {TRANSCODE_RELATIONS}
                WHERE job_id = CAST(:job_id AS uuid)
                  AND relation_ordinal = :ordinal
                {lock_clause}
                """
            ),
            {
                'job_id': relation.job_id,
                'ordinal': relation.relation_ordinal,
            },
        )
    ).one_or_none()
    if (
        row is None
        or row.source_filenode is None
        or row.replacement_filenode is None
    ):
        return None
    source_signature = await relation_schema_signature(
        connection, relation.source_relation_oid
    )
    replacement_signature = await relation_schema_signature(
        connection, relation.replacement_relation_oid
    )
    if source_signature is None or replacement_signature is None:
        return None
    return RelationVerificationToken(
        source_generation=int(row.source_mutation_generation),
        replacement_generation=int(row.replacement_mutation_generation),
        source_filenode=int(row.source_filenode),
        replacement_filenode=int(row.replacement_filenode),
        source_schema_signature=source_signature,
        replacement_schema_signature=replacement_signature,
    )


async def _mismatch_count(
    connection: AsyncConnection,
    *,
    relation: TranscodeRelationRow,
    columns: tuple[str, ...],
    component: ArchiveComponent,
    source_version: int,
    source_codec: str,
    target_version: int,
    target_codec: str,
) -> int:
    source = quoted_identifier(relation.source_relation_name)
    replacement = quoted_identifier(relation.replacement_relation_name)
    expected = transformed_select(
        columns,
        component=component,
        source_version=source_version,
        source_codec=source_codec,
        target_version=target_version,
        target_codec=target_codec,
        alias='source',
    )
    encoded = encoded_source_select(
        component,
        alias='source',
        source_version=source_version,
        source_codec=source_codec,
        forward=target_version > source_version,
    )
    expected_columns = ', '.join(
        f'expected.{quoted_identifier(column)}' for column in columns
    )
    replacement_columns = ', '.join(
        f'replacement.{quoted_identifier(column)}' for column in columns
    )
    return int(
        (
            await connection.execute(
                text(
                    f"""
                    WITH encoded AS MATERIALIZED (
                        SELECT {encoded} FROM {source} AS source
                    ), expected ({column_list(columns)}) AS MATERIALIZED (
                        SELECT {expected} FROM encoded AS source
                    )
                    SELECT count(*)
                    FROM expected
                    FULL OUTER JOIN {replacement} AS replacement
                        USING (task_id)
                    WHERE expected.task_id IS NULL
                       OR replacement.task_id IS NULL
                       OR ROW({expected_columns}) IS DISTINCT FROM
                          ROW({replacement_columns})
                    """
                )
            )
        ).scalar_one()
    )


async def _record_verification(
    connection: AsyncConnection,
    relation: TranscodeRelationRow,
    token: RelationVerificationToken,
) -> None:
    await connection.execute(
        text(
            f"""
            UPDATE {TRANSCODE_RELATIONS}
            SET state = 'VERIFIED', verified_at = statement_timestamp(),
                verified_source_generation = :source_generation,
                verified_replacement_generation = :replacement_generation,
                verified_source_filenode = :source_filenode,
                verified_replacement_filenode = :replacement_filenode,
                verified_source_schema_signature = :source_signature,
                verified_replacement_schema_signature =
                    :replacement_signature
            WHERE job_id = CAST(:job_id AS uuid)
              AND relation_ordinal = :ordinal
            """
        ),
        {
            'job_id': relation.job_id,
            'ordinal': relation.relation_ordinal,
            'source_generation': token.source_generation,
            'replacement_generation': token.replacement_generation,
            'source_filenode': token.source_filenode,
            'replacement_filenode': token.replacement_filenode,
            'source_signature': token.source_schema_signature,
            'replacement_signature': token.replacement_schema_signature,
        },
    )


async def _clear_verification(
    connection: AsyncConnection, relation: TranscodeRelationRow
) -> None:
    await connection.execute(
        text(
            f"""
            UPDATE {TRANSCODE_RELATIONS}
            SET state = CASE WHEN state = 'VERIFIED'
                             THEN 'COPIED' ELSE state END,
                verified_at = NULL,
                verified_source_generation = NULL,
                verified_replacement_generation = NULL,
                verified_source_filenode = NULL,
                verified_replacement_filenode = NULL,
                verified_source_schema_signature = NULL,
                verified_replacement_schema_signature = NULL
            WHERE job_id = CAST(:job_id AS uuid)
              AND relation_ordinal = :ordinal
            """
        ),
        {'job_id': relation.job_id, 'ordinal': relation.relation_ordinal},
    )


async def _verified_token_matches(
    connection: AsyncConnection, relation: TranscodeRelationRow
) -> bool:
    current = await _verification_token(connection, relation)
    return (
        current is not None
        and current.source_generation
        == relation.verified_source_generation
        and current.replacement_generation
        == relation.verified_replacement_generation
        and current.source_filenode == relation.verified_source_filenode
        and current.replacement_filenode
        == relation.verified_replacement_filenode
        and current.source_schema_signature
        == relation.verified_source_schema_signature
        and current.replacement_schema_signature
        == relation.verified_replacement_schema_signature
    )


async def _try_swap_locks(
    connection: AsyncConnection,
    *,
    job_id: str,
    relations: tuple[TranscodeRelationRow, ...],
) -> TranscodeSwapBusy | None:
    parent_names = tuple(
        sorted({row.parent_relation_name for row in relations})
    )
    lock_mode = SwapLockMode.PARENT
    relation_names: tuple[str, ...] = parent_names
    try:
        async with connection.begin_nested():
            for parent_name in parent_names:
                lock_mode = SwapLockMode.PARENT
                relation_names = (parent_name,)
                await connection.execute(
                    text(
                        f'LOCK TABLE {quoted_identifier(parent_name)} '
                        'IN ACCESS EXCLUSIVE MODE NOWAIT'
                    )
                )
            for relation in relations:
                lock_mode = SwapLockMode.LEAVES
                relation_names = (
                    relation.source_relation_name,
                    relation.replacement_relation_name,
                )
                await connection.execute(
                    text(
                        'LOCK TABLE '
                        f'{quoted_identifier(relation.source_relation_name)}, '
                        f'{quoted_identifier(relation.replacement_relation_name)} '
                        'IN SHARE MODE NOWAIT'
                    )
                )
    except DBAPIError as error:
        if _sqlstate(error) != '55P03':
            raise
        return TranscodeSwapBusy(
            job_id=job_id,
            lock_mode=lock_mode,
            relation_names=relation_names,
        )
    return None


def _sqlstate(error: DBAPIError) -> str | None:
    return getattr(error.orig, 'sqlstate', None) or getattr(
        error.orig, 'pgcode', None
    )


async def _capture_swap_blockers(
    connection: AsyncConnection,
    *,
    lock_mode: SwapLockMode,
    relation_names: tuple[str, ...],
) -> tuple[SwapBlocker, ...]:
    await connection.execute(text('SELECT pg_stat_clear_snapshot()'))
    rows = (
        await connection.execute(
            text(
                f"""
                WITH requested AS (
                    SELECT relation_name,
                           to_regclass(relation_name)::oid AS relation_oid
                    FROM unnest(CAST(:relation_names AS text[]))
                         AS names(relation_name)
                )
                SELECT locks.pid,
                       activity.state,
                       EXTRACT(EPOCH FROM
                           clock_timestamp() - activity.xact_start
                       )::double precision AS transaction_age_seconds,
                       activity.wait_event,
                       LEFT(activity.query,
                            {BLOCKER_QUERY_TRUNCATION_CHARS}) AS query,
                       requested.relation_name,
                       locks.mode AS held_lock_mode,
                       locks.granted
                FROM requested
                JOIN pg_locks AS locks
                  ON locks.locktype = 'relation'
                 AND locks.relation = requested.relation_oid
                JOIN pg_stat_activity AS activity
                  ON activity.pid = locks.pid
                WHERE locks.pid <> pg_backend_pid()
                  AND locks.granted
                  AND (
                      CAST(:requested_mode AS text) = 'ACCESS_EXCLUSIVE'
                      OR locks.mode = ANY(CAST(:share_conflicts AS text[]))
                  )
                ORDER BY locks.pid, requested.relation_name, locks.mode
                """
            ),
            {
                'relation_names': list(relation_names),
                'requested_mode': lock_mode.value,
                'share_conflicts': [
                    'RowExclusiveLock',
                    'ShareUpdateExclusiveLock',
                    'ShareRowExclusiveLock',
                    'ExclusiveLock',
                    'AccessExclusiveLock',
                ],
            },
        )
    ).all()
    return tuple(
        SwapBlocker(
            pid=int(row.pid),
            state=row.state,
            transaction_age_seconds=(
                float(row.transaction_age_seconds)
                if row.transaction_age_seconds is not None
                else None
            ),
            wait_event=row.wait_event,
            query=row.query,
            relation_name=str(row.relation_name),
            held_lock_mode=str(row.held_lock_mode),
            granted=bool(row.granted),
        )
        for row in rows
    )
