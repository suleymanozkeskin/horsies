"""Stage 4a: envelope preparation over legacy rows, under one policy.

The backfill consults the SAME app-level retain-input default every
fresh enqueue consults — the deployment's standing policy speaks, no
migration-only parameter, one policy owner read by both paths. Legacy
rows get exactly what a fresh row enqueued today without an explicit
override gets.

Preparation covers EVERY unprepared row, terminal and live alike,
because the tighten's postcondition is table-wide: drain deliberately
lets PENDING rows survive the cutover — the new fleet claims and runs
them, and the move projects their enqueue-time facts into history —
so the stage that establishes those facts must reach them. A
preparation that stopped at terminal rows advised a tighten it could
never satisfy.

A row with no recorded retention class resolves to the forever class,
imported from the same authority the relocation projection coalesces
with — one owner, so the two stages cannot disagree about the column
— and the resolved class is stamped back onto the row: the
fingerprint records the class the row will actually land in, and the
class column's SET NOT NULL at the tighten finds no live NULLs left
behind. No recorded policy means no deletion policy applied; resolving
to a finite class here would put every legacy row past its duration on
the drop path at the first retention pass.

Always computed, retention aside: the command fingerprint (it covers
the carried JSON strings as-is, so it exists even for rows whose
stored JSON fails the strict decode) and, where the strict decode
succeeds, the input digest over the canonical content-v1 bytes.
Rows whose stored inputs fail the strict decode take
DECLINED_BY_POLICY — current policy is strict encoding, and declining
what cannot be strictly encoded is that policy speaking; the archive
column rightly collapses this with policy-declined, while the batch
outcome reports the split so the operator report never does.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ...codec.json_io import loads_json
from ...types.result import is_err
from ..ddl.tables import FOREVER_CLASS_KEY
from ..identity.fingerprint import EnqueueCommandV1
from ..rerun.input_envelope import (
    INPUT_ENVELOPE_CODEC,
    INPUT_ENVELOPE_CONTENT_TYPE,
    INPUT_ENVELOPE_INLINE_MAX_BYTES,
    INPUT_ENVELOPE_VERSION,
    encode_input_envelope_v1,
)
from ..terminalization.move import LIVE_TASKS


@dataclass(frozen=True, slots=True)
class PreparationCursor:
    """Keyset position of the preparation walk.

    Obtained from ``start()`` and advanced by every batch outcome; the
    watermark lives inside the cursor so a driver structurally cannot
    drop it between calls. Restarting from ``start()`` mid-run is still
    correct — every prepared row fails the disposition predicate — but
    re-walks the id order once; a driver that never advances the cursor
    completes with total cost quadratic in the table size, because each
    batch then scans past every row already prepared before finding
    work (batch k discards k x batch_size rows; at 10,000-row batches
    over 1M rows the growth is ~13.5 ms per batch, and at 10M rows the
    skipped-row work alone is hours).
    """

    after_id: str | None

    @classmethod
    def start(cls) -> 'PreparationCursor':
        """The position before the first row."""
        return cls(after_id=None)


@dataclass(frozen=True, slots=True)
class PreparationBatch:
    """One committed batch of prepared legacy rows.

    ``cursor`` is the advanced keyset position; the caller passes it to
    the next call so the selection never re-walks rows this run already
    prepared. ``live_rows_prepared`` counts the drain-surviving live
    rows in this batch — the population the tighten requires prepared
    and nothing else prepares.
    """

    rows_prepared: int
    live_rows_prepared: int
    inline_rows: int
    over_bound_rows: int
    policy_declined_rows: int
    decode_failed_rows: int
    cursor: PreparationCursor


@dataclass(frozen=True, slots=True)
class PreparationComplete:
    """No row remains without a prepared disposition, terminal or live.

    The count is the table-wide prepared total — the same population
    the tighten's entry check counts, so completion here is the
    postcondition the tighten verifies.
    """

    rows_prepared: int


@dataclass(frozen=True, slots=True)
class _PreparedRow:
    task_id: str
    fingerprint: bytes
    input_digest: bytes | None
    retention_class_key: str
    retain: bool
    disposition: str
    version: int | None
    codec: str | None
    content_type: str | None
    digest: bytes | None
    inline: bytes | None
    decode_failed: bool


def _prepare_one(row: Any, *, retain_default: bool) -> _PreparedRow:
    retain = (
        bool(row.retain_rerun_input)
        if row.retain_rerun_input is not None
        else retain_default
    )
    # The forever key from the SAME authority the relocation projection
    # coalesces with: a pre-v27 row recorded no class, and no recorded
    # policy means no deletion policy applied. The fingerprint then
    # records the class the row actually lands in.
    retention_class_key = (
        row.retention_class_key
        if row.retention_class_key
        else FOREVER_CLASS_KEY
    )
    fingerprint = EnqueueCommandV1(
        task_name=row.task_name,
        queue_name=row.queue_name,
        priority=row.priority,
        args_json=row.args,
        kwargs_json=row.kwargs,
        good_until=row.good_until,
        enqueue_delay_seconds=None,
        task_options_json=row.task_options,
        retention_class_key=retention_class_key,
        retain_rerun_input=retain,
        rerun_of_task_id=None,
        rerun_root_task_id=None,
    ).fingerprint

    def parsed(value: str | None) -> Any:
        if value is None:
            return None
        loaded = loads_json(value)
        if is_err(loaded):
            raise ValueError(loaded.err_value)
        return loaded.ok_value

    try:
        args = parsed(row.args)
        kwargs = parsed(row.kwargs)
        options = parsed(row.task_options)
    except ValueError:
        return _PreparedRow(
            task_id=str(row.id),
            fingerprint=fingerprint,
            input_digest=None,
            retention_class_key=retention_class_key,
            retain=retain,
            disposition='DECLINED_BY_POLICY',
            version=None,
            codec=None,
            content_type=None,
            digest=None,
            inline=None,
            decode_failed=True,
        )
    payload = encode_input_envelope_v1(
        args=args or [],
        kwargs=kwargs or {},
        options=options,
    )
    digest = sha256(payload).digest()
    if not retain:
        disposition, envelope = 'DECLINED_BY_POLICY', False
    elif len(payload) > INPUT_ENVELOPE_INLINE_MAX_BYTES:
        disposition, envelope = 'OVER_BOUND', False
    else:
        disposition, envelope = 'INLINE', True
    return _PreparedRow(
        task_id=str(row.id),
        fingerprint=fingerprint,
        input_digest=digest,
        retention_class_key=retention_class_key,
        retain=retain,
        disposition=disposition,
        version=INPUT_ENVELOPE_VERSION if envelope else None,
        codec=INPUT_ENVELOPE_CODEC if envelope else None,
        content_type=INPUT_ENVELOPE_CONTENT_TYPE if envelope else None,
        digest=digest if envelope else None,
        inline=payload if envelope else None,
        decode_failed=False,
    )


async def prepare_legacy_batch(
    connection: AsyncConnection,
    *,
    retain_default: bool,
    batch_size: int,
    cursor: PreparationCursor,
) -> PreparationBatch | PreparationComplete:
    """Prepare one bounded batch; the caller owns the transaction.

    Selection: ANY row without a prepared disposition — terminal rows
    bound for relocation and the live rows drain deliberately spares —
    the unbackfilled marker, which also makes a resumed run idempotent,
    strictly after the cursor's keyset watermark. The tighten's entry
    check counts unprepared rows over the whole table, so the selection
    matches the postcondition this stage exists to establish. The
    watermark, not an index, removes the re-scan: a partial index on
    the IS NULL predicate would need maintenance on every backfill
    UPDATE and bloat during exactly the bulk write it is meant to help,
    while the watermark also stays immune to dead-tuple accumulation.
    The loop contract is the cursor itself: pass
    ``PreparationCursor.start()`` first, then each batch's ``cursor``.
    """
    # The keyset predicate renders only when a watermark exists, so the
    # parameter's type always resolves from the id column itself — the
    # comparison stays in the column's own order in both identity eras.
    keyset_condition = (
        '' if cursor.after_id is None else 'AND id > :after_id'
    )
    parameters: dict[str, object] = {'batch_size': batch_size}
    if cursor.after_id is not None:
        parameters['after_id'] = cursor.after_id
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT id, status, task_name, queue_name, priority, args,
                       kwargs, task_options, good_until,
                       retention_class_key, retain_rerun_input
                FROM {LIVE_TASKS}
                WHERE prepared_rerun_input_disposition IS NULL
                  {keyset_condition}
                ORDER BY id
                LIMIT :batch_size
                """
            ),
            parameters,
        )
    ).all()
    if not rows:
        total = (
            await connection.execute(
                text(
                    f"SELECT count(*) FROM {LIVE_TASKS} "
                    "WHERE prepared_rerun_input_disposition IS NOT NULL"
                )
            )
        ).scalar_one()
        return PreparationComplete(rows_prepared=int(total))

    prepared = [
        _prepare_one(row, retain_default=retain_default) for row in rows
    ]
    live_statuses = {'PENDING', 'CLAIMED', 'RUNNING'}
    live_rows = sum(row.status in live_statuses for row in rows)
    # One executemany, not one statement per row: the per-row loop cost
    # 5.4x relocation per row purely in round trips (605 vs 112 s/M).
    # The class stamp writes the resolved value — identity for rows that
    # carried one, the forever key for rows that recorded none — so the
    # tighten's SET NOT NULL finds no NULL left on any prepared row.
    await connection.execute(
        text(
            f"""
            UPDATE {LIVE_TASKS} SET
                command_fingerprint_version = 1,
                command_fingerprint = :fingerprint,
                input_digest = :input_digest,
                retention_class_key = :retention_class_key,
                retain_rerun_input = :retain,
                prepared_rerun_input_disposition = :disposition,
                prepared_rerun_input_version = :version,
                prepared_rerun_input_codec = :codec,
                prepared_rerun_input_content_type = :content_type,
                prepared_rerun_input_digest = :digest,
                prepared_rerun_input_inline = :inline
            WHERE id = :task_id
            """
        ),
        [
            {
                'task_id': item.task_id,
                'fingerprint': item.fingerprint,
                'input_digest': item.input_digest,
                'retention_class_key': item.retention_class_key,
                'retain': item.retain,
                'disposition': item.disposition,
                'version': item.version,
                'codec': item.codec,
                'content_type': item.content_type,
                'digest': item.digest,
                'inline': item.inline,
            }
            for item in prepared
        ],
    )
    return PreparationBatch(
        rows_prepared=len(prepared),
        live_rows_prepared=live_rows,
        cursor=PreparationCursor(after_id=prepared[-1].task_id),
        inline_rows=sum(p.disposition == 'INLINE' for p in prepared),
        over_bound_rows=sum(p.disposition == 'OVER_BOUND' for p in prepared),
        policy_declined_rows=sum(
            p.disposition == 'DECLINED_BY_POLICY' and not p.decode_failed
            for p in prepared
        ),
        decode_failed_rows=sum(p.decode_failed for p in prepared),
    )
