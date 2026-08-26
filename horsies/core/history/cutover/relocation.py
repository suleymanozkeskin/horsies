"""Terminal-row relocation: the cutover's own move, not the wire move.

The wire moves' guards are correct for live traffic and wrong for
backfill, so this program shares the single-owner pieces — the
history-insert projection, the disposition ladder, the attempt
encoder, the per-kind result rules — while omitting the live-path
guards and effects BY DESIGN. Each omission, named:

- no liveness guard: every source row is terminal, which is exactly
  what the wire status guard refuses;
- no claim check and no per-task advisory lock: the fleet is drained
  and the program is offline;
- no per-batch availability-gate probe: the operator owns the gate for
  the whole program, not per call;
- no pg_notify: nobody is waiting on rows settled long ago, and a
  notify storm over millions of them serves no listener;
- NO phase-2 pending creation: every pre-cutover terminal workflow row
  was already consumed by its workflow during runtime; minting pending
  rows would hand the recovery consumer a backlog of already-applied
  effects;
- no reservation terminalization: the registry postdates every
  relocated row.

Kind projection carries recorded provenance and never invents absent
provenance: ``COALESCE(terminalization_kind, 'LEGACY_TERMINAL')``.
Retention class reads the same way, and must: the class key is the
history table's partition key and it drives deletion, so a row whose
deployment never chose a class relocates into ``forever``. Assigning
the finite default instead would put every legacy row past that
duration on the drop path at the first retention pass — a deletion
policy nobody chose. No policy recorded therefore means no policy
applied; an operator who wants legacy rows to age re-classes them
deliberately. The forever class needs no separate guarantee here: its
metadata row and its partition are emitted in the same frozen fragment
sequence as the history parent, and relocation refuses to start
without that parent.
Rows with a recorded ``CANCEL_ADMIN`` take the ruled result-swap
projection (the archive's exclusivity constraint requires it); rows
with no recorded family relocate as ``LEGACY_TERMINAL`` with their
result carried as recorded. ``final_failed_reason``: FAILED/EXPIRED
rows take the last attempt's recorded reason — for a legacy row the
last attempt IS the terminal context — and COMPLETED/CANCELLED rows
take NULL, consistent with the clearing semantics of the live
families.

Idempotence instrument: the per-row history-presence probe — a row
already found in history is skipped — the same guard the wire moves
apply through the provenance function, so a resumed relocation is
idempotent by the mechanism that makes the live path safe.

Sequencing precondition: the transitional-column backfill (the
enqueue-preparation pass over legacy rows) and leaf coverage for
every (retention class, terminal day) of the relocated population
must precede this program; the disposition ladder reads the prepared
envelope columns, and the history insert routes by terminal day.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import CreateDailyHistoryLeaf, LeafBounds, LeafRef
from ..ddl.tables import FOREVER_CLASS_KEY
from ..names import TASK_HISTORY_FOREVER, TASK_HISTORY_PARENT
from ..outcomes import LeafAlreadyConformant, LeafCreated, LeafIndexRepaired
from ..partitions.catalog import (
    CatalogPartitionBound,
    LeafIndexKind,
    daily_leaf_name,
    read_leaf_catalog_row,
    read_leaf_physical_state,
    read_retention_class,
)
from ..partitions.forever import FOREVER_LEGACY_LEAF
from ..partitions.manager import create_daily_leaf
from ..partitions.publication import UnpublishedLoader
from ..reads.publisher import StagedLoaderPublisher
from ..terminalization.move import (
    ATTEMPT_ENCODER_FUNCTION,
    LIVE_ATTEMPTS,
    LIVE_TASKS,
    disposition_case_expression,
    history_insert_sql,
    rerun_carriage_expression,
)

RELOCATION_LEDGER: Final = 'horsies_cutover_relocation_ledger'

RELOCATION_LEDGER_DDL = f"""
CREATE TABLE IF NOT EXISTS {RELOCATION_LEDGER} (
    batch_number bigint PRIMARY KEY,
    task_ids uuid[] NOT NULL,
    rows_relocated integer NOT NULL,
    legacy_kind_rows integer NOT NULL,
    committed_at timestamptz NOT NULL
)
"""
"""The reverse-copy instrument: every relocated identity, per batch.
Reversal before tightening is a documented ledger walk, not a promise.
"""

_LIVE_STATUSES: Final = "('PENDING', 'CLAIMED', 'RUNNING')"


@dataclass(frozen=True, slots=True)
class RelocationBatch:
    """One committed batch of relocated terminal rows."""

    batch_number: int
    rows_relocated: int
    legacy_kind_rows: int


@dataclass(frozen=True, slots=True)
class RelocationComplete:
    """No terminal rows remain live."""

    batches_committed: int
    rows_relocated: int


def relocation_insert_sql() -> str:
    """The relocation projection over the shared column authority."""
    result_bytes = (
        "CASE WHEN t.result IS NULL THEN NULL "
        "ELSE convert_to(t.result, 'UTF8') END"
    )
    swap_is_admin = "t.terminalization_kind = 'CANCEL_ADMIN'"
    return history_insert_sql(
        {
            'task_id': 'CAST(t.id AS uuid)',
            'task_name': 't.task_name',
            'queue_name': 't.queue_name',
            'priority': 't.priority',
            'command_fingerprint_version': 't.command_fingerprint_version',
            'command_fingerprint': 't.command_fingerprint',
            'status': 't.status',
            'terminalization_kind': (
                "COALESCE(t.terminalization_kind, 'LEGACY_TERMINAL')"
            ),
            'terminal_at': 't.terminal_at',
            'retention_anchor_at': 't.terminal_at',
            'retention_class_key': (
                f"COALESCE(t.retention_class_key, '{FOREVER_CLASS_KEY}')"
            ),
            'sent_at': 't.sent_at',
            'enqueued_at': 't.enqueued_at',
            'claimed_at': 't.claimed_at',
            'started_at': 't.started_at',
            'created_at': 't.created_at',
            'good_until': 't.good_until',
            'result_envelope_version': '1',
            'result_codec': "'json-utf8'",
            'result_content_type': "'application/json'",
            'result_payload': (
                f'CASE WHEN {swap_is_admin} THEN NULL '
                f'ELSE ({result_bytes}) END'
            ),
            'prior_result_payload': (
                f'CASE WHEN {swap_is_admin} THEN ({result_bytes}) END'
            ),
            'result_digest': (
                f'CASE WHEN {swap_is_admin} '
                f'THEN CASE WHEN t.result IS NULL THEN NULL '
                f"ELSE sha256(convert_to(t.result, 'UTF8')) END "
                f'WHEN t.result IS NULL THEN NULL '
                f"ELSE sha256(convert_to(t.result, 'UTF8')) END"
            ),
            'error_code': 't.error_code',
            'final_failed_reason': (
                "CASE WHEN t.status IN ('FAILED', 'EXPIRED') "
                'THEN last_attempt.failed_reason END'
            ),
            'retry_count': 't.retry_count',
            'max_retries': 't.max_retries',
            'last_claimed_worker_id': 't.claimed_by_worker_id',
            'last_worker_hostname': 't.worker_hostname',
            'last_worker_pid': 't.worker_pid',
            'last_worker_process_name': 't.worker_process_name',
            'input_digest': 't.input_digest',
            'rerun_of_task_id': 't.rerun_of_task_id',
            'rerun_root_task_id': 't.rerun_root_task_id',
            'workflow_id': (
                'CASE WHEN t.is_workflow_task '
                'THEN CAST(node.workflow_id AS uuid) END'
            ),
            'is_workflow_task': 't.is_workflow_task',
            'history_schema_version': '1',
            'attempt_archive_version': '1',
            'attempt_snapshot_codec': "'json-utf8'",
            'attempt_snapshot_content_type': "'application/json'",
            'attempt_snapshot': f'{ATTEMPT_ENCODER_FUNCTION}(CAST(t.id AS uuid))',
            'attempt_snapshot_digest': (
                f'sha256({ATTEMPT_ENCODER_FUNCTION}(CAST(t.id AS uuid)))'
            ),
            'rerun_input_disposition': 'd.disposition',
            'rerun_input_version': rerun_carriage_expression('version'),
            'rerun_input_codec': rerun_carriage_expression('codec'),
            'rerun_input_content_type': rerun_carriage_expression(
                'content_type'
            ),
            'rerun_input_digest': rerun_carriage_expression('digest'),
            'rerun_input_inline': rerun_carriage_expression('inline'),
            'rerun_input_reference': rerun_carriage_expression('reference'),
        },
        select_tail=f"""FROM {LIVE_TASKS} t
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) node ON TRUE
    LEFT JOIN LATERAL (
        SELECT a.failed_reason
        FROM {LIVE_ATTEMPTS} a
        WHERE a.task_id = CAST(t.id AS uuid)
        ORDER BY a.attempt DESC
        LIMIT 1
    ) last_attempt ON TRUE
    CROSS JOIN LATERAL (
        SELECT {disposition_case_expression('t', 't.status')} AS disposition
    ) d
    WHERE t.id::text = ANY(CAST(:task_ids AS text[]))""",
    )


async def relocate_terminal_batch(
    connection: AsyncConnection,
    *,
    batch_size: int,
) -> RelocationBatch | RelocationComplete:
    """Relocate one bounded batch; the caller owns the transaction.

    Selection is the idempotence probe: terminal live rows whose
    identity is not already in history, in id order.
    """
    task_ids = [
        str(row)
        for row in (
            await connection.execute(
                text(
                    f"""
                    SELECT t.id FROM {LIVE_TASKS} t
                    WHERE t.status NOT IN {_LIVE_STATUSES}
                      AND NOT EXISTS (
                          SELECT 1 FROM {TASK_HISTORY_PARENT} h
                          WHERE h.task_id = CAST(t.id AS uuid)
                      )
                    ORDER BY t.id
                    LIMIT :batch_size
                    """
                ),
                {'batch_size': batch_size},
            )
        ).scalars()
    ]
    if not task_ids:
        totals = (
            await connection.execute(
                text(
                    f'SELECT COALESCE(count(*), 0) AS batches, '
                    f'COALESCE(sum(rows_relocated), 0) AS rows '
                    f'FROM {RELOCATION_LEDGER}'
                )
            )
        ).one()
        return RelocationComplete(
            batches_committed=int(totals.batches),
            rows_relocated=int(totals.rows),
        )

    await _ensure_batch_leaf_coverage(connection, task_ids=task_ids)

    inserted = (
        await connection.execute(
            text(
                relocation_insert_sql()
                + """
    RETURNING (terminalization_kind = 'LEGACY_TERMINAL')::int
                """
            ),
            {'task_ids': task_ids},
        )
    ).scalars().all()
    if len(inserted) != len(task_ids):
        raise RuntimeError(
            f'relocation inserted {len(inserted)} of {len(task_ids)} '
            'selected rows'
        )
    legacy_kind_rows = sum(int(flag) for flag in inserted)

    await connection.execute(
        text(
            f'DELETE FROM {LIVE_ATTEMPTS} '
            'WHERE task_id = ANY(CAST(:task_ids AS uuid[]))'
        ),
        {'task_ids': task_ids},
    )
    deleted = (
        await connection.execute(
            text(
                f'DELETE FROM {LIVE_TASKS} '
                'WHERE id::text = ANY(CAST(:task_ids AS text[])) '
                'RETURNING 1'
            ),
            {'task_ids': task_ids},
        )
    ).scalars().all()
    if len(deleted) != len(task_ids):
        raise RuntimeError(
            f'relocation deleted {len(deleted)} of {len(task_ids)} '
            'relocated rows'
        )

    batch_number = int(
        (
            await connection.execute(
                text(
                    f'SELECT COALESCE(max(batch_number), 0) + 1 '
                    f'FROM {RELOCATION_LEDGER}'
                )
            )
        ).scalar_one()
    )
    await connection.execute(
        text(
            f"""
            INSERT INTO {RELOCATION_LEDGER} (
                batch_number, task_ids, rows_relocated,
                legacy_kind_rows, committed_at
            ) VALUES (
                :batch_number, CAST(:task_ids AS uuid[]),
                :rows_relocated, :legacy_kind_rows,
                statement_timestamp()
            )
            """
        ),
        {
            'batch_number': batch_number,
            'task_ids': task_ids,
            'rows_relocated': len(task_ids),
            'legacy_kind_rows': legacy_kind_rows,
        },
    )
    return RelocationBatch(
        batch_number=batch_number,
        rows_relocated=len(task_ids),
        legacy_kind_rows=legacy_kind_rows,
    )


async def _ensure_batch_leaf_coverage(
    connection: AsyncConnection,
    *,
    task_ids: list[str],
) -> None:
    """Ensure every destination day in one offline relocation batch.

    The relocation projection maps a missing legacy class to ``forever``. The
    coverage query applies that same mapping, so its distinct class/day set is
    exactly the partition set this batch's insert can reach. A v34 conversion
    retains one MINVALUE-to-conversion-day forever leaf; an older destination
    already routes there and must not request an overlapping daily partition.
    Publication occurs once after all creations, avoiding a full manifest
    rewrite per leaf.
    """
    destinations = (
        await connection.execute(
            text(
                f"""
                SELECT COALESCE(
                           retention_class_key, '{FOREVER_CLASS_KEY}'
                       ) AS class_key,
                       date_trunc('day', terminal_at, 'UTC') AS lower_anchor
                FROM {LIVE_TASKS}
                WHERE id::text = ANY(CAST(:task_ids AS text[]))
                GROUP BY COALESCE(
                             retention_class_key, '{FOREVER_CLASS_KEY}'
                         ),
                         date_trunc('day', terminal_at, 'UTC')
                ORDER BY class_key, lower_anchor
                """
            ),
            {'task_ids': task_ids},
        )
    ).all()
    created = False
    legacy_forever_upper = await _attached_legacy_forever_upper(connection)
    for destination in destinations:
        class_key = str(destination.class_key)
        lower = destination.lower_anchor
        retention_class = await read_retention_class(connection, class_key)
        if retention_class is None:
            raise RuntimeError(
                f'relocation destination class {class_key!r} is not registered'
            )
        parent_name = (
            'horsies_task_history_forever'
            if class_key == FOREVER_CLASS_KEY
            else retention_class.finite_parent_name
        )
        if parent_name is None:
            raise RuntimeError(
                f'relocation destination class {class_key!r} has no RANGE parent'
            )
        if (
            class_key == FOREVER_CLASS_KEY
            and legacy_forever_upper is not None
            and lower < legacy_forever_upper
        ):
            continue
        leaf = LeafRef(
            leaf_name=daily_leaf_name(parent_name, lower),
            class_key=class_key,
            bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
        )
        outcome = await create_daily_leaf(
            connection,
            CreateDailyHistoryLeaf(leaf=leaf),
            UnpublishedLoader(),
        )
        match outcome:
            case LeafCreated():
                created = True
            case LeafAlreadyConformant() | LeafIndexRepaired():
                pass
            case _:
                raise RuntimeError(
                    f'relocation destination leaf refused: {outcome!r}'
                )
    if created:
        await StagedLoaderPublisher().republish(connection)


async def _attached_legacy_forever_upper(
    connection: AsyncConnection,
) -> datetime | None:
    """Upper bound of the attached v34 catch-all, if it is conformant.

    The catalog supplies the typed upper bound while the physical-state check
    proves the named relation is still attached with the cataloged partition
    bound. The legacy leaf's lower physical bound is MINVALUE by construction.
    """
    catalog = await read_leaf_catalog_row(connection, FOREVER_LEGACY_LEAF)
    if (
        catalog is None
        or catalog.class_key != FOREVER_CLASS_KEY
        or catalog.parent_name != TASK_HISTORY_FOREVER
        or catalog.detached_at is not None
        or catalog.dropped_at is not None
    ):
        return None
    physical = await read_leaf_physical_state(
        connection,
        leaf_name=catalog.leaf_name,
        parent_name=catalog.parent_name,
        id_index_name=catalog.id_index_name,
        bound_expectation=CatalogPartitionBound(),
        index_kind=LeafIndexKind.HISTORY,
    )
    conformant = (
        physical.detach_pending is False
        and physical.partition_bound == catalog.partition_bound
    )
    return catalog.upper_anchor if conformant else None
