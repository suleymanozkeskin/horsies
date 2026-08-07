"""Cutover columns the live table gains for the history move.

The move reads facts that today's live schema does not carry: the enqueue
fingerprint, the retention snapshot, rerun lineage and prepared input, and
the keyed-enqueue digest. They are declared here as ALTER fragments so the
cutover migration and the disposable test harness install the same shape
from one definition. Values are written by enqueue (a later wave); the
move only ever reads them.

`retain_rerun_input` and `prepared_rerun_input_disposition` are NOT NULL
with no default: an enqueue writer that fails to classify must fail at the
insert, mirroring the no-defaults rule on history. The ALTER form is valid
against the pre-cutover table only inside the offline migration, where no
live rows violate the new constraints before backfill; the harness applies
it to an empty stand-in.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..names import LIVE_TASKS


@dataclass(frozen=True, slots=True)
class CutoverColumn:
    """One declared cutover column: the structured shape authority.

    Three renderings derive from this table — the final ADD form, the
    transitional chain migration, and the offline tighten — so the
    column set has one owner and no rendering ever parses another
    rendering's text.
    """

    name: str
    column_type: str
    not_null: bool
    check: str | None


CUTOVER_COLUMNS: tuple[CutoverColumn, ...] = (
    CutoverColumn(
        'command_fingerprint_version', 'smallint', True,
        'command_fingerprint_version > 0',
    ),
    CutoverColumn(
        'command_fingerprint', 'bytea', True,
        'octet_length(command_fingerprint) = 32',
    ),
    CutoverColumn('retention_class_key', 'varchar(64)', True, None),
    CutoverColumn(
        'input_digest', 'bytea', False,
        'input_digest IS NULL OR octet_length(input_digest) = 32',
    ),
    CutoverColumn('rerun_of_task_id', 'uuid', False, None),
    CutoverColumn('rerun_root_task_id', 'uuid', False, None),
    CutoverColumn(
        'idempotency_key_digest', 'bytea', False,
        'idempotency_key_digest IS NULL\n'
        '                OR octet_length(idempotency_key_digest) = 32',
    ),
    CutoverColumn('retain_rerun_input', 'boolean', True, None),
    CutoverColumn(
        'prepared_rerun_input_disposition', 'varchar(32)', True,
        "prepared_rerun_input_disposition IN (\n"
        "                    'INLINE', 'REFERENCE', 'DECLINED_BY_POLICY',\n"
        "                    'OVER_BOUND', 'NEVER_ELIGIBLE'\n"
        '                )',
    ),
    CutoverColumn('prepared_rerun_input_version', 'smallint', False, None),
    CutoverColumn('prepared_rerun_input_codec', 'varchar(64)', False, None),
    CutoverColumn(
        'prepared_rerun_input_content_type', 'varchar(255)', False, None
    ),
    CutoverColumn('prepared_rerun_input_digest', 'bytea', False, None),
    CutoverColumn(
        'prepared_rerun_input_inline', 'bytea', False,
        'prepared_rerun_input_inline IS NULL\n'
        '                OR octet_length(prepared_rerun_input_inline) '
        '<= 65536',
    ),
    CutoverColumn(
        'prepared_rerun_input_reference', 'varchar(2048)', False, None
    ),
)

RERUN_LINEAGE_PAIR_CHECK = (
    '(rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL)\n'
    '            OR (rerun_of_task_id IS NOT NULL\n'
    '                AND rerun_root_task_id IS NOT NULL)'
)


LIVE_STATUS_DOMAIN_DDL = f"""
ALTER TABLE {LIVE_TASKS}
    ADD CONSTRAINT {LIVE_TASKS}_live_status_only
    CHECK (status IN ('PENDING', 'CLAIMED', 'RUNNING'))
"""
"""The live-only status domain, as a declared fragment.

The miss classifier reasons from this constraint — a live row cannot be
terminal — so it must exist in production DDL, not only in fixtures. The
migration applies it after every terminal row has moved to history; the
superseded status constraint that admitted terminal values is dropped in
the same migration stage.
"""

def _final_add_column(column: CutoverColumn) -> str:
    rendered = f'ADD COLUMN {column.name} {column.column_type}'
    if column.not_null:
        rendered += ' NOT NULL'
    if column.check is not None:
        rendered += (
            f'\n            CHECK ({column.check})'
            if '\n' in column.check
            else f'\n            CHECK ({column.check})'
        )
    return rendered


LIVE_CUTOVER_COLUMNS_DDL: tuple[str, ...] = (
    f"""
    ALTER TABLE {LIVE_TASKS}
        """
    + ',\n        '.join(
        _final_add_column(column) for column in CUTOVER_COLUMNS
    )
    + f""",
        ADD CONSTRAINT {LIVE_TASKS}_rerun_lineage_pair CHECK (
            {RERUN_LINEAGE_PAIR_CHECK}
        )
    """,
)


def cutover_column_definitions() -> tuple[tuple[str, str], ...]:
    """(name, type) for every cutover column, from the structured
    authority."""
    return tuple(
        (column.name, column.column_type) for column in CUTOVER_COLUMNS
    )


def tightening_cutover_ddl() -> tuple[str, ...]:
    """The offline tighten: the transitional columns reach the declared
    final shape. Rendered from the same structured table as the final
    and transitional forms — no rendering parses another's text. Valid
    only after the backfill: every terminal row has moved and every
    remaining row carries real values."""
    statements: list[str] = []
    set_not_null = ',\n    '.join(
        f'ALTER COLUMN {column.name} SET NOT NULL'
        for column in CUTOVER_COLUMNS
        if column.not_null
    )
    statements.append(f'ALTER TABLE {LIVE_TASKS}\n    {set_not_null}')
    for column in CUTOVER_COLUMNS:
        if column.check is not None:
            statements.append(
                f'ALTER TABLE {LIVE_TASKS}\n'
                f'    ADD CONSTRAINT {LIVE_TASKS}_{column.name}_cutover'
                f'\n    CHECK ({column.check})'
            )
    statements.append(
        f'ALTER TABLE {LIVE_TASKS}\n'
        f'    ADD CONSTRAINT {LIVE_TASKS}_rerun_lineage_pair\n'
        f'    CHECK ({RERUN_LINEAGE_PAIR_CHECK})'
    )
    return tuple(statements)


def transitional_cutover_columns_ddl() -> str:
    """The permissive chain migration: same columns, no constraints.

    TRANSITIONAL by design: this is the columns of
    `LIVE_CUTOVER_COLUMNS_DDL` stripped of every NOT NULL and CHECK — a
    catalog-only ALTER that is safe on any install, so the converted
    enqueue statement can write real values everywhere before the
    cutover migration runs. The fragment above remains the authoritative
    final shape; the cutover migration backfills and tightens to it.
    Old writers impose NULLs only on their own rows, which that backfill
    owns.
    """
    columns = ',\n    '.join(
        f'ADD COLUMN IF NOT EXISTS {name} {column_type}'
        for name, column_type in cutover_column_definitions()
    )
    return f'ALTER TABLE {LIVE_TASKS}\n    {columns}'
