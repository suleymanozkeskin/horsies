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

import re

from ..names import LIVE_TASKS


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

LIVE_CUTOVER_COLUMNS_DDL: tuple[str, ...] = (
    f"""
    ALTER TABLE {LIVE_TASKS}
        ADD COLUMN command_fingerprint_version smallint NOT NULL
            CHECK (command_fingerprint_version > 0),
        ADD COLUMN command_fingerprint bytea NOT NULL
            CHECK (octet_length(command_fingerprint) = 32),
        ADD COLUMN retention_class_key varchar(64) NOT NULL,
        ADD COLUMN input_digest bytea
            CHECK (input_digest IS NULL OR octet_length(input_digest) = 32),
        ADD COLUMN rerun_of_task_id uuid,
        ADD COLUMN rerun_root_task_id uuid,
        ADD COLUMN idempotency_key_digest bytea
            CHECK (
                idempotency_key_digest IS NULL
                OR octet_length(idempotency_key_digest) = 32
            ),
        ADD COLUMN retain_rerun_input boolean NOT NULL,
        ADD COLUMN prepared_rerun_input_disposition varchar(32) NOT NULL
            CHECK (
                prepared_rerun_input_disposition IN (
                    'INLINE', 'REFERENCE', 'DECLINED_BY_POLICY',
                    'OVER_BOUND', 'NEVER_ELIGIBLE'
                )
            ),
        ADD COLUMN prepared_rerun_input_version smallint,
        ADD COLUMN prepared_rerun_input_codec varchar(64),
        ADD COLUMN prepared_rerun_input_content_type varchar(255),
        ADD COLUMN prepared_rerun_input_digest bytea,
        ADD COLUMN prepared_rerun_input_inline bytea
            CHECK (
                prepared_rerun_input_inline IS NULL
                OR octet_length(prepared_rerun_input_inline) <= 65536
            ),
        ADD COLUMN prepared_rerun_input_reference varchar(2048),
        ADD CONSTRAINT {LIVE_TASKS}_rerun_lineage_pair CHECK (
            (rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL)
            OR (rerun_of_task_id IS NOT NULL
                AND rerun_root_task_id IS NOT NULL)
        )
    """,
)


_COLUMN_DEFINITION = re.compile(r'ADD COLUMN (\w+) ([a-z]+(?:\(\d+\))?)')


def cutover_column_definitions() -> tuple[tuple[str, str], ...]:
    """(name, type) for every cutover column, parsed from the fragment.

    The fragment above is the single shape authority; anything else that
    needs the column set derives it from here so the two can never
    drift.
    """
    definitions = tuple(
        (match.group(1), match.group(2))
        for fragment in LIVE_CUTOVER_COLUMNS_DDL
        for match in _COLUMN_DEFINITION.finditer(fragment)
    )
    if not definitions:
        raise AssertionError('cutover fragment yielded no column definitions')
    return definitions


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
