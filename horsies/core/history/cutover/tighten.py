"""Stage 5: tighten to frozen — the point of no return.

Crossed at the FIRST statement. Entry is refused without completed
stage-4 state and the operator's confirmation against a named backup,
and the refusal guards CORRECTNESS, not tidiness: the wire moves and
the post-cutover fleet read uuid identities and the live-only status
domain, so their correctness DEPENDS on the conversions this stage
performs — a fleet started against a half-tightened schema is not a
degraded fleet, it is a wrong one. After the first statement here the
old fleet can never run again; the only reversal is the named
restore, stated as such and never dressed as a rollback.

In order: the transitional columns reach the declared final shape
(rendered from the structured authority); the status domain narrows
to live-only, dropping whatever superseded status constraint the
catalog carries (found by column dependency, never by comparing
rendered text); the identity conversions — the live table's own id,
the attempts foreign key restored with both sides uuid, the workflow
tables' ratified column list with their foreign keys dropped and
restored around the conversion; the composite pending key that
structurally requires native-uuid shapes; and the heartbeat cutover —
the old table drops (nothing migrates) and the partitioned shape is
created EMPTY and leafless, which also discharges its identity
conversion by construction.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..ddl.fragments import cutover_fragments
from ..heartbeats.partitioning import HEARTBEATS_PARTITIONED_DDL
from ..terminalization.live_cutover import (
    LIVE_STATUS_DOMAIN_DDL,
    tightening_cutover_ddl,
)
from ..terminalization.move import LIVE_ATTEMPTS, LIVE_TASKS


@dataclass(frozen=True, slots=True)
class TightenRefused:
    """The gate held; nothing was executed."""

    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TightenComplete:
    """The schema is frozen; the old fleet can never run again."""

    statements_executed: int


def confirmation_phrase(backup_label: str) -> str:
    """The exact phrase the operator must supply — typing it is the
    confirmation that the named backup exists."""
    return f'point-of-no-return: {backup_label}'


async def _entry_violations(
    connection: AsyncConnection,
) -> tuple[str, ...]:
    counts = (
        await connection.execute(
            text(
                f"""
                SELECT
                    count(*) FILTER (
                        WHERE status NOT IN
                            ('PENDING', 'CLAIMED', 'RUNNING')
                    ) AS terminal_rows,
                    count(*) FILTER (
                        WHERE status IN ('CLAIMED', 'RUNNING')
                    ) AS in_flight_rows,
                    count(*) FILTER (
                        WHERE prepared_rerun_input_disposition IS NULL
                    ) AS unprepared_rows,
                    count(*) FILTER (
                        WHERE command_fingerprint IS NULL
                    ) AS unfingerprinted_rows,
                    count(*) FILTER (
                        WHERE retention_class_key IS NULL
                    ) AS unclassified_live_rows
                FROM {LIVE_TASKS}
                """
            )
        )
    ).one()
    attempts_uuid = bool(
        (
            await connection.execute(
                text(
                    """
                    SELECT atttypid = 'uuid'::regtype
                    FROM pg_attribute
                    WHERE attrelid = CAST(:relation AS regclass)
                      AND attname = 'task_id'
                    """
                ),
                {'relation': LIVE_ATTEMPTS},
            )
        ).scalar_one()
    )
    violations: list[str] = []
    if int(counts.terminal_rows):
        violations.append(
            f'{counts.terminal_rows} terminal rows remain live '
            '(relocation incomplete)'
        )
    if int(counts.in_flight_rows):
        violations.append(
            f'{counts.in_flight_rows} rows are in flight '
            '(the fleet is not drained)'
        )
    if int(counts.unprepared_rows):
        violations.append(
            f'{counts.unprepared_rows} rows lack a prepared disposition '
            '(preparation incomplete)'
        )
    if int(counts.unfingerprinted_rows):
        violations.append(
            f'{counts.unfingerprinted_rows} rows lack a command '
            'fingerprint (preparation incomplete)'
        )
    if int(counts.unclassified_live_rows):
        # Distinct from the preflight's unclassified count, which is
        # about TERMINAL rows: those relocate, and the relocation
        # coalesces a missing class to forever. These are LIVE rows,
        # which nothing relocates and nothing coalesces, and they are
        # what the class column's SET NOT NULL fails on.
        violations.append(
            f'{counts.unclassified_live_rows} live rows carry no '
            'retention class (backfill a class before tightening)'
        )
    if not attempts_uuid:
        violations.append(
            'the attempts identity is not uuid '
            '(identity normalization has not run)'
        )
    return tuple(violations)


_UUID_TEXT = (
    '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}'
    '-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

_CONVERTED_IDENTITY_COLUMNS: tuple[tuple[str, str], ...] = (
    (LIVE_TASKS, 'id'),
    ('horsies_workflows', 'id'),
    ('horsies_workflows', 'parent_workflow_id'),
    ('horsies_workflows', 'root_workflow_id'),
    ('horsies_workflow_tasks', 'id'),
    ('horsies_workflow_tasks', 'workflow_id'),
    ('horsies_workflow_tasks', 'task_id'),
)


async def _identity_parse_violations(
    connection: AsyncConnection,
) -> tuple[str, ...]:
    """Rows whose identity text cannot parse as uuid, named BEFORE the
    point of no return. No foreign key ever policed FORMAT on any of
    these columns — a key polices reference, not spelling — so the
    gate verifies every column the conversion will cast, rather than
    discovering a bad value mid-tighten on the wrong side of the
    boundary. Realistically zero rows; the gate must know, not
    assume."""
    violations: list[str] = []
    for table, column in _CONVERTED_IDENTITY_COLUMNS:
        bad = int(
            (
                await connection.execute(
                    text(
                        f'SELECT count(*) FROM {table} '
                        f'WHERE {column} IS NOT NULL '
                        f'AND {column}::text !~ :uuid_text'
                    ),
                    {'uuid_text': _UUID_TEXT},
                )
            ).scalar_one()
        )
        if bad:
            violations.append(
                f'{bad} rows in {table}.{column} do not parse as uuid'
            )
    return tuple(violations)


async def _status_check_constraints(
    connection: AsyncConnection,
) -> tuple[str, ...]:
    """CHECK constraints depending on the status column, by catalog
    column dependency — never by comparing rendered text."""
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT con.conname
                FROM pg_constraint con
                WHERE con.conrelid = CAST(:relation AS regclass)
                  AND con.contype = 'c'
                  AND (
                      SELECT att.attnum FROM pg_attribute att
                      WHERE att.attrelid = con.conrelid
                        AND att.attname = 'status'
                  ) = ANY(con.conkey)
                  AND con.conname <> '{LIVE_TASKS}_live_status_only'
                """
            ),
            {'relation': LIVE_TASKS},
        )
    ).scalars()
    return tuple(str(name) for name in rows)


@dataclass(frozen=True, slots=True)
class _ForeignKey:
    table: str
    name: str
    column: str
    definition: str


async def _keys_referencing_workflows(
    connection: AsyncConnection,
) -> tuple[_ForeignKey, ...]:
    """Every foreign key whose target is the workflows table — the
    self-referential parent key included. Definitions are captured for
    REPLAY after the conversion (replay-as-DDL is the safe deparse
    category; nothing compares or hashes this text)."""
    rows = (
        await connection.execute(
            text(
                """
                SELECT con.conrelid::regclass::text AS table_name,
                       con.conname,
                       (SELECT att.attname FROM pg_attribute att
                        WHERE att.attrelid = con.conrelid
                          AND att.attnum = con.conkey[1]) AS column_name,
                       pg_get_constraintdef(con.oid) AS definition
                FROM pg_constraint con
                WHERE con.confrelid = 'horsies_workflows'::regclass
                  AND con.contype = 'f'
                ORDER BY con.conname
                """
            )
        )
    ).all()
    return tuple(
        _ForeignKey(
            table=str(row.table_name),
            name=str(row.conname),
            column=str(row.column_name),
            definition=str(row.definition),
        )
        for row in rows
    )


async def tighten_to_frozen(
    connection: AsyncConnection,
    *,
    backup_label: str,
    operator_confirmation: str,
) -> TightenRefused | TightenComplete:
    """The caller owns the transaction; refusal executes nothing."""
    reasons: list[str] = []
    if operator_confirmation != confirmation_phrase(backup_label):
        reasons.append(
            'operator confirmation does not name the backup '
            f'(expected the exact phrase for {backup_label!r})'
        )
    reasons.extend(await _entry_violations(connection))
    reasons.extend(await _identity_parse_violations(connection))
    if reasons:
        return TightenRefused(reasons=tuple(reasons))

    statements: list[str] = []
    statements.extend(tightening_cutover_ddl())
    for superseded in await _status_check_constraints(connection):
        statements.append(
            f'ALTER TABLE {LIVE_TASKS} DROP CONSTRAINT "{superseded}"'
        )
    statements.append(LIVE_STATUS_DOMAIN_DDL)
    statements.append(
        f'ALTER TABLE {LIVE_TASKS} '
        'ALTER COLUMN id TYPE uuid USING id::uuid'
    )
    # On a fresh install the attempts key was never dropped (the
    # identity was uuid from birth and normalization was a no-op);
    # drop-then-add makes the restoration deterministic on both
    # shapes.
    statements.append(
        f'ALTER TABLE {LIVE_ATTEMPTS} '
        'DROP CONSTRAINT IF EXISTS horsies_task_attempts_task_id_fkey'
    )
    statements.append(
        f'ALTER TABLE {LIVE_ATTEMPTS} '
        'ADD CONSTRAINT horsies_task_attempts_task_id_fkey '
        f'FOREIGN KEY (task_id) REFERENCES {LIVE_TASKS}(id) '
        'ON DELETE CASCADE'
    )
    # Every key referencing workflows drops before the conversion and
    # replays after it — including the self-referential parent key,
    # whose referencing column the key itself forces into the
    # conversion set.
    workflow_keys = await _keys_referencing_workflows(connection)
    for key in workflow_keys:
        statements.append(
            f'ALTER TABLE {key.table} DROP CONSTRAINT "{key.name}"'
        )
    statements.append(
        'ALTER TABLE horsies_workflows '
        'ALTER COLUMN id TYPE uuid USING id::uuid'
    )
    converted: set[tuple[str, str]] = set()
    for key in workflow_keys:
        if (key.table, key.column) not in converted:
            converted.add((key.table, key.column))
            statements.append(
                f'ALTER TABLE {key.table} ALTER COLUMN {key.column} '
                f'TYPE uuid USING {key.column}::uuid'
            )
    # root_workflow_id converts by the domain ruling: the identity
    # domain converts as a DOMAIN, not as a set of FK-forced columns —
    # ::uuid preserves the value exactly; an encoding is not a fact
    # about the past. Its parse-safety was proven at the gate.
    for table, column in (
        ('horsies_workflows', 'root_workflow_id'),
        ('horsies_workflow_tasks', 'id'),
        ('horsies_workflow_tasks', 'task_id'),
    ):
        statements.append(
            f'ALTER TABLE {table} ALTER COLUMN {column} '
            f'TYPE uuid USING {column}::uuid'
        )
    for key in workflow_keys:
        statements.append(
            f'ALTER TABLE {key.table} ADD CONSTRAINT "{key.name}" '
            f'{key.definition}'
        )
    statements.extend(cutover_fragments())
    statements.append('DROP TABLE horsies_heartbeats')
    statements.append(HEARTBEATS_PARTITIONED_DDL)
    for statement in statements:
        await connection.execute(text(statement))
    return TightenComplete(statements_executed=len(statements))
