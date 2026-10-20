"""The move family's duplicate-identity guard and its clock-skew exceptions.

The guard (`horsies_task_history_duplicate_staged`) is rendered by the staged
loader publisher from the published manifest. For a UUIDv7 id it probes only
the leaves that can hold a row born at that time, minus the clock bound.
A producer whose clock ran ahead mints an id whose birth is later than the
row's terminal time, so the row lands in a leaf the guard skips. Every move
records such an id in the clock-skew exception table, and the guard consults
the table when its probes find nothing. Producers are never refused for a
wrong clock; only the guard's lookup changes.

An exception row lives as long as the history row it stands for: dropping a
leaf deletes the exceptions of the rows that leaf held.
"""

from __future__ import annotations

from typing import Final

from ..names import (
    CLOCK_SKEW_EXCEPTIONS,
    TASK_DUPLICATE_GUARD_FUNCTION,
    TASK_PROVENANCE_FUNCTION,
    UUID7_BIRTH_FUNCTION,
)
from ..reads.lookup_generation import CLOCK_BOUND_SECONDS

CLOCK_SKEW_EXCEPTIONS_DDL: Final = f"""
CREATE TABLE IF NOT EXISTS {CLOCK_SKEW_EXCEPTIONS} (
    task_id uuid PRIMARY KEY,
    retention_class_key varchar(64) NOT NULL,
    terminal_at timestamptz NOT NULL,
    birth_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL
)
"""

CLOCK_SKEW_EXCEPTIONS_INDEX_DDL: Final = f"""
CREATE INDEX IF NOT EXISTS {CLOCK_SKEW_EXCEPTIONS}_leaf_idx
    ON {CLOCK_SKEW_EXCEPTIONS} (retention_class_key, terminal_at)
"""

UUID7_BIRTH_FUNCTION_DDL: Final = f"""
CREATE OR REPLACE FUNCTION {UUID7_BIRTH_FUNCTION}(p_id uuid)
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
STRICT
AS $function$
    SELECT CASE
        WHEN (get_byte(uuid_send(p_id), 6) >> 4) = 7
         AND (get_byte(uuid_send(p_id), 8) & 192) = 128
        THEN to_timestamp((
                (get_byte(uuid_send(p_id), 0)::bigint << 40)
              | (get_byte(uuid_send(p_id), 1)::bigint << 32)
              | (get_byte(uuid_send(p_id), 2)::bigint << 24)
              | (get_byte(uuid_send(p_id), 3)::bigint << 16)
              | (get_byte(uuid_send(p_id), 4)::bigint << 8)
              | get_byte(uuid_send(p_id), 5)::bigint
             )::double precision / 1000.0)
    END
$function$
"""
"""The v7 birth time of an id, or NULL for an id that is not UUIDv7."""

# Installed only when no guard exists, so it never replaces a rendered one.
# It keeps the full probe until the first publication renders the pruned
# guard. It carries no manifest stamp, so the first coverage pass renders it.
DUPLICATE_GUARD_FIRST_VERSION_DDL: Final = f"""
DO $guard$
BEGIN
    IF to_regprocedure('{TASK_DUPLICATE_GUARD_FUNCTION}(uuid)') IS NULL THEN
        EXECUTE $first$
            CREATE FUNCTION {TASK_DUPLICATE_GUARD_FUNCTION}(p_task_id uuid)
            RETURNS boolean
            LANGUAGE plpgsql
            STABLE
            AS $function$
            BEGIN
                RETURN (
                    SELECT prov.found
                    FROM {TASK_PROVENANCE_FUNCTION}(p_task_id, FALSE) AS prov
                ) OR EXISTS (
                    SELECT 1 FROM {CLOCK_SKEW_EXCEPTIONS}
                    WHERE task_id = p_task_id
                );
            END
            $function$
        $first$;
    END IF;
END
$guard$
"""

DROP_DUPLICATE_GUARD_DDL: Final = (
    f'DROP FUNCTION IF EXISTS {TASK_DUPLICATE_GUARD_FUNCTION}(uuid)'
)
DROP_CLOCK_SKEW_EXCEPTIONS_DDL: Final = f'DROP TABLE IF EXISTS {CLOCK_SKEW_EXCEPTIONS}'
DROP_UUID7_BIRTH_FUNCTION_DDL: Final = (
    f'DROP FUNCTION IF EXISTS {UUID7_BIRTH_FUNCTION}(uuid)'
)


def duplicate_guard_fragments() -> tuple[str, ...]:
    """The exception table, the birth helper, and the first guard version."""
    return (
        CLOCK_SKEW_EXCEPTIONS_DDL,
        CLOCK_SKEW_EXCEPTIONS_INDEX_DDL,
        UUID7_BIRTH_FUNCTION_DDL,
        DUPLICATE_GUARD_FIRST_VERSION_DDL,
    )


def duplicate_guard_teardown() -> tuple[str, ...]:
    """Reverse `duplicate_guard_fragments`."""
    return (
        DROP_DUPLICATE_GUARD_DDL,
        DROP_UUID7_BIRTH_FUNCTION_DDL,
        DROP_CLOCK_SKEW_EXCEPTIONS_DDL,
    )


def record_skewed_ids_sql(*, ids_sql: str, terminal_at_sql: str) -> str:
    """Record each moved v7 id whose terminal time is before its birth
    minus the clock bound.

    `ids_sql` yields the moved ids as column `id` of the live rows `t`
    (`FROM {{LIVE_TASKS}} t WHERE ...`); `terminal_at_sql` is the move's
    terminal instant. A re-recorded id keeps its first row.
    """
    return f"""INSERT INTO {CLOCK_SKEW_EXCEPTIONS} (
        task_id, retention_class_key, terminal_at, birth_at, recorded_at
    )
    SELECT t.id, t.retention_class_key, {terminal_at_sql},
           {UUID7_BIRTH_FUNCTION}(t.id), statement_timestamp()
    {ids_sql}
      AND {UUID7_BIRTH_FUNCTION}(t.id)
          - INTERVAL '{CLOCK_BOUND_SECONDS} seconds' > {terminal_at_sql}
    ON CONFLICT (task_id) DO NOTHING"""


DELETE_LEAF_EXCEPTIONS_SQL: Final = f"""
DELETE FROM {CLOCK_SKEW_EXCEPTIONS}
WHERE retention_class_key = :class_key
  AND terminal_at >= :lower
  AND terminal_at < :upper
"""
"""Delete the exceptions of the rows a dropped leaf held."""
