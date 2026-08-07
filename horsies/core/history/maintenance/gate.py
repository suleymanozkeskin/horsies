"""The archive availability gate.

One singleton row is the rendezvous: terminal transitions take a shared
lock on it and check for an open maintenance session; maintenance opens a
session and takes the row exclusively, so a transition either commits
before maintenance begins or fails closed with a typed database error
while maintenance runs. The probe lives inside the terminalization
transaction — it is work the shipping transition does, and the qualified
measurements included it.
"""

from __future__ import annotations

from typing import Final

ARCHIVE_ACCESS_GATE: Final = 'horsies_archive_access_gate'
ARCHIVE_MAINTENANCE_SESSIONS: Final = 'horsies_archive_maintenance_sessions'
ARCHIVE_AVAILABILITY_FUNCTION: Final = 'horsies_assert_archive_available'

ARCHIVE_ACCESS_GATE_DDL = f"""
CREATE TABLE {ARCHIVE_ACCESS_GATE} (
    singleton boolean PRIMARY KEY,
    CHECK (singleton IS TRUE)
)
"""

ARCHIVE_ACCESS_GATE_ROW_DML = f"""
INSERT INTO {ARCHIVE_ACCESS_GATE} (singleton) VALUES (TRUE)
"""

ARCHIVE_MAINTENANCE_SESSIONS_DDL = f"""
CREATE TABLE {ARCHIVE_MAINTENANCE_SESSIONS} (
    session_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    CHECK (ended_at IS NULL OR ended_at >= started_at)
)
"""

ARCHIVE_AVAILABILITY_FUNCTION_DDL = f"""
CREATE FUNCTION {ARCHIVE_AVAILABILITY_FUNCTION}()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM singleton
    FROM {ARCHIVE_ACCESS_GATE}
    WHERE singleton IS TRUE
    FOR SHARE;
    IF EXISTS (
        SELECT 1
        FROM {ARCHIVE_MAINTENANCE_SESSIONS}
        WHERE ended_at IS NULL
    ) THEN
        RAISE EXCEPTION 'archive maintenance is active'
            USING ERRCODE = 'object_in_use';
    END IF;
END
$function$
"""


def gate_fragments() -> tuple[str, ...]:
    """The gate objects, in installation order."""
    return (
        ARCHIVE_ACCESS_GATE_DDL,
        ARCHIVE_ACCESS_GATE_ROW_DML,
        ARCHIVE_MAINTENANCE_SESSIONS_DDL,
        ARCHIVE_AVAILABILITY_FUNCTION_DDL,
    )
