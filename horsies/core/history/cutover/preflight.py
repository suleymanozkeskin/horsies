"""Stage 0: read-only preflight — the typed plan with its estimate.

Asserts the emitted schema is present, inventories the work the
program will do, and carries the estimate WITH its fitted coefficients
and the planning ceiling as first-class fields — never a bare
duration. The coefficients come from measured runs (the ladder
campaign refits them per rung); the ceiling is the ruled ×1.25 over
the estimate, and a run that busts its ceiling disproves the estimate
rather than overriding it.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..names import TASK_HISTORY_PARENT
from ..terminalization.move import LIVE_TASKS
from ...schemas.migrations import SCHEMA_VERSION

PLANNING_CEILING_NUMERATOR = 5
PLANNING_CEILING_DENOMINATOR = 4


@dataclass(frozen=True, slots=True)
class RelocationCoefficients:
    """Fitted from measured runs; first-class, never implied."""

    seconds_per_million_rows: float
    fixed_seconds: float


@dataclass(frozen=True, slots=True)
class CutoverEstimate:
    coefficients: RelocationCoefficients
    rows: int
    estimated_seconds: float
    ceiling_seconds: float


@dataclass(frozen=True, slots=True)
class CutoverPreflight:
    """The typed plan: what exists, what moves, what it should cost."""

    stored_schema_version: int
    history_parent_present: bool
    terminal_live_rows: int
    unrecorded_kind_rows: int
    unfingerprinted_rows: int
    unprepared_envelope_rows: int
    class_day_pairs: int
    workflow_rows: int
    heartbeat_rows: int
    estimate: CutoverEstimate


class PreflightError(Exception):
    """The database is not in the state the program requires."""


def estimate_relocation(
    coefficients: RelocationCoefficients, *, rows: int
) -> CutoverEstimate:
    estimated = (
        coefficients.fixed_seconds
        + coefficients.seconds_per_million_rows * rows / 1_000_000
    )
    return CutoverEstimate(
        coefficients=coefficients,
        rows=rows,
        estimated_seconds=estimated,
        ceiling_seconds=(
            estimated
            * PLANNING_CEILING_NUMERATOR
            / PLANNING_CEILING_DENOMINATOR
        ),
    )


async def run_preflight(
    connection: AsyncConnection,
    *,
    coefficients: RelocationCoefficients,
) -> CutoverPreflight:
    """Read-only; raises PreflightError when the schema is not ready."""
    stored = int(
        (
            await connection.execute(
                text('SELECT COALESCE(MAX(version), 0) FROM horsies_schema_version')
            )
        ).scalar_one()
    )
    if stored < SCHEMA_VERSION:
        raise PreflightError(
            f'stored schema version {stored} predates {SCHEMA_VERSION}; '
            'run the ordinary migration chain first'
        )
    parent_present = bool(
        (
            await connection.execute(
                text(
                    f"SELECT to_regclass('{TASK_HISTORY_PARENT}') "
                    'IS NOT NULL'
                )
            )
        ).scalar_one()
    )
    if not parent_present:
        raise PreflightError('the emitted history foundation is absent')

    inventory = (
        await connection.execute(
            text(
                f"""
                SELECT
                    count(*) FILTER (WHERE terminal) AS terminal_rows,
                    count(*) FILTER (
                        WHERE terminal AND terminalization_kind IS NULL
                    ) AS unrecorded_kind_rows,
                    count(*) FILTER (
                        WHERE terminal AND command_fingerprint IS NULL
                    ) AS unfingerprinted_rows,
                    count(*) FILTER (
                        WHERE terminal
                          AND prepared_rerun_input_disposition IS NULL
                    ) AS unprepared_rows,
                    count(DISTINCT CASE WHEN terminal THEN
                        (retention_class_key,
                         date_trunc('day', terminal_at AT TIME ZONE 'UTC'))
                    END) AS class_day_pairs
                FROM (
                    SELECT *,
                           status NOT IN ('PENDING', 'CLAIMED', 'RUNNING')
                               AS terminal
                    FROM {LIVE_TASKS}
                ) rows
                """
            )
        )
    ).one()
    counts = (
        await connection.execute(
            text(
                'SELECT (SELECT count(*) FROM horsies_workflows) AS wf, '
                '(SELECT count(*) FROM horsies_heartbeats) AS hb'
            )
        )
    ).one()
    terminal_rows = int(inventory.terminal_rows)
    return CutoverPreflight(
        stored_schema_version=stored,
        history_parent_present=parent_present,
        terminal_live_rows=terminal_rows,
        unrecorded_kind_rows=int(inventory.unrecorded_kind_rows),
        unfingerprinted_rows=int(inventory.unfingerprinted_rows),
        unprepared_envelope_rows=int(inventory.unprepared_rows),
        class_day_pairs=int(inventory.class_day_pairs),
        workflow_rows=int(counts.wf),
        heartbeat_rows=int(counts.hb),
        estimate=estimate_relocation(coefficients, rows=terminal_rows),
    )
