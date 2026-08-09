"""Program installation — programs arrive with their owners, REPLACING
the in-place program the old fleet runs.

The migrated schema already carries the chain-owned terminalization
program (the in-place functions and the outcome type the pre-history
fleet calls), and the move families share that type and those names
by design — one vocabulary. Installation is therefore drop-then-
create per the established function precedent, and it REPLACES the
running fleet's program: THIS STAGE MUST FOLLOW THE DRAIN, never
precede it. No table data is touched.

No union view exists by design: the read primitives are the sole
cross-lifecycle surface, and a queryable view object would
manufacture the pseudo-public surface the ratified disposition
prevents.

Rollback is R2: the same named teardown, followed by reinstating the
chain-owned in-place program by import — the old fleet restarts
against the program it was running.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..terminalization.move import LIVE_ATTEMPTS

from ..phase2.consumption import (
    PHASE2_CONSUME_FUNCTION,
    PHASE2_DISPOSITION_TYPE,
    consumption_fragments,
)
from ..phase2.quarantine import (
    PHASE2_QUARANTINE_FUNCTION,
    PHASE2_QUARANTINE_VERDICT_TYPE,
    quarantine_fragments,
)
from ..terminalization.move import (
    ATTEMPT_ENCODER_FUNCTION,
    MOVE_FUNCTION,
    cancellation_family_fragments,
    completion_family_fragments,
    expiry_family_fragments,
    failure_family_fragments,
    workflow_node_family_fragments,
)
from ..terminalization.outcome import outcome_fragments
from ..transcode.jobs import (
    TRANSCODE_BATCHES,
    TRANSCODE_JOBS,
    TRANSCODE_MUTATION_FUNCTION,
    TRANSCODE_RELATIONS,
    job_state_fragments,
)
from ...schemas.terminalization import (
    CREATE_OUTCOME_TYPE_SQL,
    CREATE_TERMINALIZATION_FUNCTIONS_SQL,
)
from .relocation import RELOCATION_LEDGER, RELOCATION_LEDGER_DDL


def installation_fragments() -> tuple[str, ...]:
    """The stage-2 statements, in dependency order, by import."""
    # The completion family carries the attempt encoder itself.
    return (
        *outcome_fragments(),
        *completion_family_fragments(),
        *failure_family_fragments(),
        *expiry_family_fragments(),
        *cancellation_family_fragments(),
        *workflow_node_family_fragments(),
        *consumption_fragments(),
        *quarantine_fragments(),
        *job_state_fragments(),
        RELOCATION_LEDGER_DDL,
    )


@dataclass(frozen=True, slots=True)
class ProgramsRefused:
    """Installation did not run; the reasons name the preconditions.

    The move program binds the uuid identity era — its functions take
    and compare uuid task identities — so installing it against a
    varchar attempts table would fail later, mid-statement, with a raw
    operator-mismatch error naming a type instead of the omission. The
    tighten refuses on the same invariant; both doors now enforce it.
    """

    reasons: tuple[str, ...]


async def install_programs(
    connection: AsyncConnection,
) -> int | ProgramsRefused:
    """Replace the in-place program with the move program.

    Drop-then-create per the function precedent; idempotent. The drop
    half removes the chain-owned in-place program (or a prior pass of
    this one — same names, same type), which is why this runs only
    against a drained fleet. Refuses, typed, when identity
    normalization has not run: the ratified order is drain, then
    normalization, then this.
    """
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
    if not attempts_uuid:
        return ProgramsRefused(
            reasons=(
                'the attempts identity is not uuid '
                '(identity normalization has not run)',
            ),
        )
    for statement in teardown_statements():
        await connection.execute(text(statement))
    fragments = installation_fragments()
    for statement in fragments:
        await connection.execute(text(statement))
    return len(fragments)


def teardown_statements() -> tuple[str, ...]:
    """R2, the named drop list: reverse stage 2 exactly.

    The outcome-type CASCADE removes every terminalization operation
    returning it; the remaining drops name the encoder, the phase-2
    programs (by their cascade roots), the transcode state, and the
    relocation ledger.
    """
    return (
        'DROP TYPE IF EXISTS horsies_terminalization_outcome CASCADE',
        # The single-row move returns void, so the type cascade misses
        # it; named explicitly, like the encoder.
        f'DROP FUNCTION IF EXISTS {MOVE_FUNCTION}'
        '(uuid, text, text, timestamptz, text, text, text)',
        f'DROP FUNCTION IF EXISTS {ATTEMPT_ENCODER_FUNCTION}(uuid)',
        f'DROP TYPE IF EXISTS {PHASE2_DISPOSITION_TYPE} CASCADE',
        f'DROP TYPE IF EXISTS {PHASE2_QUARANTINE_VERDICT_TYPE} CASCADE',
        f'DROP FUNCTION IF EXISTS {PHASE2_CONSUME_FUNCTION} CASCADE',
        f'DROP FUNCTION IF EXISTS {PHASE2_QUARANTINE_FUNCTION} CASCADE',
        f'DROP TABLE IF EXISTS {TRANSCODE_BATCHES}',
        f'DROP TABLE IF EXISTS {TRANSCODE_RELATIONS}',
        f'DROP TABLE IF EXISTS {TRANSCODE_JOBS}',
        f'DROP FUNCTION IF EXISTS {TRANSCODE_MUTATION_FUNCTION}() CASCADE',
        f'DROP TABLE IF EXISTS {RELOCATION_LEDGER}',
    )


async def uninstall_programs(connection: AsyncConnection) -> int:
    """Execute R2: tear down, then reinstate the chain-owned in-place
    program so the old fleet restarts against what it was running."""
    statements = teardown_statements()
    for statement in statements:
        await connection.execute(text(statement))
    await connection.execute(CREATE_OUTCOME_TYPE_SQL)
    for create_function in CREATE_TERMINALIZATION_FUNCTIONS_SQL:
        await connection.execute(create_function)
    return len(statements)
