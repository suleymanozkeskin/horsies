"""The three gated DDL fragments. Exact, and not authorized.

Each fragment's column shapes are ratified; what remains open is the
qualification gate conditioning it. Nothing in the frozen fragment list
references these columns, and these statements are excluded from
`frozen_fragments()` — emitting one requires an explicit call to
`gated_fragment()` naming the gate, which is the reviewable act of
authorization.

The ALTER TABLE ... ADD COLUMN ... NOT NULL forms are valid only against
empty relations. That is the sequencing contract, not a hazard: every
gated fragment lands before production writes begin, while the history
hierarchy is still empty.
"""

from __future__ import annotations

from enum import Enum

from ..names import KEY_RESERVATIONS, TASK_HISTORY_PARENT


class GatedFragment(Enum):
    """The qualification gate each conditional fragment waits on."""

    ATTEMPT_SNAPSHOT_COLUMNS = 'ATTEMPT_SNAPSHOT_COLUMNS'
    RERUN_INPUT_COLUMNS = 'RERUN_INPUT_COLUMNS'
    RESERVATION_REGISTRY_INDEXES = 'RESERVATION_REGISTRY_INDEXES'


_ATTEMPT_SNAPSHOT_COLUMNS = (
    f"""
    ALTER TABLE {TASK_HISTORY_PARENT}
        ADD COLUMN attempt_archive_version smallint NOT NULL
            CHECK (attempt_archive_version > 0),
        ADD COLUMN attempt_snapshot_codec varchar(64) NOT NULL
            CHECK (octet_length(attempt_snapshot_codec) BETWEEN 1 AND 64),
        ADD COLUMN attempt_snapshot_content_type varchar(255) NOT NULL
            CHECK (
                octet_length(attempt_snapshot_content_type) BETWEEN 1 AND 255
            ),
        ADD COLUMN attempt_snapshot bytea NOT NULL,
        ADD COLUMN attempt_snapshot_digest bytea NOT NULL
            CHECK (octet_length(attempt_snapshot_digest) = 32)
    """,
)

_RERUN_INPUT_COLUMNS = (
    f"""
    ALTER TABLE {TASK_HISTORY_PARENT}
        ADD COLUMN rerun_input_disposition varchar(32) NOT NULL
            CHECK (
                rerun_input_disposition IN (
                    'INLINE', 'REFERENCE', 'DECLINED_BY_POLICY',
                    'OVER_BOUND', 'NEVER_ELIGIBLE'
                )
            ),
        ADD COLUMN rerun_input_version smallint
            CHECK (rerun_input_version IS NULL OR rerun_input_version > 0),
        ADD COLUMN rerun_input_codec varchar(64)
            CHECK (
                rerun_input_codec IS NULL
                OR octet_length(rerun_input_codec) BETWEEN 1 AND 64
            ),
        ADD COLUMN rerun_input_content_type varchar(255)
            CHECK (
                rerun_input_content_type IS NULL
                OR octet_length(rerun_input_content_type) BETWEEN 1 AND 255
            ),
        ADD COLUMN rerun_input_digest bytea
            CHECK (
                rerun_input_digest IS NULL
                OR octet_length(rerun_input_digest) = 32
            ),
        ADD COLUMN rerun_input_inline bytea
            CHECK (
                rerun_input_inline IS NULL
                OR octet_length(rerun_input_inline) <= 65536
            ),
        ADD COLUMN rerun_input_reference varchar(2048)
            CHECK (
                rerun_input_reference IS NULL
                OR octet_length(rerun_input_reference) BETWEEN 1 AND 2048
            )
    """,
    f"""
    ALTER TABLE {TASK_HISTORY_PARENT}
        ADD CONSTRAINT {TASK_HISTORY_PARENT}_rerun_input_shape CHECK (
            (rerun_input_disposition = 'INLINE'
                AND rerun_input_version IS NOT NULL
                AND rerun_input_codec IS NOT NULL
                AND rerun_input_content_type IS NOT NULL
                AND rerun_input_digest IS NOT NULL
                AND rerun_input_inline IS NOT NULL
                AND rerun_input_reference IS NULL)
            OR (rerun_input_disposition = 'REFERENCE'
                AND rerun_input_version IS NOT NULL
                AND rerun_input_codec IS NOT NULL
                AND rerun_input_content_type IS NOT NULL
                AND rerun_input_digest IS NOT NULL
                AND rerun_input_inline IS NULL
                AND rerun_input_reference IS NOT NULL)
            OR (rerun_input_disposition IN (
                    'DECLINED_BY_POLICY', 'OVER_BOUND', 'NEVER_ELIGIBLE'
                )
                AND rerun_input_version IS NULL
                AND rerun_input_codec IS NULL
                AND rerun_input_content_type IS NULL
                AND rerun_input_digest IS NULL
                AND rerun_input_inline IS NULL
                AND rerun_input_reference IS NULL)
        ),
        ADD CONSTRAINT {TASK_HISTORY_PARENT}_rerun_input_eligibility CHECK (
            (status <> 'COMPLETED' AND NOT is_workflow_task)
            OR rerun_input_disposition = 'NEVER_ELIGIBLE'
        )
    """,
)

_RESERVATION_REGISTRY_INDEXES = (
    f"""
    CREATE INDEX {KEY_RESERVATIONS}_expiry_idx
        ON {KEY_RESERVATIONS} (expires_at)
        WHERE disposition = 'TERMINAL'
    """,
)


def gated_fragment(gate: GatedFragment) -> tuple[str, ...]:
    """The exact statements one gate authorizes. Calling this is the act
    of emitting a conditional fragment; nothing else returns them."""
    match gate:
        case GatedFragment.ATTEMPT_SNAPSHOT_COLUMNS:
            return _ATTEMPT_SNAPSHOT_COLUMNS
        case GatedFragment.RERUN_INPUT_COLUMNS:
            return _RERUN_INPUT_COLUMNS
        case GatedFragment.RESERVATION_REGISTRY_INDEXES:
            return _RESERVATION_REGISTRY_INDEXES
