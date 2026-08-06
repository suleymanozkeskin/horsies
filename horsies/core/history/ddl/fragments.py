"""The ordered fragment list: what installs, in which order, and what waits.

Three tiers, strictly ordered:

1. `frozen_fragments()` — everything the closed gates authorize, in
   dependency order. Installable today into a disposable schema or, at the
   schema-objects release phase, into the production migration program.
2. `cutover_fragments()` — statements that depend on existing relations
   changing shape at the hard cutover (today's workflow tables carry text
   identifiers; the composite pending foreign key needs their native-uuid
   conversion first). Emitting them earlier would fail on type mismatch.
3. The gated fragments in `conditional` — emitted only through
   `gated_fragment()` when their qualification gate closes.

The frozen list embeds DDL owned by other modules (the partitions leaf
catalog and lock function, the reads lookup type and manifest) by
reference, never by redefinition.
"""

from __future__ import annotations

from ..identity.reservations import reservation_function_fragments
from ..names import WORKFLOW_PHASE2_PENDING
from ..partitions.catalog import LEAF_CATALOG_DDL, LEAF_LOCK_KEY_FUNCTION_DDL
from ..reads.lookup_generation import (
    TASK_LOOKUP_MANIFEST_DDL,
    TASK_LOOKUP_TYPE_DDL,
)
from .tables import (
    FOREVER_CLASS_ROW_DML,
    KEY_RESERVATIONS_DDL,
    RETENTION_CLASSES_DDL,
    TASK_HISTORY_FOREVER_DDL,
    TASK_HISTORY_FOREVER_ID_INDEX_DDL,
    TASK_HISTORY_PARENT_DDL,
    WORKFLOW_PHASE2_PENDING_DDL,
    WORKFLOW_PHASE2_PENDING_INDEX_DDL,
    WORKFLOW_PHASE2_QUARANTINE_DDL,
)


def frozen_fragments() -> tuple[str, ...]:
    """Every statement the closed gates authorize, in dependency order."""
    return (
        RETENTION_CLASSES_DDL,
        FOREVER_CLASS_ROW_DML,
        TASK_HISTORY_PARENT_DDL,
        TASK_HISTORY_FOREVER_DDL,
        TASK_HISTORY_FOREVER_ID_INDEX_DDL,
        LEAF_CATALOG_DDL,
        LEAF_LOCK_KEY_FUNCTION_DDL,
        TASK_LOOKUP_TYPE_DDL,
        TASK_LOOKUP_MANIFEST_DDL,
        WORKFLOW_PHASE2_QUARANTINE_DDL,
        WORKFLOW_PHASE2_PENDING_DDL,
        *WORKFLOW_PHASE2_PENDING_INDEX_DDL,
        KEY_RESERVATIONS_DDL,
        *reservation_function_fragments(),
    )


def cutover_fragments() -> tuple[str, ...]:
    """Statements deferred to the hard cutover's identifier conversion.

    The composite foreign key structurally pins node identity to workflow
    while pending exists — the ratified locator contract — but it can only
    exist once `horsies_workflow_tasks` carries native-uuid identifiers
    and a unique constraint over (id, workflow_id).
    """
    return (
        """
        ALTER TABLE horsies_workflow_tasks
            ADD CONSTRAINT horsies_workflow_tasks_node_workflow_key
            UNIQUE (id, workflow_id)
        """,
        f"""
        ALTER TABLE {WORKFLOW_PHASE2_PENDING}
            ADD CONSTRAINT {WORKFLOW_PHASE2_PENDING}_node_fkey
            FOREIGN KEY (workflow_node_row_id, workflow_id)
            REFERENCES horsies_workflow_tasks (id, workflow_id)
        """,
    )
