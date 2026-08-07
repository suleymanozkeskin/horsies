"""Relation and function names shared across history modules.

One module owns each object's DDL; every other module refers to it through
this vocabulary instead of a string literal, so a rename is a one-line change
and a typo is an import error rather than a runtime probe of a missing
relation.

These names are internal. They are not a public SQL contract and may change
between releases without notice.
"""

from __future__ import annotations

from typing import Final

# Owned by the existing live schema program.
LIVE_TASKS: Final = 'horsies_tasks'

# Owned by the frozen-DDL module (retention classes, pending, quarantine).
RETENTION_CLASSES: Final = 'horsies_retention_classes'
WORKFLOW_PHASE2_PENDING: Final = 'horsies_workflow_phase2_pending'
WORKFLOW_PHASE2_QUARANTINE: Final = 'horsies_workflow_phase2_quarantine'
TASK_HISTORY_PARENT: Final = 'horsies_task_history'
TASK_HISTORY_FOREVER: Final = 'horsies_task_history_forever'

# Owned by the partition manager.
LEAF_CATALOG: Final = 'horsies_task_history_leaf_catalog'
LEAF_LOCK_KEY_FUNCTION: Final = 'horsies_task_history_leaf_lock_key'

# Owned by the keyed-enqueue reservation operations.
KEY_RESERVATIONS: Final = 'horsies_key_reservations'

# Reserved catalog class key for the heartbeat partition module. Not a
# retention class in the history sense: heartbeat leaves share the leaf
# catalog machinery but never enter history, and the staged-lookup
# manifest excludes this key at assembly.
HEARTBEAT_CLASS_KEY: Final = 'heartbeats'
HEARTBEATS_TABLE: Final = 'horsies_heartbeats'

# Owned by the staged lookup loader.
TASK_LOOKUP_FUNCTION: Final = 'horsies_task_lookup_staged'
TASK_LOOKUP_TYPE: Final = 'horsies_task_lookup'
TASK_LOOKUP_MANIFEST: Final = 'horsies_task_lookup_manifest'
TASK_PROVENANCE_FUNCTION: Final = 'horsies_task_provenance_staged'
TASK_PROVENANCE_TYPE: Final = 'horsies_task_provenance'
TASK_DETAIL_FUNCTION: Final = 'horsies_task_detail_staged'
