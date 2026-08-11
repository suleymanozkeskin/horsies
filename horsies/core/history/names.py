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

# PostgreSQL's identifier limit, NAMEDATALEN - 1. Over-long names are not
# rejected by the server; they are silently TRUNCATED to this many bytes.
POSTGRES_IDENTIFIER_LIMIT: Final = 63

# A retention class key is not just a key: it is interpolated into every
# relation the class owns, and the longest of those sets the budget.
#
#   horsies_task_history_<key>                      class parent
#   horsies_task_history_<key>_2026_08_11           daily leaf
#   horsies_task_history_<key>_2026_08_11_enqueued_idx   longest of all
#
# Built by finite_class_parent_name (ddl/classes.py), daily_leaf_name and
# leaf_enqueued_index_name (partitions/catalog.py). `test_class_key_budget`
# recomputes this from those functions, so changing a suffix without
# changing this constant fails the suite rather than drifting silently.
_DAILY_LEAF_SUFFIX: Final = len('_2026_08_11')
_LONGEST_INDEX_SUFFIX: Final = len('_enqueued_idx')

MAX_RETENTION_CLASS_KEY_LENGTH: Final = (
    POSTGRES_IDENTIFIER_LIMIT
    - len(TASK_HISTORY_PARENT)
    - 1  # the separator before the key
    - _DAILY_LEAF_SUFFIX
    - _LONGEST_INDEX_SUFFIX
)
"""Longest class key whose every derived relation name fits untruncated.

The bound is tight on purpose. Keys past it fail in three bands, and the
middle one is the reason a merely-shorter-than-63 check is not enough:

  19-29  the leaf is fine, but the index name exceeds the limit and is
         truncated. Harmless today only because the conformance probe
         pins the index's PROPERTY rather than its name.
  30-31  BOTH of a leaf's index names truncate to the SAME 63 bytes --
         they differ only in a suffix the truncation removes -- so the
         second CREATE INDEX dies on a duplicate relation name. Benign
         to a property probe is not benign to creation.
  32+    daily_leaf_name itself raises: the class registers, then every
         coverage pass fails on it.
"""

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
