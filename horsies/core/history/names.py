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

# Owned by the frozen-DDL module (retention classes, pending, quarantine).
RETENTION_CLASSES: Final = 'horsies_retention_classes'
WORKFLOW_PHASE2_PENDING: Final = 'horsies_workflow_phase2_pending'
WORKFLOW_PHASE2_QUARANTINE: Final = 'horsies_workflow_phase2_quarantine'
TASK_HISTORY_PARENT: Final = 'horsies_task_history'

# Owned by the partition manager.
LEAF_CATALOG: Final = 'horsies_task_history_leaf_catalog'
LEAF_LOCK_KEY_FUNCTION: Final = 'horsies_task_history_leaf_lock_key'
