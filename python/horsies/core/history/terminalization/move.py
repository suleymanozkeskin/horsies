"""Names of the terminal-write functions of the baseline schema.

The functions themselves are SQL in the baseline migration
(``crates/horsies-core/baseline/0003_interim_functions.sql``): the single-row
move ``horsies_move_task_to_history``, the operations that call it, and the
batch moves. This module holds the names and values that Python callers
pass to them.

Global lock-order invariant, binding on every function: the task advisory
lock (``hashtextextended(<task id>, TASK_LOCK_SEED)``) is acquired BEFORE any
row lock on that task.
"""

from __future__ import annotations

from typing import Final

LIVE_ATTEMPTS: Final = 'horsies_task_attempts'
ATTEMPT_ENCODER_FUNCTION: Final = 'horsies_encode_task_attempts'
MOVE_FUNCTION: Final = 'horsies_move_task_to_history'
CANCEL_ORPHAN_SWEEP_FUNCTION: Final = 'horsies_cancel_orphaned_tasks'
ABANDON_OWNED_NODE_FUNCTION: Final = 'horsies_abandon_owned_node'
ABANDON_OWNED_NODES_FUNCTION: Final = 'horsies_abandon_owned_nodes'
ABANDON_PAUSED_SWEEP_FUNCTION: Final = (
    'horsies_abandon_nodes_of_paused_workflows'
)
CANCEL_OWNED_NODE_FUNCTION: Final = 'horsies_cancel_owned_node'
CANCEL_OWNED_NODES_FUNCTION: Final = 'horsies_cancel_owned_nodes'
CANCEL_CANCELLED_SWEEP_FUNCTION: Final = (
    'horsies_cancel_nodes_of_cancelled_workflow'
)
CANCEL_ORPHAN_FUNCTION: Final = 'horsies_cancel_owned_orphan'
COMPLETE_FUSED_FUNCTION: Final = 'horsies_complete_task_fused'
COMPLETE_FUSED_BATCH_FUNCTION: Final = 'horsies_complete_tasks_fused'

EXPIRE_OWNED_FUNCTION: Final = 'horsies_expire_owned_claim'
EXPIRE_PENDING_FUNCTION: Final = 'horsies_expire_pending_tasks'

FINISH_FAILED_ATTEMPT_FUNCTION: Final = 'horsies_finish_failed_attempt'
LOCK_RUNNING_TASK_FUNCTION: Final = 'horsies_lock_running_task'
"""A retired locking read of a RUNNING task. The baseline does not create
it; ``horsies_finish_workflow_success`` took its one caller."""
FINISH_WORKFLOW_SUCCESS_FUNCTION: Final = 'horsies_finish_workflow_success'

TASK_LOCK_SEED: Final = 731
"""The seed of the task advisory lock. The lock key of a task is
``hashtextextended(<task id as uuid text>, TASK_LOCK_SEED)``."""
