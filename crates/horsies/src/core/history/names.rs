//! One-owner names shared across task-history modules.

pub const LIVE_TASKS: &str = "horsies_tasks";
pub const LIVE_ATTEMPTS: &str = "horsies_task_attempts";
pub const RETENTION_CLASSES: &str = "horsies_retention_classes";
pub const WORKFLOW_PHASE2_PENDING: &str = "horsies_workflow_phase2_pending";
pub const WORKFLOW_PHASE2_QUARANTINE: &str = "horsies_workflow_phase2_quarantine";
pub const TASK_HISTORY_PARENT: &str = "horsies_task_history";
pub const TASK_HISTORY_FOREVER: &str = "horsies_task_history_forever";
pub const LEAF_CATALOG: &str = "horsies_task_history_leaf_catalog";
pub const LEAF_LOCK_KEY_FUNCTION: &str = "horsies_task_history_leaf_lock_key";
pub const KEY_RESERVATIONS: &str = "horsies_key_reservations";
pub const HEARTBEAT_CLASS_KEY: &str = "heartbeats";
pub const HEARTBEATS_TABLE: &str = "horsies_heartbeats";
pub const TASK_LOOKUP_FUNCTION: &str = "horsies_task_lookup_staged";
pub const TASK_LOOKUP_TYPE: &str = "horsies_task_lookup";
pub const TASK_LOOKUP_MANIFEST: &str = "horsies_task_lookup_manifest";
pub const TASK_PROVENANCE_FUNCTION: &str = "horsies_task_provenance_staged";
pub const TASK_PROVENANCE_TYPE: &str = "horsies_task_provenance";
pub const TASK_DETAIL_FUNCTION: &str = "horsies_task_detail_staged";

/// PostgreSQL `NAMEDATALEN - 1`. Longer names are silently truncated.
pub const POSTGRES_IDENTIFIER_LIMIT: usize = 63;

const DAILY_LEAF_SUFFIX_LENGTH: usize = "_2026_08_11".len();
const LONGEST_INDEX_SUFFIX_LENGTH: usize = "_enqueued_idx".len();

/// Longest class key whose parent, daily leaf, and leaf indexes all fit.
pub const MAX_RETENTION_CLASS_KEY_LENGTH: usize = POSTGRES_IDENTIFIER_LIMIT
    - TASK_HISTORY_PARENT.len()
    - 1
    - DAILY_LEAF_SUFFIX_LENGTH
    - LONGEST_INDEX_SUFFIX_LENGTH;
