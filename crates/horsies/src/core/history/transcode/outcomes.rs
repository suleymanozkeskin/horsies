//! Closed vocabulary for replacement-partition archive transcodes.

use uuid::Uuid;

pub const BLOCKER_QUERY_TRUNCATION_CHARS: usize = 1024;
pub const SWAP_LOCK_ATTEMPTS_MAXIMUM: u32 = 120;
pub const SWAP_RETRY_BACKOFF_SECONDS: f64 = 0.25;
pub const SWAP_LOCK_SECONDS_MAXIMUM: f64 = 2.0;
pub const MAINTENANCE_SECONDS_MAXIMUM: f64 = 600.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveComponent {
    HistoryRow,
    Result,
    Attempts,
    RerunInput,
}

impl ArchiveComponent {
    pub const ALL: [Self; 4] = [
        Self::HistoryRow,
        Self::Result,
        Self::Attempts,
        Self::RerunInput,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HistoryRow => "HISTORY_ROW",
            Self::Result => "RESULT",
            Self::Attempts => "ATTEMPTS",
            Self::RerunInput => "RERUN_INPUT",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "HISTORY_ROW" => Some(Self::HistoryRow),
            "RESULT" => Some(Self::Result),
            "ATTEMPTS" => Some(Self::Attempts),
            "RERUN_INPUT" => Some(Self::RerunInput),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TranscodeJobState {
    Planned,
    Copying,
    Copied,
    Verified,
    Swapped,
    Complete,
}

impl TranscodeJobState {
    pub const ALL: [Self; 6] = [
        Self::Planned,
        Self::Copying,
        Self::Copied,
        Self::Verified,
        Self::Swapped,
        Self::Complete,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "PLANNED",
            Self::Copying => "COPYING",
            Self::Copied => "COPIED",
            Self::Verified => "VERIFIED",
            Self::Swapped => "SWAPPED",
            Self::Complete => "COMPLETE",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "PLANNED" => Some(Self::Planned),
            "COPYING" => Some(Self::Copying),
            "COPIED" => Some(Self::Copied),
            "VERIFIED" => Some(Self::Verified),
            "SWAPPED" => Some(Self::Swapped),
            "COMPLETE" => Some(Self::Complete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscodeCopyRejectionKind {
    SourceCorrupt,
    SourceSetChanged,
}

impl TranscodeCopyRejectionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceCorrupt => "SOURCE_CORRUPT",
            Self::SourceSetChanged => "SOURCE_SET_CHANGED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapLockMode {
    Parent,
    Leaves,
}

impl SwapLockMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parent => "ACCESS_EXCLUSIVE",
            Self::Leaves => "SHARE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodePlan {
    pub job_id: Uuid,
    pub component: ArchiveComponent,
    pub source_version: i16,
    pub target_version: i16,
    pub transformed_rows: i64,
    pub copied_rows: i64,
    pub payload_bytes: i64,
    pub projected_payload_bytes: i64,
    pub affected_relation_bytes: i64,
    pub relation_count: usize,
    pub peak_additional_disk_budget_bytes: i64,
    pub wal_budget_bytes: i64,
    pub rollback_wal_budget_bytes: i64,
    pub rollback_peak_additional_disk_budget_bytes: i64,
    pub reversible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodePlanRejected {
    pub component: ArchiveComponent,
    pub reason: String,
    pub affected_rows: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscodePlanOutcome {
    Planned(TranscodePlan),
    Rejected(TranscodePlanRejected),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeCopyBatch {
    pub job_id: Uuid,
    pub relation_ordinal: i32,
    pub batch_number: i32,
    pub rows_copied: i32,
    pub copied_rows_completed: i64,
    pub copied_rows_total: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeCopyRejected {
    pub job_id: Uuid,
    pub relation_ordinal: i32,
    pub kind: TranscodeCopyRejectionKind,
    pub observed_rows: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeReadyForVerification {
    pub job_id: Uuid,
    pub copied_rows_total: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscodeCopyOutcome {
    Batch(TranscodeCopyBatch),
    Rejected(TranscodeCopyRejected),
    Ready(TranscodeReadyForVerification),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeVerification {
    pub job_id: Uuid,
    pub verified: bool,
    pub source_relations_changed: i64,
    pub replacement_row_mismatches: i64,
    pub invalid_target_rows: i64,
    pub copied_rows_total: i64,
    pub wal_bytes: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeSwap {
    pub job_id: Uuid,
    pub relations_swapped: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SwapBlocker {
    pub pid: i32,
    pub state: Option<String>,
    pub transaction_age_seconds: Option<f64>,
    pub wait_event: Option<String>,
    pub query: Option<String>,
    pub relation_name: String,
    pub held_lock_mode: String,
    pub granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeSwapBusy {
    pub job_id: Uuid,
    pub lock_mode: SwapLockMode,
    pub relation_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TranscodeSwapExhausted {
    pub job_id: Uuid,
    pub lock_mode: SwapLockMode,
    pub relation_names: Vec<String>,
    pub attempts: u32,
    pub retry_sleep_seconds: f64,
    pub blockers: Vec<SwapBlocker>,
    pub blocker_capture_failed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TranscodeSwapOutcome {
    Swapped(TranscodeSwap),
    Busy(TranscodeSwapBusy),
    Exhausted(TranscodeSwapExhausted),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscodeFinalized {
    pub job_id: Uuid,
    pub retired_source_version: i16,
    pub decoder_retirement_ready: bool,
}
