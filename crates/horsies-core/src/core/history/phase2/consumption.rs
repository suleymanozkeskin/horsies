//! One transaction from pending workflow evidence to a durable disposition.

use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::core::history::errors::HistoryError;

const CONSUME_SQL: &str = "SELECT * FROM horsies_phase2_consume($1, $2)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase2DispositionKind {
    AppliedToNode,
    AlreadyApplied,
    SupersededByWorkflowTerminal,
    SourceStateConflict,
    PendingAbsent,
    SourceAbsent,
    SourceVersionConflict,
    SourceDigestMismatch,
}

impl Phase2DispositionKind {
    pub const DURABLE: [Self; 3] = [
        Self::AppliedToNode,
        Self::AlreadyApplied,
        Self::SupersededByWorkflowTerminal,
    ];

    pub fn is_durable(self) -> bool {
        Self::DURABLE.contains(&self)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AppliedToNode => "APPLIED_TO_NODE",
            Self::AlreadyApplied => "ALREADY_APPLIED",
            Self::SupersededByWorkflowTerminal => "SUPERSEDED_BY_WORKFLOW_TERMINAL",
            Self::SourceStateConflict => "SOURCE_STATE_CONFLICT",
            Self::PendingAbsent => "PENDING_ABSENT",
            Self::SourceAbsent => "SOURCE_ABSENT",
            Self::SourceVersionConflict => "SOURCE_VERSION_CONFLICT",
            Self::SourceDigestMismatch => "SOURCE_DIGEST_MISMATCH",
        }
    }
}

impl TryFrom<&str> for Phase2DispositionKind {
    type Error = HistoryError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "APPLIED_TO_NODE" => Ok(Self::AppliedToNode),
            "ALREADY_APPLIED" => Ok(Self::AlreadyApplied),
            "SUPERSEDED_BY_WORKFLOW_TERMINAL" => Ok(Self::SupersededByWorkflowTerminal),
            "SOURCE_STATE_CONFLICT" => Ok(Self::SourceStateConflict),
            "PENDING_ABSENT" => Ok(Self::PendingAbsent),
            "SOURCE_ABSENT" => Ok(Self::SourceAbsent),
            "SOURCE_VERSION_CONFLICT" => Ok(Self::SourceVersionConflict),
            "SOURCE_DIGEST_MISMATCH" => Ok(Self::SourceDigestMismatch),
            unknown => Err(HistoryError::contract(format!(
                "unknown phase-2 disposition {unknown:?}"
            ))),
        }
    }
}

#[derive(Debug, FromRow)]
struct Phase2DispositionRow {
    disposition: String,
    workflow_id: Option<Uuid>,
    node_row_id: Option<Uuid>,
    task_index: Option<i32>,
    workflow_status: Option<String>,
    workflow_depth: Option<i32>,
    root_workflow_id: Option<Uuid>,
    on_error: Option<String>,
    node_status: Option<String>,
    terminal_status: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Phase2Disposition {
    pub disposition: Phase2DispositionKind,
    pub workflow_id: Option<Uuid>,
    pub node_row_id: Option<Uuid>,
    pub task_index: Option<i32>,
    pub workflow_status: Option<String>,
    pub workflow_depth: Option<i32>,
    pub root_workflow_id: Option<Uuid>,
    pub on_error: Option<String>,
    pub node_status: Option<String>,
    pub terminal_status: Option<String>,
    pub detail: Option<String>,
}

pub async fn consume_phase2(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    terminal_node_status: &str,
) -> Result<Phase2Disposition, HistoryError> {
    let row: Phase2DispositionRow = sqlx::query_as(CONSUME_SQL)
        .bind(task_id)
        .bind(terminal_node_status)
        .fetch_one(transaction.as_mut())
        .await?;
    Ok(Phase2Disposition {
        disposition: Phase2DispositionKind::try_from(row.disposition.as_str())?,
        workflow_id: row.workflow_id,
        node_row_id: row.node_row_id,
        task_index: row.task_index,
        workflow_status: row.workflow_status,
        workflow_depth: row.workflow_depth,
        root_workflow_id: row.root_workflow_id,
        on_error: row.on_error,
        node_status: row.node_status,
        terminal_status: row.terminal_status,
        detail: row.detail,
    })
}
