//! Typed client for bounded phase-2 evidence quarantine.

use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::core::history::errors::HistoryError;

const QUARANTINE_ONE_SQL: &str = "SELECT * FROM horsies_phase2_quarantine_one($1, $2)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineVerdict {
    Repointed,
    PendingGone,
    AlreadyQuarantined,
    NodeRowAbsent,
    NodeIdentityAbsent,
    SourceAbsent,
    CopyVerificationFailed,
}

impl QuarantineVerdict {
    pub fn is_drained(self) -> bool {
        matches!(self, Self::PendingGone | Self::AlreadyQuarantined)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Repointed => "REPOINTED",
            Self::PendingGone => "PENDING_GONE",
            Self::AlreadyQuarantined => "ALREADY_QUARANTINED",
            Self::NodeRowAbsent => "NODE_ROW_ABSENT",
            Self::NodeIdentityAbsent => "NODE_IDENTITY_ABSENT",
            Self::SourceAbsent => "SOURCE_ABSENT",
            Self::CopyVerificationFailed => "COPY_VERIFICATION_FAILED",
        }
    }
}

impl TryFrom<&str> for QuarantineVerdict {
    type Error = HistoryError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "REPOINTED" => Ok(Self::Repointed),
            "PENDING_GONE" => Ok(Self::PendingGone),
            "ALREADY_QUARANTINED" => Ok(Self::AlreadyQuarantined),
            "NODE_ROW_ABSENT" => Ok(Self::NodeRowAbsent),
            "NODE_IDENTITY_ABSENT" => Ok(Self::NodeIdentityAbsent),
            "SOURCE_ABSENT" => Ok(Self::SourceAbsent),
            "COPY_VERIFICATION_FAILED" => Ok(Self::CopyVerificationFailed),
            unknown => Err(HistoryError::contract(format!(
                "unknown phase-2 quarantine verdict {unknown:?}"
            ))),
        }
    }
}

#[derive(Debug, FromRow)]
struct QuarantineVerdictRow {
    verdict: String,
    detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineOutcome {
    pub verdict: QuarantineVerdict,
    pub detail: Option<String>,
}

pub async fn quarantine_one(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    reason: &str,
) -> Result<QuarantineOutcome, HistoryError> {
    let row: QuarantineVerdictRow = sqlx::query_as(QUARANTINE_ONE_SQL)
        .bind(task_id)
        .bind(reason)
        .fetch_one(transaction.as_mut())
        .await?;
    Ok(QuarantineOutcome {
        verdict: QuarantineVerdict::try_from(row.verdict.as_str())?,
        detail: row.detail,
    })
}
