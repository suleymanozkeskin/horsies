//! Typed client for the phase-2 quarantine of one pending row.
//!
//! Calls the baseline function `horsies_phase2_quarantine_one(p_task_id uuid)`
//! (`horsies/core/baseline/sql/0003_interim_functions.sql`). The function
//! locks only the pending row, copies the recovery projection of the history
//! row into the quarantine table, verifies the copy and repoints the pending
//! row at it. A refusal keeps the history locator. The function stores no
//! reason: the recovery loop logs why it quarantines.

use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

use crate::core::history::errors::HistoryError;

const QUARANTINE_ONE_SQL: &str = "SELECT verdict, detail FROM horsies_phase2_quarantine_one($1)";

/// The verdicts of `horsies_phase2_quarantine_one`, as Python's vocabulary
/// (`horsies/core/history/phase2/quarantine.py:174-185`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineVerdict {
    Repointed,
    PendingGone,
    AlreadyQuarantined,
    NodeRowAbsent,
    SourceAbsent,
    CopyVerificationFailed,
}

impl QuarantineVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Repointed => "REPOINTED",
            Self::PendingGone => "PENDING_GONE",
            Self::AlreadyQuarantined => "ALREADY_QUARANTINED",
            Self::NodeRowAbsent => "NODE_ROW_ABSENT",
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

/// Quarantines the pending row of `task_id` in the caller's transaction.
///
/// Outcomes: one `QuarantineVerdict` with the function's detail text. Errors:
/// a database error, or a verdict outside the vocabulary (`HistoryError`
/// contract). Effects on `Repointed`: one quarantine row, and the pending row
/// points at it. It does not commit; the caller's transaction decides.
pub async fn quarantine_one(
    transaction: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
) -> Result<QuarantineOutcome, HistoryError> {
    let row: QuarantineVerdictRow = sqlx::query_as(QUARANTINE_ONE_SQL)
        .bind(task_id)
        .fetch_one(transaction.as_mut())
        .await?;
    Ok(QuarantineOutcome {
        verdict: QuarantineVerdict::try_from(row.verdict.as_str())?,
        detail: row.detail,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The verdict vocabulary is the one of the baseline function: six
    /// verdicts. `NODE_IDENTITY_ABSENT` came from an earlier function body;
    /// the baseline quarantine table has no `node_id` column, and the baseline
    /// function does not read it or return that verdict.
    #[test]
    fn the_verdict_vocabulary_is_the_baseline_function_s() {
        for verdict in [
            QuarantineVerdict::Repointed,
            QuarantineVerdict::PendingGone,
            QuarantineVerdict::AlreadyQuarantined,
            QuarantineVerdict::NodeRowAbsent,
            QuarantineVerdict::SourceAbsent,
            QuarantineVerdict::CopyVerificationFailed,
        ] {
            assert_eq!(
                QuarantineVerdict::try_from(verdict.as_str()).expect("known verdict"),
                verdict
            );
        }
        assert!(QuarantineVerdict::try_from("NODE_IDENTITY_ABSENT").is_err());
    }
}
