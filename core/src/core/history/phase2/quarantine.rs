//! Typed client for the phase-2 quarantine of one pending row.
//!
//! Calls the baseline function `horsies_phase2_quarantine_one(p_task_id uuid)`
//! (`core/baseline/0003_interim_functions.sql`). The function
//! locks only the pending row, copies the recovery projection of the history
//! row into the quarantine table, verifies the copy and repoints the pending
//! row at it. A refusal keeps the history locator. The function stores no
//! reason: the recovery loop logs why it quarantines.

use uuid::Uuid;

use crate::core::history::errors::HistoryError;
#[cfg(test)]
use crate::core::history::phase2::consumption::Phase2CallError;
#[cfg(test)]
use crate::test_support::sql::{self, TestTransaction};

#[cfg(test)]
const QUARANTINE_ONE_SQL: &str = "SELECT verdict, detail FROM horsies_phase2_quarantine_one($1)";

/// The verdicts of `horsies_phase2_quarantine_one`.
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

#[cfg(test)]
#[derive(Debug)]
struct QuarantineVerdictRow {
    verdict: String,
    detail: Option<String>,
}

#[cfg(test)]
impl sql::FromTestRow for QuarantineVerdictRow {
    fn from_row(row: &tokio_postgres::Row) -> Result<Self, sql::Error> {
        Ok(Self {
            verdict: sql::named(row, "verdict")?,
            detail: sql::named(row, "detail")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineOutcome {
    pub verdict: QuarantineVerdict,
    pub detail: Option<String>,
}

/// Quarantines the pending row of `task_id` in the caller's transaction.
///
/// Outcomes: one `QuarantineVerdict` with the function's detail text. Errors:
/// a database error, or a verdict outside the vocabulary
/// ([`Phase2CallError`]). Effects on `Repointed`: one quarantine row, and the pending row
/// points at it. It does not commit; the caller's transaction decides.
///
/// The core's phase-2 recovery runs the workflow step functions instead; the
/// tests of the function call it here.
#[cfg(test)]
pub(crate) async fn quarantine_one(
    transaction: &mut TestTransaction<'_>,
    task_id: Uuid,
) -> Result<QuarantineOutcome, Phase2CallError> {
    let row: QuarantineVerdictRow = sql::query_as(QUARANTINE_ONE_SQL)
        .bind(task_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(Phase2CallError::Database)?;
    Ok(QuarantineOutcome {
        verdict: QuarantineVerdict::try_from(row.verdict.as_str())
            .map_err(Phase2CallError::Contract)?,
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
