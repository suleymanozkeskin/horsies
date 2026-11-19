//! The settle of the claimed workflow tasks of a claim pass, on the
//! statement layer: `horsies_workflow_claimed_tasks_settle`
//! (`0005_workflow_functions.sql`), one statement in autocommit per
//! [`MAX_SETTLED_CLAIMS`] claims.
//!
//! Under the lock of each workflow row it reads the workflow status again,
//! then:
//! - a task of a PAUSED workflow: this worker's claim at its claim
//!   generation is abandoned, and the node of an abandoned task goes back to
//!   READY with no task;
//! - a task of a CANCELLED workflow: the claim is cancelled, and the node of
//!   a cancelled task is skipped.
//!
//! A task of a PAUSED or CANCELLED workflow is blocked whether its move
//! applied or found the row moved on (a lost claim, a task already
//! terminal): that row belongs to its current owner. A workflow that a
//! resume made RUNNING before the lock has its tasks dispatched.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, StatementLog};
use crate::broker::error::BrokerError;
use crate::db::{CheckoutClass, CorePool, ParameterType, StatementBound, TypedStatement};

/// The most claims one settle statement takes (the bound of
/// `horsies_workflow_claimed_tasks_settle`).
pub(crate) const MAX_SETTLED_CLAIMS: usize = 1000;

/// `horsies_workflow_claimed_tasks_settle`. Parameters in order: task ids,
/// claim generations, worker id. One row per input task, in input order.
static SETTLE_CLAIMED_TASKS: TypedStatement = TypedStatement::new(
    "workflow_claimed_tasks_settle",
    "SELECT task_id, disposition FROM horsies_workflow_claimed_tasks_settle($1, $2, $3)",
    &[
        ParameterType::UuidArray,
        ParameterType::TimestamptzArray,
        ParameterType::Text,
    ],
);

/// What the settle decided for one claimed task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimDisposition {
    /// No PAUSED or CANCELLED workflow holds the task: dispatch it.
    Dispatch,
    /// The task's workflow is PAUSED or CANCELLED: never dispatch it.
    Blocked,
}

/// Settle the claims of `worker_id` on `pool`, as a lifecycle write on
/// claimed rows ([`CheckoutClass::Reserved`], as the release of claimed
/// rows). Returns one disposition per claim, in input order.
///
/// Each statement is its own transaction. A task id that repeats inside one
/// statement is refused by the function (`22023`).
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`];
/// [`BrokerError::ClaimContract`] when the rows do not answer each claim
/// once, in order, with a known disposition. The statements before a failed
/// one stay committed; after `OutcomeUnknown` the moves of that statement
/// may have applied, and the same settle sent again blocks the same tasks.
pub(crate) async fn settle_claimed_workflow_tasks(
    pool: &CorePool,
    log: StatementLog,
    worker_id: &str,
    claims: &[(Uuid, DateTime<Utc>)],
) -> Result<Vec<(Uuid, ClaimDisposition)>, BrokerError> {
    let mut settled = Vec::with_capacity(claims.len());
    for chunk in claims.chunks(MAX_SETTLED_CLAIMS) {
        settled.extend(settle_chunk(pool, log, worker_id, chunk).await?);
    }
    Ok(settled)
}

/// One settle statement for at most [`MAX_SETTLED_CLAIMS`] claims.
async fn settle_chunk(
    pool: &CorePool,
    log: StatementLog,
    worker_id: &str,
    claims: &[(Uuid, DateTime<Utc>)],
) -> Result<Vec<(Uuid, ClaimDisposition)>, BrokerError> {
    let task_ids: Vec<Uuid> = claims.iter().map(|(task_id, _)| *task_id).collect();
    let claimed_ats: Vec<DateTime<Utc>> =
        claims.iter().map(|(_, claimed_at)| *claimed_at).collect();
    let statement = &SETTLE_CLAIMED_TASKS;
    let bound = statement
        .bind()
        .value(&task_ids)
        .value(&claimed_ats)
        .value(&worker_id)
        .finish();
    log.log(statement);
    let rows = pool
        .query(
            CheckoutClass::Reserved,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?;
    let wire = rows
        .iter()
        .map(|row| -> Result<(Uuid, String), BrokerError> {
            Ok((
                column::<Uuid>(row, statement, "task_id")?,
                column::<String>(row, statement, "disposition")?,
            ))
        })
        .collect::<Result<Vec<_>, BrokerError>>()?;
    decode_settle(&task_ids, wire).map_err(BrokerError::ClaimContract)
}

/// One disposition per input task, in input order. Pure.
fn decode_settle(
    task_ids: &[Uuid],
    rows: Vec<(Uuid, String)>,
) -> Result<Vec<(Uuid, ClaimDisposition)>, String> {
    if rows.len() != task_ids.len() {
        return Err(format!(
            "the settle answered {} rows for {} claims",
            rows.len(),
            task_ids.len()
        ));
    }
    task_ids
        .iter()
        .zip(rows)
        .enumerate()
        .map(|(position, (expected, (task_id, disposition)))| {
            if task_id != *expected {
                return Err(format!(
                    "row {} names task {task_id}, the input holds {expected}",
                    position + 1
                ));
            }
            match disposition.as_str() {
                "DISPATCH" => Ok((task_id, ClaimDisposition::Dispatch)),
                "BLOCKED" => Ok((task_id, ClaimDisposition::Blocked)),
                other => Err(format!(
                    "unknown settle disposition {other:?} for task {task_id}"
                )),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{decode_settle, ClaimDisposition};
    use uuid::Uuid;

    fn ids() -> (Uuid, Uuid) {
        (Uuid::from_u128(1), Uuid::from_u128(2))
    }

    #[test]
    fn rows_in_input_order_decode_to_their_dispositions() {
        let (first, second) = ids();
        let decoded = decode_settle(
            &[first, second],
            vec![
                (first, "BLOCKED".to_owned()),
                (second, "DISPATCH".to_owned()),
            ],
        )
        .expect("rows in input order");
        assert_eq!(
            decoded,
            vec![
                (first, ClaimDisposition::Blocked),
                (second, ClaimDisposition::Dispatch),
            ]
        );
    }

    #[test]
    fn no_claim_and_no_row_decode_to_nothing() {
        assert_eq!(decode_settle(&[], Vec::new()), Ok(Vec::new()));
    }

    #[test]
    fn a_missing_row_is_refused() {
        let (first, second) = ids();
        let refused = decode_settle(&[first, second], vec![(first, "DISPATCH".to_owned())]);
        assert!(refused.is_err(), "{refused:?}");
    }

    #[test]
    fn an_extra_row_is_refused() {
        let (first, second) = ids();
        let refused = decode_settle(
            &[first],
            vec![
                (first, "DISPATCH".to_owned()),
                (second, "DISPATCH".to_owned()),
            ],
        );
        assert!(refused.is_err(), "{refused:?}");
    }

    #[test]
    fn a_row_out_of_input_order_is_refused() {
        let (first, second) = ids();
        let refused = decode_settle(
            &[first, second],
            vec![
                (second, "DISPATCH".to_owned()),
                (first, "DISPATCH".to_owned()),
            ],
        );
        assert!(refused.is_err(), "{refused:?}");
    }

    #[test]
    fn an_unknown_disposition_is_refused() {
        let (first, _) = ids();
        let refused = decode_settle(&[first], vec![(first, "PAUSED".to_owned())]);
        assert!(refused.is_err(), "{refused:?}");
    }
}
