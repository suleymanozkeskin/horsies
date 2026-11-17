//! The bounds of the writes of a run in a worker's child, for the core
//! child host and for the child handle of the Python client (Python
//! `python/horsies/core/worker/shared_lifecycle.py`, `thread_runner.py`,
//! `child_runner.py`):
//!
//! | Write | Bound | Python |
//! |---|---|---|
//! | one start attempt: the start, its miss path, the release after a failed start | [`START_ATTEMPT_BOUND`] | `RUNNING_ATTEMPT_TIMEOUT_S` |
//! | the finalizing marker | [`FINALIZING_MARKER_BOUND`] | `FINALIZING_MARKER_TIMEOUT_S` |
//!
//! Within the bound of its write, each checkout waits at most
//! [`CHILD_WRITE_CHECKOUT_WAIT`] and each statement runs at most
//! [`CHILD_STATEMENT_BOUND`]; neither runs past the bound
//! ([`WriteDeadline`]). A statement past its time gets a cancel request
//! ([`StatementBound::CancelAfter`]): the server ends the statement, so it
//! does not commit after the bound, or it committed before the request
//! reached the server. Such a statement ends within
//! [`CANCELLED_WRITE_END`]. When the cancel request fails and no reply
//! comes within the cancel grace, the connection is discarded and the
//! outcome is unknown (`OutcomeUnknown`), as after a lost reply. A write
//! whose bound passed before its statement sends nothing.

use std::time::{Duration, Instant};

use crate::broker::error::BrokerError;
use crate::db::{
    BoundStatement, CheckoutClass, CheckoutFailure, CorePool, NotSentCause, PoolRole,
    PooledConnection, Row, StatementBound, StatementFailure, CANCEL_GRACE, CANCEL_REQUEST_BOUND,
};

/// The longest wait of one checkout of a child write for a pooled
/// connection (Python `CHILD_POOL_CONNECTION_WAIT_S`,
/// `SHARED_POOL_CONNECTION_WAIT_S`).
pub const CHILD_WRITE_CHECKOUT_WAIT: Duration = Duration::from_secs(30);

/// The longest run of one statement of a child write before its cancel
/// request (Python `RUNNING_STATEMENTS_TIMEOUT_S`,
/// `REFUSAL_STATEMENTS_TIMEOUT_S`).
pub const CHILD_STATEMENT_BOUND: Duration = Duration::from_secs(10);

/// The bound of one start attempt: the wait for a connection and the
/// statements (Python `RUNNING_ATTEMPT_TIMEOUT_S`). The start, its miss
/// path and the release of the claim after a failed start share it.
pub const START_ATTEMPT_BOUND: Duration =
    CHILD_WRITE_CHECKOUT_WAIT.saturating_add(CHILD_STATEMENT_BOUND);

/// The bound of the finalizing marker: the wait for a connection and the
/// statement (Python `FINALIZING_MARKER_TIMEOUT_S`).
pub const FINALIZING_MARKER_BOUND: Duration = Duration::from_secs(10);

/// The longest end of a statement after its time: the cancel request and
/// the wait for the reply (Python `CANCELLED_WRITE_END_S`).
pub const CANCELLED_WRITE_END: Duration = CANCEL_REQUEST_BOUND.saturating_add(CANCEL_GRACE);

/// The longest time of one start attempt, with the end of a statement
/// cancelled at the bound.
pub const START_ATTEMPT_END: Duration = START_ATTEMPT_BOUND.saturating_add(CANCELLED_WRITE_END);

/// The longest time of the finalizing marker, with the end of a statement
/// cancelled at the bound.
pub const FINALIZING_MARKER_END: Duration =
    FINALIZING_MARKER_BOUND.saturating_add(CANCELLED_WRITE_END);

/// Start attempts of one run while its workflow was resumed between a
/// missed start and its stop (Python `_PRE_START_CONFIRM_ATTEMPTS`).
pub const PRE_START_CONFIRM_ATTEMPTS: u32 = 2;

/// The longest time of the writes of a refusal in a child: each start
/// attempt and the finalizing marker, each with the end of a statement
/// cancelled at its bound (Python `REFUSAL_WRITES_BOUND_S`). The refusal
/// backstop of the parent comes after it.
pub const CHILD_REFUSAL_WRITES_BOUND: Duration = START_ATTEMPT_END
    .saturating_mul(PRE_START_CONFIRM_ATTEMPTS)
    .saturating_add(FINALIZING_MARKER_END);

/// The end of the time of one child write: its checkouts and its
/// statements end by it. Each constructor takes a bound of at most
/// [`START_ATTEMPT_BOUND`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteDeadline {
    at: Instant,
    bound: Duration,
}

impl WriteDeadline {
    /// The deadline of a start attempt that begins now:
    /// [`START_ATTEMPT_BOUND`] from now. Reads the monotonic clock.
    pub fn start_attempt() -> WriteDeadline {
        WriteDeadline::after(START_ATTEMPT_BOUND)
    }

    /// The deadline of the rest of a start attempt that began `elapsed`
    /// ago, as its caller measured it: the miss path of a start through a
    /// child handle. An attempt older than [`START_ATTEMPT_BOUND`] has no
    /// time left. Reads the monotonic clock.
    pub fn rest_of_start_attempt(elapsed: Duration) -> WriteDeadline {
        WriteDeadline::after(START_ATTEMPT_BOUND.saturating_sub(elapsed))
    }

    /// The deadline of a finalizing marker that begins now:
    /// [`FINALIZING_MARKER_BOUND`] from now. Reads the monotonic clock.
    pub fn finalizing_marker() -> WriteDeadline {
        WriteDeadline::after(FINALIZING_MARKER_BOUND)
    }

    /// `bound` from now. Every caller passes at most
    /// [`START_ATTEMPT_BOUND`], which no monotonic clock overflows on.
    fn after(bound: Duration) -> WriteDeadline {
        WriteDeadline {
            at: Instant::now() + bound,
            bound,
        }
    }

    /// The time left, or `None` once the deadline passed. Reads the
    /// monotonic clock.
    pub fn left(self) -> Option<Duration> {
        self.at
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
    }

    /// The failure of `statement` when the deadline passed before it was
    /// sent: a checkout that got no usable connection within the bound of
    /// the write. Nothing was sent; the failure is transient.
    fn passed(self, role: PoolRole, statement: &'static str) -> BrokerError {
        BrokerError::Statement(StatementFailure::NotSent {
            statement,
            cause: NotSentCause::Checkout(CheckoutFailure::TimedOut {
                role,
                waited: self.bound,
            }),
        })
    }
}

/// A connection of `pool` for `statement` of a child write, checked out as
/// reserved work, and the bound of that statement. The checkout waits at
/// most [`CHILD_WRITE_CHECKOUT_WAIT`] and the time left of `deadline`; the
/// statement gets its cancel request after [`CHILD_STATEMENT_BOUND`] or at
/// `deadline`, whichever comes first.
///
/// Errors: [`BrokerError::Statement`] with nothing sent: the checkout
/// failed, or `deadline` passed before the statement (a checkout
/// `TimedOut`, transient).
pub(crate) async fn connection_for(
    pool: &CorePool,
    statement: &'static str,
    deadline: WriteDeadline,
) -> Result<(PooledConnection, StatementBound), BrokerError> {
    let Some(left) = deadline.left() else {
        return Err(deadline.passed(pool.role(), statement));
    };
    let connection = pool
        .checkout_within(CheckoutClass::Reserved, left.min(CHILD_WRITE_CHECKOUT_WAIT))
        .await
        .map_err(|cause| {
            BrokerError::Statement(StatementFailure::NotSent {
                statement,
                cause: NotSentCause::Checkout(cause),
            })
        })?;
    match deadline.left() {
        Some(left) => Ok((
            connection,
            StatementBound::CancelAfter(left.min(CHILD_STATEMENT_BOUND)),
        )),
        None => Err(deadline.passed(pool.role(), statement)),
    }
}

/// Run `statement` as a child write on `pool` within `deadline`
/// ([`connection_for`]) and read every row.
///
/// Errors: [`BrokerError::Statement`]: nothing sent (see
/// [`connection_for`]); refused, a cancel after the bound included
/// (nothing committed); or `OutcomeUnknown` (the statement may have
/// committed).
pub(crate) async fn query_within(
    pool: &CorePool,
    statement: &BoundStatement<'_>,
    deadline: WriteDeadline,
) -> Result<Vec<Row>, BrokerError> {
    let (mut connection, bound) =
        connection_for(pool, statement.statement().name(), deadline).await?;
    Ok(connection.query(statement, bound).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bounds_keep_their_times() {
        assert_eq!(START_ATTEMPT_BOUND, Duration::from_secs(40));
        assert_eq!(CANCELLED_WRITE_END, Duration::from_secs(10));
        assert_eq!(START_ATTEMPT_END, Duration::from_secs(50));
        assert_eq!(FINALIZING_MARKER_END, Duration::from_secs(20));
        assert_eq!(CHILD_REFUSAL_WRITES_BOUND, Duration::from_secs(120));
    }

    #[test]
    fn a_new_deadline_has_time_left_up_to_its_bound() {
        for (deadline, bound) in [
            (WriteDeadline::start_attempt(), START_ATTEMPT_BOUND),
            (WriteDeadline::finalizing_marker(), FINALIZING_MARKER_BOUND),
        ] {
            let left = deadline.left().expect("a new deadline has time left");
            assert!(left <= bound, "{left:?} > {bound:?}");
        }
    }

    #[test]
    fn the_rest_of_an_attempt_is_its_bound_less_what_elapsed() {
        let left = WriteDeadline::rest_of_start_attempt(Duration::from_secs(25))
            .left()
            .expect("15 s are left");
        assert!(left <= Duration::from_secs(15), "{left:?}");
        assert!(left > Duration::from_secs(14), "{left:?}");
    }

    #[test]
    fn an_attempt_past_its_bound_has_no_time_left() {
        assert_eq!(
            WriteDeadline::rest_of_start_attempt(START_ATTEMPT_BOUND).left(),
            None
        );
        assert_eq!(
            WriteDeadline::rest_of_start_attempt(START_ATTEMPT_BOUND * 2).left(),
            None
        );
    }

    #[test]
    fn a_passed_deadline_sends_nothing_and_may_clear() {
        let error = WriteDeadline::rest_of_start_attempt(START_ATTEMPT_BOUND)
            .passed(PoolRole::Main, "start_attempt");
        assert!(matches!(
            &error,
            BrokerError::Statement(StatementFailure::NotSent {
                statement: "start_attempt",
                cause: NotSentCause::Checkout(CheckoutFailure::TimedOut {
                    role: PoolRole::Main,
                    ..
                }),
            })
        ));
        assert!(error.is_retryable());
    }
}
