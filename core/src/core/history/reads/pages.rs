//! The terminal-time window of the history reads.

use chrono::{DateTime, Duration, Utc};

use crate::core::history::errors::HistoryError;

/// The widest terminal-time window a history read takes.
pub const HISTORY_WINDOW_MAX: Duration = Duration::days(30);

/// A terminal-time window of history reads: increasing bounds at most
/// `HISTORY_WINDOW_MAX` apart. The type carries the bound, so no read checks
/// it again. A read takes the history rows whose `terminal_at` is in
/// `[lower, upper)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryWindow {
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
}

impl HistoryWindow {
    pub fn new(lower: DateTime<Utc>, upper: DateTime<Utc>) -> Result<Self, HistoryError> {
        if lower >= upper {
            return Err(HistoryError::contract(
                "history window bounds must be increasing",
            ));
        }
        if upper - lower > HISTORY_WINDOW_MAX {
            return Err(HistoryError::contract(format!(
                "history window of {} exceeds the {}-day maximum",
                upper - lower,
                HISTORY_WINDOW_MAX.num_days()
            )));
        }
        Ok(Self { lower, upper })
    }

    pub fn lower(self) -> DateTime<Utc> {
        self.lower
    }

    pub fn upper(self) -> DateTime<Utc> {
        self.upper
    }
}
