//! Monitoring-owned history-window bounds.

use chrono::{DateTime, Duration, Utc};

use crate::core::history::reads::pages::{MonitoringWindow, HISTORY_WINDOW_MAX};

pub const MONITORING_WINDOW_DEFAULT: Duration = Duration::hours(24);
pub const MONITORING_WINDOW_MAX: Duration = HISTORY_WINDOW_MAX;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{reason}")]
pub struct WindowRefused {
    pub reason: String,
}

pub fn resolve_monitoring_window(
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    now: Option<DateTime<Utc>>,
) -> Result<MonitoringWindow, WindowRefused> {
    let anchor = now.unwrap_or_else(Utc::now);
    let upper = until.unwrap_or(anchor);
    let lower = since.unwrap_or(upper - MONITORING_WINDOW_DEFAULT);
    if lower >= upper {
        return Err(WindowRefused {
            reason: "the window must be increasing (since < until)".to_owned(),
        });
    }
    if upper - lower > MONITORING_WINDOW_MAX {
        return Err(WindowRefused {
            reason: "the window exceeds the 30-day maximum".to_owned(),
        });
    }
    MonitoringWindow::new(lower, upper).map_err(|error| WindowRefused {
        reason: error.to_string(),
    })
}
