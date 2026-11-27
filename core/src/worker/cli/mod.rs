pub mod banner;

use std::fmt;
use std::str::FromStr;

/// Log level accepted by the `--log-level` flag.
///
/// Mirrors the Python CLI choices (DEBUG, INFO, WARNING, ERROR)
/// mapped to the corresponding `tracing` levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warning => write!(f, "warn"),
            LogLevel::Error => write!(f, "error"),
        }
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warning" | "warn" => Ok(LogLevel::Warning),
            "error" => Ok(LogLevel::Error),
            "critical" => Ok(LogLevel::Error), // map CRITICAL -> error (tracing has no CRITICAL)
            other => Err(format!(
                "invalid log level '{}': expected one of debug, info, warning, error",
                other,
            )),
        }
    }
}
