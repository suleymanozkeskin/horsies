//! Record levels, thresholds, and the per-logger level snapshot.
//!
//! The package sets one threshold per logger and a default
//! (`setLogLevels`). Before that, every logger has the threshold WARNING, as
//! Python's default root level. The subscriber reads the snapshot on any
//! thread; a change applies to the records of later events.

use std::sync::atomic::{AtomicU8, Ordering};

use napi_derive::napi;

use super::names::LoggerName;
use crate::call::guard::sync_call;
use crate::call::thrown::Envelope;

/// The level of one core record.
#[napi(string_enum = "UPPERCASE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordLevel {
    Trace,
    Debug,
    Info,
    Warning,
    Error,
}

impl RecordLevel {
    /// The level of a `tracing` event.
    pub fn from_tracing(level: tracing::Level) -> Self {
        match level {
            tracing::Level::ERROR => Self::Error,
            tracing::Level::WARN => Self::Warning,
            tracing::Level::INFO => Self::Info,
            tracing::Level::DEBUG => Self::Debug,
            // `tracing` has five levels; the last one is TRACE.
            _ => Self::Trace,
        }
    }

    /// The level number, as Python `logging` numbers it. Python has no
    /// TRACE; 5 sits below DEBUG.
    pub const fn number(self) -> u8 {
        match self {
            Self::Trace => 5,
            Self::Debug => 10,
            Self::Info => 20,
            Self::Warning => 30,
            Self::Error => 40,
        }
    }

    /// The level as it appears in a stderr line.
    pub const fn text(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
        }
    }

    /// Whether a record of this level waits for space in a full channel
    /// instead of being dropped.
    pub const fn waits_when_full(self) -> bool {
        match self {
            Self::Trace | Self::Debug | Self::Info => false,
            Self::Warning | Self::Error => true,
        }
    }
}

/// The lowest level that a logger forwards. `CRITICAL` forwards no core
/// record: the core writes none above ERROR.
#[napi(string_enum = "UPPERCASE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogThreshold {
    Trace,
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

impl LogThreshold {
    /// The threshold number, as Python `logging` numbers the level.
    pub const fn number(self) -> u8 {
        match self {
            Self::Trace => 5,
            Self::Debug => 10,
            Self::Info => 20,
            Self::Warning => 30,
            Self::Error => 40,
            Self::Critical => 50,
        }
    }
}

/// The threshold of every logger before `setLogLevels`: WARNING.
pub const DEFAULT_THRESHOLD: LogThreshold = LogThreshold::Warning;

/// The threshold of each logger.
pub struct LevelSnapshot {
    thresholds: [AtomicU8; LoggerName::COUNT],
}

impl LevelSnapshot {
    pub const fn new() -> Self {
        Self {
            thresholds: [const { AtomicU8::new(DEFAULT_THRESHOLD.number()) }; LoggerName::COUNT],
        }
    }

    /// Whether a record of `level` on `logger` passes the threshold.
    pub fn enabled(&self, logger: LoggerName, level: RecordLevel) -> bool {
        level.number() >= self.thresholds[logger.index()].load(Ordering::Relaxed)
    }

    /// Set the threshold of `logger`.
    pub fn store(&self, logger: LoggerName, threshold: LogThreshold) {
        self.thresholds[logger.index()].store(threshold.number(), Ordering::Relaxed);
    }

    /// Set every logger to `default`, then each named logger to its own
    /// threshold.
    pub fn store_all(&self, default: LogThreshold, named: &[(LoggerName, LogThreshold)]) {
        for logger in LoggerName::ALL {
            self.store(logger, default);
        }
        for (logger, threshold) in named {
            self.store(*logger, *threshold);
        }
    }
}

/// The thresholds of this process.
pub static LEVELS: LevelSnapshot = LevelSnapshot::new();

/// The thresholds that the package sets: a default for every logger, and
/// one threshold per named logger.
#[napi(object, object_to_js = false)]
pub struct LogLevelsArg {
    pub default_level: LogThreshold,
    pub loggers: Vec<LoggerLevelArg>,
}

/// The threshold of one logger.
#[napi(object, object_to_js = false)]
pub struct LoggerLevelArg {
    pub logger: LoggerName,
    pub level: LogThreshold,
}

/// Set the threshold of every logger: the default, then each named logger.
/// Later events use them. Starts nothing.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
pub fn set_log_levels(levels: LogLevelsArg) -> Envelope<()> {
    sync_call("set_log_levels", || {
        let named: Vec<(LoggerName, LogThreshold)> = levels
            .loggers
            .iter()
            .map(|named| (named.logger, named.level))
            .collect();
        LEVELS.store_all(levels.default_level, &named);
        Envelope::Value(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_forwards_warning_and_above() {
        let levels = LevelSnapshot::new();
        assert!(!levels.enabled(LoggerName::Broker, RecordLevel::Info));
        assert!(levels.enabled(LoggerName::Broker, RecordLevel::Warning));
        assert!(levels.enabled(LoggerName::Broker, RecordLevel::Error));
    }

    #[test]
    fn a_stored_threshold_applies_to_its_logger_only() {
        let levels = LevelSnapshot::new();
        levels.store(LoggerName::Worker, LogThreshold::Debug);
        assert!(levels.enabled(LoggerName::Worker, RecordLevel::Debug));
        assert!(!levels.enabled(LoggerName::Worker, RecordLevel::Trace));
        assert!(!levels.enabled(LoggerName::Broker, RecordLevel::Debug));
    }

    #[test]
    fn trace_forwards_every_level_and_critical_forwards_none() {
        let levels = LevelSnapshot::new();
        levels.store(LoggerName::App, LogThreshold::Trace);
        assert!(levels.enabled(LoggerName::App, RecordLevel::Trace));
        levels.store(LoggerName::App, LogThreshold::Critical);
        assert!(!levels.enabled(LoggerName::App, RecordLevel::Error));
    }

    #[test]
    fn a_named_threshold_wins_over_the_default() {
        let levels = LevelSnapshot::new();
        levels.store_all(
            LogThreshold::Error,
            &[(LoggerName::Worker, LogThreshold::Info)],
        );
        assert!(levels.enabled(LoggerName::Worker, RecordLevel::Info));
        assert!(!levels.enabled(LoggerName::Broker, RecordLevel::Warning));
        assert!(levels.enabled(LoggerName::Broker, RecordLevel::Error));
    }

    #[test]
    fn only_warning_and_error_wait_in_a_full_channel() {
        assert!(!RecordLevel::Trace.waits_when_full());
        assert!(!RecordLevel::Debug.waits_when_full());
        assert!(!RecordLevel::Info.waits_when_full());
        assert!(RecordLevel::Warning.waits_when_full());
        assert!(RecordLevel::Error.waits_when_full());
    }

    #[test]
    fn tracing_levels_map_to_record_levels() {
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::ERROR),
            RecordLevel::Error
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::WARN),
            RecordLevel::Warning
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::INFO),
            RecordLevel::Info
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::DEBUG),
            RecordLevel::Debug
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::TRACE),
            RecordLevel::Trace
        );
    }
}
