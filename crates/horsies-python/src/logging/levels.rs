//! Record levels and the per-logger level snapshot.
//!
//! Python `logging` is the only level configuration. The forwarder copies
//! each logger's `getEffectiveLevel()` into the snapshot, and the subscriber
//! reads the snapshot without the GIL.

use std::sync::atomic::{AtomicU8, Ordering};

use super::names::LoggerName;

/// The threshold before the first read of Python's levels: Python's default
/// root level, `WARNING`.
pub const DEFAULT_THRESHOLD: u8 = 30;

/// The level of one core record.
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

    /// The Python `logging` level number. Python has no TRACE; 5 sits below
    /// `DEBUG`.
    pub const fn python_level(self) -> u8 {
        match self {
            Self::Trace => 5,
            Self::Debug => 10,
            Self::Info => 20,
            Self::Warning => 30,
            Self::Error => 40,
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

/// The effective level of each logger, as Python last reported it.
pub struct LevelSnapshot {
    thresholds: [AtomicU8; LoggerName::COUNT],
}

impl LevelSnapshot {
    pub const fn new() -> Self {
        Self {
            thresholds: [const { AtomicU8::new(DEFAULT_THRESHOLD) }; LoggerName::COUNT],
        }
    }

    /// Whether Python would handle a record of `level` on `logger`.
    pub fn enabled(&self, logger: LoggerName, level: RecordLevel) -> bool {
        level.python_level() >= self.thresholds[logger.index()].load(Ordering::Relaxed)
    }

    /// Keep the effective level that Python reported for `logger`.
    pub fn store(&self, logger: LoggerName, python_level: i64) {
        self.thresholds[logger.index()].store(threshold_of(python_level), Ordering::Relaxed);
    }
}

/// A Python level number as a threshold: below 0 is 0, above 255 is 255.
fn threshold_of(python_level: i64) -> u8 {
    match u8::try_from(python_level.clamp(0, i64::from(u8::MAX))) {
        Ok(threshold) => threshold,
        Err(_unreachable_after_clamp) => u8::MAX,
    }
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
    fn a_stored_level_applies_to_its_logger_only() {
        let levels = LevelSnapshot::new();
        levels.store(LoggerName::Worker, 10);
        assert!(levels.enabled(LoggerName::Worker, RecordLevel::Debug));
        assert!(!levels.enabled(LoggerName::Worker, RecordLevel::Trace));
        assert!(!levels.enabled(LoggerName::Broker, RecordLevel::Debug));
    }

    #[test]
    fn notset_forwards_every_level_and_a_high_level_forwards_none() {
        let levels = LevelSnapshot::new();
        levels.store(LoggerName::App, 0);
        assert!(levels.enabled(LoggerName::App, RecordLevel::Trace));
        levels.store(LoggerName::App, 1_000);
        assert!(!levels.enabled(LoggerName::App, RecordLevel::Error));
        levels.store(LoggerName::App, -5);
        assert!(levels.enabled(LoggerName::App, RecordLevel::Trace));
    }

    #[test]
    fn only_warning_and_error_wait_in_a_full_channel() {
        assert!(!RecordLevel::Debug.waits_when_full());
        assert!(!RecordLevel::Info.waits_when_full());
        assert!(RecordLevel::Warning.waits_when_full());
        assert!(RecordLevel::Error.waits_when_full());
    }

    #[test]
    fn tracing_levels_map_to_python_numbers() {
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::ERROR).python_level(),
            40
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::WARN).python_level(),
            30
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::INFO).python_level(),
            20
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::DEBUG).python_level(),
            10
        );
        assert_eq!(
            RecordLevel::from_tracing(tracing::Level::TRACE).python_level(),
            5
        );
    }
}
