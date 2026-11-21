//! Retention periods: how long a finite class keeps a record.
//!
//! A finite retention class is identified by its period in whole days. The
//! class key is derived from the period (`standard_<days>d`), so a key always
//! means one duration. History leaves are daily, so a period is whole days.

use chrono::Duration;

pub const MIN_RETENTION_DAYS: u16 = 1;
/// Five years, the bound of `terminal_record_retention_hours`. The baseline
/// CHECK `horsies_retention_classes_kind_shape` holds the same bound.
pub const MAX_RETENTION_DAYS: u16 = 1825;
/// 30 days gives `standard_30d`, the key of the default class.
pub const PERIOD_CLASS_PREFIX: &str = "standard_";

// `retention_days` is a smallint: every period fits.
const _: () = assert!(MAX_RETENTION_DAYS <= i16::MAX as u16);

/// Why a duration is not a retention period.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeriodRefused {
    #[error(
        "{period} is not a whole number of days; history partitions are daily, so a retention period is whole days"
    )]
    NotWholeDays { period: Duration },
    #[error("{period} is shorter than {MIN_RETENTION_DAYS} day")]
    TooShort { period: Duration },
    #[error("{period} is longer than {MAX_RETENTION_DAYS} days")]
    TooLong { period: Duration },
}

/// Whole days within `MIN_RETENTION_DAYS..=MAX_RETENTION_DAYS`. Only
/// `parse_period` and `from_stored` build it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RetentionDays(u16);

impl RetentionDays {
    /// The default period: 30 days.
    pub const DEFAULT: Self = Self(30);

    /// The days of a finite period, or why it is not one. Never rounds: a
    /// rounded period would keep records for a time the caller did not ask for.
    pub fn parse_period(period: Duration) -> Result<Self, PeriodRefused> {
        let whole_days = Duration::days(period.num_days());
        if whole_days != period {
            return Err(PeriodRefused::NotWholeDays { period });
        }
        match period.num_days() {
            days if days < i64::from(MIN_RETENTION_DAYS) => Err(PeriodRefused::TooShort { period }),
            days => u16::try_from(days)
                .ok()
                .filter(|days| *days <= MAX_RETENTION_DAYS)
                .map(Self)
                .ok_or(PeriodRefused::TooLong { period }),
        }
    }

    /// The days of a stored `retention_days` value, or `None` when the value
    /// is outside the bounds that the schema CHECK holds.
    pub fn from_stored(days: i16) -> Option<Self> {
        u16::try_from(days)
            .ok()
            .filter(|days| (MIN_RETENTION_DAYS..=MAX_RETENTION_DAYS).contains(days))
            .map(Self)
    }

    pub fn get(self) -> u16 {
        self.0
    }

    /// The value written to `horsies_retention_classes.retention_days`.
    pub fn as_stored(self) -> i16 {
        // Lossless: the constant assertion above bounds every period.
        self.0 as i16
    }

    pub fn as_duration(self) -> Duration {
        Duration::days(i64::from(self.0))
    }

    /// The class key of this period: `standard_<days>d`.
    pub fn class_key(self) -> String {
        format!("{PERIOD_CLASS_PREFIX}{}d", self.0)
    }
}

pub const MIN_HEARTBEAT_HORIZON_HOURS: u8 = 2;
/// The bound of `heartbeat_leaf_horizon_hours`. The baseline CHECK
/// `horsies_retention_classes_kind_shape` holds the same bounds.
pub const MAX_HEARTBEAT_HORIZON_HOURS: u8 = 48;

/// The stored horizon of the heartbeat class, in whole hours within
/// `MIN_HEARTBEAT_HORIZON_HOURS..=MAX_HEARTBEAT_HORIZON_HOURS`. One value
/// serves the create-ahead of hourly leaves and their drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeartbeatHorizonHours(u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "heartbeat horizon of {hours} hours is outside {MIN_HEARTBEAT_HORIZON_HOURS}..={MAX_HEARTBEAT_HORIZON_HOURS}"
)]
pub struct HeartbeatHorizonRefused {
    pub hours: u32,
}

impl HeartbeatHorizonHours {
    pub fn new(hours: u32) -> Result<Self, HeartbeatHorizonRefused> {
        u8::try_from(hours)
            .ok()
            .filter(|hours| {
                (MIN_HEARTBEAT_HORIZON_HOURS..=MAX_HEARTBEAT_HORIZON_HOURS).contains(hours)
            })
            .map(Self)
            .ok_or(HeartbeatHorizonRefused { hours })
    }

    /// The horizon of a stored `heartbeat_horizon_hours` value, or `None`
    /// when the value is outside the bounds that the schema CHECK holds.
    pub fn from_stored(hours: i16) -> Option<Self> {
        u32::try_from(hours)
            .ok()
            .and_then(|hours| Self::new(hours).ok())
    }

    pub fn get(self) -> u8 {
        self.0
    }

    /// The value written to `horsies_retention_classes.heartbeat_horizon_hours`.
    pub fn as_stored(self) -> i16 {
        i16::from(self.0)
    }

    pub fn as_duration(self) -> Duration {
        Duration::hours(i64::from(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_heartbeat_horizon_is_whole_hours_within_the_bounds() {
        assert_eq!(
            HeartbeatHorizonHours::new(2).map(HeartbeatHorizonHours::get),
            Ok(2)
        );
        assert_eq!(
            HeartbeatHorizonHours::new(48).map(HeartbeatHorizonHours::get),
            Ok(48)
        );
        assert_eq!(
            HeartbeatHorizonHours::new(1),
            Err(HeartbeatHorizonRefused { hours: 1 })
        );
        assert_eq!(
            HeartbeatHorizonHours::new(49),
            Err(HeartbeatHorizonRefused { hours: 49 })
        );
        assert_eq!(
            HeartbeatHorizonHours::from_stored(6).map(HeartbeatHorizonHours::as_duration),
            Some(Duration::hours(6))
        );
        assert_eq!(HeartbeatHorizonHours::from_stored(-6), None);
    }

    #[test]
    fn a_period_is_whole_days_within_the_bounds() {
        assert_eq!(
            RetentionDays::parse_period(Duration::days(7)).map(RetentionDays::get),
            Ok(7)
        );
        assert_eq!(
            RetentionDays::parse_period(Duration::days(1825)).map(RetentionDays::get),
            Ok(1825)
        );
        assert_eq!(
            RetentionDays::parse_period(Duration::hours(36)),
            Err(PeriodRefused::NotWholeDays {
                period: Duration::hours(36)
            })
        );
        assert_eq!(
            RetentionDays::parse_period(Duration::zero()),
            Err(PeriodRefused::TooShort {
                period: Duration::zero()
            })
        );
        assert_eq!(
            RetentionDays::parse_period(Duration::days(-3)),
            Err(PeriodRefused::TooShort {
                period: Duration::days(-3)
            })
        );
        assert_eq!(
            RetentionDays::parse_period(Duration::days(1826)),
            Err(PeriodRefused::TooLong {
                period: Duration::days(1826)
            })
        );
    }

    #[test]
    fn the_class_key_is_derived_from_the_period() {
        assert_eq!(RetentionDays::DEFAULT.class_key(), "standard_30d");
        let longest = RetentionDays::parse_period(Duration::days(1825)).unwrap();
        assert_eq!(longest.class_key(), "standard_1825d");
        assert_eq!(longest.as_stored(), 1825);
        assert_eq!(RetentionDays::from_stored(1825), Some(longest));
        assert_eq!(RetentionDays::from_stored(0), None);
        assert_eq!(RetentionDays::from_stored(1826), None);
    }
}
