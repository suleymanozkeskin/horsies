//! The options of one send: its delay, its deadline, its idempotency key
//! and the retention of its history record.

use chrono::{DateTime, Utc};
use horsies_core::manifest::WholeSeconds;
use horsies_core::{EnqueueRetention, RetentionConfig, RetentionDays};

/// When a send asks the task to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SendStart {
    /// When a worker claims it (the default).
    #[default]
    Now,
    /// Not before the delay after the send.
    AfterDelay(WholeSeconds),
}

/// How long the history record of a sent task is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendRetention {
    /// The period that `RetentionConfig.queue_retention` maps the task's
    /// queue to, else the 30-day default.
    #[default]
    FromQueueOrDefault,
    /// The record is kept forever.
    Forever,
    /// A period of whole days: the 30-day default, a period of
    /// `queue_retention`, or one of `retention_periods`. Build it with
    /// `RetentionDays::parse_period(chrono::Duration::days(n))`.
    Period(RetentionDays),
}

/// Options of one send (Python's `send` keyword arguments).
///
/// ```ignore
/// let options = TaskSendOptions::new()
///     .delay(WholeSeconds::from_secs(60))
///     .good_until(deadline)
///     .idempotency_key(format!("invoice-{invoice_id}"))
///     .retention(SendRetention::Forever);
/// send_invoice::with_options(options).send(&app, invoice).await?;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskSendOptions {
    start: SendStart,
    good_until: Option<DateTime<Utc>>,
    idempotency_key: Option<String>,
    retention: SendRetention,
}

impl TaskSendOptions {
    /// No delay, no deadline, no key, the retention of the queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// The task starts no earlier than `delay` after the send. The stored
    /// row is the row of a send with `enqueue_delay_seconds`.
    pub fn delay(self, delay: WholeSeconds) -> Self {
        Self {
            start: SendStart::AfterDelay(delay),
            ..self
        }
    }

    /// The task expires when no worker started it by `deadline`.
    pub fn good_until(self, deadline: DateTime<Utc>) -> Self {
        Self {
            good_until: Some(deadline),
            ..self
        }
    }

    /// Store the task once per `key` within the app's reservation window:
    /// a second send of the same command answers with the task that holds
    /// the key.
    pub fn idempotency_key(self, key: impl Into<String>) -> Self {
        Self {
            idempotency_key: Some(key.into()),
            ..self
        }
    }

    /// The retention of the task's history record.
    pub fn retention(self, retention: SendRetention) -> Self {
        Self { retention, ..self }
    }

    /// Keep the task's history record forever: `retention(SendRetention::Forever)`.
    pub fn retain_forever(self) -> Self {
        self.retention(SendRetention::Forever)
    }

    pub(crate) fn start(&self) -> SendStart {
        self.start
    }

    pub(crate) fn deadline(&self) -> Option<DateTime<Utc>> {
        self.good_until
    }

    pub(crate) fn key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    pub(crate) fn chosen_retention(&self) -> SendRetention {
        self.retention
    }
}

/// A period that this process does not register: a task sent with it
/// would finish into a history class that has no storage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "retention of {days} days is not a period this deployment registers; omit it for the queue \
     mapping or the 30-day default, choose SendRetention::Forever, or one of: {allowed:?} days. \
     Add the period to AppConfig.retention.retention_periods to send with it"
)]
pub(crate) struct PeriodNotRegistered {
    pub(crate) days: u16,
    pub(crate) allowed: Vec<u16>,
}

/// The retention of a send on `queue` (Python `_stamped_class`): the
/// send's own choice first, then the queue mapping, then the 30-day
/// default. A chosen period must be one of `sendable_periods`. Pure.
pub(crate) fn send_retention(
    config: &RetentionConfig,
    queue: &str,
    chosen: SendRetention,
) -> Result<EnqueueRetention, PeriodNotRegistered> {
    match chosen {
        SendRetention::FromQueueOrDefault => Ok(config.resolve_queue_retention(queue)),
        SendRetention::Forever => Ok(EnqueueRetention::Forever),
        SendRetention::Period(days) => {
            let allowed = config.sendable_periods();
            match allowed.contains(&days) {
                true => Ok(EnqueueRetention::Period(days)),
                false => Err(PeriodNotRegistered {
                    days: days.get(),
                    allowed: allowed.iter().map(|days| days.get()).collect(),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn days(count: i64) -> RetentionDays {
        RetentionDays::parse_period(chrono::Duration::days(count)).unwrap()
    }

    fn config() -> RetentionConfig {
        let mut config = RetentionConfig::default();
        let _previous = config.queue_retention.insert("audit".to_owned(), None);
        let _previous = config
            .queue_retention
            .insert("short".to_owned(), Some(chrono::Duration::days(7)));
        config
    }

    #[test]
    fn the_options_hold_what_the_builder_sets() {
        let deadline = Utc::now();
        let options = TaskSendOptions::new()
            .delay(WholeSeconds::from_secs(60))
            .good_until(deadline)
            .idempotency_key("invoice-7")
            .retain_forever();
        assert_eq!(
            options.start(),
            SendStart::AfterDelay(WholeSeconds::from_secs(60))
        );
        assert_eq!(options.deadline(), Some(deadline));
        assert_eq!(options.key(), Some("invoice-7"));
        assert_eq!(options.chosen_retention(), SendRetention::Forever);
        let defaults = TaskSendOptions::new();
        assert_eq!(defaults.start(), SendStart::Now);
        assert_eq!(
            defaults.chosen_retention(),
            SendRetention::FromQueueOrDefault
        );
    }

    #[test]
    fn no_choice_takes_the_queue_mapping_or_the_default() {
        let config = config();
        let resolve = |queue| send_retention(&config, queue, SendRetention::FromQueueOrDefault);
        assert_eq!(resolve("audit"), Ok(EnqueueRetention::Forever));
        assert_eq!(resolve("short"), Ok(EnqueueRetention::Period(days(7))));
        assert_eq!(resolve("default"), Ok(EnqueueRetention::Default));
    }

    #[test]
    fn a_chosen_retention_wins_over_the_queue_mapping() {
        let config = config();
        assert_eq!(
            send_retention(&config, "short", SendRetention::Forever),
            Ok(EnqueueRetention::Forever)
        );
        assert_eq!(
            send_retention(&config, "audit", SendRetention::Period(days(30))),
            Ok(EnqueueRetention::Period(days(30)))
        );
    }

    #[test]
    fn a_period_that_is_not_registered_is_refused_with_the_allowed_ones() {
        let refused =
            send_retention(&config(), "default", SendRetention::Period(days(90))).unwrap_err();
        assert_eq!(refused.days, 90);
        assert_eq!(refused.allowed, vec![7, 30]);
    }
}
