//! Acme application configuration and public task registration.

use horsies::{AppConfig, CustomQueueConfig, Horsies, HorsiesError, QueueMode};

use crate::settings::{resolve_database_settings, SettingsError};
use crate::tasks::{self, QUEUE_ANALYTICS, QUEUE_FULFILLMENT, QUEUE_NOTIFICATIONS, QUEUE_PAYMENTS};

pub const QUEUES: &[(&str, u32, Option<u32>)] = &[
    (QUEUE_PAYMENTS, 1, Some(4)),
    (QUEUE_FULFILLMENT, 10, Some(8)),
    (QUEUE_NOTIFICATIONS, 50, Some(6)),
    (QUEUE_ANALYTICS, 90, Some(4)),
];

pub fn build_app() -> Result<Horsies, ShowcaseAppError> {
    let settings = resolve_database_settings().map_err(ShowcaseAppError::Settings)?;
    build_app_for_url(settings.sqlx_url())
}

fn config_for_url(url: &str) -> AppConfig {
    let mut config = AppConfig::for_database_url(url);
    config.queue_mode = QueueMode::Custom;
    config.custom_queues = Some(
        QUEUES
            .iter()
            .map(|(name, priority, max_concurrency)| CustomQueueConfig {
                name: (*name).to_owned(),
                priority: *priority,
                max_concurrency: *max_concurrency,
            })
            .collect(),
    );
    config.recovery.worker_state_snapshot_interval_ms =
        crate::tuning::WORKER_STATE_SNAPSHOT_INTERVAL_MS;
    config.recovery.check_interval_ms = crate::tuning::RECOVERY_CHECK_INTERVAL_MS;
    config.retention.terminal_record_retention_hours =
        Some(crate::tuning::TERMINAL_RECORD_RETENTION_HOURS);
    config.resend_on_transient_err = true;
    config
}

pub fn build_app_for_url(url: &str) -> Result<Horsies, ShowcaseAppError> {
    let mut app = Horsies::new(config_for_url(url)).map_err(ShowcaseAppError::Horsies)?;
    tasks::register_all(&mut app).map_err(ShowcaseAppError::Horsies)?;
    Ok(app)
}

/// Build only the two story tasks and retain their public task handles.
///
/// The opt-in database gate uses this constructor to execute the three error
/// paths through the same public registration and worker APIs as an adopter.
pub fn build_story_app_for_url(
    url: &str,
) -> Result<(Horsies, tasks::promotions::StoryTaskHandles), ShowcaseAppError> {
    let mut app = Horsies::new(config_for_url(url)).map_err(ShowcaseAppError::Horsies)?;
    let handles = tasks::promotions::register_story(&mut app).map_err(ShowcaseAppError::Horsies)?;
    Ok((app, handles))
}

#[derive(Debug, thiserror::Error)]
pub enum ShowcaseAppError {
    #[error("settings: {0}")]
    Settings(SettingsError),
    #[error("horsies app: {0}")]
    Horsies(HorsiesError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tasks::ALL_TASK_NAMES;

    #[test]
    fn every_task_registers_and_check_passes() {
        let app = build_app_for_url("postgresql://localhost/acme_demo").expect("app");
        let mut registered = app.registry().task_names().collect::<Vec<_>>();
        registered.sort_unstable();
        let mut expected = ALL_TASK_NAMES.to_vec();
        expected.sort_unstable();
        assert_eq!(registered, expected);
        app.check().expect("task registry check");
    }
}
