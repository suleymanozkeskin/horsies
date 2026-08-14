//! Acme application configuration and public task registration.

use chrono::NaiveTime;
use horsies::{
    AppConfig, CronEnumTerm, CronNumericTerm, CronSchedule, CustomQueueConfig, DailySchedule,
    DaySelector, Horsies, HorsiesError, HourlySchedule, IntervalSchedule, Month, MonthlySchedule,
    QueueMode, ScheduleConfig, SchedulePattern, TaskSchedule, Weekday, WeeklySchedule,
};
use serde_json::json;

use crate::settings::{resolve_database_settings, SettingsError};
use crate::tasks::{self, QUEUE_ANALYTICS, QUEUE_FULFILLMENT, QUEUE_NOTIFICATIONS, QUEUE_PAYMENTS};
use crate::workflows;

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
    config.schedule = Some(schedules());
    config.resend_on_transient_err = true;
    config
}

pub fn build_app_for_url(url: &str) -> Result<Horsies, ShowcaseAppError> {
    build_app_with_handles_for_url(url).map(|(app, _, _)| app)
}

/// Build the application and retain the public task handles used by dynamic
/// workflow starts in integration tests and demo scenarios.
pub fn build_app_with_handles_for_url(
    url: &str,
) -> Result<(Horsies, tasks::TaskHandles, workflows::RegisteredWorkflows), ShowcaseAppError> {
    let mut app = Horsies::new(config_for_url(url)).map_err(ShowcaseAppError::Horsies)?;
    let handles = tasks::register_all(&mut app).map_err(ShowcaseAppError::Horsies)?;
    let order_template =
        workflows::register_all(&mut app, handles.clone()).map_err(ShowcaseAppError::Horsies)?;
    Ok((app, handles, order_template))
}

fn at(hour: u32, minute: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(hour, minute, 0).expect("valid showcase schedule time")
}

fn interval_seconds(seconds: u64) -> SchedulePattern {
    SchedulePattern::Interval(IntervalSchedule {
        seconds: Some(seconds as u32),
        ..IntervalSchedule::default()
    })
}

fn interval_minutes(minutes: u64) -> SchedulePattern {
    SchedulePattern::Interval(IntervalSchedule {
        minutes: Some(minutes as u32),
        ..IntervalSchedule::default()
    })
}

fn hourly(minute: u32) -> SchedulePattern {
    SchedulePattern::Hourly(HourlySchedule { minute, second: 0 })
}

fn daily(hour: u32, minute: u32) -> SchedulePattern {
    SchedulePattern::Daily(DailySchedule {
        time: at(hour, minute),
    })
}

fn weekly(days: Vec<Weekday>, hour: u32, minute: u32) -> SchedulePattern {
    SchedulePattern::Weekly(WeeklySchedule {
        days,
        time: at(hour, minute),
    })
}

fn monthly(day: u32, hour: u32, minute: u32) -> SchedulePattern {
    SchedulePattern::Monthly(MonthlySchedule {
        day,
        time: at(hour, minute),
    })
}

fn every() -> CronNumericTerm {
    CronNumericTerm::Every
}

fn values(values: &[i64]) -> CronNumericTerm {
    CronNumericTerm::Values {
        values: values.to_vec(),
    }
}

fn step(step: u32) -> CronNumericTerm {
    CronNumericTerm::Step { step }
}

fn every_month() -> CronEnumTerm<Month> {
    CronEnumTerm::Every
}

fn cron(
    minute: Vec<CronNumericTerm>,
    hour: Vec<CronNumericTerm>,
    day: DaySelector,
) -> SchedulePattern {
    SchedulePattern::Cron(CronSchedule {
        minute,
        hour,
        month: vec![every_month()],
        day,
    })
}

fn schedule(
    name: &str,
    task_name: &str,
    pattern: SchedulePattern,
    kwargs: serde_json::Value,
) -> TaskSchedule {
    TaskSchedule::new(name, task_name, pattern)
        .kwargs(kwargs)
        .queue(match task_name {
            "reconcile_payments" => QUEUE_PAYMENTS,
            "marketing_blast" => QUEUE_NOTIFICATIONS,
            "warm_cache_edge" => QUEUE_FULFILLMENT,
            _ => QUEUE_ANALYTICS,
        })
        .catch_up_missed(false)
}

/// The complete typed scheduler table from the showcase source.
pub fn schedules() -> ScheduleConfig {
    let mut schedules = Vec::new();
    for (index, supplier) in crate::tuning::SUPPLIERS.iter().enumerate() {
        schedules.push(schedule(
            &format!("supplier-feed-{supplier}"),
            "sync_supplier_feed",
            interval_seconds(crate::tuning::SUPPLIER_FEED_INTERVAL_SECONDS + index as u64 * 30),
            json!({"supplier": supplier}),
        ));
    }
    for (index, region) in crate::tuning::REGIONS.iter().enumerate() {
        schedules.push(schedule(
            &format!("rollup-{region}"),
            "regional_rollup",
            hourly(10 + index as u32 * 5),
            json!({"region": region}),
        ));
        schedules.push(schedule(
            &format!("cache-warm-{region}"),
            "warm_cache_edge",
            interval_minutes(crate::tuning::CACHE_WARM_INTERVAL_MINUTES + index as u64),
            json!({"campaign_id": format!("steady-{region}")}),
        ));
    }
    schedules.extend([
        schedule(
            "search-prewarm",
            "prewarm_search",
            interval_minutes(crate::tuning::SEARCH_PREWARM_INTERVAL_MINUTES),
            json!({"campaign_id": "steady-state"}),
        ),
        schedule(
            "abandoned-cart-sweep",
            "abandoned_cart_sweep",
            hourly(crate::tuning::ABANDONED_CART_MINUTE as u32),
            json!({"older_than_minutes": crate::tuning::ABANDONED_CART_AGE_MINUTES}),
        ),
        schedule(
            "retention-audit-hourly",
            "retention_audit",
            hourly(50),
            json!({"older_than_days": crate::tuning::RETENTION_AUDIT_DAYS}),
        ),
        schedule(
            "sales-rollup-daily",
            "sales_rollup",
            daily(crate::tuning::SALES_ROLLUP_HOUR as u32, 0),
            json!({"window": "daily"}),
        ),
        schedule(
            "retention-audit-daily",
            "retention_audit",
            daily(3, 30),
            json!({"older_than_days": 90}),
        ),
        schedule(
            "nightly-stocktake",
            "replenish_catalog",
            daily(4, 15),
            json!({"target_units": crate::tuning::CATALOG_STOCK_PER_SKU}),
        ),
        schedule(
            "reconcile-daily",
            "reconcile_payments",
            daily(4, 0),
            json!({"window": "daily"}),
        ),
        schedule(
            "winback-blast",
            "marketing_blast",
            daily(9, 0),
            json!({"segment": "winback"}),
        ),
        schedule(
            "newsletter-blast",
            "marketing_blast",
            daily(10, 0),
            json!({"segment": "newsletter"}),
        )
        .enabled(false),
        schedule(
            "weekly-sales-review",
            "sales_rollup",
            weekly(vec![Weekday::Monday], 6, 0),
            json!({"window": "weekly"}),
        ),
        schedule(
            "weekly-supplier-audit",
            "sync_supplier_feed",
            weekly(vec![Weekday::Wednesday], 7, 0),
            json!({"supplier": crate::tuning::SUPPLIERS[0]}),
        ),
        schedule(
            "weekend-flash-prep",
            "prewarm_search",
            weekly(vec![Weekday::Friday, Weekday::Saturday], 12, 0),
            json!({"campaign_id": "weekend"}),
        ),
        schedule(
            "weekly-retention-audit",
            "retention_audit",
            weekly(vec![Weekday::Sunday], 2, 0),
            json!({"older_than_days": 180}),
        )
        .enabled(false),
        schedule(
            "monthly-close",
            "reconcile_payments",
            monthly(1, 5, 0),
            json!({"window": "monthly"}),
        ),
        schedule(
            "monthly-catalog-audit",
            "retention_audit",
            monthly(15, 8, 0),
            json!({"older_than_days": 365}),
        ),
        schedule(
            "monthly-markdown-review",
            "sales_rollup",
            monthly(28, 11, 0),
            json!({"window": "monthly"}),
        ),
        schedule(
            "payment-reconciliation",
            "reconcile_payments",
            cron(
                vec![values(&[crate::tuning::RECONCILE_MINUTE as i64])],
                vec![step(crate::tuning::RECONCILE_HOUR_STEP as u32)],
                DaySelector::EveryDay,
            ),
            json!({"window": "4h"}),
        ),
        schedule(
            "price-sync-quarter-hour",
            "warm_cache_edge",
            cron(
                vec![step(crate::tuning::PRICE_SYNC_MINUTE_STEP as u32)],
                vec![every()],
                DaySelector::EveryDay,
            ),
            json!({"campaign_id": "price-sync"}),
        ),
        schedule(
            "fraud-review-friday-13th",
            "reconcile_payments",
            SchedulePattern::Cron(CronSchedule {
                minute: vec![values(&[0])],
                hour: vec![values(&[9])],
                month: vec![every_month()],
                day: DaySelector::BothDays {
                    day_of_month: vec![values(&[13])],
                    day_of_week: vec![CronEnumTerm::EnumValues {
                        values: vec![Weekday::Friday],
                    }],
                },
            }),
            json!({"window": "fraud-review"}),
        ),
        schedule(
            "nightly-export",
            "flaky_export",
            cron(
                vec![values(&[40])],
                vec![CronNumericTerm::Range {
                    start: 1,
                    end: 5,
                    step: 2,
                }],
                DaySelector::EveryDay,
            ),
            json!({"export_id": "nightly"}),
        )
        .enabled(false),
        schedule(
            "quarterly-supplier-review",
            "sync_supplier_feed",
            SchedulePattern::Cron(CronSchedule {
                minute: vec![values(&[30])],
                hour: vec![values(&[6])],
                month: vec![CronEnumTerm::EnumValues {
                    values: vec![Month::January, Month::April, Month::July, Month::October],
                }],
                day: DaySelector::ByMonthDay {
                    day_of_month: vec![values(&[1])],
                },
            }),
            json!({"supplier": crate::tuning::SUPPLIERS[2]}),
        ),
    ]);
    ScheduleConfig::new(schedules)
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

    #[test]
    fn scheduler_table_matches_the_pinned_entry_count() {
        let schedule = schedules();
        assert_eq!(schedule.schedules.len(), 32);
        assert_eq!(
            schedule
                .schedules
                .iter()
                .filter(|entry| entry.enabled)
                .count(),
            29
        );
        schedule.validate().expect("showcase schedules");
    }
}
