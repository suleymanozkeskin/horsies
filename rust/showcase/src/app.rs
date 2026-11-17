//! The Acme app: its config, worker profile and schedules, its tasks and
//! workflows, and the store that the task bodies use.
//!
//! Every process of the showcase builds the same app: the producer
//! scenarios, the worker, each worker child, the scheduler and the web
//! server. Building it does no I/O.

use std::collections::BTreeMap;

use chrono::NaiveTime;
use horsies::{
    AppConfig, ChildPreload, CronEnumTerm, CronNumericTerm, CronSchedule, CustomQueueConfig,
    DailySchedule, DaySelector, Horsies, HorsiesError, HourlySchedule, IntervalSchedule,
    IsolatedChildCount, IsolatedChildren, IsolatedSetting, Month, MonthlySchedule, QueueMode,
    ScheduleConfig, SchedulePattern, SchemaMode, SharedAsyncChildCount, SharedAsyncChildren,
    SharedAsyncSeats, SharedAsyncSetting, TaskSchedule, Weekday, WeeklySchedule, WorkerProfile,
};
use serde_json::json;

use crate::settings::{resolve_database_settings, SettingsError};
use crate::store::TaskStore;
use crate::tasks::{
    self, AcmeTasks, QUEUE_ANALYTICS, QUEUE_FULFILLMENT, QUEUE_NOTIFICATIONS, QUEUE_PAYMENTS,
};
use crate::workflows::{self, RegisteredWorkflows};

/// The queues: name, priority (1 is claimed first), running-task cap.
pub const QUEUES: &[(&str, u32, Option<u32>)] = &[
    (QUEUE_PAYMENTS, 1, Some(4)),
    (QUEUE_FULFILLMENT, 10, Some(8)),
    (QUEUE_NOTIFICATIONS, 50, Some(6)),
    (QUEUE_ANALYTICS, 90, Some(4)),
];

/// The worker profile that `acme worker` runs by default.
pub const WORKER_PROFILE: &str = "acme";

/// The isolated children of the profile: the store-backed tasks, one at a
/// time each.
const ISOLATED_CHILDREN: IsolatedChildCount = IsolatedChildCount::from_constant(6);
/// The shared async children of the profile.
const SHARED_ASYNC_CHILDREN: SharedAsyncChildCount = SharedAsyncChildCount::from_constant(1);
/// The seats of each shared async child: the light tasks run side by side.
const SHARED_ASYNC_SEATS: SharedAsyncSeats = SharedAsyncSeats::from_constant(32);

/// The built app with the handles that the scenarios use.
pub struct ShowcaseApp {
    pub app: Horsies,
    pub tasks: AcmeTasks,
    pub workflows: RegisteredWorkflows,
}

/// Why the showcase app was not built.
#[derive(Debug, thiserror::Error)]
pub enum ShowcaseAppError {
    #[error("settings: {0}")]
    Settings(SettingsError),
    #[error("horsies app: {0}")]
    Horsies(HorsiesError),
}

/// The app of the database that the five Acme resolution rules name.
///
/// Errors: `Settings` when no rule gives a usable URL; `Horsies` for a
/// refused config or registration.
pub fn build_app(mode: SchemaMode) -> Result<ShowcaseApp, ShowcaseAppError> {
    let settings = resolve_database_settings().map_err(ShowcaseAppError::Settings)?;
    build_app_for_url(settings.sqlx_url(), mode)
}

/// The app of the database at `url`, with the schema mode `mode`.
///
/// Errors: `Horsies` for a refused config or registration.
pub fn build_app_for_url(url: &str, mode: SchemaMode) -> Result<ShowcaseApp, ShowcaseAppError> {
    build_app_with_config(config_for_url(url), url, mode)
}

/// The app of `config`, whose task bodies use the Acme store at
/// `store_url`.
///
/// Errors: `Horsies` for a refused config or registration.
pub fn build_app_with_config(
    config: AppConfig,
    store_url: &str,
    mode: SchemaMode,
) -> Result<ShowcaseApp, ShowcaseAppError> {
    let mut app = Horsies::with_schema_mode(config, mode).map_err(ShowcaseAppError::Horsies)?;
    app.provide(TaskStore::new(store_url))
        .map_err(ShowcaseAppError::Horsies)?;
    let tasks = tasks::register_all(&mut app).map_err(ShowcaseAppError::Horsies)?;
    let workflows = workflows::register_all(&mut app).map_err(ShowcaseAppError::Horsies)?;
    Ok(ShowcaseApp {
        app,
        tasks,
        workflows,
    })
}

/// The Acme config of the database at `url`: the four queues, the worker
/// profile, the tuned recovery and retention settings, and the schedules.
pub fn config_for_url(url: &str) -> AppConfig {
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
    config.worker_profiles = BTreeMap::from([(WORKER_PROFILE.to_owned(), worker_profile())]);
    config.recovery.worker_state_snapshot_interval_ms =
        crate::tuning::WORKER_STATE_SNAPSHOT_INTERVAL_MS;
    config.recovery.check_interval_ms = crate::tuning::RECOVERY_CHECK_INTERVAL_MS;
    config.retention.terminal_record_retention_hours =
        Some(crate::tuning::TERMINAL_RECORD_RETENTION_HOURS);
    config.schedule = Some(schedules());
    config.resend_on_transient_err = true;
    config
}

/// The worker profile: isolated children for the store-backed tasks and one
/// shared async child for the light tasks. A Rust worker starts its children
/// with `exec` of its own binary, so the profile has no preload process.
pub fn worker_profile() -> WorkerProfile {
    WorkerProfile {
        isolated: IsolatedSetting::IsolatedChildren(IsolatedChildren::new(ISOLATED_CHILDREN)),
        shared_async: SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren::new(
            SHARED_ASYNC_CHILDREN,
            SHARED_ASYNC_SEATS,
        )),
        preload: ChildPreload::NoPreload,
        ..WorkerProfile::default()
    }
}

/// The time of day `hour:minute`, a constant of the schedule table.
fn at(hour: u32, minute: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(hour, minute, 0)
        .unwrap_or_else(|| panic!("{hour}:{minute} is not a time of day"))
}

fn interval_seconds(seconds: u64) -> SchedulePattern {
    SchedulePattern::Interval(IntervalSchedule {
        seconds: Some(u32::try_from(seconds).unwrap_or(u32::MAX)),
        ..IntervalSchedule::default()
    })
}

fn interval_minutes(minutes: u64) -> SchedulePattern {
    SchedulePattern::Interval(IntervalSchedule {
        minutes: Some(u32::try_from(minutes).unwrap_or(u32::MAX)),
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

fn step(step: u8) -> CronNumericTerm {
    CronNumericTerm::Step {
        step: u32::from(step),
    }
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

/// One schedule of `task_name` with `input`, the task's stored input: one
/// member per parameter. The schedule sends to the task's queue.
fn schedule(
    name: &str,
    task_name: &str,
    pattern: SchedulePattern,
    input: serde_json::Value,
) -> TaskSchedule {
    TaskSchedule::new(name, task_name, pattern)
        .input(input)
        .catch_up_missed(false)
}

/// The scheduler table: 32 schedules, 29 enabled.
pub fn schedules() -> ScheduleConfig {
    let mut schedules = Vec::new();
    for (offset, supplier) in (0_u64..).zip(crate::tuning::SUPPLIERS.iter()) {
        schedules.push(schedule(
            &format!("supplier-feed-{supplier}"),
            "sync_supplier_feed",
            interval_seconds(crate::tuning::SUPPLIER_FEED_INTERVAL_SECONDS + offset * 30),
            json!({"supplier": supplier}),
        ));
    }
    for (offset, region) in (0_u32..).zip(crate::tuning::REGIONS.iter()) {
        schedules.push(schedule(
            &format!("rollup-{region}"),
            "regional_rollup",
            hourly(10 + offset * 5),
            json!({"region": region}),
        ));
        schedules.push(schedule(
            &format!("cache-warm-{region}"),
            "warm_cache_edge",
            interval_minutes(crate::tuning::CACHE_WARM_INTERVAL_MINUTES + u64::from(offset)),
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
            hourly(u32::from(crate::tuning::ABANDONED_CART_MINUTE)),
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
            daily(u32::from(crate::tuning::SALES_ROLLUP_HOUR), 0),
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
                vec![values(&[i64::from(crate::tuning::RECONCILE_MINUTE)])],
                vec![step(crate::tuning::RECONCILE_HOUR_STEP)],
                DaySelector::EveryDay,
            ),
            json!({"window": "4h"}),
        ),
        schedule(
            "price-sync-quarter-hour",
            "warm_cache_edge",
            cron(
                vec![step(crate::tuning::PRICE_SYNC_MINUTE_STEP)],
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::tasks::ALL_TASK_NAMES;
    use crate::workflows::ALL_WORKFLOW_NAMES;

    const OFFLINE_URL: &str = "postgresql://localhost/acme_demo";

    fn offline_app() -> ShowcaseApp {
        build_app_for_url(OFFLINE_URL, SchemaMode::MigrateAndValidate).expect("the showcase app")
    }

    #[test]
    fn every_task_registers_and_check_passes() {
        let showcase = offline_app();
        let passed = showcase.app.check().expect("the app check");
        assert_eq!(passed.task_count(), ALL_TASK_NAMES.len());
        let unique = ALL_TASK_NAMES.iter().collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), ALL_TASK_NAMES.len());
    }

    #[test]
    fn every_workflow_registers() {
        let showcase = offline_app();
        let mut registered = showcase.workflows.names();
        registered.sort_unstable();
        let mut expected = ALL_WORKFLOW_NAMES.to_vec();
        expected.sort_unstable();
        assert_eq!(registered, expected);
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

    #[test]
    fn the_invoice_deadline_is_the_tuned_one() {
        let mut app = Horsies::new(config_for_url(OFFLINE_URL)).expect("the app");
        let invoice =
            crate::tasks::orders::generate_invoice::register(&mut app).expect("the invoice task");
        assert_eq!(
            invoice
                .task_options()
                .and_then(|options| options.timeout_ms),
            Some(crate::tuning::INVOICE_TIMEOUT_MS)
        );
    }

    #[test]
    fn every_queue_of_the_config_is_a_queue_of_the_app() {
        let showcase = offline_app();
        let mut valid = showcase.app.get_valid_queue_names();
        valid.sort_unstable();
        let mut expected = QUEUES
            .iter()
            .map(|(name, _, _)| (*name).to_owned())
            .collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(valid, expected);
    }
}
