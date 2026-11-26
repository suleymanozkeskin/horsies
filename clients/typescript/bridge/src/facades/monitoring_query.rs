//! The arguments of the monitoring reads, and the core queries that they
//! build.
//!
//! - The core owns every bound of a query: the history window (since before
//!   until, at most 30 days, `resolve_monitoring_window`), the page of the
//!   task list, and the limits of the breakdown and of the workflow runs. A
//!   value that the core refuses is a [`QueryRefusal`]: the read answers it
//!   at once, with no connect and no statement.
//! - A value that breaks a boundary rule (a status text that is not a task
//!   status, a count that is not a whole number) is a defect of the package,
//!   which checks every value first: [`QueryStop::Defect`].
//! - An instant outside the range of the core is a refusal of the window,
//!   not a defect: every `Temporal.Instant` is a valid argument of the
//!   package.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use horsies_core::monitoring::{
    resolve_monitoring_window, ErrorCategory, HistoryWindow, SortDirection, TaskBreakdownQuery,
    TaskFacetsQuery, TaskFilters, TaskGroupBy, TaskListQuery, TaskSortField, TaskStatsQuery,
    WorkflowRunsQuery,
};
use horsies_core::TaskStatus;
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::values::boundary::{instant_of_epoch_micros, BoundaryRefusal};

/// The rule of a count that crosses as a number.
const WHOLE_NUMBER_RULE: &str = "a whole number from -9007199254740991 to 9007199254740991";

/// The rule of a task status text.
const TASK_STATUS_RULE: &str = "the wire value of a task status, for example FAILED";

/// The largest integer that a JavaScript number holds exactly.
const MAX_EXACT_NUMBER: f64 = 9_007_199_254_740_991.0;

/// The argument of a query that the core refuses.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryField {
    /// The history window.
    Window,
    /// The page of the task list: its offset and limit together.
    Page,
    /// The limit of the breakdown or of the workflow runs.
    Limit,
}

/// A query value that the core refuses, with the core's reason.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRefusal {
    pub field: QueryField,
    pub reason: String,
}

impl QueryRefusal {
    fn new(field: QueryField, reason: impl Into<String>) -> Self {
        Self {
            field,
            reason: reason.into(),
        }
    }
}

/// Why a query was not built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryStop {
    /// The core refuses a value: an expected outcome of the read.
    Refused(QueryRefusal),
    /// A value breaks a boundary rule: a defect of the package.
    Defect(BoundaryRefusal),
}

impl From<BoundaryRefusal> for QueryStop {
    fn from(refusal: BoundaryRefusal) -> Self {
        Self::Defect(refusal)
    }
}

/// One bound of the history window.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowBoundArg {
    /// The core's default: `until` is now, `since` is 24 hours before
    /// `until`.
    CoreDefault,
    /// An instant, in epoch microseconds.
    At { epoch_micros: BigInt },
}

/// The history window of a list or aggregate read.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowArg {
    pub since: WindowBoundArg,
    pub until: WindowBoundArg,
}

/// Which tasks the retried filter keeps.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetriedArg {
    Any,
    RetriedOnly,
}

/// An error category, by its wire value.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCategoryArg {
    Operational,
    Contract,
    Retrieval,
    Outcome,
    Domain,
}

/// A count of a query: the core's default (of `TaskListQuery::new`,
/// `TaskBreakdownQuery::new` or `WorkflowRunsQuery::new`), or a value.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum CountArg {
    CoreDefault,
    Set { value: f64 },
}

/// The sort of the task list: the core's default (`TaskListQuery::new`), or
/// a field and a direction.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortArg {
    CoreDefault,
    Set {
        field: SortFieldArg,
        direction: SortDirectionArg,
    },
}

/// The sort field of the task list.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortFieldArg {
    EnqueuedAt,
    StartedAt,
    CompletedAt,
    FailedAt,
    Status,
    TaskName,
    QueueName,
    Priority,
    RetryCount,
    QueueSeconds,
    ExecutionSeconds,
}

/// The sort direction of the task list.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirectionArg {
    Ascending,
    Descending,
}

/// The column that a breakdown groups by.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupByArg {
    Worker,
    TaskName,
    Queue,
}

/// The filters of a task read. An empty list keeps every value.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FiltersArg {
    /// Task statuses, by their wire values.
    pub statuses: Vec<String>,
    pub task_names: Vec<String>,
    pub queues: Vec<String>,
    pub workers: Vec<String>,
    pub error_codes: Vec<String>,
    pub error_categories: Vec<ErrorCategoryArg>,
    pub retried: RetriedArg,
}

/// The task stats read: every filter but the statuses (the stats are the
/// status selector).
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskStatsArg {
    pub window: WindowArg,
    pub task_names: Vec<String>,
    pub queues: Vec<String>,
    pub workers: Vec<String>,
    pub error_codes: Vec<String>,
    pub error_categories: Vec<ErrorCategoryArg>,
    pub retried: RetriedArg,
}

/// The task facets read: only the coarse filters, so one selection never
/// empties the option lists of the others.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskFacetsArg {
    pub window: WindowArg,
    pub statuses: Vec<String>,
    pub error_categories: Vec<ErrorCategoryArg>,
    pub retried: RetriedArg,
}

/// The task breakdown read. `limit` is the most groups.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct TaskBreakdownArg {
    pub window: WindowArg,
    pub group_by: GroupByArg,
    pub filters: FiltersArg,
    pub limit: CountArg,
}

/// The task list read: the window, the filters, the sort and the page.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct TaskListArg {
    pub window: WindowArg,
    pub filters: FiltersArg,
    pub sort: SortArg,
    pub offset: CountArg,
    pub limit: CountArg,
}

/// The workflow name filter of the workflow runs read.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunNameArg {
    /// Runs of every workflow.
    AnyName,
    /// Runs of the workflow `name`.
    Named { name: String },
}

/// The status filter of the workflow runs read.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_to_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunStatusArg {
    /// Runs in every status.
    AnyStatus,
    /// Runs in `status`, the stored text of a workflow status.
    Status { status: String },
}

/// The workflow runs read.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowRunsArg {
    pub name: RunNameArg,
    pub status: RunStatusArg,
    pub limit: CountArg,
}

/// The core query of a task stats read. Pure.
pub fn task_stats_query(arg: TaskStatsArg) -> Result<TaskStatsQuery, QueryStop> {
    let window = history_window(&arg.window)?;
    let filters = TaskFilters {
        statuses: Vec::new(),
        task_names: arg.task_names,
        queues: arg.queues,
        workers: arg.workers,
        error_codes: arg.error_codes,
        error_categories: error_categories(&arg.error_categories),
        retried_only: retried_only(arg.retried),
    };
    Ok(TaskStatsQuery::new(window).with_filters(filters))
}

/// The core query of a task facets read. Pure.
pub fn task_facets_query(arg: &TaskFacetsArg) -> Result<TaskFacetsQuery, QueryStop> {
    let window = history_window(&arg.window)?;
    Ok(TaskFacetsQuery::new(window)
        .with_statuses(task_statuses(&arg.statuses)?)
        .with_error_categories(error_categories(&arg.error_categories))
        .retried_only(retried_only(arg.retried)))
}

/// The core query of a task breakdown read. Pure.
pub fn task_breakdown_query(arg: TaskBreakdownArg) -> Result<TaskBreakdownQuery, QueryStop> {
    let window = history_window(&arg.window)?;
    let query = TaskBreakdownQuery::new(window, group_column(arg.group_by))
        .with_filters(task_filters(arg.filters)?);
    match set_count("limit", &arg.limit)? {
        None => Ok(query),
        Some(limit) => query.with_limit(limit).map_err(|refused| {
            QueryStop::Refused(QueryRefusal::new(QueryField::Limit, refused.reason))
        }),
    }
}

/// The core query of a task list read. Pure.
///
/// A count left at the core's default takes the value of
/// `TaskListQuery::new`; the core checks the page as a whole.
pub fn task_list_query(arg: TaskListArg) -> Result<TaskListQuery, QueryStop> {
    let window = history_window(&arg.window)?;
    let query = TaskListQuery::new(window).with_filters(task_filters(arg.filters)?);
    let query = match arg.sort {
        SortArg::CoreDefault => query,
        SortArg::Set { field, direction } => {
            query.with_sort(sort_field(field), sort_direction(direction))
        }
    };
    let offset = set_count("offset", &arg.offset)?;
    let limit = set_count("limit", &arg.limit)?;
    match (offset, limit) {
        (None, None) => Ok(query),
        (offset, limit) => {
            let offset = offset.unwrap_or(query.offset());
            let limit = limit.unwrap_or(query.limit());
            query.with_pagination(offset, limit).map_err(|refused| {
                QueryStop::Refused(QueryRefusal::new(QueryField::Page, refused.reason))
            })
        }
    }
}

/// The core query of a workflow runs read. Pure.
pub fn workflow_runs_query(arg: WorkflowRunsArg) -> Result<WorkflowRunsQuery, QueryStop> {
    let limit = set_count("limit", &arg.limit)?;
    let name = match arg.name {
        RunNameArg::AnyName => None,
        RunNameArg::Named { name } => Some(name),
    };
    let status = match arg.status {
        RunStatusArg::AnyStatus => None,
        RunStatusArg::Status { status } => Some(status),
    };
    let query = WorkflowRunsQuery::new().with_name(name).with_status(status);
    match limit {
        None => Ok(query),
        Some(limit) => query.with_limit(limit).map_err(|refused| {
            QueryStop::Refused(QueryRefusal::new(QueryField::Limit, refused.reason))
        }),
    }
}

/// The value of a count, or `None` for the core's default. A value that is
/// not a whole number is a defect of the package. Pure.
fn set_count(subject: &str, count: &CountArg) -> Result<Option<i64>, BoundaryRefusal> {
    match count {
        CountArg::CoreDefault => Ok(None),
        CountArg::Set { value } => whole_number(subject, *value).map(Some),
    }
}

/// The core's window of `arg`, or the refusal of the window. Pure, but for
/// the clock: an unset `until` is the core's now.
fn history_window(arg: &WindowArg) -> Result<HistoryWindow, QueryStop> {
    let since = window_bound("since", &arg.since)?;
    let until = window_bound("until", &arg.until)?;
    resolve_monitoring_window(since, until, None).map_err(|refused| {
        QueryStop::Refused(QueryRefusal::new(QueryField::Window, refused.reason))
    })
}

/// The instant of one bound, or `None` for the core's default. An instant
/// outside the core's range refuses the window. Pure.
fn window_bound(name: &str, bound: &WindowBoundArg) -> Result<Option<DateTime<Utc>>, QueryStop> {
    match bound {
        WindowBoundArg::CoreDefault => Ok(None),
        WindowBoundArg::At { epoch_micros } => instant_of_epoch_micros(name, epoch_micros)
            .map(Some)
            .map_err(|refused| {
                QueryStop::Refused(QueryRefusal::new(
                    QueryField::Window,
                    format!("{}: {}", refused.subject, refused.rule),
                ))
            }),
    }
}

/// The core filters of `arg`. Pure.
fn task_filters(arg: FiltersArg) -> Result<TaskFilters, QueryStop> {
    Ok(TaskFilters {
        statuses: task_statuses(&arg.statuses)?,
        task_names: arg.task_names,
        queues: arg.queues,
        workers: arg.workers,
        error_codes: arg.error_codes,
        error_categories: error_categories(&arg.error_categories),
        retried_only: retried_only(arg.retried),
    })
}

/// The task statuses of their wire values. Pure.
pub fn task_statuses(values: &[String]) -> Result<Vec<TaskStatus>, BoundaryRefusal> {
    values
        .iter()
        .map(|value| {
            TaskStatus::from_str(value)
                .map_err(|_| BoundaryRefusal::new("statuses", TASK_STATUS_RULE))
        })
        .collect()
}

/// The core's error categories. Pure.
fn error_categories(values: &[ErrorCategoryArg]) -> Vec<ErrorCategory> {
    values.iter().map(|value| error_category(*value)).collect()
}

/// The core's error category of `value`. Pure.
const fn error_category(value: ErrorCategoryArg) -> ErrorCategory {
    match value {
        ErrorCategoryArg::Operational => ErrorCategory::Operational,
        ErrorCategoryArg::Contract => ErrorCategory::Contract,
        ErrorCategoryArg::Retrieval => ErrorCategory::Retrieval,
        ErrorCategoryArg::Outcome => ErrorCategory::Outcome,
        ErrorCategoryArg::Domain => ErrorCategory::Domain,
    }
}

/// The core's retried flag. Pure.
const fn retried_only(value: RetriedArg) -> bool {
    match value {
        RetriedArg::Any => false,
        RetriedArg::RetriedOnly => true,
    }
}

/// The core's sort field. Pure.
const fn sort_field(value: SortFieldArg) -> TaskSortField {
    match value {
        SortFieldArg::EnqueuedAt => TaskSortField::EnqueuedAt,
        SortFieldArg::StartedAt => TaskSortField::StartedAt,
        SortFieldArg::CompletedAt => TaskSortField::CompletedAt,
        SortFieldArg::FailedAt => TaskSortField::FailedAt,
        SortFieldArg::Status => TaskSortField::Status,
        SortFieldArg::TaskName => TaskSortField::TaskName,
        SortFieldArg::QueueName => TaskSortField::QueueName,
        SortFieldArg::Priority => TaskSortField::Priority,
        SortFieldArg::RetryCount => TaskSortField::RetryCount,
        SortFieldArg::QueueSeconds => TaskSortField::QueueSeconds,
        SortFieldArg::ExecutionSeconds => TaskSortField::ExecutionSeconds,
    }
}

/// The core's sort direction. Pure.
const fn sort_direction(value: SortDirectionArg) -> SortDirection {
    match value {
        SortDirectionArg::Ascending => SortDirection::Ascending,
        SortDirectionArg::Descending => SortDirection::Descending,
    }
}

/// The core's group column. Pure.
const fn group_column(value: GroupByArg) -> TaskGroupBy {
    match value {
        GroupByArg::Worker => TaskGroupBy::Worker,
        GroupByArg::TaskName => TaskGroupBy::TaskName,
        GroupByArg::Queue => TaskGroupBy::Queue,
    }
}

/// The `i64` of a count that crosses as a number: a whole number that a
/// JavaScript number holds exactly. The package checks it first. Pure.
pub fn whole_number(subject: &str, value: f64) -> Result<i64, BoundaryRefusal> {
    let exact = value.is_finite() && value.fract() == 0.0 && value.abs() <= MAX_EXACT_NUMBER;
    match exact {
        // The value is whole and at most 2^53 - 1 in size, so the conversion
        // is exact.
        true => Ok(value as i64),
        false => Err(BoundaryRefusal::new(subject, WHOLE_NUMBER_RULE)),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::values::boundary::epoch_micros_of;

    fn instant(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 25, hour, 0, 0)
            .single()
            .expect("a valid instant")
    }

    fn window(since: WindowBoundArg, until: WindowBoundArg) -> WindowArg {
        WindowArg { since, until }
    }

    fn at(hour: u32) -> WindowBoundArg {
        WindowBoundArg::At {
            epoch_micros: epoch_micros_of(instant(hour)),
        }
    }

    fn no_filters() -> FiltersArg {
        FiltersArg {
            statuses: Vec::new(),
            task_names: Vec::new(),
            queues: Vec::new(),
            workers: Vec::new(),
            error_codes: Vec::new(),
            error_categories: Vec::new(),
            retried: RetriedArg::Any,
        }
    }

    fn list(offset: f64, limit: f64) -> TaskListArg {
        TaskListArg {
            window: window(WindowBoundArg::CoreDefault, WindowBoundArg::CoreDefault),
            filters: no_filters(),
            sort: SortArg::CoreDefault,
            offset: CountArg::Set { value: offset },
            limit: CountArg::Set { value: limit },
        }
    }

    fn core_default_list() -> TaskListArg {
        TaskListArg {
            offset: CountArg::CoreDefault,
            limit: CountArg::CoreDefault,
            ..list(0.0, 1.0)
        }
    }

    fn refused_field<Q: std::fmt::Debug>(built: Result<Q, QueryStop>) -> QueryField {
        match built {
            Err(QueryStop::Refused(refusal)) => refusal.field,
            other => panic!("a refusal of the core, got {other:?}"),
        }
    }

    #[test]
    fn the_default_window_and_page_build_a_list_query() {
        assert!(task_list_query(list(0.0, 50.0)).is_ok());
    }

    #[test]
    fn a_page_left_at_the_core_default_keeps_the_values_of_new() {
        let built = task_list_query(core_default_list()).expect("the core's default page");
        let new = TaskListQuery::new(
            history_window(&window(
                WindowBoundArg::CoreDefault,
                WindowBoundArg::CoreDefault,
            ))
            .expect("the default window"),
        );
        assert_eq!((built.offset(), built.limit()), (new.offset(), new.limit()));
    }

    #[test]
    fn a_lone_limit_keeps_the_core_default_offset() {
        let arg = TaskListArg {
            limit: CountArg::Set { value: 7.0 },
            ..core_default_list()
        };
        let built = task_list_query(arg).expect("a page in bounds");
        assert_eq!((built.offset(), built.limit()), (0, 7));
    }

    #[test]
    fn a_lone_offset_past_the_reach_is_refused_as_the_page() {
        let arg = TaskListArg {
            offset: CountArg::Set { value: 499.0 },
            ..core_default_list()
        };
        assert_eq!(refused_field(task_list_query(arg)), QueryField::Page);
    }

    #[test]
    fn an_explicit_window_inside_the_bounds_builds() {
        let built = history_window(&window(at(1), at(5)));
        assert!(built.is_ok(), "{built:?}");
    }

    #[test]
    fn a_window_that_does_not_increase_is_refused_as_the_window() {
        let built = history_window(&window(at(5), at(5)));
        assert!(matches!(
            built,
            Err(QueryStop::Refused(QueryRefusal {
                field: QueryField::Window,
                ..
            }))
        ));
    }

    #[test]
    fn a_window_of_31_days_is_refused_as_the_window() {
        let since = WindowBoundArg::At {
            epoch_micros: epoch_micros_of(instant(0) - chrono::Duration::days(31)),
        };
        assert!(matches!(
            history_window(&window(since, at(0))),
            Err(QueryStop::Refused(QueryRefusal {
                field: QueryField::Window,
                ..
            }))
        ));
    }

    #[test]
    fn an_instant_outside_the_core_range_is_refused_as_the_window() {
        let far = WindowBoundArg::At {
            epoch_micros: BigInt::from(i128::from(i64::MAX)),
        };
        assert!(matches!(
            history_window(&window(WindowBoundArg::CoreDefault, far)),
            Err(QueryStop::Refused(QueryRefusal {
                field: QueryField::Window,
                ..
            }))
        ));
    }

    #[test]
    fn each_page_bound_of_the_core_is_refused_as_the_page() {
        for (offset, limit) in [(-1.0, 50.0), (0.0, 0.0), (0.0, 201.0), (400.0, 101.0)] {
            assert_eq!(
                refused_field(task_list_query(list(offset, limit))),
                QueryField::Page,
                "offset {offset}, limit {limit}"
            );
        }
    }

    #[test]
    fn a_breakdown_limit_outside_1_to_500_is_refused_as_the_limit() {
        for limit in [0.0, 501.0] {
            let arg = TaskBreakdownArg {
                window: window(WindowBoundArg::CoreDefault, WindowBoundArg::CoreDefault),
                group_by: GroupByArg::Queue,
                filters: no_filters(),
                limit: CountArg::Set { value: limit },
            };
            assert_eq!(refused_field(task_breakdown_query(arg)), QueryField::Limit);
        }
    }

    #[test]
    fn a_runs_limit_outside_1_to_200_is_refused_as_the_limit() {
        for limit in [0.0, 201.0] {
            let arg = WorkflowRunsArg {
                name: RunNameArg::AnyName,
                status: RunStatusArg::AnyStatus,
                limit: CountArg::Set { value: limit },
            };
            assert_eq!(refused_field(workflow_runs_query(arg)), QueryField::Limit);
        }
    }

    #[test]
    fn a_count_that_is_not_a_whole_number_is_a_defect() {
        for refused in [2.5, f64::NAN, f64::INFINITY, 9_007_199_254_740_992.0] {
            assert!(matches!(
                task_list_query(list(refused, 50.0)),
                Err(QueryStop::Defect(_))
            ));
        }
    }

    #[test]
    fn a_status_text_that_is_not_a_task_status_is_a_defect() {
        let mut arg = list(0.0, 50.0);
        arg.filters.statuses = vec!["DONE".to_owned()];
        assert_eq!(
            task_list_query(arg).err(),
            Some(QueryStop::Defect(BoundaryRefusal::new(
                "statuses",
                TASK_STATUS_RULE
            )))
        );
    }

    #[test]
    fn statuses_are_read_by_their_wire_values() {
        assert_eq!(
            task_statuses(&["PENDING".to_owned(), "FAILED".to_owned()]),
            Ok(vec![TaskStatus::Pending, TaskStatus::Failed])
        );
    }

    #[test]
    fn each_sort_field_of_the_core_has_one_argument() {
        let fields = [
            SortFieldArg::EnqueuedAt,
            SortFieldArg::StartedAt,
            SortFieldArg::CompletedAt,
            SortFieldArg::FailedAt,
            SortFieldArg::Status,
            SortFieldArg::TaskName,
            SortFieldArg::QueueName,
            SortFieldArg::Priority,
            SortFieldArg::RetryCount,
            SortFieldArg::QueueSeconds,
            SortFieldArg::ExecutionSeconds,
        ];
        let mapped: Vec<TaskSortField> = fields.into_iter().map(sort_field).collect();
        assert_eq!(mapped, TaskSortField::ALL.to_vec());
    }

    #[test]
    fn each_error_category_of_the_core_has_one_argument() {
        let categories = [
            ErrorCategoryArg::Operational,
            ErrorCategoryArg::Contract,
            ErrorCategoryArg::Retrieval,
            ErrorCategoryArg::Outcome,
            ErrorCategoryArg::Domain,
        ];
        assert_eq!(error_categories(&categories), ErrorCategory::ALL.to_vec());
    }

    #[test]
    fn each_group_column_and_direction_of_the_core_has_one_argument() {
        let groups = [GroupByArg::Worker, GroupByArg::TaskName, GroupByArg::Queue];
        let mapped: Vec<TaskGroupBy> = groups.into_iter().map(group_column).collect();
        assert_eq!(mapped, TaskGroupBy::ALL.to_vec());
        let directions = [SortDirectionArg::Ascending, SortDirectionArg::Descending];
        let mapped: Vec<SortDirection> = directions.into_iter().map(sort_direction).collect();
        assert_eq!(mapped, SortDirection::ALL.to_vec());
    }
}
