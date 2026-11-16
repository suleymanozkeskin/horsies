//! Status counts, facets, breakdown and the error-category filter.
//!
//! Python specification: `tests/integration/test_monitoring_queries.py`
//! (`TestTaskStats`, `TestTaskFacets`, `TestTaskBreakdown`,
//! `TestErrorCategoryFilter`), baseline version on `program/0.8.0`. Each test
//! runs on a database of its own ([`SpecDatabase`]).

use std::collections::{BTreeMap, BTreeSet};

use serial_test::serial;
use sqlx::PgPool;

use super::spec_support::{
    seed_failures, seed_live, seed_terminal, set_history_error_code, window, Ending, LiveTask,
    SpecDatabase,
};
use crate::broker::PostgresBroker;
use crate::core::task::error::{ContractCode, OperationalErrorCode, OutcomeCode, RetrievalCode};
use crate::monitoring::{
    list_tasks, task_breakdown, task_facets, task_stats, ErrorCategory, TaskBreakdownQuery,
    TaskFacetsQuery, TaskFilters, TaskGroupBy, TaskListQuery, TaskStatsQuery, TaskSummary,
};

/// The status cards in their fixed order (Python `_STATUS_ORDER`).
const STATUS_ORDER: [&str; 7] = [
    "PENDING",
    "CLAIMED",
    "RUNNING",
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "EXPIRED",
];

/// The cap of the error-code facet list (Python `_ERROR_FACET_CAP`).
const ERROR_FACET_CAP: usize = 30;

/// One sample code per family, and a user code for DOMAIN (Python
/// `_FAMILY_SAMPLES`).
const FAMILY_SAMPLES: [(ErrorCategory, &str); 5] = [
    (ErrorCategory::Operational, "TASK_EXCEPTION"),
    (ErrorCategory::Contract, "RETURN_TYPE_MISMATCH"),
    (ErrorCategory::Retrieval, "WAIT_TIMEOUT"),
    (ErrorCategory::Outcome, "TASK_CANCELLED"),
    (ErrorCategory::Domain, "PAYMENT_DECLINED"),
];

const BUILT_IN_FAMILIES: [ErrorCategory; 4] = [
    ErrorCategory::Operational,
    ErrorCategory::Contract,
    ErrorCategory::Retrieval,
    ErrorCategory::Outcome,
];

async fn facets_of(broker: &PostgresBroker, query: &TaskFacetsQuery) -> crate::monitoring::Facets {
    task_facets(broker, query).await.expect("read the facets")
}

fn counts_of(values: &[crate::monitoring::FacetValue]) -> BTreeMap<String, i64> {
    values
        .iter()
        .map(|facet| (facet.value.clone(), facet.count))
        .collect()
}

fn category_totals(pairs: &[(ErrorCategory, i64)]) -> BTreeMap<String, i64> {
    pairs
        .iter()
        .map(|(category, total)| (category.as_str().to_owned(), *total))
        .collect()
}

/// `values` in the order of the database collation.
async fn collated(pool: &PgPool, values: &[String]) -> Vec<String> {
    sqlx::query_scalar("SELECT value FROM unnest($1::text[]) AS value ORDER BY value")
        .bind(values)
        .fetch_all(pool)
        .await
        .expect("sort the values in the database")
}

/// The rows a category selection matches, the other dimensions open
/// (Python `rows_by_category`).
async fn rows_by_category(
    broker: &PostgresBroker,
    categories: Vec<ErrorCategory>,
    queues: Vec<String>,
    error_codes: Vec<String>,
) -> Vec<TaskSummary> {
    let query = TaskListQuery::new(window())
        .with_filters(TaskFilters {
            queues,
            error_codes,
            error_categories: categories,
            ..TaskFilters::default()
        })
        .with_pagination(0, 200)
        .expect("a page of 200 rows is accepted");
    list_tasks(broker, &query)
        .await
        .expect("list the tasks")
        .rows
}

/// One failed task per family, each with the family's sample code.
async fn seed_one_failure_per_family(pool: &PgPool) {
    let codes: Vec<String> = FAMILY_SAMPLES
        .iter()
        .map(|(_, code)| (*code).to_owned())
        .collect();
    seed_failures(pool, "family_sample_", &codes).await;
}

/// Python `TestTaskStats::test_zero_fills_all_statuses_in_fixed_order`.
#[tokio::test]
#[serial]
async fn stats_zero_fill_every_status_in_a_fixed_order() {
    let database = SpecDatabase::create().await;

    let stats = task_stats(&database.broker(), &TaskStatsQuery::new(window()))
        .await
        .expect("read the stats");

    let statuses: Vec<&str> = stats.iter().map(|row| row.status.as_str()).collect();
    assert_eq!(statuses, STATUS_ORDER);
    assert!(stats.iter().all(|row| row.count == 0), "{stats:?}");
    database.drop().await;
}

/// Python `TestTaskFacets::test_reports_distinct_values_with_counts`: the
/// task-name and the queue facets count each distinct value.
#[tokio::test]
#[serial]
async fn facets_count_each_distinct_task_name_and_queue() {
    let database = SpecDatabase::create().await;
    for task in [
        LiveTask::pending("alpha_task").with_worker(Some("w1")),
        LiveTask::pending("alpha_task")
            .on_queue("fast")
            .with_worker(Some("w1")),
        LiveTask::pending("beta_task").on_queue("fast"),
    ] {
        seed_live(&database.pool, &task).await;
    }

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    assert_eq!(
        counts_of(&facets.task_names),
        BTreeMap::from([("alpha_task".to_owned(), 2), ("beta_task".to_owned(), 1)])
    );
    assert_eq!(
        counts_of(&facets.queues),
        BTreeMap::from([("default".to_owned(), 1), ("fast".to_owned(), 2)])
    );
    database.drop().await;
}

/// Python `TestTaskFacets::test_ties_follow_the_database_collation`: values
/// with equal counts come in the order of the database collation, for the
/// task names and for the codes.
#[tokio::test]
#[serial]
async fn facet_ties_follow_the_database_collation() {
    let database = SpecDatabase::create().await;
    let names: Vec<String> = ["b_task", "B_task", "a_task", "A_task", "_task", "Z_task"]
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let codes: Vec<String> = names.iter().map(|name| format!("DOMAIN_{name}")).collect();
    for (name, code) in names.iter().zip(&codes) {
        seed_terminal(
            &database.pool,
            &LiveTask::pending(name),
            Ending::failed(code),
        )
        .await;
    }

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    let task_names: Vec<String> = facets
        .task_names
        .iter()
        .map(|facet| facet.value.clone())
        .collect();
    let error_codes: Vec<String> = facets
        .error_codes
        .iter()
        .map(|facet| facet.value.clone())
        .collect();
    assert_eq!(task_names, collated(&database.pool, &names).await);
    assert_eq!(error_codes, collated(&database.pool, &codes).await);
    database.drop().await;
}

/// Python `TestTaskFacets::test_the_result_stays_bounded_with_many_distinct_codes`:
/// an error code is free text, so the answer does not grow with the number
/// of codes: 1,000 distinct codes give 30 code facets, and the category
/// total counts all of them.
#[tokio::test]
#[serial]
async fn the_code_facets_stay_bounded_with_a_thousand_distinct_codes() {
    let database = SpecDatabase::create().await;
    let codes: Vec<String> = (0..1_000).map(|index| format!("DOMAIN_{index}")).collect();
    seed_failures(&database.pool, "many_codes_", &codes).await;

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    assert_eq!(facets.error_codes.len(), ERROR_FACET_CAP);
    assert_eq!(
        facets.error_category_totals,
        category_totals(&[(ErrorCategory::Domain, 1_000)])
    );
    database.drop().await;
}

/// Python `TestTaskFacets::test_unclaimed_tasks_are_absent_from_the_worker_facet`.
#[tokio::test]
#[serial]
async fn an_unclaimed_task_is_not_in_the_worker_facet() {
    let database = SpecDatabase::create().await;
    seed_live(
        &database.pool,
        &LiveTask::pending("alpha_task").with_worker(Some("worker-1")),
    )
    .await;
    seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    assert_eq!(
        counts_of(&facets.workers),
        BTreeMap::from([("worker-1".to_owned(), 1)])
    );
    database.drop().await;
}

/// Python `TestTaskFacets::test_null_and_empty_error_codes_are_excluded`.
#[tokio::test]
#[serial]
async fn null_and_empty_error_codes_are_not_facet_values() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::failed("BOOM"),
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::failed(""),
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::Completed,
    )
    .await;
    seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    let codes: Vec<&str> = facets
        .error_codes
        .iter()
        .map(|facet| facet.value.as_str())
        .collect();
    assert_eq!(codes, ["BOOM"]);
    database.drop().await;
}

/// Python `TestTaskFacets::test_error_codes_on_completed_tasks_are_counted`:
/// the facet counts tasks that carry a code, not failures.
#[tokio::test]
#[serial]
async fn an_error_code_on_a_completed_task_is_counted() {
    let database = SpecDatabase::create().await;
    let completed = LiveTask::pending("alpha_task");
    seed_terminal(&database.pool, &completed, Ending::Completed).await;
    set_history_error_code(&database.pool, completed.id, Some("SOFT_FAIL")).await;

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    let codes: Vec<&str> = facets
        .error_codes
        .iter()
        .map(|facet| facet.value.as_str())
        .collect();
    assert_eq!(codes, ["SOFT_FAIL"]);
    database.drop().await;
}

/// Python `TestTaskFacets::test_error_codes_carry_their_taxonomy_category`:
/// each code facet carries its category, and the totals count per category.
#[tokio::test]
#[serial]
async fn each_code_facet_carries_its_category() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::failed("TASK_EXCEPTION"),
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::Cancelled,
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::failed("MY_DOMAIN_ERROR"),
    )
    .await;

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    let categories: BTreeMap<&str, &str> = facets
        .error_codes
        .iter()
        .map(|facet| (facet.value.as_str(), facet.category.as_str()))
        .collect();
    assert_eq!(
        categories,
        BTreeMap::from([
            ("MY_DOMAIN_ERROR", "DOMAIN"),
            ("TASK_CANCELLED", "OUTCOME"),
            ("TASK_EXCEPTION", "OPERATIONAL"),
        ])
    );
    assert_eq!(
        facets.error_category_totals,
        category_totals(&[
            (ErrorCategory::Operational, 1),
            (ErrorCategory::Outcome, 1),
            (ErrorCategory::Domain, 1),
        ])
    );
    database.drop().await;
}

/// The codes `HISTORY_DOMAIN_000` to `HISTORY_DOMAIN_200`.
fn two_hundred_and_one_domain_codes() -> Vec<String> {
    (0..201)
        .map(|index| format!("HISTORY_DOMAIN_{index:03}"))
        .collect()
}

/// Python `TestTaskFacets::test_selected_error_family_is_ranked_before_its_code_cap`:
/// a selected family filters the codes before the cap, so a family code
/// that ranks after 201 other codes is still listed.
#[tokio::test]
#[serial]
async fn a_selected_family_is_filtered_before_its_code_cap() {
    let database = SpecDatabase::create().await;
    seed_failures(
        &database.pool,
        "history_domain_",
        &two_hundred_and_one_domain_codes(),
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("cancelled_task"),
        Ending::Cancelled,
    )
    .await;

    let facets = facets_of(
        &database.broker(),
        &TaskFacetsQuery::new(window()).with_error_categories(vec![ErrorCategory::Outcome]),
    )
    .await;

    let codes: Vec<&str> = facets
        .error_codes
        .iter()
        .map(|facet| facet.value.as_str())
        .collect();
    assert_eq!(codes, ["TASK_CANCELLED"]);
    database.drop().await;
}

/// Python `TestTaskFacets::test_history_category_totals_exceed_the_history_facet_cap`.
#[tokio::test]
#[serial]
async fn category_totals_count_codes_past_the_facet_cap() {
    let database = SpecDatabase::create().await;
    seed_failures(
        &database.pool,
        "history_domain_",
        &two_hundred_and_one_domain_codes(),
    )
    .await;

    let facets = facets_of(&database.broker(), &TaskFacetsQuery::new(window())).await;

    assert_eq!(facets.error_codes.len(), ERROR_FACET_CAP);
    assert_eq!(
        facets.error_category_totals,
        category_totals(&[(ErrorCategory::Domain, 201)])
    );
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_groups_by_worker_with_rollup_total`:
/// groups by worker with a count per status and of retried tasks, and a
/// `TOTAL` row over every group.
#[tokio::test]
#[serial]
async fn the_worker_breakdown_counts_each_status_the_retried_tasks_and_a_total() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task").with_worker(Some("w1")),
        Ending::Completed,
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task")
            .with_worker(Some("w1"))
            .with_retry_count(1),
        Ending::failed("TASK_EXCEPTION"),
    )
    .await;
    seed_live(&database.pool, &LiveTask::running("alpha_task", "w2")).await;

    let breakdown = task_breakdown(
        &database.broker(),
        &TaskBreakdownQuery::new(window(), TaskGroupBy::Worker),
    )
    .await
    .expect("read the breakdown");

    let groups: BTreeMap<&str, &crate::monitoring::GroupRow> = breakdown
        .groups
        .iter()
        .map(|row| (row.group.as_str(), row))
        .collect();
    assert_eq!(groups["w1"].total, 2);
    assert_eq!(groups["w1"].completed, 1);
    assert_eq!(groups["w1"].failed, 1);
    assert_eq!(groups["w1"].retried, 1);
    assert_eq!(groups["w2"].running, 1);
    assert_eq!(breakdown.total.group, "TOTAL");
    assert_eq!(breakdown.total.total, 3);
    assert_eq!(breakdown.group_count, 2);
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_empty_scope_yields_no_groups_and_a_zero_total`.
#[tokio::test]
#[serial]
async fn an_empty_scope_gives_no_groups_and_a_zero_total() {
    let database = SpecDatabase::create().await;

    let breakdown = task_breakdown(
        &database.broker(),
        &TaskBreakdownQuery::new(window(), TaskGroupBy::Worker),
    )
    .await
    .expect("read the breakdown");

    assert!(breakdown.groups.is_empty(), "{:?}", breakdown.groups);
    assert_eq!(breakdown.group_count, 0);
    assert_eq!(breakdown.total.group, "TOTAL");
    assert_eq!(breakdown.total.total, 0);
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_null_group_key_is_labelled_unknown`.
#[tokio::test]
#[serial]
async fn a_task_with_no_group_key_is_grouped_as_unknown() {
    let database = SpecDatabase::create().await;
    seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;

    let breakdown = task_breakdown(
        &database.broker(),
        &TaskBreakdownQuery::new(window(), TaskGroupBy::Worker),
    )
    .await
    .expect("read the breakdown");

    let groups: Vec<&str> = breakdown
        .groups
        .iter()
        .map(|row| row.group.as_str())
        .collect();
    assert_eq!(groups, ["unknown"]);
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_filters_apply_to_groups_and_total`: a row
/// outside the filter is in no group and not in the total.
#[tokio::test]
#[serial]
async fn the_filters_scope_the_groups_and_the_total() {
    let database = SpecDatabase::create().await;
    seed_live(
        &database.pool,
        &LiveTask::pending("alpha_task").with_worker(Some("w1")),
    )
    .await;
    seed_live(
        &database.pool,
        &LiveTask::pending("beta_task").with_worker(Some("w2")),
    )
    .await;

    let breakdown = task_breakdown(
        &database.broker(),
        &TaskBreakdownQuery::new(window(), TaskGroupBy::Worker).with_filters(TaskFilters {
            task_names: vec!["alpha_task".to_owned()],
            ..TaskFilters::default()
        }),
    )
    .await
    .expect("read the breakdown");

    let groups: Vec<&str> = breakdown
        .groups
        .iter()
        .map(|row| row.group.as_str())
        .collect();
    assert_eq!(groups, ["w1"]);
    assert_eq!(breakdown.total.total, 1);
    database.drop().await;
}

/// Python `TestErrorCategoryFilter::test_a_family_selects_exactly_its_own_codes`
/// (every family, also CONTRACT, RETRIEVAL and DOMAIN).
#[tokio::test]
#[serial]
#[ignore = "waits on #842"]
async fn each_family_selects_exactly_its_own_codes() {
    let database = SpecDatabase::create().await;
    seed_one_failure_per_family(&database.pool).await;
    let broker = database.broker();

    for (category, sample) in FAMILY_SAMPLES {
        let rows = rows_by_category(&broker, vec![category], Vec::new(), Vec::new()).await;

        let codes: Vec<Option<&str>> = rows.iter().map(|row| row.error_code.as_deref()).collect();
        assert_eq!(codes, [Some(sample)], "{category}");
    }
    database.drop().await;
}

/// Python `TestErrorCategoryFilter::test_a_user_defined_code_is_domain_and_no_family_claims_it`.
#[tokio::test]
#[serial]
#[ignore = "waits on #842"]
async fn a_user_code_is_domain_and_no_built_in_family_selects_it() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::failed("PAYMENT_DECLINED"),
    )
    .await;
    let broker = database.broker();

    let domain =
        rows_by_category(&broker, vec![ErrorCategory::Domain], Vec::new(), Vec::new()).await;

    let codes: Vec<Option<&str>> = domain.iter().map(|row| row.error_code.as_deref()).collect();
    assert_eq!(codes, [Some("PAYMENT_DECLINED")]);
    assert_eq!(domain[0].error_category.as_deref(), Some("DOMAIN"));
    for family in BUILT_IN_FAMILIES {
        let rows = rows_by_category(&broker, vec![family], Vec::new(), Vec::new()).await;
        assert!(rows.is_empty(), "{family}: {rows:?}");
    }
    database.drop().await;
}

/// Python `TestErrorCategoryFilter::test_absent_and_empty_codes_match_no_category`:
/// DOMAIN means "carries a code the library does not define", not "no code".
#[tokio::test]
#[serial]
#[ignore = "waits on #842"]
async fn absent_and_empty_codes_match_no_category() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::Completed,
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::failed(""),
    )
    .await;
    let broker = database.broker();

    for category in ErrorCategory::ALL {
        let rows = rows_by_category(&broker, vec![category], Vec::new(), Vec::new()).await;
        assert!(rows.is_empty(), "{category}: {rows:?}");
    }
    database.drop().await;
}

/// Python `TestErrorCategoryFilter::test_categories_are_or_combined`.
#[tokio::test]
#[serial]
#[ignore = "waits on #842"]
async fn two_categories_select_the_codes_of_either() {
    let database = SpecDatabase::create().await;
    seed_one_failure_per_family(&database.pool).await;

    let rows = rows_by_category(
        &database.broker(),
        vec![ErrorCategory::Contract, ErrorCategory::Domain],
        Vec::new(),
        Vec::new(),
    )
    .await;

    let codes: BTreeSet<Option<&str>> = rows.iter().map(|row| row.error_code.as_deref()).collect();
    assert_eq!(
        codes,
        BTreeSet::from([Some("PAYMENT_DECLINED"), Some("RETURN_TYPE_MISMATCH")])
    );
    database.drop().await;
}

/// Every built-in code, family by family.
fn built_in_codes() -> Vec<String> {
    OperationalErrorCode::ALL
        .iter()
        .map(ToString::to_string)
        .chain(ContractCode::ALL.iter().map(ToString::to_string))
        .chain(RetrievalCode::ALL.iter().map(ToString::to_string))
        .chain(OutcomeCode::ALL.iter().map(ToString::to_string))
        .collect()
}

/// Python `TestErrorCategoryFilter::test_every_built_in_code_is_reachable_from_the_category_it_reports`:
/// the category a row reports and the filter of that category are one
/// mapping, so a row is reachable from the category it shows.
#[tokio::test]
#[serial]
#[ignore = "waits on #842"]
async fn every_built_in_code_is_selected_by_the_category_it_reports() {
    let database = SpecDatabase::create().await;
    let codes = built_in_codes();
    seed_failures(&database.pool, "built_in_", &codes).await;
    let broker = database.broker();

    let reported: BTreeMap<String, Option<String>> =
        rows_by_category(&broker, Vec::new(), Vec::new(), Vec::new())
            .await
            .into_iter()
            .filter_map(|row| row.error_code.map(|code| (code, row.error_category)))
            .collect();

    assert_eq!(reported.len(), codes.len());
    for category in ErrorCategory::ALL {
        let matched: BTreeSet<String> =
            rows_by_category(&broker, vec![category], Vec::new(), Vec::new())
                .await
                .into_iter()
                .filter_map(|row| row.error_code)
                .collect();
        let expected: BTreeSet<String> = reported
            .iter()
            .filter(|(_, reported_category)| {
                reported_category.as_deref() == Some(category.as_str())
            })
            .map(|(code, _)| code.clone())
            .collect();
        assert_eq!(matched, expected, "{category}");
    }
    database.drop().await;
}

/// Python `TestErrorCategoryFilter::test_stats_and_breakdown_apply_the_same_expansion`.
#[tokio::test]
#[serial]
#[ignore = "waits on #842"]
async fn stats_and_breakdown_expand_a_category_the_same_way() {
    let database = SpecDatabase::create().await;
    seed_one_failure_per_family(&database.pool).await;
    let broker = database.broker();
    let outcome_only = TaskFilters {
        error_categories: vec![ErrorCategory::Outcome],
        ..TaskFilters::default()
    };

    let stats = task_stats(
        &broker,
        &TaskStatsQuery::new(window()).with_filters(outcome_only.clone()),
    )
    .await
    .expect("read the stats");
    let breakdown = task_breakdown(
        &broker,
        &TaskBreakdownQuery::new(window(), TaskGroupBy::TaskName).with_filters(outcome_only),
    )
    .await
    .expect("read the breakdown");

    assert_eq!(stats.iter().map(|row| row.count).sum::<i64>(), 1);
    assert_eq!(breakdown.total.total, 1);
    database.drop().await;
}
