//! Status counts, facets, breakdown and the error-category filter.
//!
//! Python specification: `clients/python/tests/integration/test_monitoring_queries.py`
//! (`TestTaskStats`, `TestTaskFacets`, `TestTaskBreakdown`,
//! `TestErrorCategoryFilter`), baseline version on `program/0.8.0`. Each test
//! runs on a database of its own ([`SpecDatabase`]).

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};

use super::spec_support::{
    seed_failures, seed_live, seed_terminal, set_history_error_code, window, Ending, LiveTask,
    SpecDatabase,
};
use crate::broker::lifecycle_tests::support::{single_session_pool, WireProxy};
use crate::broker::PostgresBroker;
use crate::core::history::reads::pages::HistoryWindow;
use crate::core::task::error::{ContractCode, OperationalCode, OutcomeCode, RetrievalCode};
use crate::monitoring::{
    list_tasks, task_breakdown, task_facets, task_stats, Breakdown, BreakdownGroup, ErrorCategory,
    TaskBreakdownQuery, TaskFacetsQuery, TaskFilters, TaskGroupBy, TaskListQuery, TaskStatsQuery,
    TaskSummary,
};
use crate::test_support::history::{covered_history_instant, stored_class_key, SeedInstant};
use crate::test_support::sql::{self, TestPool};

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
async fn collated(pool: &TestPool, values: &[String]) -> Vec<String> {
    sql::query_scalar("SELECT value FROM unnest($1::text[]) AS value ORDER BY value")
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

/// The group that holds `value`.
fn value_group(value: &str) -> BreakdownGroup {
    BreakdownGroup::Value {
        value: value.to_owned(),
    }
}

/// The worker breakdown over the test window (Python `_worker_breakdown`).
async fn worker_breakdown(broker: &PostgresBroker) -> Breakdown {
    task_breakdown(
        broker,
        &TaskBreakdownQuery::new(window(), TaskGroupBy::Worker),
    )
    .await
    .expect("read the breakdown")
}

/// Each group of a breakdown with its total, in order.
fn groups_with_totals(breakdown: &Breakdown) -> Vec<(BreakdownGroup, i64)> {
    breakdown
        .groups
        .iter()
        .map(|row| (row.group.clone(), row.counts.total))
        .collect()
}

/// One failed task per family, each with the family's sample code.
async fn seed_one_failure_per_family(pool: &TestPool) {
    let codes: Vec<String> = FAMILY_SAMPLES
        .iter()
        .map(|(_, code)| (*code).to_owned())
        .collect();
    seed_failures(pool, "family_sample_", &codes).await;
}

/// Python `TestTaskStats::test_zero_fills_all_statuses_in_fixed_order`.
#[tokio::test]
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

/// Python `TestTaskFacets::test_a_poll_runs_one_statement_for_both_sides`:
/// one facets read is one round trip, for the live and the history side.
/// The read runs through a proxy that counts round trips. A first read
/// opens the connection and prepares the statement, so the counted read is
/// the steady state of the statement cache; it runs within the idle ping
/// limit of the pool, so no ping is sent before it.
#[tokio::test]
async fn a_facets_poll_is_one_round_trip_for_both_sides() {
    let database = SpecDatabase::create().await;
    seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("beta_task"),
        Ending::failed("BOOM"),
    )
    .await;
    let upstream = database.pool.url();
    let proxy = WireProxy::start(upstream).await;
    let proxied = single_session_pool(&proxy.url(upstream)).await;
    let broker = PostgresBroker::from_test_url(proxied.url(), crate::ClientFamily::Rust);
    let query = TaskFacetsQuery::new(window());
    let _warm_up = facets_of(&broker, &query).await;

    proxy.reset_round_trips();
    let facets = facets_of(&broker, &query).await;
    let round_trips = proxy.round_trips();

    assert_eq!(round_trips, 1);
    assert_eq!(
        counts_of(&facets.task_names),
        BTreeMap::from([("alpha_task".to_owned(), 1), ("beta_task".to_owned(), 1)])
    );
    proxied.close().await;
    database.drop().await;
}

/// Python `TestTaskFacets::test_ties_follow_the_database_collation`: values
/// with equal counts come in the order of the database collation, for the
/// task names and for the codes.
#[tokio::test]
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
/// total over every group.
#[tokio::test]
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

    let breakdown = worker_breakdown(&database.broker()).await;

    let groups: BTreeMap<String, &crate::monitoring::BreakdownCounts> = breakdown
        .groups
        .iter()
        .filter_map(|row| match &row.group {
            BreakdownGroup::Value { value } => Some((value.clone(), &row.counts)),
            BreakdownGroup::Unclaimed => None,
        })
        .collect();
    assert_eq!(groups.len(), breakdown.groups.len(), "{breakdown:?}");
    assert_eq!(groups["w1"].total, 2);
    assert_eq!(groups["w1"].completed, 1);
    assert_eq!(groups["w1"].failed, 1);
    assert_eq!(groups["w1"].retried, 1);
    assert_eq!(groups["w2"].running, 1);
    assert_eq!(breakdown.total.total, 3);
    assert_eq!(breakdown.group_count, 2);
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_empty_scope_yields_no_groups_and_a_zero_total`.
#[tokio::test]
async fn an_empty_scope_gives_no_groups_and_a_zero_total() {
    let database = SpecDatabase::create().await;

    let breakdown = worker_breakdown(&database.broker()).await;

    assert!(breakdown.groups.is_empty(), "{:?}", breakdown.groups);
    assert_eq!(breakdown.group_count, 0);
    assert_eq!(breakdown.total.total, 0);
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_tasks_no_worker_claimed_are_the_unclaimed_group`:
/// a live task and a history task that no worker claimed are one group.
#[tokio::test]
async fn the_tasks_no_worker_claimed_are_the_unclaimed_group() {
    let database = SpecDatabase::create().await;
    seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::Cancelled,
    )
    .await;

    let breakdown = worker_breakdown(&database.broker()).await;

    assert_eq!(
        groups_with_totals(&breakdown),
        [(BreakdownGroup::Unclaimed, 2)]
    );
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_a_worker_named_unknown_is_a_group_apart_from_the_unclaimed_tasks`.
#[tokio::test]
async fn a_worker_named_unknown_is_a_group_apart_from_the_unclaimed_tasks() {
    let database = SpecDatabase::create().await;
    seed_live(&database.pool, &LiveTask::running("alpha_task", "unknown")).await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task").with_worker(Some("unknown")),
        Ending::Completed,
    )
    .await;
    seed_live(&database.pool, &LiveTask::pending("alpha_task")).await;

    let breakdown = worker_breakdown(&database.broker()).await;

    assert_eq!(
        groups_with_totals(&breakdown),
        [(value_group("unknown"), 2), (BreakdownGroup::Unclaimed, 1)]
    );
    assert_eq!(breakdown.group_count, 2);
    assert_eq!(
        breakdown.total.total,
        breakdown
            .groups
            .iter()
            .map(|row| row.counts.total)
            .sum::<i64>()
    );
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_with_one_total_a_worker_named_unknown_comes_before_the_unclaimed_group`:
/// the unclaimed group sorts as the text `unknown`, after a value group
/// `unknown` with the same total.
#[tokio::test]
async fn with_one_total_a_worker_named_unknown_comes_before_the_unclaimed_group() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task"),
        Ending::Completed,
    )
    .await;
    for worker in ["unknown", "a-worker", "zz-worker"] {
        seed_live(&database.pool, &LiveTask::running("alpha_task", worker)).await;
    }

    let breakdown = worker_breakdown(&database.broker()).await;

    let groups: Vec<BreakdownGroup> = breakdown
        .groups
        .iter()
        .map(|row| row.group.clone())
        .collect();
    assert_eq!(
        groups,
        [
            value_group("a-worker"),
            value_group("unknown"),
            BreakdownGroup::Unclaimed,
            value_group("zz-worker"),
        ]
    );
    database.drop().await;
}

/// Python `TestTaskBreakdown::test_filters_apply_to_groups_and_total`: a row
/// outside the filter is in no group and not in the total.
#[tokio::test]
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

    let groups: Vec<BreakdownGroup> = breakdown
        .groups
        .iter()
        .map(|row| row.group.clone())
        .collect();
    assert_eq!(groups, [value_group("w1")]);
    assert_eq!(breakdown.total.total, 1);
    database.drop().await;
}

/// Python `TestErrorCategoryFilter::test_a_family_selects_exactly_its_own_codes`
/// (every family, also CONTRACT, RETRIEVAL and DOMAIN).
#[tokio::test]
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
    OperationalCode::ALL
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

/// Each group column of the breakdown (worker, task name, queue) counts the
/// retried tasks of its groups and of the total.
#[tokio::test]
async fn each_group_column_counts_the_retried_tasks() {
    let database = SpecDatabase::create().await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("alpha_task")
            .with_worker(Some("w1"))
            .with_retry_count(2),
        Ending::failed("TASK_EXCEPTION"),
    )
    .await;
    seed_terminal(
        &database.pool,
        &LiveTask::pending("beta_task")
            .on_queue("other")
            .with_worker(Some("w2")),
        Ending::Completed,
    )
    .await;
    let broker = database.broker();

    for group_by in TaskGroupBy::ALL {
        let breakdown = task_breakdown(&broker, &TaskBreakdownQuery::new(window(), group_by))
            .await
            .unwrap_or_else(|error| panic!("{group_by:?}: read the breakdown: {error:?}"));

        assert_eq!(breakdown.group_count, 2, "{group_by:?}: {breakdown:?}");
        assert_eq!(breakdown.total.total, 2, "{group_by:?}: {breakdown:?}");
        assert_eq!(breakdown.total.retried, 1, "{group_by:?}: {breakdown:?}");
        let mut retried: Vec<i64> = breakdown
            .groups
            .iter()
            .map(|row| row.counts.retried)
            .collect();
        retried.sort_unstable();
        assert_eq!(retried, [0, 1], "{group_by:?}: {breakdown:?}");
    }
    database.drop().await;
}

/// The history window holds its lower bound and not its upper bound: a row
/// whose terminal time is the lower bound is read, one a microsecond before
/// it and one at the upper bound are not, in the list, the stats, the facets
/// and the breakdown.
#[tokio::test]
async fn the_history_window_holds_its_lower_bound_and_not_its_upper_bound() {
    let database = SpecDatabase::create().await;
    let at_lower = LiveTask::pending("at_lower_task");
    let before_lower = LiveTask::pending("before_lower_task");
    let at_upper = LiveTask::pending("at_upper_task");
    for task in [&at_lower, &before_lower, &at_upper] {
        seed_terminal(&database.pool, task, Ending::Completed).await;
    }
    let lower: DateTime<Utc> =
        sql::query_scalar("SELECT terminal_at FROM horsies_task_history WHERE task_id = $1")
            .bind(at_lower.id)
            .fetch_one(&database.pool)
            .await
            .expect("read the terminal time of the lower row");
    let upper = lower + Duration::hours(1);
    for (task_id, terminal_at) in [
        (before_lower.id, lower - Duration::microseconds(1)),
        (at_upper.id, upper),
    ] {
        let class_key = stored_class_key(&database.pool, task_id).await;
        let terminal_at =
            covered_history_instant(&database.pool, &class_key, SeedInstant::At(terminal_at)).await;
        sql::query("UPDATE horsies_task_history SET terminal_at = $2 WHERE task_id = $1")
            .bind(task_id)
            .bind(terminal_at.at())
            .execute(&database.pool)
            .await
            .expect("date the history row");
    }
    let window = HistoryWindow::new(lower, upper).expect("a window of one hour");
    let names = TaskFilters {
        task_names: vec![
            at_lower.task_name.clone(),
            before_lower.task_name.clone(),
            at_upper.task_name.clone(),
        ],
        ..TaskFilters::default()
    };
    let broker = database.broker();

    let list = list_tasks(
        &broker,
        &TaskListQuery::new(window).with_filters(names.clone()),
    )
    .await
    .expect("read the list");
    let stats = task_stats(
        &broker,
        &TaskStatsQuery::new(window).with_filters(names.clone()),
    )
    .await
    .expect("read the stats");
    let facets = facets_of(&broker, &TaskFacetsQuery::new(window)).await;
    let breakdown = task_breakdown(
        &broker,
        &TaskBreakdownQuery::new(window, TaskGroupBy::TaskName).with_filters(names),
    )
    .await
    .expect("read the breakdown");

    assert_eq!(list.total, 1);
    assert_eq!(
        list.rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        [at_lower.id]
    );
    assert_eq!(stats.iter().map(|row| row.count).sum::<i64>(), 1);
    assert_eq!(
        counts_of(&facets.task_names),
        BTreeMap::from([(at_lower.task_name.clone(), 1)])
    );
    assert_eq!(
        groups_with_totals(&breakdown),
        [(value_group(&at_lower.task_name), 1)]
    );
    database.drop().await;
}
