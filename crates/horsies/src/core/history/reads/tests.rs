//! P4 staged-read parity and disposable-database tests.

use std::collections::HashSet;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::broker::migrations::run_horsies_migrations;
use crate::core::history::archive::attempts::{
    encode_attempt_snapshot, AttemptRecord, StoredAttemptSnapshot,
};
use crate::core::history::archive::versions::{archive_digest, ArchiveDecodeError, ArchiveDomain};
use crate::core::history::commands::{
    CreateDailyHistoryLeaf, DetachExpiredHistoryLeaf, DropDetachedHistoryLeaf, LeafBounds, LeafRef,
};
use crate::core::history::ddl::classes::{
    finite_class_parent_name, register_finite_retention_class, ClassRegistration,
};
use crate::core::history::ddl::runtime_names::daily_leaf_name;
use crate::core::history::errors::HistoryError;
use crate::core::history::maintenance::coverage::{ensure_partition_coverage, CoverageOutcome};
use crate::core::history::names::{LEAF_CATALOG, TASK_HISTORY_PARENT};
use crate::core::history::outcomes::{LeafCreation, LeafDrop};
use crate::core::history::partitions::catalog::{database_now, LeafCatalogRow};
use crate::core::history::partitions::manager::{
    create_daily_leaf, detach_expired_leaf, drop_detached_leaf, NoQuarantine,
};
use crate::core::history::partitions::publication::LoaderPublication;

use super::aggregates::{
    history_breakdown_statement, history_count_statement, history_estimate_statement,
    history_scoped_status_counts_statement, history_status_aggregate_statement,
    plan_rows_from_explain, plan_rows_from_explain_text, HistoryBreakdownGroup,
    HistoryStatusAggregate, HISTORY_NONEMPTY_PROBE_SQL,
};
use super::detail::staged_detail_published;
use super::identity_lookup::{decode_lookup_row, LookupWireRow, TaskIdentityLookup};
use super::lookup_generation::{
    manifest_from_catalog, render_staged_detail_function, render_staged_lookup_function,
    render_staged_provenance_function, LookupLeaf, LookupManifest,
};
use super::pages::{
    history_facet_statement, history_page_statement, history_sort_expression, HistoryBindValue,
    HistoryFacet, HistoryFacetQuery, HistoryPageQuery, HistoryScope, HistorySortField,
    HistoryWindow, HISTORY_SUMMARY_COLUMNS,
};
use super::publisher::{published_manifest_absent_leaves, StagedLoaderPublisher};
use super::{detail::read_task_detail, detail::TaskDetailResult};

const PYTHON_STAGED_FIXTURE: &str =
    include_str!("../../../../tests/fixtures/task_history/python-v052-staged-readers.json");

fn timestamp(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("fixture timestamp")
        .with_timezone(&Utc)
}

fn catalog_row(
    leaf_name: impl Into<String>,
    class_key: impl Into<String>,
    parent_name: impl Into<String>,
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
    min_birth_at: Option<DateTime<Utc>>,
    min_birth_verified: bool,
) -> LeafCatalogRow {
    let leaf_name = leaf_name.into();
    LeafCatalogRow {
        id_index_name: format!("{leaf_name}_task_idx"),
        leaf_name,
        parent_name: parent_name.into(),
        class_key: class_key.into(),
        lower_anchor: lower,
        upper_anchor: upper,
        index_schema_version: 1,
        partition_bound: "FOR VALUES FROM ... TO ...".to_owned(),
        min_birth_at,
        min_birth_verified,
        created_at: lower,
        detached_at: None,
        dropped_at: None,
    }
}

fn fixture_manifest() -> (LookupManifest, Value) {
    let fixture: Value = serde_json::from_str(PYTHON_STAGED_FIXTURE).expect("parse P0 fixture");
    let rows = fixture["fixture"]["catalog_rows"]
        .as_array()
        .expect("catalog rows")
        .iter()
        .map(|row| {
            catalog_row(
                row["leaf_name"].as_str().expect("leaf name"),
                row["class_key"].as_str().expect("class key"),
                row["parent_name"].as_str().expect("parent name"),
                timestamp(row["lower_anchor"].as_str().expect("lower")),
                timestamp(row["upper_anchor"].as_str().expect("upper")),
                row["min_birth_at"].as_str().map(timestamp),
                row["min_birth_verified"]
                    .as_bool()
                    .expect("birth verification"),
            )
        })
        .collect::<Vec<_>>();
    let absent = fixture["fixture"]["absent_relations"]
        .as_array()
        .expect("absent relations")
        .iter()
        .map(|value| value.as_str().expect("absent relation").to_owned())
        .collect::<HashSet<_>>();
    (
        manifest_from_catalog(&rows, &absent).expect("build fixture manifest"),
        fixture,
    )
}

#[test]
fn staged_renderer_is_byte_identical_to_the_python_fixture() {
    let (manifest, fixture) = fixture_manifest();
    assert_eq!(
        fixture["source_commit"].as_str(),
        Some("a43b77808364868797c1d2ee6df0c96695e40122")
    );
    assert_eq!(
        manifest.birth_floor(),
        Some(timestamp("2026-08-08T23:59:59+00:00"))
    );
    assert_eq!(manifest.leaves().len(), 2);
    assert_eq!(
        manifest
            .leaves()
            .iter()
            .map(LookupLeaf::relation_name)
            .collect::<Vec<_>>(),
        vec![
            "horsies_task_history_forever_2026_08_09",
            "horsies_task_history_standard_30d_2026_08_10",
        ]
    );
    assert_eq!(
        render_staged_lookup_function(&manifest),
        fixture["functions"]["lookup"]
            .as_str()
            .expect("lookup body")
    );
    assert_eq!(
        render_staged_provenance_function(&manifest),
        fixture["functions"]["provenance"]
            .as_str()
            .expect("provenance body")
    );
    assert_eq!(
        render_staged_detail_function(&manifest),
        fixture["functions"]["detail"]
            .as_str()
            .expect("detail body")
    );
}

#[test]
fn manifest_validation_order_floor_and_heartbeat_exclusion_are_fail_closed() {
    let base = timestamp("2026-08-01T00:00:00Z");
    assert!(LookupLeaf::new("unsafe;drop", base, base + Duration::days(1), None).is_err());
    assert!(LookupLeaf::new("safe_leaf", base, base, None).is_err());
    let early = catalog_row(
        "history_early",
        "finite_7d_v1",
        "history_parent_a",
        base,
        base + Duration::days(1),
        Some(base - Duration::hours(4)),
        true,
    );
    let late = catalog_row(
        "history_late",
        "finite_30d_v1",
        "history_parent_b",
        base + Duration::days(1),
        base + Duration::days(2),
        Some(base - Duration::hours(1)),
        true,
    );
    let heartbeat = catalog_row(
        "heartbeat_leaf",
        "heartbeats",
        "horsies_heartbeats",
        base,
        base + Duration::hours(1),
        None,
        false,
    );
    let absent = HashSet::from([early.leaf_name.clone()]);
    let manifest = manifest_from_catalog(&[late.clone(), heartbeat, early.clone()], &absent)
        .expect("manifest");
    assert_eq!(manifest.birth_floor(), early.min_birth_at);
    assert_eq!(
        manifest
            .leaves()
            .iter()
            .map(LookupLeaf::relation_name)
            .collect::<Vec<_>>(),
        vec![late.leaf_name.as_str()]
    );
    assert!(manifest_from_catalog(&[late.clone(), late], &HashSet::new()).is_err());
    let unverified = catalog_row(
        "history_unverified",
        "finite_30d_v1",
        "history_parent_b",
        base + Duration::days(2),
        base + Duration::days(3),
        None,
        false,
    );
    assert_eq!(
        manifest_from_catalog(&[early, unverified], &HashSet::new())
            .expect("unverified manifest")
            .birth_floor(),
        None
    );
}

#[test]
fn staged_skeleton_covers_live_likely_fallback_legacy_and_boundary_scale() {
    let base = timestamp("2026-08-01T00:00:00Z");
    let leaves = (0..512)
        .map(|offset| {
            LookupLeaf::new(
                format!("history_leaf_{offset:03}"),
                base + Duration::days(offset),
                base + Duration::days(offset + 1),
                None,
            )
            .expect("lookup leaf")
        })
        .collect();
    let body =
        render_staged_lookup_function(&LookupManifest::new(leaves, None).expect("large manifest"));
    assert_eq!(body.matches("IF v_effective_birth <").count(), 512);
    assert_eq!(body.matches("FROM history_leaf_000\n").count(), 3);
    assert_eq!(body.matches("FROM history_leaf_255\n").count(), 3);
    assert_eq!(body.matches("FROM history_leaf_511\n").count(), 3);
    assert!(body.find("FROM horsies_tasks\n").unwrap() < body.find("history_leaf_000").unwrap());
    assert!(!body.contains("FROM horsies_task_history\n"));
    assert!(body.contains("v_birth_at - INTERVAL '5 seconds'"));
    assert!(body.contains("(get_byte(v_uuid_bytes, 8) & 192) = 128"));
    let empty = render_staged_lookup_function(
        &LookupManifest::new(Vec::new(), None).expect("empty manifest"),
    );
    assert!(empty.contains("FROM horsies_tasks\n"));
    assert!(!empty.contains("uuid_send"));
}

#[test]
fn staged_timestamp_literals_preserve_python_microsecond_rendering() {
    let lower = timestamp("2026-08-01T00:00:00.001000Z");
    let upper = timestamp("2026-08-02T00:00:00.123456Z");
    let leaf = LookupLeaf::new("history_microseconds", lower, upper, Some(lower))
        .expect("microsecond leaf");
    let body = render_staged_lookup_function(
        &LookupManifest::new(vec![leaf], Some(lower)).expect("microsecond manifest"),
    );
    assert!(body.contains("TIMESTAMPTZ '2026-08-01T00:00:00.001000Z'"));
    assert!(body.contains("TIMESTAMPTZ '2026-08-02T00:00:00.123456Z'"));
}

#[test]
fn identity_wire_decode_is_typed_and_fail_closed() {
    let task_id = Uuid::parse_str("0198c0de-0000-7000-8000-000000000001").unwrap();
    assert_eq!(
        decode_lookup_row(LookupWireRow {
            found: false,
            location: None,
            task_id: None,
            fingerprint_version: None,
            command_fingerprint: None,
        })
        .unwrap(),
        TaskIdentityLookup::Absent
    );
    assert!(matches!(
        decode_lookup_row(LookupWireRow {
            found: true,
            location: Some("LIVE".to_owned()),
            task_id: Some(task_id),
            fingerprint_version: Some(1),
            command_fingerprint: Some(vec![1; 32]),
        })
        .unwrap(),
        TaskIdentityLookup::Live(_)
    ));
    assert!(decode_lookup_row(LookupWireRow {
        found: false,
        location: Some("LIVE".to_owned()),
        task_id: None,
        fingerprint_version: None,
        command_fingerprint: None,
    })
    .is_err());
    assert!(decode_lookup_row(LookupWireRow {
        found: true,
        location: Some("QUARANTINE".to_owned()),
        task_id: Some(task_id),
        fingerprint_version: Some(1),
        command_fingerprint: Some(vec![1; 32]),
    })
    .is_err());
}

fn test_window() -> HistoryWindow {
    HistoryWindow::new(
        timestamp("2026-08-01T00:00:00Z"),
        timestamp("2026-08-02T00:00:00Z"),
    )
    .expect("window")
}

#[test]
fn page_facet_and_aggregate_builders_pin_bounds_filters_columns_and_sorts() {
    assert_eq!(HISTORY_SUMMARY_COLUMNS.len(), 18);
    for envelope in [
        "result_payload",
        "prior_result_payload",
        "attempt_snapshot",
        "rerun_input_inline",
    ] {
        assert!(!HISTORY_SUMMARY_COLUMNS.contains(&envelope));
    }
    assert!(HistoryPageQuery::new(test_window(), 0).is_err());
    assert!(HistoryPageQuery::new(test_window(), 501).is_err());
    assert!(HistoryPageQuery::new(test_window(), 1)
        .unwrap()
        .with_offset(-1)
        .is_err());
    assert!(HistoryFacetQuery::new(test_window(), HistoryFacet::Status)
        .with_limit(201)
        .is_err());
    assert!(HistorySortField::parse("invented").is_err());
    for field in [
        HistorySortField::StartedAt,
        HistorySortField::CompletedAt,
        HistorySortField::FailedAt,
        HistorySortField::QueueSeconds,
        HistorySortField::ExecutionSeconds,
    ] {
        assert!(history_sort_expression(field, true).ends_with("DESC NULLS LAST"));
    }
    let scope = HistoryScope {
        statuses: vec!["COMPLETED".to_owned()],
        task_names: vec!["acme.report".to_owned()],
        category_families: vec![vec!["TASK_EXCEPTION".to_owned()]],
        domain_complement: Some(vec!["TASK_EXCEPTION".to_owned()]),
        retried_only: true,
        ..HistoryScope::default()
    };
    let page = history_page_statement(
        &HistoryPageQuery::new(test_window(), 10)
            .unwrap()
            .with_scope(scope.clone())
            .with_sort_field(HistorySortField::StartedAt, true),
    );
    assert!(page.sql().contains("started_at DESC NULLS LAST"));
    assert!(page.sql().contains("error_code = ANY($5::text[])"));
    assert!(page.sql().contains("error_code <> ALL($6::text[])"));
    assert!(page.sql().contains("retry_count > 0"));
    assert!(!page.sql().contains("'COMPLETED'"));
    assert!(matches!(
        page.parameters()[2],
        HistoryBindValue::TextArray(_)
    ));
    let facet = history_facet_statement(&HistoryFacetQuery::new(
        test_window(),
        HistoryFacet::ErrorCode,
    ));
    assert!(facet.sql().contains("error_code <> ''"));
    let aggregate = history_status_aggregate_statement(HistoryStatusAggregate::new(test_window()));
    for statement in [page.sql(), facet.sql(), aggregate.sql()] {
        assert!(statement.contains("retention_anchor_at >= $1"));
        assert!(statement.contains("retention_anchor_at < $2"));
    }
}

#[test]
fn planner_estimate_predicate_matches_count_and_decode_fails_closed() {
    let scope = HistoryScope {
        statuses: vec!["COMPLETED".to_owned(), "FAILED".to_owned()],
        task_names: vec!["alpha".to_owned()],
        retried_only: true,
        ..HistoryScope::default()
    };
    let count = history_count_statement(test_window(), &scope);
    let estimate = history_estimate_statement(test_window(), &scope);
    assert_eq!(
        count.sql().split_once(" WHERE ").unwrap().1,
        estimate.sql().split_once(" WHERE ").unwrap().1
    );
    assert_eq!(count.parameters(), estimate.parameters());
    assert!(estimate.sql().starts_with("EXPLAIN (FORMAT JSON)"));
    assert!(!estimate.sql().contains("ANALYZE"));
    assert_eq!(
        plan_rows_from_explain(&serde_json::json!([{"Plan": {"Plan Rows": 42}}])).unwrap(),
        42
    );
    assert_eq!(
        plan_rows_from_explain_text("[{\"Plan\":{\"Plan Rows\":7.0}}]").unwrap(),
        7
    );
    for payload in [
        Value::Null,
        serde_json::json!([]),
        serde_json::json!({}),
        serde_json::json!([{"Plan": {}}]),
        serde_json::json!([{"Plan": {"Plan Rows": "many"}}]),
        serde_json::json!([{"Plan": {"Plan Rows": true}}]),
    ] {
        assert!(plan_rows_from_explain(&payload).is_err(), "{payload}");
    }
    assert!(plan_rows_from_explain_text("not json").is_err());
}

#[test]
fn every_facet_sort_and_aggregate_variant_has_a_bounded_statement() {
    assert_eq!(HistoryFacet::ALL.len(), 7);
    for facet in HistoryFacet::ALL {
        let statement = history_facet_statement(&HistoryFacetQuery::new(test_window(), facet));
        assert!(statement.sql().contains(facet.column()));
        assert!(statement.sql().contains("retention_anchor_at >= $1"));
        assert!(statement.sql().contains("LIMIT $3"));
    }
    assert_eq!(HistorySortField::ALL.len(), 11);
    for field in HistorySortField::ALL {
        let ascending = history_sort_expression(field, false);
        let descending = history_sort_expression(field, true);
        assert!(ascending.contains(" ASC"));
        assert!(descending.contains(" DESC"));
    }
    let scope = HistoryScope {
        statuses: vec!["FAILED".to_owned()],
        queue_names: vec!["priority".to_owned()],
        workers: vec!["worker-p4".to_owned()],
        error_codes: vec!["BOOM".to_owned()],
        ..HistoryScope::default()
    };
    let scoped = history_scoped_status_counts_statement(test_window(), &scope);
    assert!(scoped.sql().contains("GROUP BY status ORDER BY status"));
    assert_eq!(HistoryBreakdownGroup::ALL.len(), 3);
    for group in HistoryBreakdownGroup::ALL {
        let breakdown = history_breakdown_statement(test_window(), &scope, group);
        assert!(breakdown.sql().contains("retried_count"));
        assert!(breakdown.sql().contains("COALESCE("));
    }
    assert_eq!(
        HISTORY_NONEMPTY_PROBE_SQL.as_str(),
        "SELECT EXISTS (SELECT 1 FROM horsies_task_history)"
    );
}

struct TestDatabase {
    pool: PgPool,
    database_name: String,
    admin_options: PgConnectOptions,
}

impl TestDatabase {
    async fn create() -> Self {
        let base = database_url();
        let base_options = PgConnectOptions::from_str(&base).expect("invalid test database URL");
        let admin_options = base_options.clone().database("postgres");
        let database_name = format!("horsies_p4_{}", Uuid::new_v4().simple());
        let mut admin = PgConnection::connect_with(&admin_options)
            .await
            .expect("connect to postgres admin database");
        sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
            .execute(&mut admin)
            .await
            .expect("create P4 test database");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .expect("connect to P4 test database");
        run_horsies_migrations(&pool)
            .await
            .expect("migrate P4 test database");
        Self {
            pool,
            database_name,
            admin_options,
        }
    }

    async fn drop(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect_with(&self.admin_options)
            .await
            .expect("connect for P4 database cleanup");
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = $1 AND pid <> pg_backend_pid()",
        )
        .bind(&self.database_name)
        .execute(&mut admin)
        .await
        .expect("terminate P4 test database sessions");
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\"",
            self.database_name
        ))
        .execute(&mut admin)
        .await
        .expect("drop P4 test database");
    }
}

fn database_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return url;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let contents = std::fs::read_to_string(root.join(".env")).expect("read workspace .env");
    let password = contents
        .lines()
        .filter_map(|line| line.split_once('='))
        .find_map(|(key, value)| (key.trim() == "DB_PASSWORD").then(|| value.trim()))
        .expect("DB_PASSWORD in workspace .env");
    format!("postgresql://postgres:{password}@localhost:5432/horsies-rust-port")
}

fn utc_day(value: DateTime<Utc>) -> DateTime<Utc> {
    value.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc()
}

fn v7_with_birth(birth: DateTime<Utc>) -> Uuid {
    let milliseconds = birth.timestamp_millis() as u128;
    Uuid::from_u128((milliseconds << 80) | (0x7_u128 << 76) | (0b10_u128 << 62) | 1)
}

async fn register_test_class(
    connection: &mut PgConnection,
    class_key: &str,
    duration_days: i64,
) -> String {
    let outcome =
        register_finite_retention_class(connection, class_key, Duration::days(duration_days))
            .await
            .expect("register P4 retention class");
    assert!(matches!(
        outcome,
        ClassRegistration::Registered { .. } | ClassRegistration::AlreadyRegistered { .. }
    ));
    finite_class_parent_name(class_key).expect("finite parent name")
}

async fn create_test_leaf(
    connection: &mut PgConnection,
    parent: &str,
    class_key: &str,
    lower: DateTime<Utc>,
) -> LeafRef {
    let leaf = LeafRef::new(
        daily_leaf_name(parent, lower).expect("daily leaf name"),
        class_key,
        LeafBounds::new(lower, lower + Duration::days(1)).expect("daily leaf bounds"),
    )
    .expect("daily leaf ref");
    let outcome = create_daily_leaf(
        connection,
        &CreateDailyHistoryLeaf::new(leaf.clone()).expect("create daily command"),
        &StagedLoaderPublisher,
    )
    .await
    .expect("create P4 leaf");
    assert!(matches!(
        outcome,
        LeafCreation::Created { .. } | LeafCreation::AlreadyConformant { .. }
    ));
    leaf
}

fn attempt_snapshot(anchor: DateTime<Utc>) -> StoredAttemptSnapshot {
    encode_attempt_snapshot(&[AttemptRecord::new(
        1,
        "FAILED",
        true,
        anchor - Duration::minutes(2),
        anchor - Duration::minutes(1),
        Some("BOOM".to_owned()),
        Some("exploded once".to_owned()),
        Some("TASK_ERROR".to_owned()),
        Some("worker-p4".to_owned()),
        Some("host-p4".to_owned()),
        Some(42),
        Some("process-p4".to_owned()),
    )
    .expect("attempt record")])
    .expect("encode attempt snapshot")
}

async fn seed_history_row(
    connection: &mut PgConnection,
    task_id: Uuid,
    class_key: &str,
    anchor: DateTime<Utc>,
    status: &str,
    kind: &str,
) -> StoredAttemptSnapshot {
    let attempts = attempt_snapshot(anchor);
    let result_payload = br#"{"ok":true}"#.as_slice();
    let sql = format!(
        "INSERT INTO {TASK_HISTORY_PARENT} (
             task_id, task_name, queue_name, priority,
             command_fingerprint_version, command_fingerprint,
             status, terminalization_kind, terminal_at,
             retention_anchor_at, retention_class_key,
             sent_at, enqueued_at, started_at, created_at,
             retry_count, max_retries, last_claimed_worker_id,
             last_worker_hostname, last_worker_pid,
             result_envelope_version, result_codec, result_content_type,
             result_payload, result_digest, error_code, final_failed_reason,
             is_workflow_task, history_schema_version,
             attempt_archive_version, attempt_snapshot_codec,
             attempt_snapshot_content_type, attempt_snapshot,
             attempt_snapshot_digest, rerun_input_disposition
         ) VALUES (
             $1, 'p4.history', 'default', 100,
             1, $2, $3, $4, $5, $5, $6,
             $5, $5, $5, $5, 1, 3, 'worker-p4', 'host-p4', 42,
             1, 'json-utf8', 'application/json', $7, $8, 'BOOM', 'failed',
             FALSE, 1, $9, $10, $11, $12, $13, 'NEVER_ELIGIBLE'
         )"
    );
    sqlx::query(&sql)
        .bind(task_id)
        .bind(vec![7_u8; 32])
        .bind(status)
        .bind(kind)
        .bind(anchor)
        .bind(class_key)
        .bind(result_payload)
        .bind(archive_digest(result_payload).to_vec())
        .bind(attempts.version)
        .bind(attempts.codec)
        .bind(attempts.content_type)
        .bind(&attempts.payload)
        .bind(attempts.digest.to_vec())
        .execute(connection)
        .await
        .expect("seed P4 history row");
    attempts
}

async fn seed_live_row(
    connection: &mut PgConnection,
    task_id: Uuid,
    class_key: &str,
    anchor: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, status, sent_at, enqueued_at,
             enqueue_sha, command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input,
             prepared_rerun_input_disposition
         ) VALUES (
             $1, 'p4.live', 'default', 'PENDING', $2, $2,
             'p4-live-sha', 1, $3, $4, FALSE, 'NEVER_ELIGIBLE'
         )",
    )
    .bind(task_id)
    .bind(anchor)
    .bind(vec![9_u8; 32])
    .bind(class_key)
    .execute(connection)
    .await
    .expect("seed P4 live row");
}

async fn stamp_leaf_birth(connection: &mut PgConnection, leaf_name: &str, birth: DateTime<Utc>) {
    sqlx::query(&format!(
        "UPDATE {LEAF_CATALOG} SET min_birth_at = $1, min_birth_verified = TRUE
         WHERE leaf_name = $2"
    ))
    .bind(birth)
    .bind(leaf_name)
    .execute(connection)
    .await
    .expect("stamp P4 leaf birth");
}

#[tokio::test]
#[serial]
async fn publisher_atomically_installs_canonical_triple_and_resolves_all_identity_paths() {
    let database = TestDatabase::create().await;
    let class_key = "p4_lookup";
    let mut transaction = database.pool.begin().await.expect("begin P4 lookup setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let other_class_key = "p4_lookup_b";
    let other_parent = register_test_class(&mut transaction, other_class_key, 30).await;
    let other_leaf =
        create_test_leaf(&mut transaction, &other_parent, other_class_key, today).await;
    let normal_id = v7_with_birth(today + Duration::hours(1));
    let future_id = v7_with_birth(today + Duration::days(365));
    let legacy_id = Uuid::new_v4();
    let other_class_id = v7_with_birth(today + Duration::hours(6));
    seed_history_row(
        &mut transaction,
        normal_id,
        class_key,
        today + Duration::hours(2),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    seed_history_row(
        &mut transaction,
        future_id,
        class_key,
        today + Duration::hours(3),
        "COMPLETED",
        "COMPLETE_FUSED",
    )
    .await;
    seed_history_row(
        &mut transaction,
        legacy_id,
        class_key,
        today + Duration::hours(4),
        "COMPLETED",
        "COMPLETE_LOCKED",
    )
    .await;
    seed_history_row(
        &mut transaction,
        other_class_id,
        other_class_key,
        today + Duration::hours(7),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    let live_id = v7_with_birth(today + Duration::hours(5));
    seed_live_row(&mut transaction, live_id, class_key, now).await;
    stamp_leaf_birth(
        &mut transaction,
        leaf.leaf_name(),
        today + Duration::minutes(30),
    )
    .await;
    stamp_leaf_birth(
        &mut transaction,
        other_leaf.leaf_name(),
        today + Duration::hours(6),
    )
    .await;
    sqlx::query(
        "CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid)
         RETURNS horsies_task_provenance LANGUAGE sql STABLE AS
         $$ SELECT NULL::horsies_task_provenance $$",
    )
    .execute(&mut *transaction)
    .await
    .expect("plant superseded provenance signature");
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish staged triple");
    transaction.commit().await.expect("commit P4 lookup setup");

    let mut connection = database.pool.acquire().await.expect("acquire P4 lookup");
    assert!(matches!(
        super::identity_lookup::lookup_task_identity(&mut connection, live_id)
            .await
            .expect("lookup live"),
        TaskIdentityLookup::Live(_)
    ));
    for task_id in [normal_id, future_id, legacy_id, other_class_id] {
        assert!(matches!(
            super::identity_lookup::lookup_task_identity(&mut connection, task_id)
                .await
                .expect("lookup retained history"),
            TaskIdentityLookup::History(_)
        ));
    }
    assert_eq!(
        super::identity_lookup::lookup_task_identity(&mut connection, Uuid::new_v4())
            .await
            .expect("lookup absent"),
        TaskIdentityLookup::Absent
    );
    assert!(matches!(
        read_task_detail(&mut connection, live_id)
            .await
            .expect("read live detail"),
        TaskDetailResult::Live { task_id } if task_id == live_id
    ));
    let TaskDetailResult::History(detail) = read_task_detail(&mut connection, normal_id)
        .await
        .expect("read history detail")
    else {
        panic!("retained task did not decode as history detail");
    };
    assert_eq!(detail.task_id, normal_id);
    assert_eq!(detail.status, "FAILED");
    assert_eq!(detail.attempts.len(), 1);
    assert_eq!(detail.attempts[0].outcome(), "FAILED");
    assert!(StagedLoaderPublisher
        .references_leaf(&mut connection, leaf.leaf_name())
        .await
        .expect("published leaf reference"));
    assert!(StagedLoaderPublisher
        .references_leaf(&mut connection, other_leaf.leaf_name())
        .await
        .expect("other-class published leaf reference"));
    let manifest_positions: Vec<(String, i32)> = sqlx::query_as(
        "SELECT leaf_name, probe_position FROM horsies_task_lookup_manifest
         ORDER BY probe_position",
    )
    .fetch_all(&mut *connection)
    .await
    .expect("read published manifest positions");
    assert!(manifest_positions
        .iter()
        .any(|(name, _)| name == leaf.leaf_name()));
    assert!(manifest_positions
        .iter()
        .any(|(name, _)| name == other_leaf.leaf_name()));
    assert!(manifest_positions
        .iter()
        .enumerate()
        .all(|(position, (_, stored))| *stored == position as i32));

    let signatures: Vec<(String, String)> = sqlx::query_as(
        "SELECT procedure.proname,
                pg_get_function_identity_arguments(procedure.oid)
         FROM pg_proc AS procedure
         JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
         WHERE namespace.nspname = current_schema()
           AND procedure.proname = ANY($1::text[])
         ORDER BY procedure.proname, 2",
    )
    .bind(vec![
        "horsies_task_lookup_staged",
        "horsies_task_provenance_staged",
        "horsies_task_detail_staged",
    ])
    .fetch_all(&mut *connection)
    .await
    .expect("read staged signatures");
    assert_eq!(
        signatures,
        vec![
            (
                "horsies_task_detail_staged".to_owned(),
                "p_task_id uuid".to_owned(),
            ),
            (
                "horsies_task_lookup_staged".to_owned(),
                "p_task_id uuid".to_owned(),
            ),
            (
                "horsies_task_provenance_staged".to_owned(),
                "p_task_id uuid, p_include_live boolean".to_owned(),
            ),
        ]
    );
    let live_excluded: bool =
        sqlx::query_scalar("SELECT found FROM horsies_task_provenance_staged($1, FALSE)")
            .bind(live_id)
            .fetch_one(&mut *connection)
            .await
            .expect("exclude live provenance");
    assert!(!live_excluded);
    drop(connection);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn coverage_republishes_the_triple_when_only_the_detail_function_is_missing() {
    let database = TestDatabase::create().await;
    let mut transaction = database.pool.begin().await.expect("begin initial coverage");
    let initial = ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .expect("establish steady coverage");
    assert!(matches!(initial, CoverageOutcome::Ensured(_)));
    transaction.commit().await.expect("commit initial coverage");

    sqlx::query("DROP FUNCTION horsies_task_detail_staged(uuid)")
        .execute(&database.pool)
        .await
        .expect("drop only staged detail function");
    let mut transaction = database.pool.begin().await.expect("begin healing coverage");
    assert!(!staged_detail_published(&mut transaction)
        .await
        .expect("detail publication probe"));
    assert!(published_manifest_absent_leaves(&mut transaction)
        .await
        .expect("healthy manifest probe")
        .is_empty());
    let healed = ensure_partition_coverage(&mut transaction, 2, 2, &[], &StagedLoaderPublisher)
        .await
        .expect("heal missing staged detail");
    let CoverageOutcome::Ensured(healed) = healed else {
        panic!("missing detail function was not healed by coverage");
    };
    assert_eq!(healed.created_history_leaves, 0);
    assert_eq!(healed.created_heartbeat_leaves, 0);
    assert!(healed.republished);
    assert!(healed.absent_leaves.is_empty());
    assert!(staged_detail_published(&mut transaction)
        .await
        .expect("healed detail publication probe"));
    transaction.commit().await.expect("commit healing coverage");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn missing_relation_is_detected_excluded_reported_and_not_misclassified_as_ageing() {
    let database = TestDatabase::create().await;
    let class_key = "p4_missing";
    let mut transaction = database.pool.begin().await.expect("begin missing setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let task_id = v7_with_birth(today + Duration::hours(2));
    seed_history_row(
        &mut transaction,
        task_id,
        class_key,
        today + Duration::hours(3),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    stamp_leaf_birth(
        &mut transaction,
        leaf.leaf_name(),
        today + Duration::hours(1),
    )
    .await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish before missing relation");
    transaction.commit().await.expect("commit missing setup");

    sqlx::query(&format!("DROP TABLE {}", leaf.leaf_name()))
        .execute(&database.pool)
        .await
        .expect("drop leaf out of band");
    let mut connection = database
        .pool
        .acquire()
        .await
        .expect("acquire missing probe");
    let broken = super::identity_lookup::lookup_task_identity(&mut connection, task_id)
        .await
        .expect_err("published missing leaf must break execution");
    assert!(matches!(
        broken,
        HistoryError::Database(sqlx::Error::Database(ref error)) if error.code().as_deref() == Some("42P01")
    ));
    assert_eq!(
        published_manifest_absent_leaves(&mut connection)
            .await
            .expect("published absent leaves"),
        vec![leaf.leaf_name().to_owned()]
    );
    let republished = StagedLoaderPublisher
        .republish(&mut connection)
        .await
        .expect("heal staged readers");
    assert_eq!(republished.absent_leaves, vec![leaf.leaf_name().to_owned()]);
    assert!(published_manifest_absent_leaves(&mut connection)
        .await
        .expect("healed manifest divergence")
        .is_empty());
    assert!(!StagedLoaderPublisher
        .references_leaf(&mut connection, leaf.leaf_name())
        .await
        .expect("excluded manifest reference"));
    assert!(matches!(
        read_task_detail(&mut connection, task_id)
            .await
            .expect("classify destroyed-leaf detail"),
        TaskDetailResult::Absent {
            task_id: found,
            predates_retained_floor: Some(false),
        } if found == task_id
    ));
    let catalog_still_attached: bool = sqlx::query_scalar(&format!(
        "SELECT detached_at IS NULL AND dropped_at IS NULL FROM {LEAF_CATALOG}
         WHERE leaf_name = $1"
    ))
    .bind(leaf.leaf_name())
    .fetch_one(&mut *connection)
    .await
    .expect("read missing leaf catalog evidence");
    assert!(catalog_still_attached);
    drop(connection);
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn detail_refuses_attempt_digest_corruption_and_purged_floor_is_typed() {
    let database = TestDatabase::create().await;
    let class_key = "p4_purged";
    let mut transaction = database.pool.begin().await.expect("begin purged setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let old_day = today - Duration::days(40);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let old_leaf = create_test_leaf(&mut transaction, &parent, class_key, old_day).await;
    let current_leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let old_id = v7_with_birth(old_day + Duration::hours(1));
    let current_id = v7_with_birth(today + Duration::hours(1));
    let old_snapshot = seed_history_row(
        &mut transaction,
        old_id,
        class_key,
        old_day + Duration::hours(2),
        "FAILED",
        "FAIL_RUNNING",
    )
    .await;
    seed_history_row(
        &mut transaction,
        current_id,
        class_key,
        today + Duration::hours(2),
        "COMPLETED",
        "COMPLETE_FUSED",
    )
    .await;
    stamp_leaf_birth(
        &mut transaction,
        old_leaf.leaf_name(),
        old_day + Duration::hours(1),
    )
    .await;
    stamp_leaf_birth(
        &mut transaction,
        current_leaf.leaf_name(),
        today + Duration::hours(1),
    )
    .await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish purged setup");
    assert!(matches!(
        read_task_detail(&mut transaction, old_id)
            .await
            .expect("read old detail"),
        TaskDetailResult::History(_)
    ));
    sqlx::query(&format!(
        "UPDATE {TASK_HISTORY_PARENT} SET attempt_snapshot_digest = $1 WHERE task_id = $2"
    ))
    .bind(vec![0_u8; 32])
    .bind(old_id)
    .execute(&mut *transaction)
    .await
    .expect("corrupt attempt snapshot digest");
    assert!(matches!(
        read_task_detail(&mut transaction, old_id)
            .await
            .expect_err("digest corruption must refuse detail"),
        HistoryError::ArchiveDecode(ArchiveDecodeError::DigestMismatch {
            domain: ArchiveDomain::Attempts
        })
    ));
    sqlx::query(&format!(
        "UPDATE {TASK_HISTORY_PARENT} SET attempt_snapshot_digest = $1 WHERE task_id = $2"
    ))
    .bind(old_snapshot.digest.to_vec())
    .bind(old_id)
    .execute(&mut *transaction)
    .await
    .expect("restore attempt digest");
    transaction.commit().await.expect("commit purged setup");

    detach_expired_leaf(
        &database.pool,
        &DetachExpiredHistoryLeaf::new(old_leaf.clone(), None, Some(5_000))
            .expect("detach command"),
        &StagedLoaderPublisher,
        &NoQuarantine,
    )
    .await
    .expect("detach old P4 leaf");
    let mut transaction = database.pool.begin().await.expect("begin old leaf drop");
    assert!(matches!(
        drop_detached_leaf(
            &mut transaction,
            &DropDetachedHistoryLeaf::new(old_leaf.clone()),
            &StagedLoaderPublisher,
        )
        .await
        .expect("drop old P4 leaf"),
        LeafDrop::Dropped { .. }
    ));
    assert!(matches!(
        read_task_detail(&mut transaction, old_id)
            .await
            .expect("classify purged detail"),
        TaskDetailResult::Absent {
            task_id,
            predates_retained_floor: Some(true),
        } if task_id == old_id
    ));
    transaction.commit().await.expect("commit old leaf drop");
    database.drop().await;
}

#[tokio::test]
#[serial]
async fn page_facet_aggregate_estimate_and_plans_are_window_and_index_bounded() {
    let database = TestDatabase::create().await;
    let class_key = "p4_pages";
    let mut transaction = database.pool.begin().await.expect("begin pages setup");
    let now = database_now(&mut transaction).await.expect("database now");
    let today = utc_day(now);
    let parent = register_test_class(&mut transaction, class_key, 30).await;
    let finite_leaf = create_test_leaf(&mut transaction, &parent, class_key, today).await;
    let old_leaf = create_test_leaf(
        &mut transaction,
        &parent,
        class_key,
        today - Duration::days(10),
    )
    .await;
    let forever_leaf: String = sqlx::query_scalar(&format!(
        "SELECT leaf_name FROM {LEAF_CATALOG}
         WHERE class_key = 'forever' AND detached_at IS NULL AND dropped_at IS NULL
           AND lower_anchor <= $1 AND upper_anchor > $1"
    ))
    .bind(today + Duration::hours(1))
    .fetch_one(&mut *transaction)
    .await
    .expect("current forever leaf");
    for offset in 0..20 {
        let anchor = today + Duration::minutes(offset * 5 + 5);
        let status = if offset % 2 == 0 {
            "COMPLETED"
        } else {
            "FAILED"
        };
        let kind = if status == "COMPLETED" {
            "COMPLETE_FUSED"
        } else {
            "FAIL_RUNNING"
        };
        seed_history_row(
            &mut transaction,
            Uuid::new_v4(),
            class_key,
            anchor,
            status,
            kind,
        )
        .await;
        seed_history_row(
            &mut transaction,
            Uuid::new_v4(),
            "forever",
            anchor + Duration::seconds(1),
            status,
            kind,
        )
        .await;
    }
    stamp_leaf_birth(&mut transaction, finite_leaf.leaf_name(), today).await;
    stamp_leaf_birth(&mut transaction, &forever_leaf, today).await;
    StagedLoaderPublisher
        .republish(&mut transaction)
        .await
        .expect("publish page setup");
    sqlx::query(&format!(
        "ANALYZE {}, {}, {}",
        finite_leaf.leaf_name(),
        forever_leaf,
        old_leaf.leaf_name()
    ))
    .execute(&mut *transaction)
    .await
    .expect("analyze page leaves");
    transaction.commit().await.expect("commit page setup");

    let window = HistoryWindow::new(today, today + Duration::days(1)).unwrap();
    let page = history_page_statement(
        &HistoryPageQuery::new(window, 5)
            .unwrap()
            .with_sort_field(HistorySortField::EnqueuedAt, true),
    );
    let mut connection = database.pool.acquire().await.expect("acquire page query");
    let rows = page
        .query()
        .fetch_all(&mut *connection)
        .await
        .expect("execute history page");
    assert_eq!(rows.len(), 5);
    assert!(rows[0].try_get::<Vec<u8>, _>("result_payload").is_err());
    let facet = history_facet_statement(&HistoryFacetQuery::new(window, HistoryFacet::Status));
    let facets = facet
        .query()
        .fetch_all(&mut *connection)
        .await
        .expect("execute history facet");
    assert_eq!(
        facets
            .iter()
            .map(|row| row.get::<i64, _>("facet_count"))
            .sum::<i64>(),
        40
    );
    let aggregate = history_status_aggregate_statement(HistoryStatusAggregate::new(window));
    let aggregate_rows = aggregate
        .query()
        .fetch_all(&mut *connection)
        .await
        .expect("execute history aggregate");
    assert_eq!(
        aggregate_rows
            .iter()
            .map(|row| row.get::<i64, _>("terminal_count"))
            .sum::<i64>(),
        40
    );
    let estimate = history_estimate_statement(window, &HistoryScope::default());
    let explain: Value = estimate
        .query()
        .fetch_one(&mut *connection)
        .await
        .expect("execute history estimate")
        .get(0);
    assert!(plan_rows_from_explain(&explain).expect("decode estimate") >= 1);

    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *connection)
        .await
        .expect("disable seqscan for ordering proof");
    let explain_page = super::pages::HistoryStatement::new(
        format!("EXPLAIN {}", page.sql()),
        page.parameters().to_vec(),
    );
    let plan = explain_page
        .query()
        .fetch_all(&mut *connection)
        .await
        .expect("explain ordered page")
        .into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!plan.contains("->  Sort"), "{plan}");
    assert!(!plan.trim_start().starts_with("Sort"), "{plan}");
    assert!(plan.contains("Merge Append"), "{plan}");
    assert!(plan.contains("Index Scan Backward"), "{plan}");
    assert!(plan.contains("_enqueued_idx"), "{plan}");

    let aggregate_sql = aggregate
        .sql()
        .replace(
            "$1",
            &format!("TIMESTAMPTZ '{}'", window.lower().to_rfc3339()),
        )
        .replace(
            "$2",
            &format!("TIMESTAMPTZ '{}'", window.upper().to_rfc3339()),
        );
    let pruning_plan = sqlx::query(&format!("EXPLAIN {aggregate_sql}"))
        .fetch_all(&mut *connection)
        .await
        .expect("explain aggregate pruning")
        .into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !pruning_plan.contains(old_leaf.leaf_name()),
        "{pruning_plan}"
    );
    assert!(
        pruning_plan.contains(finite_leaf.leaf_name()),
        "{pruning_plan}"
    );
    drop(connection);
    database.drop().await;
}
