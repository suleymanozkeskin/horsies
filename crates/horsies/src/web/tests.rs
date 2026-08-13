use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serial_test::serial;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

use crate::broker::terminalization_matrix::{migrated_database_url, migrated_pool};
use crate::broker::{PostgresBroker, SchemaInitializationMode};
use crate::{AppConfig, Horsies, PostgresConfig};

use super::app::{build_router, WebState};
use super::auth::{
    AllowAll, MonitoringAuthPolicy, MonitoringRequest, TrustedHeader, ViewOnly, INTENT_HEADER,
    INTENT_VALUE,
};
use super::common::{parse_fastapi_bool, QueryValues};
use super::events::{
    data_frame, degraded_frame, EventBroadcaster, EventCoalescer, TopicEvent, CHANNEL_TOPICS,
    MAX_IDS_PER_EVENT,
};
use super::schema::{
    SchemaProbe, SchemaReader, SchemaState, SchemaStatus, SCHEMA_INCOMPATIBLE, SCHEMA_UNKNOWN,
};
use super::spa::{inject, safe_asset_path, EmbeddedAssets, MemoryAssets, MonitoringUiConfig};

fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .connect_lazy("postgresql://postgres:pw@127.0.0.1:1/none")
        .expect("lazy test pool")
}

fn status(state: SchemaState, version: Option<i64>) -> SchemaStatus {
    SchemaStatus {
        state,
        version,
        expected_version: crate::expected_schema_version(),
    }
}

enum ReaderAnswer {
    Status(SchemaStatus),
    Failure,
}

struct SequenceReader {
    answers: Mutex<VecDeque<ReaderAnswer>>,
    reads: AtomicUsize,
}

impl SequenceReader {
    fn new(answers: impl IntoIterator<Item = ReaderAnswer>) -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(answers.into_iter().collect()),
            reads: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl SchemaReader for SequenceReader {
    async fn read(&self, _expected_version: i64) -> Result<SchemaStatus, sqlx::Error> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        match self.answers.lock().await.pop_front() {
            Some(ReaderAnswer::Status(status)) => Ok(status),
            Some(ReaderAnswer::Failure) | None => Err(sqlx::Error::PoolClosed),
        }
    }
}

fn fixed_probe(schema: SchemaStatus) -> Arc<SchemaProbe> {
    Arc::new(SchemaProbe::with_reader(
        SequenceReader::new([ReaderAnswer::Status(schema)]),
        schema.expected_version,
        Duration::from_secs(60),
    ))
}

fn test_state(
    pool: PgPool,
    auth_policy: Arc<dyn MonitoringAuthPolicy>,
    schema_probe: Arc<SchemaProbe>,
    actions_enabled: bool,
    assets: Arc<dyn super::spa::AssetStore>,
) -> WebState {
    let broker = Arc::new(PostgresBroker::from_pool(pool));
    let app = Horsies::with_broker(
        AppConfig::for_database_url("postgresql://postgres:pw@127.0.0.1:1/none"),
        Arc::clone(&broker),
    )
    .expect("test app");
    WebState {
        events: EventBroadcaster::with_debounce(Arc::clone(&broker), Duration::from_millis(25)),
        broker,
        auth_policy,
        schema_probe,
        actions_enabled,
        ui_config: MonitoringUiConfig::default(),
        assets,
        workflow_registry: Arc::new(app.workflow_registry().clone()),
        payload: app.config().payload.clone(),
        retention: app.config().retention.clone(),
        heartbeat: Duration::from_millis(25),
    }
}

fn test_router(
    auth_policy: Arc<dyn MonitoringAuthPolicy>,
    schema: SchemaStatus,
    actions_enabled: bool,
) -> Router {
    build_router(test_state(
        lazy_pool(),
        auth_policy,
        fixed_probe(schema),
        actions_enabled,
        MemoryAssets::standard(),
    ))
}

fn request(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

fn action_request(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(INTENT_HEADER, INTENT_VALUE)
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .expect("action request")
}

async fn json(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

#[tokio::test]
async fn auth_policies_match_the_three_pinned_postures() {
    let base = request(Method::GET, "/api/meta");
    let facts = MonitoringRequest::from_request(&base);
    assert!(AllowAll.can_view(facts).await);
    let facts = MonitoringRequest::from_request(&base);
    assert!(AllowAll.can_act(facts).await);
    let facts = MonitoringRequest::from_request(&base);
    assert!(ViewOnly.can_view(facts).await);
    let facts = MonitoringRequest::from_request(&base);
    assert!(!ViewOnly.can_act(facts).await);

    let policy = TrustedHeader::new("X-Forwarded-User", true).expect("header");
    let absent = request(Method::GET, "/api/meta");
    assert!(
        !policy
            .can_view(MonitoringRequest::from_request(&absent))
            .await
    );
    let present = Request::builder()
        .uri("/api/meta")
        .header("x-forwarded-user", " alex ")
        .body(Body::empty())
        .unwrap();
    assert!(
        policy
            .can_view(MonitoringRequest::from_request(&present))
            .await
    );
    assert!(
        policy
            .can_act(MonitoringRequest::from_request(&present))
            .await
    );
    let view_only_header = TrustedHeader::new("X-Forwarded-User", false).unwrap();
    assert!(
        !view_only_header
            .can_act(MonitoringRequest::from_request(&present))
            .await
    );
}

#[tokio::test]
async fn every_declared_api_route_is_view_authorized() {
    let router = test_router(
        Arc::new(TrustedHeader::new("X-Forwarded-User", true).unwrap()),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        true,
    );
    let reads = [
        "/api/meta",
        "/api/tasks/stats",
        "/api/tasks/facets",
        "/api/tasks/breakdown",
        "/api/tasks",
        "/api/tasks/00000000-0000-4000-8000-000000000001",
        "/api/workflows/names",
        "/api/workflows",
        "/api/workflows/00000000-0000-4000-8000-000000000002",
        "/api/workflows/00000000-0000-4000-8000-000000000002/tasks/0",
        "/api/workers/ping",
        "/api/workers/schedules",
        "/api/workers",
        "/api/workers/w1/history",
        "/api/events",
    ];
    for path in reads {
        let response = router
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
        assert_eq!(
            json(response).await,
            serde_json::json!({"detail": "Not authorized."})
        );
    }
    for path in [
        "/api/tasks/id/cancel",
        "/api/workflows/id/pause",
        "/api/workflows/id/resume",
        "/api/workflows/id/cancel",
    ] {
        let response = router.clone().oneshot(action_request(path)).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
    }
}

#[tokio::test]
async fn action_guard_order_is_authorization_then_intent_then_schema() {
    let unknown = status(SchemaState::Unknown, None);
    let action = "/api/tasks/not-a-uuid/cancel";

    let response = test_router(Arc::new(ViewOnly), unknown, true)
        .oneshot(action_request(action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json(response).await,
        serde_json::json!({"detail": "Not authorized."})
    );

    let response = test_router(Arc::new(AllowAll), unknown, false)
        .oneshot(request(Method::POST, action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json(response).await,
        serde_json::json!({"detail": "Not authorized."})
    );

    let response = test_router(Arc::new(AllowAll), unknown, true)
        .oneshot(request(Method::POST, action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json(response).await,
        serde_json::json!({"detail": "Missing X-Horsies-Intent: action header."})
    );

    let response = test_router(Arc::new(AllowAll), unknown, true)
        .oneshot(action_request(action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(json(response).await["code"], SCHEMA_UNKNOWN);
}

#[tokio::test]
async fn task_action_body_defaults_when_absent_and_malformed_json_is_422() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        true,
    );
    let absent = Request::builder()
        .method(Method::POST)
        .uri("/api/tasks/not-a-uuid/cancel")
        .header(INTENT_HEADER, INTENT_VALUE)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(absent).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    let task_id = Uuid::new_v4();
    let malformed = Request::builder()
        .method(Method::POST)
        .uri(format!("/api/tasks/{task_id}/cancel"))
        .header(INTENT_HEADER, INTENT_VALUE)
        .header("content-type", "application/json")
        .body(Body::from("{"))
        .unwrap();
    let response = router.oneshot(malformed).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = json(response).await;
    assert_eq!(body["detail"][0]["loc"], serde_json::json!(["body"]));
    assert!(body["detail"][0].get("type").is_some());
    assert!(body["detail"][0].get("msg").is_some());
    assert!(body["detail"][0].get("input").is_some());
}

#[test]
fn fastapi_boolean_vocabulary_is_exhaustive() {
    for value in [
        "1", "true", "TRUE", "t", "T", "on", "ON", "yes", "YES", "y", "Y",
    ] {
        let query = QueryValues::parse(Some(&format!("retried_only={value}")));
        assert!(parse_fastapi_bool(&query, "retried_only", false).unwrap());
    }
    for value in [
        "0", "false", "FALSE", "f", "F", "off", "OFF", "no", "NO", "n", "N",
    ] {
        let query = QueryValues::parse(Some(&format!("retried_only={value}")));
        assert!(!parse_fastapi_bool(&query, "retried_only", true).unwrap());
    }
    assert!(parse_fastapi_bool(&QueryValues::default(), "retried_only", true).unwrap());
}

#[tokio::test]
async fn query_and_path_validation_preserve_location_input_and_window_refusals() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        true,
    );

    for field in ["since", "until"] {
        for value in [
            "2026-08-13T12%3A00",
            "2026-08-13T12%3A00%3A00.123456",
            "2026-08-13t12%3A00%3A00",
            "2026-08-13_12%3A00%3A00",
            "2026-08-13+12%3A00%3A00",
        ] {
            let naive = router
                .clone()
                .oneshot(request(
                    Method::GET,
                    &format!("/api/tasks/stats?{field}={value}"),
                ))
                .await
                .unwrap();
            assert_eq!(naive.status(), StatusCode::BAD_REQUEST, "{field}={value}");
            assert_eq!(
                json(naive).await,
                serde_json::json!({"detail": format!("{field} must be timezone-aware")})
            );
        }
    }

    for (path, location, input, error_type) in [
        (
            "/api/tasks?retried_only=maybe",
            serde_json::json!(["query", "retried_only"]),
            serde_json::json!("maybe"),
            "bool_parsing",
        ),
        (
            "/api/tasks?limit=many",
            serde_json::json!(["query", "limit"]),
            serde_json::json!("many"),
            "int_parsing",
        ),
        (
            "/api/tasks?until=not-a-date",
            serde_json::json!(["query", "until"]),
            serde_json::json!("not-a-date"),
            "datetime_parsing",
        ),
        (
            "/api/workflows/not-a-uuid/tasks/not-an-int",
            serde_json::json!(["path", "task_index"]),
            serde_json::json!("not-an-int"),
            "int_parsing",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{path}"
        );
        let body = json(response).await;
        assert_eq!(body["detail"][0]["loc"], location, "{path}");
        assert_eq!(body["detail"][0]["input"], input, "{path}");
        assert_eq!(body["detail"][0]["type"], error_type, "{path}");
        assert!(body["detail"][0]["msg"].is_string(), "{path}");
    }

    let bounded = router
        .oneshot(request(Method::GET, "/api/tasks?limit=0"))
        .await
        .unwrap();
    assert_eq!(bounded.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = json(bounded).await;
    assert_eq!(
        body["detail"][0]["loc"],
        serde_json::json!(["query", "limit"])
    );
    assert_eq!(body["detail"][0]["input"], "0");
    assert_eq!(body["detail"][0]["ctx"], serde_json::json!({"ge": 1}));
}

#[tokio::test]
async fn schema_state_matrix_controls_meta_and_actions_without_blocking_reads() {
    let cases = [
        (
            SchemaState::Match,
            Some(crate::expected_schema_version()),
            true,
            None,
        ),
        (
            SchemaState::Mismatch,
            Some(42),
            false,
            Some(SCHEMA_INCOMPATIBLE),
        ),
        (
            SchemaState::CutoverRequired,
            Some(crate::expected_schema_version()),
            false,
            Some(SCHEMA_INCOMPATIBLE),
        ),
        (SchemaState::Absent, None, false, Some(SCHEMA_INCOMPATIBLE)),
        (SchemaState::Unknown, None, false, Some(SCHEMA_UNKNOWN)),
    ];
    for (state, version, compatible, reason) in cases {
        let router = test_router(Arc::new(AllowAll), status(state, version), true);
        let meta_response = router
            .clone()
            .oneshot(request(Method::GET, "/api/meta"))
            .await
            .unwrap();
        assert_eq!(meta_response.status(), StatusCode::OK);
        let meta = json(meta_response).await;
        assert_eq!(meta["schema_compatible"], compatible);
        assert_eq!(meta["actions_enabled"], compatible);
        assert_eq!(
            meta["schema_version"],
            serde_json::to_value(version).unwrap()
        );
        assert_eq!(
            meta["actions_disabled_reason"],
            serde_json::to_value(reason).unwrap()
        );

        let action = router
            .clone()
            .oneshot(action_request("/api/tasks/not-a-uuid/cancel"))
            .await
            .unwrap();
        if compatible {
            assert_eq!(action.status(), StatusCode::NOT_FOUND);
        } else {
            assert_eq!(action.status(), StatusCode::CONFLICT);
            assert_eq!(json(action).await["code"], reason.unwrap());
        }

        let read = router
            .oneshot(request(Method::GET, "/api/tasks/not-a-uuid"))
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn every_action_route_inherits_the_schema_gate() {
    let router = test_router(
        Arc::new(AllowAll),
        status(
            SchemaState::CutoverRequired,
            Some(crate::expected_schema_version()),
        ),
        true,
    );
    for path in [
        "/api/tasks/id/cancel",
        "/api/workflows/id/pause",
        "/api/workflows/id/resume",
        "/api/workflows/id/cancel",
    ] {
        let response = router.clone().oneshot(action_request(path)).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT, "{path}");
        let body = json(response).await;
        assert_eq!(body["code"], SCHEMA_INCOMPATIBLE);
        assert!(body["detail"]
            .as_str()
            .unwrap()
            .contains("cutover is incomplete"));
    }
}

#[tokio::test]
async fn schema_probe_caches_successes_and_never_caches_a_cold_failure() {
    let expected = crate::expected_schema_version();
    let reader = SequenceReader::new([
        ReaderAnswer::Failure,
        ReaderAnswer::Status(status(SchemaState::Mismatch, Some(42))),
        ReaderAnswer::Failure,
    ]);
    let probe = SchemaProbe::with_reader(reader.clone(), expected, Duration::ZERO);

    assert_eq!(probe.status().await.state, SchemaState::Unknown);
    assert_eq!(probe.status().await.state, SchemaState::Mismatch);
    assert_eq!(probe.status().await.state, SchemaState::Mismatch);
    assert_eq!(reader.reads.load(Ordering::Relaxed), 3);

    let cached_reader = SequenceReader::new([ReaderAnswer::Status(status(
        SchemaState::Match,
        Some(expected),
    ))]);
    let cached = SchemaProbe::with_reader(cached_reader.clone(), expected, Duration::from_secs(60));
    for _ in 0..5 {
        assert_eq!(cached.status().await.state, SchemaState::Match);
    }
    assert_eq!(cached_reader.reads.load(Ordering::Relaxed), 1);
}

#[test]
fn coalescer_pins_channel_topics_order_dedup_and_cap() {
    assert_eq!(
        CHANNEL_TOPICS,
        [
            ("horsies_task_status", "tasks"),
            ("horsies_workflow_status", "workflows"),
            ("horsies_worker_state", "workers"),
        ]
    );
    let mut coalescer = EventCoalescer::new(3);
    let _ = coalescer.record("tasks", "a");
    let _ = coalescer.record("tasks", "a");
    let _ = coalescer.record("tasks", "b");
    let _ = coalescer.record("workflows", "w");
    assert_eq!(
        coalescer.drain(),
        vec![
            TopicEvent {
                topic: "tasks".to_owned(),
                ids: vec!["a".to_owned(), "b".to_owned()],
            },
            TopicEvent {
                topic: "workflows".to_owned(),
                ids: vec!["w".to_owned()],
            },
        ]
    );

    let mut boundary = EventCoalescer::new(MAX_IDS_PER_EVENT);
    for index in 0..MAX_IDS_PER_EVENT {
        let _ = boundary.record("tasks", &index.to_string());
    }
    assert_eq!(boundary.drain()[0].ids.len(), MAX_IDS_PER_EVENT);
    for index in 0..=MAX_IDS_PER_EVENT {
        let _ = boundary.record("tasks", &index.to_string());
    }
    assert!(boundary.drain()[0].ids.is_empty());
}

#[test]
fn sse_payloads_match_the_pinned_wire_format() {
    let event = TopicEvent {
        topic: "tasks".to_owned(),
        ids: vec!["a".to_owned(), "b".to_owned()],
    };
    assert_eq!(
        data_frame(&event),
        r#"{"topic": "tasks", "ids": ["a", "b"]}"#
    );
    assert_eq!(degraded_frame(), r#"{"topic": "degraded"}"#);
}

#[tokio::test]
async fn failed_listener_sends_one_degraded_frame_and_closes() {
    let pool = lazy_pool();
    pool.close().await;
    let state = test_state(
        pool,
        Arc::new(AllowAll),
        fixed_probe(status(
            SchemaState::Match,
            Some(crate::expected_schema_version()),
        )),
        true,
        MemoryAssets::standard(),
    );
    let response = build_router(state)
        .oneshot(request(Method::GET, "/api/events"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let mut body = response.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("degraded frame timeout")
        .expect("degraded frame")
        .expect("body frame")
        .into_data()
        .expect("data frame");
    assert_eq!(frame, "data: {\"topic\": \"degraded\"}\n\n");
    assert!(body.frame().await.is_none());
}

#[tokio::test]
async fn spa_is_public_mountable_injected_and_traversal_safe() {
    let mut state = test_state(
        lazy_pool(),
        Arc::new(TrustedHeader::new("X-Forwarded-User", false).unwrap()),
        fixed_probe(status(SchemaState::Unknown, None)),
        false,
        MemoryAssets::standard(),
    );
    state.ui_config.custom_css_url = Some("/brand/override.css".to_owned());
    let router = build_router(state);

    let page = router
        .clone()
        .oneshot(request(Method::GET, "/workflows/deep"))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let page = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("<base href=\"/\">"));
    assert!(page.contains(r#"{"basePath":"/","apiBase":"/api"}"#));
    assert!(page.find("window.__HORSIES_UI__").unwrap() < page.find("override.css").unwrap());

    let asset = router
        .clone()
        .oneshot(request(Method::GET, "/assets/app.js"))
        .await
        .unwrap();
    assert_eq!(asset.status(), StatusCode::OK);
    assert_eq!(asset.headers()["content-type"], "text/javascript");

    let unknown_api = router
        .clone()
        .oneshot(request(Method::GET, "/api/does-not-exist"))
        .await
        .unwrap();
    assert_eq!(unknown_api.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json(unknown_api).await,
        serde_json::json!({"detail": "Not found."})
    );

    let host = Router::new().nest("/monitoring", router);
    let mounted = host
        .clone()
        .oneshot(request(Method::GET, "/monitoring"))
        .await
        .unwrap();
    assert_eq!(mounted.status(), StatusCode::OK, "{:?}", mounted.headers());
    let mounted = String::from_utf8(
        to_bytes(mounted.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(
        mounted.contains("<base href=\"/monitoring/\">"),
        "{mounted}"
    );
    assert!(mounted.contains(r#"{"basePath":"/monitoring","apiBase":"/monitoring/api"}"#));
    let mounted_meta = host
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/monitoring/api/meta")
                .header("X-Forwarded-User", "operator")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let mounted_meta_status = mounted_meta.status();
    let mounted_meta = json(mounted_meta).await;
    assert_eq!(
        mounted_meta_status,
        StatusCode::OK,
        "mounted meta response: {mounted_meta}"
    );
    assert_eq!(mounted_meta["base_path"], "/monitoring");

    for path in ["../../Cargo.toml", "%2e%2e/%2e%2e/Cargo.toml", "a\\b"] {
        assert!(safe_asset_path(path).is_none(), "{path}");
    }
    let mixed_head = inject("<HTML><HEAD></HeAd   ><BODY>", "/x", None);
    assert!(mixed_head.find("window.__HORSIES_UI__").unwrap() < mixed_head.find("</HeAd").unwrap());
}

#[tokio::test]
#[serial]
async fn embedded_spa_and_each_primary_view_api_roundtrip_against_postgres() {
    let mut state = migrated_state().await;
    state.assets = Arc::new(EmbeddedAssets);
    let router = build_router(state);

    let page = router
        .clone()
        .oneshot(request(Method::GET, "/"))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let page = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("<base href=\"/\">"));
    assert!(page.contains("window.__HORSIES_UI__"));

    let asset_start = page.find("./assets/").expect("built JavaScript asset") + 1;
    let asset_end = page[asset_start..]
        .find('"')
        .map(|offset| asset_start + offset)
        .expect("asset URL terminator");
    let asset = router
        .clone()
        .oneshot(request(Method::GET, &page[asset_start..asset_end]))
        .await
        .unwrap();
    assert_eq!(asset.status(), StatusCode::OK);
    assert_eq!(asset.headers()["content-type"], "text/javascript");
    assert!(!to_bytes(asset.into_body(), usize::MAX)
        .await
        .unwrap()
        .is_empty());

    for path in [
        "/api/tasks?limit=1",
        "/api/workflows?limit=1",
        "/api/workers?limit=1",
    ] {
        let response = router
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let _ = json(response).await;
    }
}

#[tokio::test]
async fn missing_embedded_index_is_a_factual_503_while_api_still_answers() {
    let state = test_state(
        lazy_pool(),
        Arc::new(AllowAll),
        fixed_probe(status(SchemaState::Unknown, None)),
        true,
        MemoryAssets::empty(),
    );
    let router = build_router(state);
    let page = router
        .clone()
        .oneshot(request(Method::GET, "/"))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(json(page).await["detail"]
        .as_str()
        .unwrap()
        .contains("bun run build"));
    assert_eq!(
        router
            .oneshot(request(Method::GET, "/api/meta"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

async fn clean_w4(pool: &PgPool, prefix: &str) {
    let pattern = format!("{prefix}%");
    for statement in [
        "DELETE FROM horsies_task_history WHERE task_name LIKE $1",
        "DELETE FROM horsies_tasks WHERE task_name LIKE $1",
        "DELETE FROM horsies_workflows WHERE name LIKE $1",
    ] {
        sqlx::query(statement)
            .bind(&pattern)
            .execute(pool)
            .await
            .unwrap();
    }
}

async fn migrated_state() -> WebState {
    let pool = migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_pool(pool));
    let app = Horsies::with_broker(
        AppConfig::for_database_url(migrated_database_url().await),
        Arc::clone(&broker),
    )
    .unwrap();
    WebState {
        schema_probe: Arc::new(SchemaProbe::new(Arc::clone(&broker))),
        events: EventBroadcaster::with_debounce(Arc::clone(&broker), Duration::from_millis(25)),
        broker,
        auth_policy: Arc::new(AllowAll),
        actions_enabled: true,
        ui_config: MonitoringUiConfig::default(),
        assets: MemoryAssets::standard(),
        workflow_registry: Arc::new(app.workflow_registry().clone()),
        payload: app.config().payload.clone(),
        retention: app.config().retention.clone(),
        heartbeat: Duration::from_millis(25),
    }
}

async fn seed_pending(pool: &PgPool, prefix: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, args, kwargs, status,
             sent_at, enqueued_at, claimed, is_workflow_task,
             retry_count, max_retries, enqueue_sha,
             command_fingerprint_version, command_fingerprint,
             retention_class_key, retain_rerun_input,
             prepared_rerun_input_disposition, created_at, updated_at
         ) VALUES (
             $1, $2, 'default', 100, '[]', '{}', 'PENDING',
             NOW(), NOW(), FALSE, FALSE, 0, 3, $1::text,
             1, decode(repeat('0d', 32), 'hex'), 'forever', FALSE,
             'DECLINED_BY_POLICY', NOW(), NOW()
         )",
    )
    .bind(id)
    .bind(format!("{prefix}task_{}", id.simple()))
    .execute(pool)
    .await
    .unwrap();
    id
}

#[tokio::test]
#[serial]
async fn all_http_surfaces_route_to_the_monitoring_and_action_layers() {
    let state = migrated_state().await;
    let prefix = format!("w4_http_{}__", Uuid::new_v4().simple());
    clean_w4(state.broker.pool(), &prefix).await;
    let task_id = seed_pending(state.broker.pool(), &prefix).await;
    let router = build_router(state.clone());

    let checks = [
        "/api/meta",
        "/api/tasks/stats?retried_only=1",
        "/api/tasks/facets?retried_only=ON",
        "/api/tasks/breakdown?retried_only=yes",
        "/api/tasks?status=PENDING&status=RUNNING&retried_only=true",
        "/api/workflows/names",
        "/api/workflows",
        "/api/workers/ping?timeout_seconds=0.1",
        "/api/workers/schedules",
        "/api/workers",
        "/api/workers/never/history",
    ];
    for path in checks {
        let response = router
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{path}: {:?}",
            json(response).await
        );
    }
    for path in [
        format!("/api/tasks/{}", Uuid::new_v4()),
        format!("/api/workflows/{}", Uuid::new_v4()),
        format!("/api/workflows/{}/tasks/0", Uuid::new_v4()),
    ] {
        assert_eq!(
            router
                .clone()
                .oneshot(request(Method::GET, &path))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
    let bad = router
        .clone()
        .oneshot(request(Method::GET, "/api/tasks?error_category=NOPE"))
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        json(bad).await,
        serde_json::json!({"detail": "Unknown error category 'NOPE'."})
    );
    let reach = router
        .clone()
        .oneshot(request(Method::GET, "/api/tasks?offset=500&limit=1"))
        .await
        .unwrap();
    assert_eq!(reach.status(), StatusCode::BAD_REQUEST);

    let cancelled = router
        .oneshot(action_request(&format!("/api/tasks/{task_id}/cancel")))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::OK);
    assert_eq!(
        json(cancelled).await,
        serde_json::json!({
            "outcome": "cancelled",
            "was_status": "PENDING",
            "next_attempt_number": null,
            "warning": null
        })
    );
    clean_w4(state.broker.pool(), &prefix).await;
}

#[tokio::test]
#[serial]
async fn broadcaster_uses_its_own_listener_and_batches_real_notify_bursts() {
    let pool = migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
    let broadcaster = EventBroadcaster::with_debounce(broker, Duration::from_millis(40));
    let mut subscription = broadcaster.subscribe().await;

    let mut transaction = pool.begin().await.unwrap();
    for (channel, payload) in [
        ("horsies_task_status", "t1"),
        ("horsies_task_status", "t1"),
        ("horsies_task_status", "t2"),
        ("horsies_workflow_status", "w1"),
        ("horsies_worker_state", "worker-1"),
    ] {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(&mut *transaction)
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();

    let mut events = Vec::new();
    while events.len() < 3 {
        let event = tokio::time::timeout(Duration::from_secs(3), subscription.recv())
            .await
            .expect("event timeout")
            .expect("subscription open")
            .expect("not degraded");
        events.push(event);
    }
    events.sort_by(|left, right| left.topic.cmp(&right.topic));
    assert_eq!(events[0].topic, "tasks");
    assert_eq!(events[0].ids, ["t1", "t2"]);
    assert_eq!(events[1].topic, "workers");
    assert_eq!(events[2].topic, "workflows");
    broadcaster.close().await;
}

#[tokio::test]
#[serial]
async fn sse_route_emits_idle_heartbeat_comments() {
    let state = migrated_state().await;
    let broadcaster = Arc::clone(&state.events);
    let response = build_router(state)
        .oneshot(request(Method::GET, "/api/events"))
        .await
        .unwrap();
    let mut body = response.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .expect("heartbeat timeout")
        .expect("heartbeat frame")
        .expect("heartbeat body")
        .into_data()
        .expect("heartbeat data");
    assert_eq!(frame, ": heartbeat\n\n");
    drop(body);
    broadcaster.close().await;
}

#[tokio::test]
#[serial]
async fn observe_only_construction_runs_no_ddl_or_fleet_gate() {
    let base_url = migrated_database_url().await;
    let base_options = PgConnectOptions::from_str(&base_url).unwrap();
    let admin_options = base_options.clone().database("postgres");
    let mut admin = PgConnection::connect_with(&admin_options).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtext('horsies_w4_noddl_setup'))")
        .execute(&mut admin)
        .await
        .unwrap();
    let stale: Vec<String> = sqlx::query_scalar(
        "SELECT d.datname
         FROM pg_database d
         WHERE left(d.datname, length('horsies_w4_noddl_')) = 'horsies_w4_noddl_'
           AND NOT EXISTS (
               SELECT 1 FROM pg_stat_activity a WHERE a.datname = d.datname
           )
         ORDER BY d.datname",
    )
    .fetch_all(&mut admin)
    .await
    .unwrap();
    for stale_name in stale {
        let suffix = stale_name.strip_prefix("horsies_w4_noddl_").unwrap();
        assert!(suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
        sqlx::query(&format!("DROP DATABASE \"{stale_name}\""))
            .execute(&mut admin)
            .await
            .unwrap();
    }
    let name = format!("horsies_w4_noddl_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await
        .unwrap();
    let options = base_options.database(&name);
    let anchor = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .max_lifetime(None)
        .idle_timeout(None)
        .connect_with(options.clone())
        .await
        .unwrap();
    let unlocked: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtext('horsies_w4_noddl_setup'))")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert!(unlocked);
    let url = options.to_url_lossy().to_string();
    let app = Horsies::new_observe_only(AppConfig::for_database_url(&url)).unwrap();
    let broker = app.get_broker().await.unwrap();
    assert_eq!(
        broker.schema_initialization_mode(),
        SchemaInitializationMode::ObserveOnly
    );
    broker.ensure_schema_initialized().await.unwrap();
    broker.migrate().await.unwrap();
    let relations: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT to_regclass('horsies_tasks')::text, to_regclass('horsies_migrations')::text",
    )
    .fetch_one(broker.pool())
    .await
    .unwrap();
    assert_eq!(relations, (None, None));
    let schema = SchemaProbe::new(Arc::clone(&broker)).status().await;
    assert_eq!(schema.state, SchemaState::Absent);
    broker.pool().close().await;
    broker.session_pool().close().await;
    anchor.close().await;
    drop(broker);
    drop(app);
    let suffix = name.strip_prefix("horsies_w4_noddl_").unwrap();
    assert!(suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
    sqlx::query(&format!("DROP DATABASE \"{name}\" WITH (FORCE)"))
        .execute(&mut admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn default_broker_mode_still_migrates_and_validates() {
    let broker = PostgresBroker::from_pool(lazy_pool());
    assert_eq!(
        broker.schema_initialization_mode(),
        SchemaInitializationMode::MigrateAndValidate
    );
    let config = PostgresConfig::from_url("postgresql://postgres:pw@127.0.0.1:1/none");
    let app = Horsies::new(AppConfig {
        broker: config,
        ..AppConfig::for_database_url("postgresql://postgres:pw@127.0.0.1:1/none")
    })
    .unwrap();
    assert!(app.broker_if_initialized().is_none());
}
