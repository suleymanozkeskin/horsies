use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use serial_test::serial;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

use crate::broker::{PostgresBroker, SchemaInitializationMode};
use crate::monitoring::fleet::FleetLabels;
use crate::monitoring::TaskAbsenceReason;
use crate::telemetry::exposition::PROMETHEUS_TEXT_CONTENT_TYPE;
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::migrated_database_url;
use crate::test_support::migrated_pool;
use crate::test_support::sql::{self, TestPool, TestPoolOptions};
use crate::AppConfig;

use super::app::{build_router, WebState};
use super::auth::{MonitoringAuthorizationPolicy, MonitoringRequest, INTENT_HEADER, INTENT_VALUE};
use super::common::{parse_fastapi_bool, QueryValues};
use super::events::{
    data_frame, degraded_frame, EventBroadcaster, EventCoalescer, TopicEvent, CHANNEL_TOPICS,
    HEARTBEAT, MAX_IDS_PER_EVENT,
};
use super::fleet_metrics::WebFleetMetrics;
use super::handler::{
    AccessGrant, AccessNeed, Actions, GrantedAccess, MonitoringHandler, MonitoringHandlerConfig,
    RequestParts, ResponseForm, ResponseParts, Verdict,
};
use super::request_body::WorkflowActionBody;
use super::routes::task_not_found_text;
use super::schema::{
    SchemaProbe, SchemaReader, SchemaState, SchemaStatus, SCHEMA_INCOMPATIBLE, SCHEMA_UNKNOWN,
};
use super::spa::{
    inject, runtime_config_json, safe_asset_path, EmbeddedAssets, MemoryAssets, MonitoringUiConfig,
};
use super::task_stats_cache::{ReadCache, TaskStatsCache};
use super::trace_link::{TraceLink, TraceLinkTemplate};

/// A policy of the router tests that allows reads and actions.
struct AllowAll;

#[async_trait]
impl MonitoringAuthorizationPolicy for AllowAll {
    async fn can_view(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }

    async fn can_act(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }
}

/// A policy of the router tests that allows reads and refuses actions.
struct ViewOnly;

#[async_trait]
impl MonitoringAuthorizationPolicy for ViewOnly {
    async fn can_view(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }

    async fn can_act(&self, _request: MonitoringRequest<'_>) -> bool {
        false
    }
}

/// A policy of the router tests that allows a request with a value of the
/// header that is not blank; actions only when `actions` is enabled.
struct TrustedHeader {
    header_name: axum::http::HeaderName,
    actions: Actions,
}

impl TrustedHeader {
    fn new(
        header_name: &str,
        actions: Actions,
    ) -> Result<Self, axum::http::header::InvalidHeaderName> {
        Ok(Self {
            header_name: axum::http::HeaderName::try_from(header_name)?,
            actions,
        })
    }

    fn identified(&self, request: MonitoringRequest<'_>) -> bool {
        request
            .headers
            .get(&self.header_name)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| !value.trim().is_empty())
    }
}

#[async_trait]
impl MonitoringAuthorizationPolicy for TrustedHeader {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool {
        self.identified(request)
    }

    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool {
        match self.actions {
            Actions::Enabled => self.identified(request),
            Actions::Disabled => false,
        }
    }
}

fn lazy_pool() -> TestPool {
    TestPoolOptions::new().connect_lazy("postgresql://postgres:pw@127.0.0.1:1/none")
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
    async fn read(
        &self,
        _expected_version: i64,
    ) -> Result<SchemaStatus, crate::broker::BrokerError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        match self.answers.lock().await.pop_front() {
            Some(ReaderAnswer::Status(status)) => Ok(status),
            Some(ReaderAnswer::Failure) | None => Err(
                crate::broker::BrokerError::ConnectionFailed("the test reader fails".to_owned()),
            ),
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
    pool: &TestPool,
    authorization_policy: Arc<dyn MonitoringAuthorizationPolicy>,
    schema_probe: Arc<SchemaProbe>,
    actions: Actions,
    assets: Arc<dyn super::spa::AssetStore>,
) -> WebState {
    let broker = Arc::new(PostgresBroker::from_test_url(
        pool.url(),
        crate::ClientFamily::Rust,
    ));
    WebState {
        fleet_metrics: Arc::new(WebFleetMetrics::new(
            Arc::clone(&broker),
            FleetLabels::none(),
        )),
        events: EventBroadcaster::with_debounce(Arc::clone(&broker), Duration::from_millis(25)),
        broker,
        authorization_policy,
        schema_probe,
        task_stats_cache: Arc::new(TaskStatsCache::new()),
        task_facets_cache: Arc::new(ReadCache::new()),
        task_breakdown_cache: Arc::new(ReadCache::new()),
        actions,
        ui_config: MonitoringUiConfig::default(),
        assets,
        children: Arc::new(TestChildSpecs::empty()),
        heartbeat: Duration::from_millis(25),
        client_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

fn test_router(
    authorization_policy: Arc<dyn MonitoringAuthorizationPolicy>,
    schema: SchemaStatus,
    actions: Actions,
) -> Router {
    build_router(test_state(
        &lazy_pool(),
        authorization_policy,
        fixed_probe(schema),
        actions,
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

/// pydantic's message for the path id text `not-a-uuid` (pydantic 2.13.4:
/// `TypeAdapter(UUID).validate_python('not-a-uuid')`).
const NOT_A_UUID_MESSAGE: &str = "Input should be a valid UUID, invalid character: found `n` at 1";

/// The 400 body of the path id `not-a-uuid` in the path parameter `field`:
/// Python's `PathIdNotUuid.detail` with pydantic's message.
fn not_a_uuid_refusal(field: &str) -> serde_json::Value {
    serde_json::json!({
        "code": "INVALID_INPUT",
        "detail": format!("Path parameter {field}: {NOT_A_UUID_MESSAGE}."),
    })
}

async fn json(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

#[tokio::test]
async fn every_declared_api_route_is_view_authorized() {
    let router = test_router(
        Arc::new(TrustedHeader::new("X-Forwarded-User", Actions::Enabled).unwrap()),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
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
        "/api/workers/classes",
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

    let response = test_router(Arc::new(ViewOnly), unknown, Actions::Enabled)
        .oneshot(action_request(action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json(response).await,
        serde_json::json!({"detail": "Not authorized."})
    );

    let response = test_router(Arc::new(AllowAll), unknown, Actions::Disabled)
        .oneshot(request(Method::POST, action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json(response).await,
        serde_json::json!({"detail": "Not authorized."})
    );

    let response = test_router(Arc::new(AllowAll), unknown, Actions::Enabled)
        .oneshot(request(Method::POST, action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json(response).await,
        serde_json::json!({"detail": "Missing X-Horsies-Intent: action header."})
    );

    let response = test_router(Arc::new(AllowAll), unknown, Actions::Enabled)
        .oneshot(action_request(action))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(json(response).await["code"], SCHEMA_UNKNOWN);
}

/// Python `clients/python/tests/unit/test_web_auth.py::TestActGuard::test_rejects_a_wrong_intent_value`:
/// an intent header with another value is refused as a missing intent, before
/// the schema gate.
#[tokio::test]
async fn an_action_with_another_intent_value_is_refused() {
    let unknown = status(SchemaState::Unknown, None);
    let wrong_intent = Request::builder()
        .method(Method::POST)
        .uri("/api/tasks/not-a-uuid/cancel")
        .header(INTENT_HEADER, "read")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .expect("action request");

    let response = test_router(Arc::new(AllowAll), unknown, Actions::Enabled)
        .oneshot(wrong_intent)
        .await
        .expect("the router answers");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json(response).await,
        serde_json::json!({"detail": "Missing X-Horsies-Intent: action header."})
    );
}

#[tokio::test]
async fn task_action_body_defaults_when_absent_and_malformed_json_is_422() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );
    let absent = Request::builder()
        .method(Method::POST)
        .uri("/api/tasks/not-a-uuid/cancel")
        .header(INTENT_HEADER, INTENT_VALUE)
        .body(Body::empty())
        .unwrap();
    // The absent body is accepted; the refusal is the path id's.
    let absent_response = router.clone().oneshot(absent).await.unwrap();
    assert_eq!(absent_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json(absent_response).await, not_a_uuid_refusal("task_id"));

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

/// An action request to `uri` with the JSON body text `body`.
fn json_action_request(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(INTENT_HEADER, INTENT_VALUE)
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .expect("action request")
}

/// The 422 body of the unknown body key `key` with the value `input`: the
/// `extra_forbidden` issue, its message, and the keys that the body type
/// declares.
fn unknown_key_refusal(
    key: &str,
    input: &serde_json::Value,
    message: &str,
    known_keys: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({"detail": [{
        "type": "extra_forbidden",
        "loc": ["body", key],
        "msg": message,
        "input": input,
        "ctx": {"known_keys": known_keys},
    }]})
}

/// The router of the action tests: every verdict allows, the schema
/// matches, and the pool is lazy and unreachable, so a request that passed
/// the body and the path id answers 503 at its first statement.
fn allowed_action_router() -> Router {
    test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    )
}

/// The task cancel body refuses a key that `CancelTaskBody` does not
/// declare with 422 `extra_forbidden`, before the path id and any
/// statement. The issue names the first unknown key in body order, its
/// value and the known keys. The known body, `{}` and no body are accepted:
/// the refusal is then the path id's. A known key with a value of the wrong
/// type, and JSON that is not an object, are 422 `value_error`.
#[tokio::test]
async fn the_task_cancel_body_refuses_an_unknown_key_and_accepts_the_known_body() {
    let router = allowed_action_router();
    let path = format!("/api/tasks/{}/cancel", Uuid::new_v4());

    for (body, key, input) in [
        (
            r#"{"include_running": true}"#,
            "include_running",
            serde_json::json!(true),
        ),
        (
            r#"{"polcy": "INCLUDE_RUNNING"}"#,
            "polcy",
            serde_json::json!("INCLUDE_RUNNING"),
        ),
        (
            r#"{"policy": "INCLUDE_RUNNING", "zeta": 1, "alpha": 2}"#,
            "zeta",
            serde_json::json!(1),
        ),
    ] {
        let response = router
            .clone()
            .oneshot(json_action_request(&path, body))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{body}"
        );
        assert_eq!(
            json(response).await,
            unknown_key_refusal(
                key,
                &input,
                &format!("Unknown key \"{key}\". Known keys: \"policy\"."),
                &serde_json::json!(["policy"]),
            ),
            "{body}"
        );
    }

    for body in [r#"{"policy": "INCLUDE_RUNNING"}"#, "{}", ""] {
        let response = router
            .clone()
            .oneshot(json_action_request("/api/tasks/not-a-uuid/cancel", body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            json(response).await,
            not_a_uuid_refusal("task_id"),
            "{body}"
        );
    }

    for (body, start) in [
        (r#"{"policy": true}"#, "invalid type: boolean `true`"),
        (r#"{"policy": "yes"}"#, "unknown variant `yes`"),
    ] {
        let wrong_value = router
            .clone()
            .oneshot(json_action_request(&path, body))
            .await
            .unwrap();
        assert_eq!(
            wrong_value.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{body}"
        );
        let wrong_value = json(wrong_value).await;
        assert_eq!(wrong_value["detail"][0]["type"], "value_error", "{body}");
        assert_eq!(
            wrong_value["detail"][0]["loc"],
            serde_json::json!(["body"]),
            "{body}"
        );
        assert!(
            wrong_value["detail"][0]["msg"]
                .as_str()
                .is_some_and(|message| message.starts_with(start)),
            "{wrong_value}"
        );
    }

    for body in ["[true]", "true", "null"] {
        let response = router
            .clone()
            .oneshot(json_action_request(&path, body))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{body}"
        );
        assert_eq!(
            json(response).await,
            serde_json::json!({"detail": [{
                "type": "value_error",
                "loc": ["body"],
                "msg": "Input should be a valid JSON object",
                "input": body,
            }]}),
            "{body}"
        );
    }
}

/// A workflow action body has no keys: a key is refused with 422
/// `extra_forbidden` and the known keys `[]`, before the path id and any
/// statement. `{}` and no body are accepted: the refusal is then the path
/// id's.
async fn assert_the_workflow_body_has_no_keys(action: &str) {
    let router = allowed_action_router();
    let path = format!("/api/workflows/{}/{action}", Uuid::new_v4());

    let response = router
        .clone()
        .oneshot(json_action_request(
            &path,
            r#"{"policy": "INCLUDE_RUNNING"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "{action}"
    );
    assert_eq!(
        json(response).await,
        unknown_key_refusal(
            "policy",
            &serde_json::json!("INCLUDE_RUNNING"),
            "Unknown key \"policy\". Known keys: none.",
            &serde_json::json!([]),
        ),
        "{action}"
    );

    for body in ["{}", ""] {
        let response = router
            .clone()
            .oneshot(json_action_request(
                &format!("/api/workflows/not-a-uuid/{action}"),
                body,
            ))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{action} {body}"
        );
        assert_eq!(
            json(response).await,
            not_a_uuid_refusal("workflow_id"),
            "{action} {body}"
        );
    }
}

#[tokio::test]
async fn the_workflow_pause_body_refuses_every_key() {
    assert_the_workflow_body_has_no_keys("pause").await;
}

#[tokio::test]
async fn the_workflow_resume_body_refuses_every_key() {
    assert_the_workflow_body_has_no_keys("resume").await;
}

#[tokio::test]
async fn the_workflow_cancel_body_refuses_every_key() {
    assert_the_workflow_body_has_no_keys("cancel").await;
}

/// `WorkflowActionBody` refuses every key for any serde reader, and reads
/// `{}`.
#[test]
fn the_workflow_body_type_refuses_every_key() {
    let refused = serde_json::from_str::<WorkflowActionBody>(r#"{"policy": "INCLUDE_RUNNING"}"#)
        .expect_err("a key that the type does not declare");
    assert!(
        refused
            .to_string()
            .starts_with("unknown field `policy`, there are no fields"),
        "{refused}"
    );
    assert_eq!(
        serde_json::from_str::<WorkflowActionBody>("{}").ok(),
        Some(WorkflowActionBody {})
    );
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
        Actions::Enabled,
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
            "/api/workflows/00000000-0000-0000-0000-000000000000/tasks/not-an-int",
            serde_json::json!(["path", "task_index"]),
            serde_json::json!("not-an-int"),
            "int_parsing",
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

/// `/api/meta` reports the template of the trace link, or null when the
/// page shows the trace id as text.
#[tokio::test]
async fn the_meta_reports_the_trace_link_template_or_null() {
    const TEMPLATE: &str = "https://grafana.example.com/explore?traceId={trace_id}";
    let schema = status(SchemaState::Match, Some(crate::expected_schema_version()));
    let mut linked = test_state(
        &lazy_pool(),
        Arc::new(AllowAll),
        fixed_probe(schema),
        Actions::Enabled,
        MemoryAssets::standard(),
    );
    linked.ui_config.trace_link =
        TraceLink::Template(TraceLinkTemplate::parse(TEMPLATE).expect("the template is valid"));
    let plain = test_router(Arc::new(AllowAll), schema, Actions::Enabled);

    let linked_meta = json(
        build_router(linked)
            .oneshot(request(Method::GET, "/api/meta"))
            .await
            .unwrap(),
    )
    .await;
    let plain_meta = json(
        plain
            .oneshot(request(Method::GET, "/api/meta"))
            .await
            .unwrap(),
    )
    .await;

    assert_eq!(linked_meta["trace_link_template"], TEMPLATE);
    assert_eq!(plain_meta["trace_link_template"], serde_json::Value::Null);
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
            Some(crate::expected_schema_version() - 1),
            false,
            Some(SCHEMA_INCOMPATIBLE),
        ),
        (SchemaState::Absent, None, false, Some(SCHEMA_INCOMPATIBLE)),
        (SchemaState::ZeroX, None, false, Some(SCHEMA_INCOMPATIBLE)),
        (SchemaState::Unknown, None, false, Some(SCHEMA_UNKNOWN)),
    ];
    for (state, version, compatible, reason) in cases {
        let router = test_router(Arc::new(AllowAll), status(state, version), Actions::Enabled);
        let meta_response = router
            .clone()
            .oneshot(request(Method::GET, "/api/meta"))
            .await
            .unwrap();
        assert_eq!(meta_response.status(), StatusCode::OK);
        let meta = json(meta_response).await;
        assert_eq!(meta["schema_compatible"], compatible);
        assert_eq!(meta["schema_state"], serde_json::to_value(state).unwrap());
        let actions = match compatible {
            true => "ENABLED",
            false => "DISABLED",
        };
        assert_eq!(meta["actions"], actions);
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
            assert_eq!(action.status(), StatusCode::BAD_REQUEST);
        } else {
            assert_eq!(action.status(), StatusCode::CONFLICT);
            assert_eq!(json(action).await["code"], reason.unwrap());
        }

        let read = router
            .oneshot(request(Method::GET, "/api/tasks/not-a-uuid"))
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn every_action_route_inherits_the_schema_gate() {
    let router = test_router(
        Arc::new(AllowAll),
        status(
            SchemaState::Mismatch,
            Some(crate::expected_schema_version() - 1),
        ),
        Actions::Enabled,
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
            .contains("Actions are disabled until the versions match"));
    }
}

#[tokio::test]
async fn schema_probe_caches_successes_and_never_caches_a_cold_failure() {
    let expected = crate::expected_schema_version();
    let reader = SequenceReader::new([
        ReaderAnswer::Failure,
        ReaderAnswer::Status(status(SchemaState::Mismatch, Some(expected - 1))),
        ReaderAnswer::Failure,
    ]);
    let probe = SchemaProbe::with_reader(
        Arc::<SequenceReader>::clone(&reader),
        expected,
        Duration::ZERO,
    );

    assert_eq!(probe.status().await.state, SchemaState::Unknown);
    assert_eq!(probe.status().await.state, SchemaState::Mismatch);
    assert_eq!(probe.status().await.state, SchemaState::Mismatch);
    assert_eq!(reader.reads.load(Ordering::Relaxed), 3);

    let cached_reader = SequenceReader::new([ReaderAnswer::Status(status(
        SchemaState::Match,
        Some(expected),
    ))]);
    let cached = SchemaProbe::with_reader(
        Arc::<SequenceReader>::clone(&cached_reader),
        expected,
        Duration::from_secs(60),
    );
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
        &pool,
        Arc::new(AllowAll),
        fixed_probe(status(
            SchemaState::Match,
            Some(crate::expected_schema_version()),
        )),
        Actions::Enabled,
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
        &lazy_pool(),
        Arc::new(TrustedHeader::new("X-Forwarded-User", Actions::Disabled).unwrap()),
        fixed_probe(status(SchemaState::Unknown, None)),
        Actions::Disabled,
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
    assert!(page.contains(r#"{"basePath": "/", "apiBase": "/api"}"#));
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
    assert!(mounted.contains(r#"{"basePath": "/monitoring", "apiBase": "/monitoring/api"}"#));
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

/// The runtime config of the page is the text of Python's
/// `json.dumps(config)`: a space after each `:` and `,`, and every
/// character outside printable ASCII escaped (the texts come from
/// `json.dumps` on Python 3.13).
#[test]
fn the_runtime_config_is_the_text_of_python_json_dumps() {
    assert_eq!(
        runtime_config_json("/monitoring", "/monitoring/api"),
        r#"{"basePath": "/monitoring", "apiBase": "/monitoring/api"}"#
    );
    assert_eq!(
        runtime_config_json("/m\u{f6}n \"x\"", "/m\u{f6}n \"x\"/api"),
        r#"{"basePath": "/m\u00f6n \"x\"", "apiBase": "/m\u00f6n \"x\"/api"}"#
    );
}

/// Python `clients/python/tests/unit/test_web_app_shell.py::TestStaticServing::test_built_assets_are_served`:
/// a built asset is served with the bytes of its file.
#[tokio::test]
async fn a_built_asset_is_served_with_its_bytes() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Unknown, None),
        Actions::Enabled,
    );

    let asset = router
        .oneshot(request(Method::GET, "/assets/app.js"))
        .await
        .unwrap();

    assert_eq!(asset.status(), StatusCode::OK);
    let body = to_bytes(asset.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"console.log('horsies')");
}

/// Python `clients/python/tests/unit/test_web_app_shell.py::TestStaticServing::test_no_custom_css_means_no_extra_stylesheet`:
/// with no custom stylesheet, the page gets no stylesheet link.
#[tokio::test]
async fn no_custom_css_means_no_extra_stylesheet() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Unknown, None),
        Actions::Enabled,
    );

    let page = router.oneshot(request(Method::GET, "/")).await.unwrap();

    assert_eq!(page.status(), StatusCode::OK);
    let page = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(page.contains("window.__HORSIES_UI__"), "{page}");
    assert!(!page.contains("<link rel=\"stylesheet\""), "{page}");
}

/// Python `clients/python/tests/unit/test_web_app_shell.py::test_base_href_is_always_a_directory`:
/// the base of the page ends with a slash under every mount, so relative
/// asset URLs resolve against the mount.
#[test]
fn the_base_href_is_always_a_directory() {
    for (root_path, base) in [
        ("", "<base href=\"/\">"),
        ("/monitoring", "<base href=\"/monitoring/\">"),
        ("/a/b", "<base href=\"/a/b/\">"),
    ] {
        let page = inject("<html><head></head><body></body></html>", root_path, None);
        assert!(page.contains(base), "{root_path:?}: {page}");
    }
}

/// Python `clients/python/tests/unit/test_web_app_shell.py::test_asset_resolution_rejects_traversal`:
/// a request path resolves to an asset only for a built file inside the
/// tree; not for the page, an empty path, a missing file, or a path that
/// leaves the tree.
#[test]
fn an_asset_resolves_only_to_a_built_file_inside_the_tree() {
    let assets = MemoryAssets::standard();
    let resolve = |path: &str| {
        safe_asset_path(path)
            .and_then(|asset_path| super::spa::AssetStore::get(assets.as_ref(), &asset_path))
    };

    assert!(resolve("assets/app.js").is_some());
    for path in ["../../etc/passwd", "index.html", "", "nope.js"] {
        assert!(resolve(path).is_none(), "{path:?}");
    }
}

#[tokio::test]
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

/// Python `clients/python/tests/unit/test_web_app_shell.py::TestMissingBuild`: the page
/// names the missing build and the command that builds it; the API answers.
#[tokio::test]
async fn missing_embedded_index_is_a_factual_503_while_api_still_answers() {
    let state = test_state(
        &lazy_pool(),
        Arc::new(AllowAll),
        fixed_probe(status(SchemaState::Unknown, None)),
        Actions::Enabled,
        MemoryAssets::empty(),
    );
    let router = build_router(state);
    let page = router
        .clone()
        .oneshot(request(Method::GET, "/"))
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::SERVICE_UNAVAILABLE);
    let page = json(page).await;
    let detail = page["detail"].as_str().unwrap();
    assert!(detail.contains("assets are not built"), "{detail}");
    assert!(
        detail.contains("bun run build, then rebuild horsies-core"),
        "{detail}"
    );
    assert_eq!(
        router
            .oneshot(request(Method::GET, "/api/meta"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

/// Python `test_web_api.py::TestWorkerReads::test_worker_list_carries_the_slot_health`.
#[tokio::test]
async fn worker_list_carries_the_slot_health() {
    let state = migrated_state().await;
    let pool = migrated_pool().await;
    let worker_id = format!("itest-{}", Uuid::new_v4().simple());
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, worker_started_at,
            slots_down, class_capacity, telemetry
        ) VALUES (
            $1, now(), 'itest-host', 4242, 2, 0, 2,
            ARRAY['default'], '{}'::jsonb, '{\"default\": 3}'::jsonb, '{}'::jsonb,
            0, 0, now(),
            $2, '{}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
        )",
    )
    .bind(&worker_id)
    .bind(serde_json::json!([{
        "slot": 1,
        "cause": {"kind": "resource_shortage", "errno": "EMFILE"},
        "detail": "OSError: [Errno 24] Too many open files",
        "down_since": "2026-11-10T12:00:00+00:00",
        "failed_boots": 2,
    }]))
    .execute(&pool)
    .await
    .unwrap();

    let response = build_router(state)
        .oneshot(request(Method::GET, "/api/workers"))
        .await
        .unwrap();
    let status = response.status();
    let body = json(response).await;
    sql::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(&worker_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::OK);
    let worker = body
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["worker_id"] == worker_id.as_str())
        .expect("the worker is listed");
    assert_eq!(worker["slot_health"], "slots_down");
    assert_eq!(
        worker["down_slots"],
        serde_json::json!([{
            "slot": 1,
            "cause": "the OS ran short of a resource (EMFILE)",
            "detail": "OSError: [Errno 24] Too many open files",
            "down_since": "2026-11-10T12:00:00Z",
            "failed_boots": 2,
        }])
    );
    assert_eq!(
        worker["queue_max_concurrency"],
        serde_json::json!({"default": 3})
    );
}

/// A listed state of a worker that started at `worker_started_at`, whose
/// snapshot is at `snapshot_at` with the stored `profile_name`, read by a
/// list statement at `read_at`.
fn listed_worker(
    snapshot_at: chrono::DateTime<chrono::Utc>,
    worker_started_at: chrono::DateTime<chrono::Utc>,
    read_at: chrono::DateTime<chrono::Utc>,
    profile_name: Option<String>,
) -> crate::broker::health::ListedWorkerState {
    let stored = crate::broker::health::StoredWorkerState {
        worker_id: "w-clock".to_owned(),
        snapshot_at,
        hostname: "itest-host".to_owned(),
        pid: 4242,
        processes: 2,
        max_claim_batch: 0,
        max_claim_per_worker: 2,
        cluster_wide_cap: None,
        queues: vec!["default".to_owned()],
        queue_priorities: serde_json::json!({}),
        queue_max_concurrency: serde_json::json!({}),
        recovery_config: serde_json::json!({}),
        tasks_running: 0,
        tasks_claimed: 0,
        memory_usage_mb: None,
        memory_percent: None,
        cpu_percent: None,
        children_memory_mb: None,
        worker_started_at,
        slots_down: serde_json::json!([]),
        class_capacity: serde_json::json!({}),
        profile_name,
    };
    crate::broker::health::ListedWorkerState {
        snapshot: stored.into_snapshot().expect("a readable stored row"),
        read_at,
    }
}

/// The worker list measures the snapshot age, the staleness and the uptime
/// to the database clock of the list statement, not to the web host clock
/// (#765). The instants are days before any run of this test, so an age
/// measured to the host clock is days long and stale. Each value is exact
/// to the microsecond, and a snapshot one microsecond past the live window
/// is stale.
#[test]
fn worker_list_ages_are_measured_to_the_read_time() {
    let snapshot_at = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 11, 10, 12, 0, 0)
        .single()
        .expect("a valid instant");
    let started_at = snapshot_at - chrono::TimeDelta::hours(1);
    let threshold =
        chrono::TimeDelta::seconds(crate::core::defaults::WORKER_STATE_LIVE_WINDOW_S as i64);
    let one_microsecond = chrono::TimeDelta::microseconds(1);
    let fresh_age = chrono::TimeDelta::microseconds(30_000_250);

    let fresh = super::routes::worker_state(listed_worker(
        snapshot_at,
        started_at,
        snapshot_at + fresh_age,
        Some("default".to_owned()),
    ));
    let at_threshold = super::routes::worker_state(listed_worker(
        snapshot_at,
        started_at,
        snapshot_at + threshold,
        Some("default".to_owned()),
    ));
    let past_threshold = super::routes::worker_state(listed_worker(
        snapshot_at,
        started_at,
        snapshot_at + threshold + one_microsecond,
        Some("default".to_owned()),
    ));

    assert_eq!(fresh.snapshot_age_seconds, Some(fresh_age));
    assert!(!fresh.stale);
    assert_eq!(
        fresh.uptime_seconds,
        Some(chrono::TimeDelta::hours(1) + fresh_age)
    );
    assert_eq!(at_threshold.snapshot_age_seconds, Some(threshold));
    assert!(!at_threshold.stale, "stale only past the live window");
    assert_eq!(
        past_threshold.snapshot_age_seconds,
        Some(threshold + one_microsecond)
    );
    assert!(
        past_threshold.stale,
        "stale one microsecond past the live window"
    );
    let encoded = serde_json::to_value(&fresh).expect("serialize the worker state");
    assert_eq!(encoded["snapshot_age_seconds"], serde_json::json!(30.00025));
    assert_eq!(encoded["uptime_seconds"], serde_json::json!(3630.00025));
}

/// The `profile` of a worker state is an object tagged by `kind`: a stored
/// profile name is `named` with the name, and a row with no name (written by
/// a worker of a build before migration 1004) is `not_recorded`.
#[test]
fn the_profile_of_a_worker_state_is_tagged_by_kind() {
    let at = chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 11, 10, 12, 0, 0)
        .single()
        .expect("a valid instant");

    let named = super::routes::worker_state(listed_worker(at, at, at, Some("batch".to_owned())));
    let not_recorded = super::routes::worker_state(listed_worker(at, at, at, None));

    assert_eq!(
        serde_json::to_value(&named).expect("serialize the worker state")["profile"],
        serde_json::json!({"kind": "named", "name": "batch"})
    );
    assert_eq!(
        serde_json::to_value(&not_recorded).expect("serialize the worker state")["profile"],
        serde_json::json!({"kind": "not_recorded"})
    );
}

/// `GET /api/workers` returns the profile of the latest snapshot of each
/// worker: the stored name, or `not_recorded` for a row with no name.
#[tokio::test]
async fn worker_list_carries_the_profile_of_each_worker() {
    let state = migrated_state().await;
    let pool = migrated_pool().await;
    let named_id = format!("itest-{}", Uuid::new_v4().simple());
    let unnamed_id = format!("itest-{}", Uuid::new_v4().simple());
    for (worker_id, profile_name) in [(&named_id, Some("batch")), (&unnamed_id, None)] {
        sql::query(
            "INSERT INTO horsies_worker_states (
                worker_id, snapshot_at, hostname, pid, processes,
                max_claim_batch, max_claim_per_worker, queues,
                queue_priorities, queue_max_concurrency, recovery_config,
                tasks_running, tasks_claimed, worker_started_at,
                slots_down, class_capacity, telemetry, profile_name
            ) VALUES (
                $1, now(), 'itest-host', 4242, 2, 0, 2,
                ARRAY['default'], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                0, 0, now(),
                '[]'::jsonb, '{}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off')),
                $2
            )",
        )
        .bind(worker_id)
        .bind(profile_name)
        .execute(&pool)
        .await
        .unwrap();
    }

    let response = build_router(state)
        .oneshot(request(Method::GET, "/api/workers"))
        .await
        .unwrap();
    let status = response.status();
    let body = json(response).await;
    sql::query("DELETE FROM horsies_worker_states WHERE worker_id = ANY($1)")
        .bind(vec![named_id.clone(), unnamed_id.clone()])
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::OK);
    let listed = body.as_array().expect("the worker list is a JSON array");
    let profile_of = |worker_id: &str| -> serde_json::Value {
        let entry = listed
            .iter()
            .find(|entry| entry["worker_id"] == worker_id)
            .unwrap_or_else(|| panic!("{worker_id} is listed"));
        entry["profile"].clone()
    };
    assert_eq!(
        profile_of(named_id.as_str()),
        serde_json::json!({"kind": "named", "name": "batch"})
    );
    assert_eq!(
        profile_of(unnamed_id.as_str()),
        serde_json::json!({"kind": "not_recorded"})
    );
}

/// A snapshot written ten minutes before the read in database time is stale,
/// whatever the web host clock is (#765). Python
/// `test_web_api.py::TestWorkerReads::test_worker_list_answers` for the
/// shape: 200 and a JSON array.
#[tokio::test]
async fn a_snapshot_ten_minutes_old_in_database_time_is_stale() {
    let state = migrated_state().await;
    let pool = migrated_pool().await;
    let worker_id = format!("itest-{}", Uuid::new_v4().simple());
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, worker_started_at,
            slots_down, class_capacity, telemetry
        ) VALUES (
            $1, now() - interval '10 minutes', 'itest-host', 4242, 2, 0, 2,
            ARRAY['default'], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, now() - interval '1 hour',
            '[]'::jsonb, '{}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
        )",
    )
    .bind(&worker_id)
    .execute(&pool)
    .await
    .unwrap();

    let response = build_router(state)
        .oneshot(request(Method::GET, "/api/workers"))
        .await
        .unwrap();
    let status = response.status();
    let body = json(response).await;
    sql::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(&worker_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::OK);
    let worker = body
        .as_array()
        .expect("the worker list is a JSON array")
        .iter()
        .find(|entry| entry["worker_id"] == worker_id.as_str())
        .expect("the worker is listed");
    assert_eq!(worker["stale"], true);
    assert!(
        worker["snapshot_age_seconds"].is_f64() && worker["uptime_seconds"].is_f64(),
        "the durations are JSON floats: {worker}"
    );
    let age = worker["snapshot_age_seconds"]
        .as_f64()
        .expect("an age in seconds");
    assert!(age >= 600.0, "snapshot_age_seconds {age}");
    let uptime = worker["uptime_seconds"]
        .as_f64()
        .expect("an uptime in seconds");
    assert!(uptime >= 3_600.0, "uptime_seconds {uptime}");
}

/// Python `test_web_api.py::TestWorkerReads::test_class_report_answers_with_streams_and_budgets`.
/// The test reads only its own queue and worker.
#[tokio::test]
async fn class_report_answers_with_streams_and_budgets() {
    let state = migrated_state().await;
    let pool = migrated_pool().await;
    let suffix = Uuid::new_v4().simple().to_string();
    let worker_id = format!("w-async-{suffix}");
    let queue = format!("q-{}", &suffix[..12]);
    sql::query(
        "INSERT INTO horsies_worker_states (
            worker_id, snapshot_at, hostname, pid, processes,
            max_claim_batch, max_claim_per_worker, queues,
            queue_priorities, queue_max_concurrency, recovery_config,
            tasks_running, tasks_claimed, worker_started_at,
            slots_down, class_capacity, telemetry
        ) VALUES (
            $1, now(), 'itest-host', 4242, 2, 0, 2,
            ARRAY[$2::text], '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
            0, 0, now(), '[]'::jsonb,
            '{\"SHARED_ASYNC\": {\"capacity\": 8, \"serves\": [\"SHARED_ASYNC\"]}}'::jsonb, jsonb_build_object('endpoint', jsonb_build_object('state', 'off'))
        )",
    )
    .bind(&worker_id)
    .bind(&queue)
    .execute(&pool)
    .await
    .unwrap();
    let task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
            is_workflow_task, retry_count, max_retries, command_fingerprint_version,
            command_fingerprint, retention_class_key, retain_rerun_input, created_at,
            updated_at, execution
        ) VALUES (
            $1, 'class_report_task', $2, 100, 'null', 'PENDING', NOW(), NOW() - interval '5 seconds',
            FALSE, 0, 3, 1, decode(repeat('0d', 32), 'hex'), 'forever', FALSE, NOW(),
            NOW(), 'ISOLATED'
        )",
    )
    .bind(task_id)
    .bind(&queue)
    .execute(&pool)
    .await
    .unwrap();

    let response = build_router(state)
        .oneshot(request(Method::GET, "/api/workers/classes"))
        .await
        .unwrap();
    let status = response.status();
    let body = json(response).await;
    sql::query("DELETE FROM horsies_worker_states WHERE worker_id = $1")
        .bind(&worker_id)
        .execute(&pool)
        .await
        .unwrap();
    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["live_window_s"], 120);
    assert!(body.get("not_recorded").is_none());
    let streams: Vec<&serde_json::Value> = body["streams"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|stream| stream["queue_name"] == queue.as_str())
        .collect();
    let [stream] = streams.as_slice() else {
        panic!("one stream of the test queue: {body}");
    };
    assert_eq!(stream["execution"], "ISOLATED");
    assert_eq!(stream["coverage"], serde_json::json!({"kind": "unserved"}));
    assert_eq!(stream["wait"]["kind"], "waiting");
    let worker = body["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|worker| worker["worker_id"] == worker_id.as_str())
        .expect("the live worker is listed");
    assert_eq!(
        worker["budgets"],
        serde_json::json!([{
            "budget": "SHARED_ASYNC",
            "capacity": 8,
            "serves": ["SHARED_ASYNC"],
            "in_flight": 0,
            "free": 8,
        }])
    );
}

async fn clean_w4(pool: &TestPool, prefix: &str) {
    let pattern = format!("{prefix}%");
    for statement in [
        "DELETE FROM horsies_task_history WHERE task_name LIKE $1",
        "DELETE FROM horsies_tasks WHERE task_name LIKE $1",
        "DELETE FROM horsies_workflows WHERE name LIKE $1",
    ] {
        sql::query(statement)
            .bind(&pattern)
            .execute(pool)
            .await
            .unwrap();
    }
}

async fn migrated_state() -> WebState {
    let pool = migrated_pool().await;
    let database_url = migrated_database_url().await;
    // The worker ping route needs the broker's listener.
    let broker = Arc::new(PostgresBroker::from_test_url_and_session_url(
        pool.url(),
        &database_url,
        crate::ClientFamily::Rust,
    ));
    WebState {
        schema_probe: Arc::new(SchemaProbe::new(Arc::clone(&broker))),
        task_stats_cache: Arc::new(TaskStatsCache::new()),
        task_facets_cache: Arc::new(ReadCache::new()),
        task_breakdown_cache: Arc::new(ReadCache::new()),
        fleet_metrics: Arc::new(WebFleetMetrics::new(
            Arc::clone(&broker),
            FleetLabels::none(),
        )),
        events: EventBroadcaster::with_debounce(Arc::clone(&broker), Duration::from_millis(25)),
        broker,
        authorization_policy: Arc::new(AllowAll),
        actions: Actions::Enabled,
        ui_config: MonitoringUiConfig::default(),
        assets: MemoryAssets::standard(),
        children: Arc::new(TestChildSpecs::empty()),
        heartbeat: Duration::from_millis(25),
        client_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

async fn seed_pending(pool: &TestPool, prefix: &str) -> Uuid {
    let id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, status, sent_at, enqueued_at,
            is_workflow_task, retry_count, max_retries, command_fingerprint_version,
            command_fingerprint, retention_class_key, retain_rerun_input, created_at,
            updated_at, execution
        ) VALUES (
            $1, $2, 'default', 100, 'null', 'PENDING', NOW(), NOW(), FALSE, 0, 3, 1,
            decode(repeat('0d', 32), 'hex'), 'forever', FALSE, NOW(), NOW(), 'ISOLATED'
        )",
    )
    .bind(id)
    .bind(format!("{prefix}task_{}", id.simple()))
    .execute(pool)
    .await
    .unwrap();
    id
}

fn status_count(stats: &serde_json::Value, status: &str) -> i64 {
    stats
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["status"] == status)
        .and_then(|entry| entry["count"].as_i64())
        .unwrap()
}

#[tokio::test]
async fn task_stats_route_reuses_a_successful_scope_within_the_ttl() {
    let state = migrated_state().await;
    let prefix = format!("stats_cache_{}__", Uuid::new_v4().simple());
    clean_w4(&migrated_pool().await, &prefix).await;
    let task_id = seed_pending(&migrated_pool().await, &prefix).await;
    let task_name = format!("{prefix}task_{}", task_id.simple());
    let path = format!("/api/tasks/stats?task_name={task_name}");
    let router = build_router(state.clone());

    let first = router
        .clone()
        .oneshot(request(Method::GET, &path))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first = json(first).await;
    assert_eq!(status_count(&first, "PENDING"), 1);

    sql::query("DELETE FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .execute(&migrated_pool().await)
        .await
        .unwrap();

    let second = router.oneshot(request(Method::GET, &path)).await.unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(json(second).await, first);
    clean_w4(&migrated_pool().await, &prefix).await;
}

#[tokio::test]
async fn all_http_surfaces_route_to_the_monitoring_and_action_layers() {
    let state = migrated_state().await;
    let prefix = format!("w4_http_{}__", Uuid::new_v4().simple());
    clean_w4(&migrated_pool().await, &prefix).await;
    let task_id = seed_pending(&migrated_pool().await, &prefix).await;
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
        "/api/workers/classes",
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
            "warning": null
        })
    );
    clean_w4(&migrated_pool().await, &prefix).await;
}

#[tokio::test]
#[serial(timing)]
async fn broadcaster_subscribes_on_the_broker_listener_and_batches_real_notify_bursts() {
    let pool = migrated_pool().await;
    let broker = Arc::new(PostgresBroker::from_test_url_and_session_url(
        pool.url(),
        &migrated_database_url().await,
        crate::ClientFamily::Rust,
    ));
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
        sql::query("SELECT pg_notify($1, $2)")
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

/// The listener loses its connection: it connects again and LISTENs again,
/// and the stream sends one event with no ids per topic, because
/// notifications sent in between are lost.
#[tokio::test]
async fn a_listener_gap_sends_one_refresh_per_topic() {
    let pool = migrated_pool().await;
    let url = migrated_database_url().await;
    let application_name = format!("web_events_gap_{}", Uuid::new_v4().simple());
    let separator = if url.contains('?') { '&' } else { '?' };
    let broker = Arc::new(PostgresBroker::from_test_url_and_session_url(
        pool.url(),
        &format!("{url}{separator}application_name={application_name}"),
        crate::ClientFamily::Rust,
    ));
    let broadcaster = EventBroadcaster::with_debounce(broker, Duration::from_millis(40));
    let mut subscription = broadcaster.subscribe().await;

    let terminated: i64 = sql::query_scalar(
        "SELECT count(*) FROM (
             SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE application_name = $1
         ) AS killed",
    )
    .bind(&application_name)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(terminated, 1, "exactly the listener backend is terminated");

    // Events with ids come from other writers of the test database; the
    // refreshes are the events with no ids.
    let mut refreshed = std::collections::BTreeSet::new();
    while refreshed.len() < CHANNEL_TOPICS.len() {
        let event = tokio::time::timeout(Duration::from_secs(10), subscription.recv())
            .await
            .expect("refresh timeout")
            .expect("subscription open")
            .expect("not degraded");
        if event.ids.is_empty() {
            refreshed.insert(event.topic);
        }
    }
    let topics: Vec<&str> = refreshed.iter().map(String::as_str).collect();
    assert_eq!(topics, ["tasks", "workers", "workflows"]);
    broadcaster.close().await;
}

/// A session URL that cannot be reached: the subscriptions fail to start and
/// every client reads the degraded marker.
#[tokio::test]
async fn a_listener_that_cannot_start_degrades_the_stream() {
    let broker = Arc::new(PostgresBroker::from_test_url_and_session_url(
        lazy_pool().url(),
        "postgresql://postgres:pw@127.0.0.1:1/none",
        crate::ClientFamily::Rust,
    ));
    let broadcaster = EventBroadcaster::with_debounce(broker, Duration::from_millis(25));
    let mut subscription = broadcaster.subscribe().await;

    let item = tokio::time::timeout(Duration::from_secs(5), subscription.recv())
        .await
        .expect("degraded marker timeout");

    assert_eq!(item, Some(None));
    assert!(broadcaster.degraded());
}

#[tokio::test]
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
async fn observe_only_construction_runs_no_ddl_or_fleet_gate() {
    let database = crate::test_support::TestDatabase::create_empty().await;
    let broker = Arc::new(
        PostgresBroker::connect_observe_only(
            &AppConfig::for_database_url(database.url()).broker,
            crate::ClientFamily::Rust,
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        broker.schema_initialization_mode(),
        SchemaInitializationMode::ObserveOnly
    );
    broker.ensure_schema_initialized().await.unwrap();
    broker.migrate().await.unwrap();
    let row = database
        .client()
        .query_one(
            "SELECT to_regclass('horsies_tasks')::text, to_regclass('horsies_migrations')::text",
            &[],
        )
        .await
        .unwrap();
    let relations: (Option<String>, Option<String>) = (row.get(0), row.get(1));
    assert_eq!(relations, (None, None));
    let schema = SchemaProbe::new(Arc::clone(&broker)).status().await;
    assert_eq!(schema.state, SchemaState::Absent);
    broker.close().await;
    drop(broker);
    database.drop().await;
}

#[tokio::test]
async fn default_broker_mode_still_migrates_and_validates() {
    let broker = PostgresBroker::from_test_url(lazy_pool().url(), crate::ClientFamily::Rust);
    assert_eq!(
        broker.schema_initialization_mode(),
        SchemaInitializationMode::MigrateAndValidate
    );
}

/// A task path id that is not a UUID answers 400 `INVALID_INPUT`, as the
/// Python web app does (`require_path_uuid`), with no database call (the
/// router's pool is lazy and unreachable). The detail names the parameter
/// and carries pydantic's message.
#[tokio::test]
async fn a_task_path_id_that_is_not_a_uuid_is_invalid_input() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );
    let read = router
        .clone()
        .oneshot(request(Method::GET, "/api/tasks/not-a-uuid"))
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json(read).await, not_a_uuid_refusal("task_id"));

    let cancel = router
        .oneshot(action_request("/api/tasks/not-a-uuid/cancel"))
        .await
        .unwrap();
    assert_eq!(cancel.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json(cancel).await, not_a_uuid_refusal("task_id"));
}

/// A workflow id text that is not a UUID names no run on the two read
/// routes: 404 with the not-found text of the Python web app, which takes
/// the id as text (`get_workflow_run`, `get_workflow_node`), and no database
/// call (the router's pool is lazy and unreachable). An index that is not an
/// integer is still FastAPI's 422, and it names only `task_index`.
#[tokio::test]
async fn a_workflow_read_path_id_that_is_not_a_uuid_is_not_found() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );
    let run = router
        .clone()
        .oneshot(request(Method::GET, "/api/workflows/not-a-uuid"))
        .await
        .unwrap();
    assert_eq!(run.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json(run).await,
        serde_json::json!({"detail": "Workflow run not found."})
    );

    let node = router
        .clone()
        .oneshot(request(Method::GET, "/api/workflows/not-a-uuid/tasks/3"))
        .await
        .unwrap();
    assert_eq!(node.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json(node).await,
        serde_json::json!({"detail": "Workflow task not found."})
    );

    let both = router
        .oneshot(request(
            Method::GET,
            "/api/workflows/not-a-uuid/tasks/not-an-int",
        ))
        .await
        .unwrap();
    assert_eq!(both.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = json(both).await;
    let issues = body["detail"]
        .as_array()
        .expect("the 422 body lists the refused parameters");
    assert_eq!(issues.len(), 1, "{body}");
    assert_eq!(issues[0]["loc"], serde_json::json!(["path", "task_index"]));
    assert_eq!(issues[0]["type"], "int_parsing");
}

/// The read routes take the workflow id text as Python's `uuid.UUID(text)`
/// does. It refuses `urn:uuid:` in uppercase, which the `uuid` crate reads:
/// such a text names no run, 404 with no database call (the router's pool
/// is lazy and unreachable).
#[tokio::test]
async fn a_workflow_read_path_id_that_python_refuses_is_not_found() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );
    let id = "URN:UUID:00345678-abcd-4f12-8456-7890abcdef12";
    for (path, detail) in [
        (format!("/api/workflows/{id}"), "Workflow run not found."),
        (
            format!("/api/workflows/{id}/tasks/0"),
            "Workflow task not found.",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(request(Method::GET, &path))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(
            json(response).await,
            serde_json::json!({ "detail": detail })
        );
    }
}

/// A workflow path id that is not a UUID on an action route answers 400
/// `INVALID_INPUT`, as the task cancel route and the Python web app do
/// (`require_path_uuid`), with no database call (the router's pool is lazy
/// and unreachable). The detail names `workflow_id` and carries pydantic's
/// message.
#[tokio::test]
async fn a_workflow_action_path_id_that_is_not_a_uuid_is_invalid_input() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );
    let refusal = not_a_uuid_refusal("workflow_id");
    for path in [
        "/api/workflows/not-a-uuid/pause",
        "/api/workflows/not-a-uuid/resume",
        "/api/workflows/not-a-uuid/cancel",
    ] {
        let response = router.clone().oneshot(action_request(path)).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(json(response).await, refusal, "{path}");
    }
}

/// A node index that pydantic reads as an integer outside the 32-bit node
/// index column names no node: 404 with the not-found text, and no database
/// call (the router's pool is lazy and unreachable). Text over pydantic's
/// size bound is FastAPI's 422 `int_parsing_size`.
#[tokio::test]
async fn a_node_index_outside_the_integer_column_is_not_found() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );
    let run_id = uuid::Uuid::new_v4();
    let huge = "1".repeat(40);
    for index in ["2147483648", "-2147483649", huge.as_str()] {
        let response = router
            .clone()
            .oneshot(request(
                Method::GET,
                &format!("/api/workflows/{run_id}/tasks/{index}"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{index}");
        assert_eq!(
            json(response).await,
            serde_json::json!({"detail": "Workflow task not found."}),
            "{index}"
        );
    }

    let too_long = "1".repeat(4_301);
    let response = router
        .oneshot(request(
            Method::GET,
            &format!("/api/workflows/{run_id}/tasks/{too_long}"),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = json(response).await;
    assert_eq!(body["detail"][0]["type"], "int_parsing_size", "{body}");
    assert_eq!(
        body["detail"][0]["loc"],
        serde_json::json!(["path", "task_index"])
    );
}

/// The 404 text of each absence reason.
#[test]
fn each_absence_reason_has_the_not_found_text_of_python() {
    for (reason, text) in [
        (
            TaskAbsenceReason::PredatesRetainedHistory,
            "Task not found. Its id is older than the retained task history.",
        ),
        (TaskAbsenceReason::WithinRetainedHistory, "Task not found."),
        (TaskAbsenceReason::NotClassified, "Task not found."),
    ] {
        assert_eq!(task_not_found_text(reason), text, "{reason:?}");
    }
}

/// A task detail read that fails is never answered as an absent task. A history contract
/// violation is a server error (500); a database failure (the router's pool
/// is lazy and unreachable) is unavailable (503).
#[tokio::test]
async fn a_task_detail_read_that_fails_is_not_a_not_found() {
    let contract = super::common::query_failed(
        "Task detail",
        &crate::monitoring::MonitoringQueryError::HistoryContractViolation {
            message: "task detail history read failed: bad row".to_owned(),
            source: crate::monitoring::HistoryContractSource(
                crate::monitoring::MonitoringQueryErrorSource::History(
                    crate::core::history::errors::HistoryError::Contract("bad row".to_owned()),
                ),
            ),
        },
    );
    let contract = axum::response::IntoResponse::into_response(contract);
    assert_eq!(contract.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );
    let read = router
        .oneshot(request(
            Method::GET,
            &format!("/api/tasks/{}", Uuid::new_v4()),
        ))
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Each read kind that a database failure ends, with the start of its
/// detail: monitoring queries, and the worker reads of the broker.
const FAILING_READS: [(&str, &str); 4] = [
    ("/api/workflows/names", "Workflow names query failed: "),
    ("/api/workers", "Worker state query failed: "),
    ("/api/workers/classes", "Class report query failed: "),
    ("/api/workers/w1/history", "Worker history query failed: "),
];

/// The four action paths, on a task id and a workflow id that no row has.
fn failing_actions() -> [String; 4] {
    let task_id = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();
    [
        format!("/api/tasks/{task_id}/cancel"),
        format!("/api/workflows/{workflow_id}/pause"),
        format!("/api/workflows/{workflow_id}/resume"),
        format!("/api/workflows/{workflow_id}/cancel"),
    ]
}

/// Assert that every read of [`FAILING_READS`] and every action of
/// [`failing_actions`] answers `expected` with a `detail` text.
async fn assert_failures_answer(router: &Router, expected: StatusCode) {
    for (path, detail_start) in FAILING_READS {
        let response = router
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();
        let status = response.status();
        let body = json(response).await;
        assert_eq!(status, expected, "{path}: {body}");
        assert!(
            body["detail"]
                .as_str()
                .is_some_and(|detail| detail.starts_with(detail_start)),
            "{path}: {body}"
        );
    }
    for path in failing_actions() {
        let response = router.clone().oneshot(action_request(&path)).await.unwrap();
        let status = response.status();
        let body = json(response).await;
        assert_eq!(status, expected, "{path}: {body}");
        assert!(body["detail"].is_string(), "{path}: {body}");
    }
}

/// A database that refuses the connect (nothing listens on its port): a
/// cause that may clear. Every read kind and every action answers 503.
#[tokio::test]
async fn a_database_failure_that_may_clear_is_503_on_every_route_kind() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );

    assert_failures_answer(&router, StatusCode::SERVICE_UNAVAILABLE).await;
}

/// A database with no horsies relation or function: each read statement,
/// and the client family check of each action, is refused with an SQLSTATE
/// of class 42 (undefined table or function), a cause that stays. Every read
/// kind and every action answers 500: a repeat fails until the schema
/// changes. The schema probe reads a matching schema, so each action reaches
/// its first statement.
#[tokio::test]
async fn a_database_failure_that_stays_is_500_on_every_route_kind() {
    let database = crate::test_support::TestDatabase::create_empty().await;
    let pool = TestPoolOptions::new().connect_lazy(database.url());
    let state = test_state(
        &pool,
        Arc::new(AllowAll),
        fixed_probe(status(
            SchemaState::Match,
            Some(crate::expected_schema_version()),
        )),
        Actions::Enabled,
        MemoryAssets::standard(),
    );
    let broker = Arc::clone(&state.broker);
    let router = build_router(state);

    assert_failures_answer(&router, StatusCode::INTERNAL_SERVER_ERROR).await;
    // The test broker has no listener, so a worker ping cannot run: that
    // stays too.
    let ping = router
        .clone()
        .oneshot(request(
            Method::GET,
            "/api/workers/ping?timeout_seconds=0.1",
        ))
        .await
        .unwrap();
    assert_eq!(ping.status(), StatusCode::INTERNAL_SERVER_ERROR);

    drop(router);
    broker.close().await;
    database.drop().await;
}

/// A UUIDv7 whose embedded birth is `birth`, with random tail bits.
fn uuid7_born_at(birth: chrono::DateTime<chrono::Utc>) -> Uuid {
    let milliseconds =
        u128::try_from(birth.timestamp_millis()).expect("the birth is after the Unix epoch");
    let tail = Uuid::new_v4().as_u128() & ((1_u128 << 80) - 1);
    let value = (milliseconds << 80) | tail;
    let value = (value & !(0xF_u128 << 76)) | (0x7_u128 << 76);
    let value = (value & !(0x3_u128 << 62)) | (0x2_u128 << 62);
    Uuid::from_u128(value)
}

/// Python `test_web_api.py::TestTaskReads::test_unknown_task_is_not_found`,
/// and the route through the database for each text: an id born before the
/// upper anchor of a detached period leaf answers that it is older than the
/// retained history; a UUIDv4 and a later UUIDv7 answer `Task not found.`.
#[tokio::test]
async fn an_absent_task_is_not_found_with_the_text_of_its_absence_reason() {
    let state = migrated_state().await;
    let detached_upper = chrono::Utc::now() - chrono::Duration::days(1);
    sql::query(
        "INSERT INTO horsies_task_history_leaf_catalog (
             leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor,
             index_schema_version, created_at, detached_at, dropped_at
         ) VALUES ($1, 'standard_30d', 'AT', $2, $3, 1, now(), now(), now())",
    )
    .bind(format!("web_detached_{}", Uuid::new_v4().simple()))
    .bind(detached_upper - chrono::Duration::days(1))
    .bind(detached_upper)
    .execute(&migrated_pool().await)
    .await
    .expect("record a detached period leaf");
    let router = build_router(state);

    for (task_id, text) in [
        (Uuid::new_v4(), "Task not found."),
        (
            uuid7_born_at(detached_upper - chrono::Duration::hours(1)),
            "Task not found. Its id is older than the retained task history.",
        ),
        (
            uuid7_born_at(detached_upper + chrono::Duration::hours(1)),
            "Task not found.",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(request(Method::GET, &format!("/api/tasks/{task_id}")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{task_id}");
        assert_eq!(
            json(response).await,
            serde_json::json!({"detail": text}),
            "{task_id}"
        );
    }
}

/// Python `test_web_api.py::TestTaskReads::test_stats_returns_seven_fixed_cards`:
/// seven status cards in a fixed order, whatever the counts.
#[tokio::test]
async fn the_stats_route_answers_the_seven_status_cards_in_a_fixed_order() {
    let state = migrated_state().await;

    let response = build_router(state)
        .oneshot(request(Method::GET, "/api/tasks/stats"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = json(response).await;
    let statuses: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|card| card["status"].as_str().unwrap())
        .collect();
    assert_eq!(
        statuses,
        [
            "PENDING",
            "CLAIMED",
            "RUNNING",
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "EXPIRED"
        ]
    );
}

/// Python `test_web_api.py::TestTaskReads::test_unknown_error_category_is_a_bad_request`:
/// every task read route refuses an unknown category with 400 before it
/// reads anything.
#[tokio::test]
async fn an_unknown_error_category_is_a_bad_request_on_every_task_route() {
    let router = test_router(
        Arc::new(AllowAll),
        status(SchemaState::Match, Some(crate::expected_schema_version())),
        Actions::Enabled,
    );

    for path in [
        "/api/tasks?error_category=NOT_A_FAMILY",
        "/api/tasks/stats?error_category=NOT_A_FAMILY",
        "/api/tasks/facets?error_category=NOT_A_FAMILY",
        "/api/tasks/breakdown?group_by=worker&error_category=NOT_A_FAMILY",
    ] {
        let response = router
            .clone()
            .oneshot(request(Method::GET, path))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(
            json(response).await,
            serde_json::json!({"detail": "Unknown error category 'NOT_A_FAMILY'."}),
            "{path}"
        );
    }
}

/// Python `test_web_api.py::TestWorkflowReads::test_run_detail_carries_nodes_and_edges`:
/// the run route answers the run, its failed node indices and its edges.
#[tokio::test]
async fn the_run_route_answers_the_run_its_failed_indices_and_its_edges() {
    let state = migrated_state().await;
    let pool = migrated_pool().await;
    let run_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
            id, name, definition_key, status, on_error, output_task_index, success_policy,
            result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth,
            sent_at, created_at, started_at, completed_at, updated_at
        ) VALUES (
            $1, 'web_run_detail_flow', 'tests.alpha.v1', 'FAILED', 'fail', NULL, NULL,
            NULL, NULL, NULL, NULL, $1, 0,
            NOW() - interval '120 seconds', NOW() - interval '120 seconds',
            NOW() - interval '120 seconds', NOW(), NOW()
        )",
    )
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
            downstream_subworkflow,
            id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
            dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
            min_success, task_options, max_retries, execution, good_until,
            retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
            sub_workflow_name, sub_definition_key, sub_workflow_summary, result, error,
            created_at, started_at, completed_at
        ) VALUES (
            'NONE', $1, $2, 0, NULL, 'web_run_detail_task', '{}', 'default', 100,
            '{}'::integer[], NULL, NULL, FALSE, 'all',
            NULL, NULL, 0, 'ISOLATED', NULL,
            'forever', 'FAILED', NULL, FALSE, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NOW() - interval '120 seconds', NULL, NULL
        )",
    )
    .bind(Uuid::new_v4())
    .bind(run_id)
    .execute(&pool)
    .await
    .unwrap();

    let response = build_router(state)
        .oneshot(request(Method::GET, &format!("/api/workflows/{run_id}")))
        .await
        .unwrap();
    let status = response.status();
    let body = json(response).await;
    sql::query("DELETE FROM horsies_workflows WHERE id = $1")
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["run"]["workflow_id"], run_id.to_string());
    assert_eq!(body["failed_indices"], serde_json::json!([0]));
    assert_eq!(body["edges"], serde_json::json!([]));
    // The run and each node carry their timing. The run began 120 s before
    // the read, so it is before or after the start of the lifecycle record
    // (the install of the shared database). After it, the FAILED node with
    // no completion time contradicts itself.
    match body["timing"]["kind"].as_str() {
        Some("unavailable") => assert_eq!(
            body["nodes"][0]["timing"],
            serde_json::json!({"kind": "unavailable", "reason": "RUN_BEFORE_CAPTURE"})
        ),
        Some("composed") => assert_eq!(
            body["nodes"][0]["timing"],
            serde_json::json!({
                "kind": "inconsistent",
                "conflict": {"kind": "missing_record", "record": "COMPLETION_TIME"}
            })
        ),
        other => panic!("the run carries its timing, not {other:?}: {body}"),
    }
}

/// The task route answers the phase record of each attempt: a task that
/// failed through the terminal write of a run, whose row counted no claim
/// and whose writer observed no placement.
#[tokio::test]
async fn the_task_route_answers_the_phase_record_of_each_attempt() {
    let state = migrated_state().await;
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
        ) VALUES (
            $1, 'web_phases_task', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('0f', 32), 'hex'), NULL,
            'RUNNING', NOW() - interval '60 seconds', NOW(), 'web-phases-worker', NULL,
            NOW() - interval '30 seconds', NULL, NULL,
            4242, 'web-phases-host', 'web-phases-process',
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .unwrap();
    let failed = crate::test_support::terminal_writes::fail_running_task(
        &pool,
        task_id,
        "web-phases-worker",
        &crate::test_support::terminal_writes::TerminalFailure::of_task(
            r#"{"err": {"error_code": "TASK_EXCEPTION"}}"#,
            Some("TASK_EXCEPTION"),
            None,
        ),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            failed.as_slice(),
            [crate::core::lifecycle::TerminalizationOutcome::Applied { .. }]
        ),
        "{failed:?}"
    );

    let response = build_router(state)
        .oneshot(request(Method::GET, &format!("/api/tasks/{task_id}")))
        .await
        .unwrap();
    let status = response.status();
    let body = json(response).await;
    sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::OK);
    let phases = &body["attempts"][0]["phases"];
    assert_eq!(phases["kind"], "captured", "{body}");
    assert_eq!(phases["claims"], serde_json::json!({"kind": "not_counted"}));
    assert_eq!(
        phases["finalizing"],
        serde_json::json!({"kind": "not_marked"})
    );
    assert_eq!(
        phases["recorded_by"],
        serde_json::json!({"kind": "worker", "placement": {"kind": "not_observed"}})
    );
}

/// Python `test_web_api.py::TestTaskActionMapping::test_stored_data_that_does_not_parse_is_a_server_error`,
/// the cancel: a history status that is not a task status makes the cancel
/// route answer 500 with the stored value in the detail. The detail route
/// half reads the live columns of #842 first.
#[tokio::test]
async fn a_cancel_of_stored_data_that_does_not_parse_is_a_server_error() {
    let state = migrated_state().await;
    let pool = migrated_pool().await;
    let task_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution,
            retention_class_key, sent_at, created_at,
            command_fingerprint_version, command_fingerprint, idempotency_key_digest,
            status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at,
            started_at, finalizing_at, finalizing_by_worker_id,
            worker_pid, worker_hostname, worker_process_name,
            retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
            updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
        ) VALUES (
            $1, 'web_stored_data_task', 'default', 100, '{}', NULL, 'ISOLATED',
            'forever', NOW(), NOW(),
            1, decode(repeat('0e', 32), 'hex'), NULL,
            'RUNNING', NOW() - interval '60 seconds', NOW(), 'web-stored-worker', NULL,
            NOW() - interval '30 seconds', NULL, NULL,
            4242, 'web-stored-host', 'web-stored-process',
            0, 0, NULL, NULL, FALSE,
            NOW(), FALSE, NULL, NULL
        )",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .unwrap();
    let failed = crate::test_support::terminal_writes::fail_running_task(
        &pool,
        task_id,
        "web-stored-worker",
        &crate::test_support::terminal_writes::TerminalFailure::of_task(
            r#"{"err": {"error_code": "TASK_EXCEPTION"}}"#,
            Some("TASK_EXCEPTION"),
            None,
        ),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            failed.as_slice(),
            [crate::core::lifecycle::TerminalizationOutcome::Applied { .. }]
        ),
        "{failed:?}"
    );
    sql::query("UPDATE horsies_task_history SET status = 'NOT_KNOWN' WHERE task_id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();

    let response = build_router(state)
        .oneshot(action_request(&format!("/api/tasks/{task_id}/cancel")))
        .await
        .unwrap();
    let status = response.status();
    let body = json(response).await;
    sql::query("DELETE FROM horsies_task_history WHERE task_id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        body["detail"].as_str().unwrap().contains("NOT_KNOWN"),
        "{body}"
    );
}

/// The subject of the build in [`RaisingChildBuild`].
const RAISED_BUILD_SUBJECT: &str = "build of the child of web_resume_parent:0";

/// The cause of the build in [`RaisingChildBuild`]: an exception type and a
/// text that holds user data.
const RAISED_BUILD_CAUSE: &str = "SerializationError: options of user@example.com";

/// A client whose build of every child raises an exception
/// (`ChildSpecUnavailable::Raised`), as the Python client's `build_with`
/// does for task options of a child root that do not parse. It keeps each
/// request.
#[derive(Default)]
struct RaisingChildBuild {
    requests: std::sync::Mutex<Vec<crate::ChildSpecRequest>>,
}

impl crate::ChildSpecSource for RaisingChildBuild {
    fn child_spec(&self, request: crate::ChildSpecRequest) -> crate::ChildSpecFuture<'_> {
        self.requests
            .lock()
            .expect("the request list is not poisoned")
            .push(request);
        let answer: Result<crate::ClientChildSpec, crate::ChildSpecUnavailable> =
            Err(crate::ChildSpecUnavailable::Raised {
                subject: RAISED_BUILD_SUBJECT.to_owned(),
                cause: RAISED_BUILD_CAUSE.to_owned(),
            });
        Box::pin(async move { answer })
    }
}

/// A PAUSED top-level workflow named `<prefix>parent` whose node 0 is a READY
/// sub-workflow node with no child, so a resume owes its child start.
/// Returns the workflow id.
async fn seed_paused_parent_of_a_ready_child(pool: &TestPool, prefix: &str) -> Uuid {
    let workflow_id = Uuid::new_v4();
    sql::query(
        "INSERT INTO horsies_workflows (
             id, name, status, on_error, definition_key, depth,
             root_workflow_id, sent_at, created_at, started_at,
             completed_at, updated_at
         ) VALUES (
             $1, $2, 'PAUSED', 'fail', 'web.resume.parent.v1', 0, $1,
             NOW(), NOW(), NOW(), NULL, NOW()
         )",
    )
    .bind(workflow_id)
    .bind(format!("{prefix}parent"))
    .execute(pool)
    .await
    .expect("seed the PAUSED workflow");
    sql::query(
        "INSERT INTO horsies_workflow_tasks (
             id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
             min_success, task_options, max_retries, execution, good_until,
             retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
             sub_workflow_name, sub_definition_key, sub_workflow_params,
             downstream_subworkflow, sub_workflow_summary, result,
             created_at, started_at, completed_at
         ) VALUES (
             $1, $2, 0, 'node_0', 'web_resume_child', '{}', 'default', 100,
             '{}', NULL, NULL, FALSE, 'all',
             NULL, NULL, NULL, NULL, NULL,
             NULL, 'READY', NULL, TRUE, NULL,
             'web_resume_child', 'web.resume.child.v1', 'NOT_GIVEN',
             'NONE', NULL, NULL,
             NOW(), NULL, NULL
         )",
    )
    .bind(Uuid::new_v4())
    .bind(workflow_id)
    .execute(pool)
    .await
    .expect("seed the READY sub-workflow node");
    workflow_id
}

/// Python's resume route let an exception of a child build propagate, and
/// Starlette answered 500 with the text `Internal Server Error`. A resume
/// whose owed child build raised answers the same, with nothing of the
/// exception, and commits nothing: the workflow stays PAUSED and its node
/// READY. The build was asked once.
#[tokio::test]
async fn a_resume_whose_child_build_raised_is_python_s_internal_server_error() {
    let mut state = migrated_state().await;
    let build = Arc::new(RaisingChildBuild::default());
    let source: Arc<dyn crate::ChildSpecSource> = Arc::<RaisingChildBuild>::clone(&build);
    state.children = source;
    let pool = migrated_pool().await;
    let prefix = format!("web_resume_raised_{}__", Uuid::new_v4().simple());
    clean_w4(&pool, &prefix).await;
    let workflow_id = seed_paused_parent_of_a_ready_child(&pool, &prefix).await;

    let response = build_router(state)
        .oneshot(action_request(&format!(
            "/api/workflows/{workflow_id}/resume"
        )))
        .await
        .unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .map(|value| value.to_str().expect("an ASCII header").to_owned());
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let (workflow_status, node_status): (String, String) = sql::query_as(
        "SELECT w.status, t.status
         FROM horsies_workflows w
         JOIN horsies_workflow_tasks t ON t.workflow_id = w.id AND t.task_index = 0
         WHERE w.id = $1",
    )
    .bind(workflow_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let asked = build
        .requests
        .lock()
        .expect("the request list is not poisoned")
        .len();
    clean_w4(&pool, &prefix).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(content_type.as_deref(), Some("text/plain; charset=utf-8"));
    assert_eq!(&body[..], b"Internal Server Error");
    assert_eq!(
        (workflow_status.as_str(), node_status.as_str()),
        ("PAUSED", "READY")
    );
    assert_eq!(asked, 1);
}

/// The horsies relations and functions of the database that `client`
/// reads: `horsies_tasks`, `horsies_schema_version` and `horsies_migrations`
/// by name, then every relation and function whose name starts with
/// `horsies`.
async fn horsies_objects(
    client: &tokio_postgres::Client,
) -> (Option<String>, Option<String>, Option<String>, i64) {
    let row = client
        .query_one(
            "SELECT to_regclass('horsies_tasks')::text,
                    to_regclass('horsies_schema_version')::text,
                    to_regclass('horsies_migrations')::text,
                    (SELECT count(*) FROM pg_class WHERE left(relname, 7) = 'horsies')
                      + (SELECT count(*) FROM pg_proc WHERE left(proname, 7) = 'horsies')",
            &[],
        )
        .await
        .expect("read the horsies objects");
    (row.get(0), row.get(1), row.get(2), row.get(3))
}

/// Python `test_web_api.py::TestTheToolNeverRunsDdl::test_actions_are_refused_and_still_nothing_is_created`:
/// the monitoring handler of an observe-only broker on a new, empty database
/// answers an allowed cancel that carries the intent header with 409
/// `SCHEMA_INCOMPATIBLE` and Python's detail, and the database still holds no
/// horsies relation or function afterwards.
#[tokio::test]
async fn an_action_on_a_database_with_no_horsies_schema_is_refused_and_creates_nothing() {
    let database = crate::test_support::TestDatabase::create_empty().await;
    let url = database.url().to_owned();
    let broker = Arc::new(
        PostgresBroker::connect_observe_only(
            &AppConfig::for_database_url(&url).broker,
            crate::ClientFamily::Rust,
        )
        .await
        .expect("connect the observe-only broker"),
    );
    let handler = MonitoringHandler::new(
        Arc::clone(&broker),
        Arc::new(TestChildSpecs::empty()),
        MonitoringHandlerConfig {
            actions: Actions::Enabled,
            ui: MonitoringUiConfig::default(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            heartbeat: HEARTBEAT,
            fleet_labels: FleetLabels::none(),
        },
    );

    let response = handler
        .handle(
            action_parts(&format!("/api/tasks/{}/cancel", Uuid::new_v4())),
            grant(Verdict::Allowed, Verdict::Allowed),
        )
        .await;
    let status = response.status;
    let body = body_json(&response);
    let objects = horsies_objects(database.client()).await;
    broker.close().await;
    drop(broker);
    database.drop().await;

    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], SCHEMA_INCOMPATIBLE);
    assert!(
        body["detail"]
            .as_str()
            .expect("a text detail")
            .contains("no horsies schema"),
        "{body}"
    );
    assert_eq!(objects, (None, None, None, 0));
}

fn granted_handler(actions: Actions) -> MonitoringHandler {
    let mut state = test_state(
        &lazy_pool(),
        Arc::new(GrantedAccess),
        fixed_probe(status(
            SchemaState::Match,
            Some(crate::expected_schema_version()),
        )),
        actions,
        MemoryAssets::standard(),
    );
    state.client_version = "9.9.9-client".to_owned();
    MonitoringHandler::from_state(state)
}

fn parts(method: Method, path: &str) -> RequestParts {
    RequestParts {
        method,
        path: path.to_owned(),
        raw_query: None,
        intent_header: None,
        content_type: None,
        body: axum::body::Bytes::new(),
        base_path: "/monitor".to_owned(),
    }
}

fn action_parts(path: &str) -> RequestParts {
    RequestParts {
        intent_header: Some(INTENT_VALUE.to_owned()),
        content_type: Some("application/json".to_owned()),
        body: axum::body::Bytes::from_static(b"{}"),
        ..parts(Method::POST, path)
    }
}

const fn grant(view: Verdict, act: Verdict) -> AccessGrant {
    AccessGrant { view, act }
}

fn body_json(response: &ResponseParts) -> serde_json::Value {
    serde_json::from_slice(&response.body).expect("a JSON body")
}

/// The verdicts that each route needs are the ones the Python app asks:
/// `can_view` on every read route and the event stream, `can_view` then
/// `can_act` on `/api/meta` and on the action routes, only `can_view` on an
/// action route while actions are disabled, and none on the page, its
/// assets and every other path.
#[tokio::test]
async fn the_handler_asks_the_verdicts_of_the_python_routes() {
    let handler = granted_handler(Actions::Enabled);
    let id = Uuid::nil();
    for (method, path, need) in [
        (
            Method::GET,
            "/api/meta".to_owned(),
            AccessNeed::ViewReportAct,
        ),
        (Method::GET, "/api/tasks".to_owned(), AccessNeed::View),
        (Method::GET, "/api/tasks/stats".to_owned(), AccessNeed::View),
        (
            Method::GET,
            "/api/tasks/facets".to_owned(),
            AccessNeed::View,
        ),
        (
            Method::GET,
            "/api/tasks/breakdown".to_owned(),
            AccessNeed::View,
        ),
        (Method::GET, format!("/api/tasks/{id}"), AccessNeed::View),
        (Method::HEAD, format!("/api/tasks/{id}"), AccessNeed::View),
        (Method::GET, "/api/workflows".to_owned(), AccessNeed::View),
        (
            Method::GET,
            "/api/workflows/names".to_owned(),
            AccessNeed::View,
        ),
        (
            Method::GET,
            format!("/api/workflows/{id}"),
            AccessNeed::View,
        ),
        (
            Method::GET,
            format!("/api/workflows/{id}/tasks/3"),
            AccessNeed::View,
        ),
        (Method::GET, "/api/workers".to_owned(), AccessNeed::View),
        (
            Method::GET,
            "/api/workers/ping".to_owned(),
            AccessNeed::View,
        ),
        (
            Method::GET,
            "/api/workers/schedules".to_owned(),
            AccessNeed::View,
        ),
        (
            Method::GET,
            "/api/workers/classes".to_owned(),
            AccessNeed::View,
        ),
        (
            Method::GET,
            "/api/workers/w1/history".to_owned(),
            AccessNeed::View,
        ),
        (
            Method::POST,
            format!("/api/tasks/{id}/cancel"),
            AccessNeed::ViewAndAct,
        ),
        (
            Method::POST,
            format!("/api/workflows/{id}/pause"),
            AccessNeed::ViewAndAct,
        ),
        (
            Method::POST,
            format!("/api/workflows/{id}/resume"),
            AccessNeed::ViewAndAct,
        ),
        (
            Method::POST,
            format!("/api/workflows/{id}/cancel"),
            AccessNeed::ViewAndAct,
        ),
        (Method::GET, "/".to_owned(), AccessNeed::Public),
        (Method::GET, "/assets/app.js".to_owned(), AccessNeed::Public),
        (
            Method::GET,
            "/tasks/some/client/route".to_owned(),
            AccessNeed::Public,
        ),
        (Method::GET, "/api/unknown".to_owned(), AccessNeed::Public),
        (
            Method::GET,
            "/api/workers/w1".to_owned(),
            AccessNeed::Public,
        ),
        (
            Method::GET,
            format!("/api/tasks/{id}/cancel"),
            AccessNeed::Public,
        ),
        (Method::POST, "/api/tasks".to_owned(), AccessNeed::Public),
        (Method::GET, "/api/tasks/".to_owned(), AccessNeed::Public),
    ] {
        let access = handler.route_access(&method, &path);
        assert_eq!(access.need, need, "{method} {path}");
        assert_eq!(access.form, ResponseForm::OneResponse, "{method} {path}");
    }
    let events = handler.route_access(&Method::GET, "/api/events");
    assert_eq!(events.need, AccessNeed::View);
    assert_eq!(events.form, ResponseForm::EventStream);

    let view_only = granted_handler(Actions::Disabled);
    let cancel = view_only.route_access(&Method::POST, &format!("/api/tasks/{id}/cancel"));
    assert_eq!(cancel.need, AccessNeed::View);
}

/// The handler answers with the guards and routes of the router, reading
/// the verdicts of the grant; no database is reachable here.
#[tokio::test]
async fn the_handler_serves_the_routes_with_the_verdicts_of_the_grant() {
    let handler = granted_handler(Actions::Enabled);

    let refused = handler
        .handle(
            parts(Method::GET, "/api/tasks/not-a-uuid"),
            grant(Verdict::Refused, Verdict::NotAsked),
        )
        .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(&refused),
        serde_json::json!({"detail": "Not authorized."})
    );

    let read = handler
        .handle(
            parts(Method::GET, "/api/tasks/not-a-uuid"),
            grant(Verdict::Allowed, Verdict::NotAsked),
        )
        .await;
    assert_eq!(read.status, StatusCode::BAD_REQUEST);
    assert_eq!(body_json(&read), not_a_uuid_refusal("task_id"));

    let meta = handler
        .handle(
            parts(Method::GET, "/api/meta"),
            grant(Verdict::Allowed, Verdict::Refused),
        )
        .await;
    assert_eq!(meta.status, StatusCode::OK);
    let meta = body_json(&meta);
    assert_eq!(meta["can_act"], false);
    assert_eq!(meta["base_path"], "/monitor");
    assert_eq!(meta["horsies_version"], "9.9.9-client");

    let act_refused = handler
        .handle(
            action_parts("/api/workflows/not-a-uuid/pause"),
            grant(Verdict::Allowed, Verdict::Refused),
        )
        .await;
    assert_eq!(act_refused.status, StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(&act_refused),
        serde_json::json!({"detail": "Not authorized."})
    );

    let no_intent = handler
        .handle(
            RequestParts {
                intent_header: None,
                ..action_parts("/api/workflows/not-a-uuid/pause")
            },
            grant(Verdict::Allowed, Verdict::Allowed),
        )
        .await;
    assert_eq!(no_intent.status, StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(&no_intent),
        serde_json::json!({"detail": format!("Missing {INTENT_HEADER}: {INTENT_VALUE} header.")})
    );

    let action = handler
        .handle(
            action_parts("/api/workflows/not-a-uuid/pause"),
            grant(Verdict::Allowed, Verdict::Allowed),
        )
        .await;
    assert_eq!(action.status, StatusCode::BAD_REQUEST);
    assert_eq!(body_json(&action)["code"], "INVALID_INPUT");

    let page = handler
        .handle(
            parts(Method::GET, "/"),
            grant(Verdict::NotAsked, Verdict::NotAsked),
        )
        .await;
    assert_eq!(page.status, StatusCode::OK);
    let html = std::str::from_utf8(&page.body).expect("the page is text");
    assert!(html.contains("/monitor/"), "{html}");

    let not_a_path = handler
        .handle(
            parts(Method::GET, "/not a path"),
            grant(Verdict::NotAsked, Verdict::NotAsked),
        )
        .await;
    assert_eq!(not_a_path.status, StatusCode::BAD_REQUEST);
}

/// While actions are disabled, an action route asks only `can_view`, and
/// the act guard refuses without a verdict to act.
#[tokio::test]
async fn an_action_while_actions_are_disabled_is_refused_without_asking_to_act() {
    let handler = granted_handler(Actions::Disabled);

    let refused = handler
        .handle(
            action_parts("/api/workflows/not-a-uuid/pause"),
            grant(Verdict::Allowed, Verdict::NotAsked),
        )
        .await;

    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(&refused),
        serde_json::json!({"detail": "Not authorized."})
    );
}

#[tokio::test]
#[should_panic(expected = "does not answer the need")]
async fn a_grant_that_skips_a_needed_verdict_is_a_defect() {
    let handler = granted_handler(Actions::Enabled);
    let _ = handler
        .handle(
            parts(Method::GET, "/api/meta"),
            grant(Verdict::Allowed, Verdict::NotAsked),
        )
        .await;
}

#[tokio::test]
#[should_panic(expected = "is the event stream")]
async fn the_event_stream_has_no_single_response() {
    let handler = granted_handler(Actions::Enabled);
    let _ = handler
        .handle(
            parts(Method::GET, "/api/events"),
            grant(Verdict::Allowed, Verdict::NotAsked),
        )
        .await;
}

/// The event stream path answers a refused view with the 403 of the guard:
/// the host has no stream to open.
#[tokio::test]
async fn a_refused_view_of_the_event_stream_is_answered_by_the_guard() {
    let handler = granted_handler(Actions::Enabled);

    let refused = handler
        .handle(
            parts(Method::GET, "/api/events"),
            grant(Verdict::Refused, Verdict::NotAsked),
        )
        .await;

    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(
        body_json(&refused),
        serde_json::json!({"detail": "Not authorized."})
    );
}

/// A body over the bound is refused with 413 before any route reads it; a
/// refused verdict is answered first by the guards.
#[tokio::test]
async fn a_body_over_the_bound_is_refused_after_the_verdicts() {
    let handler = granted_handler(Actions::Enabled);
    let over = axum::body::Bytes::from(vec![b' '; super::handler::MAX_REQUEST_BODY_BYTES + 1]);

    let too_large = handler
        .handle(
            RequestParts {
                body: over.clone(),
                ..action_parts(&format!("/api/tasks/{}/cancel", Uuid::nil()))
            },
            grant(Verdict::Allowed, Verdict::Allowed),
        )
        .await;
    assert_eq!(too_large.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        body_json(&too_large),
        serde_json::json!({"detail": format!(
            "The request body is over {} bytes.",
            super::handler::MAX_REQUEST_BODY_BYTES
        )})
    );

    let refused = handler
        .handle(
            RequestParts {
                body: over,
                ..action_parts(&format!("/api/tasks/{}/cancel", Uuid::nil()))
            },
            grant(Verdict::Allowed, Verdict::Refused),
        )
        .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
}

/// The size bound comes before the key check of the body: on every action
/// route, a body over the bound whose object has an unknown key is refused
/// with 413, not with the 422 of the key.
#[tokio::test]
async fn a_body_over_the_bound_with_an_unknown_key_is_413_on_every_action_route() {
    let handler = granted_handler(Actions::Enabled);
    let mut over = br#"{"include_runing": true}"#.to_vec();
    over.resize(super::handler::MAX_REQUEST_BODY_BYTES + 1, b' ');
    let over = axum::body::Bytes::from(over);

    for path in failing_actions() {
        let response = handler
            .handle(
                RequestParts {
                    body: over.clone(),
                    ..action_parts(&path)
                },
                grant(Verdict::Allowed, Verdict::Allowed),
            )
            .await;
        assert_eq!(response.status, StatusCode::PAYLOAD_TOO_LARGE, "{path}");
        assert_eq!(
            body_json(&response),
            serde_json::json!({"detail": format!(
                "The request body is over {} bytes.",
                super::handler::MAX_REQUEST_BODY_BYTES
            )}),
            "{path}"
        );
    }
}

/// The free `route_access` is the handler's answer for the same switch.
#[tokio::test]
async fn route_access_follows_the_actions_switch() {
    let path = format!("/api/workflows/{}/resume", Uuid::nil());
    assert_eq!(
        super::handler::route_access(true, &Method::POST, &path),
        granted_handler(Actions::Enabled).route_access(&Method::POST, &path)
    );
    assert_eq!(
        super::handler::route_access(false, &Method::POST, &path).need,
        AccessNeed::View
    );
}

/// `/metrics` needs the view verdict; another method on it and a longer
/// path are public (a 405 and the page).
#[tokio::test]
async fn metrics_needs_the_view_verdict() {
    let handler = granted_handler(Actions::Enabled);
    for (method, path, need) in [
        (Method::GET, "/metrics", AccessNeed::View),
        (Method::HEAD, "/metrics", AccessNeed::View),
        (Method::POST, "/metrics", AccessNeed::Public),
        (Method::GET, "/metrics/extra", AccessNeed::Public),
    ] {
        let access = handler.route_access(&method, path);
        assert_eq!(access.need, need, "{method} {path}");
        assert_eq!(access.form, ResponseForm::OneResponse, "{method} {path}");
    }
}

/// `/metrics` answers 403 after a refused view and 405 for another method.
/// After an allowed view it answers the fleet text with the Prometheus
/// content type. The first answer comes after the first read: a database
/// that refuses connections shows in it as a failed read of each query,
/// with no fleet series, and the status is still 200.
#[tokio::test]
async fn metrics_serves_the_fleet_text_behind_the_view_verdict() {
    let handler = granted_handler(Actions::Enabled);
    let refused = handler
        .handle(
            parts(Method::GET, "/metrics"),
            grant(Verdict::Refused, Verdict::NotAsked),
        )
        .await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    let other_method = handler
        .handle(
            parts(Method::POST, "/metrics"),
            grant(Verdict::NotAsked, Verdict::NotAsked),
        )
        .await;
    assert_eq!(other_method.status, StatusCode::METHOD_NOT_ALLOWED);

    let first = handler
        .handle(
            parts(Method::GET, "/metrics"),
            grant(Verdict::Allowed, Verdict::NotAsked),
        )
        .await;
    assert_eq!(first.status, StatusCode::OK);
    let content_type: Vec<&[u8]> = first
        .headers
        .iter()
        .filter(|(name, _)| *name == axum::http::header::CONTENT_TYPE)
        .map(|(_, value)| value.as_bytes())
        .collect();
    assert_eq!(content_type, vec![PROMETHEUS_TEXT_CONTENT_TYPE.as_bytes()]);
    let body = String::from_utf8(first.body.to_vec()).expect("the text is UTF-8");
    assert!(
        body.contains("# TYPE horsies_queue_tasks gauge\n"),
        "{body}"
    );
    for query in [
        "queue_tasks",
        "oldest_pending",
        "workers",
        "class_coverage",
        "workflows",
        "schedules",
        "schema_version",
    ] {
        assert!(
            body.contains(&format!(
                "horsies_exporter_collect_failed{{query=\"{query}\"}} 1\n"
            )),
            "{query}: {body}"
        );
    }
    assert!(!body.contains("\nhorsies_queue_tasks{"), "{body}");
    assert!(!body.contains("\nhorsies_schema_version "), "{body}");
}

/// A broker without a session URL has no listener: the stream sends the
/// degraded frame and ends.
#[tokio::test]
async fn an_event_stream_without_a_listener_degrades_and_ends() {
    let handler = granted_handler(Actions::Enabled);
    let mut frames = handler.subscribe_events().await;

    let first = tokio::time::timeout(Duration::from_secs(5), frames.next())
        .await
        .expect("the first frame comes");

    assert_eq!(
        first,
        super::events::EventStreamItem::Frame("data: {\"topic\": \"degraded\"}\n\n".to_owned())
    );
    assert_eq!(frames.next().await, super::events::EventStreamItem::Ended);
}
