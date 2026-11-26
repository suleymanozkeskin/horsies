//! The axum mount over the core handler, with no database: the policy's
//! verdicts, the routes that answer before any statement, a database that
//! cannot be reached, the event stream of a handle without a listener, and
//! the mount path. The routes themselves are tested in the core
//! (`core/src/web/tests.rs`).

use std::sync::atomic::{AtomicUsize, Ordering};

use axum::http::{Method, Request};
use tower::ServiceExt;

use super::*;
use crate::{AppConfig, SchemaMode};

/// A URL with nothing listening on its port: every connect is refused at
/// once.
const UNREACHABLE_URL: &str = "postgresql://user:secret@127.0.0.1:1/none";

const TASK_ID: &str = "0195c1b4-7a2e-7d2f-9d4e-3b1a2c4d5e6f";

fn config() -> AppConfig {
    let mut config = AppConfig::for_database_url(UNREACHABLE_URL);
    config.broker.pool_timeout = 1;
    config
}

fn app() -> Horsies {
    Horsies::new(config()).unwrap()
}

fn request(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

fn action(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(INTENT_HEADER, INTENT_VALUE)
        .body(Body::empty())
        .unwrap()
}

async fn send(router: Router, request: Request<Body>) -> (StatusCode, HeaderMap, Bytes) {
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, body)
}

fn json(body: &Bytes) -> serde_json::Value {
    serde_json::from_slice(body).unwrap()
}

fn trusted() -> TrustedHeader {
    TrustedHeader::new("x-user", Actions::Enabled).unwrap()
}

/// The postures of the three policies: `AllowAll` allows reads and actions;
/// `ViewOnly` allows reads and refuses actions; `TrustedHeader` allows a
/// request whose header has a value that is not blank, and its actions only
/// when it allows actions.
#[tokio::test]
async fn the_three_policies_answer_their_postures() {
    let (bare, _body) = request(Method::GET, "/api/meta").into_parts();
    let facts = MonitoringRequest::of(&bare);
    assert!(AllowAll.can_view(facts).await);
    assert!(AllowAll.can_act(facts).await);
    assert!(ViewOnly.can_view(facts).await);
    assert!(!ViewOnly.can_act(facts).await);

    let acting = TrustedHeader::new("X-Forwarded-User", Actions::Enabled).unwrap();
    assert!(!acting.can_view(facts).await);
    let (blank, _body) = Request::builder()
        .uri("/api/meta")
        .header("x-forwarded-user", "  ")
        .body(Body::empty())
        .unwrap()
        .into_parts();
    assert!(!acting.can_view(MonitoringRequest::of(&blank)).await);
    let (identified, _body) = Request::builder()
        .uri("/api/meta")
        .header("x-forwarded-user", " alex ")
        .body(Body::empty())
        .unwrap()
        .into_parts();
    let facts = MonitoringRequest::of(&identified);
    assert!(acting.can_view(facts).await);
    assert!(acting.can_act(facts).await);
    let viewing = TrustedHeader::new("X-Forwarded-User", Actions::Disabled).unwrap();
    assert!(viewing.can_view(facts).await);
    assert!(!viewing.can_act(facts).await);
}

/// A blank comma-separated member, or a blank copy of a repeated header,
/// makes the identity ambiguous: `TrustedHeader` refuses it. Members that
/// are all present are allowed.
#[tokio::test]
async fn a_trusted_header_with_a_blank_member_is_refused() {
    let policy = TrustedHeader::new("X-Forwarded-User", Actions::Enabled).unwrap();
    for value in ["alice, ", ", alice", "alice,,bob", ","] {
        let (head, _body) = Request::builder()
            .uri("/api/meta")
            .header("x-forwarded-user", value)
            .body(Body::empty())
            .unwrap()
            .into_parts();
        let facts = MonitoringRequest::of(&head);
        assert!(!policy.can_view(facts).await, "{value:?}");
        assert!(!policy.can_act(facts).await, "{value:?}");
    }

    let (repeated, _body) = Request::builder()
        .uri("/api/meta")
        .header("x-forwarded-user", "alice")
        .header("x-forwarded-user", "")
        .body(Body::empty())
        .unwrap()
        .into_parts();
    assert!(!policy.can_view(MonitoringRequest::of(&repeated)).await);

    let (members, _body) = Request::builder()
        .uri("/api/meta")
        .header("x-forwarded-user", "alice, bob")
        .body(Body::empty())
        .unwrap()
        .into_parts();
    assert!(policy.can_view(MonitoringRequest::of(&members)).await);
}

/// The page and its assets are public: a policy that refuses every view
/// does not stop them.
#[tokio::test]
async fn the_page_is_public() {
    let router = router(&app(), MonitoringConfig::default(), trusted());
    let (status, headers, _body) = send(router, request(Method::GET, "/")).await;
    assert_eq!(status, StatusCode::OK);
    let content_type = headers[CONTENT_TYPE].to_str().unwrap();
    assert!(content_type.starts_with("text/html"), "{content_type}");
}

/// `GET /metrics` asks `can_view`: a refusal is 403, an allowed view is the
/// fleet text with the content type of the Prometheus text format.
#[tokio::test]
async fn metrics_is_the_fleet_text_behind_the_view_verdict() {
    let refused = router(&app(), MonitoringConfig::default(), trusted());
    let (status, _headers, _body) = send(refused, request(Method::GET, "/metrics")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let viewing = router(&app(), MonitoringConfig::default(), ViewOnly);
    let (status, headers, body) = send(viewing, request(Method::GET, "/metrics")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[CONTENT_TYPE].to_str().unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let text = std::str::from_utf8(&body).unwrap();
    assert!(
        text.contains("# TYPE horsies_queue_tasks gauge\n"),
        "{text}"
    );
}

/// A read route asks `can_view`; a refusal is 403 `Not authorized.`.
#[tokio::test]
async fn a_refused_view_is_403() {
    let router = router(&app(), MonitoringConfig::default(), trusted());
    let (status, _headers, body) = send(router, request(Method::GET, "/api/tasks")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json(&body)["detail"], "Not authorized.");
}

/// An action asks `can_act` after the view; `ViewOnly` refuses it with 403.
#[tokio::test]
async fn view_only_refuses_an_action() {
    let router = router(&app(), MonitoringConfig::default(), ViewOnly);
    let uri = format!("/api/tasks/{TASK_ID}/cancel");
    let (status, _headers, body) = send(router, action(&uri)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json(&body)["detail"], "Not authorized.");
}

/// A task id that is not a UUID is 400 `INVALID_INPUT`, before any
/// statement.
#[tokio::test]
async fn a_task_id_that_is_not_a_uuid_is_400_invalid_input() {
    let router = router(&app(), MonitoringConfig::default(), AllowAll);
    let (status, _headers, body) =
        send(router, request(Method::GET, "/api/tasks/not-a-uuid")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json(&body)["code"], "INVALID_INPUT");
}

/// The schema gate of the actions: a database that cannot be reached has an
/// unknown schema state, and an allowed action is refused with 409.
#[tokio::test]
async fn the_schema_gate_refuses_an_action_when_the_database_cannot_be_reached() {
    let router = router(&app(), MonitoringConfig::default(), AllowAll);
    let uri = format!("/api/workflows/{TASK_ID}/pause");
    let (status, _headers, body) = send(router, action(&uri)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json(&body)["code"], "SCHEMA_UNKNOWN");
}

/// Disabled actions refuse an action with 403 and do not ask `can_act`.
#[tokio::test]
async fn disabled_actions_refuse_without_asking_to_act() {
    let policy = CountingPolicy::default();
    let config = MonitoringConfig {
        ui: MonitoringUiConfig::default(),
        actions: Actions::Disabled,
    };
    let router = router(&app(), config, policy.clone());
    let uri = format!("/api/tasks/{TASK_ID}/cancel");
    let (status, _headers, _body) = send(router, action(&uri)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(policy.calls(), (1, 0));
}

/// `/api/meta` reports the `can_act` verdict, the client version and the
/// mount path of a nested router.
#[tokio::test]
async fn meta_reports_the_act_verdict_and_the_mount_path() {
    let monitoring = router(&app(), MonitoringConfig::default(), ViewOnly);
    let host = Router::new().nest("/monitoring", monitoring);
    let (status, _headers, body) = send(host, request(Method::GET, "/monitoring/api/meta")).await;
    assert_eq!(status, StatusCode::OK);
    let meta = json(&body);
    assert_eq!(meta["can_act"], false);
    assert_eq!(meta["base_path"], "/monitoring");
    assert_eq!(meta["horsies_version"], env!("CARGO_PKG_VERSION"));
}

/// The event stream of a handle with no listener (`AssumeInitialized`) is
/// the degraded frame, then the end, with Python's headers.
#[tokio::test]
async fn the_event_stream_without_a_listener_is_the_degraded_frame() {
    let app = Horsies::with_schema_mode(config(), SchemaMode::AssumeInitialized).unwrap();
    let router = router(&app, MonitoringConfig::default(), AllowAll);
    let (status, headers, body) = send(router, request(Method::GET, "/api/events")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[CONTENT_TYPE], "text/event-stream; charset=utf-8");
    assert_eq!(headers[CACHE_CONTROL], "no-cache");
    assert_eq!(headers["x-accel-buffering"], "no");
    assert_eq!(&body[..], b"data: {\"topic\": \"degraded\"}\n\n");
}

/// A refused view of the event stream is the handler's 403; no stream
/// starts.
#[tokio::test]
async fn a_refused_view_of_the_event_stream_is_403() {
    let router = router(&app(), MonitoringConfig::default(), trusted());
    let (status, _headers, _body) = send(router, request(Method::GET, "/api/events")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// A policy that allows everything and counts its calls.
#[derive(Clone, Default)]
struct CountingPolicy {
    views: Arc<AtomicUsize>,
    acts: Arc<AtomicUsize>,
}

impl CountingPolicy {
    fn calls(&self) -> (usize, usize) {
        (
            self.views.load(Ordering::SeqCst),
            self.acts.load(Ordering::SeqCst),
        )
    }
}

impl MonitoringAuthorizationPolicy for CountingPolicy {
    async fn can_view(&self, _request: MonitoringRequest<'_>) -> bool {
        let _previous = self.views.fetch_add(1, Ordering::SeqCst);
        true
    }

    async fn can_act(&self, _request: MonitoringRequest<'_>) -> bool {
        let _previous = self.acts.fetch_add(1, Ordering::SeqCst);
        true
    }
}

/// The verdicts asked for each need: none for a public path, `can_view`
/// alone for a read, both for an action and `/api/meta`.
#[tokio::test]
async fn the_mount_asks_only_the_needed_verdicts() {
    let (head, _body) = request(Method::GET, "/").into_parts();
    let cases = [
        (AccessNeed::Public, (0, 0)),
        (AccessNeed::View, (1, 0)),
        (AccessNeed::ViewReportAct, (1, 1)),
        (AccessNeed::ViewAndAct, (1, 1)),
    ];
    for (need, asked) in cases {
        let policy = CountingPolicy::default();
        let _grant = grant_of(&policy, need, MonitoringRequest::of(&head)).await;
        assert_eq!(policy.calls(), asked, "{need:?}");
    }
}

/// A refused view ends the questions: `can_act` is not asked.
#[tokio::test]
async fn a_refused_view_does_not_ask_to_act() {
    let (head, _body) = request(Method::GET, "/").into_parts();
    let grant = grant_of(
        &trusted(),
        AccessNeed::ViewAndAct,
        MonitoringRequest::of(&head),
    )
    .await;
    assert_eq!(grant.view, Verdict::Refused);
    assert_eq!(grant.act, Verdict::NotAsked);
}

/// A body over the bound is cut one byte over it, so the core answers 413.
#[tokio::test]
async fn a_long_body_is_cut_one_byte_over_the_bound() {
    let body = Body::from(vec![b'x'; MAX_REQUEST_BODY_BYTES + 10]);
    let read = read_body(body).await.unwrap();
    assert_eq!(read.len(), MAX_REQUEST_BODY_BYTES + 1);
    let short = read_body(Body::from("{}")).await.unwrap();
    assert_eq!(&short[..], b"{}");
}

/// A handler whose broker cannot be built answers with the cause: 503 when
/// the cause may clear, 500 for a connection setting that the core refuses
/// (a repeat fails until the settings change).
#[tokio::test]
async fn a_handler_build_failure_answers_by_the_retry_rule_of_its_error() {
    let transient = handler_build_failed(&BrokerError::ConnectionFailed("refused".to_owned()));
    assert_eq!(transient.status(), StatusCode::SERVICE_UNAVAILABLE);

    let permanent = handler_build_failed(&BrokerError::InvalidConnectionSettings(
        horsies_core::PostgresConfigError::InvalidUrlScheme("mysql".to_owned()),
    ));
    assert_eq!(permanent.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(permanent.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        json(&body)["detail"].as_str().is_some_and(|detail| {
            detail.starts_with("The monitoring handler could not connect its broker: ")
        }),
        "{body:?}"
    );
}
