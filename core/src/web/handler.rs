//! The framework-neutral monitoring handler: one request in, one response
//! out, for a host framework that runs the user's authorization policy
//! itself (the Python client mounts it in FastAPI).
//!
//! The host asks [`MonitoringHandler::route_access`] which policy verdicts a
//! request needs, runs the user's policy for those and no other, and passes
//! the verdicts as an [`AccessGrant`] with the request to
//! [`MonitoringHandler::handle`]. The handler serves the request with the
//! routes, guards, caches and page of its internal router. Its guards read
//! the verdicts of the grant.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::Request;
use axum::response::IntoResponse;
use axum::Router;
use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, StatusCode};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

use crate::broker::PostgresBroker;
use crate::monitoring::fleet::FleetLabels;
use crate::workflow_engine::child_spec::ChildSpecSource;

use super::app::{build_router, MountPath, WebState};
use super::auth::{
    MonitoringAuthorizationPolicy, MonitoringRequest, INTENT_HEADER, NOT_AUTHORIZED,
};
use super::common::ApiError;
use super::events::{EventBroadcaster, EventFrames};
use super::fleet_metrics::WebFleetMetrics;
use super::schema::SchemaProbe;
use super::spa::{EmbeddedAssets, MonitoringUiConfig};
use super::task_stats_cache::{ReadCache, TaskStatsCache};

/// The largest response body that [`MonitoringHandler::handle`] collects:
/// 64 MiB. The read routes are bounded by their row limits and the page by
/// the embedded assets, so a response over it is refused with 500.
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// The largest request body that [`MonitoringHandler::handle`] takes: 1 MiB.
/// The action routes read a body: the task cancel takes a JSON object of
/// one flag, and a workflow action takes a JSON object with no keys. A host
/// reads at most one byte more than this bound and passes what it read; a
/// body over the bound is refused with 413 before any route reads it, so
/// before a route refuses an unknown key.
pub const MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;

/// Which verdicts of the user's policy a request needs, in the order that
/// the host asks them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessNeed {
    /// No verdict: the page, its assets, and every path that no API route
    /// serves (a 404 or a 405 that reveals nothing).
    Public,
    /// `can_view`.
    View,
    /// `can_view`, then, when it allows, `can_act`, whose verdict the
    /// response reports (`/api/meta`).
    ViewReportAct,
    /// `can_view`, then, when it allows, `can_act` (the action routes while
    /// actions are enabled).
    ViewAndAct,
}

/// How the host answers a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseForm {
    /// One response: [`MonitoringHandler::handle`].
    OneResponse,
    /// The event stream (`/api/events`): the host subscribes to the event
    /// broadcaster and writes its frames.
    EventStream,
}

/// The access that a request needs and the form of its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteAccess {
    pub need: AccessNeed,
    pub form: ResponseForm,
}

/// One verdict of the user's policy for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Allowed,
    Refused,
    /// The host did not ask: the route needs no such verdict, or an earlier
    /// verdict refused the request.
    NotAsked,
}

/// The verdicts that the host's run of the user's policy gave for one
/// request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessGrant {
    pub view: Verdict,
    pub act: Verdict,
}

impl AccessGrant {
    /// Whether this grant answers exactly the verdicts that `need` asks, in
    /// order: a verdict after a refusal is not asked.
    fn fits(self, need: AccessNeed) -> bool {
        matches!(
            (need, self.view, self.act),
            (AccessNeed::Public, Verdict::NotAsked, Verdict::NotAsked)
                | (
                    AccessNeed::View,
                    Verdict::Allowed | Verdict::Refused,
                    Verdict::NotAsked
                )
                | (
                    AccessNeed::ViewReportAct | AccessNeed::ViewAndAct,
                    Verdict::Allowed,
                    Verdict::Allowed | Verdict::Refused,
                )
                | (
                    AccessNeed::ViewReportAct | AccessNeed::ViewAndAct,
                    Verdict::Refused,
                    Verdict::NotAsked,
                )
        )
    }
}

/// The policy of the handler's router: the verdicts of the request's grant.
/// A request without a grant is refused.
pub(crate) struct GrantedAccess;

#[async_trait]
impl MonitoringAuthorizationPolicy for GrantedAccess {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool {
        request
            .extensions
            .get::<AccessGrant>()
            .is_some_and(|grant| grant.view == Verdict::Allowed)
    }

    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool {
        request
            .extensions
            .get::<AccessGrant>()
            .is_some_and(|grant| grant.act == Verdict::Allowed)
    }
}

/// One request, as the host framework read it.
#[derive(Debug, Clone)]
pub struct RequestParts {
    pub method: Method,
    /// The path inside the mount, from its leading `/`, as it came on the
    /// wire (percent-encoded).
    pub path: String,
    /// The query text after `?`, as it came on the wire; `None` when the
    /// request has none.
    pub raw_query: Option<String>,
    /// The value of `X-Horsies-Intent`; `None` when the request has none.
    pub intent_header: Option<String>,
    /// The value of `Content-Type`; `None` when the request has none.
    pub content_type: Option<String>,
    pub body: Bytes,
    /// The mount path of the app (the host's root path); `""` or `"/"` at
    /// the root.
    pub base_path: String,
}

/// One response for the host framework to send.
#[derive(Debug, Clone)]
pub struct ResponseParts {
    pub status: StatusCode,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub body: Bytes,
}

/// Whether the action routes (task cancel, workflow pause, resume and
/// cancel) run. Disabled actions answer 403 without asking the policy's
/// `can_act`. `/api/meta` reports the value as `"ENABLED"` or
/// `"DISABLED"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Actions {
    Enabled,
    Disabled,
}

/// The settings of a monitoring handler.
#[derive(Debug, Clone)]
pub struct MonitoringHandlerConfig {
    pub actions: Actions,
    pub ui: MonitoringUiConfig,
    /// The version that `/api/meta` reports: the package version of the
    /// client that mounts the handler.
    pub client_version: String,
    /// How long an event stream waits without an event before it sends a
    /// heartbeat frame ([`super::HEARTBEAT`] in production).
    pub heartbeat: Duration,
    /// The queue and schedule names that `/metrics` takes as label values:
    /// the app config's ([`FleetLabels::of_config`]). Other names are
    /// `(unknown)`.
    pub fleet_labels: FleetLabels,
}

/// The monitoring app of one broker as a framework-neutral handler.
pub struct MonitoringHandler {
    router: Router,
    events: Arc<EventBroadcaster>,
    actions: Actions,
    heartbeat: Duration,
}

impl MonitoringHandler {
    /// The handler of `broker`. The resume route builds the sub-workflow
    /// children that a resume owes through `children`. Starts nothing: the
    /// event broadcaster subscribes on the broker's listener at the first
    /// subscriber, and a `/metrics` request reads the fleet state when a
    /// read is due.
    pub fn new(
        broker: Arc<PostgresBroker>,
        children: Arc<dyn ChildSpecSource>,
        config: MonitoringHandlerConfig,
    ) -> Self {
        let fleet_metrics = Arc::new(WebFleetMetrics::new(
            Arc::clone(&broker),
            config.fleet_labels,
        ));
        let state = WebState {
            schema_probe: Arc::new(SchemaProbe::new(Arc::clone(&broker))),
            task_stats_cache: Arc::new(TaskStatsCache::new()),
            task_facets_cache: Arc::new(ReadCache::new()),
            task_breakdown_cache: Arc::new(ReadCache::new()),
            events: EventBroadcaster::new(Arc::clone(&broker)),
            broker,
            authorization_policy: Arc::new(GrantedAccess),
            actions: config.actions,
            ui_config: config.ui,
            assets: Arc::new(EmbeddedAssets),
            children,
            heartbeat: config.heartbeat,
            client_version: config.client_version,
            fleet_metrics,
        };
        Self::from_state(state)
    }

    /// The handler of a built state, whose authorization policy must be
    /// [`GrantedAccess`]-like: it reads the grant of each request.
    pub(crate) fn from_state(state: WebState) -> Self {
        Self {
            events: Arc::clone(&state.events),
            actions: state.actions,
            heartbeat: state.heartbeat,
            router: build_router(state),
        }
    }

    /// The event broadcaster of the handler. Its `close` ends every event
    /// stream of the handler after the degraded frame.
    pub fn events(&self) -> &Arc<EventBroadcaster> {
        &self.events
    }

    /// Subscribe one event stream (`/api/events`). The host calls it after
    /// the user's policy allowed the view, and writes the frames of the
    /// answer. The first subscriber starts the broadcaster's subscriptions
    /// on the broker's listener; a broker without a listener, or a listener
    /// that stopped, answers the degraded frame and the end.
    pub async fn subscribe_events(&self) -> EventFrames {
        EventFrames::new(self.events.subscribe().await, self.heartbeat)
    }

    /// The access that a request needs: [`route_access`] with the actions
    /// switch of this handler. Pure.
    pub fn route_access(&self, method: &Method, path: &str) -> RouteAccess {
        route_access(self.actions, method, path)
    }

    /// Serve one request with the verdicts of `grant`.
    ///
    /// Every expected outcome is a status and a body: the refusals of the
    /// guards (403), a path that is not a valid request path (400), the
    /// schema gate (409 `SCHEMA_INCOMPATIBLE`), a request body over
    /// [`MAX_REQUEST_BODY_BYTES`] when no verdict refused (413), validation
    /// (422), a response body
    /// over [`MAX_RESPONSE_BYTES`] (500) and the missing page (503). An
    /// action writes, and runs to its end when the host leaves. The event
    /// stream path answers here only when the view is refused (403).
    ///
    /// Panics on a defect of the host: a grant that does not answer the
    /// verdicts of [`route_access`](Self::route_access), or an allowed
    /// request for the event stream, which has no single response.
    pub async fn handle(&self, request: RequestParts, grant: AccessGrant) -> ResponseParts {
        let access = self.route_access(&request.method, &request.path);
        assert!(
            grant.fits(access.need),
            "monitoring handler: the grant {grant:?} does not answer the need {:?} of {} {}",
            access.need,
            request.method,
            request.path,
        );
        assert!(
            access.form == ResponseForm::OneResponse || grant.view == Verdict::Refused,
            "monitoring handler: {} {} is the event stream; it has no single response",
            request.method,
            request.path,
        );
        // A refused verdict is answered by the guards, which read no body.
        let refused_by_policy = grant.view == Verdict::Refused || grant.act == Verdict::Refused;
        if request.body.len() > MAX_REQUEST_BODY_BYTES && !refused_by_policy {
            let refused = ApiError::detail(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("The request body is over {MAX_REQUEST_BODY_BYTES} bytes."),
            );
            return response_parts(refused.into_response()).await;
        }
        let routed = match http_request(request, grant) {
            Ok(routed) => routed,
            Err(refused) => return response_parts(refused.into_response()).await,
        };
        let response = match self.router.clone().oneshot(routed).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
        response_parts(response).await
    }
}

/// The answer to a request that a verdict of the user's policy refused: 403
/// `Not authorized.`, the answer of the router's guards. `None` when no
/// verdict refused the request.
///
/// A host that builds its [`MonitoringHandler`] on demand (a broker that
/// connects at the first request) calls it before the build: a refused
/// caller gets this answer and learns nothing about the database. Pure: it
/// reads no request body and no broker.
pub async fn refusal_by_policy(grant: AccessGrant) -> Option<ResponseParts> {
    match (grant.view, grant.act) {
        (Verdict::Refused, _) | (_, Verdict::Refused) => {
            let refused = ApiError::detail(StatusCode::FORBIDDEN, NOT_AUTHORIZED);
            let mut answer = response_parts(refused.into_response()).await;
            // The router's answer carries its length; so does this one.
            answer.headers.push((
                http::header::CONTENT_LENGTH,
                HeaderValue::from(answer.body.len()),
            ));
            Some(answer)
        }
        (Verdict::Allowed | Verdict::NotAsked, Verdict::Allowed | Verdict::NotAsked) => None,
    }
}

/// The access that a request needs, from its method and its path inside the
/// mount, as the routes of the Python app guard them: every read route,
/// `/metrics` and the event stream need `can_view`; `/api/meta` asks
/// `can_act` to report it; an action route needs `can_view` and `can_act`,
/// or only `can_view` while actions are disabled ([`Actions::Disabled`]:
/// the act guard refuses without asking). Every other path is public. Pure.
pub fn route_access(actions: Actions, method: &Method, path: &str) -> RouteAccess {
    let segments: Vec<&str> = path.strip_prefix('/').unwrap_or(path).split('/').collect();
    let action = match actions {
        Actions::Enabled => AccessNeed::ViewAndAct,
        Actions::Disabled => AccessNeed::View,
    };
    // A HEAD request is served by the GET route, with its guards.
    let verb = match method.as_str() {
        "HEAD" => "GET",
        other => other,
    };
    let need = match (verb, segments.as_slice()) {
        ("GET", ["api", "meta"]) => AccessNeed::ViewReportAct,
        ("GET", ["api", "events"]) => {
            return RouteAccess {
                need: AccessNeed::View,
                form: ResponseForm::EventStream,
            }
        }
        ("GET", ["api", "tasks" | "workflows" | "workers"]) => AccessNeed::View,
        ("GET", ["api", "tasks" | "workflows", id]) if named(id) => AccessNeed::View,
        ("GET", ["api", "workflows", id, "tasks", index]) if named(id) && named(index) => {
            AccessNeed::View
        }
        ("GET", ["api", "workers", "ping" | "schedules" | "classes"]) => AccessNeed::View,
        ("GET", ["api", "workers", id, "history"]) if named(id) => AccessNeed::View,
        ("GET", ["metrics"]) => AccessNeed::View,
        ("POST", ["api", "tasks", id, "cancel"]) if named(id) => action,
        ("POST", ["api", "workflows", id, "pause" | "resume" | "cancel"]) if named(id) => action,
        _ => AccessNeed::Public,
    };
    RouteAccess {
        need,
        form: ResponseForm::OneResponse,
    }
}

/// Whether a path segment can fill a route parameter: axum matches a
/// parameter to one segment that is not empty.
fn named(segment: &str) -> bool {
    !segment.is_empty()
}

/// The routed request of `parts`, with the grant and the mount path in its
/// extensions. A path or a header value that is not valid request text is
/// refused with 400.
fn http_request(parts: RequestParts, grant: AccessGrant) -> Result<Request<Body>, ApiError> {
    let uri = match &parts.raw_query {
        Some(query) => format!("{}?{query}", parts.path),
        None => parts.path.clone(),
    };
    let mut builder = Request::builder().method(parts.method).uri(uri);
    if let Some(intent) = &parts.intent_header {
        builder = builder.header(INTENT_HEADER, intent.as_str());
    }
    if let Some(content_type) = &parts.content_type {
        builder = builder.header(axum::http::header::CONTENT_TYPE, content_type.as_str());
    }
    let mut request = builder
        .body(Body::from(parts.body))
        .map_err(|error| ApiError::bad_request(format!("The request is not valid: {error}")))?;
    let _ = request.extensions_mut().insert(grant);
    let _ = request.extensions_mut().insert(MountPath(parts.base_path));
    Ok(request)
}

/// The parts of a router response, with its body read up to
/// [`MAX_RESPONSE_BYTES`].
async fn response_parts(response: axum::response::Response) -> ResponseParts {
    let (head, body) = response.into_parts();
    match to_bytes(body, MAX_RESPONSE_BYTES).await {
        Ok(bytes) => ResponseParts {
            status: head.status,
            headers: head
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            body: bytes,
        },
        Err(error) => {
            tracing::error!(
                "monitoring handler: the response of status {} was not read: {error}",
                head.status
            );
            let refused = ApiError::detail(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("The response is over {MAX_RESPONSE_BYTES} bytes or failed: {error}"),
            )
            .into_response();
            let (head, body) = refused.into_parts();
            ResponseParts {
                status: head.status,
                headers: head
                    .headers
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                body: to_bytes(body, MAX_RESPONSE_BYTES)
                    .await
                    .expect("a refusal body is a short JSON text"),
            }
        }
    }
}
