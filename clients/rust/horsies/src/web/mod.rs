//! The axum mount of the core's framework-neutral monitoring handler, with
//! the access policy that user code decides per request (Python
//! `horsies.web.create_monitoring_app`).
//!
//! The core serves every route, guard, cache, the event frames and the
//! embedded UI ([`horsies_core::web::MonitoringHandler`]); this module only
//! moves requests and responses between axum and the handler:
//!
//! 1. The route's needed verdicts come from the core's `route_access`.
//! 2. The mount asks the user's [`MonitoringAuthorizationPolicy`] for those
//!    verdicts, and no other.
//! 3. The event stream (`GET /api/events`), once the view is allowed, is the
//!    core's frame texts as a `text/event-stream` body. Every other request
//!    goes to the handler with its verdicts, and its answer is sent as it
//!    is.
//!
//! `GET /metrics` is the fleet metrics of the app's database, behind the
//! view verdict. Its `queue` and `schedule` labels are the queues and the
//! schedules of the app's config; other names are `(unknown)`.
//!
//! ```ignore
//! let app = horsies::Horsies::with_schema_mode(config, horsies::SchemaMode::ObserveOnly)?;
//! let monitoring = horsies::web::router(&app, MonitoringConfig::default(), ViewOnly);
//! let host = axum::Router::new().nest("/monitoring", monitoring);
//! ```

mod auth;

use std::convert::Infallible;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{NestedPath, Request, State};
use axum::http::header::{AsHeaderName, CACHE_CONTROL, CONTENT_TYPE};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use futures_util::StreamExt;
use horsies_core::db::Transience;
use horsies_core::handle::CoreConnection;
use horsies_core::monitoring::database_failure_status;
use horsies_core::monitoring::fleet::FleetLabels;
use horsies_core::telemetry::declared::{warn_folded_names, Folded};
use horsies_core::web::{
    route_access, AccessGrant, AccessNeed, EventFrames, EventStreamItem, MonitoringHandler,
    MonitoringHandlerConfig, RequestParts, ResponseForm, ResponseParts, Verdict, HEARTBEAT,
    MAX_REQUEST_BODY_BYTES,
};
use horsies_core::{BrokerError, ChildSpecSource};
use tokio::sync::OnceCell;

use crate::process_state::refuse_in_a_fork;
use crate::workflow::WorkflowRegistry;
use crate::Horsies;

pub use auth::{
    AllowAll, MonitoringAuthorizationPolicy, MonitoringRequest, TrustedHeader, ViewOnly,
};
pub use horsies_core::web::{
    Actions, MonitoringUiConfig, TraceLink, TraceLinkRefusal, TraceLinkTemplate, INTENT_HEADER,
    INTENT_VALUE,
};

/// The settings of a monitoring mount. The default enables the actions, as
/// Python's `actions=Actions.ENABLED`, with the default UI settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitoringConfig {
    pub ui: MonitoringUiConfig,
    pub actions: Actions,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            ui: MonitoringUiConfig::default(),
            actions: Actions::Enabled,
        }
    }
}

/// The monitoring web app of `app` as an axum router, for the host to
/// serve or nest (`Router::nest("/monitoring", ...)`). Every path goes to
/// the router: the page and its assets, `/api/...`, and the event stream.
///
/// Building it does no I/O: the core handler is built at the first request,
/// on the broker of the app's handle; a request whose broker cannot be built
/// answers with the cause, 503 when the cause may clear and 500 when it
/// stays (for example a connection setting that the core refuses), and the
/// next request tries again. A resume builds its owed children through the
/// workflow registry of `app`, with the definitions registered when each
/// child builds. No schema step runs: build the app with
/// `SchemaMode::ObserveOnly` for a monitoring tool.
pub fn router<P>(app: &Horsies, config: MonitoringConfig, authorization_policy: P) -> Router
where
    P: MonitoringAuthorizationPolicy,
{
    let fleet_labels = fleet_labels_of(app);
    let mount = Arc::new(Mount {
        handle: app.reads_handle(),
        workflows: Arc::clone(app.workflows()),
        settings: MonitoringHandlerConfig {
            actions: config.actions,
            ui: config.ui,
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            heartbeat: HEARTBEAT,
            fleet_labels,
        },
        policy: authorization_policy,
        handler: OnceCell::new(),
    });
    Router::new().fallback(serve::<P>).with_state(mount)
}

/// The label names of `/metrics`: the queues and the schedules of the app's
/// config. A config name that cannot be a label value of its own (empty,
/// the text `(unknown)`, or past the bound of names) is not declared: its
/// samples count in the `(unknown)` series, and one WARNING line names it.
fn fleet_labels_of(app: &Horsies) -> FleetLabels {
    let Folded {
        declared: labels,
        folded,
    } = FleetLabels::of_config(app.config());
    warn_folded_names(&"the fleet metrics of /metrics", &folded);
    labels
}

/// What every request of one mount reads.
struct Mount<P> {
    handle: Arc<CoreConnection>,
    workflows: Arc<WorkflowRegistry>,
    settings: MonitoringHandlerConfig,
    policy: P,
    handler: OnceCell<MonitoringHandler>,
}

impl<P> Mount<P> {
    /// The core handler, built at the first call on the broker of the app's
    /// handle. A failed build is not kept.
    ///
    /// Errors: the [`BrokerError`] of the handle's connect.
    ///
    /// # Panics
    ///
    /// At every request, in a process that was forked without `exec` after
    /// the core's process runtime started. Nothing ran.
    async fn handler(&self) -> Result<&MonitoringHandler, BrokerError> {
        refuse_in_a_fork(format_args!("the monitoring request"));
        self.handler.get_or_try_init(|| self.build_handler()).await
    }

    /// A handler on the broker of the app's handle; a resume builds its
    /// owed children through the app's workflow registry.
    ///
    /// Errors: the [`BrokerError`] of the handle's connect.
    async fn build_handler(&self) -> Result<MonitoringHandler, BrokerError> {
        let broker = self.handle.broker().await?;
        let registry = Arc::clone(&self.workflows);
        let source: Arc<dyn ChildSpecSource> = registry;
        Ok(MonitoringHandler::new(
            broker,
            source,
            self.settings.clone(),
        ))
    }
}

/// Serve one request of the mount: the verdicts of the policy, then the
/// event stream or the handler's answer.
async fn serve<P>(State(mount): State<Arc<Mount<P>>>, request: Request) -> Response
where
    P: MonitoringAuthorizationPolicy,
{
    let (head, body) = request.into_parts();
    let access = route_access(mount.settings.actions, &head.method, head.uri.path());
    let grant = grant_of(&mount.policy, access.need, MonitoringRequest::of(&head)).await;
    let handler = match mount.handler().await {
        Ok(handler) => handler,
        Err(error) => return handler_build_failed(&error),
    };
    match (access.form, grant.view) {
        (ResponseForm::EventStream, Verdict::Allowed) => {
            event_stream(handler.subscribe_events().await)
        }
        (ResponseForm::OneResponse, _)
        | (ResponseForm::EventStream, Verdict::Refused | Verdict::NotAsked) => {
            let body = match read_body(body).await {
                Ok(body) => body,
                Err(error) => {
                    return detail(
                        StatusCode::BAD_REQUEST,
                        &format!("The request body could not be read: {error}"),
                    )
                }
            };
            into_response(handler.handle(request_parts(&head, body), grant).await)
        }
    }
}

/// The verdicts of `policy` that `need` asks for, in order: `can_view`,
/// then `can_act` only when the view was allowed.
async fn grant_of<P>(policy: &P, need: AccessNeed, request: MonitoringRequest<'_>) -> AccessGrant
where
    P: MonitoringAuthorizationPolicy,
{
    match need {
        AccessNeed::Public => AccessGrant {
            view: Verdict::NotAsked,
            act: Verdict::NotAsked,
        },
        AccessNeed::View => AccessGrant {
            view: verdict(policy.can_view(request).await),
            act: Verdict::NotAsked,
        },
        AccessNeed::ViewReportAct | AccessNeed::ViewAndAct => {
            match policy.can_view(request).await {
                false => AccessGrant {
                    view: Verdict::Refused,
                    act: Verdict::NotAsked,
                },
                true => AccessGrant {
                    view: Verdict::Allowed,
                    act: verdict(policy.can_act(request).await),
                },
            }
        }
    }
}

/// The verdict of one answer of the policy. Pure.
fn verdict(allowed: bool) -> Verdict {
    match allowed {
        true => Verdict::Allowed,
        false => Verdict::Refused,
    }
}

/// The body of a request, read up to one byte over
/// [`MAX_REQUEST_BODY_BYTES`]: a longer body is cut there and stops being
/// read, and the core answers it 413 (or 403 when a verdict refused). The
/// read is bounded by that size.
///
/// Errors: the transfer failed (the client went away, a bad chunk).
async fn read_body(body: Body) -> Result<Bytes, axum::Error> {
    let limit = MAX_REQUEST_BODY_BYTES + 1;
    let mut read: Vec<u8> = Vec::new();
    let mut chunks = body.into_data_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        let room = limit - read.len();
        match chunk.len() >= room {
            true => {
                read.extend_from_slice(&chunk[..room]);
                break;
            }
            false => read.extend_from_slice(&chunk),
        }
    }
    Ok(Bytes::from(read))
}

/// The request parts that the core handler reads. The path is the path
/// inside the mount, as it came on the wire; the base path is the mount
/// path of a nested router, `""` at the root. A header value that is not
/// visible text is read as absent: the intent header then does not match,
/// and the content type is not JSON. Pure.
fn request_parts(head: &Parts, body: Bytes) -> RequestParts {
    RequestParts {
        method: head.method.clone(),
        path: head.uri.path().to_owned(),
        raw_query: head.uri.query().map(str::to_owned),
        intent_header: header_text(&head.headers, INTENT_HEADER),
        content_type: header_text(&head.headers, CONTENT_TYPE),
        body,
        base_path: head
            .extensions
            .get::<NestedPath>()
            .map(|nested| nested.as_str().to_owned())
            .unwrap_or_default(),
    }
}

/// The text of the header `name`, when the request has it as visible text.
/// Pure.
fn header_text<K>(headers: &HeaderMap, name: K) -> Option<String>
where
    K: AsHeaderName,
{
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// The axum response of the handler's answer. Pure.
fn into_response(parts: ResponseParts) -> Response {
    let mut response = Response::new(Body::from(parts.body));
    *response.status_mut() = parts.status;
    for (name, value) in parts.headers {
        response.headers_mut().append(name, value);
    }
    response
}

/// The event stream response: the core's frame texts in order, until the
/// end, with the headers of Python's `/api/events`.
fn event_stream(frames: EventFrames) -> Response {
    let stream = futures_util::stream::unfold(frames, |mut frames| async move {
        match frames.next().await {
            EventStreamItem::Frame(text) => {
                Some((Ok::<Bytes, Infallible>(Bytes::from(text)), frames))
            }
            EventStreamItem::Ended => None,
        }
    });
    let mut response = Response::new(Body::from_stream(stream));
    let headers = response.headers_mut();
    let _previous = headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    let _previous = headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    let _previous = headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

/// The answer of a request whose handler could not be built: the broker
/// error of the handle's connect, with the status of its retry rule
/// ([`database_failure_status`]): 503 when the cause may clear, 500 when it
/// stays. Pure.
fn handler_build_failed(error: &BrokerError) -> Response {
    let status = StatusCode::from_u16(database_failure_status(Transience::of_retryable(
        error.is_retryable(),
    )))
    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    detail(
        status,
        &format!("The monitoring handler could not connect its broker: {error}"),
    )
}

/// A refusal of the mount itself, `{"detail": ...}` as the core's
/// refusals. Pure.
fn detail(status: StatusCode, detail: &str) -> Response {
    (status, Json(serde_json::json!({ "detail": detail }))).into_response()
}

#[cfg(test)]
mod tests;
