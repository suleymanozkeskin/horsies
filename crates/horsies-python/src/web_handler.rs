//! `MonitoringHandler`: the core's framework-neutral monitoring handler
//! for the Python client's `create_monitoring_app`, which mounts it in
//! FastAPI and runs the user's authorization policy.
//!
//! The host asks `route_access` (pure) which verdicts of the user's policy a
//! request needs, runs the policy for those and no other, and passes the
//! verdicts with the request to `handle_async`, which answers with one
//! `MonitoringResponse`. The event stream (`/api/events`) has no single
//! response: after an allowed view the host calls `subscribe_events_async`
//! and writes the frame texts of the `EventStream`.
//!
//! The handler is built at its first call on the broker of its `CoreHandle`
//! (the broker that every other operation of the handle uses, so the event
//! stream subscribes on the process listener). Building it opens no
//! connection. The resume route builds the sub-workflow children that a
//! resume owes through the handle's child spec source (the Python client's
//! `build_with`); a build that raised `KeyboardInterrupt` or `SystemExit`
//! is raised again by the action call that ends after it. A build that
//! raised an `Exception` fails the resume, and the response carries its type
//! and text.

use std::sync::Arc;
use std::time::Duration;

use horsies_core::web::{
    route_access, AccessGrant as CoreGrant, AccessNeed, Bytes, EventFrames, EventStreamItem,
    Method, MonitoringHandler as CoreHandler, MonitoringHandlerConfig, MonitoringUiConfig,
    RequestParts, ResponseForm, ResponseParts, Verdict, DEGRADED_FRAME, MAX_REQUEST_BODY_BYTES,
};
use horsies_core::{BrokerError, ChildSpecSource};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::IntoPyObjectExt;
use tokio::sync::{watch, Mutex, OnceCell};

use crate::bridge::completion_queue::call_async;
use crate::bridge::CallKind;
use crate::child_spec::{PythonChildSpecSource, RaisedExceptions, RegisteredSource};
use crate::handle::{checked_core, BrokerConnection, CoreHandle};
use crate::process_core::Core;

/// The longest heartbeat of an event stream that a handler takes: one hour.
const MAX_HEARTBEAT_MS: u64 = 3_600_000;

/// The verdicts of the user's policy that a request needs, and the form of
/// its answer.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteAccess {
    /// `PUBLIC`, `VIEW`, `VIEW_REPORT_ACT` or `VIEW_AND_ACT`.
    pub need: String,
    /// `ONE_RESPONSE` or `EVENT_STREAM`.
    pub form: String,
}

impl RouteAccess {
    fn of(access: horsies_core::web::RouteAccess) -> Self {
        let need = match access.need {
            AccessNeed::Public => "PUBLIC",
            AccessNeed::View => "VIEW",
            AccessNeed::ViewReportAct => "VIEW_REPORT_ACT",
            AccessNeed::ViewAndAct => "VIEW_AND_ACT",
        };
        let form = match access.form {
            ResponseForm::OneResponse => "ONE_RESPONSE",
            ResponseForm::EventStream => "EVENT_STREAM",
        };
        Self {
            need: need.to_owned(),
            form: form.to_owned(),
        }
    }
}

/// The verdict text of a policy answer. Pure.
fn verdict_text(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::Allowed => "ALLOWED",
        Verdict::Refused => "REFUSED",
        Verdict::NotAsked => "NOT_ASKED",
    }
}

/// The verdict of its text. Pure.
///
/// Failure exits: the refusal text for another text (a defect of the host).
fn parse_verdict(field: &str, text: &str) -> Result<Verdict, String> {
    match text {
        "ALLOWED" => Ok(Verdict::Allowed),
        "REFUSED" => Ok(Verdict::Refused),
        "NOT_ASKED" => Ok(Verdict::NotAsked),
        other => Err(format!(
            "{field} must be 'ALLOWED', 'REFUSED' or 'NOT_ASKED', not {other:?}"
        )),
    }
}

/// The verdicts that the host's run of the user's policy gave for one
/// request: `ALLOWED`, `REFUSED`, or `NOT_ASKED` (the route needs no such
/// verdict, or an earlier verdict refused).
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Copy, Debug)]
pub struct AccessGrant {
    grant: CoreGrant,
}

#[pymethods]
impl AccessGrant {
    /// A grant of the two verdicts. Another text raises `ValueError`.
    #[new]
    #[pyo3(signature = (*, view, act))]
    fn new(view: &str, act: &str) -> PyResult<Self> {
        let view = parse_verdict("view", view).map_err(PyValueError::new_err)?;
        let act = parse_verdict("act", act).map_err(PyValueError::new_err)?;
        Ok(Self {
            grant: CoreGrant { view, act },
        })
    }

    #[getter]
    fn view(&self) -> &'static str {
        verdict_text(self.grant.view)
    }

    #[getter]
    fn act(&self) -> &'static str {
        verdict_text(self.grant.act)
    }
}

/// The HTTP method of its text. Pure.
///
/// Failure exits: `ValueError` for text that is not an HTTP token (a
/// defect of the host: its server parsed the request).
fn parse_method(method: &str) -> PyResult<Method> {
    Method::from_bytes(method.as_bytes())
        .map_err(|error| PyValueError::new_err(format!("method {method:?} is refused: {error}")))
}

/// One request, as the host framework read it, with the verdicts of the
/// user's policy.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct MonitoringRequest {
    parts: RequestParts,
    grant: CoreGrant,
}

#[pymethods]
impl MonitoringRequest {
    /// `path` is the path inside the mount, from its leading `/`, as it came
    /// on the wire (percent-encoded); `raw_query` the text after `?`, or
    /// None; `base_path` the mount path (the ASGI `root_path`). `body` holds
    /// at most `MonitoringHandler.max_request_body_bytes + 1` bytes: a host
    /// stops reading past the bound. A method that is not an HTTP token
    /// raises `ValueError`.
    #[new]
    #[pyo3(signature = (
        *, method, path, raw_query, intent_header, content_type, body, base_path, grant
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        method: &str,
        path: String,
        raw_query: Option<String>,
        intent_header: Option<String>,
        content_type: Option<String>,
        body: &Bound<'_, PyBytes>,
        base_path: String,
        grant: &Bound<'_, AccessGrant>,
    ) -> PyResult<Self> {
        Ok(Self {
            parts: RequestParts {
                method: parse_method(method)?,
                path,
                raw_query,
                intent_header,
                content_type,
                body: Bytes::copy_from_slice(body.as_bytes()),
                base_path,
            },
            grant: grant.get().grant,
        })
    }
}

/// One response for the host framework to send.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct MonitoringResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl MonitoringResponse {
    fn of(parts: ResponseParts) -> Self {
        Self {
            status: parts.status.as_u16(),
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        String::from_utf8_lossy(value.as_bytes()).into_owned(),
                    )
                })
                .collect(),
            body: parts.body,
        }
    }

    /// The 503 of a handler whose broker could not be built (a connection
    /// setting that the core refuses).
    fn broker_unavailable(error: &BrokerError) -> Self {
        let body = serde_json::json!({
            "detail": format!("The monitoring handler has no broker: {error}"),
        });
        Self {
            status: 503,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: Bytes::from(body.to_string()),
        }
    }
}

#[pymethods]
impl MonitoringResponse {
    /// The HTTP status code.
    #[getter]
    fn status(&self) -> u16 {
        self.status
    }

    /// The headers, in order, with lowercase names.
    #[getter]
    fn headers(&self) -> Vec<(String, String)> {
        self.headers.clone()
    }

    /// The body.
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.body)
    }
}

/// One frame of an event stream, with the blank line that ends it.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct EventFrame {
    pub text: String,
}

/// The event stream ended: after the degraded frame, after the handler
/// closed its events, or after `close`.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct EventStreamEnded {}

/// No stream: the handler has no broker. The host writes `frame`, the
/// degraded frame, and ends the response.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct EventStreamRefused {
    pub message: String,
    pub frame: String,
}

/// The next item of an event stream.
enum StreamStep {
    Frame(EventFrame),
    Ended(EventStreamEnded),
}

impl<'py> IntoPyObject<'py> for StreamStep {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Frame(frame) => frame.into_bound_py_any(py),
            Self::Ended(ended) => ended.into_bound_py_any(py),
        }
    }
}

/// One web client's event stream. `next_async` waits for the next frame;
/// a cancelled wait drops the wait, not the subscription, and loses no
/// event. `close` ends the stream.
#[pyclass(module = "horsies._core", frozen)]
pub struct EventStream {
    frames: Arc<Mutex<EventFrames>>,
    closed: watch::Sender<bool>,
}

impl EventStream {
    fn new(frames: EventFrames) -> Self {
        let (closed, _receiver) = watch::channel(false);
        Self {
            frames: Arc::new(Mutex::new(frames)),
            closed,
        }
    }
}

/// The next step of `frames`, or the end once `closed` turns true.
async fn next_step(
    frames: Arc<Mutex<EventFrames>>,
    mut closed: watch::Receiver<bool>,
) -> StreamStep {
    tokio::select! {
        biased;
        _closed = closed.wait_for(|closed| *closed) => StreamStep::Ended(EventStreamEnded {}),
        item = async { frames.lock().await.next().await } => match item {
            EventStreamItem::Frame(text) => StreamStep::Frame(EventFrame { text }),
            EventStreamItem::Ended => StreamStep::Ended(EventStreamEnded {}),
        },
    }
}

#[pymethods]
impl EventStream {
    /// The next frame: `EventFrame`, or `EventStreamEnded`. Reads only; a
    /// cancelled await drops the wait.
    fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "EventStream.next_async",
            CallKind::Read,
            next_step(Arc::clone(&self.frames), self.closed.subscribe()),
        )
    }

    /// End the stream: a pending and every later `next_async` answers
    /// `EventStreamEnded`. Starts nothing and waits for nothing; the
    /// subscription goes when the stream object goes.
    fn close(&self) {
        let _was_closed = self.closed.send_replace(true);
    }
}

/// What a handler is built from, kept until its first call.
struct HandlerPlan {
    connection: BrokerConnection,
    child_source: RegisteredSource,
    config: MonitoringHandlerConfig,
}

/// A built handler and the child spec source that its resume route uses.
struct BuiltHandler {
    handler: CoreHandler,
    child_source: Arc<PythonChildSpecSource>,
}

/// The plan of a handler and its build, made once.
struct HandlerState {
    plan: HandlerPlan,
    built: OnceCell<Arc<BuiltHandler>>,
}

impl HandlerState {
    /// The built handler. The first call connects the handle's broker (no
    /// connection opens) and builds the handler; a failure is returned and
    /// not kept.
    async fn built(&self, core: &'static Core) -> Result<Arc<BuiltHandler>, BrokerError> {
        self.built
            .get_or_try_init(|| async {
                let broker = self.plan.connection.broker().await?;
                let child_source = Arc::new(PythonChildSpecSource::new(
                    core,
                    Arc::clone(&self.plan.child_source),
                    RaisedExceptions::InRecord,
                ));
                let source = Arc::clone(&child_source) as Arc<dyn ChildSpecSource>;
                let handler = CoreHandler::new(broker, source, self.plan.config.clone());
                Ok::<_, BrokerError>(Arc::new(BuiltHandler {
                    handler,
                    child_source,
                }))
            })
            .await
            .map(Arc::clone)
    }
}

/// The answer of `handle_async`: the response, or the interrupt that a
/// child build raised.
enum HandleOutcome {
    Response(MonitoringResponse),
    Interrupted(PyErr),
}

impl<'py> IntoPyObject<'py> for HandleOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Response(response) => response.into_bound_py_any(py),
            Self::Interrupted(exception) => Err(exception),
        }
    }
}

/// The answer of `subscribe_events_async`.
enum SubscribeOutcome {
    Stream(EventStream),
    Refused(EventStreamRefused),
}

impl<'py> IntoPyObject<'py> for SubscribeOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Stream(stream) => stream.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

async fn handle_operation(
    state: Arc<HandlerState>,
    core: &'static Core,
    request: MonitoringRequest,
) -> HandleOutcome {
    let built = match state.built(core).await {
        Ok(built) => built,
        Err(error) => {
            return HandleOutcome::Response(MonitoringResponse::broker_unavailable(&error))
        }
    };
    let response = built.handler.handle(request.parts, request.grant).await;
    match built.child_source.take_interrupt() {
        Some(exception) => HandleOutcome::Interrupted(exception),
        None => HandleOutcome::Response(MonitoringResponse::of(response)),
    }
}

async fn subscribe_operation(state: Arc<HandlerState>, core: &'static Core) -> SubscribeOutcome {
    match state.built(core).await {
        Ok(built) => {
            SubscribeOutcome::Stream(EventStream::new(built.handler.subscribe_events().await))
        }
        Err(error) => SubscribeOutcome::Refused(EventStreamRefused {
            message: format!("The monitoring handler has no broker: {error}"),
            frame: DEGRADED_FRAME.to_owned(),
        }),
    }
}

/// End every event stream of a built handler after the degraded frame. A
/// handler that was never built has no stream.
async fn close_events_operation(state: Arc<HandlerState>) {
    if let Some(built) = state.built.get() {
        built.handler.events().close().await;
    }
}

/// The heartbeat of `heartbeat_ms`. Pure.
///
/// Failure exits: the refusal text for 0 or a value over
/// [`MAX_HEARTBEAT_MS`].
fn heartbeat_of(heartbeat_ms: u64) -> Result<Duration, String> {
    match heartbeat_ms {
        0 => Err("heartbeat_ms must be at least 1".to_owned()),
        over if over > MAX_HEARTBEAT_MS => Err(format!(
            "heartbeat_ms must be at most {MAX_HEARTBEAT_MS}, not {over}"
        )),
        valid => Ok(Duration::from_millis(valid)),
    }
}

/// The monitoring handler of one `CoreHandle`.
#[pyclass(module = "horsies._core", name = "MonitoringHandler", frozen)]
pub struct MonitoringHandler {
    actions_enabled: bool,
    state: Arc<HandlerState>,
}

#[pymethods]
impl MonitoringHandler {
    /// The handler of `handle`'s broker. `client_version` is the version
    /// that `/api/meta` reports; `heartbeat_ms` the wait of an event stream
    /// without an event before its heartbeat frame (1 to 3,600,000).
    /// Starts nothing: the first call builds the handler. A handle without
    /// a child spec source raises `RuntimeError`; a heartbeat out of bounds
    /// raises `ValueError`.
    #[new]
    #[pyo3(signature = (handle, *, actions_enabled, custom_css_url, client_version, heartbeat_ms))]
    fn new(
        handle: &Bound<'_, CoreHandle>,
        actions_enabled: bool,
        custom_css_url: Option<String>,
        client_version: String,
        heartbeat_ms: u64,
    ) -> PyResult<Self> {
        let heartbeat = heartbeat_of(heartbeat_ms).map_err(PyValueError::new_err)?;
        let handle = handle.get();
        let child_source = handle.registered_child_spec_source().ok_or_else(|| {
            PyRuntimeError::new_err(
                "CoreHandle has no child spec source; call register_child_spec_source first",
            )
        })?;
        Ok(Self {
            actions_enabled,
            state: Arc::new(HandlerState {
                plan: HandlerPlan {
                    connection: handle.connection(),
                    child_source,
                    config: MonitoringHandlerConfig {
                        actions_enabled,
                        ui: MonitoringUiConfig { custom_css_url },
                        client_version,
                        heartbeat,
                    },
                },
                built: OnceCell::new(),
            }),
        })
    }

    /// The most body bytes that `handle_async` takes; a host reads at most
    /// one byte more and passes what it read.
    #[getter]
    fn max_request_body_bytes(&self) -> usize {
        MAX_REQUEST_BODY_BYTES
    }

    /// The verdicts that a request needs and the form of its answer, from
    /// its method and its path inside the mount. Pure. A method that is not
    /// an HTTP token raises `ValueError`.
    fn route_access(&self, method: &str, path: &str) -> PyResult<RouteAccess> {
        Ok(RouteAccess::of(route_access(
            self.actions_enabled,
            &parse_method(method)?,
            path,
        )))
    }

    /// Serve one request: `MonitoringResponse`. Every expected outcome is a
    /// status and a body. A `POST` (an action) writes and runs to its end
    /// when the caller leaves; every other request reads, and a cancelled
    /// await drops it. A child build that raised `KeyboardInterrupt` or
    /// `SystemExit` raises it again; a grant that does not answer the need
    /// of `route_access`, or an allowed view of the event stream, raises
    /// `PanicException` (a defect of the host).
    fn handle_async<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'py, MonitoringRequest>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = request.get().clone();
        let kind = match request.parts.method == Method::POST {
            true => CallKind::Write,
            false => CallKind::Read,
        };
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "MonitoringHandler.handle_async",
            kind,
            handle_operation(Arc::clone(&self.state), core, request),
        )
    }

    /// Subscribe one event stream, after the user's policy allowed the view:
    /// `EventStream`, or `EventStreamRefused` when the handler has no
    /// broker. A broker without a listener, or a stopped listener, gives a
    /// stream whose first frame is the degraded frame. Reads only.
    fn subscribe_events_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "MonitoringHandler.subscribe_events_async",
            CallKind::Read,
            subscribe_operation(Arc::clone(&self.state), core),
        )
    }

    /// End every event stream of the handler after the degraded frame:
    /// None. Runs to its end when the caller leaves.
    fn close_events_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = checked_core(py)?;
        call_async(
            py,
            core,
            "MonitoringHandler.close_events_async",
            CallKind::Write,
            close_events_operation(Arc::clone(&self.state)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_verdict_text_parses_back() {
        for verdict in [Verdict::Allowed, Verdict::Refused, Verdict::NotAsked] {
            assert_eq!(parse_verdict("view", verdict_text(verdict)), Ok(verdict));
        }
        assert!(parse_verdict("act", "allowed").is_err());
    }

    #[test]
    fn route_access_names_need_and_form() {
        let access = RouteAccess::of(route_access(true, &Method::GET, "/api/events"));
        assert_eq!(access.need, "VIEW");
        assert_eq!(access.form, "EVENT_STREAM");
        let meta = RouteAccess::of(route_access(true, &Method::GET, "/api/meta"));
        assert_eq!(meta.need, "VIEW_REPORT_ACT");
        assert_eq!(meta.form, "ONE_RESPONSE");
    }

    #[test]
    fn the_heartbeat_is_bounded() {
        assert!(heartbeat_of(0).is_err());
        assert_eq!(heartbeat_of(15_000), Ok(Duration::from_millis(15_000)));
        assert_eq!(
            heartbeat_of(MAX_HEARTBEAT_MS),
            Ok(Duration::from_millis(MAX_HEARTBEAT_MS))
        );
        assert!(heartbeat_of(MAX_HEARTBEAT_MS + 1).is_err());
    }

    #[test]
    fn a_broker_failure_is_a_json_503() {
        let response = MonitoringResponse::broker_unavailable(&BrokerError::ConnectionFailed(
            "refused URL".to_owned(),
        ));
        assert_eq!(response.status, 503);
        let body: serde_json::Value = serde_json::from_slice(&response.body).expect("a JSON body");
        assert!(body["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("refused URL")));
    }
}
