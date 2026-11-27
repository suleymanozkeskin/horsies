//! `MonitoringHandler`: the core's framework-neutral monitoring handler for
//! the package's `createMonitoringHandler`, a Fetch-API handler that runs
//! the user's access policy.
//!
//! - `buildMonitoringHandler(handle, settings)` (sync) checks the settings
//!   and keeps them. It starts nothing and opens no connection: the first
//!   call that needs the core handler builds it on the broker of the
//!   handle, the broker that every other operation of the handle uses, so
//!   the event stream subscribes on the process listener. A failed build is
//!   not kept; the next call tries again.
//! - `routeAccess(method, path)` (sync, pure) names the verdicts of the
//!   user's policy that a request needs, and the form of its answer.
//! - `handle(request)` serves one request with the verdicts of the policy.
//!   Every expected outcome is a status and a body.
//! - `subscribeEvents()` gives one event stream after an allowed view. Its
//!   `nextFrame()` resolves with one frame per call, or the end; `close()`
//!   ends it at once and drops its subscription.
//! - `closeEvents()` ends every event stream of the handler after the
//!   degraded frame. A stream subscribed later answers the degraded frame,
//!   then the end.
//! - The resume route builds the sub-workflow children that a resume owes
//!   through the child spec source of the handle. The app sets the source
//!   when it builds its handle; a handle without one is refused with
//!   `CLIENT_DEFECT` at the build.
//! - `checkTraceLinkTemplate(text)` (sync, pure) checks the template of the
//!   trace link of the page with the core's rules. A refusal names the rule
//!   of the core that the template breaks, with its facts and its text.
//! - `logMonitoringFailure(message)` (sync) writes one ERROR record of the
//!   `horsies.web` logger, for a failure that the package handles, such as
//!   a policy that threw.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use horsies_core::db::Transience;
use horsies_core::monitoring::database_failure_status;
use horsies_core::monitoring::fleet::FleetLabels;
use horsies_core::telemetry::declared::{
    warn_folded_names, DeclaredNames, Folded, FoldedNames, QueueName, ScheduleName,
};
use horsies_core::web::{
    route_access as core_route_access, AccessGrant, AccessNeed, Actions, Bytes, EventFrames,
    EventStreamItem, Method, MonitoringHandler as CoreHandler, MonitoringHandlerConfig,
    MonitoringUiConfig, RequestParts, ResponseForm, ResponseParts, RouteAccess, TraceLink,
    TraceLinkRefusal, TraceLinkTemplate, Verdict, DEGRADED_FRAME, MAX_REQUEST_BODY_BYTES,
};
use horsies_core::{BrokerError, ChildSpecSource};
use napi::bindgen_prelude::{Object, ToNapiValue, Uint8Array};
use napi::Env;
use napi_derive::napi;
use tokio::sync::{watch, Mutex, OnceCell};

use crate::call::guard::{sync_call, CallKind};
use crate::call::operation::{guarded_start, resolve, spawn_operation, EnvelopeResolver};
use crate::call::thrown::{Envelope, NativeThrown};
use crate::handle::core_handle::{BrokerConnection, CoreHandle};
use crate::logging::bridge::started_log;
use crate::values::boundary::{bytes_of_hex, u32_of_number, BoundaryRefusal};

/// The longest heartbeat of an event stream that a handler takes: one hour.
const MAX_HEARTBEAT_MS: u32 = 3_600_000;

/// [`MAX_REQUEST_BODY_BYTES`] as a JavaScript number. 1 MiB fits a `u32`;
/// a test checks the conversion.
const MAX_REQUEST_BODY_BYTES_NUMBER: u32 = MAX_REQUEST_BODY_BYTES as u32;

/// The rule that a handle without a child spec source breaks.
const CHILD_SPEC_SOURCE_RULE: &str = "the CoreHandle has a child spec source: the app sets it \
                                      when it builds its handle, before a monitoring handler";

/// The subject of the build of a handler, in its refusals.
const BUILD_SUBJECT: &str = "buildMonitoringHandler";

/// Whether the action routes run. `disabled`: they answer 403 and the
/// handler asks no `canAct` verdict for them.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionsSwitch {
    Enabled,
    Disabled,
}

impl ActionsSwitch {
    /// The core's actions switch of the same name. Pure.
    const fn core_actions(self) -> Actions {
        match self {
            Self::Enabled => Actions::Enabled,
            Self::Disabled => Actions::Disabled,
        }
    }
}

/// The verdicts of the user's policy that a request needs, in the order the
/// package asks them.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteNeed {
    /// No verdict.
    Public,
    /// `canView`.
    View,
    /// `canView`, then, when it allows, `canAct`, which the response reports.
    ViewReportAct,
    /// `canView`, then, when it allows, `canAct`.
    ViewAndAct,
}

/// How the package answers a request.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseFormName {
    /// One response of `handle`.
    OneResponse,
    /// The event stream of `subscribeEvents`, after an allowed view.
    EventStream,
}

/// The verdicts that a request needs and the form of its answer.
#[napi(object, object_from_js = false)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteAccessRecord {
    pub need: RouteNeed,
    pub form: ResponseFormName,
}

impl RouteAccessRecord {
    /// The record of the core's route access. Pure.
    fn of(access: RouteAccess) -> Self {
        Self {
            need: match access.need {
                AccessNeed::Public => RouteNeed::Public,
                AccessNeed::View => RouteNeed::View,
                AccessNeed::ViewReportAct => RouteNeed::ViewReportAct,
                AccessNeed::ViewAndAct => RouteNeed::ViewAndAct,
            },
            form: match access.form {
                ResponseForm::OneResponse => ResponseFormName::OneResponse,
                ResponseForm::EventStream => ResponseFormName::EventStream,
            },
        }
    }
}

/// One verdict of the user's policy. `notAsked`: the route needs no such
/// verdict, or an earlier verdict refused.
#[napi(string_enum = "camelCase")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessVerdict {
    Allowed,
    Refused,
    NotAsked,
}

impl AccessVerdict {
    /// The core's verdict. Pure.
    const fn verdict(self) -> Verdict {
        match self {
            Self::Allowed => Verdict::Allowed,
            Self::Refused => Verdict::Refused,
            Self::NotAsked => Verdict::NotAsked,
        }
    }
}

/// The verdicts that the package's run of the user's policy gave.
#[napi(object, object_to_js = false)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccessGrantArg {
    pub view: AccessVerdict,
    pub act: AccessVerdict,
}

/// One request, as the package read it, with the verdicts of the policy.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitoringRequestArg {
    /// An HTTP method token.
    pub method: String,
    /// The path inside the mount, from its leading `/`, percent-encoded.
    pub path: String,
    /// The query text after `?`; absent when the URL has none.
    pub raw_query: Option<String>,
    /// The value of `X-Horsies-Intent`; absent when the request has none.
    pub intent_header: Option<String>,
    /// The value of `Content-Type`; absent when the request has none.
    pub content_type: Option<String>,
    /// The body as lower-case hexadecimal text: at most
    /// `maxRequestBodyBytes() + 1` bytes. The package stops its read past
    /// the bound.
    pub body_hex: String,
    /// The mount path of the handler: `/` at the root.
    pub base_path: String,
    pub grant: AccessGrantArg,
}

/// One header of a response.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitoringHeaderRecord {
    /// The name, in lower case.
    pub name: String,
    pub value: String,
}

/// One response for the package to send.
#[napi(object, object_from_js = false)]
pub struct MonitoringResponseRecord {
    pub status: u32,
    /// The headers, in order.
    pub headers: Vec<MonitoringHeaderRecord>,
    pub body: Uint8Array,
}

/// The facts of one response before the body crosses as a typed array.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ResponseFacts {
    status: u16,
    headers: Vec<MonitoringHeaderRecord>,
    body: Vec<u8>,
}

impl ResponseFacts {
    /// The facts of a core response. A header value that is not UTF-8 is
    /// read with the replacement character: the core writes ASCII. Pure.
    fn of(parts: ResponseParts) -> Self {
        Self {
            status: parts.status.as_u16(),
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| MonitoringHeaderRecord {
                    name: name.as_str().to_owned(),
                    value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
                })
                .collect(),
            body: Vec::from(parts.body),
        }
    }

    /// The answer of a handler whose broker could not be built, with the
    /// status of the error's retry rule: 503 for a cause that may clear,
    /// 500 for a connection setting that the core refuses. Pure.
    fn broker_failed(error: &BrokerError) -> Self {
        let body = serde_json::json!({
            "detail": format!("The monitoring handler has no broker: {error}"),
        });
        Self {
            status: database_failure_status(Transience::of_retryable(error.is_retryable())),
            headers: vec![MonitoringHeaderRecord {
                name: "content-type".to_owned(),
                value: "application/json".to_owned(),
            }],
            body: body.to_string().into_bytes(),
        }
    }

    /// The record that crosses to JavaScript.
    fn into_record(self) -> MonitoringResponseRecord {
        MonitoringResponseRecord {
            status: u32::from(self.status),
            headers: self.headers,
            body: Uint8Array::new(self.body),
        }
    }
}

/// One step of an event stream.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MonitoringEventStep {
    /// One frame text, with the blank line that ends it.
    Frame { text: String },
    /// The stream ended: after the degraded frame, after `close`, or after
    /// the handler closed its events. Every later call answers it again.
    Ended,
}

/// The settings of a handler.
#[napi(object, object_to_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct MonitoringHandlerSettings {
    pub actions: ActionsSwitch,
    /// The stylesheet that the page loads after its own; absent for none.
    pub custom_css_url: Option<String>,
    /// The trace link template of the task detail, which the package
    /// checked with `checkTraceLinkTemplate`; absent for the trace id as
    /// text.
    pub trace_link_template: Option<String>,
    /// The version that `/api/meta` reports: the package version.
    pub client_version: String,
    /// How long an event stream waits without an event before its
    /// heartbeat frame, in whole milliseconds from 1 to 3,600,000.
    pub heartbeat_ms: f64,
    /// The queue names of the app config: the `queue` label values of
    /// `/metrics`.
    pub queue_names: Vec<String>,
    /// The schedule names of the app config: the `schedule` label values of
    /// `/metrics`.
    pub schedule_names: Vec<String>,
}

/// The answer of `checkTraceLinkTemplate`.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum TraceLinkCheck {
    Accepted,
    /// `refusal` is the rule that the template breaks; `rule` is the core's
    /// text of that rule.
    Refused {
        refusal: TraceLinkRefusalRecord,
        rule: String,
    },
}

/// The rule of the core that a trace link template breaks, with its facts:
/// one variant per variant of the core's [`TraceLinkRefusal`].
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum TraceLinkRefusalRecord {
    /// The template has 0 bytes or more than the core's bound: `bytes`, its
    /// UTF-8 byte count.
    Length { bytes: f64 },
    /// The template holds a byte outside 0x21 to 0x7e: `byte`, the first one.
    Character { byte: f64 },
    /// The template does not start with `https://` or `http://`.
    Scheme,
    /// The template has no host after its scheme.
    NoHost,
    /// The template does not hold `{trace_id}` once: `count`, the times that
    /// it holds it.
    PlaceholderCount { count: f64 },
    /// The template holds a `{` or `}` outside `{trace_id}`.
    StrayBrace,
}

impl TraceLinkRefusalRecord {
    /// The record of the core's refusal. Pure.
    fn of(refusal: TraceLinkRefusal) -> Self {
        match refusal {
            TraceLinkRefusal::Length { bytes } => Self::Length {
                bytes: number_of_text_count(bytes),
            },
            TraceLinkRefusal::Character { byte } => Self::Character {
                byte: f64::from(byte),
            },
            TraceLinkRefusal::Scheme => Self::Scheme,
            TraceLinkRefusal::NoHost => Self::NoHost,
            TraceLinkRefusal::PlaceholderCount { count } => Self::PlaceholderCount {
                count: number_of_text_count(count),
            },
            TraceLinkRefusal::StrayBrace => Self::StrayBrace,
        }
    }
}

/// A count in the UTF-8 form of a text from JavaScript, as a JavaScript
/// number. A JavaScript string has at most 2^31 - 1 UTF-16 code units
/// (JavaScriptCore; V8 allows fewer), each at most 3 UTF-8 bytes, so the
/// count is under 2^53 and the number holds it exactly. A count that the
/// number does not hold is a defect. Pure.
fn number_of_text_count(count: usize) -> f64 {
    let number = count as f64;
    // A number that converts back to the same count holds it exactly.
    match number as usize == count {
        true => number,
        false => {
            unreachable!("a count in the UTF-8 form of a JavaScript string is under 2^53: {count}")
        }
    }
}

/// The handler closed its events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EventsClosed;

/// What a handler is built from, and its build, made once.
struct HandlerState {
    connection: BrokerConnection,
    children: Arc<dyn ChildSpecSource>,
    config: MonitoringHandlerConfig,
    /// The queue and schedule names that the labels of `/metrics` do not
    /// declare; the build logs them.
    folded: FoldedNames,
    built: OnceCell<Arc<CoreHandler>>,
    events_closed: OnceLock<EventsClosed>,
}

impl HandlerState {
    /// The core handler. The first call connects the handle's broker and
    /// builds the handler; a failure is returned and not kept. The build
    /// logs one WARNING line that names each name that `/metrics` does not
    /// declare.
    async fn built(&self) -> Result<Arc<CoreHandler>, BrokerError> {
        self.built
            .get_or_try_init(|| async {
                let broker = self.connection.broker().await?;
                warn_folded_names(&"the fleet metrics of /metrics", &self.folded);
                Ok::<_, BrokerError>(Arc::new(CoreHandler::new(
                    broker,
                    Arc::clone(&self.children),
                    self.config.clone(),
                )))
            })
            .await
            .map(Arc::clone)
    }
}

/// The monitoring handler of one `CoreHandle`.
#[napi]
pub struct MonitoringHandler {
    actions: ActionsSwitch,
    state: Arc<HandlerState>,
}

/// Build a handler over the broker of `handle`. Starts nothing. A setting
/// that the package should have refused, or a handle without a child spec
/// source, throws `CLIENT_DEFECT` with the rule.
#[napi(ts_return_type = "NativeEnvelope<MonitoringHandler>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn build_monitoring_handler(
    handle: &CoreHandle,
    settings: MonitoringHandlerSettings,
) -> Envelope<MonitoringHandler> {
    sync_call(BUILD_SUBJECT, || {
        match handler_of(handle.connection(), handle.child_spec_source(), settings) {
            Ok(handler) => Envelope::Value(handler),
            Err(refusal) => Envelope::Thrown(refusal.into()),
        }
    })
}

/// The handler of the checked settings. Pure.
///
/// Failure exits: the refusal of a heartbeat out of bounds, of a template
/// that the core refuses, or of a missing child spec source.
fn handler_of(
    connection: BrokerConnection,
    children: Option<Arc<dyn ChildSpecSource>>,
    settings: MonitoringHandlerSettings,
) -> Result<MonitoringHandler, BoundaryRefusal> {
    let heartbeat = heartbeat_of(settings.heartbeat_ms)?;
    let trace_link = trace_link_of(settings.trace_link_template.as_deref())?;
    let children =
        children.ok_or_else(|| BoundaryRefusal::new("handle", CHILD_SPEC_SOURCE_RULE))?;
    let Folded {
        declared: fleet_labels,
        folded,
    } = fleet_labels(settings.queue_names, settings.schedule_names);
    Ok(MonitoringHandler {
        actions: settings.actions,
        state: Arc::new(HandlerState {
            connection,
            children,
            config: MonitoringHandlerConfig {
                actions: settings.actions.core_actions(),
                ui: MonitoringUiConfig {
                    custom_css_url: settings.custom_css_url,
                    trace_link,
                },
                client_version: settings.client_version,
                heartbeat,
                fleet_labels,
            },
            folded,
            built: OnceCell::new(),
            events_closed: OnceLock::new(),
        }),
    })
}

/// The heartbeat of `heartbeat_ms`. Pure.
///
/// Failure exits: the refusal of a value that is not a whole number from 1
/// to [`MAX_HEARTBEAT_MS`].
fn heartbeat_of(heartbeat_ms: f64) -> Result<Duration, BoundaryRefusal> {
    let refused = || {
        BoundaryRefusal::new(
            "heartbeatMs",
            "a whole number of milliseconds from 1 to 3600000",
        )
    };
    match u32_of_number("heartbeatMs", heartbeat_ms) {
        Ok(0) | Err(_) => Err(refused()),
        Ok(over) if over > MAX_HEARTBEAT_MS => Err(refused()),
        Ok(valid) => Ok(Duration::from_millis(u64::from(valid))),
    }
}

/// The trace link of a template text: `Text` when absent. Pure.
///
/// Failure exits: the refusal of a template that the core refuses (the
/// package checks it first).
fn trace_link_of(template: Option<&str>) -> Result<TraceLink, BoundaryRefusal> {
    match template {
        None => Ok(TraceLink::Text),
        Some(text) => TraceLinkTemplate::parse(text)
            .map(TraceLink::Template)
            .map_err(|refused| BoundaryRefusal::new("traceLinkTemplate", &refused.to_string())),
    }
}

/// The labels of the declared names, and the names that they do not
/// declare. Never refuses: a name that cannot be a label value of its own
/// (empty, `(unknown)`, or past the bound of names) is not declared. Pure.
pub fn fleet_labels(queue_names: Vec<String>, schedule_names: Vec<String>) -> Folded<FleetLabels> {
    let queues = DeclaredNames::<QueueName>::new(queue_names);
    let schedules = DeclaredNames::<ScheduleName>::new(schedule_names);
    Folded {
        declared: FleetLabels {
            queues: queues.declared,
            schedules: schedules.declared,
        },
        folded: queues.folded.and(schedules.folded),
    }
}

/// The HTTP method of its text. Pure.
///
/// Failure exits: the refusal of text that is not an HTTP token.
fn method_of(text: &str) -> Result<Method, BoundaryRefusal> {
    Method::from_bytes(text.as_bytes()).map_err(|_not_a_token| {
        BoundaryRefusal::new("method", "an HTTP method token, for example GET")
    })
}

/// The core's request parts and grant of a request argument. Pure.
///
/// Failure exits: the refusal of a method that is not a token, or of a body
/// that is not lower-case hexadecimal text.
fn request_of(
    request: MonitoringRequestArg,
) -> Result<(RequestParts, AccessGrant), BoundaryRefusal> {
    let method = method_of(&request.method)?;
    let body = bytes_of_hex("bodyHex", &request.body_hex)?;
    let grant = AccessGrant {
        view: request.grant.view.verdict(),
        act: request.grant.act.verdict(),
    };
    Ok((
        RequestParts {
            method,
            path: request.path,
            raw_query: request.raw_query,
            intent_header: request.intent_header,
            content_type: request.content_type,
            body: Bytes::from(body),
            base_path: request.base_path,
        },
        grant,
    ))
}

/// A `POST` (an action) writes; every other request reads. Pure.
fn call_kind(method: &Method) -> CallKind {
    match *method == Method::POST {
        true => CallKind::Write,
        false => CallKind::Read,
    }
}

/// A Promise that resolves at once with `thrown`.
///
/// Failure exits: the `napi::Error` of a deferred that Node-API cannot
/// create.
pub fn thrown_promise<'env, T>(env: &'env Env, thrown: NativeThrown) -> napi::Result<Object<'env>>
where
    T: ToNapiValue + Send + 'static,
{
    let (deferred, promise) = env.create_deferred::<Envelope<T>, EnvelopeResolver<T>>()?;
    resolve(deferred, Envelope::Thrown(thrown));
    Ok(promise)
}

#[napi]
impl MonitoringHandler {
    /// The most body bytes that `handle` takes; the package reads at most
    /// one byte more and passes what it read.
    #[napi(ts_return_type = "NativeEnvelope<number>")]
    pub fn max_request_body_bytes(&self) -> Envelope<u32> {
        sync_call("MonitoringHandler.maxRequestBodyBytes", || {
            Envelope::Value(MAX_REQUEST_BODY_BYTES_NUMBER)
        })
    }

    /// The verdicts that a request needs and the form of its answer, from
    /// its method and its path inside the mount. Pure. A method that is not
    /// an HTTP token throws `CLIENT_DEFECT`.
    #[napi(ts_return_type = "NativeEnvelope<RouteAccessRecord>")]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "napi-rs passes the arguments of a JavaScript call by value"
    )]
    pub fn route_access(&self, method: String, path: String) -> Envelope<RouteAccessRecord> {
        sync_call("MonitoringHandler.routeAccess", || {
            match method_of(&method) {
                Ok(method) => Envelope::Value(RouteAccessRecord::of(core_route_access(
                    self.actions.core_actions(),
                    &method,
                    &path,
                ))),
                Err(refusal) => Envelope::Thrown(refusal.into()),
            }
        })
    }

    /// Serve one request with its verdicts: the response. Every expected
    /// outcome is a status and a body. A `POST` (an action) writes and runs
    /// to its end; every other request reads. An argument that the package
    /// should have refused throws `CLIENT_DEFECT`; a grant that does not
    /// answer the need of `routeAccess`, or an allowed view of the event
    /// stream, throws `CORE_PANIC` (a defect of the package).
    #[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringResponseRecord>>")]
    pub fn handle<'env>(
        &self,
        env: &'env Env,
        request: MonitoringRequestArg,
    ) -> napi::Result<Object<'env>> {
        const SUBJECT: &str = "MonitoringHandler.handle";
        let parsed = guarded_start(SUBJECT, || request_of(request).map_err(NativeThrown::from));
        let (parts, grant) = match parsed {
            Ok(parsed) => parsed,
            Err(thrown) => return thrown_promise::<MonitoringResponseRecord>(env, thrown),
        };
        let kind = call_kind(&parts.method);
        let state = Arc::clone(&self.state);
        spawn_operation(env, SUBJECT, kind, move |_core| {
            handle_operation(state, parts, grant)
        })
    }

    /// Subscribe one event stream, after the user's policy allowed the view.
    /// A broker without a listener, a stopped listener, a handler whose
    /// broker cannot be built (logged as one WARNING record), or a handler
    /// that closed its events gives a stream whose first frame is the
    /// degraded frame. Reads only.
    #[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringEventStream>>")]
    pub fn subscribe_events<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let state = Arc::clone(&self.state);
        spawn_operation(
            env,
            "MonitoringHandler.subscribeEvents",
            CallKind::Read,
            move |_core| subscribe_operation(state),
        )
    }

    /// End every event stream of the handler after the degraded frame; a
    /// stream subscribed later answers the degraded frame, then the end.
    /// Idempotent. The read routes and the actions still answer.
    #[napi(ts_return_type = "Promise<NativeEnvelope<undefined>>")]
    pub fn close_events<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let state = Arc::clone(&self.state);
        spawn_operation(
            env,
            "MonitoringHandler.closeEvents",
            CallKind::Write,
            move |_core| {
                let _first_or_repeated_close = state.events_closed.set(EventsClosed);
                close_events_operation(state)
            },
        )
    }
}

/// `handle`: the core handler's response, or the answer of a broker that
/// could not be built.
async fn handle_operation(
    state: Arc<HandlerState>,
    request: RequestParts,
    grant: AccessGrant,
) -> MonitoringResponseRecord {
    let facts = match state.built().await {
        Ok(handler) => ResponseFacts::of(handler.handle(request, grant).await),
        Err(error) => ResponseFacts::broker_failed(&error),
    };
    facts.into_record()
}

/// `subscribeEvents`: a stream of the core's frames, or a stream of the
/// degraded frame.
async fn subscribe_operation(state: Arc<HandlerState>) -> MonitoringEventStream {
    if let Some(EventsClosed) = state.events_closed.get() {
        return MonitoringEventStream::of_slot(FrameSlot::DegradedFrameNext);
    }
    let frames = match state.built().await {
        Ok(handler) => handler.subscribe_events().await,
        Err(error) => {
            tracing::warn!(
                target: "horsies.web",
                "the event stream has no broker and answers the degraded frame: {error}"
            );
            return MonitoringEventStream::of_slot(FrameSlot::DegradedFrameNext);
        }
    };
    // A close that ran while this subscription started may have missed it.
    match state.events_closed.get() {
        Some(EventsClosed) => MonitoringEventStream::of_slot(FrameSlot::DegradedFrameNext),
        None => MonitoringEventStream::of_slot(FrameSlot::Subscribed(Box::new(frames))),
    }
}

/// `closeEvents`: close the broadcaster of a built handler. A handler that
/// was never built has no stream.
async fn close_events_operation(state: Arc<HandlerState>) {
    if let Some(handler) = state.built.get() {
        handler.events().close().await;
    }
}

/// Where the next frame of one stream comes from.
enum FrameSlot {
    /// The subscription of the core's broadcaster.
    Subscribed(Box<EventFrames>),
    /// The stream answers the degraded frame, then the end.
    DegradedFrameNext,
    /// The stream ended; its subscription is dropped.
    Ended,
}

impl FrameSlot {
    /// The next step of the slot. The slot ends, and drops its
    /// subscription, at the end of the stream.
    async fn next_step(&mut self) -> MonitoringEventStep {
        let item = match self {
            Self::Ended => return MonitoringEventStep::Ended,
            Self::DegradedFrameNext => {
                *self = Self::Ended;
                return MonitoringEventStep::Frame {
                    text: DEGRADED_FRAME.to_owned(),
                };
            }
            Self::Subscribed(frames) => frames.next().await,
        };
        match item {
            EventStreamItem::Frame(text) => MonitoringEventStep::Frame { text },
            EventStreamItem::Ended => {
                *self = Self::Ended;
                MonitoringEventStep::Ended
            }
        }
    }
}

/// Whether a stream was closed by its owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamState {
    Open,
    Closed,
}

/// The frames of one stream and its close signal.
struct StreamSource {
    slot: Mutex<FrameSlot>,
    state: watch::Sender<StreamState>,
}

/// One web client's event stream. `nextFrame()` waits for the next frame;
/// `close()` ends the stream.
#[napi]
pub struct MonitoringEventStream {
    source: Arc<StreamSource>,
}

impl MonitoringEventStream {
    fn of_slot(slot: FrameSlot) -> Self {
        let (state, _initial) = watch::channel(StreamState::Open);
        Self {
            source: Arc::new(StreamSource {
                slot: Mutex::new(slot),
                state,
            }),
        }
    }
}

/// The next step of `source`, or the end once its state is `Closed`. A
/// close drops the subscription after the wait it ended.
async fn next_step(source: Arc<StreamSource>) -> MonitoringEventStep {
    let mut state = source.state.subscribe();
    let step = tokio::select! {
        biased;
        _closed = state.wait_for(|current| *current == StreamState::Closed) => MonitoringEventStep::Ended,
        step = async { source.slot.lock().await.next_step().await } => step,
    };
    let closed = *source.state.borrow() == StreamState::Closed;
    if closed {
        *source.slot.lock().await = FrameSlot::Ended;
    }
    step
}

#[napi]
impl MonitoringEventStream {
    /// The next frame, or the end. Reads only.
    #[napi(ts_return_type = "Promise<NativeEnvelope<MonitoringEventStep>>")]
    pub fn next_frame<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let source = Arc::clone(&self.source);
        spawn_operation(
            env,
            "MonitoringEventStream.nextFrame",
            CallKind::Read,
            move |_core| next_step(source),
        )
    }

    /// End the stream: a pending and every later `nextFrame()` answers the
    /// end, and the subscription is dropped. Idempotent; returns at once.
    #[napi(ts_return_type = "NativeEnvelope<undefined>")]
    pub fn close(&self) -> Envelope<()> {
        sync_call("MonitoringEventStream.close", || {
            let _previous = self.source.state.send_replace(StreamState::Closed);
            // A pending `nextFrame()` holds the lock; it drops the
            // subscription itself when its wait ends.
            if let Ok(mut slot) = self.source.slot.try_lock() {
                *slot = FrameSlot::Ended;
            }
            Envelope::Value(())
        })
    }
}

/// Check the trace link template `text` with the core's rules: its length,
/// its characters, its scheme, a host, one `{trace_id}`, and no other
/// brace. A refusal names the rule with its facts and the core's text of
/// the rule. Pure: starts nothing.
#[napi(ts_return_type = "NativeEnvelope<TraceLinkCheck>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn check_trace_link_template(text: String) -> Envelope<TraceLinkCheck> {
    sync_call("checkTraceLinkTemplate", || {
        Envelope::Value(match TraceLinkTemplate::parse(&text) {
            Ok(_template) => TraceLinkCheck::Accepted,
            Err(refused) => TraceLinkCheck::Refused {
                refusal: TraceLinkRefusalRecord::of(refused),
                rule: refused.to_string(),
            },
        })
    })
}

/// Write `message` as one ERROR record of the `horsies.web` logger, if its
/// threshold lets it through. Starts the log bridge, not the core runtime.
/// A log bridge that cannot start throws `RUNTIME_START_FAILED`.
#[napi(ts_return_type = "NativeEnvelope<undefined>")]
#[allow(
    clippy::needless_pass_by_value,
    reason = "napi-rs passes the arguments of a JavaScript call by value"
)]
pub fn log_monitoring_failure(message: String) -> Envelope<()> {
    sync_call("logMonitoringFailure", || match started_log() {
        Ok(log) => {
            tracing::dispatcher::with_default(log.dispatch(), || {
                tracing::error!(target: "horsies.web", "{message}");
            });
            Envelope::Value(())
        }
        Err(failure) => Envelope::Thrown(NativeThrown::RuntimeStartFailed {
            cause: failure.cause,
        }),
    })
}

/// A complete producer manifest of the tests, on a refused URL.
#[cfg(test)]
pub(crate) const TEST_MANIFEST: &str = r#"{
    "manifest_version": 1,
    "database": {
        "database_url": "postgresql://user:secret@127.0.0.1:1/none",
        "session_database": {"kind": "SAME_AS_DATABASE_URL"},
        "pgbouncer_transaction_mode": false,
        "pool_pre_ping": true,
        "pool_size": 5,
        "max_overflow": 0,
        "pool_timeout_seconds": 30,
        "pool_recycle": {"kind": "AFTER_SECONDS", "seconds": 1800},
        "echo": false,
        "tcp_keepalives": {
            "kind": "ON",
            "idle_seconds": 30,
            "interval_seconds": 10,
            "probes": 3
        },
        "tcp_user_timeout": {"kind": "AFTER", "milliseconds": 30000}
    },
    "schema": "MIGRATE_AND_VALIDATE",
    "send": {
        "retain_rerun_input_default": false,
        "idempotency_reservation_window": 86400000000
    }
}"#;

#[cfg(test)]
mod tests {
    use horsies_core::web::{HeaderName, HeaderValue, StatusCode};

    use super::*;

    fn settings() -> MonitoringHandlerSettings {
        MonitoringHandlerSettings {
            actions: ActionsSwitch::Enabled,
            custom_css_url: None,
            trace_link_template: None,
            client_version: "0.1.0".to_owned(),
            heartbeat_ms: 15_000.0,
            queue_names: vec!["default".to_owned()],
            schedule_names: vec![],
        }
    }

    fn request(method: &str, body_hex: &str) -> MonitoringRequestArg {
        MonitoringRequestArg {
            method: method.to_owned(),
            path: "/api/tasks/01890a5d-ac96-774b-bcce-b302099a8057/cancel".to_owned(),
            raw_query: None,
            intent_header: Some("action".to_owned()),
            content_type: Some("application/json".to_owned()),
            body_hex: body_hex.to_owned(),
            base_path: "/".to_owned(),
            grant: AccessGrantArg {
                view: AccessVerdict::Allowed,
                act: AccessVerdict::Refused,
            },
        }
    }

    #[test]
    fn the_body_bound_crosses_as_the_core_bound() {
        assert_eq!(
            usize::try_from(MAX_REQUEST_BODY_BYTES_NUMBER),
            Ok(MAX_REQUEST_BODY_BYTES)
        );
    }

    #[test]
    fn route_access_names_need_and_form() {
        let events = RouteAccessRecord::of(core_route_access(
            Actions::Enabled,
            &Method::GET,
            "/api/events",
        ));
        assert_eq!(events.need, RouteNeed::View);
        assert_eq!(events.form, ResponseFormName::EventStream);
        let meta = RouteAccessRecord::of(core_route_access(
            Actions::Enabled,
            &Method::GET,
            "/api/meta",
        ));
        assert_eq!(meta.need, RouteNeed::ViewReportAct);
        assert_eq!(meta.form, ResponseFormName::OneResponse);
        let page = RouteAccessRecord::of(core_route_access(Actions::Enabled, &Method::GET, "/"));
        assert_eq!(page.need, RouteNeed::Public);
    }

    #[test]
    fn an_action_needs_view_and_act_only_while_actions_are_enabled() {
        let path = "/api/workflows/01890a5d-ac96-774b-bcce-b302099a8057/pause";
        let enabled = core_route_access(ActionsSwitch::Enabled.core_actions(), &Method::POST, path);
        let disabled =
            core_route_access(ActionsSwitch::Disabled.core_actions(), &Method::POST, path);
        assert_eq!(RouteAccessRecord::of(enabled).need, RouteNeed::ViewAndAct);
        assert_eq!(RouteAccessRecord::of(disabled).need, RouteNeed::View);
    }

    #[test]
    fn each_verdict_name_is_its_core_verdict() {
        assert_eq!(AccessVerdict::Allowed.verdict(), Verdict::Allowed);
        assert_eq!(AccessVerdict::Refused.verdict(), Verdict::Refused);
        assert_eq!(AccessVerdict::NotAsked.verdict(), Verdict::NotAsked);
    }

    #[test]
    fn a_request_argument_gives_its_parts_and_grant() {
        let (parts, grant) = request_of(request("POST", "7b7d")).expect("a valid argument");
        assert_eq!(parts.method, Method::POST);
        assert_eq!(parts.body, Bytes::from_static(b"{}"));
        assert_eq!(parts.intent_header.as_deref(), Some("action"));
        assert_eq!(grant.view, Verdict::Allowed);
        assert_eq!(grant.act, Verdict::Refused);
        assert_eq!(call_kind(&parts.method), CallKind::Write);
        assert_eq!(call_kind(&Method::GET), CallKind::Read);
    }

    #[test]
    fn a_method_that_is_not_a_token_is_refused() {
        let refused = request_of(request("GE T", "")).expect_err("not a token");
        assert_eq!(refused.subject, "method");
    }

    #[test]
    fn a_body_that_is_not_hexadecimal_text_is_refused() {
        let refused = request_of(request("POST", "7B7D")).expect_err("upper-case digits");
        assert_eq!(refused.subject, "bodyHex");
    }

    #[test]
    fn the_heartbeat_is_bounded() {
        assert!(heartbeat_of(0.0).is_err());
        assert!(heartbeat_of(1.5).is_err());
        assert_eq!(heartbeat_of(15_000.0), Ok(Duration::from_millis(15_000)));
        assert_eq!(
            heartbeat_of(f64::from(MAX_HEARTBEAT_MS)),
            Ok(Duration::from_millis(u64::from(MAX_HEARTBEAT_MS)))
        );
        assert!(heartbeat_of(f64::from(MAX_HEARTBEAT_MS) + 1.0).is_err());
    }

    #[test]
    fn a_trace_link_template_is_checked_by_the_core() {
        assert_eq!(trace_link_of(None), Ok(TraceLink::Text));
        let template = "https://grafana.example.com/explore?traceId={trace_id}";
        assert!(matches!(
            trace_link_of(Some(template)),
            Ok(TraceLink::Template(parsed)) if parsed.as_str() == template
        ));
        let refused = trace_link_of(Some("ftp://example.com/{trace_id}")).expect_err("no scheme");
        assert_eq!(refused.subject, "traceLinkTemplate");
    }

    /// The answer of `checkTraceLinkTemplate` for a text that the core
    /// refuses with `core`: the record of `core` and its text.
    fn refused_with(
        core: TraceLinkRefusal,
        refusal: TraceLinkRefusalRecord,
    ) -> Envelope<TraceLinkCheck> {
        Envelope::Value(TraceLinkCheck::Refused {
            refusal,
            rule: core.to_string(),
        })
    }

    #[test]
    fn check_trace_link_template_accepts_a_valid_template() {
        assert_eq!(
            check_trace_link_template("http://jaeger:16686/trace/{trace_id}".to_owned()),
            Envelope::Value(TraceLinkCheck::Accepted)
        );
    }

    #[test]
    fn a_length_refusal_crosses_with_its_byte_count() {
        assert_eq!(
            check_trace_link_template(String::new()),
            refused_with(
                TraceLinkRefusal::Length { bytes: 0 },
                TraceLinkRefusalRecord::Length { bytes: 0.0 }
            )
        );
    }

    #[test]
    fn a_character_refusal_crosses_with_its_byte() {
        assert_eq!(
            check_trace_link_template("https://example.com/a b/{trace_id}".to_owned()),
            refused_with(
                TraceLinkRefusal::Character { byte: b' ' },
                TraceLinkRefusalRecord::Character { byte: 32.0 }
            )
        );
    }

    #[test]
    fn a_scheme_refusal_crosses_as_scheme() {
        assert_eq!(
            check_trace_link_template("ftp://example.com/{trace_id}".to_owned()),
            refused_with(TraceLinkRefusal::Scheme, TraceLinkRefusalRecord::Scheme)
        );
    }

    #[test]
    fn a_no_host_refusal_crosses_as_no_host() {
        assert_eq!(
            check_trace_link_template("https:///trace/{trace_id}".to_owned()),
            refused_with(TraceLinkRefusal::NoHost, TraceLinkRefusalRecord::NoHost)
        );
    }

    #[test]
    fn a_placeholder_count_refusal_crosses_with_its_count() {
        assert_eq!(
            check_trace_link_template("https://example.com/{trace_id}/{trace_id}".to_owned()),
            refused_with(
                TraceLinkRefusal::PlaceholderCount { count: 2 },
                TraceLinkRefusalRecord::PlaceholderCount { count: 2.0 }
            )
        );
    }

    #[test]
    fn a_stray_brace_refusal_crosses_as_stray_brace() {
        assert_eq!(
            check_trace_link_template("https://example.com/{span_id}/{trace_id}".to_owned()),
            refused_with(
                TraceLinkRefusal::StrayBrace,
                TraceLinkRefusalRecord::StrayBrace
            )
        );
    }

    #[test]
    fn fleet_labels_fold_the_names_that_cannot_be_labels() {
        let labels = fleet_labels(
            vec!["default".to_owned(), String::new()],
            vec!["(unknown)".to_owned(), "nightly".to_owned()],
        );
        assert_eq!(labels.declared.queues.names(), ["default".to_owned()]);
        assert_eq!(labels.declared.schedules.names(), ["nightly".to_owned()]);
        assert_eq!(labels.folded.names().len(), 2);
    }

    #[test]
    fn a_core_response_keeps_status_headers_and_body() {
        let facts = ResponseFacts::of(ResponseParts {
            status: StatusCode::FORBIDDEN,
            headers: vec![(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            )],
            body: Bytes::from_static(b"{\"detail\":\"Not authorized.\"}"),
        });
        assert_eq!(facts.status, 403);
        assert_eq!(
            facts.headers,
            vec![MonitoringHeaderRecord {
                name: "content-type".to_owned(),
                value: "application/json".to_owned(),
            }]
        );
        assert_eq!(facts.body, b"{\"detail\":\"Not authorized.\"}".to_vec());
    }

    #[test]
    fn a_broker_failure_that_may_clear_is_a_json_503() {
        let facts =
            ResponseFacts::broker_failed(&BrokerError::ConnectionFailed("refused URL".to_owned()));
        assert_eq!(facts.status, 503);
        let body: serde_json::Value = serde_json::from_slice(&facts.body).expect("a JSON body");
        assert!(body["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("refused URL")));
    }

    #[test]
    fn a_handle_without_a_child_spec_source_is_refused() {
        let manifest = horsies_core::manifest::parse_producer_manifest(TEST_MANIFEST)
            .expect("a complete manifest");
        let connection = BrokerConnection::new(manifest, horsies_core::PoolProfile::Producer);
        let refused = handler_of(connection, None, settings()).err();
        assert_eq!(
            refused,
            Some(BoundaryRefusal::new("handle", CHILD_SPEC_SOURCE_RULE))
        );
    }

    #[test]
    fn the_degraded_slot_answers_one_frame_then_the_end() {
        let core = crate::process_core::runtime::standalone_core_for_tests();
        let source = Arc::new(StreamSource {
            slot: Mutex::new(FrameSlot::DegradedFrameNext),
            state: watch::channel(StreamState::Open).0,
        });
        let first = core
            .runtime_handle()
            .block_on(next_step(Arc::clone(&source)));
        assert_eq!(
            first,
            MonitoringEventStep::Frame {
                text: DEGRADED_FRAME.to_owned()
            }
        );
        let second = core
            .runtime_handle()
            .block_on(next_step(Arc::clone(&source)));
        assert_eq!(second, MonitoringEventStep::Ended);
    }

    #[test]
    fn a_closed_stream_answers_the_end() {
        let core = crate::process_core::runtime::standalone_core_for_tests();
        let stream = MonitoringEventStream::of_slot(FrameSlot::DegradedFrameNext);
        assert_eq!(stream.close(), Envelope::Value(()));
        let step = core
            .runtime_handle()
            .block_on(next_step(Arc::clone(&stream.source)));
        assert_eq!(step, MonitoringEventStep::Ended);
    }
}
