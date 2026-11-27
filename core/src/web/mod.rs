//! The framework-neutral monitoring handler that a host framework mounts
//! ([`MonitoringHandler`]): the routes of the monitoring API, their access
//! guards, the event frames and the embedded UI. The host runs the user's
//! access policy and passes its verdicts with each request.

mod app;
mod auth;
mod common;
mod events;
mod fleet_metrics;
mod handler;
mod request_body;
mod routes;
mod schema;
mod spa;
mod stdlib_uuid_text;
mod task_stats_cache;
mod trace_link;
mod uuid_text;

pub use auth::{INTENT_HEADER, INTENT_VALUE};
/// The body type of the handler's request and response parts, from the
/// `bytes` crate: no web framework's own type.
pub use bytes::Bytes;
pub use events::{
    EventBroadcaster, EventCoalescer, EventFrames, EventStreamItem, EventStreamsRefused,
    TopicEvent, CHANNEL_TOPICS, DEBOUNCE, DEGRADED_FRAME, HEARTBEAT, HEARTBEAT_FRAME,
    MAX_IDS_PER_EVENT, MAX_OPEN_EVENT_STREAMS, TOPIC_DEGRADED,
};
pub use handler::{
    route_access, AccessGrant, AccessNeed, Actions, MonitoringHandler, MonitoringHandlerConfig,
    RequestParts, ResponseForm, ResponseParts, RouteAccess, Verdict, MAX_REQUEST_BODY_BYTES,
    MAX_RESPONSE_BYTES,
};
/// The HTTP types of the handler's request and response parts, from the
/// `http` crate: no web framework's own type.
pub use http::{HeaderName, HeaderValue, Method, StatusCode};
pub use schema::{
    SchemaIncompatible, SchemaProbe, SchemaState, SchemaStatus, SCHEMA_INCOMPATIBLE, SCHEMA_UNKNOWN,
};
pub use spa::{MonitoringUiConfig, ASSETS_MISSING_DETAIL};
pub use trace_link::{
    TraceLink, TraceLinkRefusal, TraceLinkTemplate, MAX_TRACE_LINK_TEMPLATE_BYTES,
    TRACE_ID_PLACEHOLDER,
};

#[cfg(test)]
mod tests;
