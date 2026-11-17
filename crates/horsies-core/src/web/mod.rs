//! The framework-neutral monitoring handler that a host framework mounts
//! ([`MonitoringHandler`]): the routes of the monitoring API, their access
//! guards, the event frames and the embedded UI. The host runs the user's
//! access policy and passes its verdicts with each request.

mod app;
mod auth;
mod common;
mod events;
mod handler;
mod routes;
mod schema;
mod spa;
mod stdlib_uuid_text;
mod task_stats_cache;
mod uuid_text;

pub use crate::monitoring::ResumeChildren;
pub use auth::{INTENT_HEADER, INTENT_VALUE};
/// The HTTP types of the handler's request and response parts.
pub use axum::body::Bytes;
pub use axum::http::{HeaderName, HeaderValue, Method, StatusCode};
pub use events::{
    EventBroadcaster, EventCoalescer, EventFrames, EventStreamItem, TopicEvent, CHANNEL_TOPICS,
    DEBOUNCE, DEGRADED_FRAME, HEARTBEAT, HEARTBEAT_FRAME, MAX_IDS_PER_EVENT, TOPIC_DEGRADED,
};
pub use handler::{
    route_access, AccessGrant, AccessNeed, MonitoringHandler, MonitoringHandlerConfig,
    RequestParts, ResponseForm, ResponseParts, RouteAccess, Verdict, MAX_REQUEST_BODY_BYTES,
    MAX_RESPONSE_BYTES,
};
pub use schema::{
    SchemaIncompatible, SchemaProbe, SchemaState, SchemaStatus, SCHEMA_INCOMPATIBLE, SCHEMA_UNKNOWN,
};
pub use spa::{MonitoringUiConfig, ASSETS_MISSING_DETAIL};

#[cfg(test)]
mod tests;
