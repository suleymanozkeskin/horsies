//! Optional axum transport for the monitoring API, and the
//! framework-neutral handler that a host framework mounts over the same
//! routes ([`MonitoringHandler`]).

mod app;
mod auth;
mod common;
mod events;
mod handler;
mod routes;
mod schema;
mod spa;
mod task_stats_cache;

pub use crate::monitoring::ResumeChildren;
pub use app::{create_monitoring_router, MetaResponse};
pub use auth::{
    AllowAll, MonitoringAuthPolicy, MonitoringRequest, TrustedHeader, ViewOnly, INTENT_HEADER,
    INTENT_VALUE,
};
pub use events::{
    EventBroadcaster, EventCoalescer, EventFrames, EventStreamItem, TopicEvent, CHANNEL_TOPICS,
    DEBOUNCE, HEARTBEAT, HEARTBEAT_FRAME, MAX_IDS_PER_EVENT, TOPIC_DEGRADED,
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
