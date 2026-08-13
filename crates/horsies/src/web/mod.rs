//! Optional axum transport for the monitoring API.

mod app;
mod auth;
mod common;
mod events;
mod routes;
mod schema;
mod spa;

pub use app::{create_monitoring_router, MetaResponse};
pub use auth::{
    AllowAll, MonitoringAuthPolicy, MonitoringRequest, TrustedHeader, ViewOnly, INTENT_HEADER,
    INTENT_VALUE,
};
pub use events::{
    EventBroadcaster, EventCoalescer, TopicEvent, CHANNEL_TOPICS, DEBOUNCE, HEARTBEAT,
    MAX_IDS_PER_EVENT, TOPIC_DEGRADED,
};
pub use schema::{
    SchemaIncompatible, SchemaProbe, SchemaState, SchemaStatus, SCHEMA_INCOMPATIBLE, SCHEMA_UNKNOWN,
};
pub use spa::{MonitoringUiConfig, ASSETS_MISSING_DETAIL};

#[cfg(test)]
mod tests;
