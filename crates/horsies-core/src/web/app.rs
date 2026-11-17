//! Axum monitoring router factory and runtime state.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{NestedPath, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::Horsies;

use super::auth::{
    MonitoringAuthPolicy, MonitoringRequest, INTENT_HEADER, INTENT_VALUE, NOT_AUTHORIZED,
};
use super::common::{ApiError, CodeBody, DetailBody};
use super::events::EventBroadcaster;
use super::events::HEARTBEAT;
use super::routes;
use super::schema::{
    SchemaIncompatible, SchemaProbe, SchemaState, SCHEMA_INCOMPATIBLE, SCHEMA_UNKNOWN,
};
use super::spa::{
    inject, normalize_base_path, safe_asset_path, AssetStore, EmbeddedAssets, MonitoringUiConfig,
    ASSETS_MISSING_DETAIL,
};
use super::task_stats_cache::{ReadCache, TaskStatsCache};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaResponse {
    pub horsies_version: String,
    pub base_path: String,
    pub actions_enabled: bool,
    pub can_act: bool,
    pub schema_version: Option<i64>,
    pub expected_schema_version: i64,
    pub schema_compatible: bool,
    /// The probe's verdict itself, so a client never derives it from the
    /// version numbers (Python `python/horsies/web/app.py:64-99`).
    pub schema_state: SchemaState,
    pub actions_disabled_reason: Option<String>,
}

#[derive(Clone)]
pub(crate) struct WebState {
    pub broker: Arc<PostgresBroker>,
    pub auth_policy: Arc<dyn MonitoringAuthPolicy>,
    pub schema_probe: Arc<SchemaProbe>,
    pub task_stats_cache: Arc<TaskStatsCache>,
    pub task_facets_cache: Arc<ReadCache<crate::monitoring::Facets>>,
    pub task_breakdown_cache: Arc<ReadCache<crate::monitoring::Breakdown>>,
    pub events: Arc<EventBroadcaster>,
    pub actions_enabled: bool,
    pub ui_config: MonitoringUiConfig,
    pub assets: Arc<dyn AssetStore>,
    pub workflow_registry: Arc<WorkflowSpecRegistry>,
    pub payload: PayloadPolicy,
    pub retention: RetentionConfig,
    pub heartbeat: std::time::Duration,
}

/// Build a monitoring router for an already connected broker.
///
/// The event stream and the worker ping subscribe on the broker's listener;
/// an observe-only broker has one. A broker built from a pool with no session
/// URL has no listener: it sends the degraded frame on the event stream and
/// answers the worker ping with 503.
pub fn create_monitoring_router<P>(
    app: &Horsies,
    broker: Arc<PostgresBroker>,
    auth_policy: P,
    config: MonitoringUiConfig,
    actions_enabled: bool,
) -> Router
where
    P: MonitoringAuthPolicy,
{
    let state = WebState {
        schema_probe: Arc::new(SchemaProbe::new(Arc::clone(&broker))),
        task_stats_cache: Arc::new(TaskStatsCache::new()),
        task_facets_cache: Arc::new(ReadCache::new()),
        task_breakdown_cache: Arc::new(ReadCache::new()),
        events: EventBroadcaster::new(Arc::clone(&broker)),
        broker,
        auth_policy: Arc::new(auth_policy),
        actions_enabled,
        ui_config: config,
        assets: Arc::new(EmbeddedAssets),
        workflow_registry: Arc::new(app.workflow_registry().clone()),
        payload: app.config().payload.clone(),
        retention: app.config().retention.clone(),
        heartbeat: HEARTBEAT,
    };
    build_router(state)
}

pub(crate) fn build_router(state: WebState) -> Router {
    let read_routes = routes::read_router()
        .route("/meta", get(read_meta))
        .route_layer(middleware::from_fn_with_state(state.clone(), view_guard));
    let action_routes = routes::action_router()
        .route_layer(middleware::from_fn_with_state(state.clone(), action_guard));

    let api = read_routes.merge(action_routes).fallback(api_not_found);
    Router::new()
        .nest("/api", api)
        .route("/", get(serve_spa))
        .fallback(serve_spa)
        .with_state(state)
}

async fn api_not_found() -> Response {
    ApiError::detail(StatusCode::NOT_FOUND, "Not found.").into_response()
}

async fn view_guard(State(state): State<WebState>, request: Request, next: Next) -> Response {
    if !state
        .auth_policy
        .can_view(MonitoringRequest::from_request(&request))
        .await
    {
        return (
            StatusCode::FORBIDDEN,
            Json(DetailBody {
                detail: NOT_AUTHORIZED.to_owned(),
            }),
        )
            .into_response();
    }
    next.run(request).await
}

async fn action_guard(State(state): State<WebState>, request: Request, next: Next) -> Response {
    let facts = MonitoringRequest::from_request(&request);
    if !state.auth_policy.can_view(facts).await {
        return forbidden(NOT_AUTHORIZED);
    }
    let facts = MonitoringRequest::from_request(&request);
    if !state.actions_enabled || !state.auth_policy.can_act(facts).await {
        return forbidden(NOT_AUTHORIZED);
    }
    if request
        .headers()
        .get(INTENT_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some(INTENT_VALUE)
    {
        return forbidden(&format!("Missing {INTENT_HEADER}: {INTENT_VALUE} header."));
    }

    let status = state.schema_probe.status().await;
    if !status.compatible() {
        let incompatible = SchemaIncompatible::from_status(status);
        return (
            StatusCode::CONFLICT,
            Json(CodeBody {
                code: incompatible.code,
                detail: incompatible.detail,
            }),
        )
            .into_response();
    }
    next.run(request).await
}

fn forbidden(detail: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(DetailBody {
            detail: detail.to_owned(),
        }),
    )
        .into_response()
}

fn base_path(request: &Request<Body>, api_route: bool) -> String {
    let nested = request
        .extensions()
        .get::<NestedPath>()
        .map(NestedPath::as_str)
        .unwrap_or_default();
    let mount = if api_route {
        nested.strip_suffix("/api").unwrap_or(nested)
    } else {
        nested
    };
    normalize_base_path(mount)
}

async fn read_meta(State(state): State<WebState>, request: Request) -> Response {
    let can_act = state
        .auth_policy
        .can_act(MonitoringRequest::from_request(&request))
        .await;
    let base_path = base_path(&request, true);
    let schema = state.schema_probe.status().await;
    let disabled_reason = match schema.state {
        SchemaState::Match => None,
        SchemaState::Unknown => Some(SCHEMA_UNKNOWN.to_owned()),
        SchemaState::Mismatch | SchemaState::Absent | SchemaState::ZeroX => {
            Some(SCHEMA_INCOMPATIBLE.to_owned())
        }
    };
    Json(MetaResponse {
        horsies_version: env!("CARGO_PKG_VERSION").to_owned(),
        base_path,
        actions_enabled: state.actions_enabled && schema.compatible(),
        can_act,
        schema_version: schema.version,
        expected_schema_version: schema.expected_version,
        schema_compatible: schema.compatible(),
        schema_state: schema.state,
        actions_disabled_reason: disabled_reason,
    })
    .into_response()
}

async fn serve_spa(State(state): State<WebState>, request: Request) -> Response {
    let path = request.uri().path();
    let inner_path = path.trim_start_matches('/');
    if inner_path == "api" || inner_path.starts_with("api/") {
        return ApiError::detail(StatusCode::NOT_FOUND, "Not found.").into_response();
    }

    if let Some(asset_path) = safe_asset_path(path) {
        if let Some(asset) = state.assets.get(&asset_path) {
            return (
                [(header::CONTENT_TYPE, asset.content_type)],
                Body::from(asset.bytes.into_owned()),
            )
                .into_response();
        }
    }

    let Some(index) = state.assets.get("index.html") else {
        return ApiError::unavailable(ASSETS_MISSING_DETAIL).into_response();
    };
    let Ok(index_html) = std::str::from_utf8(index.bytes.as_ref()) else {
        return ApiError::unavailable("horsies web UI index is not valid UTF-8").into_response();
    };
    let page = inject(
        index_html,
        &base_path(&request, false),
        state.ui_config.custom_css_url.as_deref(),
    );
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], page).into_response()
}
