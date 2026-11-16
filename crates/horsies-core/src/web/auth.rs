//! Authorization policies and request guards for monitoring HTTP routes.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Extensions, HeaderMap, Method, Request, Uri, Version};

pub const INTENT_HEADER: &str = "X-Horsies-Intent";
pub const INTENT_VALUE: &str = "action";
pub(crate) const NOT_AUTHORIZED: &str = "Not authorized.";

/// The request facts available to an authorization policy.
#[derive(Debug, Clone, Copy)]
pub struct MonitoringRequest<'a> {
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub version: Version,
    pub headers: &'a HeaderMap,
    pub extensions: &'a Extensions,
}

impl<'a> MonitoringRequest<'a> {
    pub(crate) fn from_request(request: &'a Request<Body>) -> Self {
        Self {
            method: request.method(),
            uri: request.uri(),
            version: request.version(),
            headers: request.headers(),
            extensions: request.extensions(),
        }
    }
}

/// Authorization supplied by the monitoring deployment.
#[async_trait]
pub trait MonitoringAuthPolicy: Send + Sync + 'static {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool;
    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool;
}

/// Permit reads and actions. The host deployment owns authentication.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

#[async_trait]
impl MonitoringAuthPolicy for AllowAll {
    async fn can_view(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }

    async fn can_act(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }
}

/// Permit reads and refuse every action.
#[derive(Debug, Clone, Copy, Default)]
pub struct ViewOnly;

#[async_trait]
impl MonitoringAuthPolicy for ViewOnly {
    async fn can_view(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }

    async fn can_act(&self, _request: MonitoringRequest<'_>) -> bool {
        false
    }
}

/// Trust a non-empty identity header written by a reverse proxy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedHeader {
    header_name: axum::http::HeaderName,
    allow_actions: bool,
}

impl TrustedHeader {
    pub fn new(
        header_name: impl AsRef<str>,
        allow_actions: bool,
    ) -> Result<Self, axum::http::header::InvalidHeaderName> {
        Ok(Self {
            header_name: axum::http::HeaderName::try_from(header_name.as_ref())?,
            allow_actions,
        })
    }

    pub fn header_name(&self) -> &axum::http::HeaderName {
        &self.header_name
    }

    pub const fn allows_actions(&self) -> bool {
        self.allow_actions
    }

    fn identified(&self, request: MonitoringRequest<'_>) -> bool {
        request
            .headers
            .get(&self.header_name)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| !value.trim().is_empty())
    }
}

#[async_trait]
impl MonitoringAuthPolicy for TrustedHeader {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool {
        self.identified(request)
    }

    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool {
        self.allow_actions && self.identified(request)
    }
}
