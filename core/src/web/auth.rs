//! The access guard of the monitoring routes: the request facts that a
//! guard reads, and the verdicts it asks. In production the only policy is
//! the grant of the request ([`super::handler::GrantedAccess`]): the host
//! framework runs the user's policy and passes its verdicts.

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Extensions, HeaderMap, Request};

pub const INTENT_HEADER: &str = "X-Horsies-Intent";
pub const INTENT_VALUE: &str = "action";
pub(crate) const NOT_AUTHORIZED: &str = "Not authorized.";

/// The request facts that a guard passes to the policy.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MonitoringRequest<'a> {
    /// Read by the header policy of the router tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) headers: &'a HeaderMap,
    pub(crate) extensions: &'a Extensions,
}

impl<'a> MonitoringRequest<'a> {
    pub(crate) fn from_request(request: &'a Request<Body>) -> Self {
        Self {
            headers: request.headers(),
            extensions: request.extensions(),
        }
    }
}

/// The verdicts that the guards of the router ask for one request.
#[async_trait]
pub(crate) trait MonitoringAuthPolicy: Send + Sync + 'static {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool;
    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool;
}
