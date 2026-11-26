//! The access policy of the monitoring mount: user code that decides, per
//! request, whether the request may read and whether it may act (Python
//! `horsies.web.auth`).

use std::future::Future;

use axum::http::header::InvalidHeaderName;
use axum::http::request::Parts;
use axum::http::{Extensions, HeaderMap, HeaderName, Method, Uri, Version};

/// The facts of one request that a policy reads. The path of `uri` is the
/// path inside the mount.
#[derive(Debug, Clone, Copy)]
pub struct MonitoringRequest<'a> {
    pub method: &'a Method,
    pub uri: &'a Uri,
    pub version: Version,
    pub headers: &'a HeaderMap,
    /// The extensions of the request: what the host's own layers (an
    /// authentication layer) put there.
    pub extensions: &'a Extensions,
}

impl<'a> MonitoringRequest<'a> {
    /// The facts of the request head `head`.
    pub(crate) fn of(head: &'a Parts) -> Self {
        Self {
            method: &head.method,
            uri: &head.uri,
            version: head.version,
            headers: &head.headers,
            extensions: &head.extensions,
        }
    }
}

/// The policy of a monitoring mount. The mount asks only the verdicts that
/// the route needs, in this order: `can_view` for every read route, the
/// event stream and `/api/meta`; then, when it allowed, `can_act` for an
/// action route while actions are enabled, and for `/api/meta`, which
/// reports it. The page and its assets ask nothing.
///
/// Implement it with `async fn`; each future must be `Send`.
pub trait MonitoringAuthPolicy: Send + Sync + 'static {
    /// Whether the request may read.
    fn can_view(&self, request: MonitoringRequest<'_>) -> impl Future<Output = bool> + Send;
    /// Whether the request may run an action.
    fn can_act(&self, request: MonitoringRequest<'_>) -> impl Future<Output = bool> + Send;
}

/// Allow reads and actions. The host owns authentication: mount the router
/// behind the host's own authentication layer.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl MonitoringAuthPolicy for AllowAll {
    async fn can_view(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }

    async fn can_act(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }
}

/// Allow reads and refuse every action.
#[derive(Debug, Clone, Copy, Default)]
pub struct ViewOnly;

impl MonitoringAuthPolicy for ViewOnly {
    async fn can_view(&self, _request: MonitoringRequest<'_>) -> bool {
        true
    }

    async fn can_act(&self, _request: MonitoringRequest<'_>) -> bool {
        false
    }
}

/// Trust an identity header that a reverse proxy writes: a request with a
/// value that is not blank, with no blank comma-separated member, may read, and may act when `allow_actions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedHeader {
    header_name: HeaderName,
    allow_actions: bool,
}

impl TrustedHeader {
    /// The policy of the header `header_name`.
    ///
    /// Errors: [`InvalidHeaderName`] for a name that is not a header name.
    pub fn new(
        header_name: impl AsRef<str>,
        allow_actions: bool,
    ) -> Result<Self, InvalidHeaderName> {
        Ok(Self {
            header_name: HeaderName::try_from(header_name.as_ref())?,
            allow_actions,
        })
    }

    pub fn header_name(&self) -> &HeaderName {
        &self.header_name
    }

    pub const fn allows_actions(&self) -> bool {
        self.allow_actions
    }

    /// Whether the request has the header, and every value of it is visible
    /// text with no blank comma-separated member. Two copies of the header
    /// that an HTTP stack joins to `alice, ` are refused.
    fn identified(&self, request: MonitoringRequest<'_>) -> bool {
        let mut values = request.headers.get_all(&self.header_name).iter().peekable();
        values.peek().is_some()
            && values.all(|value| {
                value
                    .to_str()
                    .is_ok_and(|text| text.split(',').all(|member| !member.trim().is_empty()))
            })
    }
}

impl MonitoringAuthPolicy for TrustedHeader {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool {
        self.identified(request)
    }

    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool {
        self.allow_actions && self.identified(request)
    }
}
