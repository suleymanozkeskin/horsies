---
title: Web UI Mount
summary: Mount the monitoring web UI in an axum app with horsies::web::router, and decide access per request with a MonitoringAuthPolicy.
related: [monitoring-queries, ../cli, ../../monitoring/web-ui-overview, ../../monitoring/web-ui-deployment, ../../monitoring/action-semantics]
tags: [rust, monitoring, web, axum, router, MonitoringAuthPolicy, TrustedHeader]
---

# Web UI Mount

`horsies::web::router` returns the monitoring web UI of an app as an `axum::Router`, for the host app to serve or nest. The core serves every route, guard and event of the UI, so the mount answers as the Python mount does. The surfaces of the UI are on [Web UI Overview](/horsies/monitoring/web-ui-overview/); the deployment modes, the policies and the schema contract are on [Web UI Deployment & Authentication](/horsies/monitoring/web-ui-deployment/).

## Add the Feature

The mount is in the `web` feature of the crate:

```bash
cargo add horsies --features web
```

The host app builds its router with `axum` 0.8, the version that the crate uses.

The `horsies web` command of the shipped binary serves the UI of one database with no app: see [CLI Reference](/horsies/rust/cli/#horsies-web).

## Mount the Router

```rust
use axum::Router;
use horsies::web::{router, MonitoringConfig, TrustedHeader};
use horsies::Horsies;

fn monitoring_routes(app: &Horsies) -> Result<Router, Box<dyn std::error::Error>> {
    let policy = TrustedHeader::new("X-Forwarded-User", true)?;
    let monitoring = router(app, MonitoringConfig::default(), policy);
    Ok(Router::new().nest("/monitoring", monitoring))
}
```

`router(&app, config, policy) -> axum::Router` serves every path of the mount: the page and its assets, `/api/...`, and the event stream `GET /api/events`.

- Building the router does no I/O. The core handler is built at the first request, on the app's handle. A request whose broker cannot be built is answered 503 with the cause; the next request tries again.
- The mount runs no schema step. For a process that only monitors, build the app with `Horsies::with_schema_mode(config, SchemaMode::ObserveOnly)`. See [App Config](/horsies/rust/configuration/app-config/#schema-modes).
- A workflow resume builds the sub-workflow children that it owes from the workflow definitions registered on `app` at the first request. Mount the router from the app's own binary, after the registrations, when the UI must resume such workflows.

## MonitoringConfig

| Field | Type | Default |
|-------|------|---------|
| `ui` | `MonitoringUiConfig { custom_css_url: Option<String> }` | No stylesheet |
| `actions` | `Actions::Enabled` or `Actions::Disabled` | `Enabled` |

- With `Actions::Disabled`, every action route answers 403 and the policy's `can_act` is not called.
- `custom_css_url` names one stylesheet that the page loads after its own. See [Custom CSS](/horsies/monitoring/web-ui-deployment/#custom-css).

## Access Policy

| Policy | `can_view` | `can_act` |
|--------|------------|-----------|
| `AllowAll` | Yes | Yes. The host owns authentication: mount the router behind it |
| `ViewOnly` | Yes | No |
| `TrustedHeader::new(header_name, allow_actions)` | The header has a value that is visible text and not blank | `allow_actions` and the header check |

`TrustedHeader::new` returns `Result<TrustedHeader, InvalidHeaderName>`. `header_name()` and `allows_actions()` read it back.

### Write a Policy

```rust
use horsies::web::{MonitoringAuthPolicy, MonitoringRequest};

struct AdminHeader;

impl MonitoringAuthPolicy for AdminHeader {
    async fn can_view(&self, request: MonitoringRequest<'_>) -> bool {
        request.headers.contains_key("x-admin-user")
    }

    async fn can_act(&self, request: MonitoringRequest<'_>) -> bool {
        request.headers.contains_key("x-admin-user")
    }
}
```

- `MonitoringAuthPolicy` is `Send + Sync + 'static`. Write its methods as `async fn`; each future must be `Send`.
- `MonitoringRequest` has `method`, `uri` (the path inside the mount), `version`, `headers` and `extensions`. `extensions` holds what the host's own layers put there, for example the user of an authentication layer.
- The mount asks only the verdicts that the route needs, in this order: `can_view` for every read route, the event stream and `/api/meta`; then, when it allowed, `can_act` for an action route while actions are enabled, and for `/api/meta`, which reports it. The page and its assets ask nothing.
- An action route also needs the header `X-Horsies-Intent: action` (`INTENT_HEADER`, `INTENT_VALUE`). It guards against cross-site requests; it is not authentication.

The answers to a refused request are on [Authorization Policies](/horsies/monitoring/web-ui-deployment/#authorization-policies).

## Things to Avoid

**Do not mount `AllowAll` without the host's own authentication.** It allows every read and every action.

**Do not trust an identity header that the reverse proxy passes through.** `TrustedHeader` is safe only when the proxy strips or overwrites the header on each incoming request.

**Do not point the mount at a database to create the schema.** The mount runs no schema step. Start an app or a worker to install it.
