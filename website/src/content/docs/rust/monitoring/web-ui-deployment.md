---
title: Deployment and Authentication
summary: Serve the monitoring dashboard safely with the CLI or an axum router.
related: [./web-ui-overview, ./action-semantics, ../configuration/broker-config]
tags: [monitoring, web, deployment, authentication, pgbouncer]
---

The dashboard can run as a standalone process or as part of an existing axum
application. Both forms use the same API and embedded assets.

## Standalone server

Build the binary with the `web` feature.

```bash
cargo build --release --features web
```

Start the server from a TOML application configuration.

```bash
horsies web ./config/horsies.toml
```

You can also supply a database URL directly.

```bash
horsies web \
  --database-url postgresql://app:secret@db.example.com/horsies \
  --host 127.0.0.1 \
  --port 8600
```

The standalone server creates an observe-only `Horsies` application. It does
not run migrations. It does not run the task-history fleet gate. It does not
register application tasks or workflows.

Reads work without a compiled registry. Workflow resume can require registered
workflow definitions when a node uses `args_from`. Mount the router in the
application process when actions need that registry.

### PgBouncer transaction mode

Use a transaction-pool URL for queries and a direct or session-pool URL for
`LISTEN/NOTIFY`.

```bash
horsies web \
  --database-url postgresql://app:secret@pool.example.com/horsies \
  --session-database-url postgresql://app:secret@db.example.com/horsies \
  --pgbouncer-transaction-mode
```

`--pgbouncer-transaction-mode` requires both URLs. The session URL must preserve
listener state.

### Standalone options

| Option | Default | Meaning |
| --- | --- | --- |
| `CONFIG` | none | TOML application configuration path |
| `--database-url URL` | none | Runtime query URL instead of `CONFIG` |
| `--session-database-url URL` | none | Direct or session URL for events |
| `--pgbouncer-transaction-mode` | false | Treat the runtime URL as a transaction pool |
| `--host HOST` | `127.0.0.1` | Bind interface or host |
| `--port PORT` | `8600` | Bind port |
| `--auth MODE` | `none` | `none` or `trusted-header` |
| `--trusted-header NAME` | `X-Forwarded-User` | Proxy identity header |
| `--enable-actions` | false | Enable task and workflow actions |
| `--custom-css-url URL` | none | Load a stylesheet after the bundled styles |
| `--loglevel LEVEL` | `info` | `debug`, `info`, `warning`, or `error` |

Invalid security arguments exit with code 2. Startup and server errors exit
with code 1. A clean shutdown exits with code 0.

## Mount in axum

`create_monitoring_router` returns a normal axum `Router`. Nest it at any path.

```rust
use std::sync::Arc;

use axum::Router;
use horsies::web::{
    create_monitoring_router, MonitoringUiConfig, ViewOnly,
};
use horsies::Horsies;

async fn router(app: &Horsies) -> Result<Router, horsies::AppError> {
    let broker = app.get_broker().await?;
    let monitoring = create_monitoring_router(
        app,
        Arc::clone(&broker),
        ViewOnly,
        MonitoringUiConfig::default(),
        false,
    );

    Ok(Router::new().nest("/monitoring", monitoring))
}
```

The server injects the mount path into the SPA. API requests and assets remain
relative to that path.

## Authorization policies

The router checks view authorization before any action-specific check. It then
checks action authorization, the intent header, and schema compatibility.

| Policy | Reads | Actions |
| --- | --- | --- |
| `ViewOnly` | Allowed | Refused |
| `AllowAll` | Allowed | Allowed when actions are enabled |
| `TrustedHeader` | Requires a non-empty identity header | Also requires the policy to allow actions |
| Custom `MonitoringAuthPolicy` | Defined by the host | Defined by the host |

`AllowAll` means that the host application owns the authentication boundary.
Do not expose an `AllowAll` router without that boundary.

Every action request must include this header:

```text
X-Horsies-Intent: action
```

The intent header does not replace authentication. It is an extra guard for
state-changing requests.

### Trusted reverse proxy

`--auth trusted-header` trusts the configured identity header. The reverse
proxy MUST strip or overwrite that header on every incoming request. A proxy
that forwards a client value makes the policy spoofable.

The CLI prints this warning on every trusted-header start.

The standalone CLI serves at the root path. Proxy it at the root path too.

```nginx
location / {
    proxy_set_header X-Forwarded-User $remote_user;
    proxy_pass http://127.0.0.1:8600/;
}
```

The authentication layer before this block must set `$remote_user`. The proxy
must not preserve a client-supplied `X-Forwarded-User` value.

Mount `create_monitoring_router` in an axum application when the dashboard
needs a path prefix such as `/horsies/`.

`--auth none` is accepted only on `localhost` or a loopback IP address. Use a
trusted proxy or a custom mounted policy for any network-reachable bind.

## Read-only schema mode

The schema probe uses catalog reads only. It never creates tables, applies a
migration, or writes a cutover attestation.

The server can serve reads when the stored version differs from the expected
version. It disables every action until the version matches and the task-history
cutover attestation is present.

An absent schema and an unknown schema still serve the embedded shell. The UI
shows the matching refusal state instead of issuing actions.

## Custom CSS

The CLI accepts one stylesheet URL.

```bash
horsies web ./config/horsies.toml \
  --custom-css-url https://assets.example.com/horsies.css
```

Mounted deployments set the same value in `MonitoringUiConfig`.

```rust
use horsies::web::MonitoringUiConfig;

let ui = MonitoringUiConfig {
    custom_css_url: Some("/assets/horsies.css".to_owned()),
};
```

The link appears after the bundled stylesheet. The browser loads it on each
page. The server does not fetch, validate, or copy the stylesheet.

## Assets and processes

The production SPA is embedded in the Rust binary. No Node.js process is
required at runtime.

One dashboard process owns one dedicated event-listener session. Size direct
or session-pooled PostgreSQL capacity for that connection plus normal query
traffic.

Run more than one dashboard process for process-level availability. Each
process owns its own event listener and cache.
