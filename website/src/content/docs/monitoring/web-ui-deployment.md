---
title: Web UI Deployment & Authentication
summary: Mounting the monitoring app in a host application, running it standalone, and the identity, connection, and schema contracts each mode carries.
related: [web-ui-overview, action-semantics, ../configuration/remote-postgres, ../troubleshooting/pgbouncer]
tags: [monitoring, web, deployment, authentication, security, pgbouncer]
---

The monitoring app runs in one of two modes: mounted inside a host FastAPI
application, or standalone from the `horsies web` CLI. Both serve the same API
and the same SPA. They differ in who owns identity and who owns the broker.

horsies never owns identity. Every deployment supplies an authorization policy,
and the library enforces the verdict it returns.

## Mounted mode

`create_monitoring_app` builds a FastAPI sub-app. Mount it at any path; the SPA
detects its mount point per request and works from any prefix.

```python
from fastapi import FastAPI
from horsies.web import AllowAll, create_monitoring_app

from myapp.tasks import app as horsies_app

api = FastAPI()
api.mount(
    "/monitoring",
    create_monitoring_app(horsies_app, auth_policy=AllowAll()),
)
```

`auth_policy` is keyword-only and has no default. `AllowAll()` is not "no
authentication" — it is the statement *this mount is already guarded by the host
application*. Mount it behind the host's own authentication, or behind a policy
that enforces one.

Mounted mode constructs nothing: it uses the broker the host app already
configured, with whatever pooling and migration behavior that app was built
with.

### Authorization policies

A policy answers two questions per request.

```python
from typing import Protocol

from fastapi import Request


class MonitoringAuthPolicy(Protocol):
    async def can_view(self, request: Request) -> bool: ...
    async def can_act(self, request: Request) -> bool: ...
```

| Policy | `can_view` | `can_act` | Use |
|---|---|---|---|
| `AllowAll()` | `True` | `True` | The mount already sits behind the host app's authentication |
| `ViewOnly()` | `True` | `False` | Read-only dashboard |
| `TrustedHeader(header_name, allow_actions=...)` | header present and non-empty | same check, gated by `allow_actions` | A reverse proxy is the identity boundary |

Every `/api` route depends on `can_view`; a false verdict returns 403
`{"detail": "Not authorized."}` and the SPA renders a full-screen
not-authorized state. Every mutating route additionally requires `can_act` and
the header `X-Horsies-Intent: action`; a POST without that header is rejected
with 403.

Implement the protocol directly to delegate to the host app's session, JWT, or
RBAC layer.

```python
from fastapi import Request

from myapp.security import current_user


class SessionPolicy:
    async def can_view(self, request: Request) -> bool:
        user = await current_user(request)
        return user is not None

    async def can_act(self, request: Request) -> bool:
        user = await current_user(request)
        return user is not None and user.is_operator
```

### Custom CSS

`MonitoringUIConfig` injects one stylesheet link last in the served
`index.html`, after every built-in stylesheet, so it overrides any design token.

```python
from horsies.web import MonitoringUIConfig, ViewOnly, create_monitoring_app

from myapp.tasks import app as horsies_app

monitoring = create_monitoring_app(
    horsies_app,
    auth_policy=ViewOnly(),
    config=MonitoringUIConfig(custom_css_url="/static/horsies-theme.css"),
)
```

Overriding CSS custom properties is the entire customization contract. There is
no build-time theming and no theme JSON.

## Standalone CLI

```bash
horsies web myapp.tasks:app
```

```bash
horsies web --database-url "postgresql+psycopg://user:pass@localhost:5432/mydb"
```

The first form imports the application module and gets the full feature set. It
runs the same startup validation as `horsies worker` and `horsies scheduler`,
so a configuration error refuses to serve rather than surfacing later as a
failed action.

The second form is registry-less: it constructs a minimal `Horsies` app from the
URL alone. Task rows carry their own encoded arguments, queue, priority, and
options, so this form supports every read, every task action, and workflow pause
and cancel. It does not support resuming a run whose next nodes carry
`args_from`: the resume re-enqueues those nodes, and encoding an upstream result
into a fresh task row requires the source task's registered return type. Use the
app-path form where that matters.

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `<app_path>` | — | Dotted path to the app, e.g. `myapp.tasks:app`. Mutually exclusive with `--database-url` |
| `--database-url URL` | — | Registry-less mode. Mutually exclusive with `<app_path>` |
| `--session-database-url URL` | — | Direct, session-capable URL for LISTEN/NOTIFY when the main URL is transaction-pooled |
| `--pgbouncer-transaction-mode` | off | Disable server-side statement caching for a transaction-pooled PgBouncer |
| `--host HOST` | `127.0.0.1` | Bind address |
| `--port PORT` | `8600` | Bind port |
| `--auth MODE` | `none` | `none` or `trusted-header` |
| `--trusted-header NAME` | `X-Forwarded-User` | Header carrying the proxy-verified identity |
| `--enable-actions` | off | Permit the five mutating endpoints |

### Bind rules

The CLI fails closed on the argument combination.

| Condition | Result |
|---|---|
| Both `<app_path>` and `--database-url`, or neither | Argument error |
| Non-loopback `--host` with `--auth none` | Exit 2 with an explanatory error |
| Non-loopback `--host` | Requires `--auth trusted-header` and a header name |
| `--auth none` on loopback, without `--enable-actions` | Policy is `ViewOnly` |
| `--auth none` on loopback, with `--enable-actions` | Policy is `AllowAll` |

Actions are off unless `--enable-actions` is passed. A dashboard exposed on a
non-loopback address without a proxy-enforced identity is not a reachable
configuration.

### Trusted-header mode

```bash
horsies web myapp.tasks:app \
  --host 0.0.0.0 --port 8600 \
  --auth trusted-header --trusted-header X-Forwarded-User \
  --enable-actions
```

A request is authorized if and only if the named header is present and
non-empty. The reverse proxy is the identity boundary; horsies performs no
verification of its own.

**The proxy MUST strip or overwrite the trusted header on every incoming
request.** A proxy that forwards a client-supplied `X-Forwarded-User` makes this
mode trivially spoofable: any caller sets the header and is authorized. horsies
cannot detect this — it is an invariant of the deployment, not of the library.
The CLI prints this warning at startup in this mode.

Proxies that satisfy the invariant when configured to set the identity header
themselves: oauth2-proxy, Cloudflare Access, Tailscale Serve.

## Connection footprint

The web app uses the broker's existing pool for queries and adds one dedicated
listener.

| Source | Cost |
|---|---|
| API requests | One pooled connection per concurrent request, released on response |
| SSE event listener | ~2 connections for the process lifetime |
| Manual worker ping | One LISTEN round trip per invocation |

The listener is the web app's own `PostgresListener`, not the broker's internal
one, which isolates it from result-waiter state. It starts lazily on the first
SSE subscriber and closes at shutdown. Its subscriptions are held for the app's
lifetime, so there is no per-request LISTEN churn.

Size the pool for the number of concurrent dashboard viewers, and read
[PgBouncer Troubleshooting](../troubleshooting/pgbouncer) before pointing the
monitoring app at a transaction-pooled URL.

### PgBouncer

LISTEN/NOTIFY requires a session-mode connection. When the main URL points at a
transaction-pooled PgBouncer, give the listener a direct URL:

```bash
horsies web \
  --database-url "$DATABASE_URL_POOLED" \
  --session-database-url "$DATABASE_URL_DIRECT" \
  --pgbouncer-transaction-mode
```

In mounted mode this is already the host app's `PostgresConfig`: the web app
uses `session_database_url` when set, and falls back to `database_url`
otherwise. Without a session-capable URL the event stream cannot connect, and
the dashboard runs permanently on its polling fallback.

## Schema handling

**The monitoring layer never executes DDL.** Creating, migrating, or repairing
the schema belongs to the library and its workers. A broker the monitoring layer
constructs itself — the registry-less CLI path — skips schema migrations
entirely. Mounted mode constructs nothing, so the host app's broker behaves
exactly as that app configured it.

Instead of migrating, the app reads `horsies_schema_version` at startup with a
plain `SELECT`, caches the verdict for 60 seconds, and re-reads lazily. There
are four states.

| State | Condition | Reads | Actions | UI |
|---|---|---|---|---|
| `MATCH` | Stored version equals the version this build expects | Served | Permitted, subject to the auth policy | Normal |
| `MISMATCH` | Stored version differs, older or newer | Served | Force-disabled server-side | Persistent read-only banner naming both versions |
| `ABSENT` | A **successful** probe observed the version table or row missing | — | Disabled | Full-screen state: the database has no horsies schema |
| `UNKNOWN` | The probe has **never** succeeded — connection failure with no prior verdict | — | Disabled | Full-screen state: the database cannot be reached to determine its schema |

Two rules follow from that table:

- `ABSENT` may only come from a successful observation. A failed read is not a
  verdict. `UNKNOWN` exists so that an unreachable database is never reported as
  an uninitialized one, which would tell an operator to initialize a database
  that is merely down.
- A failed probe that has a prior verdict keeps that verdict until the cache
  expires and a read succeeds.

`ABSENT` and `UNKNOWN` both stop the SPA at a full-screen state, so no read is
issued from the dashboard.

While the schema does not match, every action endpoint returns 409
`{"code": "SCHEMA_INCOMPATIBLE"}` regardless of the auth policy, and
`/api/meta` reports `actions_enabled: false` with `actions_disabled_reason` set
to `SCHEMA_INCOMPATIBLE`. `UNKNOWN` reports `SCHEMA_UNKNOWN`. Under `MISMATCH`
reads continue to be served: a query that is genuinely incompatible fails on its
own terms rather than being pre-emptively blocked.

The remedy for `MISMATCH` is to upgrade horsies — workers apply migrations — or
to run the UI version that matches the stored schema. The remedy for `ABSENT` is
to start a horsies app or worker against that database.

## Things to Avoid

**Don't expose the CLI on a non-loopback address with `--auth none`.** The CLI
refuses this combination, and the refusal is the point: the dashboard exposes
every task payload, error message, and worker hostname in the deployment.

```bash
# Wrong — refused with exit 2
horsies web myapp.tasks:app --host 0.0.0.0 --auth none

# Correct — a proxy owns identity, and it strips the header on ingress
horsies web myapp.tasks:app --host 0.0.0.0 \
  --auth trusted-header --trusted-header X-Forwarded-User
```

**Don't pass `AllowAll()` to reach a dashboard you have not otherwise
guarded.** `AllowAll` asserts that the mount is already authenticated. It is
required to be explicit precisely so the choice is deliberate.

**Don't point the monitoring app at a database expecting it to create the
schema.** It reports `ABSENT` and stops. Start a horsies app or worker instead.

## Static assets

The SPA is served from `horsies/web/static/`, which CI builds into the wheel. If
that directory is missing — a source checkout, or a build that skipped the
frontend step — `/api/*` continues to work and `GET /` returns 503:

```json
{"detail": "horsies web UI assets are not built. Run: cd webui && bun install && bun run build"}
```

Static assets are served without the auth dependency; they carry no data. Every
`/api/*` route is gated, so an unauthorized viewer receives the shell and then
the not-authorized state.
