---
title: Web UI Deployment & Authentication
section: shared
summary: Mounting the monitoring app in a host application, running it standalone, and the identity, connection, and schema contracts each mode carries.
related: [web-ui-overview, web-ui-mount, action-semantics, ../configuration/remote-postgres, ../troubleshooting/pgbouncer]
tags: [monitoring, web, deployment, authentication, security, pgbouncer]
---

The monitoring app runs in one of two modes: mounted inside a host web
application, or standalone from the `horsies web` command of the Python package.
Both serve the same API and the same SPA. They differ in who owns identity and
who owns the broker.

horsies never owns identity. Every deployment supplies an authorization policy,
and the library enforces the verdict it returns.

The API is on the client pages: [Web UI Mount](/horsies/monitoring/web-ui-mount/)
(Python). The Rust page is not written yet.

## Mounted mode

The client builds the monitoring app, and the host application mounts it at any
path. The SPA detects its mount point per request and works from any prefix.

The mount takes an authorization policy, and the policy has no default.
`AllowAll` is not "no authentication": it is the statement *this mount is
already guarded by the host application*. Mount it behind the host's own
authentication, or behind a policy that enforces one.

Mounted mode constructs nothing: it uses the broker the host app already
configured, with whatever pooling and migration behavior that app was built
with.

### Authorization policies

A policy answers two questions per request: can the request view (`can_view`),
and can it act (`can_act`).

| Policy | `can_view` | `can_act` | Use |
|---|---|---|---|
| `AllowAll` | `True` | `True` | The mount already sits behind the host app's authentication |
| `ViewOnly` | `True` | `False` | Read-only dashboard |
| `TrustedHeader` (a header name, and whether actions are allowed) | header present and non-empty | same check, when actions are allowed | A reverse proxy is the identity boundary |

Every `/api` route depends on `can_view`; a false verdict returns 403
`{"detail": "Not authorized."}` and the SPA renders a full-screen
not-authorized state. Every mutating route additionally requires `can_act` and
the header `X-Horsies-Intent: action`; a POST without that header is rejected
with 403.

A custom policy answers the two questions itself, for example from the host
app's session, JWT, or RBAC layer.

### Custom CSS

The UI config can name one stylesheet URL. The app injects its link last in the
served `index.html`, after every built-in stylesheet, so it overrides any design
token.

Overriding CSS custom properties is the entire customization contract. There is
no build-time theming and no theme JSON.

## Standalone CLI

```bash
horsies web myapp.tasks:app
```

```bash
horsies web --database-url "postgresql://user:pass@localhost:5432/mydb"
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

The flags are in the [CLI Reference](/horsies/cli/#horsies-web).

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

The web app uses the broker's existing pool for queries and the process
listener of the broker for the event stream and the worker ping.

| Source | Cost |
|---|---|
| API requests | One pooled connection per concurrent request, released on response |
| SSE event stream | No connection of its own: subscriptions on the process listener (one connection per session URL and process) |
| Manual worker ping | One LISTEN round trip per invocation, on the process listener |

Each event stream has a bounded queue on the process listener, so a slow
stream cannot hold the broker's result waiters. The stream's subscriptions
start on the first SSE subscriber and are held for the app's lifetime, so
there is no per-request LISTEN churn.

Size the pool for the number of concurrent dashboard viewers, and read
[PgBouncer Troubleshooting](/horsies/troubleshooting/pgbouncer/) before pointing the
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

In mounted mode this is already the broker config of the host app: the
process listener uses `session_database_url` when set, and falls back to
`database_url` otherwise. Without a session-capable URL the event stream cannot connect, and
the dashboard runs permanently on its polling fallback.

## Schema handling

**The monitoring layer never executes DDL.** Creating, migrating, or repairing
the schema belongs to the library and its workers. A broker the monitoring layer
constructs itself — the registry-less CLI path — skips schema migrations
entirely. Mounted mode constructs nothing, so the host app's broker behaves
exactly as that app configured it.

Instead of migrating, the app reads the highest successful version in
`horsies_migrations` at startup with a plain `SELECT`, caches the verdict for
60 seconds, and re-reads lazily. There are five states.

<!-- members: SchemaState -->
| State | Condition | Reads | Actions | UI |
|---|---|---|---|---|
| `MATCH` | Stored version equals the version this build expects | Served | Permitted, subject to the auth policy | Normal |
| `MISMATCH` | Stored version differs, older or newer | Served | Force-disabled server-side | Persistent read-only banner naming both versions |
| `ABSENT` | A **successful** probe found neither `horsies_migrations` nor `horsies_schema_version`, or no successful row in `horsies_migrations` | — | Disabled | Full-screen state: the database has no horsies schema |
| `ZERO_X` | A **successful** probe observed a horsies 0.x schema: `horsies_schema_version` without `horsies_migrations` (Python 0.x), or a highest version below 1000 (horsies-rust 0.x) | — | Disabled | Full-screen state: the database holds a 0.x schema that this build does not read or upgrade |
| `UNKNOWN` | The probe has **never** succeeded — connection failure with no prior verdict | — | Disabled | Full-screen state: the database cannot be reached to determine its schema |

`/api/meta` reports the state in the field `schema_state`, with the values in
the first column. The SPA picks its screen and banner from this field. It does
not derive the state from the version numbers.

Two rules follow from that table:

- `ABSENT` may only come from a successful observation. A failed read is not a
  verdict. `UNKNOWN` exists so that an unreachable database is never reported as
  an uninitialized one, which would tell an operator to initialize a database
  that is merely down.
- A failed probe that has a prior verdict keeps that verdict until the cache
  expires and a read succeeds.

`ABSENT`, `ZERO_X` and `UNKNOWN` stop the SPA at a full-screen state, so no
read is issued from the dashboard.

While the schema does not match, every action endpoint returns 409
`{"code": "SCHEMA_INCOMPATIBLE"}` regardless of the auth policy, and
`/api/meta` reports `actions_enabled: false` with `actions_disabled_reason` set
to `SCHEMA_INCOMPATIBLE`. `UNKNOWN` reports `SCHEMA_UNKNOWN`. Under `MISMATCH`
reads continue to be served: a query that is genuinely incompatible fails on its
own terms rather than being pre-emptively blocked.

The remedy for `MISMATCH` is to upgrade horsies — workers apply migrations — or
to run the UI version that matches the stored schema. The remedy for `ABSENT` is
to start a horsies app or worker against that database. The remedy for `ZERO_X`
is a new database: this release does not upgrade a 0.x database, so drain it and
point the fleet and the monitoring app at a new one.

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

**Don't use the `AllowAll` policy to reach a dashboard you have not otherwise
guarded.** `AllowAll` asserts that the mount is already authenticated. It is
required to be explicit precisely so the choice is deliberate.

**Don't point the monitoring app at a database expecting it to create the
schema.** It reports `ABSENT` and stops. Start a horsies app or worker instead.

## Static assets

When the built SPA assets are missing, for example in a source checkout,
`/api/*` continues to work and `GET /` returns 503 with a detail that names the
build command. The Python path and message are in
[Static Assets](/horsies/monitoring/web-ui-mount/#static-assets).

Static assets are served without the auth dependency; they carry no data. Every
`/api/*` route is gated, so an unauthorized viewer receives the shell and then
the not-authorized state.
