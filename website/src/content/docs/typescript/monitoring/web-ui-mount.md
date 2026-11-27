---
title: Web UI Mount
summary: Serve the monitoring web UI of an app with createMonitoringHandler, a Fetch-API handler for Bun.serve, node:http, Hono and Next.js routes, and decide access per request with a MonitoringAuthorizationPolicy.
related: [monitoring-queries, ../cli, ../../monitoring/web-ui-overview, ../../monitoring/web-ui-deployment, ../../monitoring/action-semantics]
tags: [typescript, monitoring, web, createMonitoringHandler, Bun.serve, node:http, Hono, Next.js, MonitoringAuthorizationPolicy, trustedHeader]
---

# Web UI Mount

`createMonitoringHandler(app, options)` returns the monitoring web UI of an app as a Fetch-API handler: a function `(request: Request) => Promise<Response>`. The host serves it with `Bun.serve`, `node:http`, Hono or a Next.js route. The core serves every route, guard, read cache and event of the UI, so the handler answers as the Python and Rust mounts do; the handler runs the access policy of the host. The surfaces of the UI are on [Web UI Overview](/horsies/monitoring/web-ui-overview/); the deployment modes, the policies and the schema contract are on [Web UI Deployment & Authentication](/horsies/monitoring/web-ui-deployment/).

The `web` command of `cli.run(app)` serves the handler of the app with no host server: see [CLI Reference](/horsies/typescript/cli/).

## Create the Handler

```ts
import { createMonitoringHandler, type Horsies, trustedHeader } from "horsies";

function serveMonitoring(app: Horsies): void {
  const policy = trustedHeader("X-Forwarded-User", { actions: "enabled" });

  if (!policy.ok) {
    console.error(`the header name is refused: ${policy.error.kind}`);

    return;
  }

  const monitoring = createMonitoringHandler(app, { authorizationPolicy: policy.value, basePath: "/monitoring" });

  const server = Bun.serve({
    port: 3000,
    // Two event stream heartbeats (15 s each): Bun closes an idle connection after 10 s by default.
    idleTimeout: 30,
    fetch: async (request) => {
      const { pathname } = new URL(request.url);

      if (pathname === "/monitoring" || pathname.startsWith("/monitoring/")) {
        return monitoring(request);
      }

      return new Response("Not found", { status: 404 });
    },
  });

  process.once("SIGTERM", () => {
    void monitoring.close().then(async () => server.stop());
  });
}
```

The handler serves every path of the mount: the page and its assets, `/api/...`, the event stream `GET /api/events`, and `GET /metrics`, the fleet metrics of the app's database behind the view verdict. The `queue` label values of `/metrics` are the queues of the app config. See [Fleet Collector](/horsies/observability/fleet-collector/).

- `createMonitoringHandler` does no I/O. The first request connects the app's handle. A request whose handle cannot be built is answered with the cause as `{"detail": ...}`: 503 when the cause may clear, 500 when it stays (for example a connection setting that the core refuses). The next request tries again.
- The handler runs no schema step. The schema gate of the core answers 409 `SCHEMA_INCOMPATIBLE`: see [Schema Handling](/horsies/monitoring/web-ui-deployment/#schema-handling).
- A workflow resume builds the sub-workflow children that it owes from the workflows of the app: the `workflows` list of `Horsies.create`. Create the handler from the app that lists every workflow whose runs the UI resumes.
- A request body is read up to the core's bound (1 MiB) plus one byte; a longer body is answered 413.
- Every expected outcome is a response. The handler throws only the process-state errors of the package (`CoreError`) and defects (`CoreDefect`): see [Task Errors](/horsies/typescript/tasks/errors/).
- `createMonitoringHandler` runs on the main thread of a process. In a `worker_threads` thread it throws `AddonOutsideMainThread`.

## MonitoringHandler

| Member | Does |
|--------|------|
| `(request: Request): Promise<Response>` | Answers one request |
| `close(): Promise<void>` | Ends every event stream of the handler after the degraded frame. A stream that opens later answers the degraded frame, then the end. The other routes still answer. Call it before the server stops |

An event stream sends a heartbeat frame after 15 s with no event, so that a proxy does not close it. Set the idle timeout of the server and of each proxy above 15 s.

## MonitoringHandlerOptions

| Field | Type | Default |
|-------|------|---------|
| `authorizationPolicy` | `MonitoringAuthorizationPolicy` | Required, with no default: a deployment chooses it |
| `actions` | `ActionsSwitch`: `"enabled"` or `"disabled"` | `"enabled"` |
| `ui` | `MonitoringUiOptions`: `customCssUrl?: string`, `traceLink?: TraceLinkTemplate` | No stylesheet; no trace link (the task detail shows the trace id as text) |
| `basePath` | `MountPathText`: `` `/${string}` `` | `"/"` |

- With `actions: "disabled"`, every action route answers 403 and the policy's `canAct` is not asked for it.
- `customCssUrl` names one stylesheet that the page loads after its own, so its CSS custom properties win. A lone surrogate in the text becomes U+FFFD. See [Custom CSS](/horsies/monitoring/web-ui-deployment/#custom-css).
- `basePath` is the path where the host mounts the handler, as it appears in the URL (percent-encoded). A trailing `/` is ignored. The page builds its links from it. A prefix matches whole path segments only: with `"/monitoring"`, `/monitoring` and `/monitoring/api/tasks` are inside the mount, and `/monitoringx` is outside. A path outside the mount is answered 404 `{"detail": "Not found."}`, and no policy runs.

### Trace Link

```ts
import { createMonitoringHandler, type Horsies, type MonitoringHandler, TraceLinkTemplate, viewOnly } from "horsies";

function withTraceLink(app: Horsies): MonitoringHandler | undefined {
  const traceLink = TraceLinkTemplate.parse("https://grafana.example.com/explore?traceId={trace_id}");

  if (!traceLink.ok) {
    console.error(traceLink.error.rule);

    return undefined;
  }

  return createMonitoringHandler(app, { authorizationPolicy: viewOnly(), ui: { traceLink: traceLink.value } });
}
```

`TraceLinkTemplate.parse(text)` returns `Result<TraceLinkTemplate, TraceLinkRefusal>`. The core checks the text: 1 to 2,048 bytes of printable ASCII with no space, an `http://` or `https://` scheme, a host, one `{trace_id}`, and no other brace. The page fills `{trace_id}` with the 32 hex digits of the trace id of a task. Each refusal has `kind`, `template` (the text) and `rule` (the core's text of the rule):

| `kind` | Extra field | The template |
|--------|-------------|--------------|
| `"length"` | `bytes`: its UTF-8 byte count | Has 0 bytes or more than 2,048 bytes |
| `"character"` | `byte`: the first such byte | Holds a space, a control or a non-ASCII byte |
| `"scheme"` | - | Does not start with `https://` or `http://` |
| `"noHost"` | - | Has no host after its scheme |
| `"placeholderCount"` | `count`: the times it holds `{trace_id}` | Does not hold `{trace_id}` once |
| `"strayBrace"` | - | Holds a `{` or `}` outside `{trace_id}` |

## Access Policy

| Policy | `canView` | `canAct` |
|--------|-----------|----------|
| `allowAll()` | Yes | Yes. The host owns authentication: mount the handler behind it |
| `viewOnly()` | Yes | No |
| `trustedHeader(headerName, { actions })` | The header is present, and no member between its commas is blank | With `actions: "enabled"`, the same check; with `"disabled"`, no |

`trustedHeader` returns `Result<MonitoringAuthorizationPolicy, TrustedHeaderRefused>`. It refuses a blank name (`{ kind: "blankHeaderName" }`) and a name that is not an HTTP token (`{ kind: "invalidHeaderName", headerName, rule }`). A request with the header two times, one of them blank, is refused: `Headers.get` joins the values with `", "`.

### Write a Policy

```ts
import type { MonitoringAuthorizationPolicy } from "horsies";

const adminHeader: MonitoringAuthorizationPolicy = {
  canView: (request) => request.headers.has("x-admin-user"),
  canAct: (request) => request.headers.get("x-admin-role") === "operator",
};
```

- `MonitoringAuthorizationPolicy` has `canView(request)` and `canAct(request)`. Each returns `boolean` or `Promise<boolean>`. Only an answer of `true` allows; every other answer refuses.
- The `request` of a verdict has the method, the URL, the headers and the signal of the incoming request, and no body: the core reads the body.
- A verdict that throws or rejects answers the request with 500 `Internal Server Error`, and writes one ERROR record of the `horsies.web` logger that names the route.
- The handler asks only the verdicts that the route needs, in this order: `canView` for every read route, the event stream, `/metrics` and `/api/meta`; then, when the view is allowed, `canAct` for an action route while actions are enabled, and for `/api/meta`, which reports it. The page and its assets ask nothing.
- An action route also needs the header `X-Horsies-Intent: action`. It guards against cross-site requests; it is not authentication.

The answers to a refused request are on [Authorization Policies](/horsies/monitoring/web-ui-deployment/#authorization-policies).

## Mount in node:http

`node:http` gives an `IncomingMessage` and a `ServerResponse`. An adapter turns them into a `Request` and writes the `Response`:

<!-- ts-check: alone -->
```ts
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { pipeline } from "node:stream/promises";

import { createMonitoringHandler, type Horsies, type MonitoringHandler, viewOnly } from "horsies";

/** The core's request body bound (1 MiB) plus one byte: the handler answers a longer body with 413. */
const BODY_READ_LIMIT_BYTES = 1_048_577;

/** The body of `incoming`, at most `BODY_READ_LIMIT_BYTES` and one chunk; the rest is read and dropped. */
async function bodyOf(incoming: IncomingMessage): Promise<Uint8Array<ArrayBuffer>> {
  const chunks: Buffer[] = [];
  let size = 0;

  for await (const part of incoming) {
    const chunk: Buffer = part;

    if (size < BODY_READ_LIMIT_BYTES) {
      chunks.push(chunk);
    }

    size += chunk.length;
  }

  return new Uint8Array(Buffer.concat(chunks));
}

async function requestOf(incoming: IncomingMessage, signal: AbortSignal): Promise<Request> {
  const headers = new Headers();

  for (let index = 0; index + 1 < incoming.rawHeaders.length; index += 2) {
    headers.append(incoming.rawHeaders[index] ?? "", incoming.rawHeaders[index + 1] ?? "");
  }

  const method = incoming.method ?? "GET";
  const url = new URL(incoming.url ?? "/", "http://127.0.0.1:8600");

  switch (method) {
    case "GET":
    case "HEAD":
      return new Request(url, { method, headers, signal });
    default:
      return new Request(url, { method, headers, signal, body: await bodyOf(incoming) });
  }
}

async function answer(monitoring: MonitoringHandler, incoming: IncomingMessage, outgoing: ServerResponse): Promise<void> {
  const leaving = new AbortController();

  outgoing.once("close", () => leaving.abort());

  const response = await monitoring(await requestOf(incoming, leaving.signal));

  outgoing.writeHead(response.status, Object.fromEntries(response.headers));
  // An event stream can send no frame for a while: the client gets the head now.
  outgoing.flushHeaders();

  if (response.body === null) {
    outgoing.end();

    return;
  }

  await pipeline(response.body, outgoing);
}

function serveOnNode(app: Horsies): void {
  const monitoring = createMonitoringHandler(app, { authorizationPolicy: viewOnly() });

  const server = createServer((incoming, outgoing) => {
    answer(monitoring, incoming, outgoing).catch((cause: unknown) => {
      console.error("the monitoring request ended early", cause);
      outgoing.destroy();
    });
  });

  server.listen(8600, "127.0.0.1");
}
```

- The abort of `leaving` when the client leaves aborts the request's signal: the handler then ends the event stream and drops its subscription.
- The monitoring routes take small JSON bodies. `bodyOf` keeps at most the core's bound plus one byte, so a longer body gets the 413 of the core and the memory of one request stays bounded.
- `pipeline` writes the response body with back pressure, so a long event stream holds one frame at a time. A client that leaves an event stream also ends `pipeline` with an error.
- The handler answers at the root (`basePath` `"/"`) here. With a prefix, give the same prefix as `basePath`.

## Mount in Hono

Route the mount path and every path under it to the handler, with the same prefix as `basePath`. `context.req.raw` is the Fetch `Request`:

<!-- ts-check: skip partial -- hono is not a dependency of the docs check -->
```ts
import { Hono } from "hono";
import { createMonitoringHandler, type Horsies, viewOnly } from "horsies";

export function honoServer(app: Horsies): Hono {
  const monitoring = createMonitoringHandler(app, { authorizationPolicy: viewOnly(), basePath: "/monitoring" });
  const server = new Hono();

  server.all("/monitoring", async (context) => monitoring(context.req.raw));
  server.all("/monitoring/*", async (context) => monitoring(context.req.raw));

  return server;
}
```

## Mount in a Next.js Route

An optional catch-all route handler (`app/monitoring/[[...path]]/route.ts`) answers `/monitoring` and every path under it. The addon is a Node-API addon: the route runs on the Node.js runtime, not on the edge runtime.

<!-- ts-check: skip partial -- next is not a dependency of the docs check, and the app module is the user's own -->
```ts
import { createMonitoringHandler, viewOnly } from "horsies";

import { app } from "@/horsies/app";

export const runtime = "nodejs";

const monitoring = createMonitoringHandler(app, { authorizationPolicy: viewOnly(), basePath: "/monitoring" });

export async function GET(request: Request): Promise<Response> {
  return monitoring(request);
}

export async function HEAD(request: Request): Promise<Response> {
  return monitoring(request);
}

export async function POST(request: Request): Promise<Response> {
  return monitoring(request);
}
```

The module `@/horsies/app` builds the app with `Horsies.create` and exports it. See [App Config](/horsies/typescript/configuration/app-config/).

## Things to Avoid

**Do not mount `allowAll()` without the host's own authentication.** It allows every read and every action.

**Do not trust an identity header that the reverse proxy passes through.** `trustedHeader` is safe only when the proxy strips or overwrites the header on each incoming request. The package cannot detect a forwarded header that the client set.

**Do not keep the default idle timeout of `Bun.serve`.** Bun closes a connection after 10 s with no traffic, and the event stream sends its heartbeat after 15 s. Set `idleTimeout` to 30 or more.

**Do not give a `basePath` that differs from the path where the host routes the handler.** A request outside `basePath` is answered 404, and the page builds its links from `basePath`.

**Do not point the handler at a database to create the schema.** The handler runs no schema step. Start a worker or send a task from the app to install it.
