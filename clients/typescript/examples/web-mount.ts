// The monitoring web API and page, mounted in the app's own HTTP server under a path prefix. The handler is a
// Fetch-API function (`(request: Request) => Promise<Response>`), so any server that speaks `Request` and `Response`
// can mount it: `Bun.serve` on Bun, and `node:http` with a small adapter on Node.
//
//   node examples/web-mount.ts serve          # http://127.0.0.1:8700/ and http://127.0.0.1:8700/ops/horsies/
//   node examples/web-mount.ts worker --profile default
//   node examples/web-mount.ts demo           # sends one task, so the page has a row
//
// The mount allows reads only (`viewOnly()`): put the app's own authentication in front of it before you allow
// actions. The `web` command of `cli.run` serves the same handler at the root, with no app server.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import { createMonitoringHandler, defineTask, h, Horsies, type MonitoringHandler, type Result, viewOnly } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  ISOLATED_PROFILE,
  printLine,
  readLine,
  refusalLines,
  RESULT_WAIT_MS,
  runExample,
  sendErrorText,
} from "./support.ts";

/** The command that serves the app's own server with the mount. */
const SERVE_COMMAND = "serve";

/** The address of the app's server: this machine only. */
const SERVE_HOST = "127.0.0.1";
const SERVE_PORT = 8700;

/** The path under which the app mounts the monitoring handler. */
const MOUNT_PATH = "/ops/horsies";

/**
 * Seconds of no traffic before Bun closes a connection: two heartbeats of a monitoring event stream (one each 15 s),
 * so an idle event stream stays open. Bun's default is 10 s.
 */
const BUN_IDLE_TIMEOUT_SECONDS = 30;

/** The page of the app's own root. */
const HOME_TEXT = `Acme operations. The monitoring page is at ${MOUNT_PATH}/\n`;

const ping = defineTask({ name: "ping", input: {}, output: h.string() });

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [ping],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(ping, async (_input, { ok }) => ok("pong"));

/** Where a request path goes in the app's server. */
type Route = "home" | "monitoring" | "notFound";

/** The route of `path`. The mount matches whole path segments only. Pure. */
function routeOf(path: string): Route {
  if (path === "/") {
    return "home";
  }

  return path === MOUNT_PATH || path.startsWith(`${MOUNT_PATH}/`) ? "monitoring" : "notFound";
}

/** The answer of the app's server to `request`. The monitoring handler answers every request under the mount. */
async function answer(request: Request, monitoring: MonitoringHandler): Promise<Response> {
  const route = routeOf(new URL(request.url).pathname);

  switch (route) {
    case "home":
      return new Response(HOME_TEXT, { headers: { "content-type": "text/plain; charset=utf-8" } });
    case "monitoring":
      return monitoring(request);
    case "notFound":
      return new Response("not found\n", { status: 404, headers: { "content-type": "text/plain; charset=utf-8" } });
    default: {
      const unexpected: never = route;

      throw new TypeError(`answer: a route outside its type: ${String(unexpected)}`);
    }
  }
}

/** A server that listens: its stop closes the listener and the connections. */
type RunningServer = { readonly stop: () => Promise<void> };

/** The members of `Bun.serve` that this example uses. */
interface BunNamespace {
  serve(options: {
    readonly hostname: string;
    readonly port: number;
    readonly idleTimeout: number;
    readonly fetch: (request: Request) => Promise<Response>;
  }): { stop(closeActiveConnections: boolean): Promise<void> };
}

/** The runtime of this process: Bun when `globalThis.Bun` exists, else Node. */
type ServerRuntime = { readonly kind: "bun"; readonly bun: BunNamespace } | { readonly kind: "node" };

function serverRuntime(): ServerRuntime {
  const bun: unknown = Reflect.get(globalThis, "Bun");

  // SAFETY: on Bun, `globalThis.Bun` is the Bun namespace, whose `serve` takes these options and gives a server with
  // this `stop` (bun-types 1.4, `Bun.serve`).
  return bun === undefined ? { kind: "node" } : { kind: "bun", bun: bun as BunNamespace };
}

/** Serve `fetch` with `Bun.serve`, or the bind failure. */
function serveOnBun(bun: BunNamespace, fetch: (request: Request) => Promise<Response>): Result<RunningServer, string> {
  try {
    const server = bun.serve({ hostname: SERVE_HOST, port: SERVE_PORT, idleTimeout: BUN_IDLE_TIMEOUT_SECONDS, fetch });

    return { ok: true, value: { stop: async () => server.stop(true) } };
  } catch (cause) {
    return { ok: false, error: cause instanceof Error ? cause.message : String(cause) };
  }
}

/** The Fetch request of a Node request. Its signal fires when the client leaves. */
function requestOf(incoming: IncomingMessage, signal: AbortSignal): Request {
  const headers = new Headers();

  for (let index = 0; index + 1 < incoming.rawHeaders.length; index += 2) {
    headers.append(incoming.rawHeaders[index] ?? "", incoming.rawHeaders[index + 1] ?? "");
  }

  const url = new URL(incoming.url ?? "/", `http://${SERVE_HOST}:${String(SERVE_PORT)}`);
  const method = incoming.method ?? "GET";
  const body: RequestInit = method === "GET" || method === "HEAD" ? {} : { body: Readable.toWeb(incoming), duplex: "half" };

  return new Request(url, { method, headers, signal, ...body });
}

/** Write a Fetch response to a Node response: the head at once (an event stream may wait for its first frame), then the body. */
async function writeResponse(response: Response, outgoing: ServerResponse): Promise<void> {
  const headerList: string[] = [];

  response.headers.forEach((value, name) => {
    headerList.push(name, value);
  });

  outgoing.writeHead(response.status, headerList);
  outgoing.flushHeaders();

  if (response.body === null) {
    outgoing.end();

    return;
  }

  // `pipeline` keeps the back pressure, and ends the body when the client leaves.
  await pipeline(Readable.fromWeb(response.body), outgoing);
}

/** Answer one Node request through `fetch`. A failure ends the connection and is written to stderr. */
async function answerOnNode(
  fetch: (request: Request) => Promise<Response>,
  incoming: IncomingMessage,
  outgoing: ServerResponse,
): Promise<void> {
  const leaving = new AbortController();

  outgoing.once("close", () => leaving.abort());

  try {
    await writeResponse(await fetch(requestOf(incoming, leaving.signal)), outgoing);
  } catch (cause) {
    // A client that leaves during an event stream ends the pipeline: that is not a failure of the server.
    if (!leaving.signal.aborted) {
      process.stderr.write(`web-mount: ${incoming.method ?? ""} ${incoming.url ?? ""}: ${String(cause)}\n`);
    }

    outgoing.destroy();
  }
}

/** Listen with `server`; the first `listening` or `error` event decides. */
async function listen(server: Server): Promise<Result<void, string>> {
  const { promise, resolve } = Promise.withResolvers<Result<void, string>>();

  server.once("error", (cause) => resolve({ ok: false, error: cause.message }));
  server.once("listening", () => resolve({ ok: true, value: undefined }));
  server.listen(SERVE_PORT, SERVE_HOST);

  return promise;
}

/** Serve `fetch` with `node:http`, or the bind failure. */
async function serveOnNode(fetch: (request: Request) => Promise<Response>): Promise<Result<RunningServer, string>> {
  const server = createServer((incoming, outgoing) => {
    void answerOnNode(fetch, incoming, outgoing);
  });

  const listening = await listen(server);

  if (!listening.ok) {
    return listening;
  }

  const stop = async (): Promise<void> => {
    const { promise, resolve } = Promise.withResolvers<void>();

    server.close(() => resolve());
    server.closeAllConnections();

    return promise;
  };

  return { ok: true, value: { stop } };
}

/** Wait for `SIGINT` or `SIGTERM`. */
async function stopSignal(): Promise<NodeJS.Signals> {
  const { promise, resolve } = Promise.withResolvers<NodeJS.Signals>();

  process.once("SIGINT", resolve);
  process.once("SIGTERM", resolve);

  return promise;
}

/** Serve the app's server until `SIGINT` or `SIGTERM`. Resolves with the exit code: 0 after a stop, 1 when the address does not bind. */
async function serve(): Promise<number> {
  const monitoring = createMonitoringHandler(app, { auth: viewOnly(), basePath: MOUNT_PATH });
  const fetch = async (request: Request): Promise<Response> => answer(request, monitoring);
  const runtime = serverRuntime();
  const running = runtime.kind === "bun" ? serveOnBun(runtime.bun, fetch) : await serveOnNode(fetch);

  if (!running.ok) {
    process.stderr.write(`web-mount: ${SERVE_HOST}:${String(SERVE_PORT)} does not bind: ${running.error}\n`);

    return 1;
  }

  printLine(`serving http://${SERVE_HOST}:${String(SERVE_PORT)}/ with the monitoring page at ${MOUNT_PATH}/`);

  const signal = await stopSignal();

  printLine(`${signal}: stopping`);
  // The event streams end first, so the server's stop does not wait for them.
  await monitoring.close();
  await running.value.stop();

  return 0;
}

async function demo(): Promise<Result<void, string>> {
  const sent = await app.send(ping, {});

  if (!sent.ok) {
    return demoFailed(`send ping: ${sendErrorText(sent.error)}`);
  }

  printLine(readLine("ping", await sent.value.get({ timeoutMs: RESULT_WAIT_MS }), (text) => text));

  return DEMO_DONE;
}

process.exitCode = process.argv[2] === SERVE_COMMAND ? await serve() : await runExample(app, demo);
