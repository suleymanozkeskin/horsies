// The HTTP server of the `web` command on Bun: `Bun.serve` with the handler as its `fetch`.
//
// Bun closes a connection after its idle timeout (10 s by default); an event stream with no event
// sends one heartbeat frame per `EVENT_HEARTBEAT_MS` (15 s). The server sets an idle timeout of two
// heartbeats, so an idle event stream lives on.

import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { EVENT_HEARTBEAT_MS, INTERNAL_SERVER_ERROR_TEXT } from "./handler.ts";
import { SERVER_CLOSE_GRACE_MS } from "./server.ts";
import type { FetchHandler, HandlerFailureReport, RunningServer, ServeAddress, ServeOutcome } from "./server.ts";

/** The idle timeout of a connection, in whole seconds: two event stream heartbeats. */
export const BUN_IDLE_TIMEOUT_SECONDS: number = (2 * EVENT_HEARTBEAT_MS) / 1000;

/** The options of `Bun.serve` that the server sets. */
interface BunServeOptions {
  readonly hostname: string;
  readonly port: number;
  /** Seconds of no traffic before Bun closes a connection. */
  readonly idleTimeout: number;
  readonly fetch: (request: Request) => Promise<Response>;
}

/** The members of a Bun server that the server reads. */
interface BunServer {
  readonly port: number | undefined;
  readonly hostname: string | undefined;
  /** Stop new connections; `true` also ends the requests in flight. Resolves when all closed. */
  stop(closeActiveConnections?: boolean): Promise<void>;
}

/** The member of the `Bun` namespace that the server calls. */
interface BunNamespace {
  serve(options: BunServeOptions): BunServer;
}

/**
 * Serve `handler` on `address` with `Bun.serve`. An address that does not bind is `bindFailed`. A
 * call outside Bun is a defect of the package, which picks the server by the runtime.
 */
export async function serveOnBun(
  handler: FetchHandler,
  address: ServeAddress,
  report: HandlerFailureReport,
): Promise<ServeOutcome> {
  const bun = bunNamespace();
  let server: BunServer;

  try {
    server = bun.serve({
      hostname: address.host,
      port: address.port,
      idleTimeout: BUN_IDLE_TIMEOUT_SECONDS,
      fetch: async (request) => answerOnBun(handler, request, report),
    });
  } catch (cause) {
    return { kind: "bindFailed", address, cause: thrownFacts(cause).message };
  }

  return { kind: "serving", server: runningServer(server) };
}

/** The `Bun` namespace of this process; a defect outside Bun. */
function bunNamespace(): BunNamespace {
  const found: unknown = Reflect.get(globalThis, "Bun");

  if (found === undefined) {
    return raiseDefect(new ClientDefect({ subject: "serveOnBun", rule: "the Bun server runs only on Bun" }));
  }

  // SAFETY: on Bun, `globalThis.Bun` is the Bun namespace, whose `serve` takes these options and
  // gives a server with these members (bun-types 1.4, `Bun.serve`).
  return found as BunNamespace;
}

/** Answer one request through `handler`; a throw or a rejection is a reported 500. */
async function answerOnBun(handler: FetchHandler, request: Request, report: HandlerFailureReport): Promise<Response> {
  try {
    return await handler(request);
  } catch (cause) {
    report({ method: request.method, path: new URL(request.url).pathname, ...thrownFacts(cause) });

    return new Response(INTERNAL_SERVER_ERROR_TEXT, {
      status: 500,
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  }
}

/** The running server of `server`. A server on a host and a port has both; else a defect. */
function runningServer(server: BunServer): RunningServer {
  const port = server.port;
  const host = server.hostname;

  if (port === undefined || host === undefined) {
    return raiseDefect(new ClientDefect({ subject: "serveOnBun", rule: "a server on a host and a port has both" }));
  }

  return { address: { host, port }, close: async () => closeBunServer(server) };
}

/** How the wait for a stop ended. */
type StopWait = "stopped" | "graceEnded";

/** Stop new connections and wait for the requests in flight; past the grace, end them. */
async function closeBunServer(server: BunServer): Promise<undefined> {
  const { promise: graceEnded, resolve: endGrace } = Promise.withResolvers<StopWait>();
  const grace = setTimeout(() => endGrace("graceEnded"), SERVER_CLOSE_GRACE_MS);
  const stopping = server.stop(false).then((): StopWait => "stopped");
  const first = await Promise.race([stopping, graceEnded]);

  clearTimeout(grace);

  switch (first) {
    case "stopped":
      return undefined;
    case "graceEnded":
      await server.stop(true);

      return undefined;
    default:
      return unreachable(first);
  }
}
