// The HTTP server of the `web` command on Node: `node:http` with a small adapter between its
// request and response and the Fetch `Request` and `Response`. No framework.
//
// - The request body streams to the handler; the handler reads what it needs.
// - The response body streams to the client with back pressure, so a long event stream holds one
//   frame at a time.
// - When the client leaves, the request's signal aborts and the response body is cancelled.
// - A handler that throws or rejects is answered with 500 and reported.

import { createServer } from "node:http";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Readable } from "node:stream";

import { unreachable } from "../native/call/never.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { INTERNAL_SERVER_ERROR_TEXT } from "./handler.ts";
import { addressText, SERVER_CLOSE_GRACE_MS } from "./server.ts";
import type {
  BoundAddress,
  FetchHandler,
  HandlerFailureReport,
  RunningServer,
  ServeAddress,
  ServeOutcome,
} from "./server.ts";

/** Serve `handler` on `address` with `node:http`. An address that does not bind is `bindFailed`. */
export async function serveOnNode(
  handler: FetchHandler,
  address: ServeAddress,
  report: HandlerFailureReport,
): Promise<ServeOutcome> {
  const origin = `http://${addressText(address)}`;

  const server = createServer((incoming, outgoing) => {
    answerOnNode(handler, origin, incoming, outgoing, report).then(noAnswer, (cause: unknown) => {
      report({ method: incoming.method ?? "", path: incoming.url ?? "", ...thrownFacts(cause) });
      outgoing.destroy();
    });
  });

  const listening = await listen(server, address);

  switch (listening.kind) {
    case "failed":
      return { kind: "bindFailed", address, cause: listening.cause };
    case "listening":
      return { kind: "serving", server: runningServer(server, listening.address) };
    default:
      return unreachable(listening);
  }
}

/** How a listen ended. */
type Listening =
  | { readonly kind: "listening"; readonly address: BoundAddress }
  | { readonly kind: "failed"; readonly cause: string };

/** Listen on `address`; the first `listening` or `error` event decides. */
async function listen(server: Server, address: ServeAddress): Promise<Listening> {
  const { promise, resolve } = Promise.withResolvers<Listening>();

  const onError = (cause: Error): void => {
    server.off("listening", onListening);
    resolve({ kind: "failed", cause: cause.message });
  };

  const onListening = (): void => {
    server.off("error", onError);
    // SAFETY: a server that listens on a host and a port has an `AddressInfo` address (node:net).
    const bound = server.address() as AddressInfo;

    resolve({ kind: "listening", address: { host: bound.address, port: bound.port } });
  };

  server.once("error", onError);
  server.once("listening", onListening);
  server.listen({ host: address.host, port: address.port });

  return promise;
}

/** The running server of `server`. */
function runningServer(server: Server, address: BoundAddress): RunningServer {
  return {
    address,
    close: async () => closeNodeServer(server),
  };
}

/** Stop new connections, close idle ones, and end the rest after the grace. */
async function closeNodeServer(server: Server): Promise<undefined> {
  const { promise, resolve } = Promise.withResolvers<undefined>();
  const grace = setTimeout(() => server.closeAllConnections(), SERVER_CLOSE_GRACE_MS);

  server.close(() => {
    clearTimeout(grace);
    resolve(undefined);
  });
  server.closeIdleConnections();

  return promise;
}

/** Answer one request through `handler`. */
async function answerOnNode(
  handler: FetchHandler,
  origin: string,
  incoming: IncomingMessage,
  outgoing: ServerResponse,
  report: HandlerFailureReport,
): Promise<undefined> {
  const leaving = new AbortController();

  // `close` also comes after a response that ended: the abort then changes nothing.
  outgoing.once("close", () => leaving.abort());
  // A write to a connection that the client closed reports here; the body pump stops at `close`.
  outgoing.on("error", noAnswer);

  const request = requestOf(incoming, origin, leaving.signal);

  switch (request.kind) {
    case "refused":
      return writeText(outgoing, 400, `The request is not valid: ${request.cause}`);
    case "request":
      break;
    default:
      return unreachable(request);
  }

  let response: Response;

  try {
    response = await handler(request.request);
  } catch (cause) {
    report({ method: request.request.method, path: incoming.url ?? "", ...thrownFacts(cause) });

    return writeText(outgoing, 500, INTERNAL_SERVER_ERROR_TEXT);
  }

  return writeResponse(response, outgoing, report, request.request);
}

/** The Fetch request of `incoming`, or the cause that the Fetch API refused it. */
type NodeRequest =
  | { readonly kind: "request"; readonly request: Request }
  | { readonly kind: "refused"; readonly cause: string };

/** The Fetch request of `incoming`, with `signal` as its signal. */
function requestOf(incoming: IncomingMessage, origin: string, signal: AbortSignal): NodeRequest {
  const target = incoming.url ?? "/";
  const url = URL.parse(target, origin);

  if (url === null) {
    return { kind: "refused", cause: `the request target ${JSON.stringify(target)} is not a URL` };
  }

  const method = incoming.method ?? "GET";

  try {
    const headers = new Headers();

    for (let index = 0; index + 1 < incoming.rawHeaders.length; index += 2) {
      headers.append(incoming.rawHeaders[index] ?? "", incoming.rawHeaders[index + 1] ?? "");
    }

    return { kind: "request", request: new Request(url.href, { method, headers, signal, ...bodyOf(method, incoming) }) };
  } catch (cause) {
    return { kind: "refused", cause: thrownFacts(cause).message };
  }
}

/** The body of a request init: none for `GET` and `HEAD`, else the incoming stream. */
function bodyOf(method: string, incoming: IncomingMessage): RequestInit {
  switch (method) {
    case "GET":
    case "HEAD":
      return {};
    default:
      return { body: Readable.toWeb(incoming), duplex: "half" };
  }
}

/** Write `response` to `outgoing`: the head at once, then the body as it comes. */
async function writeResponse(
  response: Response,
  outgoing: ServerResponse,
  report: HandlerFailureReport,
  request: Request,
): Promise<undefined> {
  const headerList: string[] = [];

  response.headers.forEach((value, name) => {
    headerList.push(name, value);
  });

  outgoing.writeHead(response.status, headerList);
  // An event stream may send no frame for a while: the client gets the head now.
  outgoing.flushHeaders();

  const body = response.body;

  if (body === null) {
    outgoing.end();

    return undefined;
  }

  const pumped = await pumpBody(body, outgoing);

  switch (pumped.kind) {
    case "ended":
      return undefined;
    case "bodyFailed":
      report({ method: request.method, path: new URL(request.url).pathname, ...pumped.cause });
      outgoing.destroy();

      return undefined;
    default:
      return unreachable(pumped);
  }
}

/** How a body pump ended. */
type Pumped = { readonly kind: "ended" } | { readonly kind: "bodyFailed"; readonly cause: { name: string; message: string } };

/**
 * Copy `body` to `outgoing`, waiting for `drain` when the socket buffer is full. A close of the
 * connection cancels the body. The copy ends at the end of the body or at the close.
 */
async function pumpBody(body: ReadableStream<Uint8Array>, outgoing: ServerResponse): Promise<Pumped> {
  const reader = body.getReader();

  const cancelOnClose = (): void => {
    reader.cancel().then(noAnswer, noAnswer);
  };

  outgoing.once("close", cancelOnClose);

  try {
    while (!outgoing.destroyed) {
      const read = await reader.read();

      if (read.done) {
        break;
      }

      if (!outgoing.write(read.value)) {
        await drainedOrClosed(outgoing);
      }
    }
  } catch (cause) {
    return { kind: "bodyFailed", cause: thrownFacts(cause) };
  } finally {
    outgoing.off("close", cancelOnClose);
    reader.releaseLock();
  }

  if (!outgoing.destroyed) {
    outgoing.end();
  }

  return { kind: "ended" };
}

/** Wait until `outgoing` takes more data or its connection closes. */
async function drainedOrClosed(outgoing: ServerResponse): Promise<undefined> {
  const { promise, resolve } = Promise.withResolvers<undefined>();

  const done = (): void => {
    outgoing.off("drain", done);
    outgoing.off("close", done);
    resolve(undefined);
  };

  outgoing.once("drain", done);
  outgoing.once("close", done);

  return promise;
}

/** Answer `status` with a plain text body. */
function writeText(outgoing: ServerResponse, status: number, text: string): undefined {
  outgoing.writeHead(status, { "content-type": "text/plain; charset=utf-8" });
  outgoing.end(text);

  return undefined;
}

/** The explicit discard of an outcome that needs no answer. */
function noAnswer(): undefined {
  return undefined;
}
