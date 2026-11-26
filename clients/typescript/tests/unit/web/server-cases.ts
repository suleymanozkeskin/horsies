// The cases that the Node server and the Bun server both pass, with a fake handler on an ephemeral
// port of 127.0.0.1. Each case closes its server.

import assert from "node:assert/strict";
import { createServer } from "node:net";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import type {
  FetchHandler,
  HandlerFailure,
  HandlerFailureReport,
  RunningServer,
  ServeAddress,
  ServeOutcome,
} from "../../../horsies/src/web/server.ts";

/** A server start of one runtime. */
export type ServeOn = (handler: FetchHandler, address: ServeAddress, report: HandlerFailureReport) => Promise<ServeOutcome>;

/** The loopback address of the tests, on a free port. */
const LOOPBACK: ServeAddress = { host: "127.0.0.1", port: 0 };

/** Start `handler` with `serveOn` on a free loopback port. */
async function started(serveOn: ServeOn, handler: FetchHandler, failures: HandlerFailure[] = []): Promise<RunningServer> {
  const outcome = await serveOn(handler, LOOPBACK, (failure) => {
    failures.push(failure);

    return undefined;
  });

  assert.equal(outcome.kind, "serving");

  return outcome.server;
}

/** The URL of `path` on `server`. */
function urlOf(server: RunningServer, path: string): string {
  return `http://127.0.0.1:${server.address.port}${path}`;
}

/** Hold a TCP port of 127.0.0.1; the returned close frees it. */
async function heldPort(): Promise<{ readonly port: number; readonly close: () => Promise<void> }> {
  const holder = createServer();
  const { promise, resolve } = Promise.withResolvers<undefined>();

  holder.listen({ host: "127.0.0.1", port: 0 }, () => resolve(undefined));
  await promise;

  // SAFETY: a TCP server that listens on a host and a port has an `AddressInfo` address.
  const port = (holder.address() as AddressInfo).port;

  return {
    port,
    close: async () => {
      const closed = Promise.withResolvers<undefined>();

      holder.close(() => closed.resolve(undefined));
      await closed.promise;
    },
  };
}

/** Register the shared cases for the server of `serveOn`; each is skipped when `skip` is set. */
export function serverCases(serveOn: ServeOn, skip: boolean): void {
  test("answers with the status, the headers and the body of the handler", { skip }, async () => {
    // The facts of the request, read while the handler runs: Bun reads a request of `Bun.serve`
    // only during its answer.
    const seen: { readonly url: string; readonly method: string; readonly intent: string | null }[] = [];

    const server = await started(serveOn, async (request) => {
      seen.push({ url: request.url, method: request.method, intent: request.headers.get("x-horsies-intent") });

      return new Response("hello", { status: 201, headers: { "x-test": "yes" } });
    });

    try {
      const response = await fetch(urlOf(server, "/api/tasks?limit=2"), { headers: { "X-Horsies-Intent": "action" } });

      assert.equal(response.status, 201);
      assert.equal(response.headers.get("x-test"), "yes");
      assert.equal(await response.text(), "hello");

      const url = new URL(seen[0]?.url ?? "http://none");

      assert.equal(url.pathname, "/api/tasks");
      assert.equal(url.search, "?limit=2");
      assert.equal(seen[0]?.method, "GET");
      assert.equal(seen[0]?.intent, "action");
    } finally {
      await server.close();
    }
  });

  test("a POST body reaches the handler", { skip }, async () => {
    const server = await started(serveOn, async (request) => new Response(`got ${await request.text()}`));

    try {
      const response = await fetch(urlOf(server, "/api/tasks/x/cancel"), { method: "POST", body: "{\"a\":1}" });

      assert.equal(await response.text(), "got {\"a\":1}");
    } finally {
      await server.close();
    }
  });

  test("a handler that throws answers 500 and is reported", { skip }, async () => {
    const failures: HandlerFailure[] = [];

    const server = await started(
      serveOn,
      async () => Promise.reject(new RangeError("planned failure")),
      failures,
    );

    try {
      const response = await fetch(urlOf(server, "/api/tasks"));

      assert.equal(response.status, 500);
      assert.equal(await response.text(), "Internal Server Error");
      assert.deepEqual(failures, [{ method: "GET", path: "/api/tasks", name: "RangeError", message: "planned failure" }]);
    } finally {
      await server.close();
    }
  });

  test("a stream body reaches the client before the stream ends", { skip }, async () => {
    const release = Promise.withResolvers<undefined>();

    const server = await started(serveOn, async () => {
      const body = new ReadableStream<Uint8Array>({
        async pull(controller) {
          controller.enqueue(new TextEncoder().encode(": heartbeat\n\n"));
          await release.promise;
          controller.close();
        },
      });

      return new Response(body, { headers: { "content-type": "text/event-stream" } });
    });

    try {
      const response = await fetch(urlOf(server, "/api/events"));
      const reader = response.body?.getReader();
      const first = await reader?.read();

      assert.equal(new TextDecoder().decode(first?.value), ": heartbeat\n\n");
      release.resolve(undefined);
      assert.equal((await reader?.read())?.done, true);
    } finally {
      release.resolve(undefined);
      await server.close();
    }
  });

  test("a client that leaves aborts the request signal", { skip }, async () => {
    const aborted = Promise.withResolvers<undefined>();
    const answered = Promise.withResolvers<undefined>();

    const server = await started(serveOn, async (request) => {
      request.signal.addEventListener("abort", () => aborted.resolve(undefined), { once: true });
      answered.resolve(undefined);

      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(": open\n\n"));
        },
      });

      return new Response(body, { headers: { "content-type": "text/event-stream" } });
    });

    try {
      const leaving = new AbortController();
      const response = await fetch(urlOf(server, "/api/events"), { signal: leaving.signal });

      await answered.promise;
      await response.body?.getReader().read();
      leaving.abort();
      await aborted.promise;
    } finally {
      await server.close();
    }
  });

  test("an address in use is bindFailed with the cause", { skip }, async () => {
    const held = await heldPort();

    try {
      const outcome = await serveOn(async () => new Response("never"), { host: "127.0.0.1", port: held.port }, () => undefined);

      const kind = outcome.kind;

      if (outcome.kind === "serving") {
        await outcome.server.close();
      }

      assert.equal(kind, "bindFailed");
    } finally {
      await held.close();
    }
  });

  test("close resolves once the server ended", { skip }, async () => {
    const server = await started(serveOn, async () => new Response("ok"));

    await (await fetch(urlOf(server, "/"))).text();
    await server.close();
    await assert.rejects(fetch(urlOf(server, "/")));
  });
}
