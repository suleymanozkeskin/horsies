// The servers of the `web` command with a built addon (`build:dev`), a database and the real
// handler: it serves on Node and on Bun, and on Bun an idle event stream lives past one heartbeat
// of 15 s (Bun's default idle timeout is 10 s).

import assert from "node:assert/strict";
import { before, describe, test } from "node:test";

import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { allowAll } from "../../../horsies/src/web/auth.ts";
import { serveOnBun } from "../../../horsies/src/web/bun-server.ts";
import { createMonitoringHandler, EVENT_HEARTBEAT_MS } from "../../../horsies/src/web/handler.ts";
import { serveOnNode } from "../../../horsies/src/web/node-server.ts";
import type { HandlerFailureReport, RunningServer, ServeOutcome } from "../../../horsies/src/web/server.ts";
import { manifestJson, testDatabaseUrl } from "./manifest.ts";
import { probedHandle, testApp } from "./web-probe.ts";

/** Whether this process runs on Bun. */
const notOnBun = process.versions["bun"] === undefined;

/** The bound of the idle stream test: one heartbeat and a margin. */
const IDLE_STREAM_TEST_MS = EVENT_HEARTBEAT_MS + 10_000;

const noReport: HandlerFailureReport = () => undefined;

function serving(outcome: ServeOutcome): RunningServer {
  assert.equal(outcome.kind, "serving");

  return outcome.server;
}

describe("the servers of the web command with a built addon and a database", () => {
  before(async () => {
    const installer = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal((await installer.installSchema()).kind, "installed");
    await installer.close();
  });

  test("it serves the page on Node's server", async () => {
    const handler = createMonitoringHandler(testApp(probedHandle(manifestJson(testDatabaseUrl(), false))), {
      auth: allowAll(),
    });

    const server = serving(await serveOnNode(handler, { host: "127.0.0.1", port: 0 }, noReport));

    try {
      const response = await fetch(`http://127.0.0.1:${server.address.port}/`);

      assert.equal(response.status, 200);
      await response.text();
    } finally {
      await handler.close();
      await server.close();
    }
  });

  test("on Bun an idle event stream lives past one heartbeat", { skip: notOnBun, timeout: IDLE_STREAM_TEST_MS }, async () => {
    const handler = createMonitoringHandler(testApp(probedHandle(manifestJson(testDatabaseUrl(), false))), {
      auth: allowAll(),
    });

    const server = serving(await serveOnBun(handler, { host: "127.0.0.1", port: 0 }, noReport));

    try {
      const response = await fetch(`http://127.0.0.1:${server.address.port}/api/events`);
      const first = await response.body?.getReader().read();

      assert.equal(new TextDecoder().decode(first?.value), ": heartbeat\n\n");
    } finally {
      await handler.close();
      await server.close();
    }
  });
});
