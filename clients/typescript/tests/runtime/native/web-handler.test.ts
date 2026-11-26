// The monitoring handler with a built addon (`build:dev`) and a database: the handler of an app of
// `Horsies.create`, the page, the access needs, the refusals and the body bound in the core,
// `/metrics`, the event stream with a short test heartbeat, `close()`, the client's abort, the
// degraded frame of a broker with no listener, the refusals of the build, and the trace link
// refusals of the core, one per rule.

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";

import { Horsies } from "../../../horsies/src/index.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { buildCoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { allowAll, viewOnly } from "../../../horsies/src/web/auth.ts";
import type { MonitoringAuthorizationPolicy } from "../../../horsies/src/web/auth.ts";
import { nativeMonitoringBridge } from "../../../horsies/src/web/bridge.ts";
import { clientVersion } from "../../../horsies/src/web/client-version.ts";
import {
  createMonitoringHandler,
  handlerSettings,
  monitoringHandlerOf,
  monitoringHandlerOver,
} from "../../../horsies/src/web/handler.ts";
import type { MonitoringHandler, MonitoringHandlerOptions } from "../../../horsies/src/web/handler.ts";
import { parseTraceLink } from "../../../horsies/src/web/trace-link.ts";
import type { TraceLinkRefusal } from "../../../horsies/src/web/trace-link.ts";
import { manifestJson, manifestJsonWithSchema, testDatabaseUrl } from "./manifest.ts";
import { probedHandle, testApp } from "./web-probe.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The heartbeat of the event stream tests. */
const TEST_HEARTBEAT_MS = 200;

/** The most frames that a stream test reads before it gives up. */
const MAX_FRAMES_READ = 20;

/** The core's request body bound: 1 MiB. */
const MAX_REQUEST_BODY_BYTES = 1024 * 1024;

/** A database URL whose connect is refused at once: nothing listens on port 1. */
const REFUSED_DATABASE_URL = "postgresql://horsies@127.0.0.1:1/none";

const TASK_ID = "01890a5d-ac96-774b-bcce-b302099a8057";

const decoder = new TextDecoder();

/** A handler over a probed handle of `manifest`, with the short test heartbeat. */
function shortHeartbeatHandler(manifest: string, options: MonitoringHandlerOptions): MonitoringHandler {
  const app = testApp(probedHandle(manifest));

  return monitoringHandlerOver(
    nativeMonitoringBridge(app.coreHandle, handlerSettings(app, options, TEST_HEARTBEAT_MS)),
    options,
  );
}

/** Read frames of `response` until one equals `wanted`; the frames read, or a failure past the bound. */
async function framesUntil(response: Response, wanted: string): Promise<string[]> {
  const reader = response.body?.getReader();
  const frames: string[] = [];

  assert.ok(reader !== undefined, "the event stream has a body");

  while (frames.length < MAX_FRAMES_READ) {
    const read = await reader.read();

    if (read.done) {
      return frames;
    }

    frames.push(decoder.decode(read.value));

    if (frames.at(-1) === wanted) {
      reader.releaseLock();

      return frames;
    }
  }

  return assert.fail(`no ${JSON.stringify(wanted)} in ${MAX_FRAMES_READ} frames: ${frames.join("|")}`);
}

/** An app of the test database with no task, or the failed test with every refusal. */
function appOfTestDatabase(): Horsies {
  const created = Horsies.create({ broker: { databaseUrl: { kind: "value", value: testDatabaseUrl() } }, tasks: [] });

  if (!created.ok) {
    assert.fail(`Horsies.create refused: ${created.error.refusals.map((refusal) => refusal.message).join("; ")}`);
  }

  return created.value;
}

/** A request to the test origin. */
function request(path: string, init: RequestInit = {}): Request {
  return new Request(`http://monitoring.test${path}`, init);
}

describe("the monitoring handler with a built addon and a database", () => {
  let handler: MonitoringHandler;

  before(async () => {
    const installer = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.equal((await installer.installSchema()).kind, "installed");
    await installer.close();
    handler = monitoringHandlerOf(testApp(probedHandle(manifestJson(testDatabaseUrl(), false))), {
      authorizationPolicy: allowAll(),
    });
  });

  after(async () => {
    await handler.close();
  });

  test("the page answers HTML", async () => {
    const response = await handler(request("/"));

    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type") ?? "", /text\/html/u);
  });

  test("/api/meta reports the client version and the verdict of canAct", async () => {
    const response = await handler(request("/api/meta"));
    const meta: unknown = await response.json();

    assert.equal(response.status, 200);
    assert.ok(meta instanceof Object);
    assert.equal(Reflect.get(meta, "horsies_version"), "0.0.0-test");
    assert.equal(Reflect.get(meta, "can_act"), true);
  });

  // The core refuses to build a handler over a handle with no child spec source; `Horsies.create` registers the app's
  // source on the app's one handle, so the handler of an app builds and answers.
  test("the handler of an app of Horsies.create answers with the app's handle and the package version", async () => {
    const app = createMonitoringHandler(appOfTestDatabase(), { authorizationPolicy: allowAll() });

    try {
      const response = await app(request("/api/meta"));
      const meta: unknown = await response.json();

      assert.equal(response.status, 200);
      assert.ok(meta instanceof Object);
      assert.equal(Reflect.get(meta, "horsies_version"), clientVersion());
      assert.equal((await app(request("/api/tasks"))).status, 200);
    } finally {
      await app.close();
    }
  });

  test("a refused view of a read route is 403", async () => {
    const refusing: MonitoringAuthorizationPolicy = { canView: () => false, canAct: () => false };
    const refused = monitoringHandlerOf(testApp(probedHandle(manifestJson(testDatabaseUrl(), false))), { authorizationPolicy: refusing });

    assert.equal((await refused(request("/api/tasks"))).status, 403);
  });

  test("a body of 1 MiB + 1 byte is 413", async () => {
    const response = await handler(
      request(`/api/tasks/${TASK_ID}/cancel`, {
        method: "POST",
        headers: { "X-Horsies-Intent": "action", "Content-Type": "application/json" },
        body: new Uint8Array(MAX_REQUEST_BODY_BYTES + 1),
      }),
    );

    assert.equal(response.status, 413);
  });

  test("/metrics answers the fleet metrics after an allowed view", async () => {
    const response = await handler(request("/metrics"));

    assert.equal(response.status, 200);
    assert.match(await response.text(), /horsies_/u);
  });

  test("a mount under a prefix answers the page links under the prefix", async () => {
    const mounted = monitoringHandlerOf(testApp(probedHandle(manifestJson(testDatabaseUrl(), false))), {
      authorizationPolicy: viewOnly(),
      basePath: "/ops/monitoring",
    });

    const page = await mounted(request("/ops/monitoring"));

    assert.equal(page.status, 200);
    assert.match(await page.text(), /<base href="\/ops\/monitoring\/">/u);
    assert.equal((await mounted(request("/monitoring"))).status, 404);
  });
});

describe("the event stream with a built addon and a database", () => {
  test("a quiet stream sends the heartbeat frame after the short test heartbeat", async () => {
    const handler = shortHeartbeatHandler(manifestJson(testDatabaseUrl(), false), { authorizationPolicy: allowAll() });
    const response = await handler(request("/api/events"));

    assert.equal(response.headers.get("content-type"), "text/event-stream; charset=utf-8");
    assert.ok((await framesUntil(response, ": heartbeat\n\n")).length >= 1);
    await response.body?.cancel();
    await handler.close();
  });

  test("close() ends an open stream after the degraded frame", async () => {
    const handler = shortHeartbeatHandler(manifestJson(testDatabaseUrl(), false), { authorizationPolicy: allowAll() });
    const response = await handler(request("/api/events"));

    await framesUntil(response, ": heartbeat\n\n");
    await handler.close();

    const after = await framesUntil(response, 'data: {"topic": "degraded"}\n\n');

    assert.equal(after.at(-1), 'data: {"topic": "degraded"}\n\n');
  });

  test("the client's abort ends the stream", async () => {
    const handler = shortHeartbeatHandler(manifestJson(testDatabaseUrl(), false), { authorizationPolicy: allowAll() });
    const leaving = new AbortController();
    const response = await handler(request("/api/events", { signal: leaving.signal }));
    const reader = response.body?.getReader();

    leaving.abort();

    let read = await reader?.read();

    for (let frames = 0; frames < MAX_FRAMES_READ && read?.done === false; frames += 1) {
      read = await reader?.read();
    }

    assert.equal(read?.done, true);
    await handler.close();
  });

  test("a broker with no listener answers the degraded frame, then the end", async () => {
    const handler = shortHeartbeatHandler(
      manifestJsonWithSchema(testDatabaseUrl(), false, "ASSUME_INITIALIZED"),
      { authorizationPolicy: allowAll() },
    );

    const response = await handler(request("/api/events"));

    assert.equal(await response.text(), 'data: {"topic": "degraded"}\n\n');
    await handler.close();
  });
});

describe("the refusals of the build and of the broker", () => {
  test("a handle with no child spec source is refused with ClientDefect", () => {
    const handle = buildCoreHandle(manifestJson(testDatabaseUrl(), false), "PRODUCER");

    assert.throws(() => monitoringHandlerOf(testApp(handle), { authorizationPolicy: allowAll() }), { name: "ClientDefect" });
  });

  // The core builds the broker with no connect (each pool connects at its first checkout), so a database that does
  // not answer is the core's read failure, not a handler with no broker.
  test("a broker that cannot connect answers 503 with the cause", async () => {
    const handler = monitoringHandlerOf(testApp(probedHandle(manifestJson(REFUSED_DATABASE_URL, false))), {
      authorizationPolicy: allowAll(),
    });

    const response = await handler(request("/api/tasks"));
    const body: unknown = await response.json();

    assert.equal(response.status, 503);
    assert.ok(body instanceof Object);
    const detail = String(Reflect.get(body, "detail"));
    assert.match(detail, /^Task list query failed: /u);
    assert.ok(detail.includes(`could not open a connection: connecting to ${REFUSED_DATABASE_URL} failed`), detail);
  });

  test("the core accepts a trace link template", () => {
    assert.equal(parseTraceLink("https://grafana.test/explore?traceId={trace_id}").ok, true);
  });
});

describe("the trace link refusals of the core", () => {
  /** One template per rule of the core, and the refusal of the core. */
  const refusals: readonly TraceLinkRefusal[] = [
    { kind: "length", bytes: 0, template: "", rule: "the template has 0 bytes; a template has 1 to 2048 bytes" },
    {
      kind: "character",
      byte: 0x20,
      template: "https://grafana.test/a b/{trace_id}",
      rule: "the template holds the byte 0x20; a template holds printable ASCII with no space (0x21 to 0x7e)",
    },
    {
      kind: "scheme",
      template: "ftp://grafana.test/{trace_id}",
      rule: "the template does not start with https:// or http://",
    },
    { kind: "noHost", template: "https:///trace/{trace_id}", rule: "the template has no host after its scheme" },
    {
      kind: "placeholderCount",
      count: 0,
      template: "https://grafana.test/explore",
      rule: "the template holds {trace_id} 0 times; it holds it once",
    },
    {
      kind: "strayBrace",
      template: "https://grafana.test/{span_id}/{trace_id}",
      rule: "the template holds a { or } outside {trace_id}",
    },
  ];

  for (const refusal of refusals) {
    test(`the core refuses with ${refusal.kind}, its facts and the rule`, () => {
      assert.deepEqual(parseTraceLink(refusal.template), { ok: false, error: refusal });
    });
  }
});
