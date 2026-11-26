// The web handler over a fake core handler: the mount, the policy calls and their order, the
// refusals, the body bound, a policy that throws, and the response.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import type { CoreHandle } from "../../../horsies/src/native/handle/core-handle.ts";
import { allowAll, viewOnly } from "../../../horsies/src/web/auth.ts";
import type { MonitoringAuthorizationPolicy } from "../../../horsies/src/web/auth.ts";
import {
  EVENT_HEARTBEAT_MS,
  INTENT_HEADER,
  INTENT_VALUE,
  INTERNAL_SERVER_ERROR_TEXT,
  handlerSettings,
  monitoringHandlerOver,
  mountOf,
  pathInsideMount,
  responseOf,
} from "../../../horsies/src/web/handler.ts";
import type { MonitoringHandlerApp, MonitoringHandlerOptions } from "../../../horsies/src/web/handler.ts";
import { traceLinkOf } from "../../../horsies/src/web/trace-link.ts";
import { MAX_REQUEST_BODY_BYTES, fakeBridge, recordingPolicy } from "./support.ts";

const TASK_ID = "01890a5d-ac96-774b-bcce-b302099a8057";

/** A request to the test origin. */
function request(path: string, init: RequestInit = {}): Request {
  return new Request(`http://monitoring.test${path}`, init);
}

/** A handler over a fake core with `policy` and the other options. */
function handlerWith(policy: MonitoringAuthorizationPolicy, options: Omit<MonitoringHandlerOptions, "authorizationPolicy"> = {}) {
  const fake = fakeBridge();

  return { ...fake, handler: monitoringHandlerOver(fake.bridge, { authorizationPolicy: policy, ...options }) };
}

describe("the policy calls", () => {
  test("the page asks the policy nothing", async () => {
    const { policy, asked } = recordingPolicy(() => true, () => true);
    const { handler, calls } = handlerWith(policy);
    const response = await handler(request("/"));

    assert.equal(response.status, 200);
    assert.deepEqual(asked, []);
    assert.deepEqual(calls.handled[0]?.grant, { view: "notAsked", act: "notAsked" });
  });

  test("a read route asks canView only", async () => {
    const { policy, asked } = recordingPolicy(() => true, () => true);
    const { handler, calls } = handlerWith(policy);

    await handler(request("/api/tasks"));

    assert.deepEqual(asked, ["canView"]);
    assert.deepEqual(calls.handled[0]?.grant, { view: "allowed", act: "notAsked" });
  });

  test("an action route asks canView, then canAct", async () => {
    const { policy, asked } = recordingPolicy(() => true, async () => true);
    const { handler, calls } = handlerWith(policy);

    await handler(request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body: "{}" }));

    assert.deepEqual(asked, ["canView", "canAct"]);
    assert.deepEqual(calls.handled[0]?.grant, { view: "allowed", act: "allowed" });
  });

  test("a refused view asks no canAct and answers 403", async () => {
    const { policy, asked } = recordingPolicy(() => false, () => true);
    const { handler, calls } = handlerWith(policy);
    const response = await handler(request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body: "{}" }));

    assert.deepEqual(asked, ["canView"]);
    assert.deepEqual(calls.handled[0]?.grant, { view: "refused", act: "notAsked" });
    assert.equal(response.status, 403);
  });

  test("/api/meta asks canAct to report it", async () => {
    const { policy, asked } = recordingPolicy(() => true, () => false);
    const { handler, calls } = handlerWith(policy);

    await handler(request("/api/meta"));

    assert.deepEqual(asked, ["canView", "canAct"]);
    assert.deepEqual(calls.handled[0]?.grant, { view: "allowed", act: "refused" });
  });

  test("only an answer of true allows", async () => {
    // SAFETY: a policy of untyped JavaScript code can give any value; the test sends one.
    const loose = { canView: () => "yes", canAct: () => 1 } as unknown as MonitoringAuthorizationPolicy;
    const { handler, calls } = handlerWith(loose);
    const response = await handler(request("/api/tasks"));

    assert.equal(response.status, 403);
    assert.deepEqual(calls.handled[0]?.grant, { view: "refused", act: "notAsked" });
  });
});

describe("the request of the policy", () => {
  test("a policy that reads the body leaves the body of an action to the core", async () => {
    const policy: MonitoringAuthorizationPolicy = {
      canView: async (incoming) => (await incoming.text()) === "",
      canAct: async (incoming) => (await incoming.text()) === "",
    };

    const { handler, calls } = handlerWith(policy);
    const response = await handler(request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body: "{}" }));

    assert.equal(response.status, 200);
    assert.deepEqual(calls.handled[0]?.grant, { view: "allowed", act: "allowed" });
    assert.equal(calls.handled[0]?.bodyHex, "7b7d");
  });

  test("the policy reads the method, the URL, the headers and the signal, and no body", async () => {
    const seen: Request[] = [];

    const recordSeen = (incoming: Request): boolean => {
      seen.push(incoming);

      return true;
    };

    const { handler } = handlerWith({ canView: recordSeen, canAct: recordSeen });
    const leaving = new AbortController();
    const path = `/api/tasks/${TASK_ID}/cancel`;

    await handler(
      request(path, { method: "POST", body: "{}", headers: { cookie: "session=abc" }, signal: leaving.signal }),
    );

    assert.equal(seen.length, 2);

    for (const incoming of seen) {
      assert.equal(incoming.method, "POST");
      assert.equal(incoming.url, `http://monitoring.test${path}`);
      assert.equal(incoming.headers.get("cookie"), "session=abc");
      assert.equal(incoming.body, null);
    }

    leaving.abort();
    assert.equal(seen[0]?.signal.aborted, true);
  });
});

describe("a policy that fails", () => {
  test("a throw answers 500 and logs the route and the error; the core is not asked", async () => {
    const { policy } = recordingPolicy(
      () => {
        throw new TypeError("no session store");
      },
      () => true,
    );

    const { handler, calls } = handlerWith(policy);
    const response = await handler(request("/api/tasks"));

    assert.equal(response.status, 500);
    assert.equal(await response.text(), INTERNAL_SERVER_ERROR_TEXT);
    assert.deepEqual(calls.handled, []);
    assert.equal(calls.logged.length, 1);
    assert.match(calls.logged[0] ?? "", /GET \/api\/tasks: canView threw TypeError: no session store/u);
  });

  test("a rejection of canAct answers 500 and logs it", async () => {
    const { policy } = recordingPolicy(
      () => true,
      async () => Promise.reject(new Error("directory down")),
    );

    const { handler, calls } = handlerWith(policy);
    const response = await handler(request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body: "{}" }));

    assert.equal(response.status, 500);
    assert.match(calls.logged[0] ?? "", /canAct threw Error: directory down/u);
  });
});

describe("the request body", () => {
  test("a body of 1 MiB + 1 byte reaches the core whole and is answered 413", async () => {
    const { handler, calls } = handlerWith(allowAll());
    const body = new Uint8Array(MAX_REQUEST_BODY_BYTES + 1);
    const response = await handler(request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body }));

    assert.equal(response.status, 413);
    assert.equal(calls.handled[0]?.bodyHex.length, 2 * (MAX_REQUEST_BODY_BYTES + 1));
  });

  test("a longer body is cut at 1 MiB + 1 byte", async () => {
    const { handler, calls } = handlerWith(allowAll());
    const body = new Uint8Array(3 * MAX_REQUEST_BODY_BYTES);

    await handler(request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body }));

    assert.equal(calls.handled[0]?.bodyHex.length, 2 * (MAX_REQUEST_BODY_BYTES + 1));
  });

  test("a short body reaches the core as hexadecimal text", async () => {
    const { handler, calls } = handlerWith(allowAll());
    const response = await handler(request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body: "{}" }));

    assert.equal(response.status, 200);
    assert.equal(calls.handled[0]?.bodyHex, "7b7d");
  });

  test("a body that cannot be read answers 400 with the cause", async () => {
    const { handler, calls } = handlerWith(allowAll());

    const body = new ReadableStream<Uint8Array>({
      pull() {
        throw new Error("connection reset");
      },
    });

    const response = await handler(
      request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body, duplex: "half" }),
    );

    assert.equal(response.status, 400);
    assert.deepEqual(await response.json(), { detail: "The request body could not be read: connection reset" });
    assert.deepEqual(calls.handled, []);
  });

  test("a body that is locked answers 400 with the cause", async () => {
    const { handler, calls } = handlerWith(allowAll());
    const incoming = request(`/api/tasks/${TASK_ID}/cancel`, { method: "POST", body: "{}" });

    // A reader that code before the handler took, and kept.
    const _held = incoming.body?.getReader();
    const response = await handler(incoming);
    const body: unknown = await response.json();

    assert.equal(response.status, 400);
    assert.ok(body instanceof Object);
    assert.match(String(Reflect.get(body, "detail")), /^The request body could not be read: ./u);
    assert.deepEqual(calls.handled, []);
  });
});

describe("the mount", () => {
  test("a path outside basePath answers 404 and asks nothing", async () => {
    const { policy, asked } = recordingPolicy(() => true, () => true);
    const { handler, calls } = handlerWith(policy, { basePath: "/monitoring" });
    const response = await handler(request("/other/api/tasks"));

    assert.equal(response.status, 404);
    assert.deepEqual(await response.json(), { detail: "Not found." });
    assert.deepEqual(asked, []);
    assert.deepEqual(calls.routeAccess, []);
  });

  test("a prefix matches whole path segments only", async () => {
    const { handler } = handlerWith(viewOnly(), { basePath: "/monitoring/" });

    assert.equal((await handler(request("/monitoringx/api/tasks"))).status, 404);
  });

  test("the core gets the path inside the mount, the query and the mount path", async () => {
    const { handler, calls } = handlerWith(viewOnly(), { basePath: "/monitoring" });

    await handler(request("/monitoring/api/tasks?limit=5&statuses=FAILED"));

    assert.deepEqual(calls.routeAccess, [{ method: "GET", path: "/api/tasks" }]);
    assert.equal(calls.handled[0]?.path, "/api/tasks");
    assert.equal(calls.handled[0]?.rawQuery, "limit=5&statuses=FAILED");
    assert.equal(calls.handled[0]?.basePath, "/monitoring");
  });

  test("the mount path itself is the page", async () => {
    const { handler, calls } = handlerWith(viewOnly(), { basePath: "/monitoring" });

    await handler(request("/monitoring"));

    assert.equal(calls.handled[0]?.path, "/");
  });

  test("the root mount passes every path with the base path /", async () => {
    const { handler, calls } = handlerWith(viewOnly());

    await handler(request("/api/workers"));

    assert.equal(calls.handled[0]?.path, "/api/workers");
    assert.equal(calls.handled[0]?.basePath, "/");
    assert.equal(calls.handled[0]?.rawQuery, undefined);
  });

  test("mountOf and pathInsideMount", () => {
    assert.deepEqual(mountOf("/"), { kind: "root" });
    assert.deepEqual(mountOf("/ops//"), { kind: "prefix", prefix: "/ops" });
    assert.deepEqual(pathInsideMount("/ops/api", mountOf("/ops")), { kind: "inside", path: "/api", basePath: "/ops" });
    assert.deepEqual(pathInsideMount("/opsx", mountOf("/ops")), { kind: "outside" });
  });
});

describe("the request facts", () => {
  test("the intent and content type headers reach the core", async () => {
    const { handler, calls } = handlerWith(viewOnly());

    await handler(
      request(`/api/tasks/${TASK_ID}/cancel`, {
        method: "POST",
        body: "{}",
        headers: { [INTENT_HEADER]: INTENT_VALUE, "Content-Type": "application/json" },
      }),
    );

    assert.equal(INTENT_HEADER, "X-Horsies-Intent");
    assert.equal(calls.handled[0]?.intentHeader, "action");
    assert.equal(calls.handled[0]?.contentType, "application/json");
  });

  test("absent headers are absent", async () => {
    const { handler, calls } = handlerWith(viewOnly());

    await handler(request("/api/tasks"));

    assert.equal(calls.handled[0]?.intentHeader, undefined);
    assert.equal(calls.handled[0]?.contentType, undefined);
  });
});

describe("the response", () => {
  test("keeps the status, the body and the headers, with no content-length", async () => {
    const response = responseOf({
      status: 409,
      headers: [
        { name: "content-type", value: "application/json" },
        { name: "content-length", value: "2" },
        { name: "cache-control", value: "no-store" },
      ],
      body: new TextEncoder().encode("{}"),
    });

    assert.equal(response.status, 409);
    assert.equal(response.headers.get("content-type"), "application/json");
    assert.equal(response.headers.get("cache-control"), "no-store");
    assert.equal(response.headers.get("content-length"), null);
    assert.equal(await response.text(), "{}");
  });
});

describe("the settings of the core handler", () => {
  const app: MonitoringHandlerApp = {
    // SAFETY: `handlerSettings` reads no member of the handle; the test needs none.
    coreHandle: {} as CoreHandle,
    queueNames: ["default"],
    scheduleNames: ["nightly"],
    clientVersion: "0.1.0",
  };

  test("the defaults: actions enabled, no stylesheet, no trace link", () => {
    assert.deepEqual(handlerSettings(app, { authorizationPolicy: viewOnly() }, EVENT_HEARTBEAT_MS), {
      actions: "enabled",
      clientVersion: "0.1.0",
      heartbeatMs: 15_000,
      queueNames: ["default"],
      scheduleNames: ["nightly"],
    });
  });

  test("the page settings and the actions switch", () => {
    const template = traceLinkOf("https://grafana.test/explore?traceId={trace_id}", { kind: "accepted" });

    assert.ok(template.ok);

    const settings = handlerSettings(
      app,
      {
        authorizationPolicy: viewOnly(),
        actions: "disabled",
        ui: { customCssUrl: "/theme.css", traceLink: template.value },
      },
      200,
    );

    assert.equal(settings.actions, "disabled");
    assert.equal(settings.customCssUrl, "/theme.css");
    assert.equal(settings.traceLinkTemplate, "https://grafana.test/explore?traceId={trace_id}");
    assert.equal(settings.heartbeatMs, 200);
  });
});
