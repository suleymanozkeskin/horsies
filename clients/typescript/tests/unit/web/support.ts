// A fake core handler for the web handler tests: the route access of the core for the routes the
// tests use, the 403 and 413 answers of the core, an event stream per subscription, and a record of
// every call.

import type { MonitoringAuthorizationPolicy } from "../../../horsies/src/web/auth.ts";
import type { MonitoringBridge } from "../../../horsies/src/web/bridge.ts";
import type { EventFrameSource } from "../../../horsies/src/web/events.ts";
import type {
  MonitoringEventStep,
  MonitoringRequestArg,
  MonitoringResponseRecord,
  RouteAccessRecord,
} from "../../../horsies/src/native/facades/web-handler.ts";

/** The core's request body bound: 1 MiB. */
export const MAX_REQUEST_BODY_BYTES = 1024 * 1024;

/** The degraded frame of the core. */
export const DEGRADED_FRAME = 'data: {"topic": "degraded"}\n\n';

/** A frame source that the test feeds. `close()` and `degrade()` end it. */
export class FakeFrameSource implements EventFrameSource {
  readonly #queued: MonitoringEventStep[] = [];
  #waiting: ((step: MonitoringEventStep) => void) | undefined;
  #state: "open" | "closed" = "open";
  closeCalls = 0;
  nextCalls = 0;

  /** Give one frame to the reader. */
  push(text: string): void {
    this.#give({ kind: "frame", text });
  }

  /** The end of the stream after the frames given before. */
  finish(): void {
    this.#give({ kind: "ended" });
  }

  /** The core's close of the broadcaster: the degraded frame, then the end. */
  degrade(): void {
    this.#give({ kind: "frame", text: DEGRADED_FRAME });
    this.#give({ kind: "ended" });
  }

  async next(): Promise<MonitoringEventStep> {
    this.nextCalls += 1;

    const queued = this.#queued.shift();

    if (queued !== undefined) {
      return queued;
    }

    if (this.#state === "closed") {
      return { kind: "ended" };
    }

    const { promise, resolve } = Promise.withResolvers<MonitoringEventStep>();

    this.#waiting = resolve;

    return promise;
  }

  close(): void {
    this.closeCalls += 1;
    this.#state = "closed";
    this.#queued.length = 0;
    this.#give({ kind: "ended" });
  }

  #give(step: MonitoringEventStep): void {
    const waiting = this.#waiting;

    if (waiting === undefined) {
      this.#queued.push(step);

      return;
    }

    this.#waiting = undefined;
    waiting(step);
  }
}

/** Every call that the handler made to the fake core. */
export interface BridgeCalls {
  readonly routeAccess: { method: string; path: string }[];
  readonly handled: MonitoringRequestArg[];
  readonly sources: FakeFrameSource[];
  readonly logged: string[];
  closeEvents: number;
}

/** The route access of the core for the routes of the tests. */
export function coreRouteAccess(method: string, path: string): RouteAccessRecord {
  const verb = method === "HEAD" ? "GET" : method;

  if (verb === "GET" && path === "/api/meta") {
    return { need: "viewReportAct", form: "oneResponse" };
  }

  if (verb === "GET" && path === "/api/events") {
    return { need: "view", form: "eventStream" };
  }

  if ((verb === "GET" && path.startsWith("/api/")) || path === "/metrics") {
    return { need: "view", form: "oneResponse" };
  }

  if (verb === "POST" && /^\/api\/tasks\/[^/]+\/cancel$/u.test(path)) {
    return { need: "viewAndAct", form: "oneResponse" };
  }

  return { need: "public", form: "oneResponse" };
}

/** The answer of the core: 403 after a refused verdict, 413 past the body bound, else 200. */
export function coreAnswer(request: MonitoringRequestArg): MonitoringResponseRecord {
  const refused = request.grant.view === "refused" || request.grant.act === "refused";
  const bodyBytes = request.bodyHex.length / 2;

  if (refused) {
    return jsonRecord(403, { detail: "Not authorized." });
  }

  return bodyBytes > MAX_REQUEST_BODY_BYTES
    ? jsonRecord(413, { detail: `The request body is over ${MAX_REQUEST_BODY_BYTES} bytes.` })
    : jsonRecord(200, { path: request.path });
}

function jsonRecord(status: number, body: object): MonitoringResponseRecord {
  const text = JSON.stringify(body);

  return {
    status,
    headers: [
      { name: "content-type", value: "application/json" },
      { name: "content-length", value: String(text.length) },
    ],
    body: new TextEncoder().encode(text),
  };
}

/** A fake core handler and the record of its calls. */
export function fakeBridge(): { readonly bridge: MonitoringBridge; readonly calls: BridgeCalls } {
  const calls: BridgeCalls = { routeAccess: [], handled: [], sources: [], logged: [], closeEvents: 0 };

  const bridge: MonitoringBridge = {
    maxRequestBodyBytes: MAX_REQUEST_BODY_BYTES,
    routeAccess: (method, path) => {
      calls.routeAccess.push({ method, path });

      return coreRouteAccess(method, path);
    },
    handle: async (request) => {
      calls.handled.push(request);

      return coreAnswer(request);
    },
    subscribeEvents: async () => {
      const source = new FakeFrameSource();

      calls.sources.push(source);

      return source;
    },
    closeEvents: async () => {
      calls.closeEvents += 1;

      for (const source of calls.sources) {
        source.degrade();
      }

      return undefined;
    },
    logFailure: (message) => {
      calls.logged.push(message);

      return undefined;
    },
  };

  return { bridge, calls };
}

/** A policy that records its calls and answers as told. */
export function recordingPolicy(
  view: () => boolean | Promise<boolean>,
  act: () => boolean | Promise<boolean>,
): { readonly policy: MonitoringAuthorizationPolicy; readonly asked: string[] } {
  const asked: string[] = [];

  return {
    asked,
    policy: {
      canView: () => {
        asked.push("canView");

        return view();
      },
      canAct: () => {
        asked.push("canAct");

        return act();
      },
    },
  };
}

/** Read a body to its end as text. */
export async function bodyText(response: Response): Promise<string> {
  return response.text();
}
