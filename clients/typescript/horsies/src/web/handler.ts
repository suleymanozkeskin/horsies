// `createMonitoringHandler`: the monitoring web API and page of an app as a Fetch-API handler,
// `(request: Request) => Promise<Response>`, for Hono, `Bun.serve`, Next.js routes and the `web`
// command.
//
// The core serves every route, guard, read cache, the schema gate, the event frames and the page.
// This module runs the user's access policy. For one request:
// 1. The path inside the mount: the URL path with `basePath` removed. A path outside the mount is
//    answered 404, and no policy runs.
// 2. The core names the verdicts that the route needs and the form of its answer.
// 3. The policy is asked those verdicts only, in order; `canAct` only after `canView` allowed. A
//    policy that throws or rejects answers 500, and the error is logged with the route.
// 4. The event stream after an allowed view is the core's frames. Every other request reads at most
//    the core's body bound plus one byte, and the core answers it with its verdicts.

import { unreachable } from "../native/call/never.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import type { ThrownFacts } from "../native/call/thrown-facts.ts";
import type {
  AccessGrantArg,
  MonitoringHandlerSettings,
  MonitoringRequestArg,
  MonitoringResponseRecord,
  ResponseFormName,
  RouteNeed,
  AccessVerdict,
} from "../native/facades/web-handler.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { ActionsSwitch, MonitoringAuthPolicy } from "./auth.ts";
import { nativeMonitoringBridge } from "./bridge.ts";
import type { MonitoringBridge } from "./bridge.ts";
import { EVENT_STREAM_HEADERS, eventStreamBody } from "./events.ts";
import { hexOf, readBoundedBody } from "./request-body.ts";
import type { TraceLinkTemplate } from "./trace-link.ts";

/**
 * How long an event stream waits without an event before it sends a heartbeat frame, which keeps an
 * intermediary proxy from closing it. The same value as the Python client and the core's default.
 */
export const EVENT_HEARTBEAT_MS = 15_000;

/** The header of an action request, which the core checks: a cross-site form post cannot set it. */
export const INTENT_HEADER = "X-Horsies-Intent";

/** The body of a 500 answer, as the core's actions write it. */
export const INTERNAL_SERVER_ERROR_TEXT = "Internal Server Error";

/** The detail of a 404 answer, as the core's routes write it. */
const NOT_FOUND_DETAIL = "Not found.";

/** The facts of an app that a monitoring handler reads. */
export interface MonitoringHandlerApp {
  /** The core handle of the app, with its child spec source set. */
  readonly coreHandle: CoreHandle;
  /** The queue names of the app config: the `queue` label values of `/metrics`. */
  readonly queueNames: readonly string[];
  /** The schedule names of the app config: the `schedule` label values of `/metrics`. */
  readonly scheduleNames: readonly string[];
  /** The package version, which `/api/meta` reports. */
  readonly clientVersion: string;
}

/** A mount path: `/` for the root, else the path from its leading `/`, as in the URL. */
export type MountPathText = `/${string}`;

/** The page settings of a handler. */
export interface MonitoringUiOptions {
  /**
   * A stylesheet that the page loads after its own, so its CSS custom properties win. A lone
   * surrogate in the text becomes U+FFFD, as a browser reads it in the page.
   */
  readonly customCssUrl?: string;
  /** The link of a task's trace id in the task detail. Absent: the trace id as text. */
  readonly traceLink?: TraceLinkTemplate;
}

/** The options of `createMonitoringHandler`. */
export interface MonitoringHandlerOptions {
  /** The access policy. Required, with no default: a deployment chooses it. */
  readonly auth: MonitoringAuthPolicy;
  /** Whether the action routes run. Default `enabled`. */
  readonly actions?: ActionsSwitch;
  readonly ui?: MonitoringUiOptions;
  /**
   * The path where the host mounts the handler, as it appears in the URL (percent-encoded). Default
   * `/`. A trailing `/` is ignored. The page builds its links from it.
   */
  readonly basePath?: MountPathText;
}

/** A Fetch-API handler of the monitoring API and page. */
export interface MonitoringHandler {
  /**
   * Answer one request. Every expected outcome is a response. Throws only the package's
   * process-state errors (`CoreError`) and defects.
   */
  (request: Request): Promise<Response>;
  /**
   * End every event stream of the handler after the degraded frame. A stream that opens later
   * answers the degraded frame, then the end. The other routes still answer.
   */
  close(): Promise<void>;
}

/**
 * The monitoring handler of `app`. Starts nothing: the first request connects the app's broker.
 * Throws `ClientDefect` when the app's handle has no child spec source.
 */
export function createMonitoringHandler(app: MonitoringHandlerApp, options: MonitoringHandlerOptions): MonitoringHandler {
  return monitoringHandlerOver(
    nativeMonitoringBridge(app.coreHandle, handlerSettings(app, options, EVENT_HEARTBEAT_MS)),
    options,
  );
}

/** The settings of the core handler of `app` with `options`. Pure. */
export function handlerSettings(
  app: MonitoringHandlerApp,
  options: MonitoringHandlerOptions,
  heartbeatMs: number,
): MonitoringHandlerSettings {
  const customCssUrl = options.ui?.customCssUrl;
  const traceLink = options.ui?.traceLink;

  return {
    actions: options.actions ?? "enabled",
    ...(customCssUrl === undefined ? {} : { customCssUrl: customCssUrl.toWellFormed() }),
    ...(traceLink === undefined ? {} : { traceLinkTemplate: traceLink.text }),
    clientVersion: app.clientVersion,
    heartbeatMs,
    queueNames: [...app.queueNames],
    scheduleNames: [...app.scheduleNames],
  };
}

/** The handler that answers through `bridge` with the policy and the mount of `options`. */
export function monitoringHandlerOver(bridge: MonitoringBridge, options: MonitoringHandlerOptions): MonitoringHandler {
  const serving: Serving = { bridge, policy: options.auth, mount: mountOf(options.basePath ?? "/") };

  const answer = async (request: Request): Promise<Response> => serveRequest(serving, request);

  const close = async (): Promise<void> => {
    await bridge.closeEvents();
  };

  return Object.assign(answer, { close });
}

/** What every request of one handler reads. */
interface Serving {
  readonly bridge: MonitoringBridge;
  readonly policy: MonitoringAuthPolicy;
  readonly mount: Mount;
}

/** The mount of a handler: the root, or a path prefix with no trailing `/`. */
export type Mount = { readonly kind: "root" } | { readonly kind: "prefix"; readonly prefix: string };

/** A request path inside the mount, with the mount path that the core takes. */
export interface InsidePath {
  readonly kind: "inside";
  /** The path inside the mount, from its leading `/`. */
  readonly path: string;
  /** The mount path: `/` at the root. */
  readonly basePath: string;
}

/** Where a request path is, against the mount. */
export type MountedPath = InsidePath | { readonly kind: "outside" };

/** The mount of `basePath`. Pure. */
export function mountOf(basePath: MountPathText): Mount {
  const prefix = basePath.replace(/\/+$/u, "");

  return prefix === "" ? { kind: "root" } : { kind: "prefix", prefix };
}

/** Where `pathname` is against `mount`. A prefix matches whole segments only. Pure. */
export function pathInsideMount(pathname: string, mount: Mount): MountedPath {
  switch (mount.kind) {
    case "root":
      return { kind: "inside", path: pathname, basePath: "/" };
    case "prefix":
      if (pathname === mount.prefix) {
        return { kind: "inside", path: "/", basePath: mount.prefix };
      }

      return pathname.startsWith(`${mount.prefix}/`)
        ? { kind: "inside", path: pathname.slice(mount.prefix.length), basePath: mount.prefix }
        : { kind: "outside" };
    default:
      return unreachable(mount);
  }
}

/** Answer one request. */
async function serveRequest(serving: Serving, request: Request): Promise<Response> {
  const url = new URL(request.url);
  const mounted = pathInsideMount(url.pathname, serving.mount);

  switch (mounted.kind) {
    case "outside":
      return detailResponse(404, NOT_FOUND_DETAIL);
    case "inside":
      return serveMounted(serving, request, url, mounted);
    default:
      return unreachable(mounted);
  }
}

/** Answer one request inside the mount: the policy's verdicts, then the core's answer. */
async function serveMounted(serving: Serving, request: Request, url: URL, mounted: InsidePath): Promise<Response> {
  const access = serving.bridge.routeAccess(request.method, mounted.path);
  const answer = await askPolicy(access.need, serving.policy, request);

  switch (answer.kind) {
    case "policyFailed":
      serving.bridge.logFailure(policyFailureText(request.method, url.pathname, answer));

      return internalServerError();
    case "answered":
      return answerGranted(serving.bridge, request, requestFacts(request, url, mounted, answer.grant), access.form);
    default:
      return unreachable(answer);
  }
}

/** The core's answer to a request with its verdicts: the event stream, or one response. */
async function answerGranted(
  bridge: MonitoringBridge,
  request: Request,
  facts: RequestFacts,
  routeForm: ResponseFormName,
): Promise<Response> {
  const form = answerForm(routeForm, facts.grant);

  switch (form) {
    case "eventStream":
      return eventStreamResponse(bridge, request.signal);
    case "oneResponse":
      return oneResponse(bridge, facts, request.body);
    default:
      return unreachable(form);
  }
}

/** The policy's answer for one request: the verdicts, or the verdict call that threw. */
export type PolicyAnswer =
  | { readonly kind: "answered"; readonly grant: AccessGrantArg }
  | PolicyFailed;

/** A verdict call of the policy that threw or rejected. */
export interface PolicyFailed {
  readonly kind: "policyFailed";
  readonly verdict: "canView" | "canAct";
  readonly cause: ThrownFacts;
}

/** One verdict call of the policy. */
type VerdictCall = { readonly kind: "verdict"; readonly verdict: "allowed" | "refused" } | PolicyFailed;

/** Ask `policy` the verdicts that `need` names, in order: `canAct` only after `canView` allowed. */
export async function askPolicy(need: RouteNeed, policy: MonitoringAuthPolicy, request: Request): Promise<PolicyAnswer> {
  switch (need) {
    case "public":
      return answered("notAsked", "notAsked");
    case "view": {
      const view = await verdictOf("canView", () => policy.canView(request));

      return view.kind === "verdict" ? answered(view.verdict, "notAsked") : view;
    }

    case "viewReportAct":
    case "viewAndAct":
      return askViewThenAct(policy, request);
    default:
      return unreachable(need);
  }
}

/** `canView`, then `canAct` when the view was allowed. */
async function askViewThenAct(policy: MonitoringAuthPolicy, request: Request): Promise<PolicyAnswer> {
  const view = await verdictOf("canView", () => policy.canView(request));

  switch (view.kind) {
    case "policyFailed":
      return view;
    case "verdict":
      break;
    default:
      return unreachable(view);
  }

  const viewVerdict = view.verdict;

  switch (viewVerdict) {
    case "refused":
      return answered("refused", "notAsked");
    case "allowed": {
      const act = await verdictOf("canAct", () => policy.canAct(request));

      return act.kind === "verdict" ? answered("allowed", act.verdict) : act;
    }

    default:
      return unreachable(viewVerdict);
  }
}

/** The verdict of one policy call: only `true` allows; a throw or a rejection is `policyFailed`. */
async function verdictOf(
  verdict: PolicyFailed["verdict"],
  call: () => boolean | Promise<boolean>,
): Promise<VerdictCall> {
  try {
    const allowed = await call();

    return { kind: "verdict", verdict: allowed === true ? "allowed" : "refused" };
  } catch (cause) {
    return { kind: "policyFailed", verdict, cause: thrownFacts(cause) };
  }
}

function answered(view: AccessVerdict, act: AccessVerdict): PolicyAnswer {
  return { kind: "answered", grant: { view, act } };
}

/** The log text of a policy call that threw. Pure. */
function policyFailureText(method: string, pathname: string, failed: PolicyFailed): string {
  return `the monitoring access policy failed on ${method} ${pathname}: ${failed.verdict} threw ${failed.cause.name}: ${failed.cause.message}`;
}

/** The form of the answer: the event stream only after an allowed view of its route. Pure. */
function answerForm(form: ResponseFormName, grant: AccessGrantArg): ResponseFormName {
  switch (form) {
    case "oneResponse":
      return "oneResponse";
    case "eventStream":
      return grant.view === "allowed" ? "eventStream" : "oneResponse";
    default:
      return unreachable(form);
  }
}

/** The request facts that the core reads, before the body. */
type RequestFacts = Omit<MonitoringRequestArg, "bodyHex">;

/** The facts of `request` inside the mount, with its verdicts. Pure. */
function requestFacts(request: Request, url: URL, mounted: InsidePath, grant: AccessGrantArg): RequestFacts {
  const intentHeader = request.headers.get(INTENT_HEADER);
  const contentType = request.headers.get("content-type");

  return {
    method: request.method,
    path: mounted.path,
    ...(url.search === "" ? {} : { rawQuery: url.search.slice(1) }),
    ...(intentHeader === null ? {} : { intentHeader }),
    ...(contentType === null ? {} : { contentType }),
    basePath: mounted.basePath,
    grant,
  };
}

/** The core's one response to a request: its body read up to the core's bound plus one byte. */
async function oneResponse(
  bridge: MonitoringBridge,
  facts: RequestFacts,
  body: ReadableStream<Uint8Array> | null,
): Promise<Response> {
  const read = await readBoundedBody(body, bridge.maxRequestBodyBytes + 1);

  switch (read.kind) {
    case "failed":
      return detailResponse(400, `The request body could not be read: ${read.cause}`);
    case "read":
      return responseOf(await bridge.handle({ ...facts, bodyHex: hexOf(read.bytes) }));
    default:
      return unreachable(read);
  }
}

/** The event stream response of one client. */
async function eventStreamResponse(bridge: MonitoringBridge, signal: AbortSignal): Promise<Response> {
  const source = await bridge.subscribeEvents();

  return new Response(eventStreamBody(source, signal), { status: 200, headers: EVENT_STREAM_HEADERS });
}

/** The response of the core's answer, with no `content-length`: the server writes its own. Pure. */
export function responseOf(record: MonitoringResponseRecord): Response {
  const headers = new Headers();

  for (const header of record.headers) {
    switch (header.name) {
      case "content-length":
        break;
      default:
        headers.append(header.name, header.value);
    }
  }

  return new Response(record.body, { status: record.status, headers });
}

/** A JSON answer `{"detail": ...}`, the form of the core's refusals. Pure. */
function detailResponse(status: number, detail: string): Response {
  return new Response(JSON.stringify({ detail }), { status, headers: { "content-type": "application/json" } });
}

/** The 500 answer of a policy that threw. Pure. */
function internalServerError(): Response {
  return new Response(INTERNAL_SERVER_ERROR_TEXT, {
    status: 500,
    headers: { "content-type": "text/plain; charset=utf-8" },
  });
}
