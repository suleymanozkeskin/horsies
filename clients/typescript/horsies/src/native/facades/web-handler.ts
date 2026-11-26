// The functions of the monitoring web handler in the `facades` area of the addon, the records they
// carry, and the accessor.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { ActionsSwitch } from "../../web/auth.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";

/** The verdicts of the user's policy that a request needs, in the order that the package asks them. */
export type RouteNeed = "public" | "view" | "viewReportAct" | "viewAndAct";

/** How the package answers a request: one response, or the event stream after an allowed view. */
export type ResponseFormName = "oneResponse" | "eventStream";

/** The verdicts that a request needs and the form of its answer. */
export interface RouteAccessRecord {
  readonly need: RouteNeed;
  readonly form: ResponseFormName;
}

/** One verdict of the user's policy. `notAsked`: the route needs none, or an earlier verdict refused. */
export type AccessVerdict = "allowed" | "refused" | "notAsked";

/** The verdicts that the package's run of the user's policy gave. */
export interface AccessGrantArg {
  readonly view: AccessVerdict;
  readonly act: AccessVerdict;
}

/** One request, as the package read it, with the verdicts of the policy. */
export interface MonitoringRequestArg {
  /** An HTTP method token. */
  readonly method: string;
  /** The path inside the mount, from its leading `/`, percent-encoded. */
  readonly path: string;
  /** The query text after `?`; absent when the URL has none. */
  readonly rawQuery?: string;
  /** The value of `X-Horsies-Intent`; absent when the request has none. */
  readonly intentHeader?: string;
  /** The value of `Content-Type`; absent when the request has none. */
  readonly contentType?: string;
  /** The body as lower-case hexadecimal text: at most `maxRequestBodyBytes() + 1` bytes. */
  readonly bodyHex: string;
  /** The mount path of the handler: `/` at the root. */
  readonly basePath: string;
  readonly grant: AccessGrantArg;
}

/** One header of a response. */
export interface MonitoringHeaderRecord {
  /** The name, in lower case. */
  readonly name: string;
  readonly value: string;
}

/** One response for the package to send. */
export interface MonitoringResponseRecord {
  readonly status: number;
  /** The headers, in order. */
  readonly headers: MonitoringHeaderRecord[];
  readonly body: Uint8Array;
}

/** One step of an event stream: one frame text with the blank line that ends it, or the end. */
export type MonitoringEventStep = { readonly kind: "frame"; readonly text: string } | { readonly kind: "ended" };

/** One web client's event stream. */
export interface NativeEventStream {
  /** The next frame, or the end. */
  nextFrame(): Promise<NativeEnvelope<MonitoringEventStep>>;
  /** End the stream: a pending and every later `nextFrame()` answers the end. Returns at once. */
  close(): NativeEnvelope<undefined>;
}

/** The settings of a handler. */
export interface MonitoringHandlerSettings {
  readonly actions: ActionsSwitch;
  /** The stylesheet that the page loads after its own; absent for none. */
  readonly customCssUrl?: string;
  /** The trace link template that `checkTraceLinkTemplate` accepted; absent for the trace id as text. */
  readonly traceLinkTemplate?: string;
  /** The version that `/api/meta` reports. */
  readonly clientVersion: string;
  /** The wait of an event stream without an event before its heartbeat frame: 1 to 3,600,000 ms. */
  readonly heartbeatMs: number;
  /** The `queue` label values of `/metrics`. */
  readonly queueNames: string[];
  /** The `schedule` label values of `/metrics`. */
  readonly scheduleNames: string[];
}

/** The monitoring handler of one core handle. */
export interface NativeMonitoringHandler {
  /** The most body bytes that `handle` takes. */
  maxRequestBodyBytes(): NativeEnvelope<number>;
  /** The verdicts that a request needs and the form of its answer. Pure. */
  routeAccess(method: string, path: string): NativeEnvelope<RouteAccessRecord>;
  /** Serve one request with its verdicts. Every expected outcome is a status and a body. */
  handle(request: MonitoringRequestArg): Promise<NativeEnvelope<MonitoringResponseRecord>>;
  /** One event stream, after an allowed view. */
  subscribeEvents(): Promise<NativeEnvelope<NativeEventStream>>;
  /** End every event stream of the handler after the degraded frame. */
  closeEvents(): Promise<NativeEnvelope<undefined>>;
}

/** The answer of the trace link check. `rule`: the rule that the template breaks. */
export type TraceLinkCheck = { readonly kind: "accepted" } | { readonly kind: "refused"; readonly rule: string };

/** The monitoring handler functions of the `facades` area. */
export interface WebHandlerAddon {
  buildMonitoringHandler(
    handle: NativeCoreHandle,
    settings: MonitoringHandlerSettings,
  ): NativeEnvelope<NativeMonitoringHandler>;
  checkTraceLinkTemplate(text: string): NativeEnvelope<TraceLinkCheck>;
  logMonitoringFailure(message: string): NativeEnvelope<undefined>;
}

/** The monitoring handler functions of the addon. Throws as `requireAddon` does. */
export function webHandlerAddon(): WebHandlerAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this part compares this interface with the declarations that the native build writes.
  return requireAddon() as WebHandlerAddon;
}
