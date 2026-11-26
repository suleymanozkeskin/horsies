// The monitoring handler of the addon, as the web handler uses it: each call goes through the call
// wrapper, so a thrown envelope is its error and a throw of the call is a defect.

import { nativeValue, nativeValueAsync } from "../native/call/envelope.ts";
import { webHandlerAddon } from "../native/facades/web-handler.ts";
import type {
  MonitoringHandlerSettings,
  MonitoringRequestArg,
  MonitoringResponseRecord,
  NativeEventStream,
  NativeMonitoringHandler,
  RouteAccessRecord,
} from "../native/facades/web-handler.ts";
import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { EventFrameSource } from "./events.ts";

/**
 * The calls of the web handler to the core's monitoring handler. Every method throws only the
 * package's process-state errors (`CoreError`) and defects.
 */
export interface MonitoringBridge {
  /** The most body bytes that `handle` takes. */
  readonly maxRequestBodyBytes: number;
  /** The verdicts that a request needs and the form of its answer. Pure. */
  routeAccess(method: string, path: string): RouteAccessRecord;
  /** Serve one request with its verdicts. */
  handle(request: MonitoringRequestArg): Promise<MonitoringResponseRecord>;
  /** One event stream, after an allowed view. */
  subscribeEvents(): Promise<EventFrameSource>;
  /** End every event stream after the degraded frame. */
  closeEvents(): Promise<undefined>;
  /** Write one ERROR record of the `horsies.web` logger. */
  logFailure(message: string): undefined;
}

/**
 * The bridge of a core handler over the broker of `handle`. Starts nothing. Throws `ClientDefect`
 * for settings that the package should have refused and for a handle with no child spec source.
 */
export function nativeMonitoringBridge(handle: CoreHandle, settings: MonitoringHandlerSettings): MonitoringBridge {
  const addon = webHandlerAddon();
  const native = nativeValue("buildMonitoringHandler", () => addon.buildMonitoringHandler(handle.native, settings));

  return bridgeOver(native, (message) =>
    nativeValue("logMonitoringFailure", () => addon.logMonitoringFailure(message)),
  );
}

/** The bridge of the addon's handler `native`, with `log` as its log call. */
function bridgeOver(native: NativeMonitoringHandler, log: (message: string) => undefined): MonitoringBridge {
  return {
    maxRequestBodyBytes: nativeValue("MonitoringHandler.maxRequestBodyBytes", () => native.maxRequestBodyBytes()),
    routeAccess: (method, path) => nativeValue("MonitoringHandler.routeAccess", () => native.routeAccess(method, path)),
    handle: async (request) => nativeValueAsync("MonitoringHandler.handle", () => native.handle(request)),
    subscribeEvents: async () =>
      frameSourceOver(await nativeValueAsync("MonitoringHandler.subscribeEvents", () => native.subscribeEvents())),
    closeEvents: async () => nativeValueAsync("MonitoringHandler.closeEvents", () => native.closeEvents()),
    logFailure: log,
  };
}

/** The frame source of the addon's stream `stream`. */
function frameSourceOver(stream: NativeEventStream): EventFrameSource {
  return {
    next: async () => nativeValueAsync("MonitoringEventStream.nextFrame", () => stream.nextFrame()),
    close: () => nativeValue("MonitoringEventStream.close", () => stream.close()),
  };
}
