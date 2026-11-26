// The facts of an app that the monitoring handler and the fleet metrics read, from the app's context on a main thread.

import { DEFAULT_QUEUE_ROUTE } from "../app/contract-list.ts";
import { appCoreHandle } from "../app/core-access.ts";
import type { AppContext } from "../app/context.ts";
import { unreachable } from "../native/call/never.ts";
import { clientVersion } from "./client-version.ts";
import type { MonitoringHandlerApp } from "./handler.ts";

/**
 * The facts of the app of `context` that the monitoring handler and the fleet metrics read, on a main thread: the
 * app's core handle (`appCoreHandle`, which holds the app's child spec source), the queue names of its config, its
 * schedule names (none: the config holds no schedules yet) and the package version. The refusals of `appCoreHandle`.
 */
export function monitoringAppOf(context: AppContext): MonitoringHandlerApp {
  return {
    coreHandle: appCoreHandle(context.core),
    queueNames: queueNamesOf(context),
    scheduleNames: [],
    clientVersion: clientVersion(),
  };
}

/** The queue names of the config of `context`: the default queue, or the custom queues. Pure. */
function queueNamesOf(context: AppContext): readonly string[] {
  const queues = context.config.queues;

  switch (queues.kind) {
    case "default":
      return [DEFAULT_QUEUE_ROUTE.queueName];
    case "custom":
      return [...queues.queues.keys()];
    default:
      return unreachable(queues);
  }
}
