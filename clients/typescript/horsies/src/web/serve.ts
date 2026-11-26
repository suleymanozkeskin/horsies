// The choice of the HTTP server of the `web` command: `node:http` in Node, `Bun.serve` in Bun.

import { unreachable } from "../native/call/never.ts";
import { serveOnBun } from "./bun-server.ts";
import { serveOnNode } from "./node-server.ts";
import type { FetchHandler, HandlerFailureReport, ServeAddress, ServeOutcome } from "./server.ts";

/** The runtime of this process, for the choice of the server. */
export type ServerRuntime = "node" | "bun";

/** The runtime of this process: Bun when `process.versions.bun` is set, else Node. */
export function serverRuntime(): ServerRuntime {
  return process.versions["bun"] === undefined ? "node" : "bun";
}

/** Serve `handler` on `address` with the server of `runtime`. */
export async function serveHandler(
  runtime: ServerRuntime,
  handler: FetchHandler,
  address: ServeAddress,
  report: HandlerFailureReport,
): Promise<ServeOutcome> {
  switch (runtime) {
    case "node":
      return serveOnNode(handler, address, report);
    case "bun":
      return serveOnBun(handler, address, report);
    default:
      return unreachable(runtime);
  }
}
