// The Bun server of the `web` command (`Bun.serve`) with a fake handler on an ephemeral port. The
// cases run on Bun only.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { BUN_IDLE_TIMEOUT_SECONDS, serveOnBun } from "../../../horsies/src/web/bun-server.ts";
import { EVENT_HEARTBEAT_MS } from "../../../horsies/src/web/handler.ts";
import { serverCases } from "./server-cases.ts";

/** Whether this process runs on Bun. */
const notOnBun = process.versions["bun"] === undefined;

describe("serveOnBun", () => {
  serverCases(serveOnBun, notOnBun);

  test("the idle timeout is longer than one heartbeat and within Bun's bound", () => {
    assert.ok(BUN_IDLE_TIMEOUT_SECONDS * 1000 > EVENT_HEARTBEAT_MS);
    assert.ok(Number.isInteger(BUN_IDLE_TIMEOUT_SECONDS) && BUN_IDLE_TIMEOUT_SECONDS <= 255);
  });
});
