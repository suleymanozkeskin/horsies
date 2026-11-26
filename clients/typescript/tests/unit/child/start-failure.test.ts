// A child whose start fails, in an entry that does not await `cli.run` (a CommonJS entry) and with an open user
// handle: the process ends with 1 within the exit rule's bound, on the runtime that runs this test (Bun in `bun test`,
// Node in the Node step of `ci/unit.sh`).

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

/** The longest time the fixture may take: the exit rule waits at most 1 s. */
const FIXTURE_BOUND_MS = 20_000;

const FIXTURE = fileURLToPath(new URL("fixtures/start-failure.ts", import.meta.url));

describe("a child whose start fails", () => {
  test("ends with 1, not 0, and does not hang on an open user handle", () => {
    const ended = spawnSync(process.execPath, [FIXTURE], { encoding: "utf8", timeout: FIXTURE_BOUND_MS });

    assert.equal(ended.signal, null, `the fixture was killed at the bound: ${ended.stderr}`);
    assert.equal(ended.status, 1, ended.stderr);
    assert.match(ended.stderr, /horsies child: child mode failed: RangeError: the host thread did not start/u);
  });
});
