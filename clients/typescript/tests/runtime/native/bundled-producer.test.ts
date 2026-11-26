// A producer that `bun build` bundles into one file sends a task with zod schemas and reads its info. The main entry
// point imports the zod adapter statically, so the bundle compiles its zod contract as an unbundled run does: no
// module loads from a path next to a source file.
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`) and `HORSIES_TEST_DATABASE_URL` names a
// throwaway PostgreSQL database. The build needs `bun` on PATH; the bundle runs on the runtime of this test (Bun or
// Node), with the environment of this process. Each process has a wall-time bound and is killed at it.

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

/** The producer that the test bundles. */
const ENTRY = fileURLToPath(new URL("bundled-producer-entry.ts", import.meta.url));

/** The command of the bundler. */
const BUN_COMMAND = "bun";

/** The longest time the build may take. */
const BUILD_BOUND_MS = 60_000;

/** The longest time the run of the bundle may take. */
const RUN_BOUND_MS = 30_000;

/** The task name of the producer's contract (`bundled-producer-entry.ts`). */
const TASK_NAME = "bundled_producer_place_order";

/** The bundle of the producer in `folder`: one ES module file for Node and Bun. The build must pass. */
function bundleOf(folder: string): string {
  const bundle = join(folder, "producer.js");

  const build = spawnSync(
    BUN_COMMAND,
    ["build", ENTRY, "--target=node", "--format=esm", `--outfile=${bundle}`],
    { encoding: "utf8", timeout: BUILD_BOUND_MS },
  );

  assert.equal(build.status, 0, `bun build failed: ${build.stderr}`);

  return bundle;
}

describe("a bundled producer", () => {
  test("compiles its zod contract, sends it and reads PENDING from info()", () => {
    const folder = mkdtempSync(join(tmpdir(), "horsies-bundled-producer-"));

    try {
      const run = spawnSync(process.execPath, [bundleOf(folder)], { encoding: "utf8", timeout: RUN_BOUND_MS });

      assert.equal(run.status, 0, `the bundle failed (signal ${String(run.signal)}): ${run.stderr}`);
      assert.deepEqual(JSON.parse(run.stdout.trim()), { outcome: "read", taskName: TASK_NAME, status: "PENDING" });
    } finally {
      rmSync(folder, { recursive: true, force: true });
    }
  });
});
