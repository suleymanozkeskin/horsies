// The process core with a built addon: nothing starts at load or at a pure call, the environment
// count, the task id source, the migration files, and the process-wide rules in child processes.

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { describe, test } from "node:test";
import { fileURLToPath } from "node:url";

import { configureLogging } from "../../../horsies/src/logging.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import {
  coreState,
  environmentCount,
  migrationSqlFiles,
  mintTaskId,
  setPanicRule,
} from "../../../horsies/src/native/process-core/process-core.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest time a child case may take. */
const CHILD_CASE_BOUND_MS = 30_000;

/** The abort signal of the process. */
const ABORT_SIGNAL = "SIGABRT";

const CHILD = fileURLToPath(new URL("process-core-child.ts", import.meta.url));

/** Run one case of `process-core-child.ts` in a new process of this runtime. */
function runChild(name: string): { readonly stdout: string; readonly stderr: string; readonly signal: string | null } {
  const result = spawnSync(process.execPath, [CHILD, name], {
    encoding: "utf8",
    timeout: CHILD_CASE_BOUND_MS,
  });

  return { stdout: result.stdout.trim(), stderr: result.stderr, signal: result.signal };
}

describe("the process core with a built addon", () => {
  test("loading the addon and every pure function start nothing", () => {
    assert.equal(coreState(), "not_started");

    const pureCalls: readonly (() => unknown)[] = [
      () => mintTaskId(),
      () => migrationSqlFiles(),
      () => environmentCount(),
      () => {
        setPanicRule("STOP_CORE");
      },
      () => {
        configureLogging({ levels: { default: "WARNING" }, sink: { kind: "stderr" } });
      },
    ];

    for (const call of pureCalls) {
      call();
      assert.equal(coreState(), "not_started", call.toString());
    }
  });

  test("one Node-API environment registered the addon", () => {
    assert.equal(environmentCount(), 1);
  });

  test("a minted task id is a lower-case UUIDv7", () => {
    const minted = mintTaskId();

    assert.equal(minted.kind, "minted");
    assert.match(minted.kind === "minted" ? minted.taskId : "", /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u);
  });

  test("the migration files start with the baseline and keep their order", () => {
    const versions = migrationSqlFiles().map((file) => file.version);

    assert.equal(versions[0], 1000n);
    assert.deepEqual(versions, versions.toSorted((left, right) => Number(left - right)));
  });
});

describe("the process-wide rules, each in a child process", () => {
  test("another panic rule after a set one throws PanicRuleFixed with the rule in force", () => {
    assert.equal(runChild("panic-rule-fixed").stdout, "PanicRuleFixed STOP_CORE");
  });

  test("a defect under STOP_CORE throws its class", () => {
    assert.equal(runChild("defect-under-stop-core").stdout, "ClientDefect");
  });

  test("a defect under ABORT_PROCESS writes its line and aborts the process", () => {
    const child = runChild("defect-under-abort-process");

    assert.equal(child.signal, ABORT_SIGNAL, child.stderr);
    assert.match(child.stderr, /aborts: defect CLIENT_DEFECT of the horsies package: a defect of the horsies package: taskId: a UUID/u);
    assert.equal(child.stdout, "");
  });

  test("a handle that never connected closes at once and starts no core", () => {
    assert.equal(runChild("never-connected-close").stdout, "undefined not_started");
  });

  test("a second environment that opens the addon itself is refused at registration", () => {
    assert.match(runChild("second-environment").stdout, /^1 2 .*ADDON_OUTSIDE_MAIN_THREAD/u);
  });
});
