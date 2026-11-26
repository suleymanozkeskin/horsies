// The `child` area of the addon with a built addon: the read of the child-mode arguments, the answers of the settle,
// cancel-state and spawn calls for a run that is not in the table, and the child host entry in child processes (a
// socket that is not open, a parent that is gone, the backstop, a second serve, the loop marks before a serve).
//
// Runs only where a built addon exists (`HORSIES_NATIVE_ADDON_PATH`). Run it under the memory watchdog (2 GiB, with
// a wall-time bound). No case connects to a database.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  childArguments,
  endSpawn,
  registerSpawn,
  runCancelStateOf,
  settleBodyValue,
} from "../../../horsies/src/native/child/child.ts";
import { requireDevAddon } from "../../../horsies/src/native/dev-addon.ts";
import { coreState } from "../../../horsies/src/native/process-core/process-core.ts";
import { ClientDefect } from "../../../horsies/src/thrown-errors.ts";
import { runChildCase } from "./child-process.ts";

// The addon of these tests is the dev build that HORSIES_NATIVE_ADDON_PATH names.
void requireDevAddon();

/** The longest time a child case may take: the backstop case blocks for 6 s. */
const CHILD_CASE_BOUND_MS = 20_000;

/** A run key and a work key that no serve gave. */
const NO_KEY = 987_654_321n;

const SCRIPT = "child-host-child.ts";

describe("the child area of the addon", () => {
  test("the child flag with a descriptor and a pid is a child; each bad argument gives its cause and 73", () => {
    assert.deepEqual(childArguments(["--horsies-child", "3", "4242"]), { kind: "child" });

    for (const bad of [["--horsies-child"], ["--horsies-child", "x", "1"], ["--horsies-child", "3", "1", "extra"]]) {
      const check = childArguments(bad);

      assert.equal(check.kind, "badArguments");
      assert.equal(check.kind === "badArguments" ? check.code : 0, 73);
    }

    assert.throws(() => childArguments(["worker"]), ClientDefect);
    assert.equal(coreState(), "not_started");
  });

  test("a run and a work that are not in the table", () => {
    assert.equal(settleBodyValue(NO_KEY, "null"), "unknownRun");
    assert.deepEqual(runCancelStateOf(NO_KEY), { kind: "runEnded" });
    assert.deepEqual(registerSpawn(NO_KEY, "work"), { kind: "runEnded" });
    assert.equal(endSpawn(NO_KEY), "unknownWork");
    assert.equal(coreState(), "not_started");
  });

  test("a socket descriptor that is not open ends the serve with 73", () => {
    assert.equal(runChildCase(SCRIPT, "socket-not-open", CHILD_CASE_BOUND_MS).stdout, "BAD_ARGUMENTS 73");
  });

  test("a worker pid that is not the parent ends the serve with 71", () => {
    assert.equal(runChildCase(SCRIPT, "parent-gone", CHILD_CASE_BOUND_MS).stdout, "PARENT_GONE 71");
  });

  test("the host thread ends a process whose JavaScript thread does not exit, with the host's code", () => {
    const ended = runChildCase(SCRIPT, "backstop", CHILD_CASE_BOUND_MS);

    assert.equal(ended.stdout, "BAD_ARGUMENTS 73");
    assert.equal(ended.status, 73);
  });

  test("a second serve in a process is a defect", () => {
    assert.equal(runChildCase(SCRIPT, "second-serve", CHILD_CASE_BOUND_MS).stdout, "ClientDefect");
  });

  test("a loop mark before the serve is a defect", () => {
    assert.equal(runChildCase(SCRIPT, "marks-before-serve", CHILD_CASE_BOUND_MS).stdout, "ClientDefect");
  });
});
