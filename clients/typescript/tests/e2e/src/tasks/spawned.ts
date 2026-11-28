// `ISOLATED` tasks of the run rules of the TypeScript child: work that a body started with `runtime.spawn` and left
// when it returned, and the deadline of an isolated run, which sends no signal (the worker kills the child).

import { defineTask, h, type Horsies } from "#horsies";
import { appendLine, markerErrorText, unixMillis } from "../markers.ts";
import { STOP_TIMEOUT_MS } from "./shared.ts";
import { PAST_THE_DEADLINE_MS, sleepFor } from "./waits.ts";

const SPAWN_CODES = ["WORK_FAILED"] as const;

/**
 * Spawns work that waits `workMs` and then writes the time to `<token>-work-end`; the body returns at once. The run
 * ends when the work it left has ended.
 */
export const isolatedLeftWork = defineTask({
  name: "e2e_isolated_left_work",
  input: { token: h.string(), workMs: h.int() },
  output: h.string(),
  errorCodes: SPAWN_CODES,
});

/**
 * Spawns work that waits for its own signal and writes the reason's cause to `<token>-work`; the body returns at
 * once. The stop of the left work fires the work's signal with `LEFT_WORK`.
 */
export const isolatedLeftWorkSignal = defineTask({
  name: "e2e_isolated_left_work_signal",
  input: { token: h.string() },
  output: h.string(),
  errorCodes: SPAWN_CODES,
});

/**
 * Listens to its signal, writing the cause to `<token>-signal` when it fires, and waits past its 2 s deadline. An
 * isolated deadline sends no signal: the worker kills the child.
 */
export const isolatedDeadline = defineTask({
  name: "e2e_isolated_deadline",
  input: { token: h.string() },
  output: h.string(),
  timeoutMs: STOP_TIMEOUT_MS,
});

/** Every contract of this module. */
export const SPAWNED_CONTRACTS = [isolatedLeftWork, isolatedLeftWorkSignal, isolatedDeadline] as const;

/** The implementations of this module on `app`. */
export function implementSpawned(app: Horsies): void {
  app.implement(isolatedLeftWork, async ({ token, workMs }, { ok, err, spawn }) => {
    const work = spawn("left_work", async () => {
      await sleepFor(workMs, "noSignal");

      const written = appendLine(`${token}-work-end`, String(unixMillis()));

      return written.ok ? "written" : markerErrorText(written.error);
    });

    return work.ok
      ? ok("returned")
      : err({ code: "WORK_FAILED", message: `spawn refused: ${work.error.kind}` });
  });
  app.implement(isolatedLeftWorkSignal, async ({ token }, { ok, err, spawn }) => {
    const work = spawn("left_work_waits_for_signal", async (signal) => {
      const end = await sleepFor(PAST_THE_DEADLINE_MS, signal);
      const written = appendLine(`${token}-work`, end.kind === "aborted" ? end.reason : "slept");

      return written.ok ? "written" : markerErrorText(written.error);
    });

    return work.ok
      ? ok("returned")
      : err({ code: "WORK_FAILED", message: `spawn refused: ${work.error.kind}` });
  });
  app.implement(isolatedDeadline, async ({ token }, { ok, signal }) => {
    signal.addEventListener(
      "abort",
      () => {
        const written = appendLine(`${token}-signal`, "fired");

        if (!written.ok) {
          process.stderr.write(`e2e_isolated_deadline: ${markerErrorText(written.error)}\n`);
        }
      },
      { once: true },
    );
    await sleepFor(PAST_THE_DEADLINE_MS, "noSignal");

    return ok("never");
  });
}
