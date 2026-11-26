// The tasks and the workflows of the instance `recovery_queues` (Rust `clients/rust/tests/worker/src/tasks/recovery.rs`,
// Python `clients/python/tests/e2e/tasks/instance_recovery.py` and `priority_binding.py`). In the custom queue mode
// every task names one of the queues `default` and `recovery`; `recovery` runs one task at a time.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../../horsies/src/index.ts";
import { SERIAL_QUEUE } from "../instances.ts";
import { named } from "../workflows/naming.ts";
import { holdThread } from "./waits.ts";
import { node } from "../workflows/node-options.ts";

/** The queue of the readiness task and of the first nodes of the crash workflow. */
export const DEFAULT_QUEUE = "default";

/** How long the nodes c and d of `recovery_crash` run: the worker is killed while they run. */
export const CRASHED_NODE_MS = 60_000;

/** How long the other nodes of `recovery_crash` run. */
const SHORT_NODE_MS = 50;

/** The readiness task of a worker of the instance. */
export const recoveryHealthcheck = defineTask({
  name: "e2e_recovery_healthcheck",
  queue: DEFAULT_QUEUE,
  input: {},
  output: h.string(),
});

/** Python `e2e_recovery_wf_step`: `step_<step>_done` after `delayMs`. */
export const recoveryStep = defineTask({
  name: "e2e_recovery_wf_step",
  queue: DEFAULT_QUEUE,
  input: { step: h.string(), delayMs: h.int() },
  output: h.string(),
});

/** Python `pb_blocker`: holds the one slot of the serial queue for `durationMs`. */
export const serialBlocker = defineTask({
  name: "e2e_serial_blocker",
  queue: SERIAL_QUEUE.name,
  input: { durationMs: h.int() },
  output: h.string(),
});

/** Python `pb_scrape`: the task of the child workflow, on the serial queue. */
export const serialChildTask = defineTask({ name: "e2e_serial_child_task", queue: SERIAL_QUEUE.name, input: {}, output: h.int() });

/** Python `pb_direct`: a task that a send puts on the serial queue. */
export const serialDirectTask = defineTask({ name: "e2e_serial_direct_task", queue: SERIAL_QUEUE.name, input: {}, output: h.int() });

/**
 * Python `spec_recovery_crash`: a and b, then c and d that run long; e after c and d, which allows failed
 * dependencies; f after e. e and f run on the serial queue.
 */
export const recoveryCrash = defineWorkflow({ ...named("recovery_crash"), parameters: {}, outputType: h.string() }, (w) => {
  const a = w.task(recoveryStep, { step: "a", delayMs: SHORT_NODE_MS }, node("a"));
  const b = w.task(recoveryStep, { step: "b", delayMs: SHORT_NODE_MS }, node("b", { after: [a] }));
  const c = w.task(recoveryStep, { step: "c", delayMs: CRASHED_NODE_MS }, node("c", { after: [b] }));
  const d = w.task(recoveryStep, { step: "d", delayMs: CRASHED_NODE_MS }, node("d", { after: [b] }));

  const e = w.task(
    recoveryStep,
    { step: "e", delayMs: SHORT_NODE_MS },
    node("e", { after: [c, d], runsAfterFailure: true, queue: SERIAL_QUEUE.name }),
  );

  return { output: w.task(recoveryStep, { step: "f", delayMs: SHORT_NODE_MS }, node("f", { after: [e], queue: SERIAL_QUEUE.name })) };
});

/** Python `PriorityChildWorkflow`: one node of the serial queue task; the node names no priority. */
export const serialChild = defineWorkflow({ ...named("serial_child"), parameters: {}, outputType: h.int() }, (w) => ({
  output: w.task(serialChildTask, {}, node("scrape")),
}));

/** Python `spec_priority_parent`: one sub-workflow node of `serial_child`. */
export const serialParent = defineWorkflow({ ...named("serial_parent"), parameters: {}, outputType: h.int() }, (w) => ({
  output: w.subWorkflow(serialChild, {}, node("child")),
}));

/** Every task contract of the instance. */
export const RECOVERY_QUEUE_CONTRACTS = [recoveryHealthcheck, recoveryStep, serialBlocker, serialChildTask, serialDirectTask] as const;

/** Every workflow of the instance. */
export const RECOVERY_QUEUE_WORKFLOWS = [recoveryCrash, serialChild, serialParent] as const;

/** The implementations of the instance's tasks on `app`. */
export function implementRecoveryQueues(app: Horsies): void {
  app.implement(recoveryHealthcheck, (_input, { ok }) => ok("ready"));
  app.implement(recoveryStep, ({ step, delayMs }, { ok }) => {
    holdThread(delayMs);

    return ok(`step_${step}_done`);
  });
  app.implement(serialBlocker, ({ durationMs }, { ok }) => {
    holdThread(durationMs);

    return ok(`blocked_${String(durationMs)}`);
  });
  app.implement(serialChildTask, (_input, { ok }) => {
    holdThread(SHORT_NODE_MS);

    return ok(1);
  });
  app.implement(serialDirectTask, (_input, { ok }) => {
    holdThread(SHORT_NODE_MS);

    return ok(2);
  });
}
