// A seat isolate entry: the spy on native module loads first, then `configureLogging` at the top of the entry (as an
// app entry calls it), an app with `SHARED_THREAD` tasks, and `cli.run(app)`, as an app entry runs in each seat
// isolate. The test runs it in a `worker_threads` isolate with the seat marker and plays the main thread of the child.
//
// - `probe`: the number of native module loads in this isolate.
// - `spin`: a sync body that holds the isolate until its cancel cell holds a cause, at most `SPIN_BOUND_MS`; 1 when the
//   cause is DEADLINE, 0 at the bound.
// - `send_and_wait`: sends `probe`, then waits for its result; the text names the send and the read error.
// - `exit_now`: ends its isolate with `process.exit(3)`.
// - `throw_later`: a callback that it scheduled throws; the body returns 5 after the throw.
// - `reject_unawaited`: leaves a rejected Promise that it does not await; the body returns 7.
// - `foreign_state`: reads the state of a key of another app (a defect of the package).
// - `facades_probe`: pings the database and cancels a task through the facades; the text names both outcomes.

import { dlopenFiles } from "./dlopen-spy.ts";

import { cli, configureLogging, defineTask, h, Horsies, TaskId } from "../../../../horsies/src/index.ts";

configureLogging({ levels: { default: "INFO" } });

/** The longest hold of `spin`, in milliseconds. */
const SPIN_BOUND_MS = 5000;

const probe = defineTask({ name: "probe", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const spin = defineTask({ name: "spin", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const sendAndWait = defineTask({
  name: "send_and_wait",
  input: {},
  output: h.string(),
  execution: "SHARED_THREAD",
  timeoutMs: 5000,
});

// The isolate connects nowhere: the URL is never opened.
const databaseUrl = { kind: "value", value: "postgresql://postgres@127.0.0.1:55432/horsies" } as const;

const exitNow = defineTask({ name: "exit_now", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const throwLater = defineTask({ name: "throw_later", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const rejectUnawaited = defineTask({ name: "reject_unawaited", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const foreignState = defineTask({ name: "foreign_state", input: {}, output: h.int(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const facadesProbe = defineTask({ name: "facades_probe", input: {}, output: h.string(), execution: "SHARED_THREAD", timeoutMs: 5000 });

const created = Horsies.create({
  broker: { databaseUrl },
  tasks: [probe, spin, sendAndWait, exitNow, throwLater, rejectUnawaited, foreignState, facadesProbe],
});

const otherApp = Horsies.create({ broker: { databaseUrl }, tasks: [] });

if (!otherApp.ok) {
  throw new Error("the second fixture app was refused");
}

/** A state key of another app: `runtime.state` of it is a defect. */
const foreignKey = otherApp.value.provide(() => 1);

if (!created.ok) {
  throw new Error(`the fixture app was refused: ${created.error.refusals.map((refused) => refused.message).join("; ")}`);
}

const app = created.value;
const sleepCell = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));

app.implement(probe, (_input, { ok }) => ok(dlopenFiles.length));

app.implement(spin, (_input, runtime) => {
  for (let waited = 0; waited < SPIN_BOUND_MS; waited += 1) {
    const state = runtime.cancelState();

    if (state.kind === "requested") {
      return runtime.ok(state.cause === "DEADLINE" ? 1 : 0);
    }

    // A sync sleep of 1 ms: the isolate's event loop does not turn while the body holds it.
    Atomics.wait(sleepCell, 0, 0, 1);
  }

  return runtime.ok(0);
});

app.implement(sendAndWait, async (_input, { ok }) => {
  const sent = await app.send(probe, {});

  if (!sent.ok) {
    return ok(`send failed: ${sent.error.code}`);
  }

  const read = await sent.value.get({ waitMs: 100 });

  return ok(`sent ${sent.value.taskId}; ${read.ok ? "read a result" : `read error ${read.error.kind}`}`);
});

app.implement(exitNow, () => {
  process.exit(3);
});

app.implement(throwLater, async (_input, { ok }) => {
  setTimeout(() => {
    throw new RangeError("a late throw");
  }, 0);

  await new Promise((resolve) => setTimeout(resolve, 50));

  return ok(5);
});

app.implement(rejectUnawaited, async (_input, { ok }) => {
  void Promise.reject(new TypeError("not awaited"));

  await new Promise((resolve) => setTimeout(resolve, 50));

  return ok(7);
});

app.implement(foreignState, (_input, runtime) => runtime.ok(runtime.state(foreignKey)));

app.implement(facadesProbe, async (_input, { ok }) => {
  const ping = await app.pingDatabase();
  const taskId = TaskId.parse("0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b");

  if (!taskId.ok) {
    return ok("the task id does not parse");
  }

  const cancel = await app.monitoring.cancelTask(taskId.value, { policy: "includeRunning" });

  return ok(`ping ${ping.ok ? String(ping.value.latencyMs) : ping.error.code}; cancel ${cancel.ok ? "ok" : cancel.error.code}`);
});

await cli.run(app);
