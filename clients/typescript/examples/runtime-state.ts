// App state and the task runtime: `provide` gives a value to task bodies, which read it with `state(key)` of their
// second argument; `onChildStart` runs once in each worker child before its first task; a task body sends another
// task.
//
// Every worker child runs this entry again, so each child builds its own state.
//
//   node examples/runtime-state.ts worker --profile default
//   node examples/runtime-state.ts demo

import { defineTask, h, Horsies, type Result } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  ISOLATED_PROFILE,
  printLine,
  readLine,
  refusalLines,
  RESULT_WAIT_MS,
  runExample,
  sendErrorText,
} from "./support.ts";

/** The state that task bodies read. */
type Greeter = { readonly greeting: string };

const welcome = defineTask({
  name: "welcome",
  input: { user: h.string() },
  output: h.string(),
  errorCodes: ["AUDIT_NOT_SENT"],
});

const audit = defineTask({ name: "audit", input: { entry: h.string() }, output: h.int() });

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [welcome, audit],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

// The factory runs in each task child before the child reports ready; the key exists only with its provider.
const greeterKey = app.provide((): Greeter => ({ greeting: "welcome" }));

app.onChildStart(() => {
  process.stderr.write(`worker child ${String(process.pid)} is ready\n`);
});

app.implement(welcome, async ({ user }, { ok, err, state }) => {
  const message = `${state(greeterKey).greeting}, ${user}`;
  // A send from a task body goes through the send pool of the child.
  const sent = await app.send(audit, { entry: message });

  return sent.ok ? ok(message) : err({ code: "AUDIT_NOT_SENT", message: sendErrorText(sent.error) });
});

app.implement(audit, async ({ entry }, { ok }) => ok(entry.length));

async function demo(): Promise<Result<void, string>> {
  const sent = await app.send(welcome, { user: "ada" });

  if (!sent.ok) {
    return demoFailed(`send welcome: ${sendErrorText(sent.error)}`);
  }

  printLine(readLine("welcome", await sent.value.get({ waitMs: RESULT_WAIT_MS }), (message) => message));

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
