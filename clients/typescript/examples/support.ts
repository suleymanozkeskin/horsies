// What every example shares: the database URL, a worker profile, the entry that runs the example's demo or hands
// over to `cli.run`, and the texts of a read.
//
// Each example is one entry file. The same entry is the worker, each of its children and the producer:
//
//   node examples/basic-tasks.ts worker --profile default
//   node examples/basic-tasks.ts demo
//
// `DATABASE_URL` names the database; each example installs the schema at its first operation. In this workspace, run
// an example with the dev start module (`dev/source-package.ts`).

import {
  cli,
  type Horsies,
  type ReadError,
  type Result,
  type Secret,
  type TaskError,
  type TaskOutcome,
  type WorkerProfileInput,
  type WorkflowReadError,
} from "horsies";

/** The database of every example: the URL in `DATABASE_URL`, read at `Horsies.create`. */
export const DATABASE_URL: Secret = { kind: "fromEnv", name: "DATABASE_URL" };

/** The name of the worker profile that every example defines. */
export const DEFAULT_PROFILE = "default";

/** A worker profile of two isolated children: each runs one task at a time in its own process. */
export const ISOLATED_PROFILE: WorkerProfileInput = { isolated: { count: 2 } };

/** The longest wait of a demo for one result, in milliseconds: the worker starts its children first. */
export const RESULT_WAIT_MS = 30_000;

/** The longest wait of a demo for a result that waits for retries or for a workflow, in milliseconds. */
export const LONG_RESULT_WAIT_MS = 60_000;

/** The command that runs an example's demo instead of a command of `cli.run`. */
const DEMO_COMMAND = "demo";

/** The demo of an example: it sends and reads, and gives the text of its failure. */
export type Demo = (app: Horsies) => Promise<Result<void, string>>;

/** The end of a demo that ran to its end. */
export const DEMO_DONE: Result<void, string> = Object.freeze({ ok: true, value: undefined });

/** The end of a demo that could not go on: `text` names the step and the cause. Pure. */
export function demoFailed(text: string): Result<void, string> {
  return { ok: false, error: text };
}

/**
 * The entry of an example: `<entry> demo` runs `demo`; any other command goes to `cli.run(app)`, which serves a child
 * of the worker first, then `worker`, `check`, `web` and `metrics`. Resolves with the exit code of the process.
 */
export async function runExample(app: Horsies, demo: Demo): Promise<number> {
  if (process.argv[2] !== DEMO_COMMAND) {
    return cli.run(app);
  }

  const ran = await demo(app);

  if (!ran.ok) {
    process.stderr.write(`the demo failed: ${ran.error}\n`);

    return 1;
  }

  return 0;
}

/** The text of every refusal of `Horsies.create`, one per line. Pure. */
export function refusalLines(refusals: readonly { readonly message: string }[]): string {
  return refusals.map((refusal) => refusal.message).join("\n");
}

/** Who chose the code of a task error: the task (`user`) or the library (`library`). */
export type CodeSource = "user" | "library";

/** The code of a task error and who chose it. */
export type FailureCode = { readonly source: CodeSource; readonly code: string };

/**
 * The code of a task error. This is the one place where the examples read the code shape of an error (`code.kind`
 * and `code.code`), so a change of that shape changes this function only. Pure.
 */
export function failureCode(error: TaskError): FailureCode {
  return { source: error.code.kind, code: error.code.code };
}

/** The code text of a task error, for example `OVER_LIMIT`. Pure. */
export function failureCodeText(error: TaskError): string {
  return failureCode(error).code;
}

/** `CODE: message`, or the code alone when the error has no message. Pure. */
export function failureText(error: TaskError): string {
  const code = failureCodeText(error);

  return error.message === null ? code : `${code}: ${error.message}`;
}

/** The text of a task read that gave no outcome. Pure. */
export function readErrorText(error: ReadError): string {
  switch (error.kind) {
    case "waitTimedOut":
      return `the task is ${error.status} and has not finished; is a worker running?`;
    case "taskNotFound":
      return `no task has this id (${error.absence})`;
    case "resultNotAvailable":
      return `the task is ${error.status} and stores no result`;
    case "storedResultUnreadable":
      return `the stored result does not read with the contract: ${error.cause}`;
    case "unknownUserCode":
      return `the stored error has the code ${error.code}, which the contract does not declare`;
    case "waitInsideTask":
      return "a wait inside a task body";
    case "database":
      return `the database read failed (${error.code}): ${error.message}`;
    case "invalidWait":
      return `the wait is not a whole number of milliseconds from 0 to ${String(error.maxMs)}`;
    default: {
      const unexpected: never = error;

      throw new TypeError(`readErrorText: a read error outside its type: ${String(unexpected)}`);
    }
  }
}

/** The text of a workflow read that gave no outcome. Pure. */
export function workflowReadErrorText(error: WorkflowReadError): string {
  switch (error.kind) {
    case "waitTimedOut":
      return "the workflow has not ended; is a worker running?";
    case "workflowNotFound":
      return "no workflow has this id";
    case "resultNotReady":
      return "the node stores no result yet";
    case "storedResultUnreadable":
      return `the stored result does not read with the contract: ${error.cause}`;
    case "unknownUserCode":
      return `the stored error has the code ${error.code}, which the contract does not declare`;
    case "waitInsideTask":
      return "a wait inside a task body";
    case "workflowNotListed":
      return `the app does not list the workflow ${error.workflowName}`;
    case "database":
      return `the database read failed (${error.code}): ${error.message}`;
    case "invalidWait":
      return `the wait is not a whole number of milliseconds from 0 to ${String(error.maxMs)}`;
    default: {
      const unexpected: never = error;

      throw new TypeError(`workflowReadErrorText: a read error outside its type: ${String(unexpected)}`);
    }
  }
}

/** `label: value` for a success (the value as JSON text), `label: failed: CODE: message` for a failure. Pure. */
export function outcomeLine<O>(label: string, outcome: TaskOutcome<O, string>, valueText: (value: O) => string): string {
  switch (outcome.kind) {
    case "succeeded":
      return `${label}: ${valueText(outcome.value)}`;
    case "failed":
      return `${label}: failed: ${failureText(outcome.error)}`;
    default: {
      const unexpected: never = outcome;

      throw new TypeError(`outcomeLine: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The line of a task read: its outcome line, or why the read gave no outcome. Pure. */
export function readLine<O>(
  label: string,
  read: Result<TaskOutcome<O, string>, ReadError>,
  valueText: (value: O) => string,
): string {
  return read.ok ? outcomeLine(label, read.value, valueText) : `${label}: the read failed: ${readErrorText(read.error)}`;
}

/** Write one line to stdout. */
export function printLine(line: string): void {
  process.stdout.write(`${line}\n`);
}

/** `CODE: message` of a failed send or start. Pure. */
export function sendErrorText(error: { readonly code: string; readonly message: string }): string {
  return `${error.code}: ${error.message}`;
}
