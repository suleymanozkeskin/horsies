// The send process of the TypeScript side of a parity run.
//
// `<node|bun> clients/typescript/tests/parity/src/main.ts send <scenario>` sends the scenario's tasks, reruns the
// tasks of its reruns, starts its workflows, runs its actions on the started workflows, then pings, through the
// parity app on the database of `HORSIES_PARITY_DATABASE_URL`. It prints one JSON line per send, rerun, start and
// ping, in this order, as `crossclient/parity/send.py` prints them:
// - `{"kind":"task","index":0,"id":"<uuid>"}` for each send, then for each rerun (the new task), with one index
//   sequence;
// - `{"kind":"workflow","index":0,"id":"<uuid>"}` for each start;
// - `{"kind":"ping","index":0,"workers":2,"targeted":1}`: the count of workers that answered the ping of every worker,
//   and the count that answered the ping of the first of them by its id.
//
// stdout holds only these lines, printed once every step was written: the core's log sink writes to stderr. Exit
// status 0 when every step was written; 1 with the refusal on stderr; 2 for a name that no scenario has.

import {
  type AnyTaskContract,
  Horsies,
  type PingWorkersError,
  type RerunEnqueuePolicy,
  type TaskError,
  type TaskId,
  type TaskOutcome,
  type WorkflowHandleBase,
  type BuildReturn,
} from "../../../horsies/src/index.ts";
import type { TaskSendOptions } from "../../../horsies/src/send/options.ts";
import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";
import {
  add,
  addAsync,
  addThread,
  asyncPastDeadline,
  CHAIN_NAME,
  PARENT_NAME,
  parityChain,
  parityParent,
  refuse,
  refuseAsync,
  refusedBrokerConfig,
  refuseThread,
  sleepPastDeadline,
  threadPastDeadline,
  transient,
  transientAsync,
  transientThread,
  type WorkflowName,
} from "./app.ts";
import {
  type KeyChoice,
  type PairInput,
  type Rerun,
  type Scenario,
  scenarioNamed,
  type Send,
  type TaskInput,
  type WorkflowAction,
} from "./scenarios.ts";

/** The longest wait for the end of a rerun source, in milliseconds. */
export const RESULT_WAIT_MS = 30_000;

/** The window of one worker ping, in milliseconds. */
export const PING_WINDOW_MS = 2_000;

/** The pings of every worker before the send process gives up: a worker that has just started may not listen yet. */
export const PING_ATTEMPTS = 10;

/** The retries of a send of the form `retryOfRefusedConnection`. */
export const SEND_RETRIES = 2;

/** The code that a rerun source must end with. */
const REFUSED = "REFUSED";

/**
 * The policy of each rerun: the default retention class (30 days), the input kept, a 24 h key reservation (Python
 * `RerunEnqueuePolicy(retention=timedelta(days=30), retain_rerun_input=True, reservation_window=timedelta(hours=24))`,
 * Rust `RerunEnqueuePolicy::standard(true)`).
 */
const RERUN_POLICY: RerunEnqueuePolicy = {
  retention: { kind: "period", days: 30 },
  retainRerunInput: "keep",
  reservationWindow: Temporal.Duration.from({ hours: 24 }),
};

/** The exit status of the send command: every step written, a refused step, or a name that no scenario has. */
export const EXIT_SENT = 0;
export const EXIT_REFUSED = 1;
export const EXIT_UNKNOWN_SCENARIO = 2;

/** What the entry runs for its arguments: the send process of a scenario, or `cli.run` (worker, children, scheduler). */
export type EntryCommand = { readonly kind: "send"; readonly scenario: string } | { readonly kind: "cli" };

/** The command of the arguments after the entry script: `send <scenario>`, else `cli.run`. Pure. */
export function entryCommand(args: readonly string[]): EntryCommand {
  const [command, scenario, ...rest] = args;

  return command === "send" && scenario !== undefined && rest.length === 0 ? { kind: "send", scenario } : { kind: "cli" };
}

/** Where the send command writes: one line to stdout, one line to stderr. */
export interface SendOutput {
  line(text: string): void;
  error(text: string): void;
}

/** Run the scenario `name` through `app`, write its lines, and give the exit status. */
export async function runSendCommand(app: Horsies, name: string, output: SendOutput): Promise<number> {
  const lookup = scenarioNamed(name);

  if (lookup.kind === "notFound") {
    output.error(`horsies parity send: no scenario ${lookup.name}`);

    return EXIT_UNKNOWN_SCENARIO;
  }

  const lines = await runSends(app, lookup.scenario);

  if (!lines.ok) {
    output.error(`horsies parity send: ${lines.error}`);

    return EXIT_REFUSED;
  }

  for (const line of lines.value) {
    output.line(line);
  }

  return EXIT_SENT;
}

/** One task line: the task id, and the contract whose types read the task. */
type TaskLine = { readonly taskId: TaskId; readonly contract: AnyTaskContract };

/** The handle operations of a started workflow that the actions use. */
type StartedWorkflow = Pick<WorkflowHandleBase<BuildReturn>, "workflowId" | "pause" | "resume" | "cancel">;

/**
 * The lines of `scenario`: its sends, its reruns, its workflow starts, its actions and its pings, in this order.
 * Effects: the writes and reads of each step through `app`. The error names the first refused step; the steps before
 * it stay written.
 */
export async function runSends(app: Horsies, scenario: Scenario): Promise<Result<readonly string[], string>> {
  const tasks = await sendAndRerun(app, scenario);

  if (!tasks.ok) {
    return tasks;
  }

  const lines = tasks.value.map((line, index) => taskLineText(index, line.taskId));
  const handles: StartedWorkflow[] = [];

  for (const name of scenario.workflows) {
    const started = await startOne(app, name);

    if (!started.ok) {
      return started;
    }

    lines.push(workflowLineText(handles.length, started.value.workflowId));
    handles.push(started.value);
  }

  for (const action of scenario.actions) {
    const acted = await act(action, handles);

    if (!acted.ok) {
      return acted;
    }
  }

  switch (scenario.health) {
    case "pinged": {
      const pinged = await ping(app, scenario.profiles.length);

      return pinged.ok ? ok([...lines, pinged.value]) : pinged;
    }

    case "notPinged":
      return ok(lines);
    default: {
      const unexpected: never = scenario.health;

      throw new TypeError(`runSends: a health outside its type: ${String(unexpected)}`);
    }
  }
}

/** The task lines of the sends, then of the reruns, in order. */
async function sendAndRerun(app: Horsies, scenario: Scenario): Promise<Result<TaskLine[], string>> {
  const lines: TaskLine[] = [];

  for (const send of scenario.sends) {
    const sent = await sendOne(app, send);

    if (!sent.ok) {
      return sent;
    }

    lines.push(sent.value);
  }

  for (const rerun of scenario.reruns) {
    const rerunLine = await rerunOne(app, rerun, lines);

    if (!rerunLine.ok) {
      return rerunLine;
    }

    lines.push(rerunLine.value);
  }

  return ok(lines);
}

/** The task line of `send`. */
async function sendOne(app: Horsies, send: Send): Promise<Result<TaskLine, string>> {
  switch (send.form) {
    case "direct":
      return sendDirect(app, send, sendOptions(send.key));
    case "retryOfRefusedConnection":
      return sendRetry(app, send.input);
    default: {
      const unexpected: never = send;

      throw new TypeError(`sendOne: a send outside its type: ${String(unexpected)}`);
    }
  }
}

/** The send options of a key choice. Pure. */
export function sendOptions(key: KeyChoice): TaskSendOptions {
  switch (key.kind) {
    case "noKey":
      return {};
    case "key":
      return { idempotencyKey: key.key };
    default: {
      const unexpected: never = key;

      throw new TypeError(`sendOptions: a key outside its type: ${String(unexpected)}`);
    }
  }
}

/** The task line of one send of `send` through `app`, with `options`. */
async function sendDirect(app: Horsies, send: TaskInput, options: TaskSendOptions): Promise<Result<TaskLine, string>> {
  switch (send.task) {
    case "parity.add":
      return taskLine(send.task, add, await app.send(add, send.input, options));
    case "parity.add_async":
      return taskLine(send.task, addAsync, await app.send(addAsync, send.input, options));
    case "parity.add_thread":
      return taskLine(send.task, addThread, await app.send(addThread, send.input, options));
    case "parity.refuse":
      return taskLine(send.task, refuse, await app.send(refuse, send.input, options));
    case "parity.transient":
      return taskLine(send.task, transient, await app.send(transient, send.input, options));
    case "parity.refuse_async":
      return taskLine(send.task, refuseAsync, await app.send(refuseAsync, send.input, options));
    case "parity.transient_async":
      return taskLine(send.task, transientAsync, await app.send(transientAsync, send.input, options));
    case "parity.refuse_thread":
      return taskLine(send.task, refuseThread, await app.send(refuseThread, send.input, options));
    case "parity.transient_thread":
      return taskLine(send.task, transientThread, await app.send(transientThread, send.input, options));
    case "parity.sleep_past_deadline":
      return taskLine(send.task, sleepPastDeadline, await app.send(sleepPastDeadline, {}, options));
    case "parity.async_past_deadline":
      return taskLine(send.task, asyncPastDeadline, await app.send(asyncPastDeadline, {}, options));
    case "parity.thread_past_deadline":
      return taskLine(send.task, threadPastDeadline, await app.send(threadPastDeadline, {}, options));
    default: {
      const unexpected: never = send;

      throw new TypeError(`sendDirect: a send outside its type: ${String(unexpected)}`);
    }
  }
}

/** A failure of the catalog: its code and its message. */
type Failure = { readonly code: string; readonly message: string };

/** The text of a failure: its code and its message. Pure. */
function failureText(failure: Failure): string {
  return `${failure.code}: ${failure.message}`;
}

/** The task line of a send of `taskName` to `contract`, or the text of its refusal. Pure. */
function taskLine(
  taskName: string,
  contract: AnyTaskContract,
  sent: Result<{ readonly taskId: TaskId }, Failure>,
): Result<TaskLine, string> {
  return sent.ok
    ? ok({ taskId: sent.value.taskId, contract })
    : err(`the send of ${taskName} was refused: ${failureText(sent.error)}`);
}

/**
 * The task line that two retries of a refused send wrote. The first send of `parity.add` goes through a second app on
 * `REFUSED_BROKER_URL`, which ends `ENQUEUE_FAILED` with the prepared payload; each retry runs on `app`, under the
 * prepared task id, and both give that id.
 */
async function sendRetry(app: Horsies, input: PairInput): Promise<Result<TaskLine, string>> {
  const refusedApp = Horsies.create(refusedBrokerConfig());

  if (!refusedApp.ok) {
    return err(`the app of the refused broker was refused: ${refusedApp.error.refusals.map((refused) => refused.message).join("; ")}`);
  }

  const first = await refusedApp.value.send(add, input);

  if (first.ok) {
    return err(`the send of ${add.name} through the refused broker wrote task ${first.value.taskId}`);
  }

  if (first.error.code !== "ENQUEUE_FAILED") {
    return err(`the send of ${add.name} through the refused broker answered ${failureText(first.error)}`);
  }

  const refused = first.error;
  const ids: TaskId[] = [];

  for (let attempt = 0; attempt < SEND_RETRIES; attempt += 1) {
    const retried = await app.resend(refused);

    if (!retried.ok) {
      return err(`the retry of ${add.name} was refused: ${failureText(retried.error)}`);
    }

    ids.push(retried.value.taskId);
  }

  const [written] = ids;

  if (written === undefined || ids.some((id) => id !== refused.taskId)) {
    return err(`the retries of ${add.name} wrote other tasks than ${refused.taskId}: ${ids.join(", ")}`);
  }

  return ok({ taskId: written, contract: add });
}

/** The code of a stored task error: who chose it (`builtIn` or `custom`) and the code. */
export type StoredCode = { readonly origin: TaskError["kind"]; readonly code: string };

/**
 * The code of the stored error `error`. The one place of this program that reads the code of an error in a read, so
 * a change of the read form of `TaskError` changes this function only. Pure.
 */
export function storedCode(error: TaskError): StoredCode {
  return { origin: error.kind, code: error.code };
}

/** Whether `outcome` is a failure with the custom code `REFUSED`. Pure. */
export function endedRefused(outcome: TaskOutcome<unknown, string>): boolean {
  switch (outcome.kind) {
    case "succeeded":
      return false;
    case "failed": {
      const code = storedCode(outcome.error);

      return code.origin === "custom" && code.code === REFUSED;
    }

    default: {
      const unexpected: never = outcome;

      throw new TypeError(`endedRefused: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The text of an outcome for a refusal: `succeeded`, or the origin and code of the failure. Pure. */
export function outcomeText(outcome: TaskOutcome<unknown, string>): string {
  switch (outcome.kind) {
    case "succeeded":
      return "succeeded";
    case "failed": {
      const code = storedCode(outcome.error);

      return `failed with the ${code.origin} code ${code.code}`;
    }

    default: {
      const unexpected: never = outcome;

      throw new TypeError(`outcomeText: an outcome outside its type: ${String(unexpected)}`);
    }
  }
}

/** The task line of the new task of `rerun`, after the end of its source with `REFUSED`. */
async function rerunOne(app: Horsies, rerun: Rerun, lines: readonly TaskLine[]): Promise<Result<TaskLine, string>> {
  const source = lines[rerun.source];

  if (source === undefined) {
    return err(`the rerun of task line ${String(rerun.source)} names no earlier task line`);
  }

  const ended = await app.taskHandle(source.contract, source.taskId).get({ waitMs: RESULT_WAIT_MS });

  if (!ended.ok) {
    return err(`the rerun source ${source.taskId} was not read: ${ended.error.kind}`);
  }

  if (!endedRefused(ended.value)) {
    return err(`the rerun source ${source.taskId} did not end with ${REFUSED}: ${outcomeText(ended.value)}`);
  }

  const outcome = await app.rerunTask({ sourceTaskId: source.taskId, goodUntil: { kind: "none" } }, RERUN_POLICY);

  if (!outcome.ok) {
    return err(`the rerun of ${source.taskId} was refused: ${failureText(outcome.error)}`);
  }

  return outcome.value.kind === "enqueued"
    ? ok({ taskId: outcome.value.newTaskId, contract: source.contract })
    : err(`the rerun of ${source.taskId} answered ${outcome.value.kind}`);
}

/** The handle of the started workflow `name`. */
async function startOne(app: Horsies, name: WorkflowName): Promise<Result<StartedWorkflow, string>> {
  switch (name) {
    case CHAIN_NAME:
      return startedWorkflow(name, await app.start(parityChain, {}));
    case PARENT_NAME:
      return startedWorkflow(name, await app.start(parityParent, {}));
    default: {
      const unexpected: never = name;

      throw new TypeError(`startOne: a workflow outside its type: ${String(unexpected)}`);
    }
  }
}

/** The handle of a start of `name`, or the text of its refusal. Pure. */
function startedWorkflow(name: WorkflowName, started: Result<StartedWorkflow, Failure>): Result<StartedWorkflow, string> {
  return started.ok ? started : err(`the start of ${name} was refused: ${failureText(started.error)}`);
}

/** Run `action` on its started workflow. A pause and a resume must answer true. */
async function act(action: WorkflowAction, handles: readonly StartedWorkflow[]): Promise<Result<null, string>> {
  const handle = handles[action.index];
  const named = `the ${action.kind} of workflow ${String(action.index)}`;

  if (handle === undefined) {
    return err(`${named} names no started workflow`);
  }

  switch (action.kind) {
    case "pause":
      return answeredTrue(named, await handle.pause());
    case "resume":
      return answeredTrue(named, await handle.resume());
    case "cancel": {
      const cancelled = await handle.cancel();

      return cancelled.ok ? ok(null) : err(`${named} was refused: ${failureText(cancelled.error)}`);
    }

    default: {
      const unexpected: never = action.kind;

      throw new TypeError(`act: an action outside its type: ${String(unexpected)}`);
    }
  }
}

/** `null` when the action `named` answered true; else the text of the answer or the refusal. Pure. */
function answeredTrue(named: string, answered: Result<boolean, Failure>): Result<null, string> {
  if (!answered.ok) {
    return err(`${named} was refused: ${failureText(answered.error)}`);
  }

  return answered.value ? ok(null) : err(`${named} answered false`);
}

/** The ping line: the database answers, then each of the `expected` workers, then the first of them by its id. */
async function ping(app: Horsies, expected: number): Promise<Result<string, string>> {
  const database = await app.pingDatabase();

  if (!database.ok) {
    return err(`pingDatabase was refused: ${failureText(database.error)}`);
  }

  const answered = await everyWorker(app, expected);

  if (!answered.ok) {
    return answered;
  }

  const [first] = answered.value;

  if (first === undefined) {
    return err("no worker answered");
  }

  const targeted = await app.pingWorkers({ target: { kind: "worker", workerId: first.workerId }, waitMs: PING_WINDOW_MS });

  if (!targeted.ok) {
    return err(`the ping of worker ${first.workerId} was refused: ${pingWorkersErrorText(targeted.error)}`);
  }

  return ok(pingLineText(answered.value.length, targeted.value.length));
}

/** The text of a refused worker ping: its code, and its message or the refused field and the reason. Pure. */
function pingWorkersErrorText(error: PingWorkersError): string {
  return "reason" in error ? `${error.code}: ${error.field}: ${error.reason}` : failureText(error);
}

/** The answers to a ping of every worker, once `expected` workers answered one ping, at most `PING_ATTEMPTS` pings. */
async function everyWorker(app: Horsies, expected: number): Promise<Result<readonly { readonly workerId: string }[], string>> {
  let answered = 0;

  for (let attempt = 0; attempt < PING_ATTEMPTS; attempt += 1) {
    const pinged = await app.pingWorkers({
      waitMs: PING_WINDOW_MS,
      minResponses: { kind: "atLeast", count: expected },
    });

    if (!pinged.ok) {
      return err(`pingWorkers was refused: ${pingWorkersErrorText(pinged.error)}`);
    }

    if (pinged.value.length >= expected) {
      return ok(pinged.value);
    }

    answered = pinged.value.length;
  }

  return err(`${String(answered)} of ${String(expected)} workers answered the last of ${String(PING_ATTEMPTS)} pings`);
}

/** The line of the task at `index`. Pure. */
export function taskLineText(index: number, taskId: string): string {
  return JSON.stringify({ kind: "task", index, id: taskId });
}

/** The line of the started workflow at `index`. Pure. */
export function workflowLineText(index: number, workflowId: string): string {
  return JSON.stringify({ kind: "workflow", index, id: workflowId });
}

/** The ping line: the workers that answered the ping of every worker, and the ping of one by its id. Pure. */
export function pingLineText(workers: number, targeted: number): string {
  return JSON.stringify({ kind: "ping", index: 0, workers, targeted });
}
