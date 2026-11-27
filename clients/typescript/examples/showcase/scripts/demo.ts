// The bounded run of the showcase: `seed`, one worker and one scheduler, each scenario in turn, the stop of the
// scheduler, the wait until no task is live, the report, and the stop of the worker. `scripts/run.sh` runs it under the watchdog. Every step is the entry of the
// showcase in a process of its own, as an operator runs it.
//
// Exit code: 0 when every step passed and the worker stopped after its drain; 1 otherwise.

import type { Horsies, Result } from "horsies";

import { appRefusedText, buildShowcase, WORKER_PROFILE } from "../src/app.ts";
import { err, ok } from "../src/result.ts";
import { processDatabaseSettings, settingsRefusedText } from "../src/settings.ts";
import { type EntryProcess, logTail, runEntry, startEntry, stopEntry } from "./entry-process.ts";

/** The scenarios of the run, in order, each with its arguments. `steady` is compressed by its pace. */
const SCENARIO_STEPS: readonly (readonly string[])[] = [
  ["steady", "--orders", "20", "--cover-errors", "--pace", "20"],
  ["rush"],
  ["problem-child"],
  ["bulk-import"],
  ["flash-sale"],
  ["chaos"],
  ["maintenance"],
];

/** The task statuses of work that has not ended. */
const LIVE_STATUSES: readonly string[] = ["PENDING", "CLAIMED", "RUNNING"];

/** The longest wait for the live tasks to end, and the pause between two reads. */
const DRAIN_WAIT_MS = 420_000;
const DRAIN_POLL_MS = 5_000;

/** The worker log lines that a failed run shows. */
const LOG_TAIL_LINES = 60;

function say(line: string): void {
  process.stdout.write(`${line}\n`);
}

/** Run one step of the entry and print its output. */
async function step(args: readonly string[], env: Readonly<Record<string, string>>): Promise<Result<void, string>> {
  say(`\n$ acme ${args.join(" ")}`);

  const run = await runEntry(args, env);

  process.stdout.write(run.output);

  return run.code === 0 ? ok(undefined) : err(`acme ${args.join(" ")} ended with ${String(run.code)}`);
}

/** The count of the live tasks of `app`, or the read failure. */
async function liveTasks(app: Horsies): Promise<Result<number, string>> {
  const stats = await app.monitoring.taskStatusCounts();

  if (!stats.ok) {
    return err(`the task status counts read failed: ${stats.error.code}`);
  }

  return ok(stats.value.filter((count) => LIVE_STATUSES.includes(count.status)).reduce((sum, count) => sum + count.count, 0));
}

/** Wait until no task is live, at most `DRAIN_WAIT_MS`. */
async function drain(app: Horsies): Promise<Result<void, string>> {
  for (let waited = 0; waited <= DRAIN_WAIT_MS; waited += DRAIN_POLL_MS) {
    const live = await liveTasks(app);

    if (!live.ok) {
      return live;
    }

    if (live.value === 0) {
      say(`\nno live task after ${String(Math.round(waited / 1_000))} s`);

      return ok(undefined);
    }

    await new Promise((resolve) => setTimeout(resolve, DRAIN_POLL_MS));
  }

  return err(`tasks are still live after ${String(DRAIN_WAIT_MS / 1_000)} s`);
}

/** The scenarios with `scheduler` running, the stop of the scheduler, the drain and the report. */
async function steps(app: Horsies, scheduler: EntryProcess, env: Readonly<Record<string, string>>): Promise<Result<void, string>> {
  for (const args of SCENARIO_STEPS) {
    const ran = await step(args, env);

    if (!ran.ok) {
      return ran;
    }
  }

  const schedulerStop = await stopEntry(scheduler);

  say(`\nscheduler stop: ${schedulerStop}`);

  if (schedulerStop !== "exited" || scheduler.child.exitCode !== 0) {
    return err(`the scheduler did not stop after its run:\n${logTail(scheduler, LOG_TAIL_LINES)}`);
  }

  const drained = await drain(app);

  return drained.ok ? step(["report", "--failed", "12"], env) : drained;
}

async function main(): Promise<number> {
  const settings = processDatabaseSettings();

  if (!settings.ok) {
    say(settingsRefusedText(settings.error));

    return 1;
  }

  const built = buildShowcase(settings.value);

  if (!built.ok) {
    say(appRefusedText(built.error));

    return 1;
  }

  const env = { ACME_DATABASE_URL: settings.value.url };
  const seeded = await step(["seed"], env);

  if (!seeded.ok) {
    say(seeded.error);

    return 1;
  }

  say(`\n$ acme worker --profile ${WORKER_PROFILE} (in the background)`);

  const worker: EntryProcess = startEntry(["worker", "--profile", WORKER_PROFILE], env);

  say("$ acme scheduler (in the background)");

  const scheduler: EntryProcess = startEntry(["scheduler"], env);
  const ran = await steps(built.value.app, scheduler, env);
  const stopped = await stopEntry(worker);

  say(`\nworker stop: ${stopped}`);

  if (!ran.ok || stopped !== "exited") {
    say(ran.ok ? "the worker did not stop after its drain" : ran.error);
    say(`worker log (${worker.logPath}):\n${logTail(worker, LOG_TAIL_LINES)}`);

    return 1;
  }

  return 0;
}

process.exitCode = await main();
