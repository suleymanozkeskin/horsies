// Schedules: the scheduler sends a task on a typed pattern. A schedule names its task contract, so its input is checked
// against the task's input types at typecheck, and again by `check` and the scheduler start.
//
//   node examples/schedules.ts check
//   node examples/schedules.ts scheduler
//   node examples/schedules.ts worker --profile default
//   node examples/schedules.ts demo
//
// `demo` prints each schedule and the state that the scheduler stored for it.

import { defineTask, h, Horsies, type Result, type ScheduleStateInfo } from "horsies";

import { DATABASE_URL, DEFAULT_PROFILE, DEMO_DONE, demoFailed, ISOLATED_PROFILE, printLine, refusalLines, runExample } from "./support.ts";

const recordHeartbeat = defineTask({ name: "record_heartbeat", input: { source: h.string() }, output: h.string() });

const compactLogs = defineTask({ name: "compact_logs", input: { keepDays: h.int() }, output: h.int() });

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [recordHeartbeat, compactLogs],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(recordHeartbeat, ({ source }) => recordHeartbeat.ok(`heartbeat from ${source}`));

app.implement(compactLogs, ({ keepDays }) => compactLogs.ok(keepDays));

/** The schedules: a 10 s interval, and the fifth minute of each hour. A wrong input fails typecheck at its entry. */
const SCHEDULES = [
  {
    name: "heartbeat-every-10s",
    task: recordHeartbeat,
    input: { source: "scheduler" },
    pattern: { kind: "interval", every: Temporal.Duration.from({ seconds: 10 }) },
  },
  {
    name: "compact-hourly",
    task: compactLogs,
    input: { keepDays: 7 },
    pattern: { kind: "hourly", minute: 5 },
  },
] as const;

app.schedules(SCHEDULES);

/** The line of the stored state of one schedule. Pure. */
function stateLine(state: ScheduleStateInfo): string {
  const next = state.nextRunAt === null ? "no next slot" : `next slot ${state.nextRunAt.toString()}`;

  return `  ${state.scheduleName}: ${String(state.runCount)} slot(s) sent, ${next}`;
}

async function demo(): Promise<Result<void, string>> {
  for (const entry of SCHEDULES) {
    printLine(`${entry.name} sends ${entry.task.name} with ${JSON.stringify(entry.input)}`);
  }

  const states = await app.monitoring.schedules();

  if (!states.ok) {
    return demoFailed(`read the schedule states: ${states.error.code}: ${states.error.message}`);
  }

  printLine(states.value.length === 0 ? "no stored state yet: run the scheduler first" : "stored states:");

  for (const state of states.value) {
    printLine(stateLine(state));
  }

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
