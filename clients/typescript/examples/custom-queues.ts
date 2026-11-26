// Custom queues: each queue has a priority (1 is claimed first) and an optional cap on its running tasks. A task names
// its queue.
//
//   node examples/custom-queues.ts worker --profile default
//   node examples/custom-queues.ts demo

import { defineTask, h, Horsies, type Result, type TaskHandle } from "horsies";

import {
  DATABASE_URL,
  DEFAULT_PROFILE,
  DEMO_DONE,
  demoFailed,
  ISOLATED_PROFILE,
  LONG_RESULT_WAIT_MS,
  printLine,
  readLine,
  refusalLines,
  runExample,
  sendErrorText,
} from "./support.ts";

/** The shards that the demo rebuilds. */
const SHARD_COUNT = 4;

const sendAlert = defineTask({
  name: "send_alert",
  queue: "critical",
  input: { message: h.string() },
  output: h.string(),
});

const rebuildIndex = defineTask({
  name: "rebuild_index",
  queue: "bulk",
  input: { shard: h.int() },
  output: h.int(),
});

const created = Horsies.create({
  broker: { databaseUrl: DATABASE_URL },
  queueMode: "custom",
  customQueues: [
    { name: "critical", priority: 1 },
    { name: "bulk", priority: 50, maxConcurrency: { kind: "cap", tasks: 1 } },
  ],
  workerProfiles: { [DEFAULT_PROFILE]: ISOLATED_PROFILE },
  tasks: [sendAlert, rebuildIndex],
});

if (!created.ok) {
  process.stderr.write(`${refusalLines(created.error.refusals)}\n`);
  process.exit(1);
}

const app = created.value;

app.implement(sendAlert, async ({ message }, { ok }) => ok(`alert sent: ${message}`));

app.implement(rebuildIndex, async ({ shard }, { ok }) => ok(shard));

async function demo(): Promise<Result<void, string>> {
  const shards: TaskHandle<number, string>[] = [];

  for (let shard = 0; shard < SHARD_COUNT; shard += 1) {
    const sent = await app.send(rebuildIndex, { shard });

    if (!sent.ok) {
      return demoFailed(`send rebuild_index ${String(shard)}: ${sendErrorText(sent.error)}`);
    }

    shards.push(sent.value);
  }

  // Sent last, claimed first: `critical` has the higher priority.
  const alert = await app.send(sendAlert, { message: "disk almost full" });

  if (!alert.ok) {
    return demoFailed(`send send_alert: ${sendErrorText(alert.error)}`);
  }

  printLine(readLine("send_alert", await alert.value.get({ waitMs: LONG_RESULT_WAIT_MS }), (text) => text));

  for (const shard of shards) {
    const read = await shard.get({ waitMs: LONG_RESULT_WAIT_MS });

    printLine(readLine("rebuild_index", read, (done) => `rebuilt shard ${String(done)}`));
  }

  return DEMO_DONE;
}

process.exitCode = await runExample(app, demo);
