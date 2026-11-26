// The scheduler manifest of a declaration (the model: the Rust client's `scheduler_manifest`). Per schedule, in
// declaration order: a disabled schedule is its name; an enabled schedule is its pattern text, its zone, its catch-up
// rule and the send facts of each slot: the task, the queue and its priority, the input text, the task's stored
// options text, the retention class key of a send that chooses none, and the rerun-input default of the config.
//
// The builder writes the document with the generated `schedulerManifestToWire` and the manifest text writer; the
// core's parser checks every bound once (`prepareScheduler`): the check interval, the catch-up count, the pattern, a
// repeated name.

import {
  MANIFEST_VERSION,
  type ScheduleManifest,
  schedulerManifestToWire,
  type ScheduleSendManifest,
} from "../_catalog/scheduler_manifest.ts";
import type { QueueSendFacts, SchedulerConfigFacts } from "../native/scheduler/addon.ts";
import type { SendPriority } from "../native/sends/addon.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { DEFAULT_QUEUE_PRIORITY } from "../send/bounds.ts";
import type { ListedTask } from "../send/send.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { manifestText } from "../worker/manifest-text.ts";
import type { CheckedActivation, CheckedSchedule } from "./checks.ts";

/** The section of a declaration, with the defaults of the fields that it leaves out. */
export type SectionSettings = { readonly enabled: boolean; readonly checkIntervalSeconds: number };

/** What the manifest takes from the core: the facts of the accepted config, and the stored options text of a task. */
export type ManifestFacts = {
  readonly config: SchedulerConfigFacts;
  readonly optionsText: (task: ListedTask) => string;
};

/** The text of the scheduler manifest of `section` and `schedules`. */
export function schedulerManifestText(
  section: SectionSettings,
  schedules: readonly CheckedSchedule[],
  facts: ManifestFacts,
): string {
  return manifestText(
    schedulerManifestToWire({
      manifest_version: MANIFEST_VERSION,
      enabled: section.enabled,
      check_interval_seconds: section.checkIntervalSeconds,
      schedules: schedules.map((schedule) => scheduleManifest(schedule, facts)),
    }),
  );
}

/** One schedule of the manifest. */
function scheduleManifest(schedule: CheckedSchedule, facts: ManifestFacts): ScheduleManifest {
  switch (schedule.kind) {
    case "disabled":
      return { name: schedule.name, activation: { kind: "DISABLED" } };
    case "enabled": {
      const { activation } = schedule;

      return {
        name: schedule.name,
        activation: {
          kind: "ENABLED",
          pattern_json: activation.patternText,
          timezone: activation.timezone,
          catch_up_missed: activation.catchUpMissed,
          max_catch_up_runs: activation.maxCatchUpRuns,
          send: sendManifest(activation, facts),
        },
      };
    }

    default:
      return unreachable(schedule, "scheduleManifest");
  }
}

/** The send facts of each slot: what a plain send of the input on the queue stores. */
function sendManifest(activation: CheckedActivation, facts: ManifestFacts): ScheduleSendManifest {
  const queue = queueFacts(facts.config, activation.queueName);

  return {
    task_name: activation.task.compiled.record.name,
    queue_name: activation.queueName,
    priority: queuePriority(queue.priority),
    input_json: activation.inputJson,
    task_options: { kind: "ENCODED", json: facts.optionsText(activation.task) },
    retention_class_key: queue.retentionClassKey,
    retain_rerun_input: facts.config.retainRerunInput,
  };
}

/** The facts of `queueName`. The declaration asks for every queue of the app, so a missing queue is a defect. */
function queueFacts(config: SchedulerConfigFacts, queueName: string): QueueSendFacts {
  const found = config.queues.find((queue) => queue.queueName === queueName);

  return found ?? raiseDefect(new ClientDefect({ subject: "schedulerManifestText", rule: `the config facts hold the queue ${queueName}` }));
}

/** The priority number of a queue: the package's priority of the default queue, or the configured one. Pure. */
function queuePriority(priority: SendPriority): number {
  switch (priority.kind) {
    case "defaultQueue":
      return DEFAULT_QUEUE_PRIORITY;
    case "configured":
      return priority.priority;
    default:
      return unreachable(priority, "queuePriority");
  }
}
