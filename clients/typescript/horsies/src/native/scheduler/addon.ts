// The functions of the `scheduler` area of the addon, their records, and the accessor.
//
// - `checkSchedulePattern`, `schedulerConfigFacts` and `prepareScheduler` are pure: they start no core, so a task
//   child that runs the entry before `cli.run` still lets the child host shape its process runtime.
// - `runScheduler` is a service run on the core's process runtime: it starts the core of the process.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";
import type { SendPriority } from "../sends/addon.ts";

/** The outcome of the core's check of one pattern text: the rules hold, or the cause of the rule that breaks. */
export type PatternCheck = { readonly kind: "accepted" } | { readonly kind: "refused"; readonly cause: string };

/** The payload bound of each schedule's input: none, or the most UTF-8 bytes of an input text that a send takes. */
export type RejectBound = { readonly kind: "noBound" } | { readonly kind: "bytes"; readonly bytes: bigint };

/** What the accepted config gives a send on one queue: its priority and the retention class of a plain send. */
export interface QueueSendFacts {
  readonly queueName: string;
  readonly priority: SendPriority;
  readonly retentionClassKey: string;
}

/**
 * What the accepted config resolves for the schedules: the payload bound, the rerun-input default, and the facts of
 * each asked queue, in the asked order.
 */
export interface SchedulerConfigFacts {
  readonly rejectBound: RejectBound;
  readonly retainRerunInput: boolean;
  readonly queues: QueueSendFacts[];
}

/** The scheduler of one scheduler manifest, not run yet. It runs once. */
export interface NativePreparedScheduler {
  /** Ask the run to stop. Idempotent; returns at once. A request before the run ends the run after its start. */
  requestStop(): NativeEnvelope<undefined>;
}

/**
 * How the preparation of a scheduler ended: the scheduler, or the refusal of the core's parser with the field path
 * (`schedules[1].name`) and the rule.
 */
export type SchedulerPreparation =
  | { readonly kind: "prepared"; readonly scheduler: NativePreparedScheduler }
  | { readonly kind: "manifestRefused"; readonly fieldPath: string; readonly cause: string };

/**
 * How a scheduler run ended:
 * - `stopped`: at a stop request;
 * - `startRefused`: the start refused `schedule` before any state write (a zone that the core does not know, a send
 *   that the command rules refuse, a pattern with no slot after the database time); `reason` names both;
 * - `startFailed`: the connect, the schema install or the client family check failed, and no state was written.
 */
export type SchedulerRunEnded =
  | { readonly kind: "stopped" }
  | { readonly kind: "startRefused"; readonly schedule: string; readonly reason: string }
  | { readonly kind: "startFailed"; readonly message: string; readonly retryable: boolean };

/** The functions of the `scheduler` area. */
export interface SchedulerAddon {
  checkSchedulePattern(patternJson: string): NativeEnvelope<PatternCheck>;
  schedulerConfigFacts(appConfigJson: string, queueNames: string[]): NativeEnvelope<SchedulerConfigFacts>;
  prepareScheduler(manifestJson: string): NativeEnvelope<SchedulerPreparation>;
  runScheduler(scheduler: NativePreparedScheduler, handle: NativeCoreHandle): Promise<NativeEnvelope<SchedulerRunEnded>>;
}

/** The `scheduler` functions of the addon. Throws as `requireAddon` does. */
export function schedulerAddon(): SchedulerAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as SchedulerAddon;
}
