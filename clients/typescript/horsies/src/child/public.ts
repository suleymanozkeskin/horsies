// The public names of the task child: the runtime of a body, its types, the attempt number, app state, and the start
// hooks. `src/index.ts` exports this module.

export {
  type CancelCause,
  type CancelReason,
  type CancelState,
  type FencingToken,
  type SpawnedWork,
  type SpawnRefused,
  TaskRuntime,
  type ThreadTaskRuntime,
  type WorkStopReason,
} from "./runtime.ts";

export { AttemptNumber, type AttemptRefused } from "./attempt.ts";

export type { AnyStateKey, StateKey, StateValueOf } from "./state.ts";

export type { ChildStartHook, ProviderFactory } from "./app-members.ts";

export type { TestStateEntries, TestStateEntry, TestRuntimeOptions } from "../testing/task-runtime.ts";
