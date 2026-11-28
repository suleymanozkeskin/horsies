// Compile-pass cases of the task runtime: `TaskRuntime.forTest` with typed state entries, `state(key)` gives the
// key's value type, and the runtime type of a body by its execution class.

import { type AttemptNumber, MaxRetries, type RetriesLeft, type StateKey, TaskRuntime, type ThreadTaskRuntime } from "../../../horsies/src/child/public.ts";
import type { TaskId } from "../../../horsies/src/contract/ids.ts";
import type { RuntimeOf } from "../../../horsies/src/contract/public.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

type Pool = { readonly query: (sql: string) => number };

declare const Db: StateKey<Pool>;
declare const Name: StateKey<string>;
declare const taskId: TaskId;
declare const attempt: AttemptNumber;

const pool: Pool = { query: () => 1 };

export const runtime: TaskRuntime = TaskRuntime.forTest({ taskId, attempt, state: [[Db, pool], [Name, "eu-1"]] });

export const noState: TaskRuntime = TaskRuntime.forTest({ taskId, attempt });

export const db: Pool = runtime.state(Db);

export const name: string = runtime.state(Name);

export const isolated = expectType<Equal<RuntimeOf<"ISOLATED">, TaskRuntime>>();

export const sharedAsync = expectType<Equal<RuntimeOf<"SHARED_ASYNC">, TaskRuntime>>();

export const sharedThread = expectType<Equal<RuntimeOf<"SHARED_THREAD">, ThreadTaskRuntime>>();

export const attemptOfRuntime: AttemptNumber = runtime.attempt;

export const maxRetriesOfRuntime: MaxRetries = runtime.maxRetries;

export const retriesLeftOfRuntime: RetriesLeft = runtime.retriesLeft;

export const threadRetriesLeft = (thread: ThreadTaskRuntime): RetriesLeft => thread.retriesLeft;

export const withBudget: TaskRuntime = TaskRuntime.forTest({ taskId, attempt, maxRetries: MaxRetries.NONE });
