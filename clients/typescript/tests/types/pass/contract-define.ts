// Compile-pass cases of `defineTask`: the input that a body gets and that a sender gives, for `h.*` and zod
// parameters, and the result constructors of a body.

import { z } from "zod";

import type { Horsies } from "../../../horsies/src/app.ts";
import {
  type BodyInput,
  defineTask,
  type SendableInput,
  type SendInput,
  type UserFailure,
} from "../../../horsies/src/contract/public.ts";
import { type Decimal, type EmptyObject, type Err, h, type Ok } from "../../../horsies/src/wire/public.ts";
import { hz } from "../../../horsies/src/zod.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const Order = z.object({
  orderId: z.uuid(),
  placedAt: hz.instant(),
  note: z.string().optional(),
  retries: z.int().default(3),
});

declare const app: Horsies;

/** The upstream task of the `h.taskOutcome` parameters below. */
const counter = defineTask({ name: "counter", input: {}, output: h.int() });

const parameters = {
  order: Order,
  region: h.string(),
  limit: h.int().default(10),
  tag: h.string().optional(),
  upstream: h.taskOutcome(counter).optional(),
};

type TaskParameters = typeof parameters;

// A body gets every parameter: a default is filled; an optional one may be absent.
type Body = BodyInput<TaskParameters>;

expectType<Equal<Body["order"], { orderId: string; placedAt: Temporal.Instant; note?: string | undefined; retries: number }>>();

expectType<Equal<Body["region"], string>>();

expectType<Equal<Body["limit"], number>>();

expectType<Equal<Pick<Body, "tag">, { tag?: string }>>();

// A sender may leave out a defaulted or optional parameter and a defaulted zod member, and never gives an upstream
// outcome.
type Sent = SendInput<TaskParameters>;

expectType<Equal<keyof Sent, "order" | "region" | "limit" | "tag">>();

expectType<Equal<Sent["order"], { orderId: string; placedAt: Temporal.Instant; note?: string; retries?: number }>>();

expectType<Equal<Pick<Sent, "limit">, { limit?: number }>>();

// A task with no parameters takes `{}`.
expectType<Equal<SendInput<{}>, EmptyObject>>();

// A task with a required upstream outcome cannot be sent.
const workflowOnly = { upstream: h.taskOutcome(counter) };

expectType<
  Equal<
    SendableInput<typeof workflowOnly>,
    "horsies: this task takes an upstream result (h.taskOutcome); only a workflow node can run it"
  >
>();

const validateOrder = defineTask({
  name: "validate_order",
  queue: "standard",
  input: parameters,
  output: h.object({ orderId: h.uuid(), total: h.decimal() }),
  errors: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

// A body gets the result constructors of its contract in its second argument.
declare const total: Decimal;

app.implement(validateOrder, (_input, { ok, err }) => {
  const okValue = ok({ orderId: crypto.randomUUID(), total });

  expectType<Equal<typeof okValue, Ok<{ orderId: string; total: Decimal }>>>();

  const errValue = err({ code: "EMPTY_ORDER", message: "no lines" });

  expectType<Equal<typeof errValue, Err<UserFailure<"EMPTY_ORDER" | "INVALID_EMAIL">>>>();

  return okValue;
});

// Without `errors`, any code that is not built in.
const ping = defineTask({ name: "ping", input: {}, output: h.string() });

app.implement(ping, (_input, { err }) => {
  expectType<Equal<ReturnType<typeof err>, Err<UserFailure<string>>>>();

  return err({ code: "RATE_LIMITED" });
});

// Shared classes take a timeout; `SHARED_ASYNC` takes a grace.
defineTask({
  name: "notify",
  execution: "SHARED_ASYNC",
  timeoutMs: 10_000,
  cancelGraceMs: 2_000,
  input: {},
  output: h.null(),
});

defineTask({ name: "resize", execution: "SHARED_THREAD", timeoutMs: 60_000, input: { width: h.int() }, output: h.string() });

// A retry policy names the task's codes and the library's codes.
defineTask({
  name: "charge",
  input: {},
  output: h.null(),
  errors: ["PROVIDER_DOWN"],
  retry: { kind: "exponential", baseSeconds: 30, maxRetries: 5, autoRetryFor: ["PROVIDER_DOWN", "TASK_TIMEOUT"] },
});

// A zod output.
const report = defineTask({ name: "report", input: {}, output: z.object({ lines: z.int() }) });

app.implement(report, (_input, { ok }) => {
  expectType<Equal<Parameters<typeof ok>[0], { lines: number }>>();

  return ok({ lines: 1 });
});
