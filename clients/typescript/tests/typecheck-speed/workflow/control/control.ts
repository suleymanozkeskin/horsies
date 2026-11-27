// The control of the workflow typecheck cost (`../fifty/fifty.ts`): the same 10 task contracts, and no workflow.

import { defineTask, h } from "../../../../horsies/src/index.ts";

/**
 * The upstream contract of every `up` parameter: the output and the codes of the ten tasks. Each node takes the node
 * before it, of the task before it, so no task can name its own source contract.
 */
const upstream = defineTask({
  name: "upstream",
  input: {},
  output: h.int(),
  errorCodes: ["E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9"],
});

const t0 = defineTask({
  name: "t0",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E0"],
});

const t1 = defineTask({
  name: "t1",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E1"],
});

const t2 = defineTask({
  name: "t2",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E2"],
});

const t3 = defineTask({
  name: "t3",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E3"],
});

const t4 = defineTask({
  name: "t4",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E4"],
});

const t5 = defineTask({
  name: "t5",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E5"],
});

const t6 = defineTask({
  name: "t6",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E6"],
});

const t7 = defineTask({
  name: "t7",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E7"],
});

const t8 = defineTask({
  name: "t8",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E8"],
});

const t9 = defineTask({
  name: "t9",
  input: { a: h.int(), b: h.string(), up: h.taskOutcome(upstream).optional() },
  output: h.int(),
  errorCodes: ["E9"],
});

export const used = [t0, t1, t2, t3, t4, t5, t6, t7, t8, t9];
