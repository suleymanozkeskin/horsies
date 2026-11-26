// The control of the workflow typecheck cost (`../fifty/fifty.ts`): the same 10 task contracts, and no workflow.

import { defineTask, h } from "../../../../horsies/src/index.ts";

const t0 = defineTask({
  name: "t0",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E0"],
});

const t1 = defineTask({
  name: "t1",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E1"],
});

const t2 = defineTask({
  name: "t2",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E2"],
});

const t3 = defineTask({
  name: "t3",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E3"],
});

const t4 = defineTask({
  name: "t4",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E4"],
});

const t5 = defineTask({
  name: "t5",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E5"],
});

const t6 = defineTask({
  name: "t6",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E6"],
});

const t7 = defineTask({
  name: "t7",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E7"],
});

const t8 = defineTask({
  name: "t8",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E8"],
});

const t9 = defineTask({
  name: "t9",
  input: { a: h.int(), b: h.string(), up: h.resultOf(h.int()).optional() },
  output: h.int(),
  errors: ["E9"],
});

export const used = [t0, t1, t2, t3, t4, t5, t6, t7, t8, t9];
