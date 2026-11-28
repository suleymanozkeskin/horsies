// The typecheck cost of one workflow of 50 nodes: 10 task contracts, 50 nodes (each takes a value and the outcome of
// the node before it; 16 waitsFor, 7 any joins), a success policy, a start and a typed resultFor. The control
// (`../control/control.ts`) is the same file with no workflow. Run from clients/typescript:
//
//   tests/node_modules/.bin/tsc --project tests/typecheck-speed/workflow/control --extendedDiagnostics
//   tests/node_modules/.bin/tsc --project tests/typecheck-speed/workflow/fifty --extendedDiagnostics
//
// The cost is the difference of the instantiation counts, which is the same on every run. The check time of the two
// files is within the spread of repeated runs.

import { defineTask, defineWorkflow, h, type Horsies } from "../../../../horsies/src/index.ts";

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

export const fifty = defineWorkflow(
  { name: "fifty", definitionKey: "speed.fifty.v1", outputType: h.int(), parameters: {} },
  (w) => {
    const n0 = w.task(t0, { a: 0, b: "x" }, { nodeId: "n0" });
    const n1 = w.task(t1, { a: 1, b: "x", up: n0 }, { nodeId: "n1" });
    const n2 = w.task(t2, { a: 2, b: "x", up: n1 }, { nodeId: "n2" });
    const n3 = w.task(t3, { a: 3, b: "x", up: n2 }, { nodeId: "n3", waitsFor: [n1] });
    const n4 = w.task(t4, { a: 4, b: "x", up: n3 }, { nodeId: "n4" });
    const n5 = w.task(t5, { a: 5, b: "x", up: n4 }, { nodeId: "n5" });
    const n6 = w.task(t6, { a: 6, b: "x", up: n5 }, { nodeId: "n6", waitsFor: [n4] });
    const n7 = w.task(t7, { a: 7, b: "x", up: n6 }, { nodeId: "n7", join: { kind: "any" } });
    const n8 = w.task(t8, { a: 8, b: "x", up: n7 }, { nodeId: "n8" });
    const n9 = w.task(t9, { a: 9, b: "x", up: n8 }, { nodeId: "n9", waitsFor: [n7] });
    const n10 = w.task(t0, { a: 10, b: "x", up: n9 }, { nodeId: "n10" });
    const n11 = w.task(t1, { a: 11, b: "x", up: n10 }, { nodeId: "n11" });
    const n12 = w.task(t2, { a: 12, b: "x", up: n11 }, { nodeId: "n12", waitsFor: [n10] });
    const n13 = w.task(t3, { a: 13, b: "x", up: n12 }, { nodeId: "n13" });
    const n14 = w.task(t4, { a: 14, b: "x", up: n13 }, { nodeId: "n14", join: { kind: "any" } });
    const n15 = w.task(t5, { a: 15, b: "x", up: n14 }, { nodeId: "n15", waitsFor: [n13] });
    const n16 = w.task(t6, { a: 16, b: "x", up: n15 }, { nodeId: "n16" });
    const n17 = w.task(t7, { a: 17, b: "x", up: n16 }, { nodeId: "n17" });
    const n18 = w.task(t8, { a: 18, b: "x", up: n17 }, { nodeId: "n18", waitsFor: [n16] });
    const n19 = w.task(t9, { a: 19, b: "x", up: n18 }, { nodeId: "n19" });
    const n20 = w.task(t0, { a: 20, b: "x", up: n19 }, { nodeId: "n20" });
    const n21 = w.task(t1, { a: 21, b: "x", up: n20 }, { nodeId: "n21", waitsFor: [n19], join: { kind: "any" } });
    const n22 = w.task(t2, { a: 22, b: "x", up: n21 }, { nodeId: "n22" });
    const n23 = w.task(t3, { a: 23, b: "x", up: n22 }, { nodeId: "n23" });
    const n24 = w.task(t4, { a: 24, b: "x", up: n23 }, { nodeId: "n24", waitsFor: [n22] });
    const n25 = w.task(t5, { a: 25, b: "x", up: n24 }, { nodeId: "n25" });
    const n26 = w.task(t6, { a: 26, b: "x", up: n25 }, { nodeId: "n26" });
    const n27 = w.task(t7, { a: 27, b: "x", up: n26 }, { nodeId: "n27", waitsFor: [n25] });
    const n28 = w.task(t8, { a: 28, b: "x", up: n27 }, { nodeId: "n28", join: { kind: "any" } });
    const n29 = w.task(t9, { a: 29, b: "x", up: n28 }, { nodeId: "n29" });
    const n30 = w.task(t0, { a: 30, b: "x", up: n29 }, { nodeId: "n30", waitsFor: [n28] });
    const n31 = w.task(t1, { a: 31, b: "x", up: n30 }, { nodeId: "n31" });
    const n32 = w.task(t2, { a: 32, b: "x", up: n31 }, { nodeId: "n32" });
    const n33 = w.task(t3, { a: 33, b: "x", up: n32 }, { nodeId: "n33", waitsFor: [n31] });
    const n34 = w.task(t4, { a: 34, b: "x", up: n33 }, { nodeId: "n34" });
    const n35 = w.task(t5, { a: 35, b: "x", up: n34 }, { nodeId: "n35", join: { kind: "any" } });
    const n36 = w.task(t6, { a: 36, b: "x", up: n35 }, { nodeId: "n36", waitsFor: [n34] });
    const n37 = w.task(t7, { a: 37, b: "x", up: n36 }, { nodeId: "n37" });
    const n38 = w.task(t8, { a: 38, b: "x", up: n37 }, { nodeId: "n38" });
    const n39 = w.task(t9, { a: 39, b: "x", up: n38 }, { nodeId: "n39", waitsFor: [n37] });
    const n40 = w.task(t0, { a: 40, b: "x", up: n39 }, { nodeId: "n40" });
    const n41 = w.task(t1, { a: 41, b: "x", up: n40 }, { nodeId: "n41" });
    const n42 = w.task(t2, { a: 42, b: "x", up: n41 }, { nodeId: "n42", waitsFor: [n40], join: { kind: "any" } });
    const n43 = w.task(t3, { a: 43, b: "x", up: n42 }, { nodeId: "n43" });
    const n44 = w.task(t4, { a: 44, b: "x", up: n43 }, { nodeId: "n44" });
    const n45 = w.task(t5, { a: 45, b: "x", up: n44 }, { nodeId: "n45", waitsFor: [n43] });
    const n46 = w.task(t6, { a: 46, b: "x", up: n45 }, { nodeId: "n46" });
    const n47 = w.task(t7, { a: 47, b: "x", up: n46 }, { nodeId: "n47" });
    const n48 = w.task(t8, { a: 48, b: "x", up: n47 }, { nodeId: "n48", waitsFor: [n46] });
    const n49 = w.task(t9, { a: 49, b: "x", up: n48 }, { nodeId: "n49", join: { kind: "any" } });

    return { output: n49, successPolicy: { cases: [{ required: [n49, n10] }], optional: [n0] } };
  },
);

declare const app: Horsies;

export async function run(): Promise<number> {
  const started = await app.start(fifty, {});

  if (!started.ok) return 0;
  const read = await started.value.resultFor("n10");

  return read.ok && read.value.kind === "succeeded" ? read.value.value : 0;
}
