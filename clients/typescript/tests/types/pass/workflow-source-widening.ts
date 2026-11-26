// Compile-pass cases of a source whose type is a subtype of the target: each typechecks, and the run-time checks
// (HRS-024, HRS-025) accept each one too (`tests/unit/workflow/checks.test.ts`, "the stored-form fit of a source").

import { defineTask, h } from "../../../horsies/src/index.ts";
import { defineWorkflow } from "../../../horsies/src/workflow/public.ts";

const text = defineTask({ name: "text", input: {}, output: h.string() });
const letter = defineTask({ name: "letter", input: {}, output: h.literal("a", "b") });
const count = defineTask({ name: "count", input: {}, output: h.int() });

// The contracts that the parameters below name: their outputs are the target types.
const maybeText = defineTask({ name: "maybe_text", input: {}, output: h.string().nullable() });
const ratio = defineTask({ name: "ratio", input: {}, output: h.float() });

const takesMaybeText = defineTask({
  name: "takes_maybe_text",
  input: { value: h.taskOutcome(maybeText) },
  output: h.int(),
});

const takesText = defineTask({ name: "takes_text", input: { value: h.taskOutcome(text) }, output: h.int() });
const takesFloat = defineTask({ name: "takes_float", input: { value: h.taskOutcome(ratio) }, output: h.int() });

/**
 * A string source for an `h.taskOutcome` of a contract of a nullable string output; a literal for its base scalar; an
 * int for a float.
 */
export const widenedSources = defineWorkflow(
  { name: "widened_sources", definitionKey: "tests.widened_sources.v1", output: h.int(), params: {} },
  (w) => {
    const maybe = w.task(takesMaybeText, { value: w.task(text, {}, { nodeId: "text" }) }, { nodeId: "maybe" });
    const base = w.task(takesText, { value: w.task(letter, {}, { nodeId: "letter" }) }, { nodeId: "base" });

    return {
      output: w.task(
        takesFloat,
        { value: w.task(count, {}, { nodeId: "count" }) },
        { nodeId: "float", waitsFor: [maybe, base] },
      ),
    };
  },
);

/** A string output node for a declared nullable string output. */
export const widenedOutput = defineWorkflow(
  { name: "widened_output", definitionKey: "tests.widened_output.v1", output: h.string().nullable(), params: {} },
  (w) => ({ output: w.task(text, {}, { nodeId: "text" }) }),
);
