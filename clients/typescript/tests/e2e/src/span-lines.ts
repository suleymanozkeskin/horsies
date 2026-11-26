// The span lines of the `worker_ended_spans` instance: one JSON line per ended span in the marker `span-lines`. The
// SDK of each task child and of the handler child writes them (`span-sdk.ts`); a test reads them with `parseSpanLine`,
// the one parser of the line form.
// The model: the `LineExporter` of Python `clients/python/tests/e2e/tasks/instance_worker_ended_spans.py`.

import { z } from "zod";

import { err, ok, type Result } from "../../../horsies/src/wire/result.ts";

/** The marker that holds the span lines. */
export const SPAN_LINES_MARKER = "span-lines";

/** One item of an array attribute: JSON writes an absent item as `null`. */
const ATTRIBUTE_ITEM = z.union([z.string(), z.number(), z.boolean(), z.null()]);

/** An attribute value of a span line. */
const ATTRIBUTE_VALUE = z.union([z.string(), z.number(), z.boolean(), z.array(ATTRIBUTE_ITEM)]);

/** One ended span. The times are nanoseconds since 1970, as decimal text; the status is the name of its code. */
const SPAN_LINE = z.object({
  name: z.string(),
  traceId: z.string(),
  spanId: z.string(),
  parent: z.discriminatedUnion("kind", [z.object({ kind: z.literal("parent"), spanId: z.string() }), z.object({ kind: z.literal("root") })]),
  startNanos: z.string(),
  endNanos: z.string(),
  status: z.enum(["UNSET", "OK", "ERROR"]),
  attributes: z.record(z.string(), ATTRIBUTE_VALUE),
});

/** One ended span. */
export type SpanLine = z.infer<typeof SPAN_LINE>;

/** An attribute value of a span line. */
export type SpanLineValue = z.infer<typeof ATTRIBUTE_VALUE>;

/** The status of a span line. */
export type SpanLineStatus = SpanLine["status"];

/** The span line of `text`, or why it is not one. Pure. */
export function parseSpanLine(text: string): Result<SpanLine, string> {
  let value: unknown;

  try {
    value = JSON.parse(text);
  } catch (cause) {
    return err(`not JSON: ${String(cause)}: ${text}`);
  }

  const parsed = SPAN_LINE.safeParse(value);

  return parsed.success ? ok(parsed.data) : err(`not a span line: ${parsed.error.message}: ${text}`);
}
