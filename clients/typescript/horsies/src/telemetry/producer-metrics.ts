// The producer metrics of the core's catalog: `horsies.task.sent` (task, queue, result) and
// `horsies.task.send.duration` (queue), one point of each per send call that reached the database. The names, units,
// help texts, label keys and bucket bounds come from the generated catalog. A producer has no metrics endpoint: the
// points go to the global meter provider of the process.

import {
  QUEUE_ATTRIBUTE,
  RESULT_ATTRIBUTE,
  SEND_OK_RESULT,
  TASK_ATTRIBUTE,
  TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS,
  TASK_SEND_DURATION_DESCRIPTION,
  TASK_SEND_DURATION_NAME,
  TASK_SEND_DURATION_UNIT,
  TASK_SENT_DESCRIPTION,
  TASK_SENT_NAME,
  TASK_SENT_UNIT,
} from "../_catalog/producer_metrics.ts";
import {
  INSTRUMENTATION_SCOPE,
  INSTRUMENTATION_SCOPE_VERSION,
  type OtelApi,
  type OtelCounter,
  type OtelHistogram,
  type OtelMeter,
} from "./otel-api.ts";

/** How one send call ended, as the `result` label names it: `ok`, or the code of the send error. */
export type SendResultLabel = { readonly kind: "sent" } | { readonly kind: "failed"; readonly code: string };

/** The facts of one point of each producer metric. `seconds` is on the monotonic clock. */
export type SendPoint = {
  readonly taskName: string;
  readonly queueName: string;
  readonly result: SendResultLabel;
  readonly seconds: number;
};

/** The instruments of one meter. */
type ProducerInstruments = { readonly sent: OtelCounter; readonly duration: OtelHistogram };

/** The instruments of each meter that a send used: a meter of a later provider gets its own. */
const instrumentsByMeter = new WeakMap<OtelMeter, ProducerInstruments>();

/** The instruments of `meter`, made at its first use. `valueType` is the API's `ValueType.INT`: `horsies.task.sent` counts whole sends. */
function instrumentsOf(meter: OtelMeter, valueType: number): ProducerInstruments {
  const known = instrumentsByMeter.get(meter);

  if (known !== undefined) {
    return known;
  }

  const made: ProducerInstruments = {
    sent: meter.createCounter(TASK_SENT_NAME, { unit: TASK_SENT_UNIT, description: TASK_SENT_DESCRIPTION, valueType }),
    duration: meter.createHistogram(TASK_SEND_DURATION_NAME, {
      unit: TASK_SEND_DURATION_UNIT,
      description: TASK_SEND_DURATION_DESCRIPTION,
      advice: { explicitBucketBoundaries: [...TASK_SEND_DURATION_BUCKET_BOUNDS_SECONDS] },
    }),
  };

  instrumentsByMeter.set(meter, made);

  return made;
}

/** The text of the `result` label. Pure. */
export function resultLabelText(result: SendResultLabel): string {
  switch (result.kind) {
    case "sent":
      return SEND_OK_RESULT;
    case "failed":
      return result.code;
    default: {
      const unexpected: never = result;

      throw new TypeError(`resultLabelText: a result outside its type: ${String(unexpected)}`);
    }
  }
}

/** Record one point of each producer metric with the global meter provider. May throw what the user's SDK throws. */
export function recordSendPoint(api: OtelApi, point: SendPoint): void {
  const instruments = instrumentsOf(api.metrics.getMeter(INSTRUMENTATION_SCOPE, INSTRUMENTATION_SCOPE_VERSION), api.ValueType.INT);

  instruments.sent.add(1, {
    [TASK_ATTRIBUTE]: point.taskName,
    [QUEUE_ATTRIBUTE]: point.queueName,
    [RESULT_ATTRIBUTE]: resultLabelText(point.result),
  });
  instruments.duration.record(point.seconds, { [QUEUE_ATTRIBUTE]: point.queueName });
}
