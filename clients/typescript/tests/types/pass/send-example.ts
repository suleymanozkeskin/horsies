// Compile-pass cases of a send and a read through the app: the send error switch with a full match (`retrySend` of
// `ENQUEUE_FAILED`; `IDEMPOTENCY_KEY_CONFLICT` has `retryable: false`), the send
// options, and the read switch over the tag and the code of the task error.

import { defineTask, type Horsies, TaskId } from "../../../horsies/src/index.ts";
import { h } from "../../../horsies/src/wire/public.ts";

declare const app: Horsies;

function assertNever(value: never): never {
  throw new Error(`no case for ${String(value)}`);
}

const ValidatedOrder = h.object({ orderId: h.uuid(), total: h.decimal(), validatedAt: h.instant() });

const validateOrder = defineTask({
  name: "validate_order",
  input: { orderId: h.uuid(), lines: h.array(h.object({ sku: h.string(), quantity: h.int() })) },
  output: ValidatedOrder,
  errorCodes: ["EMPTY_ORDER", "INVALID_EMAIL"],
});

export async function sendAndRead(): Promise<void> {
  const sent = await app.send(validateOrder, { orderId: crypto.randomUUID(), lines: [{ sku: "a", quantity: 1 }] });

  if (!sent.ok) {
    switch (sent.error.code) {
      case "IDEMPOTENCY_KEY_CONFLICT":
        // Not retryable: `retrySend` does not take it. Send the owner's command, or use another key.
        console.warn(sent.error.ownerTaskId);
        break;
      case "ENQUEUE_FAILED":
        if (sent.error.retryable) {
          void (await app.retrySend(sent.error));
        }

        break;
      case "SEAT_RELAY_FULL":
        // Only a send from a SHARED_THREAD body gives it; nothing was sent, so a new send is the retry.
        void (await app.send(validateOrder, { orderId: crypto.randomUUID(), lines: [{ sku: "a", quantity: 1 }] }));
        break;
      case "VALIDATION_FAILED":
      case "PAYLOAD_TOO_LARGE":
      case "PAYLOAD_MISMATCH":
      case "SEND_SUPPRESSED":
        console.warn(sent.error.message);
        break;
      default:
        assertNever(sent.error);
    }

    return;
  }

  const handle = sent.value;
  const read = await handle.get({ waitMs: 5_000 });

  if (!read.ok) {
    console.warn(read.error.kind);

    return;
  }

  const outcome = read.value;

  switch (outcome.kind) {
    case "succeeded":
      console.log(outcome.value.total.toString(), outcome.value.validatedAt.epochNanoseconds);
      break;
    case "failed":
      switch (outcome.error.kind) {
        case "custom": {
          const code = outcome.error.code;

          switch (code) {
            case "EMPTY_ORDER":
            case "INVALID_EMAIL":
              break;
            default:
              assertNever(code);
          }

          break;
        }

        case "builtIn":
          console.warn(outcome.error.code);
          break;
        default:
          assertNever(outcome.error);
      }

      break;
    default:
      assertNever(outcome);
  }

  const info = await handle.info();

  if (info.ok) {
    console.log(info.value.status, info.value.attempts.length);
  }

  const stored = TaskId.parse(handle.taskId);

  if (stored.ok) {
    void (await app.taskHandle(validateOrder, stored.value).get());
  }
}

export async function sendWithOptions(): Promise<void> {
  void (await app.send(
    validateOrder,
    { orderId: crypto.randomUUID(), lines: [] },
    {
      idempotencyKey: "validate:1",
      goodUntil: Temporal.Now.instant().add({ minutes: 10 }),
      delaySeconds: 30,
      retention: { kind: "days", days: 30 },
    },
  ));
  void (await app.send(validateOrder, { orderId: crypto.randomUUID(), lines: [] }, { retention: { kind: "forever" } }));
}

/** A key conflict is not retryable (`retryable: false`). */
export async function keyConflictIsNotRetryable(): Promise<void> {
  const sent = await app.send(validateOrder, { orderId: crypto.randomUUID(), lines: [] }, { idempotencyKey: "validate:2" });

  if (!sent.ok && sent.error.code === "IDEMPOTENCY_KEY_CONFLICT") {
    const retryable: false = sent.error.retryable;

    console.warn(retryable, sent.error.ownerTaskId);
  }
}
