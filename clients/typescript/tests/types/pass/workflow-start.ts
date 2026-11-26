// Compile-pass cases of the sketch's start and handle code (section 5.3): the start error switch with a full match,
// retryStart of ENQUEUE_FAILED, get with open user codes, resultFor typed by the node's contract, pause, resume,
// cancel, and a handle by id.

import {
  defineTask,
  defineWorkflow,
  h,
  type Horsies,
  type TaskOutcome,
  WorkflowId,
} from "../../../horsies/src/index.ts";

declare const app: Horsies;

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

function assertNever(value: never): never {
  throw new Error(`no case for ${String(value)}`);
}

const Receipt = h.object({ receiptId: h.string() });
const validate = defineTask({ name: "validate", input: { orderId: h.uuid() }, output: h.int() });

const charge = defineTask({
  name: "charge",
  input: { amount: h.resultOf(h.int()) },
  output: Receipt,
  errors: ["PAYMENT_DECLINED"],
});

const fulfil = defineWorkflow(
  {
    name: "fulfil",
    definitionKey: "shop.fulfil.v1",
    params: { orderId: h.uuid() },
    checkCases: [{ orderId: "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b" }],
  },
  (w, { orderId }) => {
    const validated = w.task(validate, { orderId }, { id: "validate" });
    const charged = w.task(charge, { amount: validated }, { id: "charge" });

    return { output: charged, successPolicy: { cases: [{ required: [charged] }], optional: [validated] } };
  },
);

export async function startAndRead(workflowId: WorkflowId): Promise<void> {
  const started = await app.start(fulfil, { orderId: workflowId }, { workflowId });

  if (!started.ok) {
    switch (started.error.code) {
      case "WORKFLOW_COMMAND_CONFLICT":
      case "WORKFLOW_ID_CONFLICT":
      case "VALIDATION_FAILED":
        break;
      case "ENQUEUE_FAILED":
        if (started.error.retryable) {
          void (await app.retryStart(started.error));
        }

        break;
      default:
        assertNever(started.error);
    }

    return;
  }

  const wf = started.value;
  const result = await wf.get({ timeoutMs: 60_000 });

  if (result.ok) {
    expectType<Equal<typeof result.value, TaskOutcome<{ receiptId: string }, string>>>();
  }

  const charged = await wf.resultFor("charge");
  const validated = await wf.resultFor("validate");

  if (charged.ok && validated.ok) {
    expectType<Equal<typeof charged.value, TaskOutcome<{ receiptId: string }, "PAYMENT_DECLINED">>>();
    expectType<Equal<typeof validated.value, TaskOutcome<number, string>>>();
  }

  void (await wf.pause());
  void (await wf.resume());
  void (await wf.cancel());

  const again = app.workflowHandle(fulfil, wf.workflowId);
  const byId = await again.resultFor("charge", charge);

  if (byId.ok) {
    expectType<Equal<typeof byId.value, TaskOutcome<{ receiptId: string }, "PAYMENT_DECLINED">>>();
  }
}
