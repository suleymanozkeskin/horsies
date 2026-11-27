---
title: Crash-Safe Multi-Step Work
section: python
summary: Put each step that has an effect in its own workflow node, so that a retry or a worker crash runs only the step that failed.
related: [../concepts/workflows/workflow-semantics, ../concepts/workflows/typed-node-builder, ../tasks/retry-policy, ../workers/heartbeats-recovery, start-workflow-once]
tags: [guides, workflows, retry, WORKER_CRASHED, idempotency, args_from, from_node]
---

# Crash-Safe Multi-Step Work

Horsies has no durable steps inside one task. Each attempt of a task runs its body from the first line. For work with several steps that have effects, use a workflow with one node per step. The engine stores the result of each node, so a retry or a worker crash runs only the node that failed.

## Why and When

Use this pattern when a task does two or more steps with effects outside the task, for example a payment, a database write or a message.

A checkout in one task body:

<!-- check: skip elided -- the one-task form, with the step bodies left out -->
```python
@app.task(
    "checkout",
    error_codes=CheckoutCode,
    retry_policy=RetryPolicy.exponential(
        base_seconds=30,
        max_retries=5,
        auto_retry_for=[
            CheckoutCode.PAYMENTS_UNAVAILABLE,
            CheckoutCode.MAIL_UNAVAILABLE,
            OperationalCode.WORKER_CRASHED,
        ],
    ),
)
def checkout(*, order: Order) -> Result[str, TaskError]:
    charge = charge_the_card(order)             # 1. money moves
    invoice = write_the_invoice(order, charge)  # 2. a record is written
    return send_the_receipt(order, invoice)     # 3. a message is sent
```

What a failure does to this task:

- An error code in `auto_retry_for` starts a retry. The retry runs the body from the first line. A transient failure in step 3 charges the card again in step 1.
- A worker crash while the body runs ends the attempt with `WORKER_CRASHED`. When `auto_retry_for` does not name `WORKER_CRASHED`, the task is `FAILED`. This is the default. The steps before the crash have run, and the stored error does not tell which steps ran.
- When `auto_retry_for` names `WORKER_CRASHED`, the retry runs the body from the first line. Steps 1 and 2 run again.

The rules are on [Stale RUNNING Recovery](/horsies/workers/heartbeats-recovery/#stale-running-recovery) and [Auto-Retry Triggers](/horsies/tasks/retry-policy/#auto-retry-triggers).

## How To

### Define One Task per Step

Each step is a task with its own retry policy. A step takes the outcome of the step before it as a parameter of type `TaskOutcome[T, TaskError]`.

```python
from enum import Enum

import httpx
from pydantic import BaseModel

from horsies import (
    Err,
    Failed,
    JsonValue,
    Ok,
    OperationalCode,
    Result,
    RetryPolicy,
    Succeeded,
    TaskError,
    TaskOutcome,
)
from instance import app

PAYMENTS_URL = "https://payments.example.com/v1/charges"
INVOICES_URL = "https://billing.example.com/v1/invoices"
MESSAGES_URL = "https://mail.example.com/v1/messages"
HTTP_TIMEOUT_SECONDS = 10.0


class CheckoutCode(str, Enum):
    """The codes of the steps. Each step declares them with `error_codes=`."""

    PAYMENTS_UNAVAILABLE = "PAYMENTS_UNAVAILABLE"
    CHARGE_REFUSED = "CHARGE_REFUSED"
    BILLING_UNAVAILABLE = "BILLING_UNAVAILABLE"
    INVOICE_REFUSED = "INVOICE_REFUSED"
    MAIL_UNAVAILABLE = "MAIL_UNAVAILABLE"
    MAIL_REFUSED = "MAIL_REFUSED"


class Charge(BaseModel):
    charge_id: str
    amount_cents: int


class Invoice(BaseModel):
    invoice_number: str
    charge_id: str


def post_with_key(
    url: str,
    body: dict[str, JsonValue],
    *,
    idempotency_key: str,
    unavailable_code: str,
    refused_code: str,
) -> Result[dict[str, JsonValue], TaskError]:
    """POST `body` once, with an `Idempotency-Key` header.

    No answer and a 5xx answer are `unavailable_code`: a later call can succeed.
    Another answer that is not 2xx is `refused_code`: the same call gets the same answer.
    """
    try:
        response = httpx.post(
            url,
            json=body,
            headers={"Idempotency-Key": idempotency_key},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    except httpx.TransportError as error:
        return Err(TaskError(error_code=unavailable_code, message=f"POST {url}: {error!r}"))
    match response.status_code // 100:
        case 2:
            return Ok(response.json())
        case 5:
            return Err(TaskError(
                error_code=unavailable_code,
                message=f"POST {url}: HTTP {response.status_code}",
            ))
        case _:
            return Err(TaskError(
                error_code=refused_code,
                message=f"POST {url}: HTTP {response.status_code}",
            ))


@app.task(
    "charge_card",
    error_codes=CheckoutCode,
    retry_policy=RetryPolicy.exponential(
        base_seconds=30,
        max_retries=5,
        auto_retry_for=[CheckoutCode.PAYMENTS_UNAVAILABLE, OperationalCode.WORKER_CRASHED],
    ),
)
def charge_card(
    *,
    order_id: str,
    customer_id: str,
    amount_cents: int,
) -> Result[Charge, TaskError]:
    match post_with_key(
        PAYMENTS_URL,
        {"customer_id": customer_id, "amount_cents": amount_cents},
        idempotency_key=f"checkout-{order_id}-charge",
        unavailable_code="PAYMENTS_UNAVAILABLE",
        refused_code="CHARGE_REFUSED",
    ):
        case Ok(answer):
            return Ok(Charge(charge_id=str(answer["id"]), amount_cents=amount_cents))
        case Err(error):
            return Err(error)


@app.task(
    "write_invoice",
    error_codes=CheckoutCode,
    retry_policy=RetryPolicy.exponential(
        base_seconds=10,
        max_retries=5,
        auto_retry_for=[CheckoutCode.BILLING_UNAVAILABLE, OperationalCode.WORKER_CRASHED],
    ),
)
def write_invoice(
    *,
    order_id: str,
    charge: TaskOutcome[Charge, TaskError],
) -> Result[Invoice, TaskError]:
    match charge:
        case Failed(error):
            return Err(error)
        case Succeeded(paid):
            pass
    match post_with_key(
        INVOICES_URL,
        {"order_id": order_id, "charge_id": paid.charge_id, "amount_cents": paid.amount_cents},
        idempotency_key=f"checkout-{order_id}-invoice",
        unavailable_code="BILLING_UNAVAILABLE",
        refused_code="INVOICE_REFUSED",
    ):
        case Ok(answer):
            return Ok(Invoice(
                invoice_number=str(answer["number"]),
                charge_id=paid.charge_id,
            ))
        case Err(error):
            return Err(error)


@app.task(
    "send_receipt",
    error_codes=CheckoutCode,
    retry_policy=RetryPolicy.fixed(
        [60, 300, 900],
        auto_retry_for=[CheckoutCode.MAIL_UNAVAILABLE, OperationalCode.WORKER_CRASHED],
    ),
)
def send_receipt(
    *,
    order_id: str,
    to_address: str,
    invoice: TaskOutcome[Invoice, TaskError],
) -> Result[str, TaskError]:
    match invoice:
        case Failed(error):
            return Err(error)
        case Succeeded(issued):
            pass
    match post_with_key(
        MESSAGES_URL,
        {
            "to": to_address,
            "template": "receipt",
            "invoice_number": issued.invoice_number,
        },
        idempotency_key=f"checkout-{order_id}-receipt",
        unavailable_code="MAIL_UNAVAILABLE",
        refused_code="MAIL_REFUSED",
    ):
        case Ok(answer):
            return Ok(str(answer["message_id"]))
        case Err(error):
            return Err(error)
```

- The `auto_retry_for` of each step names `WORKER_CRASHED` and the code that the step returns when its provider is not available. Another code, for example `CHARGE_REFUSED`, ends the step as `FAILED` at once.
- Each call sends an idempotency key made from the order id and the step name. The key makes a repeated run of the step safe. See [External Effects Need Idempotency](#external-effects-need-idempotency).
- With the default dependency rules, a step runs only when the step before it is `COMPLETED`. The `Failed` arm receives a stored result that does not decode (`RESULT_DESERIALIZATION_ERROR`). See [Upstream Failure and args_from](/horsies/concepts/workflows/workflow-semantics/#upstream-failure-and-args_from).

### Connect the Steps in a Workflow

```python
from typing import Any

from pydantic import BaseModel

from horsies import (
    Horsies,
    WorkflowDefinition,
    WorkflowHandle,
    WorkflowSpec,
    WorkflowStartResult,
    from_node,
)


class Order(BaseModel):
    order_id: str
    customer_id: str
    customer_email: str
    amount_cents: int


class CheckoutWorkflow(WorkflowDefinition[str]):
    name = "checkout"
    definition_key = "shop.checkout.v1"

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        order: Order,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[str]:
        charge = charge_card.node(node_id="charge_card")(
            order_id=order.order_id,
            customer_id=order.customer_id,
            amount_cents=order.amount_cents,
        )
        invoice = write_invoice.node(node_id="write_invoice")(
            order_id=order.order_id,
            charge=from_node(charge),
        )
        receipt = send_receipt.node(node_id="send_receipt")(
            order_id=order.order_id,
            to_address=order.customer_email,
            invoice=from_node(invoice),
        )
        return app.workflow(name=cls.name, tasks=[charge, invoice, receipt], output=receipt)


def start_checkout(order: Order) -> WorkflowStartResult[WorkflowHandle[str]]:
    return CheckoutWorkflow.build_with(app, order=order).start()
```

- `from_node(charge)` gives the result of the `charge_card` node to the `charge` parameter. It adds the `args_from` entry and the `waits_for` entry. See [Typed Node Builder](/horsies/concepts/workflows/typed-node-builder/).
- The `node_id` of a node is the name of the step in the web UI, and the key of its result in `handle.results()`.
- `start()` returns `Ok(WorkflowHandle)` or `Err(WorkflowStartError)`. See [Workflow API](/horsies/concepts/workflows/workflow-api/).
- To start one checkout per order, derive the `workflow_id` from the order id. A repeat start with that id and the same start command returns the handle of the first workflow and starts nothing. A repeat start with another start command, for example another amount, is refused with `WORKFLOW_COMMAND_CONFLICT`. See [Start a Workflow Once per Entity](/horsies/guides/start-workflow-once/).

## What Runs Again

| Event | One task with three steps | One node per step |
|---|---|---|
| Step 3 returns a code in `auto_retry_for` | The retry runs steps 1, 2 and 3 | The retry runs step 3 |
| The worker crashes during step 3, and `auto_retry_for` names `WORKER_CRASHED` | The retry runs steps 1, 2 and 3 | The retry runs step 3 |
| The worker crashes during step 3, and `auto_retry_for` does not name `WORKER_CRASHED` | The task is `FAILED` with `WORKER_CRASHED`. The error does not tell which steps ran | Step 3 is `FAILED` with `WORKER_CRASHED`. Steps 1 and 2 are `COMPLETED` and keep their results |
| Step 3 fails after its last retry | The task is `FAILED` | Step 3 is `FAILED`, and the workflow is `FAILED`. Steps 1 and 2 keep their results |

The rules of the table:

- When a node ends, the engine stores its result. A dependent node gets the stored results as its input when the engine enqueues it.
- A retry runs on the same task row, with the same input. The other nodes do not change. See [Retries and Crash Recovery](/horsies/concepts/workflows/workflow-semantics/#retries-and-crash-recovery).
- A `COMPLETED` node does not run again in the same workflow run.
- A workflow run has no restart. A new start of the workflow runs every node again. See [No workflow restart](/horsies/monitoring/action-semantics/#no-workflow-restart).

## See Each Step in the Web UI

Each step is a task with its own name and its own attempts. The Workflows surface of the [web UI](/horsies/monitoring/web-ui-overview/#workflows) shows the run as a graph with one node per step:

- the status of each node, its node time, and its attempt count when the step ran more than once (`3 attempts`);
- for a selected node: its error, its task, and the attempts of its task. Each attempt shows its outcome, its error code and its phase times, with the retry wait between two attempts. An attempt that a worker crash ended has a `worker lost` interval, and its tooltip says `recorded by recovery`. See [Attempt panel](/horsies/monitoring/web-ui-overview/#attempt-panel);
- the Timeline view: one row per node on one time axis, which shows the order of the steps and the waits between them;
- failure navigation, which moves through the `FAILED` nodes.

The Tasks surface lists each step as a task. The task name filter shows one step across all orders.

## When One Task Is the Better Choice

A node per step adds queue work. Use one task in these cases:

- **Short steps.** Each node is one task: a task row, a claim by a worker, a stored result, and the update that makes the next node ready. The next node starts only after a worker claims it. When the work of a step is smaller than this queue work, put the steps in one task.
- **Shared live state.** A node gets its input from stored results only. Each result is encoded as JSON, stored, and decoded for the next node, which can run in another process. An open database transaction, a connection, a session or a lock cannot pass from one node to the next. Steps that must commit in one database transaction belong in one task.
- **Decisions on intermediate results.** The DAG of a workflow is fixed when the workflow starts. A node cannot select the next node from a value. A `SubWorkflowNode` builds a child DAG from the results of its `args_from` nodes when it becomes ready. See [Dynamic Fan-Out with `args_from`](/horsies/concepts/workflows/subworkflows/#dynamic-fan-out-with-args_from). When the next step depends on intermediate values at many points, keep that logic in one task.

Group the steps by effect: one node for each external effect, with the short steps and the decisions of that effect in the same node.

## External Effects Need Idempotency

A node runs at least once. Its body can run more than once:

- The worker crashes after the effect and before the result is stored. With `WORKER_CRASHED` in `auto_retry_for`, the retry does the effect again.
- The answer is lost. The provider did the effect, but the call timed out. The step returns a code in `auto_retry_for`, and the retry calls the provider again.
- The workflow starts again. A new start runs every node again.

A cancel does not undo an effect. An operator cancel of a running step (`cancel_task` with `include_running=True`, or the web UI) makes the step `CANCELLED` at once. The body gets the cancel: an `async def` body at its next `await`, a `def` body when it reads `runtime.cancel_state()`. An effect that the body did before it stopped stays, and no result is stored for it. See [Cancelling a RUNNING Task](/horsies/monitoring/action-semantics/#cancelling-a-running-task).

Rules:

- A step that calls an external API sends the idempotency key of that API. The API answers a repeated key with the result of the first call, and does not do the effect again.
- Make the key from a stable business id and the step name, for example `checkout-{order_id}-charge`. The same order and the same step give the same key on each run.
- A step that writes to your own database uses a unique key for the write, for example `INSERT ... ON CONFLICT (order_id) DO NOTHING`.
- A call to an API with no idempotency key can do its effect again on each run of its step. Put that call in a node with no other step, so that only its own failures repeat it.

## Things to Avoid

**Don't put two effects in one node.** A retry of the node repeats both effects.

**Don't make an idempotency key from a random value or from the task id.** A random value changes on each run. A task id changes on each new start of the workflow, and when a resume after a pause starts the node again. In both cases the provider gets a new key and does the effect again.

**Don't add `WORKER_CRASHED` to `auto_retry_for` of a step whose effect has no idempotency key.** The retry does the effect again.

**Don't use the `idempotency_key` of a task send for this.** A repeat of the same send returns the first task. The key does not stop a retry of the task body. See [Make a Send Repeatable](/horsies/tasks/sending-tasks/#make-a-send-repeatable).
