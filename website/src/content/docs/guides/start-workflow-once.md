---
title: Start a Workflow Once per Entity
section: python
summary: Derive a deterministic workflow_id from an entity key so that repeated starts for the same entity create one workflow; a repeat start must carry the same start command.
related: [../concepts/workflows/workflow-api, ../concepts/workflows/workflow-semantics, ../configuration/retention-config]
tags: [guides, workflows, idempotency, workflow_id, start-command]
---

# Start a Workflow Once per Entity

A deterministic `workflow_id` makes `start()` idempotent for one entity. The second start with the same id and the same start command creates no second workflow. It returns a handle to the first one. A second start with the same id and another start command is refused.

## Why and When

Use this when more than one caller can start the same workflow for the same entity. Examples are a retried HTTP request, a duplicated event, or two producers that react to the same row.

Horsies does not know your entity key. It knows only the `workflow_id`. A random id (the default) makes every start a new workflow.

## How To

Build a UUID from the entity key with `uuid.uuid5`. Use a fixed namespace UUID and include the workflow name in the key. Pass the string form as `workflow_id`.

```python
import uuid
from typing import Any

from horsies import (
    Err,
    Horsies,
    Ok,
    TaskError,
    TaskResult,
    WorkflowDefinition,
    WorkflowSpec,
    WorkflowStartCommandConflict,
    WorkflowStartIdConflict,
)
from instance import app

# Fixed for the life of the project. A new value changes every derived id.
WORKFLOW_ID_NAMESPACE = uuid.UUID("6f1d5c0e-8a54-4c57-9a0e-2b7a4f3c9d10")


@app.task("onboard_customer")
def onboard_customer(*, customer_id: str) -> TaskResult[str, TaskError]:
    return TaskResult(ok=customer_id)


class OnboardingWorkflow(WorkflowDefinition[str]):
    name = "customer_onboarding"
    definition_key = "myapp.customer_onboarding.v1"

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        customer_id: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[str]:
        onboard = onboard_customer.node(node_id="onboard")(customer_id=customer_id)
        return app.workflow(name=cls.name, tasks=[onboard], output=onboard)


def onboarding_workflow_id(customer_id: str) -> str:
    """Return the same id for the same workflow name and the same customer."""
    key = f"{OnboardingWorkflow.name}:{customer_id}"
    return str(uuid.uuid5(WORKFLOW_ID_NAMESPACE, key))


async def start_onboarding(customer_id: str) -> str:
    spec = OnboardingWorkflow.build_with(app, customer_id=customer_id)
    match await spec.start_async(workflow_id=onboarding_workflow_id(customer_id)):
        case Ok(handle):
            return f"workflow {handle.workflow_id} is running or finished"
        case Err(WorkflowStartIdConflict(existing_workflow_name=existing)):
            return f"the id belongs to another workflow: {existing}"
        case Err(WorkflowStartCommandConflict(workflow_id=existing_id)):
            return f"workflow {existing_id} was started with another start command"
        case Err(start_error):
            return f"start failed: {start_error.code.value}"
```

## Semantics

The start inserts the workflow row. When the id exists, the start locks that row and compares the start command of this start with the stored one. The result depends on the row that holds the id.

| State of the id | Result of `start()` |
|---|---|
| No row | A new workflow starts. `Ok(handle)`. |
| Row with the same workflow name and the same start command | `Ok(handle)` for the existing workflow. Horsies logs a warning. No node is inserted or enqueued. |
| Row with the same workflow name and another start command | `Err(WorkflowStartCommandConflict)`, code `WORKFLOW_COMMAND_CONFLICT`. Nothing is written. |
| Row with another workflow name | `Err(WorkflowStartIdConflict)`, code `WORKFLOW_ID_CONFLICT`. The error has the field `existing_workflow_name`. Nothing is written. |
| Row of a sub-workflow | `Err(WorkflowStartCommandConflict)`, code `WORKFLOW_COMMAND_CONFLICT`. A sub-workflow has no start command. Nothing is written. |
| Row of a run started before this release | `Ok(handle)` for the existing workflow. The row has no stored start command, so the start does not compare it. Horsies logs a warning. |
| Value that is not a UUID | `Err(WorkflowStartValidationFailed)`. See `HRS-032`. |

`WorkflowStartCommandConflict` and `WorkflowStartIdConflict` are never retryable. `retry_start()` refuses them. Send the same start command, or use another id.

Other rules:

- Horsies accepts any UUID version. The check parses the string as a UUID. It does not look at the version.
- Runs started before this release are not checked. A start with their id and their name returns their handle.
- `Ok(handle)` does not tell whether this call created the workflow. The handle can point to a workflow that is already `COMPLETED` or `FAILED`.
- Two starts with the same id at the same time: the first commit wins. The second start waits for it. Then it returns the existing handle (same start command) or `WorkflowStartCommandConflict` (another start command).
- A start that failed before its transaction committed leaves no row. The same id can start again. `retry_start()` builds the same spec again, so a first attempt that committed is found as the same start command. See `retry_start()` in [Workflow API](/horsies/concepts/workflows/workflow-api/).

## What the Start Command Is

The core computes the start command from the spec. No client computes it. The row stores its SHA-256 (version 1).

The start command holds:

- The workflow: name, `definition_key`, `on_error`, the output node, the success policy.
- Each node of the top-level workflow, in index order: index, `node_id`, task name, kwargs (as the client encoded them, byte for byte), queue, priority, dependencies, `args_from`, `workflow_ctx_from`, `allow_failed_deps`, join rule, `min_success`, task options (with `good_until` merged in), retention class, whether the node is a sub-workflow node, its sub-workflow `definition_key`, and whether it gives params.

The start command does not hold:

- The `workflow_id`.
- The call time (`sent_at`).
- The trace carrier.
- `resend_on_transient_err`.
- The child workflows that the sub-workflow roots build at the start. Their ids are new on each start.

## Retention Ends the Protection

The protection lasts while the workflow row exists. The retention sweep deletes a terminal workflow row after `terminal_record_retention_hours` (default 720). After that, a start with the same id starts a new workflow, with no compare.

Set `terminal_record_retention_hours=None` to keep terminal rows. See [Retention Config](/horsies/configuration/retention-config/).

## Things to Avoid

**Don't leave the workflow name out of the key.** The id is unique across all workflows. Two workflow types that use the same entity key get the same id. The second type fails with `WORKFLOW_ID_CONFLICT`.

**Don't put changing data in the key.** A key that includes a timestamp or a status gives a new id for each change, so nothing deduplicates.

**Don't put changing data in the start command.** A value that changes on each call makes each repeat start with the same id a `WORKFLOW_COMMAND_CONFLICT`. Examples:

- A `good_until` computed from the current time.
- A timestamp or a request id in the kwargs.
- A queue, priority or retention config that changed between the two starts.
- A spec that changed in a deploy between the two starts.

Pass only the data of the entity. Read changing values inside the task.

**Don't use `hash()` or a random value for the key.** `hash()` of a string differs between Python processes. Use `uuid5`.

**Don't change the namespace UUID.** Each derived id changes. Running workflows are no longer found by their old ids.
