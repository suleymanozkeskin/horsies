---
title: Start a Workflow Once per Entity
summary: Derive a deterministic workflow_id from an entity key so that repeated starts for the same entity create one workflow.
related: [../concepts/workflows/workflow-api, ../concepts/workflows/workflow-semantics, ../configuration/retention-config]
tags: [guides, workflows, idempotency, workflow_id]
---

# Start a Workflow Once per Entity

A deterministic `workflow_id` makes `start()` idempotent for one entity. The second start with the same id creates no second workflow. It returns a handle to the first one.

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
            return f"the id belongs to workflow {existing}"
        case Err(start_error):
            return f"start failed: {start_error.code.value}"
```

## Semantics

The workflow row is inserted with `ON CONFLICT (id) DO NOTHING`. The result depends on the row that holds the id.

| State of the id | Result of `start()` |
|---|---|
| No row | A new workflow starts. `Ok(handle)`. |
| Row with the same workflow name | `Ok(handle)` for the existing workflow. Horsies logs a warning. No node is inserted or enqueued. |
| Row with another workflow name | `Err(WorkflowStartIdConflict)`, code `WORKFLOW_ID_CONFLICT`. The error has the field `existing_workflow_name`. Nothing is written. |
| Value that is not a UUID | `Err(WorkflowStartValidationFailed)`. See `HRS-032`. |

Other rules:

- Horsies accepts any UUID version. The check parses the string as a UUID. It does not look at the version.
- Horsies does not compare the payload. A second start with other arguments and the same id returns the first workflow. The new arguments are dropped.
- `Ok(handle)` does not tell whether this call created the workflow. The handle can point to a workflow that is already `COMPLETED` or `FAILED`.
- A start that failed before its transaction committed leaves no row. The same id can start again. See `retry_start()` in [Workflow API](/horsies/concepts/workflows/workflow-api/).

## Retention Ends the Protection

The protection exists while the workflow row exists. The retention sweep deletes terminal workflow rows after `terminal_record_retention_hours` (default 720). After that, the same id starts a new workflow.

Set `terminal_record_retention_hours=None` to keep terminal rows. See [Retention Config](/horsies/configuration/retention-config/).

## Things to Avoid

**Don't leave the workflow name out of the key.** The id is unique across all workflows. Two workflow types that use the same entity key get the same id. The second type fails with `WORKFLOW_ID_CONFLICT`.

**Don't put changing data in the key.** A key that includes a timestamp or a status gives a new id for each change, so nothing deduplicates.

**Don't use `hash()` or a random value for the key.** `hash()` of a string differs between Python processes. Use `uuid5`.

**Don't change the namespace UUID.** Each derived id changes. Running workflows are no longer found by their old ids.
