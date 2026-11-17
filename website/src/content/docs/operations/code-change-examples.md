---
title: Code Change Examples
section: python
summary: The Python code of the deploy steps - a parameter with a default, two names for one task, a model field with a default, and a renamed model field with AliasChoices.
related: [deploying-code-changes, ../tasks/defining-tasks, ../tasks/task-types]
tags: [operations, deploy, rename, model-change, AliasChoices, pydantic]
---

# Code Change Examples

The order of each deploy and the error of each change are on [Deploying Code Changes](/horsies/operations/deploying-code-changes/). This page holds the Python code of its steps.

## Add a Parameter

Give the new parameter a default value. See [Add a Parameter](/horsies/operations/deploying-code-changes/#add-a-parameter) for the deploy order.

```python
from horsies import TaskError, TaskResult

from myapp.instance import app


@app.task('send_email')
def send_email(
    *,
    to: str,
    subject: str,
    locale: str = 'en',
) -> TaskResult[None, TaskError]:
    return TaskResult(ok=None)
```

## Rename or Remove a Task

Register the old name and the new name, both calling one function body. Remove the old task after no row with its name remains. See [Rename or Remove a Task](/horsies/operations/deploying-code-changes/#rename-or-remove-a-task) for the deploy order.

```python
from horsies import TaskError, TaskResult

from myapp.instance import app


def _deliver_report(*, report_id: str) -> TaskResult[None, TaskError]:
    return TaskResult(ok=None)


@app.task('build_report')
def build_report(*, report_id: str) -> TaskResult[None, TaskError]:
    return _deliver_report(report_id=report_id)


# Old name. Remove it after no row with this name remains.
@app.task('make_report')
def make_report(*, report_id: str) -> TaskResult[None, TaskError]:
    return _deliver_report(report_id=report_id)
```

## Add a Model Field

Give the new field a default. See [Add a Field](/horsies/operations/deploying-code-changes/#add-a-field) for the deploy order.

```python
from pydantic import BaseModel


class Car(BaseModel):
    model: str
    km: int
    colour: str = 'unknown'
```

## Rename a Model Field

`AliasChoices` accepts both names on read. The new name must be in `AliasChoices`, or `@app.task` refuses the model. See [Rename a Field](/horsies/operations/deploying-code-changes/#rename-a-field) for the deploy order.

```python
from pydantic import AliasChoices, BaseModel, Field


class Car(BaseModel):
    model: str
    km: int
    registration_year: int = Field(
        validation_alias=AliasChoices('registration_year', 'register_year'),
    )
```
