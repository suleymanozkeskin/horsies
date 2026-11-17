"""The send process of the Python side of a parity run.

`python -m tests.parity.send <scenario>` sends the scenario's tasks, starts
its workflows, then runs its actions on the started workflows, through the
parity app on the database of `HORSIES_PARITY_DATABASE_URL`. It prints one
JSON line per send and per start, in order: `{"kind": "task", "index": 0, "id": "<uuid>"}` and
`{"kind": "workflow", "index": 0, "id": "<uuid>"}`. Exit status 0 when
every send and start was written; 1 with the refusal on stderr.

The Rust side runs `horsies-parity send <scenario>`, which prints the same
lines.
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Mapping
from typing import Any

from horsies import TaskSendAsyncContext
from horsies.core.models.workflow import WorkflowHandle
from horsies.core.types.result import Err, Ok
from tests.parity import app as parity
from tests.parity.scenarios import ActionKind, Send, SendForm, WorkflowAction, scenario_named


class SendRefused(Exception):
    """A send or a start of the scenario was refused; the message names it."""


def _int(kwargs: Mapping[str, int], name: str) -> int:
    value = kwargs.get(name)
    if value is None:
        raise SendRefused(f'the send has no input {name!r}: {dict(kwargs)}')
    return value


async def _send_retry(send: Send) -> str:
    """The id of the task that two retries of a refused blocking send wrote.
    Only `parity.add` has this form."""
    kwargs = dict(send.kwargs)
    match send.task_name:
        case 'parity.add':
            pass
        case other:
            raise SendRefused(f'the retry form has no task {other!r}')
    match parity.add.send(a=_int(kwargs, 'a'), b=_int(kwargs, 'b')):
        case Err(TaskSendAsyncContext() as refused):
            pass
        case other:
            raise SendRefused(f'the blocking send of {send.task_name} answered {other}')
    ids: list[str] = []
    for _attempt in range(2):
        match await parity.add.retry_send_async(refused):
            case Ok(handle):
                ids.append(str(handle.task_id))
            case Err(error):
                raise SendRefused(f'the retry of {send.task_name} was refused: {error}')
    if ids[0] != ids[1]:
        raise SendRefused(f'the two retries of {send.task_name} wrote two tasks: {ids}')
    return ids[0]


async def _send_one(send: Send) -> str:
    """The id of the task of `send`."""
    match send.form:
        case SendForm.RETRY_OF_ASYNC_CONTEXT:
            return await _send_retry(send)
        case SendForm.DIRECT:
            pass
    kwargs = dict(send.kwargs)
    key = send.idempotency_key
    match send.task_name:
        case 'parity.add':
            sent = await parity.add.with_options(idempotency_key=key).send_async(
                a=_int(kwargs, 'a'), b=_int(kwargs, 'b'),
            )
        case 'parity.refuse':
            sent = await parity.refuse.with_options(idempotency_key=key).send_async(
                value=_int(kwargs, 'value'),
            )
        case 'parity.transient':
            sent = await parity.transient.with_options(idempotency_key=key).send_async(
                value=_int(kwargs, 'value'),
            )
        case 'parity.sleep_past_deadline':
            sent = await parity.sleep_past_deadline.with_options(
                idempotency_key=key,
            ).send_async()
        case 'parity.add_async':
            sent = await parity.add_async.with_options(idempotency_key=key).send_async(
                a=_int(kwargs, 'a'), b=_int(kwargs, 'b'),
            )
        case 'parity.refuse_async':
            sent = await parity.refuse_async.with_options(
                idempotency_key=key,
            ).send_async(value=_int(kwargs, 'value'))
        case 'parity.transient_async':
            sent = await parity.transient_async.with_options(
                idempotency_key=key,
            ).send_async(value=_int(kwargs, 'value'))
        case 'parity.async_past_deadline':
            sent = await parity.async_past_deadline.with_options(
                idempotency_key=key,
            ).send_async()
        case 'parity.add_thread':
            sent = await parity.add_thread.with_options(idempotency_key=key).send_async(
                a=_int(kwargs, 'a'), b=_int(kwargs, 'b'),
            )
        case 'parity.refuse_thread':
            sent = await parity.refuse_thread.with_options(
                idempotency_key=key,
            ).send_async(value=_int(kwargs, 'value'))
        case 'parity.transient_thread':
            sent = await parity.transient_thread.with_options(
                idempotency_key=key,
            ).send_async(value=_int(kwargs, 'value'))
        case 'parity.thread_past_deadline':
            sent = await parity.thread_past_deadline.with_options(
                idempotency_key=key,
            ).send_async()
        case other:
            raise SendRefused(f'the parity app has no task {other!r}')
    match sent:
        case Ok(handle):
            return str(handle.task_id)
        case Err(refused):
            raise SendRefused(f'the send of {send.task_name} was refused: {refused}')


async def _start_one(workflow_name: str) -> WorkflowHandle[Any]:
    """The handle of the started workflow `workflow_name`."""
    match workflow_name:
        case parity.CHAIN_NAME:
            started = await parity.parity_chain.start_async()
        case parity.PARENT_NAME:
            started = await parity.parity_parent.start_async()
        case other:
            raise SendRefused(f'the parity app has no workflow {other!r}')
    match started:
        case Ok(handle):
            return handle
        case Err(refused):
            raise SendRefused(f'the start of {workflow_name} was refused: {refused}')


async def _act(action: WorkflowAction, handles: list[WorkflowHandle[Any]]) -> None:
    """Run `action` on its started workflow. A pause and a resume must
    answer true."""
    if not 0 <= action.index < len(handles):
        raise SendRefused(f'{action} names no started workflow')
    handle = handles[action.index]
    match action.kind:
        case ActionKind.PAUSE:
            answered = await handle.pause_async()
        case ActionKind.RESUME:
            answered = await handle.resume_async()
        case ActionKind.CANCEL:
            match await handle.cancel_async():
                case Ok(_):
                    return
                case Err(refused):
                    raise SendRefused(f'{action} was refused: {refused}')
    match answered:
        case Ok(ok_value=answer):
            match answer:
                case True:
                    return
                case False:
                    raise SendRefused(f'{action} answered false')
        case Err(refused):
            raise SendRefused(f'{action} was refused: {refused}')


async def _run(scenario_name: str) -> list[str]:
    scenario = scenario_named(scenario_name)
    lines: list[str] = []
    for index, send in enumerate(scenario.sends):
        task_id = await _send_one(send)
        lines.append(json.dumps({'kind': 'task', 'index': index, 'id': task_id}))
    handles: list[WorkflowHandle[Any]] = []
    for index, workflow_name in enumerate(scenario.workflows):
        handle = await _start_one(workflow_name)
        handles.append(handle)
        lines.append(
            json.dumps({'kind': 'workflow', 'index': index, 'id': str(handle.workflow_id)})
        )
    for action in scenario.actions:
        await _act(action, handles)
    return lines


def main(arguments: list[str]) -> int:
    """Send the scenario named by the one argument; the exit status."""
    match arguments:
        case [scenario_name]:
            pass
        case _:
            print('usage: python -m tests.parity.send <scenario>', file=sys.stderr)
            return 2
    try:
        lines = asyncio.run(_run(scenario_name))
    except (SendRefused, KeyError) as refused:
        print(f'parity send: {refused}', file=sys.stderr)
        return 1
    for line in lines:
        print(line)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
