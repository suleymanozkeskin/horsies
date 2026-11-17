"""The send process of the Python side of a parity run.

`python -m tests.parity.send <scenario>` sends the scenario's tasks, then
starts its workflows, through the parity app on the database of
`HORSIES_PARITY_DATABASE_URL`. It prints one JSON line per send and per
start, in order: `{"kind": "task", "index": 0, "id": "<uuid>"}` and
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

from horsies.core.types.result import Err, Ok
from tests.parity import app as parity
from tests.parity.scenarios import Send, scenario_named


class SendRefused(Exception):
    """A send or a start of the scenario was refused; the message names it."""


def _int(kwargs: Mapping[str, int], name: str) -> int:
    value = kwargs.get(name)
    if value is None:
        raise SendRefused(f'the send has no input {name!r}: {dict(kwargs)}')
    return value


async def _send_one(send: Send) -> str:
    """The id of the task of `send`."""
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


async def _start_one(workflow_name: str) -> str:
    """The id of the started workflow `workflow_name`."""
    match workflow_name:
        case parity.CHAIN_NAME:
            started = await parity.parity_chain.start_async()
        case other:
            raise SendRefused(f'the parity app has no workflow {other!r}')
    match started:
        case Ok(handle):
            return str(handle.workflow_id)
        case Err(refused):
            raise SendRefused(f'the start of {workflow_name} was refused: {refused}')


async def _run(scenario_name: str) -> list[str]:
    scenario = scenario_named(scenario_name)
    lines: list[str] = []
    for index, send in enumerate(scenario.sends):
        task_id = await _send_one(send)
        lines.append(json.dumps({'kind': 'task', 'index': index, 'id': task_id}))
    for index, workflow_name in enumerate(scenario.workflows):
        workflow_id = await _start_one(workflow_name)
        lines.append(json.dumps({'kind': 'workflow', 'index': index, 'id': workflow_id}))
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
