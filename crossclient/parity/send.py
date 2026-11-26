"""The send process of the Python side of a parity run.

`python -m crossclient.parity.send <scenario>` sends the scenario's tasks, reruns
the tasks of its reruns, starts its workflows, runs its actions on the
started workflows, then pings, through the parity app on the database of
`HORSIES_PARITY_DATABASE_URL`. It prints one JSON line per send, rerun,
start and ping, in this order:
- `{"kind": "task", "index": 0, "id": "<uuid>"}` for each send, then for
  each rerun (the new task), with one index sequence;
- `{"kind": "workflow", "index": 0, "id": "<uuid>"}` for each start;
- `{"kind": "ping", "index": 0, "workers": 2, "targeted": 1}`: the count of
  workers that answered the ping of every worker, and the count that
  answered the ping of the first of them by its id.

Exit status 0 when every step was written; 1 with the refusal on stderr.

The Rust side runs `horsies-parity send <scenario>`, which prints the same
lines.
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Final
from uuid import UUID

from sqlalchemy.ext.asyncio import create_async_engine

from horsies import (
    RerunCommand,
    RerunEnqueued,
    RerunEnqueuePolicy,
    TaskSendAsyncContext,
    WorkerPong,
    rerun_task_in_async,
)
from horsies.core.models.outcome import Failed
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import WorkflowHandle
from horsies.core.types.result import Err, Ok
from crossclient.parity import app as parity
from crossclient.parity.scenarios import (
    ActionKind,
    Health,
    Rerun,
    Send,
    SendForm,
    WorkflowAction,
    scenario_named,
)
from tests.sqlalchemy_url import sqlalchemy_url

RESULT_WAIT_MS: Final = 30_000
"""The longest wait for the end of a rerun source."""

RERUN_POLICY: Final = RerunEnqueuePolicy(
    retention=timedelta(days=30),
    retain_rerun_input=True,
    reservation_window=timedelta(hours=24),
)
"""The default retention class (`standard_30d`), the input retained, a
24 h key reservation: Rust `RerunEnqueuePolicy::standard(true)`."""

PING_WINDOW_S: Final = 2.0
"""The window of one worker ping."""

PING_ATTEMPTS: Final = 10
"""The pings of every worker before the send process gives up: a worker
that has just started may not listen yet."""


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
            raise SendRefused(f'the resend form has no task {other!r}')
    match parity.add.send(a=_int(kwargs, 'a'), b=_int(kwargs, 'b')):
        case Err(TaskSendAsyncContext() as refused):
            pass
        case other:
            raise SendRefused(f'the blocking send of {send.task_name} answered {other}')
    ids: list[str] = []
    for _attempt in range(2):
        match await parity.add.resend_async(refused):
            case Ok(handle):
                ids.append(str(handle.task_id))
            case Err(error):
                raise SendRefused(f'the resend of {send.task_name} was refused: {error}')
    if ids[0] != ids[1]:
        raise SendRefused(f'the two resends of {send.task_name} wrote two tasks: {ids}')
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


async def _rerun_one(rerun: Rerun, sent: list[str]) -> str:
    """The id of the new task of `rerun`, after the end of its source."""
    if not 0 <= rerun.source < len(sent):
        raise SendRefused(f'{rerun} names no earlier task line')
    source = UUID(sent[rerun.source])
    match await parity.app.get_result_async(source, timeout_ms=RESULT_WAIT_MS):
        case Ok(Failed(TaskError(error_code='REFUSED'))):
            pass
        case other:
            raise SendRefused(f'the rerun source {source} did not end with REFUSED: {other}')
    database_url = parity.config.broker.database_url.get_secret_value()
    engine = create_async_engine(sqlalchemy_url(database_url))
    try:
        async with engine.begin() as connection:
            outcome = await rerun_task_in_async(
                connection,
                RerunCommand(source_task_id=source, good_until=None),
                RERUN_POLICY,
            )
    finally:
        await engine.dispose()
    match outcome:
        case RerunEnqueued(new_task_id=new_task_id):
            return str(new_task_id)
        case refused:
            raise SendRefused(f'the rerun of {source} answered {refused}')


async def _every_worker(expected: int) -> list[WorkerPong]:
    """The answers to a ping of every worker, once `expected` workers
    answered one ping."""
    answered: list[WorkerPong] = []
    for _attempt in range(PING_ATTEMPTS):
        match await parity.app.ping_workers_async(
            timeout_seconds=PING_WINDOW_S, min_responses=expected,
        ):
            case Ok(pongs):
                answered = pongs
            case Err(refused):
                raise SendRefused(f'ping_workers was refused: {refused}')
        if len(answered) >= expected:
            return answered
    raise SendRefused(
        f'{len(answered)} of {expected} workers answered the last of '
        f'{PING_ATTEMPTS} pings',
    )


async def _ping(expected: int) -> str:
    """The ping line: the database answers, then each of the `expected`
    workers, then the first of them by its id."""
    match await parity.app.ping_database_async():
        case Ok(_):
            pass
        case Err(refused):
            raise SendRefused(f'ping_database was refused: {refused}')
    answered = await _every_worker(expected)
    match await parity.app.ping_workers_async(
        target_worker_id=answered[0].worker_id, timeout_seconds=PING_WINDOW_S,
    ):
        case Ok(targeted):
            pass
        case Err(refused):
            raise SendRefused(f'the ping of worker {answered[0].worker_id} was refused: {refused}')
    return json.dumps(
        {'kind': 'ping', 'index': 0, 'workers': len(answered), 'targeted': len(targeted)},
    )


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
    sent: list[str] = []
    for send in scenario.sends:
        sent.append(await _send_one(send))
    for rerun in scenario.reruns:
        sent.append(await _rerun_one(rerun, sent))
    for index, task_id in enumerate(sent):
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
    match scenario.health:
        case Health.PINGED:
            lines.append(await _ping(len(scenario.profiles)))
        case Health.NOT_PINGED:
            pass
    return lines


def main(arguments: list[str]) -> int:
    """Send the scenario named by the one argument; the exit status."""
    match arguments:
        case [scenario_name]:
            pass
        case _:
            print('usage: python -m crossclient.parity.send <scenario>', file=sys.stderr)
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
