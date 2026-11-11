"""The pool and the task-send pool of each child kind, from the worker
config to the init frame that each child reads.

Enters at ``Worker._slot_pool_config`` and ``SlotPool``, the real builders
of the init frames, and asserts on the frames after a trip over the wire.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from horsies.core.models.child_sizing import (
    DEFAULT_TASK_SEND_POOL_CONNECTIONS,
    shared_async_executor_threads,
    shared_async_pool_connections,
    shared_thread_pool_connections,
)
from horsies.core.models.worker_profile import (
    ExecutorThreads,
    PoolSize,
    ResolvedPool,
    SharedAsyncChildren,
    SharedPool,
    SharedThreadChildren,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    ChildKind,
    InitFrame,
    IsolatedChild,
    SharedAsyncChild,
    SharedThreadChild,
    decode_parent_frame,
    encode_parent_frame,
)
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.slots import SlotPool
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.unit, pytest.mark.asyncio(loop_scope='function')]

_ONE_ASYNC_CHILD: dict[str, object] = {
    'shared_async': SharedAsyncChildren(count=1, seats=1),
}


def _worker(cfg: WorkerConfig) -> Worker:
    return Worker(session_factory=MagicMock(), listener=MagicMock(), cfg=cfg)


def _config(**fields: object) -> WorkerConfig:
    return WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
        **fields,  # type: ignore[arg-type]
    )


def _over_the_wire(init: InitFrame) -> ChildKind:
    """The child kind that a child decodes from ``init``."""
    encoded = encode_parent_frame(init)
    meta = encoded.head[FRAME_HEADER.size :]
    match decode_parent_frame(meta, encoded.body):
        case Ok(InitFrame(child_kind=child_kind)):
            return child_kind
        case Ok(other):
            raise AssertionError(f'not an init frame: {other!r}')
        case Err(malformed):
            raise AssertionError(malformed)


def _kinds_of_children(worker: Worker) -> list[ChildKind]:
    """The child kind of each slot, in slot order, as its child reads it."""
    config = worker._slot_pool_config()  # pyright: ignore[reportPrivateUsage]
    pool = SlotPool(config, pending_limit=1)
    slots = [
        *pool._slots,  # pyright: ignore[reportPrivateUsage]
        *pool._shared_slots,  # pyright: ignore[reportPrivateUsage]
        *pool._thread_slots,  # pyright: ignore[reportPrivateUsage]
    ]
    return [_over_the_wire(slot.init_for(config.init)) for slot in slots]


async def test_each_kind_gets_the_resolved_pool_of_its_setting() -> None:
    cfg = _config(
        processes=1,
        isolated_pool=ResolvedPool(min_size=1, max_size=3, check=False),
        shared_async=SharedAsyncChildren(
            count=1,
            seats=25,
            pool=SharedPool(min_size=1),
            executor=ExecutorThreads(threads=6),
        ),
        shared_thread=SharedThreadChildren(count=1, seats=8),
    )

    assert _kinds_of_children(_worker(cfg)) == [
        IsolatedChild(
            pool=ResolvedPool(min_size=1, max_size=3, check=False),
            task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
        ),
        SharedAsyncChild(
            seats=25,
            pool=ResolvedPool(
                min_size=1,
                max_size=shared_async_pool_connections(25),
                check=False,
            ),
            executor_threads=6,
            task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
        ),
        SharedThreadChild(
            seats=8,
            pool=ResolvedPool(
                min_size=0,
                max_size=shared_thread_pool_connections(8),
                check=False,
            ),
            task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
        ),
    ]


async def test_every_child_of_a_kind_gets_the_same_pool() -> None:
    cfg = _config(
        processes=0,
        shared_async=SharedAsyncChildren(count=3, seats=40),
        shared_thread=SharedThreadChildren(
            count=2,
            seats=4,
            pool=SharedPool(max_size=PoolSize(connections=7), check=False),
        ),
    )

    kinds = _kinds_of_children(_worker(cfg))

    async_kind = SharedAsyncChild(
        seats=40,
        pool=ResolvedPool(
            min_size=0,
            max_size=shared_async_pool_connections(40),
            check=False,
        ),
        executor_threads=shared_async_executor_threads(40),
        task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
    )
    thread_kind = SharedThreadChild(
        seats=4,
        pool=ResolvedPool(min_size=0, max_size=7, check=False),
        task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
    )
    assert kinds == [async_kind, async_kind, async_kind, thread_kind, thread_kind]


async def test_the_isolated_default_is_a_pool_of_two_without_a_check() -> None:
    isolated_kind = IsolatedChild(
        pool=ResolvedPool(min_size=0, max_size=2, check=False),
        task_send_pool=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
    )
    kinds = _kinds_of_children(_worker(_config(processes=2)))
    assert kinds == [isolated_kind, isolated_kind]


async def test_each_kind_gets_the_task_send_pool_of_its_setting() -> None:
    """Each kind carries its own task_send_pool, not the parent pool and not
    the pool of another kind."""
    cfg = _config(
        processes=1,
        isolated_task_send_pool=3,
        shared_async=SharedAsyncChildren(
            count=1,
            seats=4,
            task_send_pool=PoolSize(connections=5),
        ),
        shared_thread=SharedThreadChildren(
            count=1,
            seats=4,
            task_send_pool=PoolSize(connections=7),
        ),
    )

    kinds = _kinds_of_children(_worker(cfg))

    assert [kind.task_send_pool for kind in kinds] == [3, 5, 7]


async def test_the_preload_template_carries_the_isolated_task_send_pool() -> None:
    """The template init frame, which a preload parent imports the app with,
    is an isolated kind with the isolated task-send pool."""
    cfg = _config(processes=0, isolated_task_send_pool=4, **_ONE_ASYNC_CHILD)
    template = _worker(cfg)._slot_pool_config().init  # pyright: ignore[reportPrivateUsage]
    assert _over_the_wire(template) == IsolatedChild(
        pool=ResolvedPool(min_size=0, max_size=2, check=False),
        task_send_pool=4,
    )
