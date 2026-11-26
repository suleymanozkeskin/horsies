"""Workload tasks for the memory-recycle spike: noop, alloc, leaky.

Each returns the executing child PID so the driver can measure rotation.

- ``noop``: no allocation — baseline rotation under count recycling only.
- ``alloc``: a transient allocation freed before return — exercises the gap
  between current RSS (what the recycle reader samples) and peak RSS.
- ``leaky``: a retained allocation that accumulates per child, so child RSS
  ratchets up until ``max_memory_per_child_mb`` recycles it.
"""

from __future__ import annotations

import os

from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result

from tests.spike.mem_accum.repro_app import app

# Retained per child: never freed, so child RSS climbs across leaky calls.
_RETAINED: list[bytearray] = []


@app.task(task_name='mem_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(task_name='mem_noop')
def noop_task() -> Result[int, TaskError]:
    return Ok(os.getpid())


@app.task(task_name='mem_alloc')
def alloc_task(*, mb: int) -> Result[int, TaskError]:
    """Allocate `mb` MB transiently and free it before returning."""
    buf = bytearray(mb * 1024 * 1024)
    size = len(buf)
    del buf
    assert size == mb * 1024 * 1024
    return Ok(os.getpid())


@app.task(task_name='mem_leaky')
def leaky_task(*, mb: int) -> Result[int, TaskError]:
    """Retain `mb` MB so child RSS ratchets up across calls."""
    _RETAINED.append(bytearray(mb * 1024 * 1024))
    return Ok(os.getpid())
