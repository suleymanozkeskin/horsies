"""Memory-bounded child pool: per-child RSS recycle on top of stdlib.

``HorsiesProcessPoolExecutor`` extends ``ProcessPoolExecutor`` with a
per-child memory recycle: each child samples its own RSS after every real
task and, when at or above ``max_memory_per_child_mb``, exits cleanly via the
stdlib ``exit_pid`` marker so the pool manager replaces only that child. The
recycle is a child-side clean exit (never a parent-side kill), which keeps it
on the stdlib's clean single-child replacement path rather than the
``BrokenProcessPool`` crash path.

This couples to private ``concurrent.futures.process`` internals; the surface
it depends on is asserted by ``verify_cpython_internals`` before the feature is
enabled, and the feature is CPython-only.
"""

# This module is a pinned copy and subclass of private concurrent.futures
# internals; private/unknown access is intentional and verified at runtime by
# verify_cpython_internals.
# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

import inspect
import os
import platform
import sys
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import process as _cf_process
from typing import Any, Optional

from horsies.core.logging import get_logger
from horsies.core.worker.runtime import (
    _read_current_rss_mb,
    _warm_child_process,
    _warm_child_process_probe,
)

logger = get_logger('worker')

# Module-level aliases for the private worker protocol. Resolved once at import;
# verify_cpython_internals() guarantees these exist before the feature enables.
# Typed Any: the stubs are stricter than the real (untyped) runtime functions
# the stdlib worker loop itself calls this exact way.
_sendback_result: Any = _cf_process._sendback_result
_ExceptionWithTraceback: Any = _cf_process._ExceptionWithTraceback

# Exact CPython minor versions whose ProcessPoolExecutor internals this module
# is verified against. The version guard refuses to enable outside this set.
_SUPPORTED_PYTHON_MINORS = frozenset({(3, 13)})

# Calls the memory predicate must never recycle on: the internal warmup
# probes, whose RSS is the baseline, not a per-task signal. Identity holds
# under spawn (children re-import these module-level functions).
_RECYCLE_EXEMPT_CALLS = (_warm_child_process, _warm_child_process_probe)


class MemoryRecycleUnsupportedError(RuntimeError):
    """The interpreter cannot safely run the memory-recycle feature.

    Raised by ``verify_cpython_internals`` when the implementation is not
    CPython, the minor version is outside the verified set, or a required
    private ProcessPoolExecutor symbol has drifted. The feature stays
    disabled rather than silently running on an unverified internal shape.
    """


def _memory_recycle_rss(call_item: Any, threshold_mb: Optional[int]) -> Optional[float]:
    """Return the measured RSS when this call should trigger a memory recycle.

    Returns ``None`` (no recycle) when the feature is off, the call is an
    internal warmup probe (identity match, which survives ``spawn``), RSS is
    unknown, or RSS is below the threshold.
    """
    if threshold_mb is None:
        return None
    if call_item.fn in _RECYCLE_EXEMPT_CALLS:
        return None
    rss_mb = _read_current_rss_mb()
    if rss_mb is not None and rss_mb >= threshold_mb:
        return rss_mb
    return None


def _horsies_process_worker(
    call_queue: Any,
    result_queue: Any,
    initializer: Optional[Any],
    initargs: tuple[Any, ...],
    max_tasks: Optional[int],
    max_memory_per_child_mb: Optional[int],
) -> None:
    """Pinned copy of CPython 3.13 ``_process_worker`` with a memory predicate.

    Identical to stdlib except: after each real task it samples RSS and ORs a
    memory-recycle decision into the same ``exit_pid`` channel the count
    predicate uses. The count predicate runs before the task (as in stdlib);
    the memory predicate runs after, because post-task RSS is the signal. Both
    share one ``exit_pid`` on the result item, so the parent replacement path
    is identical to count recycling.
    """
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException:
            logger.critical('Exception in worker initializer', exc_info=True)
            # Parent notices the stopped child and marks the pool broken.
            return
    num_tasks = 0
    exit_pid = None
    while True:
        call_item = call_queue.get(block=True)
        if call_item is None:
            # Wake up the executor manager thread.
            result_queue.put(os.getpid())
            return

        recycle_reasons: list[str] = []
        if max_tasks is not None:
            num_tasks += 1
            if num_tasks >= max_tasks:
                exit_pid = os.getpid()
                recycle_reasons.append('max_tasks_per_child')

        mem_rss: Optional[float] = None
        try:
            r = call_item.fn(*call_item.args, **call_item.kwargs)
        except BaseException as e:
            mem_rss = _memory_recycle_rss(call_item, max_memory_per_child_mb)
            if mem_rss is not None:
                exit_pid = os.getpid()
                recycle_reasons.append('max_memory_per_child_mb')
            exc = _ExceptionWithTraceback(e, e.__traceback__)
            _sendback_result(
                result_queue,
                call_item.work_id,
                exception=exc,
                exit_pid=exit_pid,
            )
        else:
            mem_rss = _memory_recycle_rss(call_item, max_memory_per_child_mb)
            if mem_rss is not None:
                exit_pid = os.getpid()
                recycle_reasons.append('max_memory_per_child_mb')
            _sendback_result(
                result_queue,
                call_item.work_id,
                result=r,
                exit_pid=exit_pid,
            )
            del r

        del call_item

        if exit_pid is not None:
            if mem_rss is not None:
                logger.info(
                    'Worker child pid=%s recycling after task: rss=%.0fMB '
                    'threshold=%sMB reasons=%s',
                    os.getpid(),
                    mem_rss,
                    max_memory_per_child_mb,
                    ','.join(recycle_reasons),
                )
            return


# Instance attrs the gh-115634 _adjust_process_count override reads.
_RECYCLE_INSTANCE_ATTRS = ('_processes', '_max_workers', '_idle_worker_semaphore')
# Additional instance attrs the memory-aware _spawn_process override reads.
_SPAWN_INSTANCE_ATTRS = (
    '_call_queue',
    '_result_queue',
    '_initializer',
    '_initargs',
    '_max_tasks_per_child',
)

# Cached result of recycle_replacement_supported().
_recycle_support: Optional[bool] = None


class _RecycleProcessPoolExecutor(ProcessPoolExecutor):
    """``ProcessPoolExecutor`` with only the gh-115634 replacement fix.

    Overrides ``_adjust_process_count`` so a cleanly-recycled child — by count
    *or* memory — is always replaced. stdlib consults the idle-worker semaphore
    first and can skip the replacement spawn after a recycle, hanging queued
    work. This checks actual process count first.

    It uses the **stock** worker loop, so it carries no dependency on the pinned
    ``_process_worker`` / ``exit_pid`` surface and needs no version gate. The
    count-recycle path uses it directly; whether it is safe to use is decided by
    ``recycle_replacement_supported`` (falling back to the stock pool otherwise).
    """

    def _adjust_process_count(self) -> None:  # pyright: ignore[reportImplicitOverride]
        # Count first, semaphore second (stdlib does the reverse and can leave
        # a recycled child unreplaced — gh-115634). Horsies warms the full pool
        # at startup, so the lost lazy-spawn optimization does not matter.
        if len(self._processes) < self._max_workers:
            self._spawn_process()
            return
        if self._idle_worker_semaphore.acquire(blocking=False):
            return


class HorsiesProcessPoolExecutor(_RecycleProcessPoolExecutor):
    """``_RecycleProcessPoolExecutor`` plus per-child memory recycle.

    Inherits the gh-115634 ``_adjust_process_count`` fix and additionally
    overrides ``_spawn_process`` to launch children on the memory-aware worker
    loop, threading ``max_memory_per_child_mb`` through to each child.
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        mp_context: Optional[Any] = None,
        initializer: Optional[Any] = None,
        initargs: tuple[Any, ...] = (),
        *,
        max_tasks_per_child: Optional[int] = None,
        max_memory_per_child_mb: Optional[int] = None,
    ) -> None:
        super().__init__(
            max_workers=max_workers,
            mp_context=mp_context,
            initializer=initializer,
            initargs=initargs,
            max_tasks_per_child=max_tasks_per_child,
        )
        self._max_memory_per_child_mb = max_memory_per_child_mb

    def _spawn_process(self) -> None:  # pyright: ignore[reportImplicitOverride]
        mp_context: Any = self._mp_context
        process = mp_context.Process(
            target=_horsies_process_worker,
            args=(
                self._call_queue,
                self._result_queue,
                self._initializer,
                self._initargs,
                self._max_tasks_per_child,
                self._max_memory_per_child_mb,
            ),
        )
        process.start()
        self._processes[process.pid] = process


def _probe_executor_attrs(names: tuple[str, ...]) -> set[str]:
    """Return which of ``names`` are absent from a fresh, child-free executor.

    Constructing without submitting sets the instance attrs in ``__init__`` but
    spawns no children; the probe executor is shut down immediately.
    """
    try:
        probe = ProcessPoolExecutor(1)
    except Exception:
        return set(names)
    try:
        return {name for name in names if not hasattr(probe, name)}
    finally:
        probe.shutdown(wait=False)


def _executor_method_params(method: str) -> Optional[list[str]]:
    """Return the parameter names for a ProcessPoolExecutor private method."""
    if not hasattr(ProcessPoolExecutor, method):
        return None
    try:
        return list(inspect.signature(getattr(ProcessPoolExecutor, method)).parameters)
    except (TypeError, ValueError):
        return None


def _executor_method_is_self_only(method: str) -> bool:
    """Whether a ProcessPoolExecutor private method is still ``method(self)``."""
    return _executor_method_params(method) == ['self']


def recycle_replacement_supported() -> bool:
    """Whether the gh-115634 ``_adjust_process_count`` override can run here.

    Cached. Checks that the methods and instance attrs the override reads exist
    on a freshly-constructed executor. The count-recycle path falls back to the
    stock pool when this is False, so an interpreter that renames or drops one
    of these attrs degrades to stock behavior instead of raising in the manager
    thread. No minor-version gate: the override is correct (and harmless if
    gh-115634 is fixed upstream) on any interpreter where the surface exists.
    """
    global _recycle_support
    if _recycle_support is not None:
        return _recycle_support
    supported = all(
        _executor_method_is_self_only(method)
        for method in ('_adjust_process_count', '_spawn_process')
    )
    if supported:
        supported = not _probe_executor_attrs(_RECYCLE_INSTANCE_ATTRS)
    _recycle_support = supported
    return supported


def verify_cpython_internals() -> None:
    """Assert the private ProcessPoolExecutor surface this module depends on.

    Raises:
        MemoryRecycleUnsupportedError: non-CPython implementation, a minor
            version outside the verified set, or a drifted/absent private
            symbol. Refusing here keeps the feature from running on an
            unverified internal shape.
    """
    implementation = platform.python_implementation()
    if implementation != 'CPython':
        raise MemoryRecycleUnsupportedError(
            'max_memory_per_child_mb requires CPython; it is built on private '
            'ProcessPoolExecutor internals (_process_worker, _spawn_process, '
            f'_ResultItem.exit_pid, _adjust_process_count). Found {implementation}.'
        )
    minor = sys.version_info[:2]
    if minor not in _SUPPORTED_PYTHON_MINORS:
        supported = ', '.join(
            f'{maj}.{min_}' for maj, min_ in sorted(_SUPPORTED_PYTHON_MINORS)
        )
        raise MemoryRecycleUnsupportedError(
            f'max_memory_per_child_mb is verified only on CPython {supported}; '
            f'running {minor[0]}.{minor[1]}. The feature stays disabled until '
            'this minor is added to the tested set.'
        )

    missing: list[str] = []
    for symbol in (
        '_ResultItem',
        '_sendback_result',
        '_ExceptionWithTraceback',
        '_process_worker',
    ):
        if not hasattr(_cf_process, symbol):
            missing.append(f'concurrent.futures.process.{symbol}')
    for method in ('_spawn_process', '_adjust_process_count'):
        if not hasattr(ProcessPoolExecutor, method):
            missing.append(f'ProcessPoolExecutor.{method}')
    if missing:
        raise MemoryRecycleUnsupportedError(
            'max_memory_per_child_mb: required ProcessPoolExecutor internals '
            f'absent: {", ".join(missing)}.'
        )

    # Constructive smoke checks: exit_pid still rides on the result item, and
    # _sendback_result still accepts it.
    result_item = _cf_process._ResultItem(work_id=0, exit_pid=123)
    if getattr(result_item, 'exit_pid', None) != 123:
        raise MemoryRecycleUnsupportedError(
            'max_memory_per_child_mb: _ResultItem no longer carries exit_pid.'
        )
    if 'exit_pid' not in inspect.signature(_cf_process._sendback_result).parameters:
        raise MemoryRecycleUnsupportedError(
            'max_memory_per_child_mb: _sendback_result no longer accepts exit_pid.'
        )

    # _CallItem still carries (work_id, fn, args, kwargs) the way the pinned
    # worker loop unpacks them.
    call_item = _cf_process._CallItem(0, len, (), {})
    if (
        call_item.work_id,
        call_item.fn,
        call_item.args,
        call_item.kwargs,
    ) != (0, len, (), {}):
        raise MemoryRecycleUnsupportedError(
            'max_memory_per_child_mb: _CallItem shape (work_id/fn/args/kwargs) '
            'changed.'
        )

    # The overridden methods are still zero-argument instance methods, so the
    # manager-side replacement contract (clean recycle -> _adjust_process_count
    # -> _spawn_process) holds.
    for method in ('_spawn_process', '_adjust_process_count'):
        params = _executor_method_params(method)
        if params != ['self']:
            raise MemoryRecycleUnsupportedError(
                f'max_memory_per_child_mb: ProcessPoolExecutor.{method} '
                f'signature changed (params={params}).'
            )

    # The private instance attrs both overrides read are present on a fresh
    # executor.
    missing_attrs = _probe_executor_attrs(
        _RECYCLE_INSTANCE_ATTRS + _SPAWN_INSTANCE_ATTRS
    )
    if missing_attrs:
        raise MemoryRecycleUnsupportedError(
            'max_memory_per_child_mb: ProcessPoolExecutor instance attrs '
            f'absent: {", ".join(sorted(missing_attrs))}.'
        )
