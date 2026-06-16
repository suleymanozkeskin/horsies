"""Tests for max_memory_per_child_mb: executor, RSS reader, version guard,
predicate, and the startup baseline guard.

The executor tests spawn real child processes (forcing the 'spawn' start
method, as production does) and assert pid rotation; they need no database.
"""

from __future__ import annotations

import multiprocessing
import os
from multiprocessing.context import BaseContext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from concurrent.futures import ProcessPoolExecutor

from horsies.core.worker import mem_pool, runtime, worker as worker_mod
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.mem_pool import (
    HorsiesProcessPoolExecutor,
    MemoryRecycleUnsupportedError,
    _RecycleProcessPoolExecutor,
    _memory_recycle_rss,
    recycle_replacement_supported,
    verify_cpython_internals,
)
from horsies.core.worker.runtime import (
    MemoryBaselineExceedsThresholdError,
    _read_current_rss_mb,
    _warm_child_process,
    _warm_child_process_probe,
)
from horsies.core.worker.worker import Worker

# ---- module-level task fns (must be importable for the spawn start method) ----

_RETAINED: list[bytearray] = []


def _return_pid() -> int:
    return os.getpid()


def _alloc_and_return_pid(nbytes: int) -> int:
    _RETAINED.append(bytearray(nbytes))
    return os.getpid()


def _spawn_ctx() -> BaseContext:
    return multiprocessing.get_context('spawn')


def _make_config(**overrides: object) -> WorkerConfig:
    base: dict[str, object] = {
        'dsn': 'postgresql+psycopg://u:p@localhost/db',
        'psycopg_dsn': 'postgresql://u:p@localhost/db',
        'queues': ['default'],
    }
    base.update(overrides)
    return WorkerConfig(**base)  # type: ignore[arg-type]


# --------------------------- config validation -------------------------------


class TestConfigValidation:
    def test_none_is_default_and_accepted(self) -> None:
        assert _make_config().max_memory_per_child_mb is None

    def test_positive_value_accepted(self) -> None:
        assert _make_config(max_memory_per_child_mb=200).max_memory_per_child_mb == 200

    @pytest.mark.parametrize('bad', [0, -1, -200])
    def test_non_positive_rejected(self, bad: int) -> None:
        with pytest.raises(ValueError, match='positive integer'):
            _make_config(max_memory_per_child_mb=bad)


# ------------------------------- CLI type fn ---------------------------------


class TestCliType:
    def test_accepts_positive(self) -> None:
        from horsies.core.cli import _max_memory_per_child_mb

        assert _max_memory_per_child_mb('150') == 150

    @pytest.mark.parametrize('bad', ['0', '-5'])
    def test_rejects_non_positive(self, bad: str) -> None:
        import argparse

        from horsies.core.cli import _max_memory_per_child_mb

        with pytest.raises(argparse.ArgumentTypeError, match='positive integer'):
            _max_memory_per_child_mb(bad)


# ------------------------------- RSS reader ----------------------------------


class TestRssReader:
    def test_returns_positive_float_for_live_process(self) -> None:
        rss = _read_current_rss_mb()
        # psutil is a hard dependency, so the reader resolves on any platform.
        assert rss is not None
        assert rss > 0

    def test_statm_parse_matches_page_math(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Feed a known statm line; resident is field 2 (index 1).
        page = os.sysconf('SC_PAGE_SIZE')

        class _FakeStatm:
            def __enter__(self) -> _FakeStatm:
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def read(self) -> str:
                return '1000 4096 64 1 0 512 0'

        monkeypatch.setattr(
            'builtins.open',
            lambda *a, **k: _FakeStatm(),  # type: ignore[arg-type]
        )
        rss = _read_current_rss_mb()
        assert rss == pytest.approx(4096 * page / 1024 / 1024)

    def test_fail_open_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # statm read raises, and psutil is forced to fail -> None, no raise.
        def _boom(*_: object, **__: object) -> object:
            raise OSError('no /proc here')

        monkeypatch.setattr('builtins.open', _boom)
        monkeypatch.setattr(runtime, '_rss_reader_failed_logged', False)

        import psutil

        monkeypatch.setattr(
            psutil,
            'Process',
            lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError('x')),
        )
        assert _read_current_rss_mb() is None


# ------------------------------ version guard --------------------------------


class TestVersionGuard:
    def test_passes_on_current_cpython(self) -> None:
        verify_cpython_internals()  # must not raise on the test interpreter

    def test_rejects_non_cpython(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mem_pool.platform, 'python_implementation', lambda: 'PyPy')
        with pytest.raises(MemoryRecycleUnsupportedError, match='CPython'):
            verify_cpython_internals()

    def test_rejects_untested_minor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mem_pool, '_SUPPORTED_PYTHON_MINORS', frozenset({(3, 99)}))
        with pytest.raises(MemoryRecycleUnsupportedError, match='verified only'):
            verify_cpython_internals()

    def test_rejects_missing_instance_attr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            mem_pool, '_probe_executor_attrs', lambda names: {'_call_queue'}
        )
        with pytest.raises(MemoryRecycleUnsupportedError, match='instance attrs'):
            verify_cpython_internals()

    def test_rejects_executor_method_signature_drift(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _spawn_process(self: object, unexpected: object) -> None:
            return None

        monkeypatch.setattr(ProcessPoolExecutor, '_spawn_process', _spawn_process)
        with pytest.raises(MemoryRecycleUnsupportedError, match='signature changed'):
            verify_cpython_internals()


# --------------------------- recycle predicate -------------------------------


class TestMemoryRecyclePredicate:
    def test_disabled_when_threshold_none(self) -> None:
        item = SimpleNamespace(fn=_return_pid)
        assert _memory_recycle_rss(item, None) is None

    @pytest.mark.parametrize('fn', [_warm_child_process, _warm_child_process_probe])
    def test_warmup_calls_are_exempt(
        self,
        fn: object,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(mem_pool, '_read_current_rss_mb', lambda: 9999.0)
        item = SimpleNamespace(fn=fn)
        assert _memory_recycle_rss(item, 10) is None

    def test_real_call_over_threshold_returns_rss(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(mem_pool, '_read_current_rss_mb', lambda: 250.0)
        item = SimpleNamespace(fn=_return_pid)
        assert _memory_recycle_rss(item, 200) == 250.0

    def test_real_call_under_threshold_returns_none(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(mem_pool, '_read_current_rss_mb', lambda: 50.0)
        item = SimpleNamespace(fn=_return_pid)
        assert _memory_recycle_rss(item, 200) is None

    def test_unknown_rss_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mem_pool, '_read_current_rss_mb', lambda: None)
        item = SimpleNamespace(fn=_return_pid)
        assert _memory_recycle_rss(item, 200) is None


# ----------------------------- executor behavior -----------------------------


def _create_executor(**cfg_overrides: object) -> ProcessPoolExecutor:
    """Build an executor through Worker._create_executor with a stub self.

    Exercises the production selection logic (`_create_executor` only reads
    `self.cfg`). Children spawn lazily on submit, so the returned executor holds
    no child processes; callers must shut it down.
    """
    cfg = _make_config(**cfg_overrides)
    return worker_mod.Worker._create_executor(SimpleNamespace(cfg=cfg))


class TestExecutor:
    @pytest.mark.parametrize(
        'executor_cls', [_RecycleProcessPoolExecutor, HorsiesProcessPoolExecutor]
    )
    def test_gh115634_queued_recycle_does_not_hang(self, executor_cls: type) -> None:
        # Bug shape: queue all futures BEFORE any completes, so pending work
        # spans a recycle (max_tasks_per_child=2). Stock ProcessPoolExecutor
        # hangs here (gh-115634); the _adjust_process_count override replaces the
        # recycled child so every future completes and pids rotate.
        with executor_cls(1, mp_context=_spawn_ctx(), max_tasks_per_child=2) as ex:
            futures = [ex.submit(_return_pid) for _ in range(10)]
            pids = [f.result(timeout=30) for f in futures]
        assert len(pids) == 10
        assert len(set(pids)) >= 2  # children rotated across the queued burst

    def test_create_executor_count_only_uses_recycle_executor(self) -> None:
        # Product path: count recycling must route through the gh-115634 fix,
        # not the raw stdlib pool.
        ex = _create_executor(max_tasks_per_child=5, max_memory_per_child_mb=None)
        try:
            assert type(ex) is _RecycleProcessPoolExecutor
        finally:
            ex.shutdown(wait=False)

    def test_create_executor_memory_uses_memory_executor(self) -> None:
        ex = _create_executor(max_tasks_per_child=5, max_memory_per_child_mb=500)
        try:
            assert type(ex) is HorsiesProcessPoolExecutor
        finally:
            ex.shutdown(wait=False)

    def test_create_executor_no_recycle_uses_stock_pool(self) -> None:
        ex = _create_executor(max_tasks_per_child=0, max_memory_per_child_mb=None)
        try:
            assert type(ex) is ProcessPoolExecutor
        finally:
            ex.shutdown(wait=False)

    def test_create_executor_count_falls_back_to_stock_when_unsupported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # If the override surface is unavailable, count recycling degrades to the
        # stock pool rather than raising (fail-closed to current behavior).
        monkeypatch.setattr(worker_mod, 'recycle_replacement_supported', lambda: False)
        ex = _create_executor(max_tasks_per_child=5, max_memory_per_child_mb=None)
        try:
            assert type(ex) is ProcessPoolExecutor
        finally:
            ex.shutdown(wait=False)

    def test_recycle_replacement_supported_true_on_cpython(self) -> None:
        assert recycle_replacement_supported() is True

    def test_recycle_replacement_supported_rejects_signature_drift(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _spawn_process(self: object, unexpected: object) -> None:
            return None

        monkeypatch.setattr(mem_pool, '_recycle_support', None)
        monkeypatch.setattr(ProcessPoolExecutor, '_spawn_process', _spawn_process)

        assert recycle_replacement_supported() is False

    def test_memory_recycle_rotates_children(self) -> None:
        # Each task retains 120MB; with threshold 120MB the child crosses it
        # after the first real task and recycles, so pids rotate.
        with HorsiesProcessPoolExecutor(
            1,
            mp_context=_spawn_ctx(),
            max_memory_per_child_mb=120,
        ) as ex:
            pids = [
                ex.submit(_alloc_and_return_pid, 120 * 1024 * 1024).result(timeout=30)
                for _ in range(4)
            ]
        assert len(pids) == 4
        assert len(set(pids)) >= 2  # memory recycle rotated children


# ------------------------------ baseline guard -------------------------------


class TestBaselineGuard:
    @staticmethod
    def _stub(threshold: int | None) -> SimpleNamespace:
        return SimpleNamespace(cfg=SimpleNamespace(max_memory_per_child_mb=threshold))

    def test_hard_fails_when_baseline_at_or_above_threshold(self) -> None:
        with pytest.raises(MemoryBaselineExceedsThresholdError, match='does not fit'):
            Worker._check_memory_baseline(self._stub(100), {1: 100.0, 2: 80.0})

    def test_warns_within_80_percent_but_does_not_raise(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Assert on the module logger (not caplog), which other tests'
        # logging reconfiguration can detach from the root propagation chain.
        log = MagicMock()
        monkeypatch.setattr(worker_mod, 'logger', log)
        Worker._check_memory_baseline(self._stub(100), {1: 85.0})
        assert any('within 80%' in str(c.args[0]) for c in log.warning.call_args_list)

    def test_ok_when_baseline_well_below_threshold(self) -> None:
        # Must not raise.
        Worker._check_memory_baseline(self._stub(200), {1: 50.0, 2: 60.0})

    def test_skips_when_all_baselines_unknown(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log = MagicMock()
        monkeypatch.setattr(worker_mod, 'logger', log)
        Worker._check_memory_baseline(self._stub(100), {1: None, 2: None})
        assert any('unreadable' in str(c.args[0]) for c in log.warning.call_args_list)

    def test_noop_when_threshold_none(self) -> None:
        Worker._check_memory_baseline(self._stub(None), {1: 999.0})

    def test_record_keeps_highest_per_pid(self) -> None:
        acc: dict[int, float | None] = {}
        Worker._record_child_baseline(acc, 7, 50.0)
        Worker._record_child_baseline(acc, 7, 80.0)
        Worker._record_child_baseline(acc, 7, 60.0)
        Worker._record_child_baseline(acc, 9, None)
        assert acc == {7: 80.0, 9: None}
