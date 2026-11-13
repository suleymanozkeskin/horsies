"""``ParentCpu`` placement: the CPU split, the topology reader, the plan,
and the OS calls, with the OS replaced where a test must not touch it."""

from __future__ import annotations

import errno
from pathlib import Path

import pytest

from horsies.core.types.result import Err, Ok
from horsies.core.worker import cpu_placement
from horsies.core.worker.cpu_placement import (
    NotLinux,
    OwnCore,
    ParentCpu,
    PlaceRefused,
    SharedCpus,
    TooFewCores,
    TopologyUnreadable,
    describe_refusal,
    parse_cpu_list,
    place_process,
    place_worker_threads,
    plan_cpu_placement,
    read_siblings,
    split_own_core,
)

pytestmark = [pytest.mark.unit]

# A CCX43 worker set: 4 physical cores, 2 SMT threads each.
PAIRED = {cpu: frozenset({cpu - cpu % 2, cpu - cpu % 2 + 1}) for cpu in range(6, 14)}


def _topology(root: Path, siblings: dict[int, str]) -> Path:
    for cpu, text in siblings.items():
        topology = root / f'cpu{cpu}' / 'topology'
        topology.mkdir(parents=True)
        (topology / 'thread_siblings_list').write_text(text)
    return root


class TestParseCpuList:
    @pytest.mark.parametrize(
        ('text', 'expected'),
        [
            ('0-3,8-11\n', frozenset({0, 1, 2, 3, 8, 9, 10, 11})),
            ('6,7', frozenset({6, 7})),
            ('5', frozenset({5})),
        ],
    )
    def test_ranges_and_single_cpus(self, text: str, expected: frozenset[int]) -> None:
        assert parse_cpu_list(text) == Ok(expected)

    @pytest.mark.parametrize('text', ['', 'a', '3-1', '1-', '-2', '70000', '1,,2'])
    def test_text_that_is_not_a_cpu_list_is_refused(self, text: str) -> None:
        match parse_cpu_list(text):
            case Err(reason):
                assert repr(text) in reason
            case Ok(cpus):
                raise AssertionError(f'{text!r} parsed as {cpus}')


class TestSplitOwnCore:
    def test_the_worker_takes_the_core_of_the_lowest_cpu(self) -> None:
        assert split_own_core(frozenset(range(6, 14)), PAIRED) == Ok(
            OwnCore(parent=frozenset({6, 7}), children=frozenset(range(8, 14))),
        )

    def test_a_core_cut_by_the_allowed_cpus_counts_as_a_core(self) -> None:
        allowed = frozenset({7, 8, 9})

        assert split_own_core(allowed, PAIRED) == Ok(
            OwnCore(parent=frozenset({7}), children=frozenset({8, 9})),
        )

    def test_cpus_without_smt_siblings_are_one_core_each(self) -> None:
        alone = {cpu: frozenset({cpu}) for cpu in range(3)}

        assert split_own_core(frozenset(range(3)), alone) == Ok(
            OwnCore(parent=frozenset({0}), children=frozenset({1, 2})),
        )

    def test_one_physical_core_is_too_few(self) -> None:
        allowed = frozenset({6, 7})

        assert split_own_core(allowed, PAIRED) == Err(TooFewCores(allowed=allowed, cores=1))


class TestReadSiblings:
    def test_it_reads_each_allowed_cpu(self, tmp_path: Path) -> None:
        root = _topology(tmp_path, {8: '8-9\n', 9: '8,9\n'})

        assert read_siblings(frozenset({8, 9}), root) == Ok(
            {8: frozenset({8, 9}), 9: frozenset({8, 9})},
        )

    def test_a_missing_list_is_unreadable(self, tmp_path: Path) -> None:
        match read_siblings(frozenset({4}), tmp_path):
            case Err(TopologyUnreadable(cpu=4, cause=cause)):
                assert 'FileNotFoundError' in cause
            case other:
                raise AssertionError(f'expected TopologyUnreadable, got {other!r}')

    def test_a_list_that_omits_its_cpu_is_unreadable(self, tmp_path: Path) -> None:
        root = _topology(tmp_path, {8: '9,10\n'})

        match read_siblings(frozenset({8}), root):
            case Err(TopologyUnreadable(cpu=8, cause=cause)):
                assert 'omits' in cause
            case other:
                raise AssertionError(f'expected TopologyUnreadable, got {other!r}')

    def test_a_list_that_does_not_parse_is_unreadable(self, tmp_path: Path) -> None:
        root = _topology(tmp_path, {8: 'eight\n'})

        match read_siblings(frozenset({8}), root):
            case Err(TopologyUnreadable(cpu=8)):
                pass
            case other:
                raise AssertionError(f'expected TopologyUnreadable, got {other!r}')


class TestPlanCpuPlacement:
    def test_shared_places_nothing_on_any_platform(self) -> None:
        assert plan_cpu_placement(ParentCpu.SHARED) == Ok(SharedCpus())

    def test_own_core_off_linux_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cpu_placement.sys, 'platform', 'darwin')

        assert plan_cpu_placement(ParentCpu.OWN_CORE) == Err(NotLinux(platform='darwin'))

    def test_own_core_on_linux_splits_the_allowed_cpus(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        root = _topology(tmp_path, {cpu: f'{min(s)}-{max(s)}' for cpu, s in PAIRED.items()})
        monkeypatch.setattr(cpu_placement.sys, 'platform', 'linux')
        monkeypatch.setattr(cpu_placement, '_allowed_cpus', lambda: frozenset(range(6, 14)))
        monkeypatch.setattr(cpu_placement, 'CPU_TOPOLOGY_DIR', root)

        assert plan_cpu_placement(ParentCpu.OWN_CORE) == Ok(
            OwnCore(parent=frozenset({6, 7}), children=frozenset(range(8, 14))),
        )


class TestDescribeRefusal:
    @pytest.mark.parametrize(
        ('refused', 'phrase'),
        [
            (NotLinux(platform='darwin'), 'Linux only'),
            (TooFewCores(allowed=frozenset({6, 7}), cores=1), 'at least 2 physical cores'),
            (TopologyUnreadable(cpu=3, cause='gone'), 'CPU 3'),
        ],
    )
    def test_each_refusal_names_its_cause(
        self,
        refused: NotLinux | TooFewCores | TopologyUnreadable,
        phrase: str,
    ) -> None:
        assert phrase in describe_refusal(refused)


def _refuse(target: int, cpus: frozenset[int]) -> None:
    del cpus
    raise OSError(errno.EINVAL, f'no CPU of the set is allowed for {target}')


class TestPlaceProcess:
    def test_a_refused_call_names_the_process(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cpu_placement, '_set_cpus', _refuse)

        match place_process(4321, frozenset({8})):
            case Err(PlaceRefused(target=4321, cause=cause)):
                assert 'OSError' in cause
            case other:
                raise AssertionError(f'expected PlaceRefused, got {other!r}')

    def test_an_accepted_call_sets_the_cpus_of_that_pid(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[tuple[int, frozenset[int]]] = []

        def _set(target: int, cpus: frozenset[int]) -> None:
            calls.append((target, cpus))

        monkeypatch.setattr(cpu_placement, '_set_cpus', _set)

        assert place_process(4321, frozenset({8, 9})) == Ok(None)
        assert calls == [(4321, frozenset({8, 9}))]


class TestPlaceWorkerThreads:
    def test_every_listed_thread_gets_the_cpus(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        for thread_id in (100, 101, 102):
            (tmp_path / str(thread_id)).mkdir()
        calls: list[int] = []

        def _set(target: int, cpus: frozenset[int]) -> None:
            del cpus
            calls.append(target)

        monkeypatch.setattr(cpu_placement, '_set_cpus', _set)

        assert place_worker_threads(frozenset({6, 7}), tmp_path) == Ok(3)
        assert sorted(calls) == [100, 101, 102]

    def test_a_thread_that_ended_is_skipped(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        for thread_id in (100, 101):
            (tmp_path / str(thread_id)).mkdir()

        def _set(target: int, cpus: frozenset[int]) -> None:
            del cpus
            if target == 101:
                raise ProcessLookupError(errno.ESRCH, 'gone')

        monkeypatch.setattr(cpu_placement, '_set_cpus', _set)

        assert place_worker_threads(frozenset({6}), tmp_path) == Ok(1)

    def test_a_refused_thread_stops_the_placement(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        (tmp_path / '100').mkdir()
        monkeypatch.setattr(cpu_placement, '_set_cpus', _refuse)

        match place_worker_threads(frozenset({6}), tmp_path):
            case Err(PlaceRefused(target=100)):
                pass
            case other:
                raise AssertionError(f'expected PlaceRefused, got {other!r}')
