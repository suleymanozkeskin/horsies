# pyright: reportPrivateUsage=false
"""Tests of the client parts of a worker start.

The ``sys.path`` helpers of the child import (``child_runner``), the freeze of
the objects of a completed start (``gc_policy``, which the core worker host
runs on ``on_started``), the import of the task modules
(``Horsies.import_task_modules``, which the worker CLI runs before the core
run), the registered class of each task (``row_class``, which goes into the
worker manifest), and the send suppression of ``Horsies.check``, which imports
the task modules first.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution
from horsies.core.worker import gc_policy
from horsies.core.worker.child_runner import (
    _build_sys_path_roots,
    _dedupe_paths,
    _derive_sys_path_roots_from_file,
)
from horsies.core.worker.core_run import CoreWorkerHost
from horsies.core.worker.row_class import registered_task_classes


def _app() -> Horsies:
    """An app that never connects."""
    return Horsies(
        AppConfig(broker=PostgresConfig(database_url=SecretStr('postgresql://u:p@localhost/db'))),
    )


# ---------------------------------------------------------------------------
# 1. _dedupe_paths
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDedupePaths:
    """Tests for _dedupe_paths: order-preserving dedup, skip empties."""

    def test_preserves_order_and_deduplicates(self) -> None:
        assert _dedupe_paths(['a', 'b', 'a', 'c']) == ['a', 'b', 'c']

    def test_filters_empty_strings(self) -> None:
        assert _dedupe_paths(['a', '', 'b', '', 'c']) == ['a', 'b', 'c']

    def test_empty_input(self) -> None:
        assert _dedupe_paths([]) == []

    def test_all_empty_strings(self) -> None:
        assert _dedupe_paths(['', '', '']) == []

    def test_single_element(self) -> None:
        assert _dedupe_paths(['x']) == ['x']


# ---------------------------------------------------------------------------
# 3. _derive_sys_path_roots_from_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDeriveSysPathRootsFromFile:
    """Tests for _derive_sys_path_roots_from_file: parent dir extraction."""

    def test_normal_file_path(self) -> None:
        result = _derive_sys_path_roots_from_file('/foo/bar/baz.py')
        assert result == [os.path.realpath('/foo/bar')]

    def test_relative_file_path(self) -> None:
        result = _derive_sys_path_roots_from_file('some/module.py')
        assert len(result) == 1
        assert os.path.isabs(result[0])

    def test_root_level_file(self) -> None:
        result = _derive_sys_path_roots_from_file('/file.py')
        assert result == ['/']


# ---------------------------------------------------------------------------
# 4. _build_sys_path_roots
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBuildSysPathRoots:
    """Tests for _build_sys_path_roots: compose and dedupe roots."""

    def test_extra_roots_included_and_made_absolute(self) -> None:
        result = _build_sys_path_roots(
            app_locator='',
            imports=[],
            extra_roots=['./src'],
        )
        assert os.path.abspath('./src') in result

    def test_file_based_app_locator_adds_parent(self) -> None:
        result = _build_sys_path_roots(
            app_locator='/x/app.py:app',
            imports=[],
            extra_roots=[],
        )
        assert os.path.realpath('/x') in result

    def test_module_path_app_locator_skipped(self) -> None:
        result = _build_sys_path_roots(
            app_locator='my.mod:app',
            imports=[],
            extra_roots=[],
        )
        assert result == []

    def test_import_with_py_suffix_adds_parent(self) -> None:
        result = _build_sys_path_roots(
            app_locator='',
            imports=['/y/tasks.py'],
            extra_roots=[],
        )
        assert os.path.realpath('/y') in result

    def test_import_without_py_or_sep_skipped(self) -> None:
        result = _build_sys_path_roots(
            app_locator='',
            imports=['my.tasks'],
            extra_roots=[],
        )
        assert result == []

    def test_duplicates_removed(self) -> None:
        result = _build_sys_path_roots(
            app_locator='/x/app.py:app',
            imports=['/x/tasks.py'],
            extra_roots=[os.path.realpath('/x')],
        )
        # /x appears from extra_roots, app_locator, and import — all resolve
        # to the same realpath, so the list must contain no duplicates.
        assert len(result) == len(set(result))

    def test_empty_inputs(self) -> None:
        result = _build_sys_path_roots(
            app_locator='',
            imports=[],
            extra_roots=[],
        )
        assert result == []


# ---------------------------------------------------------------------------
# The freeze of the objects of a completed start
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStartupObjectsFreeze:
    def test_a_completed_start_freezes_the_startup_objects_once(self) -> None:
        """The freeze collects the garbage of the start first, then moves every
        object that exists to the permanent generation: each once."""
        calls = MagicMock()
        with (
            patch.object(gc_policy.gc, 'collect', calls.collect),
            patch.object(gc_policy.gc, 'freeze', calls.freeze),
        ):
            gc_policy.freeze_startup_objects()

        assert calls.mock_calls == [call.collect(), call.freeze()]

    def test_a_start_that_ends_stopped_freezes_nothing(self) -> None:
        """The core worker host freezes only in ``on_started``, which the core
        calls after a completed start; a host whose start never completes
        freezes nothing."""
        with patch('horsies.core.worker.core_run.freeze_startup_objects') as freeze:
            host = CoreWorkerHost(MagicMock(), MagicMock())
            assert freeze.call_count == 0

            host.on_started()

        freeze.assert_called_once_with()


# ---------------------------------------------------------------------------
# The import of the task modules before the core run
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPreloadModulesMain:
    """``Horsies.import_task_modules``: the worker CLI imports the task
    modules of the app with it before the core run."""

    def test_py_file_uses_import_by_path_dotted_uses_import_module(
        self,
        tmp_path: object,
    ) -> None:
        """.py paths go to import_by_path, dotted names to import_module."""
        task_file = os.path.join(str(tmp_path), 'my_tasks.py')
        with open(task_file, 'w', encoding='utf-8') as handle:
            handle.write('')
        app = _app()

        with (
            patch('horsies.core.app.import_by_path') as import_by_path,
            patch('horsies.core.app.importlib.import_module') as import_module,
        ):
            imported = app.import_task_modules([task_file, 'myapp.tasks'])

        import_by_path.assert_called_once_with(os.path.realpath(task_file))
        import_module.assert_called_once_with('myapp.tasks')
        assert imported == [os.path.realpath(task_file), 'myapp.tasks']

    def test_includes_discovered_task_modules(self) -> None:
        """With no list, the modules that the app discovered are imported."""
        app = _app()
        app._discovered_task_modules = ['auto.discovered']

        with patch('horsies.core.app.importlib.import_module') as import_module:
            imported = app.import_task_modules()

        import_module.assert_called_once_with('auto.discovered')
        assert imported == ['auto.discovered']

    def test_records_the_class_of_each_registered_task(self) -> None:
        """The class of each registered task, which the worker manifest
        carries to the core for its class check of a claimed row."""
        classes = registered_task_classes(
            [
                ('plain', None),
                ('awaited', '{"execution": "SHARED_ASYNC", "cancel_grace_ms": 5000}'),
            ],
        )

        assert classes == {
            'plain': Execution.ISOLATED,
            'awaited': Execution.SHARED_ASYNC,
        }

    def test_import_error_logged_and_reraised(self) -> None:
        """An import error propagates with its type and text: the start
        aborts at the import site."""
        app = _app()

        with patch(
            'horsies.core.app.importlib.import_module',
            side_effect=ImportError('no such module'),
        ):
            with pytest.raises(ImportError, match='no such module'):
                app.import_task_modules(['bad.module'])

    def test_suppress_sends_bracket(self) -> None:
        """``check`` imports the task modules with sends suppressed, and
        gives the setting back after: a top-level send of a task module
        sends nothing during the start."""
        app = _app()
        app._discovered_task_modules = ['myapp.tasks']
        seen: list[bool] = []

        def _import(name: str) -> SimpleNamespace:
            seen.append(app.are_sends_suppressed())
            return SimpleNamespace(__name__=name)

        assert app.are_sends_suppressed() is False
        with patch('horsies.core.app.importlib.import_module', side_effect=_import):
            _ = app._check_task_imports()

        assert seen == [True]
        assert app.are_sends_suppressed() is False

    def test_suppress_sends_exception_stops_startup(self) -> None:
        """An exception while ``check`` suppresses sends propagates: no
        module is imported without the suppression, and the start aborts."""
        app = _app()
        app._discovered_task_modules = ['myapp.tasks']

        with (
            patch.object(app, 'suppress_sends', side_effect=RuntimeError('suppress broken')),
            patch('horsies.core.app.importlib.import_module') as import_module,
        ):
            with pytest.raises(RuntimeError, match='suppress broken'):
                app.check(live=False)

        import_module.assert_not_called()
