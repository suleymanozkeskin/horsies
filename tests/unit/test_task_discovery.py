"""Tests for task discovery: discover_tasks(), expand_module_globs(), get_discovered_task_modules()."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

from horsies.core.app import Horsies
from horsies.core.cli import discover_app
from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig


def _make_app() -> Horsies:
    return Horsies(
        config=AppConfig(
            broker=PostgresConfig(database_url="postgresql+psycopg://test:test@localhost/test"),
        )
    )


@pytest.mark.unit
class TestDiscoverTasks:
    """Tests for discover_tasks() registration logic."""

    def test_records_explicit_modules(self) -> None:
        """Dotted module paths are stored and retrievable."""
        app = _make_app()

        app.discover_tasks(['myapp.tasks', 'myapp.jobs.worker_tasks'])

        discovered = app.get_discovered_task_modules()
        assert discovered == ['myapp.tasks', 'myapp.jobs.worker_tasks']

    def test_replaces_on_second_call(self) -> None:
        """Second call replaces previous modules, not appends."""
        app = _make_app()

        app.discover_tasks(['first.module'])
        app.discover_tasks(['second.module', 'third.module'])

        discovered = app.get_discovered_task_modules()
        assert discovered == ['second.module', 'third.module']

    def test_warns_on_replacement(self) -> None:
        """Warning logged with old/new counts when re-calling discover_tasks()."""
        app = _make_app()
        app.discover_tasks(['a.tasks', 'b.tasks'])

        with mock.patch.object(app.logger, 'warning') as mock_warn:
            app.discover_tasks(['c.tasks'])

        mock_warn.assert_called_once()
        message = mock_warn.call_args[0][0]
        assert 'replacing 2' in message
        assert '1 new' in message

    def test_empty_module_list_accepted(self) -> None:
        """Empty list is valid, stored as empty, no info log emitted."""
        app = _make_app()

        with mock.patch.object(app.logger, 'info') as mock_info:
            app.discover_tasks([])

        assert app.get_discovered_task_modules() == []
        mock_info.assert_not_called()

    def test_logs_info_on_registration(self) -> None:
        """Info log fires with module count on non-empty registration."""
        app = _make_app()

        with mock.patch.object(app.logger, 'info') as mock_info:
            app.discover_tasks(['a.tasks', 'b.tasks'])

        mock_info.assert_called_once()
        assert '2 task module(s)' in mock_info.call_args[0][0]

    def test_suppresses_log_in_child_process(self) -> None:
        """HORSIES_CHILD_PROCESS=1 suppresses the info log."""
        app = _make_app()

        with (
            mock.patch.dict(os.environ, {'HORSIES_CHILD_PROCESS': '1'}, clear=False),
            mock.patch.object(app.logger, 'info') as mock_info,
        ):
            app.discover_tasks(['a.tasks'])

        mock_info.assert_not_called()

    def test_child_process_log_override(self) -> None:
        """HORSIES_CHILD_DISCOVERY_LOGS=1 overrides child process suppression."""
        app = _make_app()

        with (
            mock.patch.dict(
                os.environ,
                {'HORSIES_CHILD_PROCESS': '1', 'HORSIES_CHILD_DISCOVERY_LOGS': '1'},
                clear=False,
            ),
            mock.patch.object(app.logger, 'info') as mock_info,
        ):
            app.discover_tasks(['a.tasks'])

        mock_info.assert_called_once()
        assert '1 task module(s)' in mock_info.call_args[0][0]

    def test_duplicate_modules_stored_as_given(self) -> None:
        """Duplicate entries are stored as given (list(modules) behavior)."""
        app = _make_app()

        app.discover_tasks(['a.tasks', 'a.tasks', 'b.tasks'])

        discovered = app.get_discovered_task_modules()
        assert discovered == ['a.tasks', 'a.tasks', 'b.tasks']

    def test_first_call_no_warning(self) -> None:
        """First call does NOT emit a warning log."""
        app = _make_app()

        with mock.patch.object(app.logger, 'warning') as mock_warn:
            app.discover_tasks(['a.tasks'])

        mock_warn.assert_not_called()


@pytest.mark.unit
class TestExpandModuleGlobs:
    """Tests for expand_module_globs() pattern expansion."""

    def test_default_excludes_test_files(self, tmp_path: Path) -> None:
        """test_*.py, *_test.py, conftest.py excluded by default."""
        app = _make_app()

        (tmp_path / "tasks_a.py").write_text("# a")
        (tmp_path / "tasks_b.py").write_text("# b")
        (tmp_path / "test_tasks.py").write_text("# should be excluded")
        (tmp_path / "conftest.py").write_text("# should be excluded")
        (tmp_path / "notes.txt").write_text("ignore")

        glob_pattern = os.path.join(str(tmp_path), "*.py")
        paths = app.expand_module_globs([glob_pattern])

        assert os.path.realpath(tmp_path / "tasks_a.py") in paths
        assert os.path.realpath(tmp_path / "tasks_b.py") in paths
        assert os.path.realpath(tmp_path / "test_tasks.py") not in paths
        assert os.path.realpath(tmp_path / "conftest.py") not in paths

    def test_custom_exclude_overrides_defaults(self, tmp_path: Path) -> None:
        """Custom exclude patterns replace the defaults entirely."""
        app = _make_app()

        (tmp_path / "tasks_a.py").write_text("# a")
        (tmp_path / "tasks_b.py").write_text("# b")
        (tmp_path / "test_tasks.py").write_text("# should be included now")

        glob_pattern = os.path.join(str(tmp_path), "*.py")
        paths = app.expand_module_globs([glob_pattern], exclude=["*_b.py"])

        assert os.path.realpath(tmp_path / "tasks_a.py") in paths
        assert os.path.realpath(tmp_path / "tasks_b.py") not in paths
        assert os.path.realpath(tmp_path / "test_tasks.py") in paths

    def test_passthrough_dotted_paths(self) -> None:
        """Non-glob, non-file patterns pass through unchanged."""
        app = _make_app()

        paths = app.expand_module_globs(['myapp.tasks', 'myapp.jobs'])

        assert paths == ['myapp.tasks', 'myapp.jobs']

    def test_empty_patterns_returns_empty(self) -> None:
        """expand_module_globs([]) returns an empty list."""
        app = _make_app()

        paths = app.expand_module_globs([])

        assert paths == []

    def test_skips_directories(self, tmp_path: Path) -> None:
        """Directories matching the glob pattern are excluded."""
        app = _make_app()

        dir_named_py = tmp_path / "something.py"
        dir_named_py.mkdir()
        (tmp_path / "real_module.py").write_text("# module")

        glob_pattern = os.path.join(str(tmp_path), "*.py")
        paths = app.expand_module_globs([glob_pattern])

        assert len(paths) == 1
        assert os.path.realpath(tmp_path / "real_module.py") in paths

    def test_skips_non_py_files(self, tmp_path: Path) -> None:
        """.txt and .pyc files are excluded by the .py suffix check."""
        app = _make_app()

        (tmp_path / "tasks.py").write_text("# valid")
        (tmp_path / "tasks.txt").write_text("not python")
        (tmp_path / "tasks.pyc").write_bytes(b"\x00")

        glob_pattern = os.path.join(str(tmp_path), "tasks.*")
        paths = app.expand_module_globs([glob_pattern])

        assert len(paths) == 1
        assert os.path.realpath(tmp_path / "tasks.py") in paths

    def test_skips_nonexistent_direct_path(self) -> None:
        """Missing .py file path is silently skipped."""
        app = _make_app()

        paths = app.expand_module_globs(['/nonexistent/path/tasks.py'])

        assert paths == []

    def test_deduplicates_across_patterns(self, tmp_path: Path) -> None:
        """Same file matched by two globs appears only once."""
        app = _make_app()

        (tmp_path / "shared_tasks.py").write_text("# tasks")

        pattern_star = os.path.join(str(tmp_path), "*.py")
        pattern_shared = os.path.join(str(tmp_path), "shared_*.py")
        paths = app.expand_module_globs([pattern_star, pattern_shared])

        assert paths.count(os.path.realpath(tmp_path / "shared_tasks.py")) == 1

    def test_direct_file_path_resolved(self, tmp_path: Path) -> None:
        """Non-glob .py path is resolved to absolute path."""
        app = _make_app()

        target = tmp_path / "worker.py"
        target.write_text("# worker")

        paths = app.expand_module_globs([str(target)])

        assert len(paths) == 1
        assert paths[0] == os.path.realpath(target)

    def test_exclude_empty_list_is_falsy(self, tmp_path: Path) -> None:
        """exclude=[] is falsy, so defaults are used (documents `or` behavior)."""
        app = _make_app()

        (tmp_path / "real.py").write_text("# keep")
        (tmp_path / "test_real.py").write_text("# excluded by default")

        glob_pattern = os.path.join(str(tmp_path), "*.py")
        paths = app.expand_module_globs([glob_pattern], exclude=[])

        assert os.path.realpath(tmp_path / "real.py") in paths
        assert os.path.realpath(tmp_path / "test_real.py") not in paths

    def test_mixed_pattern_types(self, tmp_path: Path) -> None:
        """Glob + dotted + direct path in one call all resolve correctly."""
        app = _make_app()

        (tmp_path / "glob_match.py").write_text("# glob")
        direct_file = tmp_path / "direct.py"
        direct_file.write_text("# direct")

        glob_pattern = os.path.join(str(tmp_path), "glob_*.py")
        paths = app.expand_module_globs([glob_pattern, 'myapp.tasks', str(direct_file)])

        assert os.path.realpath(tmp_path / "glob_match.py") in paths
        assert 'myapp.tasks' in paths
        assert os.path.realpath(direct_file) in paths
        assert len(paths) == 3

    def test_path_with_separator_no_py_extension(self, tmp_path: Path) -> None:
        """Path containing os.sep but no .py extension is silently skipped."""
        app = _make_app()

        # Create a real file without .py extension
        non_py = tmp_path / "module"
        non_py.write_text("# not a py file")

        paths = app.expand_module_globs([str(non_py)])

        assert paths == []

    def test_recursive_glob_pattern(self, tmp_path: Path) -> None:
        """**/*.py matches files in subdirectories."""
        app = _make_app()

        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "deep_tasks.py").write_text("# deep")
        (tmp_path / "top_tasks.py").write_text("# top")

        glob_pattern = os.path.join(str(tmp_path), "**", "*.py")
        paths = app.expand_module_globs([glob_pattern])

        assert os.path.realpath(sub / "deep_tasks.py") in paths
        assert os.path.realpath(tmp_path / "top_tasks.py") in paths

    def test_exclude_applied_to_direct_path(self, tmp_path: Path) -> None:
        """Direct .py path matching default excludes is excluded."""
        app = _make_app()

        test_file = tmp_path / "test_something.py"
        test_file.write_text("# test file")

        paths = app.expand_module_globs([str(test_file)])

        assert paths == []


@pytest.mark.unit
class TestGetDiscoveredTaskModules:
    """Tests for get_discovered_task_modules() return behavior."""

    def test_copy_safety(self) -> None:
        """Mutating the returned list does not affect internal state."""
        app = _make_app()
        app.discover_tasks(['a.tasks', 'b.tasks'])

        returned = app.get_discovered_task_modules()
        returned.append('injected.module')
        returned.clear()

        assert app.get_discovered_task_modules() == ['a.tasks', 'b.tasks']

    def test_empty_before_discovery(self) -> None:
        """Returns empty list before any discover_tasks() call."""
        app = _make_app()

        assert app.get_discovered_task_modules() == []


@pytest.mark.unit
class TestExpandAndDiscoverIntegration:
    """Integration test for expand → discover → get pipeline."""

    def test_expand_then_discover_pipeline(self, tmp_path: Path) -> None:
        """Full expand → discover → get flow works end-to-end."""
        app = _make_app()

        (tmp_path / "my_tasks.py").write_text("# tasks")
        (tmp_path / "other_tasks.py").write_text("# more tasks")

        glob_pattern = os.path.join(str(tmp_path), "*_tasks.py")
        paths = app.expand_module_globs([glob_pattern])
        app.discover_tasks(paths)

        discovered = app.get_discovered_task_modules()
        assert os.path.realpath(tmp_path / "my_tasks.py") in discovered
        assert os.path.realpath(tmp_path / "other_tasks.py") in discovered


@pytest.mark.unit
class TestCheckTaskImportsErrorClassification:
    """Tests for _check_task_imports() error code classification.

    Verifies that import resolution failures (E206) are distinguished
    from module execution errors (E210).
    """

    def test_missing_dotted_module_returns_e206(self) -> None:
        """Non-existent dotted module path → E206 (CLI_INVALID_ARGS)."""
        app = _make_app()
        app.discover_tasks(['nonexistent.module.path.xyzzy'])

        errors = app.check()

        assert len(errors) == 1
        assert errors[0].code == ErrorCode.CLI_INVALID_ARGS
        assert 'failed to import module' in errors[0].message

    def test_missing_file_path_returns_e206(self, tmp_path: Path) -> None:
        """Non-existent file path → E206 (CLI_INVALID_ARGS)."""
        app = _make_app()
        missing = str(tmp_path / 'does_not_exist.py')
        app.discover_tasks([missing])

        errors = app.check()

        assert len(errors) == 1
        assert errors[0].code == ErrorCode.CLI_INVALID_ARGS
        assert 'task module not found' in errors[0].message

    def test_type_error_in_module_returns_e210(self, tmp_path: Path) -> None:
        """Module that raises TypeError during import → E210 (MODULE_EXEC_ERROR)."""
        bad_module = tmp_path / 'bad_type.py'
        bad_module.write_text('raise TypeError("missing required argument")')

        app = _make_app()
        app.discover_tasks([str(bad_module)])

        errors = app.check()

        assert len(errors) == 1
        assert errors[0].code == ErrorCode.MODULE_EXEC_ERROR
        assert 'error while importing module' in errors[0].message
        assert 'TypeError' in errors[0].notes[0]
        assert 'missing required argument' in errors[0].notes[0]

    def test_attribute_error_in_module_returns_e210(self, tmp_path: Path) -> None:
        """Module that raises AttributeError during import → E210 (MODULE_EXEC_ERROR)."""
        bad_module = tmp_path / 'bad_attr.py'
        bad_module.write_text('None.nonexistent_method()')

        app = _make_app()
        app.discover_tasks([str(bad_module)])

        errors = app.check()

        assert len(errors) == 1
        assert errors[0].code == ErrorCode.MODULE_EXEC_ERROR
        assert 'AttributeError' in errors[0].notes[0]

    def test_import_error_in_module_returns_e206(self, tmp_path: Path) -> None:
        """Module that raises ImportError (missing dependency) → E206 (CLI_INVALID_ARGS)."""
        bad_module = tmp_path / 'bad_import.py'
        bad_module.write_text('from nonexistent_package_xyzzy import something')

        app = _make_app()
        app.discover_tasks([str(bad_module)])

        errors = app.check()

        assert len(errors) == 1
        assert errors[0].code == ErrorCode.CLI_INVALID_ARGS
        assert 'failed to import module' in errors[0].message

    def test_syntax_error_in_module_returns_e210(self, tmp_path: Path) -> None:
        """Module with syntax error → E210 (MODULE_EXEC_ERROR)."""
        bad_module = tmp_path / 'bad_syntax.py'
        bad_module.write_text('def broken(\n')

        app = _make_app()
        app.discover_tasks([str(bad_module)])

        errors = app.check()

        assert len(errors) == 1
        assert errors[0].code == ErrorCode.MODULE_EXEC_ERROR
        assert 'SyntaxError' in errors[0].notes[0]

    def test_help_text_differs_between_e206_and_e210(self, tmp_path: Path) -> None:
        """E206 help text points at importability; E210 at module-level code bugs."""
        exec_module = tmp_path / 'exec_err.py'
        exec_module.write_text('raise RuntimeError("boom")')

        app = _make_app()
        app.discover_tasks(['nonexistent.module.xyzzy', str(exec_module)])

        errors = app.check()

        # check() early-returns on first phase errors, so both should appear
        assert len(errors) == 2
        e206 = [e for e in errors if e.code == ErrorCode.CLI_INVALID_ARGS]
        e210 = [e for e in errors if e.code == ErrorCode.MODULE_EXEC_ERROR]
        assert len(e206) == 1
        assert len(e210) == 1
        assert 'importable' in (e206[0].help_text or '')
        assert 'module-level code' in (e210[0].help_text or '')


@pytest.mark.unit
class TestDiscoverAppErrorClassification:
    """Tests for discover_app() error code classification.

    Verifies that file-path and dotted-path branches both
    correctly classify import errors vs execution errors.
    """

    def test_file_path_type_error_returns_e210(self, tmp_path: Path) -> None:
        """File that raises TypeError during import → E210 via discover_app()."""
        bad_file = tmp_path / 'bad_app.py'
        bad_file.write_text('raise TypeError("bad call")')

        with pytest.raises(ConfigurationError) as exc_info:
            discover_app(f'{bad_file}:app')

        assert exc_info.value.code == ErrorCode.MODULE_EXEC_ERROR
        assert 'TypeError' in exc_info.value.notes[0]

    def test_file_path_import_error_returns_e206(self, tmp_path: Path) -> None:
        """File that raises ImportError → E206 via discover_app()."""
        bad_file = tmp_path / 'bad_deps.py'
        bad_file.write_text('from nonexistent_package_xyzzy import foo')

        with pytest.raises(ConfigurationError) as exc_info:
            discover_app(f'{bad_file}:app')

        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS

    def test_dotted_path_type_error_returns_e210(self, tmp_path: Path) -> None:
        """Dotted module that raises TypeError → E210 via discover_app()."""
        # Create a module that raises TypeError, importable via sys.path
        pkg = tmp_path / 'test_pkg_e210'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        (pkg / 'crasher.py').write_text('raise TypeError("bad decorator arg")')

        original_path = sys.path.copy()
        sys.modules.pop('test_pkg_e210.crasher', None)
        sys.modules.pop('test_pkg_e210', None)
        try:
            sys.path.insert(0, str(tmp_path))
            with pytest.raises(ConfigurationError) as exc_info:
                discover_app('test_pkg_e210.crasher:app')

            assert exc_info.value.code == ErrorCode.MODULE_EXEC_ERROR
            assert 'TypeError' in exc_info.value.notes[0]
        finally:
            sys.path[:] = original_path
            sys.modules.pop('test_pkg_e210.crasher', None)
            sys.modules.pop('test_pkg_e210', None)

    def test_dotted_path_import_error_returns_e206(self, tmp_path: Path) -> None:
        """Dotted module that raises ImportError (not ModuleNotFoundError) → E206."""
        pkg = tmp_path / 'test_pkg_e206'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        (pkg / 'bad_dep.py').write_text('from nonexistent_package_xyzzy import bar')

        original_path = sys.path.copy()
        sys.modules.pop('test_pkg_e206.bad_dep', None)
        sys.modules.pop('test_pkg_e206', None)
        try:
            sys.path.insert(0, str(tmp_path))
            with pytest.raises(ConfigurationError) as exc_info:
                discover_app('test_pkg_e206.bad_dep:app')

            # ModuleNotFoundError (subclass of ImportError) → E206
            assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        finally:
            sys.path[:] = original_path
            sys.modules.pop('test_pkg_e206.bad_dep', None)
            sys.modules.pop('test_pkg_e206', None)
