"""Tests for import utilities and worker module loading."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.utils.imports import (
    _compute_synthetic_module_name,
    compute_package_path_from_fs,
    import_file_path,
    import_module_path,
    find_project_root,
    unload_file_path,
)
from horsies.core.worker.worker import _locate_app


@pytest.mark.unit
class TestComputePackagePathFromFs:
    """Tests for compute_package_path_from_fs function."""

    def test_returns_none_for_non_package_file(self, tmp_path: Path) -> None:
        """File without __init__.py in parent should return (None, None)."""
        standalone = tmp_path / 'standalone.py'
        standalone.write_text('x = 1')

        dotted, root = compute_package_path_from_fs(str(standalone))

        assert dotted is None
        assert root is None

    def test_computes_single_level_package(self, tmp_path: Path) -> None:
        """File in a single-level package should return correct path."""
        pkg = tmp_path / 'pkg'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        module = pkg / 'module.py'
        module.write_text('x = 1')

        dotted, root = compute_package_path_from_fs(str(module))

        assert dotted == 'pkg.module'
        assert root == str(tmp_path)

    def test_computes_nested_package(self, tmp_path: Path) -> None:
        """File in nested package should return full dotted path."""
        a = tmp_path / 'a'
        b = a / 'b'
        c = b / 'c'
        c.mkdir(parents=True)

        (a / '__init__.py').write_text('')
        (b / '__init__.py').write_text('')
        (c / '__init__.py').write_text('')
        deep = c / 'deep.py'
        deep.write_text('x = 1')

        dotted, root = compute_package_path_from_fs(str(deep))

        assert dotted == 'a.b.c.deep'
        assert root == str(tmp_path)

    def test_stops_at_missing_init(self, tmp_path: Path) -> None:
        """Should stop walking up when __init__.py is missing."""
        outer = tmp_path / 'outer'
        inner = outer / 'inner'
        inner.mkdir(parents=True)

        (inner / '__init__.py').write_text('')
        mod = inner / 'mod.py'
        mod.write_text('x = 1')

        dotted, root = compute_package_path_from_fs(str(mod))

        assert dotted == 'inner.mod'
        assert root == str(outer)


@pytest.mark.unit
class TestImportFilePath:
    """Tests for import_file_path function - explicit file loading."""

    def test_imports_standalone_module(self, tmp_path: Path) -> None:
        """Standalone file gets synthetic module name."""
        standalone = tmp_path / 'standalone_test.py'
        standalone.write_text('VALUE = 42')

        # Clean up exact keys
        realpath = str(standalone.resolve())
        synthetic_name = _compute_synthetic_module_name(realpath)
        sys.modules.pop(synthetic_name, None)
        sys.modules.pop('standalone_test', None)

        mod = import_file_path(str(standalone))

        assert mod.VALUE == 42
        # Module gets synthetic name (explicit, not heuristic-based)
        assert mod.__name__.startswith('horsies._dynamic.')
        # Basename alias is also registered
        assert 'standalone_test' in sys.modules

    def test_adds_parent_to_sys_path(self, tmp_path: Path) -> None:
        """Parent directory should be added to sys.path."""
        mod_file = tmp_path / 'target.py'
        mod_file.write_text('X = 1')

        original_path = sys.path.copy()

        try:
            import_file_path(str(mod_file))
            assert str(tmp_path) in sys.path
        finally:
            sys.path[:] = original_path

    def test_with_explicit_module_name(self, tmp_path: Path) -> None:
        """Can specify explicit module name."""
        mod_file = tmp_path / 'mymodule.py'
        mod_file.write_text('VALUE = 123')

        # Clean up
        sys.modules.pop('custom_name', None)

        mod = import_file_path(str(mod_file), module_name='custom_name')

        assert mod.VALUE == 123
        assert mod.__name__ == 'custom_name'
        assert 'custom_name' in sys.modules

    def test_returns_cached_module(self, tmp_path: Path) -> None:
        """Second import of same file returns cached module."""
        mod_file = tmp_path / 'cached_mod.py'
        mod_file.write_text('VALUE = 999')

        # Clean up exact keys
        realpath = str(mod_file.resolve())
        synthetic_name = _compute_synthetic_module_name(realpath)
        sys.modules.pop(synthetic_name, None)
        sys.modules.pop('cached_mod', None)

        mod1 = import_file_path(str(mod_file))
        mod2 = import_file_path(str(mod_file))

        assert mod1 is mod2

    def test_raises_file_not_found_for_missing_file(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError with the resolved file path in the message."""
        missing = tmp_path / 'does_not_exist.py'

        with pytest.raises(FileNotFoundError, match='Module file not found') as exc_info:
            import_file_path(str(missing))

        # Error message should include the resolved path for debuggability
        assert str(missing.resolve()) in str(exc_info.value)

    def test_skips_adding_parent_to_sys_path(self, tmp_path: Path) -> None:
        """Parent directory should NOT be added when add_parent_to_path=False."""
        mod_file = tmp_path / 'no_path_mod.py'
        mod_file.write_text('X = 1')

        # Clean up
        sys.modules.pop('no_path_mod', None)
        realpath = str(mod_file.resolve())
        synthetic_name = _compute_synthetic_module_name(realpath)
        sys.modules.pop(synthetic_name, None)

        original_path = sys.path.copy()

        try:
            import_file_path(str(mod_file), add_parent_to_path=False)
            # Parent should not have been added
            parent = str(mod_file.resolve().parent)
            assert parent not in [p for p in sys.path if p not in original_path]
        finally:
            sys.path[:] = original_path

    def test_unload_file_path_removes_registered_module_keys(self, tmp_path: Path) -> None:
        """unload_file_path should remove synthetic and basename aliases."""
        mod_file = tmp_path / 'to_unload.py'
        mod_file.write_text('VALUE = 7')

        realpath = str(mod_file.resolve())
        synthetic_name = _compute_synthetic_module_name(realpath)
        sys.modules.pop(synthetic_name, None)
        sys.modules.pop('to_unload', None)

        import_file_path(str(mod_file))
        removed = unload_file_path(str(mod_file))

        assert synthetic_name in removed
        assert 'to_unload' in removed
        assert synthetic_name not in sys.modules
        assert 'to_unload' not in sys.modules


@pytest.mark.unit
class TestImportModulePath:
    """Tests for import_module_path function - standard Python import."""

    def test_imports_existing_module(self) -> None:
        """Should import existing Python modules."""
        mod = import_module_path('tests.e2e.tasks.basic')

        assert mod.__name__ == 'tests.e2e.tasks.basic'

    def test_raises_for_nonexistent_module(self) -> None:
        """Should raise ModuleNotFoundError for missing modules."""
        with pytest.raises(ModuleNotFoundError):
            import_module_path('nonexistent.module.path')


@pytest.mark.unit
class TestFindProjectRoot:
    """Tests for find_project_root function.

    NOTE: find_project_root does NOT traverse up directories.
    It only checks if the given directory itself contains marker files.
    This is intentional to avoid monorepo collisions.
    """

    def test_finds_pyproject_toml_in_given_dir(self, tmp_path: Path) -> None:
        """Should return dir if it contains pyproject.toml."""
        (tmp_path / 'pyproject.toml').write_text('[project]')

        result = find_project_root(str(tmp_path))

        assert result == str(tmp_path)

    def test_finds_setup_cfg_in_given_dir(self, tmp_path: Path) -> None:
        """Should return dir if it contains setup.cfg."""
        (tmp_path / 'setup.cfg').write_text('[metadata]')

        result = find_project_root(str(tmp_path))

        assert result == str(tmp_path)

    def test_finds_setup_py_in_given_dir(self, tmp_path: Path) -> None:
        """Should return dir if it contains setup.py."""
        (tmp_path / 'setup.py').write_text('')

        result = find_project_root(str(tmp_path))

        assert result == str(tmp_path)

    def test_does_not_traverse_up(self, tmp_path: Path) -> None:
        """Should NOT traverse up to find pyproject.toml (monorepo safety)."""
        (tmp_path / 'pyproject.toml').write_text('[project]')
        subdir = tmp_path / 'src' / 'pkg'
        subdir.mkdir(parents=True)

        # Looking from subdir should NOT find parent's pyproject.toml
        result = find_project_root(str(subdir))

        assert result is None

    def test_returns_none_when_not_found(self, tmp_path: Path) -> None:
        """Should return None if no marker files in given directory."""
        subdir = tmp_path / 'isolated'
        subdir.mkdir()

        result = find_project_root(str(subdir))

        assert result is None


@pytest.mark.unit
class TestLocateApp:
    """Tests for _locate_app function with module paths."""

    def test_locates_app_via_module_path(self) -> None:
        """Should locate app when given module path format."""
        app = _locate_app('tests.e2e.tasks.instance:app')

        assert app is not None
        assert len(app.tasks) > 0

    def test_locates_app_via_file_path(self) -> None:
        """Should locate app when given file path format."""
        project_root = Path(__file__).parent.parent.parent
        instance_path = project_root / 'tests' / 'e2e' / 'tasks' / 'instance.py'

        app = _locate_app(f'{instance_path}:app')

        assert app is not None

    def test_raises_for_missing_colon_separator(self) -> None:
        """Should raise ConfigurationError with WORKER_INVALID_LOCATOR code."""
        locator = 'tests.e2e.tasks.instance'

        with pytest.raises(ConfigurationError, match='invalid app locator format') as exc_info:
            _locate_app(locator)

        exc = exc_info.value
        assert exc.code == ErrorCode.WORKER_INVALID_LOCATOR
        assert any(locator in note for note in exc.notes)
        assert exc.help_text is not None

    def test_raises_for_empty_locator(self) -> None:
        """Should raise ConfigurationError with WORKER_INVALID_LOCATOR code."""
        with pytest.raises(ConfigurationError, match='invalid app locator format') as exc_info:
            _locate_app('')

        exc = exc_info.value
        assert exc.code == ErrorCode.WORKER_INVALID_LOCATOR
        assert any("''" in note for note in exc.notes)

    def test_raises_for_non_horsies_object(self) -> None:
        """Should raise ConfigurationError with resolved type name in notes."""
        locator = 'tests.e2e.tasks.basic:UserInput'

        with pytest.raises(
            ConfigurationError,
            match='app locator did not resolve to Horsies instance',
        ) as exc_info:
            _locate_app(locator)

        exc = exc_info.value
        assert exc.code == ErrorCode.WORKER_INVALID_LOCATOR
        # Notes should include the locator and the resolved type
        assert any(locator in note for note in exc.notes)
        assert any('ModelMetaclass' in note for note in exc.notes)


@pytest.mark.unit
class TestComputeSyntheticModuleName:
    """Tests for _compute_synthetic_module_name determinism and uniqueness."""

    def test_same_path_produces_same_name(self, tmp_path: Path) -> None:
        """Same file path should always produce the same synthetic name."""
        path = str(tmp_path / 'module.py')

        name1 = _compute_synthetic_module_name(path)
        name2 = _compute_synthetic_module_name(path)

        assert name1 == name2

    def test_different_paths_produce_different_names(self, tmp_path: Path) -> None:
        """Different file paths should produce different synthetic names."""
        path_a = str(tmp_path / 'module_a.py')
        path_b = str(tmp_path / 'module_b.py')

        name_a = _compute_synthetic_module_name(path_a)
        name_b = _compute_synthetic_module_name(path_b)

        assert name_a != name_b

    def test_namespace_prefix(self, tmp_path: Path) -> None:
        """Synthetic names should be under horsies._dynamic namespace."""
        path = str(tmp_path / 'anything.py')

        name = _compute_synthetic_module_name(path)

        assert name.startswith('horsies._dynamic.')


@pytest.mark.unit
class TestModuleIdentity:
    """Tests for correct __module__ attribute on task modules."""

    def test_task_module_has_correct_module_attr(self) -> None:
        """Task modules should have full package path as __name__."""
        from tests.e2e.tasks import basic

        assert basic.__name__ == 'tests.e2e.tasks.basic'

    def test_pydantic_model_has_correct_module_attr(self) -> None:
        """Pydantic models in task modules should have correct __module__."""
        from tests.e2e.tasks.basic import UserInput

        assert UserInput.__module__ == 'tests.e2e.tasks.basic'

    def test_standard_import_preserves_module_identity(self) -> None:
        """Standard import_module preserves __module__ correctly."""
        from importlib import import_module

        basic = import_module('tests.e2e.tasks.basic')

        assert basic.__name__ == 'tests.e2e.tasks.basic'
        assert basic.UserInput.__module__ == 'tests.e2e.tasks.basic'
