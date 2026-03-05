"""Unit tests for CLI module (horsies/core/cli.py)."""

from __future__ import annotations

import argparse
import logging
import types
from unittest.mock import MagicMock, patch

import pytest

from horsies.core.app import Horsies
from horsies.core.cli import (
    _is_file_path,
    _parse_locator,
    _resolve_module_argument,
    discover_app,
    main,
    setup_logging,
)
from horsies.core.errors import ConfigurationError, ErrorCode


# ---------------------------------------------------------------------------
# _parse_locator
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParseLocator:
    """Tests for _parse_locator — splits 'module:attr' into parts."""

    def test_with_colon_returns_module_and_attr(self) -> None:
        assert _parse_locator('app.configs.horsies:app') == ('app.configs.horsies', 'app')

    def test_without_colon_returns_module_and_none(self) -> None:
        assert _parse_locator('app.configs.horsies') == ('app.configs.horsies', None)

    def test_multiple_colons_splits_on_last(self) -> None:
        assert _parse_locator('a:b:c') == ('a:b', 'c')

    def test_file_path_with_attr(self) -> None:
        assert _parse_locator('/path/to/file.py:my_app') == ('/path/to/file.py', 'my_app')

    def test_empty_string(self) -> None:
        assert _parse_locator('') == ('', None)

    def test_colon_only(self) -> None:
        assert _parse_locator(':') == ('', '')

    def test_trailing_colon(self) -> None:
        assert _parse_locator('module:') == ('module', '')


# ---------------------------------------------------------------------------
# _is_file_path
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestIsFilePath:
    """Tests for _is_file_path — distinguishes file paths from dotted modules."""

    def test_py_suffix_detected(self) -> None:
        assert _is_file_path('app/configs/horsies.py') is True

    def test_slash_detected(self) -> None:
        assert _is_file_path('app/configs/horsies') is True

    def test_dotted_module_is_not_file_path(self) -> None:
        assert _is_file_path('app.configs.horsies') is False

    def test_bare_name_is_not_file_path(self) -> None:
        assert _is_file_path('horsies') is False

    def test_py_suffix_alone(self) -> None:
        assert _is_file_path('module.py') is True


# ---------------------------------------------------------------------------
# _resolve_module_argument
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolveModuleArgument:
    """Tests for _resolve_module_argument — extracts module path from args."""

    @staticmethod
    def _make_ns(
        module: str | None = None,
        module_pos: str | None = None,
    ) -> argparse.Namespace:
        return argparse.Namespace(module=module, module_pos=module_pos)

    def test_module_flag_returned(self) -> None:
        assert _resolve_module_argument(self._make_ns(module='app:inst')) == 'app:inst'

    def test_positional_returned(self) -> None:
        assert _resolve_module_argument(self._make_ns(module_pos='app:inst')) == 'app:inst'

    def test_module_flag_takes_precedence(self) -> None:
        ns = self._make_ns(module='flag_mod', module_pos='pos_mod')
        assert _resolve_module_argument(ns) == 'flag_mod'

    def test_both_missing_raises_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            _resolve_module_argument(self._make_ns())
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS

    def test_empty_strings_treated_as_missing(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            _resolve_module_argument(self._make_ns(module='', module_pos=''))
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS

    def test_error_includes_help_text(self) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            _resolve_module_argument(self._make_ns())
        assert 'horsies worker' in exc_info.value.help_text


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSetupLogging:
    """Tests for setup_logging — configures logging level globally."""

    def test_sets_debug_level(self) -> None:
        setup_logging('DEBUG')
        logger = logging.getLogger('horsies')
        assert logger.level == logging.DEBUG

    def test_case_insensitive(self) -> None:
        setup_logging('warning')
        logger = logging.getLogger('horsies')
        assert logger.level == logging.WARNING

    def test_invalid_level_falls_back_to_info(self) -> None:
        setup_logging('NONEXISTENT')
        logger = logging.getLogger('horsies')
        assert logger.level == logging.INFO


# ---------------------------------------------------------------------------
# discover_app — error paths (mocked imports)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDiscoverAppErrors:
    """Tests for discover_app error branches with mocked imports."""

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    def test_module_not_found_raises(self, _mock_sys: MagicMock) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('nonexistent.module.that.does.not.exist:app')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'module not found' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    def test_attribute_not_found_raises(self, _mock_sys: MagicMock) -> None:
        # Use a real module but request a nonexistent attribute
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('os:definitely_not_an_attribute_xyz')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'no attribute' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    def test_attribute_not_horsies_instance_raises(self, _mock_sys: MagicMock) -> None:
        # os.path is not a Horsies instance
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('os:path')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'not a Horsies instance' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.importlib.import_module')
    def test_no_horsies_instance_in_module_raises(
        self,
        mock_import: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        # Module with no Horsies instances
        fake_module = types.ModuleType('fake_empty')
        fake_module.x = 42
        fake_module.y = 'hello'
        mock_import.return_value = fake_module

        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('fake_empty')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'no Horsies instance found' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.importlib.import_module')
    def test_multiple_horsies_instances_raises(
        self,
        mock_import: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        fake_module = types.ModuleType('fake_multi')
        fake_module.app1 = MagicMock(spec=Horsies)
        fake_module.app2 = MagicMock(spec=Horsies)
        mock_import.return_value = fake_module

        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('fake_multi')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'multiple Horsies instances' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.importlib.import_module')
    def test_import_error_raises(
        self,
        mock_import: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        mock_import.side_effect = ImportError('broken dependency')

        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('broken.module:app')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'failed to import' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.importlib.import_module')
    def test_module_exec_error_raises(
        self,
        mock_import: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        mock_import.side_effect = TypeError('bad module-level code')

        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('broken.module:app')
        assert exc_info.value.code == ErrorCode.MODULE_EXEC_ERROR
        assert 'error while importing' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    def test_ambiguous_dotted_py_pattern_raises(self, _mock_sys: MagicMock) -> None:
        # "app.configs.horsies.py" looks like a dotted module with .py extension
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('app.configs.horsies.py:app')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'ambiguous' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    def test_file_not_found_raises(self, _mock_sys: MagicMock) -> None:
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('/tmp/nonexistent_horsies_file.py:app')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'module file not found' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.importlib.import_module')
    def test_single_horsies_instance_auto_discovered(
        self,
        mock_import: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        fake_module = types.ModuleType('fake_single')
        mock_app = MagicMock(spec=Horsies)
        fake_module.my_app = mock_app
        fake_module.other = 42
        mock_import.return_value = fake_module

        app, var_name, module_name, sys_path_root = discover_app('fake_single')
        assert app is mock_app
        assert var_name == 'my_app'
        assert module_name == 'fake_single'


# ---------------------------------------------------------------------------
# main() — argument parser routing
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMainEntrypoint:
    """Tests for main() — CLI argument parsing and subcommand routing."""

    def test_no_subcommand_exits_with_1(self) -> None:
        with patch('sys.argv', ['horsies']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_help_flag_exits_with_0(self) -> None:
        with patch('sys.argv', ['horsies', '--help']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    def test_worker_subcommand_routes_to_worker_command(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'worker', 'my.mod:app']),
            patch('horsies.core.cli.worker_command', autospec=True) as mock_cmd,
        ):
            main()
            mock_cmd.assert_called_once()
            args = mock_cmd.call_args[0][0]
            assert args.module_pos == 'my.mod:app'

    def test_check_subcommand_routes_to_check_command(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'check', 'my.mod:app']),
            patch('horsies.core.cli.check_command', autospec=True) as mock_cmd,
        ):
            main()
            mock_cmd.assert_called_once()

    def test_scheduler_subcommand_routes_to_scheduler_command(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'scheduler', 'my.mod:app']),
            patch('horsies.core.cli.scheduler_command', autospec=True) as mock_cmd,
        ):
            main()
            mock_cmd.assert_called_once()

    def test_get_docs_subcommand_routes_to_get_docs_command(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'get-docs']),
            patch('horsies.core.cli.get_docs_command', autospec=True) as mock_cmd,
        ):
            main()
            mock_cmd.assert_called_once()

    def test_worker_loglevel_default_is_info(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'worker', 'my.mod:app']),
            patch('horsies.core.cli.worker_command', autospec=True) as mock_cmd,
        ):
            main()
            args = mock_cmd.call_args[0][0]
            assert args.loglevel == 'INFO'

    def test_worker_processes_default_is_1(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'worker', 'my.mod:app']),
            patch('horsies.core.cli.worker_command', autospec=True) as mock_cmd,
        ):
            main()
            args = mock_cmd.call_args[0][0]
            assert args.processes == 1

    def test_check_live_flag_parsed(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'check', '--live', 'my.mod:app']),
            patch('horsies.core.cli.check_command', autospec=True) as mock_cmd,
        ):
            main()
            args = mock_cmd.call_args[0][0]
            assert args.live is True

    def test_check_live_default_is_false(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'check', 'my.mod:app']),
            patch('horsies.core.cli.check_command', autospec=True) as mock_cmd,
        ):
            main()
            args = mock_cmd.call_args[0][0]
            assert args.live is False

    def test_worker_module_flag(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'worker', '-m', 'my.mod:app']),
            patch('horsies.core.cli.worker_command', autospec=True) as mock_cmd,
        ):
            main()
            args = mock_cmd.call_args[0][0]
            assert args.module == 'my.mod:app'

    def test_get_docs_output_default(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'get-docs']),
            patch('horsies.core.cli.get_docs_command', autospec=True) as mock_cmd,
        ):
            main()
            args = mock_cmd.call_args[0][0]
            assert args.output == '.horsies-docs'

    def test_get_docs_custom_output(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'get-docs', '--output', '/tmp/docs']),
            patch('horsies.core.cli.get_docs_command', autospec=True) as mock_cmd,
        ):
            main()
            args = mock_cmd.call_args[0][0]
            assert args.output == '/tmp/docs'
