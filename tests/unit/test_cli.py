"""Unit tests for CLI module (horsies/core/cli.py)."""

from __future__ import annotations

import argparse
import logging
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.app import Horsies
from horsies.core.cli import (
    _ensure_schema_with_retry,
    _is_file_path,
    _parse_locator,
    _resolve_module_argument,
    check_command,
    discover_app,
    get_docs_command,
    main,
    scheduler_command,
    setup_logging,
    worker_command,
)
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    WorkflowValidationError,
)
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.types.result import Err, Ok


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
    """Tests for setup_logging — delegates to configure_logging."""

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
    @patch('horsies.core.cli.importlib.import_module')
    def test_horsies_error_during_import_is_preserved(
        self,
        mock_import: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        mock_import.side_effect = WorkflowValidationError(
            message='duplicate definition_key',
            code=ErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY,
        )

        with pytest.raises(WorkflowValidationError) as exc_info:
            discover_app('broken.module:app')
        assert exc_info.value.code == ErrorCode.WORKFLOW_DUPLICATE_DEFINITION_KEY

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


# ---------------------------------------------------------------------------
# Helpers for heavy-mocking command tests
# ---------------------------------------------------------------------------


def _make_schema_err(
    *,
    retryable: bool = True,
    message: str = 'db error',
    exception: Exception | None = None,
) -> Any:
    """Build a simple object with retryable/message/exception attrs."""
    obj = types.SimpleNamespace(
        retryable=retryable,
        message=message,
        exception=exception,
    )
    return obj


def _make_worker_namespace(
    *,
    module: str = 'my.mod:app',
    loglevel: str = 'INFO',
    processes: int = 1,
    max_claim_batch: int = 2,
    max_claim_per_worker: int = 0,
) -> argparse.Namespace:
    """Build a valid argparse.Namespace for worker_command."""
    return argparse.Namespace(
        module=module,
        module_pos=None,
        loglevel=loglevel,
        processes=processes,
        max_claim_batch=max_claim_batch,
        max_claim_per_worker=max_claim_per_worker,
    )


def _make_scheduler_namespace(
    *,
    module: str = 'my.mod:app',
    loglevel: str = 'INFO',
) -> argparse.Namespace:
    """Build a valid argparse.Namespace for scheduler_command."""
    return argparse.Namespace(
        module=module,
        module_pos=None,
        loglevel=loglevel,
    )


def _make_check_namespace(
    *,
    module: str = 'my.mod:app',
    loglevel: str = 'WARNING',
    live: bool = False,
) -> argparse.Namespace:
    """Build a valid argparse.Namespace for check_command."""
    return argparse.Namespace(
        module=module,
        module_pos=None,
        loglevel=loglevel,
        live=live,
    )


def _make_mock_app() -> MagicMock:
    """Build a mock Horsies app with all attributes worker/scheduler/check need."""
    app = MagicMock(spec=Horsies)
    app.set_role = MagicMock()
    app.check = MagicMock(return_value=[])
    app.get_valid_queue_names = MagicMock(return_value=['default'])
    app.get_discovered_task_modules = MagicMock(return_value=['my.tasks'])
    app.import_task_modules = MagicMock()
    app.list_tasks = MagicMock(return_value=[])

    # Config sub-objects
    queue_mode = MagicMock()
    queue_mode.name = 'DEFAULT'

    schedule_config = MagicMock()
    schedule_config.enabled = True
    schedule_config.schedules = []

    resilience = WorkerResilienceConfig()

    app.config = MagicMock()
    app.config.queue_mode = queue_mode
    app.config.custom_queues = None
    app.config.schedule = schedule_config
    app.config.resilience = resilience
    app.config.cluster_wide_cap = 0
    app.config.prefetch_buffer = 2
    app.config.claim_lease_ms = 30000
    app.config.max_claim_renew_age_ms = 120000
    app.config.recovery = None
    return app


def _closing_run_side_effect(
    exc: BaseException | type[BaseException],
) -> Any:
    """Side effect for asyncio.run mock that closes the coroutine before raising.

    Prevents 'coroutine was never awaited' RuntimeWarning.
    """
    def _effect(coro: Any, *args: Any, **kwargs: Any) -> None:
        coro.close()
        if isinstance(exc, type):
            raise exc()
        raise exc
    return _effect


def _make_mock_broker() -> MagicMock:
    """Build a mock broker with all worker-needed attributes."""
    broker = MagicMock()
    broker.config = MagicMock()
    broker.config.database_url = 'postgresql://localhost/test'
    broker.session_factory = MagicMock()
    broker.listener = MagicMock()
    broker.close_async = AsyncMock(return_value=Ok(None))
    broker.ensure_schema_initialized = AsyncMock(return_value=Ok(None))
    return broker


# ---------------------------------------------------------------------------
# _ensure_schema_with_retry
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestEnsureSchemaWithRetry:
    """Tests for _ensure_schema_with_retry — async retry loop for DB init."""

    async def test_ok_on_first_attempt_returns(self) -> None:
        # Arrange
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(None))
        resilience = WorkerResilienceConfig()
        logger = logging.getLogger('test')

        # Act — should return without error
        await _ensure_schema_with_retry(broker, resilience, logger)

        # Assert
        broker.ensure_schema_initialized.assert_awaited_once()

    async def test_non_retryable_error_raises_immediately(self) -> None:
        # Arrange
        err_obj = _make_schema_err(retryable=False, message='fatal error')
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(return_value=Err(err_obj))
        resilience = WorkerResilienceConfig()
        logger = logging.getLogger('test')

        # Act / Assert
        with pytest.raises(RuntimeError, match='fatal error'):
            await _ensure_schema_with_retry(broker, resilience, logger)

        broker.ensure_schema_initialized.assert_awaited_once()

    @patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock)
    @patch('horsies.core.cli.random.uniform', autospec=True, return_value=0.0)
    async def test_retryable_error_max_attempts_exceeded_raises(
        self,
        _mock_uniform: MagicMock,
        _mock_sleep: AsyncMock,
    ) -> None:
        # Arrange — max_attempts=2, fails 3 times (attempt 1, 2, then exceeded)
        err_obj = _make_schema_err(retryable=True, message='transient')
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(return_value=Err(err_obj))
        resilience = WorkerResilienceConfig(db_retry_max_attempts=2)
        logger = logging.getLogger('test')

        # Act / Assert
        with pytest.raises(RuntimeError, match='transient'):
            await _ensure_schema_with_retry(broker, resilience, logger)

        # First call fails (attempt 1) → retries, second call fails (attempt 2) →
        # retries, third call fails (attempt 3) → exceeds max=2 → raises.
        # But wait: the logic checks `attempts <= max_attempts` AFTER incrementing.
        # attempt 1: should_retry = (1 <= 2) = True → sleep → loop
        # attempt 2: should_retry = (2 <= 2) = True → sleep → loop
        # attempt 3: should_retry = (3 <= 2) = False → raise
        assert broker.ensure_schema_initialized.await_count == 3

    @patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock)
    @patch('horsies.core.cli.random.uniform', autospec=True, return_value=0.0)
    async def test_retryable_error_succeeds_on_second_attempt(
        self,
        _mock_uniform: MagicMock,
        _mock_sleep: AsyncMock,
    ) -> None:
        # Arrange — fail once, then succeed
        err_obj = _make_schema_err(retryable=True, message='transient')
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[Err(err_obj), Ok(None)],
        )
        resilience = WorkerResilienceConfig(db_retry_max_attempts=3)
        logger = logging.getLogger('test')

        # Act
        await _ensure_schema_with_retry(broker, resilience, logger)

        # Assert
        assert broker.ensure_schema_initialized.await_count == 2
        _mock_sleep.assert_awaited_once()

    @patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock)
    @patch('horsies.core.cli.random.uniform', autospec=True, return_value=0.0)
    async def test_infinite_retries_succeeds_on_third_attempt(
        self,
        _mock_uniform: MagicMock,
        _mock_sleep: AsyncMock,
    ) -> None:
        # Arrange — max_attempts=0 (infinite), fail twice then succeed
        err_obj = _make_schema_err(retryable=True, message='transient')
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[Err(err_obj), Err(err_obj), Ok(None)],
        )
        resilience = WorkerResilienceConfig(db_retry_max_attempts=0)
        logger = logging.getLogger('test')

        # Act
        await _ensure_schema_with_retry(broker, resilience, logger)

        # Assert
        assert broker.ensure_schema_initialized.await_count == 3
        assert _mock_sleep.await_count == 2

    @patch('horsies.core.cli.asyncio.sleep', new_callable=AsyncMock)
    @patch('horsies.core.cli.random.uniform', autospec=True, return_value=0.0)
    async def test_backoff_delay_capped_at_max_ms(
        self,
        _mock_uniform: MagicMock,
        mock_sleep: AsyncMock,
    ) -> None:
        # Arrange — initial=500ms, max=1000ms, so second attempt should be capped
        # attempt 1: base_ms = min(1000, 500 * 2^0) = 500 → delay_s = 0.5
        # attempt 2: base_ms = min(1000, 500 * 2^1) = 1000 → delay_s = 1.0
        # attempt 3: base_ms = min(1000, 500 * 2^2) = 1000 (capped) → delay_s = 1.0
        err_obj = _make_schema_err(retryable=True, message='transient')
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[Err(err_obj), Err(err_obj), Err(err_obj), Ok(None)],
        )
        resilience = WorkerResilienceConfig(
            db_retry_initial_ms=500,
            db_retry_max_ms=1000,
            db_retry_max_attempts=0,
        )
        logger = logging.getLogger('test')

        # Act
        await _ensure_schema_with_retry(broker, resilience, logger)

        # Assert — verify the sleep delays are capped
        assert mock_sleep.await_count == 3
        calls = [call.args[0] for call in mock_sleep.call_args_list]
        # delay_s = base_ms / 1000 (jitter=0 since uniform returns 0)
        assert calls[0] == pytest.approx(0.5, abs=0.01)
        assert calls[1] == pytest.approx(1.0, abs=0.01)
        assert calls[2] == pytest.approx(1.0, abs=0.01)  # capped, not 2.0


# ---------------------------------------------------------------------------
# worker_command
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWorkerCommand:
    """Tests for worker_command — worker startup error paths."""

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_horsies_error_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange
        mock_discover.side_effect = HorsiesError(message='bad config')
        args = _make_worker_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            worker_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_value_error_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange
        mock_discover.side_effect = ValueError('bad value')
        args = _make_worker_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            worker_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_generic_exception_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange
        mock_discover.side_effect = RuntimeError('unexpected')
        args = _make_worker_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            worker_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_startup_validation_errors_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        validation_error = HorsiesError(message='bad queue config')
        app.check.return_value = [validation_error]
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_worker_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            worker_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_broker_horsies_error_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        app.get_broker.side_effect = HorsiesError(message='broker config error')
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_worker_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            worker_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_broker_generic_exception_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        app.get_broker.side_effect = RuntimeError('connection failed')
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_worker_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            worker_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_keyboard_interrupt_during_run_returns_gracefully(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_worker_namespace()

        # Act — should NOT raise SystemExit
        worker_command(args)

        # Assert — asyncio.run was called (started the worker)
        mock_run.assert_called_once()

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_exception_during_run_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        mock_run.side_effect = _closing_run_side_effect(RuntimeError('worker crashed'))
        args = _make_worker_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            worker_command(args)
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# scheduler_command
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSchedulerCommand:
    """Tests for scheduler_command — scheduler startup error paths."""

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_horsies_error_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        mock_discover.side_effect = HorsiesError(message='bad config')
        args = _make_scheduler_namespace()

        with pytest.raises(SystemExit) as exc_info:
            scheduler_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_value_error_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        mock_discover.side_effect = ValueError('bad value')
        args = _make_scheduler_namespace()

        with pytest.raises(SystemExit) as exc_info:
            scheduler_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_generic_exception_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        mock_discover.side_effect = RuntimeError('unexpected')
        args = _make_scheduler_namespace()

        with pytest.raises(SystemExit) as exc_info:
            scheduler_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_no_schedule_config_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange — app.config.schedule is None
        app = _make_mock_app()
        app.config.schedule = None
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_scheduler_namespace()

        with pytest.raises(SystemExit) as exc_info:
            scheduler_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_schedule_disabled_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange — schedule.enabled = False
        app = _make_mock_app()
        app.config.schedule.enabled = False
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_scheduler_namespace()

        with pytest.raises(SystemExit) as exc_info:
            scheduler_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_startup_validation_errors_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        validation_error = HorsiesError(message='bad schedule config')
        app.check.return_value = [validation_error]
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_scheduler_namespace()

        with pytest.raises(SystemExit) as exc_info:
            scheduler_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_keyboard_interrupt_during_run_returns_gracefully(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_scheduler_namespace()

        # Act — should NOT raise SystemExit
        scheduler_command(args)

        mock_run.assert_called_once()

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_exception_during_run_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        # Arrange
        app = _make_mock_app()
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        mock_run.side_effect = _closing_run_side_effect(RuntimeError('scheduler crashed'))
        args = _make_scheduler_namespace()

        with pytest.raises(SystemExit) as exc_info:
            scheduler_command(args)
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# check_command
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckCommand:
    """Tests for check_command — validation without starting services."""

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_discovery_fails_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        mock_discover.side_effect = HorsiesError(message='bad config')
        args = _make_check_namespace()

        with pytest.raises(SystemExit) as exc_info:
            check_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_validation_errors_prints_report_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Arrange
        app = _make_mock_app()
        validation_error = HorsiesError(message='queue config invalid')
        app.check.return_value = [validation_error]
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_check_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            check_command(args)
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        assert 'queue config invalid' in captured.err

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_no_errors_prints_ok_exits_0(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Arrange
        app = _make_mock_app()
        app.check.return_value = []
        app.list_tasks.return_value = ['task1', 'task2']
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_check_namespace()

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            check_command(args)
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert 'ok: all validations passed' in captured.out
        assert '2 task(s) registered' in captured.out


# ---------------------------------------------------------------------------
# get_docs_command
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetDocsCommand:
    """Tests for get_docs_command — docs fetcher."""

    @patch('horsies.core.docs_fetcher.fetch_docs', autospec=True)
    def test_success_calls_fetch_docs(
        self,
        mock_fetch: MagicMock,
    ) -> None:
        # Arrange
        args = argparse.Namespace(output='/tmp/docs')

        # Act
        get_docs_command(args)

        # Assert
        mock_fetch.assert_called_once_with(output_dir='/tmp/docs')

    @patch('horsies.core.docs_fetcher.fetch_docs', autospec=True)
    def test_exception_prints_error_exits_1(
        self,
        mock_fetch: MagicMock,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        # Arrange
        mock_fetch.side_effect = RuntimeError('download failed')
        args = argparse.Namespace(output='/tmp/docs')

        # Act / Assert
        with pytest.raises(SystemExit) as exc_info:
            get_docs_command(args)
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        assert 'download failed' in captured.err


# ---------------------------------------------------------------------------
# discover_app — file-path import branch
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDiscoverAppFilePath:
    """Tests for discover_app file-path import branch (lines 148-203)."""

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.os.path.exists', return_value=True)
    @patch('horsies.core.cli.import_file_path', autospec=True)
    def test_file_exists_import_ok_explicit_attr_returns_app(
        self,
        mock_import_fp: MagicMock,
        _mock_exists: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        # Arrange
        fake_module = types.ModuleType('my_module')
        fake_module.__name__ = 'my_module'
        mock_app = MagicMock(spec=Horsies)
        fake_module.app = mock_app  # type: ignore[attr-defined]
        mock_import_fp.return_value = fake_module

        # Act
        app, var_name, module_name, sys_path_root = discover_app('/path/to/file.py:app')

        # Assert
        assert app is mock_app
        assert var_name == 'app'
        assert module_name == 'my_module'

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.os.path.exists', return_value=True)
    @patch('horsies.core.cli.import_file_path', autospec=True)
    def test_file_exists_import_error_raises_config_error(
        self,
        mock_import_fp: MagicMock,
        _mock_exists: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        # Arrange
        mock_import_fp.side_effect = ImportError('missing dep')

        # Act / Assert
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('/path/to/file.py:app')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'failed to import' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.os.path.exists', return_value=True)
    @patch('horsies.core.cli.import_file_path', autospec=True)
    def test_file_exists_generic_exception_raises_module_exec_error(
        self,
        mock_import_fp: MagicMock,
        _mock_exists: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        # Arrange
        mock_import_fp.side_effect = TypeError('bad code in module')

        # Act / Assert
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('/path/to/file.py:app')
        assert exc_info.value.code == ErrorCode.MODULE_EXEC_ERROR
        assert 'error while importing' in exc_info.value.message

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    @patch('horsies.core.cli.os.path.exists', return_value=True)
    @patch('horsies.core.cli.import_file_path', autospec=True)
    def test_file_path_without_py_extension_appends_py(
        self,
        mock_import_fp: MagicMock,
        _mock_exists: MagicMock,
        _mock_sys: MagicMock,
    ) -> None:
        """File paths like 'app/configs/horsies' get .py appended (line 151)."""
        # Arrange
        fake_module = types.ModuleType('my_module')
        mock_app = MagicMock(spec=Horsies)
        fake_module.app = mock_app  # type: ignore[attr-defined]
        mock_import_fp.return_value = fake_module

        # Act — path has slash (so _is_file_path=True) but no .py extension
        app, var_name, _module_name, _sys_path_root = discover_app('app/configs/horsies:app')

        # Assert
        assert app is mock_app
        assert var_name == 'app'
        # import_file_path was called with a path ending in .py
        called_path = mock_import_fp.call_args[0][0]
        assert called_path.endswith('.py')


# ---------------------------------------------------------------------------
# discover_app — additional branch coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDiscoverAppAdditionalBranches:
    """Extra tests to cover remaining discover_app branches."""

    @patch('horsies.core.cli.importlib.import_module')
    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value='/my/project')
    def test_project_root_logged_when_truthy(
        self,
        _mock_sys: MagicMock,
        mock_import: MagicMock,
    ) -> None:
        """When setup_sys_path_from_cwd returns a value, project_root is set (line 142)."""
        # Arrange
        fake_module = types.ModuleType('fake_single')
        mock_app = MagicMock(spec=Horsies)
        fake_module.my_app = mock_app  # type: ignore[attr-defined]
        mock_import.return_value = fake_module

        # Act
        app, var_name, module_name, sys_path_root = discover_app('fake_single')

        # Assert
        assert app is mock_app
        assert sys_path_root == '/my/project'

    @patch('horsies.core.cli.setup_sys_path_from_cwd', return_value=None)
    def test_ambiguous_pattern_without_attr_includes_extra_help(
        self,
        _mock_sys: MagicMock,
    ) -> None:
        """Ambiguous locator without ':attr' appends extra help about <app_name> (line 166)."""
        # 'app.configs.horsies.py' with no ':attr' → attr_name is None
        with pytest.raises(ConfigurationError) as exc_info:
            discover_app('app.configs.horsies.py')
        assert exc_info.value.code == ErrorCode.CLI_INVALID_ARGS
        assert 'ambiguous' in exc_info.value.message
        assert '<app_name>' in (exc_info.value.help_text or '')


# ---------------------------------------------------------------------------
# worker_command — additional branch coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWorkerCommandAdditionalBranches:
    """Extra tests to cover remaining worker_command branches."""

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_no_discovered_modules_still_proceeds(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        """When no task modules discovered, warning is logged but worker starts (line 381)."""
        # Arrange
        app = _make_mock_app()
        app.get_discovered_task_modules.return_value = []
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_worker_namespace()

        # Act — should not raise
        worker_command(args)

        mock_run.assert_called_once()

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_timeout_error_during_run_returns_gracefully(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        """asyncio.TimeoutError during run → graceful return, no sys.exit (lines 471-472)."""
        # Arrange
        import asyncio as _asyncio

        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        mock_run.side_effect = _closing_run_side_effect(_asyncio.TimeoutError())
        args = _make_worker_namespace()

        # Act — should NOT raise SystemExit
        worker_command(args)

        mock_run.assert_called_once()

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_file_path_locator_builds_absolute_app_locator(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        """File path module locator builds absolute app_locator (lines 395-398)."""
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        # Use file path locator format
        mock_discover.return_value = (app, 'app', 'my_module', '/project')
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_worker_namespace(module='app/configs/horsies.py:app')

        # Act
        worker_command(args)

        # Assert — worker was started (gets past the locator building)
        mock_run.assert_called_once()

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_custom_queue_mode_builds_priorities(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        """CUSTOM queue mode builds queue_priorities and queue_max_concurrency (lines 370-374)."""
        # Arrange
        app = _make_mock_app()
        app.config.queue_mode.name = 'CUSTOM'
        custom_q = MagicMock()
        custom_q.name = 'high'
        custom_q.priority = 10
        custom_q.max_concurrency = 5
        app.config.custom_queues = [custom_q]
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_worker_namespace()

        # Act
        worker_command(args)

        # Assert — worker was started (passes through CUSTOM branch)
        mock_run.assert_called_once()


# ---------------------------------------------------------------------------
# check_command — additional branch coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckCommandAdditionalBranches:
    """Extra tests for check_command ValueError and generic Exception."""

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_value_error_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        mock_discover.side_effect = ValueError('bad value')
        args = _make_check_namespace()

        with pytest.raises(SystemExit) as exc_info:
            check_command(args)
        assert exc_info.value.code == 1

    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_generic_exception_during_discover_exits_1(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
    ) -> None:
        mock_discover.side_effect = RuntimeError('unexpected')
        args = _make_check_namespace()

        with pytest.raises(SystemExit) as exc_info:
            check_command(args)
        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# main() — KeyboardInterrupt
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMainKeyboardInterrupt:
    """Test main() KeyboardInterrupt handling (lines 789-791)."""

    def test_keyboard_interrupt_exits_0(self) -> None:
        with (
            patch('sys.argv', ['horsies', 'worker', 'my.mod:app']),
            patch(
                'horsies.core.cli.worker_command',
                autospec=True,
                side_effect=KeyboardInterrupt,
            ),
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# setup_logging — handler branch coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSetupLoggingHandlers:
    """setup_logging replaces existing handlers via configure_logging."""

    def test_replaces_handlers_with_colored_formatter(self) -> None:
        from horsies.core.logging import ColoredFormatter

        setup_logging('DEBUG')

        logger = logging.getLogger('horsies')
        assert logger.level == logging.DEBUG
        non_null = [h for h in logger.handlers if not isinstance(h, logging.NullHandler)]
        assert len(non_null) == 1
        assert isinstance(non_null[0].formatter, ColoredFormatter)


# ---------------------------------------------------------------------------
# worker_command — outer KeyboardInterrupt and file-path .py append
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWorkerCommandOuterInterrupt:
    """Covers file-path locator .py append branch (line 397)."""

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_file_path_locator_without_py_appends_py(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        """File path locator without .py gets .py appended (line 397)."""
        # Arrange — use file path without .py extension
        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my_module', '/project')
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_worker_namespace(module='app/configs/horsies:app')

        # Act
        worker_command(args)

        mock_run.assert_called_once()


# ---------------------------------------------------------------------------
# scheduler_command — outer KeyboardInterrupt + file-path module import
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSchedulerCommandAdditionalBranches:
    """Extra tests for scheduler_command branches."""

    @patch('horsies.core.cli.import_file_path', autospec=True)
    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_file_path_module_imports_via_import_file_path(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
        mock_import_fp: MagicMock,
    ) -> None:
        """File-path modules use import_file_path in scheduler (line 530)."""
        # Arrange
        app = _make_mock_app()
        app.get_discovered_task_modules.return_value = ['tasks/my_tasks.py']
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_scheduler_namespace()

        # Act
        scheduler_command(args)

        # Assert — import_file_path was called for the file-path module
        mock_import_fp.assert_called_once()


# ---------------------------------------------------------------------------
# worker_command — CUSTOM queue exception branch
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWorkerCommandQueueException:
    """Cover the except pass in CUSTOM queue building (lines 373-374)."""

    @patch('horsies.core.cli.asyncio.run', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    def test_custom_queue_exception_silently_passes(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        """Exception in CUSTOM queue building is silently ignored (except: pass)."""
        # Arrange — make custom_queues iteration raise
        app = _make_mock_app()
        app.config.queue_mode.name = 'CUSTOM'
        # custom_queues is not iterable → raises TypeError when for-loop runs
        app.config.custom_queues = 42  # type: ignore[assignment]
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        mock_run.side_effect = _closing_run_side_effect(KeyboardInterrupt)
        args = _make_worker_namespace()

        # Act — should NOT raise (exception is caught by except: pass)
        worker_command(args)


# ---------------------------------------------------------------------------
# run_worker / run_scheduler inner async function coverage
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.filterwarnings('ignore:coroutine.*was never awaited:RuntimeWarning')
class TestRunWorkerCoroutine:
    """Cover the inner run_worker async body by capturing the coroutine."""

    @patch('horsies.core.cli.Worker', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    async def test_run_worker_coroutine_success(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_worker_cls: MagicMock,
    ) -> None:
        """Capture the coroutine passed to asyncio.run and execute it."""
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        args = _make_worker_namespace()

        # Mock Worker instance
        mock_worker = MagicMock()
        mock_worker.run_forever = AsyncMock()
        mock_worker.request_stop = MagicMock()
        mock_worker_cls.return_value = mock_worker

        captured_coro: list[Any] = []

        def capture_run(coro: Any) -> None:
            captured_coro.append(coro)
            raise KeyboardInterrupt  # Break out of worker_command

        with patch('horsies.core.cli.asyncio.run', side_effect=capture_run):
            worker_command(args)

        # Act — now run the captured coroutine
        assert len(captured_coro) == 1
        await captured_coro[0]

        # Assert
        broker.ensure_schema_initialized.assert_awaited_once()
        mock_worker_cls.assert_called_once()
        mock_worker.run_forever.assert_awaited_once()
        broker.close_async.assert_awaited_once()

    @patch('horsies.core.cli.Worker', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    async def test_run_worker_coroutine_schema_fail_reraises(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_worker_cls: MagicMock,
    ) -> None:
        """Schema init failure inside run_worker raises out."""
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        err_obj = _make_schema_err(retryable=False, message='fatal')
        broker.ensure_schema_initialized = AsyncMock(return_value=Err(err_obj))
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        args = _make_worker_namespace()

        captured_coro: list[Any] = []

        def capture_run(coro: Any) -> None:
            captured_coro.append(coro)
            raise KeyboardInterrupt

        with patch('horsies.core.cli.asyncio.run', side_effect=capture_run):
            worker_command(args)

        # Act / Assert — coroutine should raise RuntimeError
        with pytest.raises(RuntimeError, match='fatal'):
            await captured_coro[0]

    @patch('horsies.core.cli.Worker', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.to_psycopg_url', autospec=True, return_value='psycopg://x')
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    async def test_run_worker_broker_close_err_logged(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_url: MagicMock,
        _mock_banner: MagicMock,
        mock_worker_cls: MagicMock,
    ) -> None:
        """Broker close returning Err is handled gracefully (logged, no crash)."""
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        close_err = _make_schema_err(retryable=False, message='close failed')
        broker.close_async = AsyncMock(return_value=Err(close_err))
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', '/project')
        args = _make_worker_namespace()

        mock_worker = MagicMock()
        mock_worker.run_forever = AsyncMock()
        mock_worker_cls.return_value = mock_worker

        captured_coro: list[Any] = []

        def capture_run(coro: Any) -> None:
            captured_coro.append(coro)
            raise KeyboardInterrupt

        with patch('horsies.core.cli.asyncio.run', side_effect=capture_run):
            worker_command(args)

        # Act — should complete without error despite close failure
        await captured_coro[0]

        broker.close_async.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.filterwarnings('ignore:coroutine.*was never awaited:RuntimeWarning')
class TestRunSchedulerCoroutine:
    """Cover the inner run_scheduler async body by capturing the coroutine."""

    @patch('horsies.core.cli.Scheduler', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    async def test_run_scheduler_coroutine_success(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_banner: MagicMock,
        mock_scheduler_cls: MagicMock,
    ) -> None:
        """Capture and run the scheduler coroutine."""
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_scheduler_namespace()

        mock_scheduler = MagicMock()
        mock_scheduler.run_forever = AsyncMock()
        mock_scheduler.request_stop = MagicMock()
        mock_scheduler_cls.return_value = mock_scheduler

        captured_coro: list[Any] = []

        def capture_run(coro: Any) -> None:
            captured_coro.append(coro)
            raise KeyboardInterrupt

        with patch('horsies.core.cli.asyncio.run', side_effect=capture_run):
            scheduler_command(args)

        # Act
        await captured_coro[0]

        # Assert
        broker.ensure_schema_initialized.assert_awaited_once()
        mock_scheduler_cls.assert_called_once_with(app)
        mock_scheduler.run_forever.assert_awaited_once()

    @patch('horsies.core.cli.Scheduler', autospec=True)
    @patch('horsies.core.cli.print_banner', autospec=True)
    @patch('horsies.core.cli.setup_logging', autospec=True)
    @patch('horsies.core.cli.discover_app', autospec=True)
    async def test_run_scheduler_coroutine_error_reraises(
        self,
        mock_discover: MagicMock,
        _mock_logging: MagicMock,
        _mock_banner: MagicMock,
        mock_scheduler_cls: MagicMock,
    ) -> None:
        """Scheduler error inside run_scheduler is logged and re-raised."""
        # Arrange
        app = _make_mock_app()
        broker = _make_mock_broker()
        app.get_broker.return_value = broker
        mock_discover.return_value = (app, 'app', 'my.mod', None)
        args = _make_scheduler_namespace()

        mock_scheduler = MagicMock()
        mock_scheduler.run_forever = AsyncMock(side_effect=RuntimeError('scheduler boom'))
        mock_scheduler_cls.return_value = mock_scheduler

        captured_coro: list[Any] = []

        def capture_run(coro: Any) -> None:
            captured_coro.append(coro)
            raise KeyboardInterrupt

        with patch('horsies.core.cli.asyncio.run', side_effect=capture_run):
            scheduler_command(args)

        # Act / Assert
        with pytest.raises(RuntimeError, match='scheduler boom'):
            await captured_coro[0]
