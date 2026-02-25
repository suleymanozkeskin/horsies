# app/core/app.py
from typing import (
    Optional,
    Callable,
    TypeVar,
    overload,
    TYPE_CHECKING,
    ParamSpec,
    Any,
    Union,
    cast,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskOptions, RetryPolicy
from horsies.core.task_decorator import create_task_wrapper, effective_priority
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowSpec,
    WorkflowTerminalResults,
    OnError,
    SuccessPolicy,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.logging import get_logger
from horsies.core.registry.tasks import TaskRegistry
from horsies.core.exception_mapper import (
    ExceptionMapper,
    validate_exception_mapper,
    validate_error_code_string,
)
from horsies.core.errors import (
    ConfigurationError,
    HorsiesError,
    MultipleValidationErrors,
    TaskDefinitionError,
    ErrorCode,
    SourceLocation,
    ValidationReport,
    raise_collected,
)
import inspect
import os
import importlib
import glob
import sys
from dataclasses import dataclass
from fnmatch import fnmatch
from horsies.core.utils.imports import import_by_path
from horsies.core.codec.serde import loads_json
from horsies.core.types.result import is_err

if TYPE_CHECKING:
    from horsies.core.task_decorator import TaskFunction
    from horsies.core.models.tasks import TaskResult, TaskError

P = ParamSpec('P')
T = TypeVar('T')
OutT = TypeVar('OutT')

_E = TypeVar('_E', bound=HorsiesError)
_F = TypeVar('_F', bound=Callable[..., Any])

# Sentinel attribute name stamped on functions decorated with @app.workflow_builder
_BUILDER_ATTR = '__horsies_workflow_builder__'

# Attribute name for opt-out of undecorated builder detection
_NO_CHECK_ATTR = '__horsies_no_check__'


@dataclass
class _WorkflowBuilderMeta:
    """Internal metadata for a registered workflow builder."""

    fn: Callable[..., Any]
    cases: list[dict[str, Any]]
    location: SourceLocation | None


def _no_location(error: _E) -> _E:
    """Strip the auto-detected source location from a programmatic error.

    Used for errors created inside horsies internals where the auto-detected
    frame (e.g., CLI entry point) is misleading. The error message itself
    contains the relevant context (e.g., module path).
    """
    error.location = None
    return error


class Horsies:
    """
    Configuration-driven task management app.
    Requires an AppConfig instance for proper validation and type safety.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self._broker: Optional['PostgresBroker'] = None
        self.tasks: TaskRegistry[Callable[..., Any]] = TaskRegistry()
        self.logger = get_logger('app')
        self._discovered_task_modules: list[str] = []
        self._workflow_builders: list[_WorkflowBuilderMeta] = []
        # When True, task sends/schedules are suppressed (used during import/discovery)
        self._suppress_sends: bool = False
        # Role indicates context: 'producer', 'worker', or 'scheduler'
        self._role: str = 'producer'

        if os.getenv('HORSIES_CHILD_PROCESS') == '1':
            self.logger.info(
                f'horsies subprocess initialized with {config.queue_mode.name} mode (pid={os.getpid()})'
            )
        else:
            self.logger.info(
                f'horsies initialized as {self._role} with {config.queue_mode.name} mode'
            )

    def set_role(self, role: str) -> None:
        """Set the role and log it. Called by CLI after discovery."""
        self._role = role
        self.logger.info(f'horsies running as {role}')

    def get_valid_queue_names(self) -> list[str]:
        """Get list of valid queue names based on configuration"""
        if self.config.queue_mode == QueueMode.DEFAULT:
            return ['default']
        else:  # CUSTOM mode
            return [queue.name for queue in (self.config.custom_queues or [])]

    def validate_queue_name(self, queue_name: Optional[str]) -> str:
        """Validate queue name against app configuration"""
        if self.config.queue_mode == QueueMode.DEFAULT:
            if queue_name is not None:
                raise ConfigurationError(
                    message='cannot specify queue_name in DEFAULT mode',
                    code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
                    notes=[
                        f"queue_name='{queue_name}' was specified",
                        'but app is configured with QueueMode.DEFAULT',
                    ],
                    help_text='either remove queue_name or switch to QueueMode.CUSTOM',
                )
            return 'default'
        else:  # CUSTOM mode
            if queue_name is None:
                raise ConfigurationError(
                    message='queue_name is required in CUSTOM mode',
                    code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
                    notes=['app is configured with QueueMode.CUSTOM'],
                    help_text='specify queue_name from configured queues',
                )
            valid_queues = self.get_valid_queue_names()
            if queue_name not in valid_queues:
                raise ConfigurationError(
                    message=f"invalid queue_name '{queue_name}'",
                    code=ErrorCode.TASK_INVALID_QUEUE,
                    notes=[f'valid queues: {valid_queues}'],
                    help_text='use one of the configured queue names',
                )
            return queue_name

    @overload
    def task(
        self, task_name: str, func: Callable[P, 'TaskResult[T, TaskError]']
    ) -> 'TaskFunction[P, T]': ...

    @overload
    def task(
        self,
        task_name: str,
        *,
        queue_name: Optional[str] = None,
        good_until: Any = None,
        retry_policy: Optional['RetryPolicy'] = None,
        exception_mapper: Optional['ExceptionMapper'] = None,
        default_unhandled_error_code: Optional[str] = None,
    ) -> Callable[
        [Callable[P, 'TaskResult[T, TaskError]']],
        'TaskFunction[P, T]',
    ]: ...

    def task(
        self,
        task_name: str,
        func: Optional[Callable[P, 'TaskResult[T, TaskError]']] = None,
        **task_options_kwargs: Any,
    ) -> Union[
        'TaskFunction[P, T]',
        Callable[
            [Callable[P, 'TaskResult[T, TaskError]']],
            'TaskFunction[P, T]',
        ],
    ]:
        """
        Decorator to register a task with this app.
        Task options are validated against TaskOptions model and app configuration.
        """

        def decorator(fn: Callable[P, 'TaskResult[T, TaskError]']):
            fn_location = SourceLocation.from_function(fn)

            if 'auto_retry_for' in task_options_kwargs:
                raise TaskDefinitionError(
                    message='invalid task options',
                    code=ErrorCode.TASK_INVALID_OPTIONS,
                    location=fn_location,
                    notes=[
                        f"task '{fn.__name__}'",
                        "top-level 'auto_retry_for' is deprecated",
                    ],
                    help_text=(
                        "move retry triggers into RetryPolicy, e.g. "
                        "RetryPolicy.fixed([...], auto_retry_for=[...])"
                    ),
                )

            # Pop mapper-related kwargs (not part of TaskOptions, not serialized)
            exception_mapper: ExceptionMapper | None = task_options_kwargs.pop(
                'exception_mapper', None,
            )
            default_unhandled_error_code: str | None = task_options_kwargs.pop(
                'default_unhandled_error_code', None,
            )

            # Validate per-task mapper if provided
            if exception_mapper is not None:
                mapper_errors = validate_exception_mapper(
                    exception_mapper,
                )
                if mapper_errors:
                    raise TaskDefinitionError(
                        message='invalid exception_mapper',
                        code=ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        location=fn_location,
                        notes=[f"task '{fn.__name__}'", *mapper_errors],
                        help_text='keys must be BaseException subclasses, values must be UPPER_SNAKE_CASE error codes',
                    )

            # Validate per-task default_unhandled_error_code if provided
            if default_unhandled_error_code is not None:
                code_error = validate_error_code_string(
                    default_unhandled_error_code,
                    field_name='default_unhandled_error_code',
                )
                if code_error is not None:
                    raise TaskDefinitionError(
                        message='invalid default_unhandled_error_code',
                        code=ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        location=fn_location,
                        notes=[f"task '{fn.__name__}'", code_error],
                        help_text='use UPPER_SNAKE_CASE error codes',
                    )

            # Validate and create TaskOptions - this enforces pydantic validation
            try:
                task_options = TaskOptions(task_name=task_name, **task_options_kwargs)
            except Exception as e:
                raise TaskDefinitionError(
                    message=f'invalid task options',
                    code=ErrorCode.TASK_INVALID_OPTIONS,
                    location=fn_location,
                    notes=[f"task '{fn.__name__}'", str(e)],
                    help_text='check task decorator arguments',
                )

            # VALIDATION AT DEFINITION TIME
            # Validate queue_name against app configuration
            try:
                self.validate_queue_name(task_options.queue_name)
            except ConfigurationError:
                raise  # Re-raise with original formatting

            # Create wrapper that uses this app's configuration
            task_function = create_task_wrapper(
                fn,
                self,
                task_name,
                task_options,
                exception_mapper=exception_mapper,
                default_unhandled_error_code=default_unhandled_error_code,
            )

            # Register task with this app, passing source for duplicate detection
            # Normalize path with realpath to handle symlinks and relative paths
            source_str = (
                f'{os.path.realpath(fn_location.file)}:{fn_location.line}'
                if fn_location
                else None
            )
            self.tasks.register(task_function, name=task_name, source=source_str)

            return task_function

        if func is None:
            # Called with arguments: @app.task(queue_name="custom")
            return decorator
        else:
            # Called without arguments: @app.task
            return decorator(func)

    def check(self, *, live: bool = False) -> list[HorsiesError]:
        """Orchestrate phased validation and return all errors found.

        Phase 1: Config — already validated at construction (implicit pass).
        Phase 2: Task module imports — import each module, collect errors.
        Phase 3: Workflow validation — happens during imports (WorkflowSpec construction).
        Phase 3.1: Workflow builder execution — run registered builders under send suppression.
        Phase 3.2: Undecorated builder detection — fail-closed on missing decorators.
        Phase 3.5: Policy safety checks — runtime retry/mapping validation.
        Phase 4 (if live): Broker connectivity — async SELECT 1.

        Args:
            live: If True, also check broker connectivity (Phase 4).

        Returns:
            List of all HorsiesError instances found across phases.
            Empty list means all validations passed.
        """
        all_errors: list[HorsiesError] = []

        # Phase 2: task module imports (also triggers Phase 3 workflow validation)
        all_errors.extend(self._check_task_imports())
        if all_errors:
            return all_errors

        # Phase 3.1: execute registered workflow builders under send suppression
        all_errors.extend(self._check_workflow_builders())
        if all_errors:
            return all_errors

        # Phase 3.2: detect undecorated top-level WorkflowSpec-returning functions
        all_errors.extend(self._check_undecorated_builders())
        if all_errors:
            return all_errors

        # Phase 3.5: policy safety checks that require imported task metadata.
        all_errors.extend(self._check_runtime_policy_safety())
        if all_errors:
            return all_errors

        # Phase 4 (optional): broker connectivity
        if live:
            all_errors.extend(self._check_broker_connectivity())

        return all_errors

    def _check_task_imports(self) -> list[HorsiesError]:
        """Import task modules and collect any errors."""
        errors: list[HorsiesError] = []
        modules = self._discovered_task_modules
        prev_suppress = self._suppress_sends
        self.suppress_sends(True)
        try:
            for module_path in modules:
                try:
                    if module_path.endswith('.py') or os.path.sep in module_path:
                        abs_path = os.path.realpath(module_path)
                        if not os.path.exists(abs_path):
                            errors.append(
                                _no_location(
                                    ConfigurationError(
                                        message=f'task module not found: {module_path}',
                                        code=ErrorCode.CLI_INVALID_ARGS,
                                        notes=[f'resolved path: {abs_path}'],
                                        help_text=(
                                            'remove it from app.discover_tasks([...]) or fix the path; \n'
                                            'if using globs, run app.expand_module_globs([...]) first'
                                        ),
                                    )
                                )
                            )
                            continue
                        import_by_path(abs_path)
                    else:
                        importlib.import_module(module_path)
                except MultipleValidationErrors as exc:
                    errors.extend(exc.report.errors)
                except HorsiesError as exc:
                    errors.append(exc)
                except (ModuleNotFoundError, ImportError) as exc:
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=f'failed to import module: {module_path}',
                                code=ErrorCode.CLI_INVALID_ARGS,
                                notes=[str(exc)],
                                help_text=(
                                    'ensure the module is importable; '
                                    'for file paths include .py and a valid path, '
                                    'for dotted paths verify PYTHONPATH or run from the project root'
                                ),
                            )
                        )
                    )
                except Exception as exc:
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=f'error while importing module: {module_path}',
                                code=ErrorCode.MODULE_EXEC_ERROR,
                                notes=[f'{type(exc).__name__}: {exc}'],
                                help_text=(
                                    'the module was found but raised an error during import;\n'
                                    'check the module-level code for bugs'
                                ),
                            )
                        )
                    )
        finally:
            self.suppress_sends(prev_suppress)
        return errors

    def _check_runtime_policy_safety(self) -> list[HorsiesError]:
        """Validate runtime retry/mapping policies after task imports."""
        errors: list[HorsiesError] = []

        # Re-validate global settings so post-construction mutations fail startup.
        mapper_errors = validate_exception_mapper(self.config.exception_mapper)
        for msg in mapper_errors:
            errors.append(
                _no_location(
                    ConfigurationError(
                        message=msg,
                        code=ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        notes=['app config'],
                        help_text='exception_mapper must be a mapping with exception-class keys and UPPER_SNAKE_CASE code values',
                    )
                )
            )

        global_default_error = validate_error_code_string(
            self.config.default_unhandled_error_code,
            field_name='default_unhandled_error_code',
        )
        if global_default_error is not None:
            errors.append(
                _no_location(
                    ConfigurationError(
                        message=global_default_error,
                        code=ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                        notes=['app config'],
                        help_text='use UPPER_SNAKE_CASE error codes',
                    )
                )
            )

        for task_name, task in self.tasks.items():
            task_default = getattr(task, 'default_unhandled_error_code', None)
            if task_default is not None:
                code_error = validate_error_code_string(
                    task_default,
                    field_name='default_unhandled_error_code',
                )
                if code_error is not None:
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message=code_error,
                                code=ErrorCode.CONFIG_INVALID_EXCEPTION_MAPPER,
                                notes=[f"task '{task_name}'"],
                                help_text='use UPPER_SNAKE_CASE error codes',
                            )
                        )
                    )

            task_options_json = getattr(task, 'task_options_json', None)
            if not isinstance(task_options_json, str) or not task_options_json:
                continue

            options_result = loads_json(task_options_json)
            if is_err(options_result):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='invalid task_options_json',
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'", str(options_result.err_value)],
                            help_text='task options metadata must be valid JSON',
                        )
                    )
                )
                continue
            options = options_result.ok_value
            if not isinstance(options, dict):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='task_options_json must be an object',
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'", f'task_options_json={options!r}'],
                            help_text='task options metadata must be a JSON object',
                        )
                    )
                )
                continue
            retry_policy_raw = options.get('retry_policy')
            if retry_policy_raw is None:
                continue
            if not isinstance(retry_policy_raw, dict):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='retry_policy must be an object',
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'", f'retry_policy={retry_policy_raw!r}'],
                            help_text='retry policy metadata must be a JSON object',
                        )
                    )
                )
                continue
            auto_retry_for = retry_policy_raw.get('auto_retry_for')
            if auto_retry_for is None:
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='retry_policy.auto_retry_for is required',
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'"],
                            help_text='configure retry triggers in RetryPolicy(auto_retry_for=[...])',
                        )
                    )
                )
                continue
            if not isinstance(auto_retry_for, list):
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='auto_retry_for must be a list',
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[
                                f"task '{task_name}'",
                                f'auto_retry_for={auto_retry_for!r}',
                            ],
                            help_text='use a list of UPPER_SNAKE_CASE error codes',
                        )
                    )
                )
                continue
            if len(auto_retry_for) == 0:
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='auto_retry_for must not be empty',
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'"],
                            help_text='provide at least one UPPER_SNAKE_CASE error code',
                        )
                    )
                )
                continue

            has_entry_errors = False
            for idx, entry in enumerate(auto_retry_for):
                if not isinstance(entry, str):
                    errors.append(
                        _no_location(
                            ConfigurationError(
                                message='auto_retry_for entries must be strings',
                                code=ErrorCode.TASK_INVALID_OPTIONS,
                                notes=[
                                    f"task '{task_name}'",
                                    f'auto_retry_for[{idx}]={entry!r}',
                                ],
                                help_text='use explicit UPPER_SNAKE_CASE error codes',
                            )
                        )
                    )
                    has_entry_errors = True
                    continue
                code_error = validate_error_code_string(
                    entry,
                    field_name='auto_retry_for',
                )
                if code_error is None:
                    continue
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message=code_error,
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'"],
                            help_text='replace exception names with explicit error codes',
                        )
                    )
                )
                has_entry_errors = True
            if has_entry_errors:
                continue

            try:
                RetryPolicy.model_validate(retry_policy_raw)
            except Exception as exc:
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message='invalid retry_policy metadata',
                            code=ErrorCode.TASK_INVALID_OPTIONS,
                            notes=[f"task '{task_name}'", str(exc)],
                            help_text='ensure retry_policy matches RetryPolicy schema',
                        )
                    )
                )

        return errors

    def _check_broker_connectivity(self) -> list[HorsiesError]:
        """Check broker connectivity via SELECT 1 using an isolated engine.

        Uses a short-lived async engine/session to avoid mutating the app's
        long-lived broker/session pool inside an ephemeral asyncio.run loop.
        """
        import asyncio

        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

        HEALTH_CHECK_SQL = text("""SELECT 1""")

        errors: list[HorsiesError] = []
        try:
            engine_cfg = self.config.broker.model_dump(
                exclude={'database_url'}, exclude_none=True
            )
            health_engine = create_async_engine(
                self.config.broker.database_url, **engine_cfg
            )
            health_session_factory = async_sessionmaker(
                health_engine, expire_on_commit=False
            )

            async def _test_connection() -> None:
                try:
                    async with health_session_factory() as session:
                        await session.execute(HEALTH_CHECK_SQL)
                finally:
                    await health_engine.dispose()

            asyncio.run(_test_connection())
        except HorsiesError as exc:
            errors.append(exc)
        except Exception as exc:
            errors.append(
                _no_location(
                    ConfigurationError(
                        message='broker connectivity check failed',
                        code=ErrorCode.BROKER_INVALID_URL,
                        notes=[str(exc)],
                        help_text='check database_url in PostgresConfig',
                    )
                )
            )
        return errors

    def list_tasks(self) -> list[str]:
        """List tasks registered with this app"""
        return list(self.tasks.keys_list())

    def get_broker(self) -> 'PostgresBroker':
        """Get the configured PostgreSQL broker for this app"""
        try:
            if self._broker is None:
                self._broker = PostgresBroker(self.config.broker)
                self._broker.app = self  # Store app reference for subworkflow support
            return self._broker
        except HorsiesError:
            raise
        except Exception as e:
            raise ValueError(f'Failed to get broker: {e}')

    def discover_tasks(
        self,
        modules: list[str],
    ) -> None:
        """
        Register task modules for later import.

        This method only records module paths — no file I/O happens here.
        Actual imports occur when import_task_modules() is called (typically by worker).

        Args:
            modules: List of dotted module paths (e.g., ['myapp.tasks', 'myapp.jobs.tasks'])
                     or file paths (e.g., ['tasks.py', 'src/worker_tasks.py'])

        Examples:
            app.discover_tasks(['myapp.tasks'])  # dotted module path
            app.discover_tasks(['tasks.py'])     # file path

            # For glob patterns, use expand_module_globs() first:
            paths = app.expand_module_globs(['src/**/*_tasks.py'])
            app.discover_tasks(paths)
        """
        if self._discovered_task_modules:
            self.logger.warning(
                f'discover_tasks() called again — replacing {len(self._discovered_task_modules)} '
                f'previously registered module(s) with {len(modules)} new module(s)'
            )
        self._discovered_task_modules = list(modules)

        is_child_process = os.getenv('HORSIES_CHILD_PROCESS') == '1'
        child_logs_enabled = os.getenv('HORSIES_CHILD_DISCOVERY_LOGS') == '1'
        should_log = not is_child_process or child_logs_enabled

        if should_log and len(modules) > 0:
            self.logger.info(f'Registered {len(modules)} task module(s) for discovery')

    def expand_module_globs(
        self,
        patterns: list[str],
        exclude: list[str] | None = None,
    ) -> list[str]:
        """
        Expand glob patterns to file paths.

        Use this explicitly when you need glob-based discovery.
        This is separated from discover_tasks() to make the I/O cost explicit.

        Args:
            patterns: Glob patterns like ['src/**/*_tasks.py'] or file paths
            exclude: Glob patterns to exclude (default: test files)

        Returns:
            List of absolute file paths

        Examples:
            paths = app.expand_module_globs(['src/**/*_tasks.py'])
            app.discover_tasks(paths)
        """
        exclude_patterns = exclude or ['*_test.py', 'test_*.py', 'conftest.py']
        results: list[str] = []

        def _is_excluded(path: str) -> bool:
            basename = os.path.basename(path)
            for pattern in exclude_patterns:
                if fnmatch(path, pattern) or fnmatch(basename, pattern):
                    return True
            return False

        for pattern in patterns:
            has_glob = any(ch in pattern for ch in ['*', '?', '[', ']'])
            if has_glob:
                for match in glob.glob(pattern, recursive=True):
                    abs_path = os.path.realpath(match)
                    if not os.path.exists(abs_path):
                        continue
                    if os.path.isdir(abs_path):
                        continue
                    if not abs_path.endswith('.py'):
                        continue
                    if _is_excluded(abs_path):
                        continue
                    if abs_path not in results:
                        results.append(abs_path)
            elif pattern.endswith('.py') or os.path.sep in pattern:
                # Direct file path
                abs_path = os.path.realpath(pattern)
                if os.path.exists(abs_path) and abs_path.endswith('.py'):
                    if not _is_excluded(abs_path) and abs_path not in results:
                        results.append(abs_path)
            else:
                # Dotted module path - pass through as-is
                if pattern not in results:
                    results.append(pattern)

        return results

    def get_discovered_task_modules(self) -> list[str]:
        """Get the list of discovered task modules"""
        return self._discovered_task_modules.copy()

    def import_task_modules(
        self,
        modules: Optional[list[str]] = None,
    ) -> list[str]:
        """Import task modules to eagerly register tasks.

        If modules is None, imports the modules discovered by discover_tasks().
        Returns the list of module identifiers that were imported.
        """
        modules_to_import = (
            self._discovered_task_modules if modules is None else modules
        )
        imported: list[str] = []
        for module in modules_to_import:
            if module.endswith('.py') or os.path.sep in module:
                abs_path = os.path.realpath(module)
                if not os.path.exists(abs_path):
                    self.logger.warning(f'Task module not found: {module}')
                    continue
                import_by_path(abs_path)
                imported.append(abs_path)
            else:
                importlib.import_module(module)
                imported.append(module)
        return imported

    # -------- side-effect control (import/discovery) --------
    def suppress_sends(self, value: bool = True) -> None:
        """Enable/disable suppression of task sends/schedules.

        Library-internal use: the worker sets this True while importing user
        modules for task discovery so any top-level `.send()` calls in those
        modules do not enqueue tasks as an import side effect.
        """
        self._suppress_sends = value

    def are_sends_suppressed(self) -> bool:
        """Return True if sends/schedules should be no-ops.

        Environment override: if TASKLIB_SUPPRESS_SENDS=1, suppression is also
        considered active (useful for ad-hoc scripting).
        """
        env_flag = os.getenv('TASKLIB_SUPPRESS_SENDS', '').strip()
        return self._suppress_sends or env_flag == '1'

    # -------- workflow factory --------
    @overload
    def workflow(
        self,
        name: str,
        tasks: list[TaskNode[Any] | SubWorkflowNode[Any]],
        on_error: OnError,
        output: TaskNode[OutT] | SubWorkflowNode[OutT],
        success_policy: SuccessPolicy | None = None,
    ) -> WorkflowSpec[OutT]: ...

    @overload
    def workflow(
        self,
        name: str,
        tasks: list[TaskNode[Any] | SubWorkflowNode[Any]],
        on_error: OnError = OnError.FAIL,
        *,
        output: TaskNode[OutT] | SubWorkflowNode[OutT],
        success_policy: SuccessPolicy | None = None,
    ) -> WorkflowSpec[OutT]: ...

    @overload
    def workflow(
        self,
        name: str,
        tasks: list[TaskNode[Any] | SubWorkflowNode[Any]],
        on_error: OnError = OnError.FAIL,
        output: None = None,
        success_policy: SuccessPolicy | None = None,
    ) -> WorkflowSpec[WorkflowTerminalResults]: ...

    def workflow(
        self,
        name: str,
        tasks: list[TaskNode[Any] | SubWorkflowNode[Any]],
        on_error: OnError = OnError.FAIL,
        output: TaskNode[OutT] | SubWorkflowNode[OutT] | None = None,
        success_policy: SuccessPolicy | None = None,
    ) -> WorkflowSpec[OutT] | WorkflowSpec[WorkflowTerminalResults]:
        """
        Create a validated WorkflowSpec with proper queue and priority resolution.

        Validates queues against app config and resolves priorities using
        effective_priority() to match non-workflow task behavior.

        Args:
            name: Human-readable workflow name
            tasks: List of TaskNode/SubWorkflowNode instances
            on_error: Error handling policy (FAIL or PAUSE)
            output: Explicit output task (optional)
            success_policy: Custom success policy for workflow completion (optional)

        Returns:
            WorkflowSpec ready to start

        Raises:
            ValueError: If any TaskNode.queue is not in app config
        """
        report = ValidationReport('workflow')
        for node in tasks:
            # SubWorkflowNode doesn't have queue/priority - handled at execution time
            if isinstance(node, SubWorkflowNode):
                continue

            # TaskNode: resolve queue and priority
            task = node

            # Resolve queue: explicit override > task decorator > "default"
            resolved_queue = (
                task.queue or getattr(task.fn, 'task_queue_name', None) or 'default'
            )

            # Validate queue against app config
            # In DEFAULT mode, queue must be "default" (or None which resolves to "default")
            # In CUSTOM mode, queue must be in custom_queues list
            queue_valid = True
            if self.config.queue_mode == QueueMode.CUSTOM:
                valid_queues = self.get_valid_queue_names()
                if resolved_queue not in valid_queues:
                    report.add(
                        ConfigurationError(
                            message='TaskNode queue not in app config',
                            code=ErrorCode.TASK_INVALID_QUEUE,
                            notes=[
                                f"TaskNode '{task.name}' has queue '{resolved_queue}'",
                                f'valid queues: {valid_queues}',
                            ],
                            help_text='use one of the configured queue names or add this queue to app config',
                        )
                    )
                    queue_valid = False
            elif resolved_queue != 'default':
                report.add(
                    ConfigurationError(
                        message='TaskNode has non-default queue in DEFAULT mode',
                        code=ErrorCode.CONFIG_INVALID_QUEUE_MODE,
                        notes=[
                            f"TaskNode '{task.name}' has queue '{resolved_queue}'",
                            "app is in DEFAULT mode (only 'default' queue allowed)",
                        ],
                        help_text='either remove queue override or switch to QueueMode.CUSTOM',
                    )
                )
                queue_valid = False

            if queue_valid:
                # Store resolved queue for later use
                task.queue = resolved_queue

                # Resolve priority if not explicitly set
                if task.priority is None:
                    task.priority = effective_priority(self, resolved_queue)

        raise_collected(report)

        # Create spec with validated tasks and broker
        spec = WorkflowSpec(
            name=name,
            tasks=tasks,
            on_error=on_error,
            output=output,
            success_policy=success_policy,
            broker=self.get_broker(),
        )
        if output is None:
            return cast('WorkflowSpec[WorkflowTerminalResults]', spec)
        return spec

    # -------- workflow builder contract --------
    def workflow_builder(
        self,
        *,
        cases: list[dict[str, Any]] | None = None,
    ) -> Callable[[_F], _F]:
        """Decorator registering a workflow builder for check-phase validation.

        Builders are functions that return WorkflowSpec. During `horsies check`,
        registered builders are executed under send suppression to validate the
        workflows they produce.

        Args:
            cases: For parameterized builders (with required params), a list of
                   kwarg dicts to invoke the builder with. Required when the
                   builder has parameters without defaults.
        """

        def decorator(fn: _F) -> _F:
            location = SourceLocation.from_function(fn)
            setattr(fn, _BUILDER_ATTR, True)
            self._workflow_builders.append(
                _WorkflowBuilderMeta(
                    fn=fn,
                    cases=cases or [],
                    location=location,
                ),
            )
            return fn

        return decorator

    def get_workflow_builders(self) -> list[_WorkflowBuilderMeta]:
        """Return registered builders (for tests and introspection)."""
        return list(self._workflow_builders)

    def _check_workflow_builders(self) -> list[HorsiesError]:
        """Execute registered builders under send suppression, collect errors."""
        errors: list[HorsiesError] = []
        prev_suppress = self._suppress_sends
        self.suppress_sends(True)
        try:
            for meta in self._workflow_builders:
                errors.extend(self._run_single_builder(meta))
        finally:
            self.suppress_sends(prev_suppress)
        return errors

    def _run_single_builder(
        self,
        meta: _WorkflowBuilderMeta,
    ) -> list[HorsiesError]:
        """Run a single builder (all cases or auto-invoke) and collect errors."""
        errors: list[HorsiesError] = []
        fn = meta.fn
        fn_name = getattr(fn, '__qualname__', fn.__name__)
        sig = inspect.signature(fn)

        # Determine if builder has required params
        required_params = [
            p
            for p in sig.parameters.values()
            if p.default is inspect.Parameter.empty
            and p.kind not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        ]
        has_required = len(required_params) > 0

        if not has_required:
            # Zero-arg or all-defaults builder: auto-run once
            errors.extend(self._invoke_builder(fn, fn_name, {}, meta.location))
            return errors

        # Parameterized builder: must have cases
        if not meta.cases:
            errors.append(
                ConfigurationError(
                    message=f"parameterized builder '{fn_name}' missing cases",
                    code=ErrorCode.WORKFLOW_CHECK_CASES_REQUIRED,
                    location=meta.location,
                    notes=[
                        f'required params: {[p.name for p in required_params]}',
                    ],
                    help_text=(
                        'add cases= to @app.workflow_builder() with at least one '
                        'dict of kwargs covering each required parameter'
                    ),
                ),
            )
            return errors

        # Validate and run each case
        param_names = set(sig.parameters.keys())
        for idx, case in enumerate(meta.cases):
            if not isinstance(case, dict):  # type: ignore[reportUnnecessaryIsInstance]
                errors.append(
                    ConfigurationError(
                        message=f"builder '{fn_name}' case[{idx}] is not a dict",
                        code=ErrorCode.WORKFLOW_CHECK_CASE_INVALID,
                        location=meta.location,
                        notes=[f'got {type(case).__name__}'],
                        help_text='each case must be a dict of keyword arguments',
                    ),
                )
                continue

            # Check for unknown keys
            unknown_keys = set(case.keys()) - param_names
            if unknown_keys:
                errors.append(
                    ConfigurationError(
                        message=f"builder '{fn_name}' case[{idx}] has unknown keys",
                        code=ErrorCode.WORKFLOW_CHECK_CASE_INVALID,
                        location=meta.location,
                        notes=[
                            f'unknown: {sorted(unknown_keys)}',
                            f'valid params: {sorted(param_names)}',
                        ],
                        help_text='case keys must match builder parameter names',
                    ),
                )
                continue

            # Check for missing required keys
            missing_keys = {p.name for p in required_params} - set(case.keys())
            if missing_keys:
                errors.append(
                    ConfigurationError(
                        message=f"builder '{fn_name}' case[{idx}] missing required keys",
                        code=ErrorCode.WORKFLOW_CHECK_CASE_INVALID,
                        location=meta.location,
                        notes=[
                            f'missing: {sorted(missing_keys)}',
                        ],
                        help_text='each case must provide all required parameters',
                    ),
                )
                continue

            errors.extend(self._invoke_builder(fn, fn_name, case, meta.location))

        return errors

    def _invoke_builder(
        self,
        fn: Callable[..., Any],
        fn_name: str,
        kwargs: dict[str, Any],
        location: SourceLocation | None,
    ) -> list[HorsiesError]:
        """Call a builder with kwargs and collect any errors it produces."""
        errors: list[HorsiesError] = []
        try:
            result = fn(**kwargs)
            if not self._is_workflow_spec_like(result):
                case_summary = f' with {kwargs}' if kwargs else ''
                errors.append(
                    _no_location(
                        ConfigurationError(
                            message=f"builder '{fn_name}' returned non-WorkflowSpec{case_summary}",
                            code=ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
                            location=location,
                            notes=[f'got {type(result).__name__}'],
                            help_text='workflow builders must return a WorkflowSpec instance',
                        ),
                    ),
                )
        except MultipleValidationErrors as exc:
            errors.extend(exc.report.errors)
        except HorsiesError as exc:
            errors.append(exc)
        except Exception as exc:
            case_summary = f' with {kwargs}' if kwargs else ''
            errors.append(
                _no_location(
                    ConfigurationError(
                        message=f"builder '{fn_name}' raised an unexpected exception{case_summary}",
                        code=ErrorCode.WORKFLOW_CHECK_BUILDER_EXCEPTION,
                        location=location,
                        notes=[f'{type(exc).__name__}: {exc}'],
                        help_text='workflow builders must not raise non-horsies exceptions during check',
                    ),
                ),
            )
        return errors

    def _check_undecorated_builders(self) -> list[HorsiesError]:
        """Detect top-level functions returning WorkflowSpec without decorator."""
        errors: list[HorsiesError] = []

        for module_path in self._discovered_task_modules:
            module = self._resolve_imported_module(module_path)
            if module is None:
                continue

            try:
                hints_by_name = self._collect_module_function_return_hints(module)
            except Exception:
                # If we can't resolve type hints (forward refs, etc.), skip module
                continue

            for fn_name, return_hint in hints_by_name.items():
                fn_obj = getattr(module, fn_name)

                # Skip if already decorated
                if getattr(fn_obj, _BUILDER_ATTR, False):
                    continue

                # Skip if explicitly opted out
                if getattr(fn_obj, _NO_CHECK_ATTR, False):
                    continue

                if self._hint_is_workflow_spec(return_hint):
                    location = SourceLocation.from_function(fn_obj)
                    errors.append(
                        ConfigurationError(
                            message=f"undecorated workflow builder '{fn_name}'",
                            code=ErrorCode.WORKFLOW_CHECK_UNDECORATED_BUILDER,
                            location=location,
                            notes=[
                                f"function '{fn_name}' returns WorkflowSpec but is not decorated with @app.workflow_builder",
                            ],
                            help_text=(
                                'add @app.workflow_builder() to register this builder for check validation;\n'
                                f'or set {fn_name}.__horsies_no_check__ = True to suppress this warning'
                            ),
                        ),
                    )

        return errors

    @staticmethod
    def _resolve_imported_module(module_path: str) -> Any | None:
        """Look up an already-imported module by path or dotted name."""
        if module_path.endswith('.py') or os.path.sep in module_path:
            abs_path = os.path.realpath(module_path)
            # Find module in sys.modules by filename
            for mod in sys.modules.values():
                mod_file = getattr(mod, '__file__', None)
                if mod_file is not None and os.path.realpath(mod_file) == abs_path:
                    return mod
            return None
        return sys.modules.get(module_path)

    @staticmethod
    def _collect_module_function_return_hints(
        module: Any,
    ) -> dict[str, Any]:
        """Get return type hints for top-level functions in a module."""
        result: dict[str, Any] = {}
        for name, obj in inspect.getmembers(module, inspect.isfunction):
            # Only top-level functions defined in this module
            if getattr(obj, '__module__', None) != module.__name__:
                continue
            try:
                hints = inspect.get_annotations(obj, eval_str=True)
            except Exception:
                continue
            return_hint = hints.get('return')
            if return_hint is not None:
                result[name] = return_hint
        return result

    @staticmethod
    def _hint_is_workflow_spec(hint: Any) -> bool:
        """Check if a type hint resolves to WorkflowSpec[...] or bare WorkflowSpec."""
        from typing import get_origin

        if hint is WorkflowSpec:
            return True
        origin = get_origin(hint)
        if origin is WorkflowSpec:
            return True
        return False

    @staticmethod
    def _is_workflow_spec_like(value: Any) -> bool:
        """Return True for WorkflowSpec instances and test doubles with spec shape."""
        if isinstance(value, WorkflowSpec):
            return True
        required_attrs = ('name', 'tasks', 'on_error', 'broker')
        return all(hasattr(value, attr) for attr in required_attrs)
