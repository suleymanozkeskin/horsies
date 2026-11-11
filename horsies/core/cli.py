# app/core/cli.py
"""
CLI for horsies worker, scheduler, and check commands.

Module path resolution follows Celery's approach:
1. User provides dotted module path: `horsies worker app.configs.horsies:app`
2. User is responsible for PYTHONPATH / running from correct directory
3. Convenience: if cwd has pyproject.toml, we add cwd to sys.path
"""

import argparse
import asyncio
import importlib
import ipaddress
import logging
import os
import random
import signal
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, NoReturn, assert_never

from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.banner import print_banner
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import ListenerDisabled, ListenerEnabled
from horsies.core.brokers.result_types import BrokerOperationError, broker_error_cause
from horsies.core.types.error_detail import NoCause
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    ValidationReport,
)
from horsies.core.logging import get_logger
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.child_sizing import DEFAULT_TASK_SEND_POOL_CONNECTIONS
from horsies.core.models.resilience import WorkerResilienceConfig
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    IsolatedPool,
    MemoryLimit,
    NoIsolatedChildren,
    NoMemoryLimit,
    NoSharedAsyncChildren,
    NoSharedThreadChildren,
    NoTaskLimit,
    ResolvedPool,
    SharedAsyncChildren,
    SharedThreadChildren,
    TaskLimit,
    WorkerProfile,
    describe_worker_profile,
)
from horsies.core.scheduler import Scheduler
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.profile_fit import (
    CONNECTIONS_HELP,
    CgroupLimit,
    CgroupUnreadable,
    ConnectionsExceedServer,
    ConnectionsFit,
    MemoryLimitsExceedMachine,
    MemoryLimitsFit,
    MemoryLimitsNotSet,
    NoCgroupLimit,
    ServerConnectionLimit,
    ServerConnectionLimitUnreadable,
    UsableMemory,
    UsableMemoryUnreadable,
    compare_connections,
    compare_memory_limits,
    read_cgroup_memory_limit,
    read_machine_memory,
    read_server_connection_limit,
    usable_memory,
)
from horsies.core.worker.worker import Worker, WorkerConfig
from horsies.core.types.result import Err, Ok, Result, is_err, is_ok
from horsies.core.utils.imports import (
    import_file_path,
    setup_sys_path_from_cwd,
)
from horsies.core.utils.url import to_psycopg_url


def _raise_schema_error(err: BrokerOperationError) -> NoReturn:
    """Raise ``RuntimeError(err.message)``, chained from the cause when there is one."""
    match broker_error_cause(err):
        case NoCause():
            raise RuntimeError(err.message)
        case cause:
            raise RuntimeError(err.message) from cause


async def _ensure_schema_with_retry(
    broker: PostgresBroker,
    resilience: WorkerResilienceConfig,
    logger: logging.Logger,
) -> None:
    """Initialize the schema, backoff-retrying retryable broker errors.

    Raises:
        RuntimeError: a non-retryable initialization error, or a retryable
            one after the retry budget — chained from the broker error.
            Both command paths log it and exit 1; nothing was claimed or
            started yet, so there is no partial state to recover.
    """
    attempts = 0
    while True:
        result = await broker.ensure_schema_initialized()
        if is_ok(result):
            return

        err = result.err_value
        if not err.retryable:
            _raise_schema_error(err)

        attempts += 1
        match resilience.db_retry_max_attempts:
            case 0:
                should_retry = True
            case _:
                should_retry = attempts <= resilience.db_retry_max_attempts

        if not should_retry:
            logger.error(
                f'Database initialization failed after {attempts} attempts: {err.message}'
            )
            _raise_schema_error(err)

        exponent = max(0, attempts - 1)
        base_ms = min(
            resilience.db_retry_max_ms,
            int(resilience.db_retry_initial_ms * (2**exponent)),
        )
        jitter_range = base_ms * 0.25
        delay_ms = base_ms + random.uniform(-jitter_range, jitter_range)
        delay_s = max(0.1, delay_ms / 1000.0)
        logger.error(
            f'Database initialization failed: {err.message}. Retrying in {delay_s:.1f}s '
            f'(attempt {attempts}/{resilience.db_retry_max_attempts or "inf"})'
        )
        await asyncio.sleep(delay_s)


async def _close_broker(broker: Any, logger: logging.Logger) -> None:
    close_result = await broker.close_async()
    if is_err(close_result):
        logger.error(f'Broker close failed: {close_result.err_value.message}')


def _resolve_module_argument(args: argparse.Namespace) -> str:
    """Return module path from --module or positional, error if missing or conflicting."""
    module_flag = getattr(args, 'module', None)
    module_pos = getattr(args, 'module_pos', None)
    if module_flag and module_pos and module_flag != module_pos:
        raise ConfigurationError(
            message='conflicting module paths provided',
            code=ErrorCode.CLI_INVALID_ARGS,
            notes=[
                f'--module={module_flag!r} conflicts with positional {module_pos!r}',
                'pass the module path exactly once (either as -m/--module or positionally)',
            ],
            help_text=(
                'provide the module path exactly once, e.g.:\n'
                '  horsies worker app.configs.horsies:app  (positional, recommended)\n'
                '  horsies worker -m app.configs.horsies:app  (flag)'
            ),
        )
    module_path = module_flag or module_pos
    if not module_path:
        raise ConfigurationError(
            message='module path is required',
            code=ErrorCode.CLI_INVALID_ARGS,
            notes=['no --module flag or positional module argument provided'],
            help_text=(
                'provide module path in one of these formats:\n'
                '  horsies worker app.configs.horsies:app  (recommended)\n'
                '  horsies worker app/configs/horsies.py:app  (file path)\n'
                '  horsies worker app.configs.horsies  (auto-discover app variable)'
            ),
        )
    return module_path


def _parse_locator(locator: str) -> tuple[str, str | None]:
    """
    Parse a module locator into (module_path, attribute_name).

    Formats:
    - "app.configs.horsies:app" -> ("app.configs.horsies", "app")
    - "app.configs.horsies" -> ("app.configs.horsies", None)
    - "/path/to/file.py:app" -> ("/path/to/file.py", "app")
    - "app/configs/horsies.py" -> ("app/configs/horsies.py", None)
    """
    if ':' in locator:
        module_part, attr = locator.rsplit(':', 1)
        return (module_part, attr)
    return (locator, None)


def _is_file_path(path: str) -> bool:
    """Check if path looks like a file path (vs dotted module path)."""
    return path.endswith('.py') or os.path.sep in path or '/' in path


def _build_queue_settings(app: Horsies) -> tuple[dict[str, int], dict[str, int]]:
    """Build (queue_priorities, queue_max_concurrency) for CUSTOM mode.

    Uncapped queues (``max_concurrency=None``) are omitted from the
    concurrency map: the worker's claim pass only counts and limits
    queues present in it.
    """
    queue_priorities: dict[str, int] = {}
    queue_max_concurrency: dict[str, int] = {}
    if app.config.queue_mode.name == 'CUSTOM' and app.config.custom_queues:
        for q in app.config.custom_queues:
            queue_priorities[q.name] = q.priority
            if q.max_concurrency is not None:
                queue_max_concurrency[q.name] = q.max_concurrency
    return queue_priorities, queue_max_concurrency


def discover_app(module_locator: str) -> tuple[Horsies, str, str, str | None]:
    """
    Import module and discover horsies instance.

    Supports two formats (like Celery):
    1. Dotted module path: "app.configs.horsies:app" or "app.configs.horsies"
    2. File path: "app/configs/horsies.py:app" or "app/configs/horsies.py"

    The pyproject.toml convenience is applied before import:
    if cwd contains pyproject.toml, cwd is added to sys.path.

    Returns:
        (app_instance, variable_name, module_name, sys_path_root)

    Raises:
        ConfigurationError: locator invalid, module/file missing, attribute
            missing, not a Horsies instance, or ambiguous discovery.
        Exception: import errors from the user's module body propagate
            unwrapped. Every command catches both at its boundary and
            exits 1.
    """
    logger = get_logger('cli')

    # Convenience: add project root to sys.path if pyproject.toml found
    project_root = setup_sys_path_from_cwd()
    if project_root:
        logger.info(f'Added project root to sys.path: {project_root}')

    # Parse the locator
    module_path, attr_name = _parse_locator(module_locator)

    # Import the module
    if _is_file_path(module_path):
        # File path - normalize and import
        if not module_path.endswith('.py'):
            module_path += '.py'
        file_path = os.path.realpath(module_path)

        if not os.path.exists(file_path):
            # Detect ambiguous "dotted.module.py" pattern
            stem = module_path.removesuffix('.py')
            if '.' in stem and '/' not in stem and os.path.sep not in stem:
                attr_hint = attr_name if attr_name else '<app_name>'
                dotted_form = f'{stem}:{attr_hint}'
                file_form = f'{stem.replace(".", "/")}.py:{attr_hint}'
                help_lines = (
                    f'use dotted module path: {dotted_form}\n'
                    f'or use file path:       {file_form}'
                )
                if not attr_name:
                    help_lines += '\nwhere <app_name> is the variable name of your Horsies instance'
                raise ConfigurationError(
                    message=f"ambiguous module locator: '{module_locator}'",
                    code=ErrorCode.CLI_INVALID_ARGS,
                    notes=[
                        f"'{module_path}' mixes dotted module notation with a .py file extension",
                    ],
                    help_text=help_lines,
                )
            raise ConfigurationError(
                message=f'module file not found: {file_path}',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[f"resolved path: '{file_path}'"],
                help_text='check that the file path is correct and the file exists',
            )

        # import_file_path adds parent directory to sys.path
        try:
            module = import_file_path(file_path)
        except HorsiesError:
            raise
        except ImportError as e:
            raise ConfigurationError(
                message=f'failed to import module: {file_path}',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[str(e)],
                help_text='check that the module and its dependencies are importable',
            )
        except Exception as e:
            raise ConfigurationError(
                message=f'error while importing module: {file_path}',
                code=ErrorCode.MODULE_EXEC_ERROR,
                notes=[f'{type(e).__name__}: {e}'],
                help_text=(
                    'the module was found but raised an error during import;\n'
                    'check the module-level code for bugs'
                ),
            )
        module_name = module.__name__
        sys_path_root = os.path.dirname(file_path)
    else:
        # Dotted module path - use standard import
        try:
            module = importlib.import_module(module_path)
        except HorsiesError:
            raise
        except ModuleNotFoundError as e:
            raise ConfigurationError(
                message=f'module not found: {module_path}',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[
                    str(e),
                    f'sys.path: {sys.path[:5]}...',
                ],
                help_text=(
                    'ensure you are running from the correct directory\n'
                    'or set PYTHONPATH to include your project root'
                ),
            )
        except ImportError as e:
            raise ConfigurationError(
                message=f'failed to import module: {module_path}',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[str(e)],
                help_text=(
                    'ensure the module and its dependencies are importable;\n'
                    'check PYTHONPATH or run from the project root'
                ),
            )
        except Exception as e:
            raise ConfigurationError(
                message=f'error while importing module: {module_path}',
                code=ErrorCode.MODULE_EXEC_ERROR,
                notes=[f'{type(e).__name__}: {e}'],
                help_text=(
                    'the module was found but raised an error during import;\n'
                    'check the module-level code for bugs'
                ),
            )
        module_name = module_path
        sys_path_root = project_root

    # Find horsies instance
    if attr_name:
        # Explicit attribute name
        if not hasattr(module, attr_name):
            raise ConfigurationError(
                message=f"module '{module_name}' has no attribute '{attr_name}'",
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[
                    f"available attributes: {[n for n in dir(module) if not n.startswith('_')]}"
                ],
                help_text='check the variable name after the colon in your module locator',
            )
        obj = getattr(module, attr_name)
        if not isinstance(obj, Horsies):
            raise ConfigurationError(
                message=f"'{attr_name}' in module '{module_name}' is not a Horsies instance",
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[f'got {type(obj).__name__} instead of Horsies'],
                help_text='ensure the variable points to a Horsies() instance',
            )
        app = obj
        var_name = attr_name
    else:
        # Auto-discover horsies instance
        app_instances: list[tuple[Horsies, str]] = []
        for name in dir(module):
            if not name.startswith('_'):
                obj = getattr(module, name)
                if isinstance(obj, Horsies):
                    app_instances.append((obj, name))

        if not app_instances:
            raise ConfigurationError(
                message=f'no Horsies instance found in {module_name}',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[f"scanned module '{module_name}' for Horsies instances"],
                help_text='specify the variable name: module.path:variable',
            )

        if len(app_instances) > 1:
            var_names = [name for _, name in app_instances]
            raise ConfigurationError(
                message=f'multiple Horsies instances found in {module_name}',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[f'found: {var_names}'],
                help_text='specify which one: module.path:variable',
            )

        app, var_name = app_instances[0]

    logger.info(f"Discovered horsies '{var_name}' from {module_name}")
    return app, var_name, module_name, sys_path_root


def setup_logging(loglevel: str) -> None:
    """Configure logging level globally."""
    from horsies.core.logging import configure_logging

    level = getattr(logging, loglevel.upper(), logging.INFO)
    configure_logging(level)


@dataclass(frozen=True, slots=True)
class SelectedProfile:
    """The worker profile that ``--profile`` names."""

    name: str
    profile: WorkerProfile


@dataclass(frozen=True, slots=True)
class ProfileNotGiven:
    """``--profile`` was not passed. There is no default profile."""

    defined: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProfileUnknown:
    """``--profile`` names no profile of the app."""

    requested: str
    defined: tuple[str, ...]


def select_worker_profile(
    profiles: Mapping[str, WorkerProfile],
    requested: str | None,
) -> Result[SelectedProfile, ProfileNotGiven | ProfileUnknown]:
    """Find the profile that ``--profile`` names.

    ``requested`` is the ``--profile`` value as argparse gives it: ``None``
    when the option was not passed. Both errors carry the defined names.
    """
    defined = tuple(sorted(profiles))
    match requested:
        case None:
            return Err(ProfileNotGiven(defined=defined))
        case str() as name if name in profiles:
            return Ok(SelectedProfile(name=name, profile=profiles[name]))
        case str() as name:
            return Err(ProfileUnknown(requested=name, defined=defined))


def _defined_profiles_note(defined: tuple[str, ...]) -> str:
    match defined:
        case ():
            return (
                'the app defines no worker profiles: AppConfig.worker_profiles is empty'
            )
        case _:
            return f'defined profiles: {", ".join(defined)}'


def _profile_help(defined: tuple[str, ...]) -> str:
    match defined:
        case ():
            return (
                'add a profile to the app config, for example:\n'
                '  AppConfig(..., worker_profiles={\n'
                "      'default': WorkerProfile(isolated=IsolatedChildren(count=4)),\n"
                '  })\n'
                'then start the worker with: horsies worker <app> --profile default'
            )
        case _:
            return f'start the worker with: horsies worker <app> --profile {defined[0]}'


def _profile_selection_error(
    error: ProfileNotGiven | ProfileUnknown,
) -> ConfigurationError:
    """The startup error for a missing or unknown ``--profile``."""
    match error:
        case ProfileNotGiven(defined=defined):
            config_error = ConfigurationError(
                message='--profile is required',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[
                    'a worker takes its settings from a named profile in '
                    'AppConfig.worker_profiles; there is no default profile',
                    _defined_profiles_note(defined),
                ],
                help_text=_profile_help(defined),
            )
        case ProfileUnknown(requested=requested, defined=defined):
            config_error = ConfigurationError(
                message=f'unknown worker profile {requested!r}',
                code=ErrorCode.CLI_INVALID_ARGS,
                notes=[_defined_profiles_note(defined)],
                help_text=_profile_help(defined),
            )
        case _ as unreachable:
            assert_never(unreachable)
    # The cause is a command-line argument, not a line of user code.
    config_error.location = None
    return config_error


@dataclass(frozen=True, slots=True)
class ExecutionSettings:
    """A profile in the encoding of ``WorkerConfig``.

    ``WorkerConfig``, the slot pool and the claim function read 0 as no task
    limit and ``None`` as no memory limit. The profile has named variants
    for those; this is the one place that turns them into that encoding.
    ``processes`` is the number of isolated children, 0 when the profile
    has none. ``isolated_pool`` is the pool of each isolated child, the
    default ``IsolatedPool`` when there are none; ``isolated_task_send_pool``
    is the task-send pool of each isolated child, the default when there are
    none. ``shared_async`` and ``shared_thread`` keep their typed form.
    """

    processes: int
    max_tasks_per_child: int
    max_memory_per_child_mb: int | None
    isolated_pool: ResolvedPool
    isolated_task_send_pool: int
    max_claim_batch: int
    max_claim_per_worker: int
    drain_timeout_s: float
    child_preload: ChildPreload
    shared_async: SharedAsyncChildren | NoSharedAsyncChildren
    shared_thread: SharedThreadChildren | NoSharedThreadChildren


def _tasks_or_zero(limit: TaskLimit | NoTaskLimit) -> int:
    match limit:
        case TaskLimit(tasks=tasks):
            return tasks
        case NoTaskLimit():
            return 0
        case _ as unreachable:
            assert_never(unreachable)


def _mb_or_none(limit: MemoryLimit | NoMemoryLimit) -> int | None:
    match limit:
        case MemoryLimit(mb=mb):
            return mb
        case NoMemoryLimit():
            return None
        case _ as unreachable:
            assert_never(unreachable)


def execution_settings(profile: WorkerProfile) -> ExecutionSettings:
    """Turn a validated profile into the settings of ``WorkerConfig``.

    A profile with no isolated children gives ``processes`` 0, no task
    limit, no memory limit and the default pool and task-send pool for
    isolated children. AppConfig validation
    refuses a profile with no children of any mode, so such a profile here
    is a defect: ``AssertionError``.
    """
    if profile.child_count() < 1:
        raise AssertionError(
            'AppConfig validation refuses a worker profile with no children',
        )
    match profile.isolated:
        case IsolatedChildren(count=count, recycle=recycle) as isolated:
            processes = count
            max_tasks_per_child = _tasks_or_zero(recycle.after)
            max_memory_per_child_mb = _mb_or_none(recycle.above)
            isolated_pool = isolated.resolved_pool()
            isolated_task_send_pool = isolated.task_send_pool.connections
        case NoIsolatedChildren():
            processes = 0
            max_tasks_per_child = 0
            max_memory_per_child_mb = None
            isolated_pool = IsolatedPool().resolved()
            isolated_task_send_pool = DEFAULT_TASK_SEND_POOL_CONNECTIONS
        case _ as unreachable:
            assert_never(unreachable)
    return ExecutionSettings(
        processes=processes,
        max_tasks_per_child=max_tasks_per_child,
        max_memory_per_child_mb=max_memory_per_child_mb,
        isolated_pool=isolated_pool,
        isolated_task_send_pool=isolated_task_send_pool,
        max_claim_batch=_tasks_or_zero(profile.claim.per_queue_per_pass),
        max_claim_per_worker=_tasks_or_zero(profile.claim.claimed_per_worker),
        drain_timeout_s=profile.drain_timeout.total_seconds(),
        child_preload=profile.preload,
        shared_async=profile.shared_async,
        shared_thread=profile.shared_thread,
    )


def _read_usable_memory(
    selected: SelectedProfile,
    logger: logging.Logger,
) -> UsableMemory | UsableMemoryUnreadable:
    """Read the host RAM and the cgroup limit, and keep the smaller.

    Warns when a cgroup limit file exists but cannot be read: the host RAM
    is then the only bound, and inside a container it can be far too high.
    """
    cgroup = read_cgroup_memory_limit()
    match cgroup:
        case CgroupUnreadable(reason=reason):
            logger.warning(
                'Worker profile %r: the container memory limit could not be '
                'read (%s); the per-child memory limits are compared with the '
                'host RAM',
                selected.name,
                reason,
            )
        case CgroupLimit() | NoCgroupLimit():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    return usable_memory(read_machine_memory(), cgroup)


def _log_memory_fit(selected: SelectedProfile, logger: logging.Logger) -> None:
    """Warn when the per-child memory limits add up to more than the memory
    that the worker can use: the container limit, or the host RAM.

    A warning, not a refusal: a deployment can move between machine sizes.
    The worker starts in every outcome.
    """
    match _read_usable_memory(selected, logger):
        case UsableMemoryUnreadable(reason=reason):
            logger.warning(
                'Worker profile %r: the memory of the machine could not be read '
                '(%s); the per-child memory limits were not compared with it',
                selected.name,
                reason,
            )
        case UsableMemory(source=source) as usable:
            match compare_memory_limits(selected.profile, usable):
                case MemoryLimitsExceedMachine(total_mb=total_mb, machine_mb=usable_mb):
                    logger.warning(
                        'Worker profile %r: the per-child memory limits add up '
                        'to %s MB, above the %s MB %s. Children can use more '
                        'memory than the worker has before they are recycled.',
                        selected.name,
                        total_mb,
                        usable_mb,
                        source.value,
                    )
                case MemoryLimitsFit(total_mb=total_mb, machine_mb=usable_mb):
                    logger.info(
                        'Worker profile %r: per-child memory limits add up to '
                        '%s MB of the %s MB %s',
                        selected.name,
                        total_mb,
                        usable_mb,
                        source.value,
                    )
                case MemoryLimitsNotSet():
                    logger.info(
                        'Worker profile %r: no per-child memory limit, so '
                        'there is no sum to compare with the memory',
                        selected.name,
                    )
                case _ as unreachable:
                    assert_never(unreachable)
        case _ as unreachable_memory:
            assert_never(unreachable_memory)


def _log_connection_limit(
    selected: SelectedProfile,
    broker: PostgresConfig,
    limit: ServerConnectionLimit,
    logger: logging.Logger,
) -> None:
    match compare_connections(selected.profile, broker, limit):
        case ConnectionsExceedServer() as exceed:
            logger.warning(
                'Worker profile %r: %s. %s; %s. To fix: %s',
                selected.name,
                exceed.describe(),
                exceed.need.formula(),
                exceed.need.task_sends_note(),
                CONNECTIONS_HELP,
            )
        case ConnectionsFit() as fit:
            logger.info('Worker profile %r: %s', selected.name, fit.describe())
        case _ as unreachable:
            assert_never(unreachable)


async def _log_connection_fit(
    selected: SelectedProfile,
    broker: PostgresConfig,
    session_factory: Callable[[], AsyncSession],
    logger: logging.Logger,
) -> None:
    """Warn when one worker can open more connections than the server allows.

    Runs at boot, before any child starts or any row is claimed. A warning,
    not a refusal; ``horsies check --live`` reports the same fact as an
    error. Through a transaction pooler the pooled connections do not map to
    server connections, so nothing is compared.
    """
    match broker.pgbouncer_transaction_mode:
        case True:
            logger.info(
                'Worker profile %r: connections go through a transaction '
                'pooler; the server connection limit is not compared',
                selected.name,
            )
            return
        case False:
            pass
    match await read_server_connection_limit(session_factory):
        case ServerConnectionLimitUnreadable(reason=reason):
            logger.warning(
                'Worker profile %r: the server connection limit could not be '
                'read (%s); the connections of this worker were not compared '
                'with it',
                selected.name,
                reason,
            )
        case ServerConnectionLimit() as limit:
            _log_connection_limit(selected, broker, limit, logger)
        case _ as unreachable:
            assert_never(unreachable)


def worker_command(args: argparse.Namespace) -> None:
    """Handle worker command.

    Exit contract (supervisor-facing): 0 only on operator-initiated stop
    (signal/KeyboardInterrupt); any failure — discovery, validation,
    schema init, worker crash, startup timeout — logs and exits 1.
    """
    logger = get_logger('cli')

    # Setup logging first
    loglevel: str = args.loglevel
    setup_logging(loglevel)
    logger.info(f'Starting horsies worker with loglevel={loglevel}')

    # Discover app
    try:
        module_locator: str = _resolve_module_argument(args)
        app, var_name, module_name, sys_path_root = discover_app(module_locator)
        app.set_role('worker')
    except HorsiesError as e:
        logger.error(str(e))
        sys.exit(1)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f'Failed to discover app: {e}')
        sys.exit(1)

    # The profile is chosen before anything starts. AppConfig validation
    # already checked every profile when the app module built the config.
    match select_worker_profile(app.config.worker_profiles, args.profile):
        case Err(selection_error):
            report = ValidationReport('worker-startup')
            report.add(_profile_selection_error(selection_error))
            print(report.format_rust_style(), file=sys.stderr)
            sys.exit(1)
        case Ok(selected):
            pass
    settings = execution_settings(selected.profile)
    logger.info(
        f'Worker profile {selected.name!r}: '
        f'{describe_worker_profile(selected.profile)}'
    )

    # Strict startup validation before worker accepts tasks.
    startup_errors = app.check(live=False)
    if startup_errors:
        report = ValidationReport('worker-startup')
        for error in startup_errors:
            report.add(error)
        print(report.format_rust_style(), file=sys.stderr)
        sys.exit(1)

    # Build the worker broker config directly from the app config. Avoid
    # app.get_broker() here: user apps may also use a larger producer/web broker
    # pool, and any broker opened before child forking must not be inherited by
    # child workers.
    try:
        postgres_config = app.config.broker
        worker_postgres_config = postgres_config.worker_runtime_config()
    except HorsiesError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.error(f'Failed to initialize worker broker config: {e}')
        sys.exit(1)

    # Get queues from app config
    queues: list[str] = app.get_valid_queue_names()
    logger.info(f'Worker will process queues: {queues}')

    # Build per-queue settings for CUSTOM mode
    try:
        queue_priorities, queue_max_concurrency = _build_queue_settings(app)
    except Exception as exc:
        logger.error(f'Failed to parse custom queue config: {exc}')
        queue_priorities, queue_max_concurrency = {}, {}

    # Get discovered task modules
    discovered_modules = app.get_discovered_task_modules()
    if discovered_modules:
        logger.info('Using discovered task modules')
    else:
        logger.warning('No task modules discovered.')

    # Create worker config
    log_level_int = getattr(logging, loglevel.upper(), logging.INFO)

    # Build sys_path_roots
    sys_path_roots: list[str] = []
    if sys_path_root:
        sys_path_roots.append(sys_path_root)

    # Build app_locator - prefer module path format
    if _is_file_path(module_locator.split(':')[0]):
        # File path - use absolute path
        file_path = module_locator.split(':')[0]
        if not file_path.endswith('.py'):
            file_path += '.py'
        app_locator = f'{os.path.realpath(file_path)}:{var_name}'
    else:
        # Module path - use as-is
        app_locator = f'{module_name}:{var_name}'

    worker_config = WorkerConfig(
        dsn=postgres_config.database_url.get_secret_value(),
        session_dsn=postgres_config.effective_session_database_url,
        psycopg_dsn=to_psycopg_url(postgres_config.effective_session_database_url),
        pgbouncer_transaction_mode=postgres_config.pgbouncer_transaction_mode,
        parent_pool_size=worker_postgres_config.pool_size,
        parent_max_overflow=worker_postgres_config.max_overflow,
        isolated_pool=settings.isolated_pool,
        isolated_task_send_pool=settings.isolated_task_send_pool,
        child_connect_kwargs=postgres_config.pooled_connect_args,
        queues=queues,
        processes=settings.processes,
        app_locator=app_locator,
        sys_path_roots=sys_path_roots,
        imports=discovered_modules,
        queue_priorities=queue_priorities,
        queue_max_concurrency=queue_max_concurrency,
        cluster_wide_cap=app.config.cluster_wide_cap,
        prefetch_buffer=app.config.prefetch_buffer,
        claim_lease_ms=app.config.claim_lease_ms,
        max_claim_renew_age_ms=app.config.max_claim_renew_age_ms,
        max_claim_batch=settings.max_claim_batch,
        max_claim_per_worker=settings.max_claim_per_worker,
        max_tasks_per_child=settings.max_tasks_per_child,
        max_memory_per_child_mb=settings.max_memory_per_child_mb,
        drain_timeout_s=settings.drain_timeout_s,
        child_preload=settings.child_preload,
        shared_async=settings.shared_async,
        shared_thread=settings.shared_thread,
        recovery_config=app.config.recovery,
        retention_config=app.config.retention,
        resilience_config=app.config.resilience,
        loglevel=log_level_int,
    )

    # Machine facts: a warning, never a refusal.
    _log_memory_fit(selected, logger)

    # Print startup banner
    app.import_task_modules()  # Import tasks so they show in banner
    print_banner(app, role='worker', show_tasks=True)

    # Start worker
    logger.info('Starting worker...')
    try:

        async def run_worker() -> None:
            loop = asyncio.get_running_loop()
            run_task = asyncio.current_task()
            worker: Worker | None = None

            def signal_handler() -> None:
                logger.info('Received interrupt signal, stopping worker...')
                if worker is None:
                    if run_task is not None:
                        run_task.cancel()
                    return
                worker.request_stop()

            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, signal_handler)
                except NotImplementedError:
                    pass

            broker: PostgresBroker | None = None
            try:
                schema_broker = PostgresBroker(worker_postgres_config)
                schema_broker.app = app
                try:
                    logger.info(
                        'Ensuring Postgres schema and triggers are initialized...'
                    )
                    await _ensure_schema_with_retry(
                        schema_broker,
                        app.config.resilience,
                        logger,
                    )
                except asyncio.CancelledError:
                    logger.info('Worker startup interrupted')
                    return
                except Exception as e:
                    logger.error(f'Failed to initialize database schema: {e}')
                    raise
                finally:
                    await _close_broker(schema_broker, logger)

                broker = PostgresBroker(worker_postgres_config)
                broker.app = app
                # A fresh broker on this loop: reaching the loop limit is a programmer error.
                match await broker.select_resources():
                    case Err(loop_limit):
                        raise AssertionError(
                            f'worker broker resources: {loop_limit.describe()}',
                        )
                    case Ok(resources):
                        pass
                match resources.listener:
                    case ListenerEnabled(listener=listener):
                        pass
                    case ListenerDisabled():
                        raise AssertionError(
                            'worker broker has no listener; a broker built '
                            'without assume_initialized always owns one',
                        )
                await _log_connection_fit(
                    selected,
                    postgres_config,
                    resources.session_factory,
                    logger,
                )
                worker = Worker(
                    resources.session_factory,
                    listener,
                    worker_config,
                    broker=broker,
                )
                await worker.run_forever()
            finally:
                if broker is not None:
                    await _close_broker(broker, logger)

        try:
            asyncio.run(run_worker())
        except KeyboardInterrupt:
            logger.info('Worker interrupted by user')
            return
        except asyncio.TimeoutError:
            # Retry-exhausted startup timeout: the worker never came up.
            # Exit non-zero — a clean exit here told the supervisor nothing
            # went wrong, so nothing restarted the worker.
            logger.error('Worker startup timed out')
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info('Worker interrupted by user')
        return
    except Exception as e:
        logger.error(f'Worker failed: {e}')
        sys.exit(1)


def scheduler_command(args: argparse.Namespace) -> None:
    """Handle scheduler command.

    Exit contract (supervisor-facing): 0 only on operator-initiated stop;
    any failure — discovery, config/validation, schema init, scheduler
    crash — logs and exits 1.
    """
    logger = get_logger('cli')

    # Setup logging first
    loglevel: str = args.loglevel
    setup_logging(loglevel)
    logger.info(f'Starting scheduler with loglevel={loglevel}')

    # Discover app
    try:
        module_locator: str = _resolve_module_argument(args)
        app, _var_name, _module_name, _sys_path_root = discover_app(module_locator)
        app.set_role('scheduler')
    except HorsiesError as e:
        logger.error(str(e))
        sys.exit(1)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f'Failed to discover app: {e}')
        sys.exit(1)

    # Validate schedule config
    if not app.config.schedule:
        logger.error('Schedule configuration not found in app config')
        sys.exit(1)

    if not app.config.schedule.enabled:
        logger.warning('Scheduler is disabled in config')
        sys.exit(1)

    # Strict startup validation before scheduler starts.
    startup_errors = app.check(live=False)
    if startup_errors:
        report = ValidationReport('scheduler-startup')
        for error in startup_errors:
            report.add(error)
        print(report.format_rust_style(), file=sys.stderr)
        sys.exit(1)

    # Import task modules for validation
    discovered_modules = app.get_discovered_task_modules()
    if discovered_modules:
        logger.info(f'Task modules available: {discovered_modules}')
        for module in discovered_modules:
            try:
                if _is_file_path(module):
                    import_file_path(os.path.abspath(module))
                else:
                    importlib.import_module(module)
            except Exception as e:
                logger.warning(f"Failed to import task module '{module}': {e}")

    schedule_count = len(app.config.schedule.schedules)
    enabled_count = sum(1 for s in app.config.schedule.schedules if s.enabled)

    # Print startup banner
    print_banner(app, role='scheduler', show_tasks=True)

    # Start scheduler
    logger.info(f'Scheduler: {enabled_count}/{schedule_count} schedules enabled')
    try:

        async def run_scheduler() -> None:
            loop = asyncio.get_running_loop()
            run_task = asyncio.current_task()
            broker: Any | None = None
            scheduler: Scheduler | None = None

            def signal_handler() -> None:
                logger.info('Received interrupt signal, stopping scheduler...')
                if scheduler is None:
                    if run_task is not None:
                        run_task.cancel()
                    return
                scheduler.request_stop()

            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, signal_handler)
                except NotImplementedError:
                    pass

            try:
                # Ensure schema exists with retry, same as worker path
                broker = app.get_broker()
                logger.info('Ensuring Postgres schema and triggers are initialized...')
                await _ensure_schema_with_retry(
                    broker,
                    app.config.resilience,
                    logger,
                )

            except asyncio.CancelledError:
                logger.info('Scheduler startup interrupted')
                if broker is not None:
                    await _close_broker(broker, logger)
                return
            except Exception as e:
                logger.error(f'Scheduler error: {e}', exc_info=True)
                if broker is not None:
                    await _close_broker(broker, logger)
                raise

            try:
                scheduler = Scheduler(app)
                await scheduler.run_forever()
            except Exception as e:
                logger.error(f'Scheduler error: {e}', exc_info=True)
                if scheduler is None and broker is not None:
                    await _close_broker(broker, logger)
                raise

        try:
            asyncio.run(run_scheduler())
        except KeyboardInterrupt:
            logger.info('Scheduler interrupted by user')
            return

    except KeyboardInterrupt:
        logger.info('Scheduler interrupted by user')
        return
    except Exception as e:
        logger.error(f'Scheduler failed: {e}')
        sys.exit(1)


def check_command(args: argparse.Namespace) -> None:
    """Handle check command — validate app configuration without starting services.

    Exit contract: 0 when all validations pass, 1 otherwise (discovery
    failures and collected validation errors both end at sys.exit(1)).
    """
    logger = get_logger('cli')

    # Setup logging
    loglevel: str = args.loglevel
    setup_logging(loglevel)

    # Discover app
    try:
        module_locator: str = _resolve_module_argument(args)
        app, _var_name, _module_name, _sys_path_root = discover_app(module_locator)
    except HorsiesError as e:
        logger.error(str(e))
        sys.exit(1)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f'Failed to discover app: {e}')
        sys.exit(1)

    # Run phased validation
    live: bool = args.live
    errors = app.check(live=live)

    if errors:
        report = ValidationReport('check')
        for error in errors:
            report.add(error)
        print(report.format_rust_style(), file=sys.stderr)
        sys.exit(1)
    else:
        task_count = len(app.list_tasks())
        print(f'ok: all validations passed\n  {task_count} task(s) registered')
        sys.exit(0)


def get_docs_command(args: argparse.Namespace) -> None:
    """Handle get-docs command.

    Downloads the docs of the release tag that matches the installed version.
    ``--ref main`` downloads the ``main`` branch docs instead.

    Exit contract: a version with no release tag, or any other failure
    (import, filesystem, download), prints and exits 1.
    """
    output_dir: str = args.output
    use_main_branch: bool = args.ref == 'main'
    try:
        from horsies import __version__
        from horsies.core.docs_fetcher import fetch_docs, resolve_docs_ref

        match resolve_docs_ref(
            installed_version=__version__,
            use_main_branch=use_main_branch,
        ):
            case Err(ref_error):
                print(f'error: {ref_error.message()}', file=sys.stderr)
                sys.exit(1)
            case Ok(ref):
                fetch_docs(ref=ref, output_dir=output_dir)
    except Exception as e:
        print(f'error: {e}', file=sys.stderr)
        sys.exit(1)


def skills_command(args: argparse.Namespace) -> None:
    """Handle ``skills install``.

    Copies the packaged skill folder to ``<target>/horsies/``. Exit contract:
    exit 0 when the folder is written or already current; exit 1 on any
    ``SkillInstallError``, with the message on stderr.
    """
    from horsies.core.skills_installer import (
        SkillAlreadyCurrent,
        SkillInstalled,
        describe_install_error,
        install_skill,
    )

    match install_skill(
        target=Path(args.target),
        overwrite_changed=args.force,
    ):
        case Err(error):
            print(f'error: {describe_install_error(error)}', file=sys.stderr)
            sys.exit(1)
        case Ok(outcome):
            match outcome:
                case SkillInstalled(destination=destination, file_count=count):
                    print(f'installed {count} files to {destination}')
                case SkillAlreadyCurrent(destination=destination):
                    print(f'{destination} is already current')
                case _:
                    assert_never(outcome)


_TRUSTED_HEADER_WARNING = (
    'SECURITY: the reverse proxy in front of this server MUST strip or '
    'overwrite the %s header on incoming requests. A proxy that forwards a '
    'client-supplied header makes this mode trivially spoofable, and horsies '
    'cannot detect that.'
)


def _web_arg_error(message: str) -> NoReturn:
    """Report a bad ``horsies web`` invocation and exit 2, as argparse does."""
    print(f'horsies web: error: {message}', file=sys.stderr)
    sys.exit(2)


def _is_loopback(host: str) -> bool:
    """Whether a bind address reaches only this machine.

    Anything that is not a recognizable loopback address is treated as
    exposed, so an unparseable host fails closed rather than open.
    """
    if host == 'localhost':
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _registryless_app(args: argparse.Namespace) -> Horsies:
    """Build a task-registry-less app that can only read and act on rows.

    Monitoring never calls task functions, so an app with an empty registry
    serves every endpoint. Typed result decoding is the one thing it cannot
    do, which the monitoring surface does not use.

    The broker is built unable to execute DDL: pointing this command at a
    database must never migrate it.
    """
    from pydantic import SecretStr

    from horsies.core.models.app import AppConfig
    from horsies.core.models.broker import PostgresConfig

    session_url: str | None = args.session_database_url
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr(args.database_url),
                session_database_url=(SecretStr(session_url) if session_url else None),
                pgbouncer_transaction_mode=args.pgbouncer_transaction_mode,
            )
        ),
        # The monitoring tool never migrates a database it is only reading.
        run_schema_migrations=False,
    )


def resolve_web_app(args: argparse.Namespace) -> Horsies:
    """Resolve the app a ``horsies web`` invocation should serve.

    Exactly one source is allowed: a module locator, or a database URL. Both
    or neither is a usage error.
    """
    locator: str | None = args.app_path
    database_url: str | None = args.database_url

    match (locator is not None, database_url is not None):
        case (True, True):
            _web_arg_error('pass either an app path or --database-url, not both')
        case (False, False):
            _web_arg_error(
                'provide an app path (e.g. myapp.tasks:app) or --database-url'
            )
        case (True, False):
            app, _var_name, _module_name, _root = discover_app(str(locator))
            return app
        case (False, True):
            return _registryless_app(args)


def resolve_web_auth_policy(args: argparse.Namespace) -> Any:
    """Build the authorization policy for a ``horsies web`` invocation.

    Binding beyond loopback without a proxy-supplied identity is refused: the
    default posture is that anyone who can reach the port can read the
    deployment's task data.
    """
    from horsies.web.auth import AllowAll, TrustedHeader, ViewOnly

    host: str = args.host
    enable_actions: bool = args.enable_actions

    match args.auth:
        case 'trusted-header':
            header: str = args.trusted_header
            if not header.strip():
                _web_arg_error('--trusted-header requires a header name')
            print(_TRUSTED_HEADER_WARNING % header, file=sys.stderr)
            return TrustedHeader(header, allow_actions=enable_actions)
        case 'none':
            if not _is_loopback(host):
                _web_arg_error(
                    f'--auth none is only allowed on a loopback host; '
                    f'{host!r} is reachable from the network. Use '
                    f'--auth trusted-header with a proxy-set header.'
                )
            return AllowAll() if enable_actions else ViewOnly()
        case _:
            _web_arg_error(f'unknown --auth value {args.auth!r}')


def web_command(args: argparse.Namespace) -> None:
    """Handle the web command — serve the monitoring UI.

    Exit contract: 2 for a bad invocation (argparse convention), 1 when the
    app cannot be discovered or the web extra is missing.
    """
    setup_logging(args.loglevel)
    logger = get_logger('cli')

    try:
        from horsies.web import create_monitoring_app
    except ImportError as e:
        print(f'error: {e}', file=sys.stderr)
        sys.exit(1)

    try:
        app = resolve_web_app(args)
    except HorsiesError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f'Failed to resolve app: {e}')
        sys.exit(1)

    # An app path carries a registry, and the same strict startup validation
    # `worker` and `scheduler` run also imports the discovered task modules.
    # Serving with an empty registry breaks the actions that need it: resuming
    # a run re-enqueues its READY nodes, and encoding an `args_from` envelope
    # needs the source task's registered OkT. The `--database-url` form has no
    # app to import and stays registry-less by design.
    if args.database_url is None:
        startup_errors = app.check(live=False)
        if startup_errors:
            report = ValidationReport('web-startup')
            for error in startup_errors:
                report.add(error)
            print(report.format_rust_style(), file=sys.stderr)
            sys.exit(1)

    policy = resolve_web_auth_policy(args)
    monitoring_app = create_monitoring_app(
        app,
        auth_policy=policy,
        actions_enabled=args.enable_actions,
    )

    try:
        import uvicorn  # pyright: ignore[reportMissingImports]
    except ImportError:
        print(
            'error: horsies web requires the web extra. Install it with: '
            "pip install 'horsies[web]'",
            file=sys.stderr,
        )
        sys.exit(1)

    host: str = args.host
    port: int = args.port
    logger.info(f'Serving horsies monitoring UI on http://{host}:{port}')
    uvicorn.run(  # pyright: ignore[reportUnknownMemberType]
        monitoring_app, host=host, port=port
    )


REMOVED_WORKER_FLAGS: Final[Mapping[str, str]] = {
    '--processes': 'isolated=IsolatedChildren(count=N)',
    '--max-claim-batch': 'claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=N))',
    '--max-claim-per-worker': (
        'claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=N))'
    ),
    '--max-tasks-per-child': (
        'isolated=IsolatedChildren(count=..., '
        'recycle=IsolatedRecycle(after=TaskLimit(tasks=N)))'
    ),
    '--max-memory-per-child-mb': (
        'isolated=IsolatedChildren(count=..., '
        'recycle=IsolatedRecycle(above=MemoryLimit(mb=N)))'
    ),
    '--drain-timeout': 'drain_timeout=timedelta(seconds=N)',
    '--child-preload': 'preload=ChildPreload.LIBRARY, .APP or .NONE',
}
"""The worker flags that moved into a worker profile, and the profile field
that replaces each one."""


class _RemovedWorkerFlag(argparse.Action):
    """Refuse a worker flag that moved into a worker profile, and name the
    profile field that replaces it. Exits 2, as argparse does."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,
    ) -> NoReturn:
        match option_string:
            case str() as flag:
                replacement = REMOVED_WORKER_FLAGS[flag]
            case None:
                raise AssertionError('argparse passes the flag of an option')
        parser.error(
            f'{flag} was removed in 0.7.0: worker settings live in a worker '
            f'profile. Set WorkerProfile({replacement}) in '
            'AppConfig.worker_profiles, and start the worker with '
            '--profile <name>.'
        )


def main() -> None:
    """Main CLI entry point.

    Argparse errors exit 2 (argparse convention); command failures exit 1
    via each command's own boundary. An exception escaping a command is a
    horsies bug and surfaces as a traceback (non-zero exit) by design.
    """
    try:
        parser = argparse.ArgumentParser(
            prog='horsies',
            description='Horsies task queue - worker and scheduler management',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Using dotted module path (recommended)
  horsies worker app.configs.horsies:app --profile default

  # Using file path
  horsies worker app/configs/horsies.py:app --profile default

  # Auto-discover app variable
  horsies worker app.configs.horsies --profile default

  # Validate configuration without starting services
  horsies check app.configs.horsies:app
  horsies check app.configs.horsies:app --live
""",
        )
        subparsers = parser.add_subparsers(dest='command', help='Available commands')

        # Worker command
        worker_parser = subparsers.add_parser(
            'worker',
            help='Start a horsies worker',
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        worker_parser.add_argument(
            '-m',
            '--module',
            dest='module',
            help='Module path (e.g., app.configs.horsies:app)',
        )
        worker_parser.add_argument(
            'module_pos',
            nargs='?',
            help='Module path (e.g., app.configs.horsies:app)',
        )
        worker_parser.add_argument(
            '--loglevel',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO',
            type=str.upper,
            help='Logging level (default: INFO)',
        )
        worker_parser.add_argument(
            '--profile',
            default=None,
            help=(
                'Name of the worker profile in AppConfig.worker_profiles. '
                'Required: the profile holds the settings of the worker '
                '(children, recycling, claim limits, drain timeout, preload).'
            ),
        )
        for removed_flag in REMOVED_WORKER_FLAGS:
            worker_parser.add_argument(
                removed_flag,
                action=_RemovedWorkerFlag,
                nargs='?',
                help=argparse.SUPPRESS,
            )

        # Scheduler command
        scheduler_parser = subparsers.add_parser(
            'scheduler',
            help='Start the scheduler service',
        )
        scheduler_parser.add_argument(
            '-m',
            '--module',
            dest='module',
            help='Module path (e.g., app.configs.horsies:app)',
        )
        scheduler_parser.add_argument(
            'module_pos',
            nargs='?',
            help='Module path (e.g., app.configs.horsies:app)',
        )
        scheduler_parser.add_argument(
            '--loglevel',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO',
            type=str.upper,
            help='Logging level (default: INFO)',
        )

        # Check command
        check_parser = subparsers.add_parser(
            'check',
            help='Validate app configuration without starting services',
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        check_parser.add_argument(
            '-m',
            '--module',
            dest='module',
            help='Module path (e.g., app.configs.horsies:app)',
        )
        check_parser.add_argument(
            'module_pos',
            nargs='?',
            help='Module path (e.g., app.configs.horsies:app)',
        )
        check_parser.add_argument(
            '--loglevel',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='WARNING',
            type=str.upper,
            help='Logging level (default: WARNING)',
        )
        check_parser.add_argument(
            '--live',
            action='store_true',
            default=False,
            help='Also check broker connectivity (SELECT 1)',
        )

        # Web command
        web_parser = subparsers.add_parser(
            'web',
            help='Serve the monitoring web UI',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Full feature set, from your app module
  horsies web myapp.tasks:app

  # Registry-less: monitor a database directly
  horsies web --database-url postgresql+psycopg://user:pw@host/db

  # Behind a proxy that authenticates callers and sets the header
  horsies web myapp.tasks:app --host 0.0.0.0 \\
      --auth trusted-header --trusted-header X-Forwarded-User --enable-actions
""",
        )
        web_parser.add_argument(
            'app_path',
            nargs='?',
            help='App locator (e.g. myapp.tasks:app); omit when using --database-url',
        )
        web_parser.add_argument(
            '--database-url',
            help='Monitor a database without importing an app module',
        )
        web_parser.add_argument(
            '--session-database-url',
            help='Direct/session-capable URL when the main URL is pooled',
        )
        web_parser.add_argument(
            '--pgbouncer-transaction-mode',
            action='store_true',
            default=False,
            help='Disable prepared statements for a transaction-pooled URL',
        )
        web_parser.add_argument(
            '--host', default='127.0.0.1', help='Bind address (default: 127.0.0.1)'
        )
        web_parser.add_argument(
            '--port', type=int, default=8600, help='Bind port (default: 8600)'
        )
        web_parser.add_argument(
            '--auth',
            choices=['none', 'trusted-header'],
            default='none',
            help='Authorization mode (default: none, loopback only)',
        )
        web_parser.add_argument(
            '--trusted-header',
            default='X-Forwarded-User',
            help='Identity header set by the proxy (default: X-Forwarded-User)',
        )
        web_parser.add_argument(
            '--enable-actions',
            action='store_true',
            default=False,
            help='Allow cancel/pause/resume (default: view-only)',
        )
        web_parser.add_argument(
            '--loglevel',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO',
            type=str.upper,
            help='Logging level (default: INFO)',
        )

        # Get-docs command
        get_docs_parser = subparsers.add_parser(
            'get-docs',
            help='Fetch documentation locally for AI agents',
        )
        get_docs_parser.add_argument(
            '--output',
            default='.horsies-docs',
            help='Output directory (default: .horsies-docs)',
        )
        get_docs_parser.add_argument(
            '--ref',
            choices=['main'],
            default=None,
            help=(
                'Download the main branch docs instead of the release tag that '
                'matches the installed version (required for a development version)'
            ),
        )

        # Skills command
        skills_parser = subparsers.add_parser(
            'skills',
            help='Manage the horsies agent skill folder',
        )
        skills_subparsers = skills_parser.add_subparsers(
            dest='skills_command',
            required=True,
        )
        skills_install_parser = skills_subparsers.add_parser(
            'install',
            help='Copy the packaged skill folder to <target>/horsies/',
        )
        skills_install_parser.add_argument(
            '--target',
            required=True,
            help=(
                'Skills directory of your agent tool, for example .claude/skills. '
                'The skill is written to <target>/horsies/'
            ),
        )
        skills_install_parser.add_argument(
            '--force',
            action='store_true',
            help='Replace an existing copy that differs from the packaged skill',
        )

        args = parser.parse_args()

        match args.command:
            case 'worker':
                worker_command(args)
            case 'scheduler':
                scheduler_command(args)
            case 'check':
                check_command(args)
            case 'web':
                web_command(args)
            case 'get-docs':
                get_docs_command(args)
            case 'skills':
                skills_command(args)
            case _:
                parser.print_help()
                sys.exit(1)
    except KeyboardInterrupt:
        print('\nInterrupted by user')
        sys.exit(0)


if __name__ == '__main__':
    main()
