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
import logging
import os
import signal
import sys

from horsies.core.app import Horsies
from horsies.core.banner import print_banner
from horsies.core.errors import ConfigurationError, ErrorCode, HorsiesError, ValidationReport
from horsies.core.logging import get_logger
from horsies.core.scheduler import Scheduler
from horsies.core.worker.worker import Worker, WorkerConfig
from horsies.core.utils.imports import (
    import_file_path,
    setup_sys_path_from_cwd,
)


def _resolve_module_argument(args: argparse.Namespace) -> str:
    """Return module path from --module or positional, error if missing."""
    module_path = getattr(args, 'module', None) or getattr(args, 'module_pos', None)
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
            raise FileNotFoundError(f'Module file not found: {file_path}')

        # import_file_path adds parent directory to sys.path
        module = import_file_path(file_path)
        module_name = module.__name__
        sys_path_root = os.path.dirname(file_path)
    else:
        # Dotted module path - use standard import
        try:
            module = importlib.import_module(module_path)
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
        module_name = module_path
        sys_path_root = project_root

    # Find horsies instance
    if attr_name:
        # Explicit attribute name
        if not hasattr(module, attr_name):
            raise AttributeError(
                f"Module '{module_name}' has no attribute '{attr_name}'"
            )
        obj = getattr(module, attr_name)
        if not isinstance(obj, Horsies):
            raise TypeError(
                f"'{attr_name}' in module '{module_name}' is not a Horsies instance "
                f"(got {type(obj).__name__})"
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
            raise AttributeError(
                f'No Horsies instance found in {module_name}. '
                'Specify the variable name: module.path:variable'
            )

        if len(app_instances) > 1:
            var_names = [name for _, name in app_instances]
            raise AttributeError(
                f'Multiple Horsies instances found in {module_name}: {var_names}. '
                'Specify which one: module.path:variable'
            )

        app, var_name = app_instances[0]

    logger.info(f"Discovered horsies '{var_name}' from {module_name}")
    return app, var_name, module_name, sys_path_root


def setup_logging(loglevel: str) -> None:
    """Configure logging level globally."""
    from horsies.core.logging import set_default_level

    level = getattr(logging, loglevel.upper(), logging.INFO)
    set_default_level(level)

    root_logger = logging.getLogger('horsies')
    root_logger.setLevel(level)

    for handler in root_logger.handlers:
        handler.setLevel(level)

    for name in logging.Logger.manager.loggerDict:
        if isinstance(name, str) and name.startswith('horsies.'):
            lgr = logging.getLogger(name)
            lgr.setLevel(level)
            for handler in lgr.handlers:
                handler.setLevel(level)


def worker_command(args: argparse.Namespace) -> None:
    """Handle worker command."""
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

    # Get broker config from app
    try:
        broker = app.get_broker()
        postgres_config = broker.config
    except Exception as e:
        logger.error(f'Failed to get broker config: {e}')
        sys.exit(1)

    # Get queues from app config
    queues: list[str] = app.get_valid_queue_names()
    logger.info(f'Worker will process queues: {queues}')

    # Build per-queue settings for CUSTOM mode
    queue_priorities: dict[str, int] = {}
    queue_max_concurrency: dict[str, int] = {}
    try:
        if app.config.queue_mode.name == 'CUSTOM' and app.config.custom_queues:
            for q in app.config.custom_queues:
                queue_priorities[q.name] = q.priority
                queue_max_concurrency[q.name] = q.max_concurrency
    except Exception:
        pass

    # Get discovered task modules
    discovered_modules = app.get_discovered_task_modules()
    if discovered_modules:
        logger.info(f'Using discovered task modules')
    else:
        logger.warning('No task modules discovered.')

    # Create worker config
    processes: int = args.processes
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
        dsn=postgres_config.database_url,
        psycopg_dsn=postgres_config.database_url,
        queues=queues,
        processes=processes,
        app_locator=app_locator,
        sys_path_roots=sys_path_roots,
        imports=discovered_modules,
        queue_priorities=queue_priorities,
        queue_max_concurrency=queue_max_concurrency,
        cluster_wide_cap=app.config.cluster_wide_cap,
        prefetch_buffer=app.config.prefetch_buffer,
        claim_lease_ms=app.config.claim_lease_ms,
        max_claim_batch=args.max_claim_batch,
        max_claim_per_worker=args.max_claim_per_worker,
        recovery_config=app.config.recovery,
        loglevel=log_level_int,
    )

    # Print startup banner
    app.import_task_modules()  # Import tasks so they show in banner
    print_banner(app, role='worker', show_tasks=True)

    # Start worker
    logger.info('Starting worker...')
    try:

        async def run_worker() -> None:
            try:
                logger.info('Ensuring Postgres schema and triggers are initialized...')
                await broker.ensure_schema_initialized()
            except Exception as e:
                logger.error(f'Failed to initialize database schema: {e}')
                raise

            worker = Worker(broker.session_factory, broker.listener, worker_config)

            loop = asyncio.get_running_loop()

            def signal_handler() -> None:
                logger.info('Received interrupt signal, stopping worker...')
                worker.request_stop()

            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, signal_handler)
                except NotImplementedError:
                    pass

            await worker.run_forever()

        try:
            asyncio.run(run_worker())
        except KeyboardInterrupt:
            logger.info('Worker interrupted by user')
            return
        except asyncio.TimeoutError:
            logger.error('Worker startup timed out')
            return
    except KeyboardInterrupt:
        logger.info('Worker interrupted by user')
        return
    except Exception as e:
        logger.error(f'Worker failed: {e}')
        sys.exit(1)


def scheduler_command(args: argparse.Namespace) -> None:
    """Handle scheduler command."""
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
            try:
                scheduler = Scheduler(app)

                loop = asyncio.get_running_loop()

                def signal_handler() -> None:
                    logger.info('Received interrupt signal, stopping scheduler...')
                    scheduler.request_stop()

                for sig in (signal.SIGTERM, signal.SIGINT):
                    try:
                        loop.add_signal_handler(sig, signal_handler)
                    except NotImplementedError:
                        pass

                await scheduler.run_forever()

            except Exception as e:
                logger.error(f'Scheduler error: {e}', exc_info=True)
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
    """Handle check command — validate app configuration without starting services."""
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


def main() -> None:
    """Main CLI entry point."""
    try:
        parser = argparse.ArgumentParser(
            prog='horsies',
            description='Horsies task queue - worker and scheduler management',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Using dotted module path (recommended)
  horsies worker app.configs.horsies:app

  # Using file path
  horsies worker app/configs/horsies.py:app

  # Auto-discover app variable
  horsies worker app.configs.horsies

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
            '--processes',
            type=int,
            default=1,
            help='Number of worker processes (default: 1)',
        )
        worker_parser.add_argument(
            '--max-claim-batch',
            type=int,
            default=2,
            help='Max tasks per queue per pass (default: 2)',
        )
        worker_parser.add_argument(
            '--max-claim-per-worker',
            type=int,
            default=0,
            help='Max claimed tasks per worker, 0=auto (default: 0)',
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

        args = parser.parse_args()

        match args.command:
            case 'worker':
                worker_command(args)
            case 'scheduler':
                scheduler_command(args)
            case 'check':
                check_command(args)
            case _:
                parser.print_help()
                sys.exit(1)
    except KeyboardInterrupt:
        print('\nInterrupted by user')
        sys.exit(0)


if __name__ == '__main__':
    main()
