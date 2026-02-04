"""Startup banner for horsies."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, TextIO

if TYPE_CHECKING:
    from horsies.core.app import Horsies


# Braille art of galloping horse - converted from the golden horse image
# Each braille character represents a 2x4 pixel block for higher resolution
HORSE_BRAILLE = """
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣶⣾⣿⣿⣿⣿⣷⣯⡀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣟⣿⣆⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿⣿⣿⣿⣿⣿⣿⡟⠹⣿⣿⣿⣿⣿⣦⡀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⣀⣠⣴⣾⣿⣿⣿⣶⣶⣶⣤⣤⣤⣤⣤⣶⣾⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠈⠉⠛⠻⡿⣿⣿⠂⠀
⠀⠀⢀⣀⠀⢀⣀⣠⣶⣿⣿⠟⢛⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⡐⠀⠀⠀⠀⠀⠀⠈⠋⡿⠁⠀⠀
⠀⠀⠀⢹⣿⣿⣿⣿⣿⡿⠁⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣯⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠛⠻⠿⠛⠉⠀⠀⠀⠈⢯⡻⣿⣿⣿⣿⣿⢿⣿⣿⣿⣿⣿⣿⡿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⣷⣿⣿⣿⡟⠀⠙⠻⠿⠿⣿⣿⠃⣿⣿⣿⣿⣿⣿⣿⣿⡁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⡿⠁⢀⠀⠀⠀⠀⠀⠂⠀⢿⣿⣿⣿⡍⠈⢁⣙⣿⢦⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
  ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢰⣿⣿⠏⠀⠀⣼⠀⠀⠀⠀⠀⠀⠀⠀⠙⢿⣿⣷⠀⠀⠀⠀⠁⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣿⣧⣀⢄⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣿⣧⠀⠀⢀⣼⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⣀⠀⠀⠀⢀⡀⠀⠀⠀⡀⠀⠀⠀⣀⠛⣿⣷⣍⠛⣦⡀⠀⠂⢠⣷⣶⣾⡿⠟⠛⢃⠀⢠⣾⡟⠀⠀⠀⠀⡀⠀⠀⠀⡀⠀⠀⠀
⠄⡤⠦⠤⠤⢤⡤⠡⠤⠤⢤⠬⠤⠤⠤⢤⠅⠀⠤⣿⣷⠄⠎⢻⣤⡦⠄⠀⠤⢵⠄⠠⠤⠬⣾⠏⠁⠀⠥⡤⠄⠠⠬⢦⡤⠀⠠⠵⢤⠠
⠚⠒⠒⠒⡶⠚⠒⠒⠒⡰⠓⠒⠒⠒⢲⠓⠒⠒⠒⢻⠿⠀⠀⠚⢿⡷⠐⠒⠒⠚⡆⠒⠒⠒⠚⣖⠒⠒⠒⠚⢶⠒⠒⠒⠚⢶⠂⠐⠒⠛
"""

# Figlet-style "horsies" text
LOGO_TEXT = r"""
    __                    _
   / /_  ____  __________(_)__  _____
  / __ \/ __ \/ ___/ ___/ / _ \/ ___/
 / / / / /_/ / /  (__  ) /  __(__  )
/_/ /_/\____/_/  /____/_/\___/____/
"""

# Full banner combining horse and logo
BANNER = (
    HORSE_BRAILLE
    + r"""
    __                    _
   / /_  ____  __________(_)__  _____
  / __ \/ __ \/ ___/ ___/ / _ \/ ___/      {version}
 / / / / /_/ / /  (__  ) /  __(__  )       distributed task queue
/_/ /_/\____/_/  /____/_/\___/____/        and workflow engine
"""
)


def get_version() -> str:
    """Get horsies version."""
    try:
        import horsies

        version = getattr(horsies, '__version__', None)
        return str(version) if version else '0.1.0'
    except ImportError:
        return '0.1.0'


def format_banner(version: str | None = None) -> str:
    """Format the banner string with version."""
    if version is None:
        version = get_version()
    return BANNER.format(version=f'v{version}')


def print_banner(
    app: 'Horsies',
    role: str = 'worker',
    show_tasks: bool = True,
    file: TextIO | None = None,
) -> None:
    """
    Print startup banner with configuration and task list.

    Args:
        app: The Horsies app instance
        role: The role (worker, scheduler, producer)
        show_tasks: Whether to list discovered tasks
        file: Output file (default: sys.stdout)
    """
    if file is None:
        file = sys.stdout

    version = get_version()

    # Build the banner
    lines: list[str] = []

    # ASCII art header
    banner = format_banner(version)
    lines.append(banner)

    # Configuration section
    lines.append('[config]')
    lines.append(f'  .> app:         {app.__class__.__name__}')
    lines.append(f'  .> role:        {role}')
    lines.append(f'  .> queue_mode:  {app.config.queue_mode.name}')

    # Queue info
    if app.config.queue_mode.name == 'CUSTOM' and app.config.custom_queues:
        queues_str = ', '.join(q.name for q in app.config.custom_queues)
        lines.append(f'  .> queues:      {queues_str}')
    else:
        lines.append(f'  .> queues:      default')

    # Broker info
    broker_url = app.config.broker.database_url
    # Mask password in URL
    if '@' in broker_url:
        pre, post = broker_url.split('@', 1)
        if ':' in pre:
            scheme_user = pre.rsplit(':', 1)[0]
            broker_url = f'{scheme_user}:****@{post}'
    lines.append(f'  .> broker:      {broker_url}')

    # Concurrency info (if available)
    if hasattr(app.config, 'cluster_wide_cap') and app.config.cluster_wide_cap:
        lines.append(f'  .> cap:         {app.config.cluster_wide_cap} (cluster-wide)')

    lines.append('')

    # Tasks section
    if show_tasks:
        task_names = app.list_tasks()
        lines.append(f'[tasks] ({len(task_names)} registered)')
        for task_name in sorted(task_names):
            lines.append(f'  . {task_name}')
        lines.append('')

    # Print everything
    output = '\n'.join(lines)
    print(output, file=file)


def print_simple_banner(file: TextIO | None = None) -> None:
    """Print just the ASCII art banner without config."""
    if file is None:
        file = sys.stdout
    print(format_banner(), file=file)
