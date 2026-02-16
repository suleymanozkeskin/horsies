"""Startup banner for horsies."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, TextIO

from horsies.core.utils.url import mask_database_url

if TYPE_CHECKING:
    from horsies.core.app import Horsies


# ---------------------------------------------------------------------------
# ANSI color codes (matching Rust owo-colors output)
# ---------------------------------------------------------------------------

class Colors:
    """ANSI escape codes for terminal colors."""

    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIMMED = '\033[2m'

    # Standard colors
    WHITE = '\033[37m'
    BRIGHT_WHITE = '\033[97m'
    CYAN = '\033[36m'
    BRIGHT_CYAN = '\033[96m'
    YELLOW = '\033[33m'
    BRIGHT_YELLOW = '\033[93m'


def _color(text: str, *codes: str) -> str:
    """Apply ANSI color codes to text."""
    if not codes:
        return text
    return ''.join(codes) + text + Colors.RESET


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

# Figlet-style "horsies" text with version and tagline
LOGO_TEXT = r"""
    __                    _
   / /_  ____  __________(_)__  _____
  / __ \/ __ \/ ___/ ___/ / _ \/ ___/      {version}
 / / / / /_/ / /  (__  ) /  __(__  )       distributed task queue
/_/ /_/\____/_/  /____/_/\___/____/        and workflow engine
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
    """Get horsies version from package metadata."""
    try:
        from importlib.metadata import version
        return version('horsies')
    except Exception:
        return 'dev'


def format_banner(version: str | None = None) -> str:
    """Format the banner string with version."""
    if version is None:
        version = get_version()
    return BANNER.format(version=f'v{version}')


# ---------------------------------------------------------------------------
# Formatting helpers (matching Rust banner.rs)
# ---------------------------------------------------------------------------

def _format_ms(ms: int) -> str:
    """Format milliseconds into a human-readable string."""
    if ms >= 60_000:
        mins = ms // 60_000
        remainder_s = (ms % 60_000) // 1_000
        if remainder_s > 0:
            return f'{mins}m{remainder_s}s'
        return f'{mins}m'
    elif ms >= 1_000:
        secs = ms // 1_000
        remainder_ms = ms % 1_000
        if remainder_ms > 0:
            return f'{secs}.{remainder_ms // 100}s'
        return f'{secs}s'
    return f'{ms}ms'


def _format_bool(val: bool) -> str:
    """Format a boolean for display."""
    return 'yes' if val else 'no'


def _write_section_header(lines: list[str], name: str) -> None:
    """Write a colored [section] header line."""
    header = _color(name, Colors.BRIGHT_CYAN, Colors.BOLD)
    lines.append(f'[{header}]')


def _write_kv(lines: list[str], key: str, value: str) -> None:
    """Write a colored '.> key: value' line with consistent alignment."""
    prefix = _color('.>', Colors.DIMMED)
    # Pad key before coloring to maintain alignment
    padded_key = f'{key}:'.ljust(22)
    colored_key = _color(padded_key, Colors.WHITE, Colors.BOLD)
    colored_value = _color(value, Colors.BRIGHT_WHITE)
    lines.append(f'  {prefix} {colored_key} {colored_value}')


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

    # ASCII art header - cyan colored
    colored_horse = _color(HORSE_BRAILLE, Colors.CYAN)
    lines.append(colored_horse)

    # Logo with version - bright yellow bold
    logo = LOGO_TEXT.replace('{version}', f'v{version}')
    colored_logo = _color(logo, Colors.BRIGHT_YELLOW, Colors.BOLD)
    lines.append(colored_logo)

    # [config] section
    _write_section_header(lines, 'config')
    _write_kv(lines, 'app', app.__class__.__name__)
    _write_kv(lines, 'role', role)
    _write_kv(lines, 'queue_mode', app.config.queue_mode.name.lower())

    # Queue info
    if app.config.queue_mode.name == 'CUSTOM' and app.config.custom_queues:
        queues_str = ', '.join(q.name for q in app.config.custom_queues)
    else:
        queues_str = 'default'
    _write_kv(lines, 'queues', queues_str)

    # Broker info (mask password)
    _write_kv(lines, 'broker', mask_database_url(app.config.broker.database_url))

    # Cluster-wide cap
    if app.config.cluster_wide_cap is not None:
        _write_kv(lines, 'cluster_cap', f'{app.config.cluster_wide_cap} (cluster-wide)')

    # Prefetch buffer
    if app.config.prefetch_buffer > 0:
        _write_kv(lines, 'prefetch', str(app.config.prefetch_buffer))

    lines.append('')

    # [recovery] section
    recovery = app.config.recovery
    _write_section_header(lines, 'recovery')
    _write_kv(
        lines,
        'requeue_stale_claimed',
        _format_bool(recovery.auto_requeue_stale_claimed),
    )
    _write_kv(
        lines,
        'fail_stale_running',
        _format_bool(recovery.auto_fail_stale_running),
    )
    _write_kv(
        lines,
        'check_interval',
        _format_ms(recovery.check_interval_ms),
    )
    _write_kv(
        lines,
        'heartbeat_interval',
        _format_ms(recovery.runner_heartbeat_interval_ms),
    )
    lines.append('')

    # [tasks] section
    if show_tasks:
        task_names = app.list_tasks()
        tasks_header = _color('tasks', Colors.BRIGHT_CYAN, Colors.BOLD)
        lines.append(f'[{tasks_header}] ({len(task_names)} registered)')
        for task_name in sorted(task_names):
            bullet = _color('.', Colors.DIMMED)
            name = _color(task_name, Colors.WHITE)
            lines.append(f'  {bullet} {name}')
        lines.append('')

    # Print everything
    output = '\n'.join(lines)
    print(output, file=file)


def print_simple_banner(file: TextIO | None = None) -> None:
    """Print just the ASCII art banner without config."""
    if file is None:
        file = sys.stdout

    version = get_version()

    # Horse art - cyan
    colored_horse = _color(HORSE_BRAILLE, Colors.CYAN)
    print(colored_horse, file=file)

    # Logo with version - bright yellow bold
    logo = LOGO_TEXT.replace('{version}', f'v{version}')
    colored_logo = _color(logo, Colors.BRIGHT_YELLOW, Colors.BOLD)
    print(colored_logo, file=file)
