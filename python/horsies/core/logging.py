import logging
import os
import sys
from datetime import datetime

# Library convention: NullHandler so logs are silent by default.
# The application (or horsies CLI) configures handlers.
logging.getLogger('horsies').addHandler(logging.NullHandler())


class ColoredFormatter(logging.Formatter):
    """Tabular log formatter for horsies, with optional ANSI color.

    ``use_color=False`` emits the same layout and padding without escape codes,
    so captured/non-TTY sinks (log drains, container logs, journald, files) stay
    parseable. Color is selected by ``configure_logging`` via
    ``_should_use_color``.
    """

    def __init__(self, use_color: bool = True) -> None:
        super().__init__()
        self._use_color = use_color

    COLORS = {
        'RESET': '\033[0m',
        'LIGHT_BLUE': '\033[94m',
        'WHITE': '\033[97m',
        'GRAY': '\033[90m',
        'GREEN': '\033[92m',
        'YELLOW': '\033[93m',
        'RED': '\033[91m',
        'BRIGHT_RED': '\033[1;91m',
    }

    LEVEL_COLORS = {
        'DEBUG': COLORS['GRAY'],
        'INFO': COLORS['GREEN'],
        'WARNING': COLORS['YELLOW'],
        'ERROR': COLORS['RED'],
        'CRITICAL': COLORS['BRIGHT_RED'],
    }

    def format(self, record: logging.LogRecord) -> str:
        # Format time as HH:MM:SS
        time_str = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')

        # Get component name from logger name (e.g., 'horsies.broker' -> 'broker')
        component = record.name.split('.')[-1] if '.' in record.name else record.name

        # Calculate padding for alignment
        component_section = f'[{component}]'
        level_section = f'[{record.levelname}]'

        # Pad sections to fixed widths for tabular layout
        component_padded = component_section.ljust(
            14,
        )  # [dispatcher] = 12 chars, so 14 for padding
        level_padded = level_section.ljust(10)  # [WARNING] = 9 chars, so 10 for padding

        # Resolve colors (empty strings when disabled, keeping the same layout)
        def c(key: str) -> str:
            return self.COLORS[key] if self._use_color else ''

        reset = c('RESET')
        level_color = (
            self.LEVEL_COLORS.get(record.levelname, self.COLORS['WHITE'])
            if self._use_color
            else ''
        )

        # Format: [time] [comp_name]   [level]     message
        formatted = (
            f"{c('LIGHT_BLUE')}[{time_str}]{reset} "
            f"{c('WHITE')}{component_padded}{reset}"
            f'{level_color}{level_padded}{reset}'
            f"{c('WHITE')}{record.getMessage()}{reset}"
        )

        # Add exception info if present
        if record.exc_info:
            formatted += '\n' + self.formatException(record.exc_info)

        return formatted


def _should_use_color(stream: object) -> bool:
    """Decide whether to emit ANSI color for a log stream.

    Precedence: ``FORCE_COLOR`` (set, non-empty, not ``"0"``) forces color on;
    otherwise ``NO_COLOR`` (set to any value) forces it off; otherwise color
    follows whether the stream is a TTY. This keeps ANSI escapes out of non-TTY
    sinks (log drains, container logs, journald, files) where they would break
    log parsing.
    """
    force = os.environ.get('FORCE_COLOR')
    if force not in (None, '', '0'):
        return True
    if 'NO_COLOR' in os.environ:
        return False
    isatty = getattr(stream, 'isatty', None)
    return bool(callable(isatty) and isatty())


def configure_logging(level: int = logging.INFO) -> None:
    """Configure horsies logging on stderr; color only when the sink is a TTY.

    Called by the CLI entrypoint and child worker processes. Color is gated by
    ``_should_use_color`` (TTY / ``NO_COLOR`` / ``FORCE_COLOR``). Library users
    should configure the 'horsies' logger via standard logging.
    """
    root = logging.getLogger('horsies')
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ColoredFormatter(use_color=_should_use_color(sys.stderr)))
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(component_name: str) -> logging.Logger:
    """Get a logger for the specified component.

    Returns a bare logger under the 'horsies' namespace.
    No handlers are attached — the logger propagates to the
    'horsies' root, which is configured by the CLI or by
    the host application.
    """
    return logging.getLogger(f'horsies.{component_name}')
