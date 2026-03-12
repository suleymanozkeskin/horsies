import logging
import sys
from datetime import datetime

# Library convention: NullHandler so logs are silent by default.
# The application (or horsies CLI) configures handlers.
logging.getLogger('horsies').addHandler(logging.NullHandler())


class ColoredFormatter(logging.Formatter):
    """Colored log formatter used by horsies CLI."""

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

        # Get level color
        level_color = self.LEVEL_COLORS.get(record.levelname, self.COLORS['WHITE'])

        # Format: [time] [comp_name]   [level]     message
        formatted = (
            f"{self.COLORS['LIGHT_BLUE']}[{time_str}]{self.COLORS['RESET']} "
            f"{self.COLORS['WHITE']}{component_padded}{self.COLORS['RESET']}"
            f"{level_color}{level_padded}{self.COLORS['RESET']}"
            f"{self.COLORS['WHITE']}{record.getMessage()}{self.COLORS['RESET']}"
        )

        # Add exception info if present
        if record.exc_info:
            formatted += '\n' + self.formatException(record.exc_info)

        return formatted


def configure_logging(level: int = logging.INFO) -> None:
    """Configure horsies logging with colored output on stderr.

    Called by the CLI entrypoint and child worker processes.
    Library users should configure the 'horsies' logger via standard logging.
    """
    root = logging.getLogger('horsies')
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ColoredFormatter())
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
