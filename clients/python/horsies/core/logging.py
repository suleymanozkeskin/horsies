import logging
import os
import sys
import threading
from datetime import datetime
from typing import Final, TextIO

from horsies._core import refresh_log_levels

# Library convention: NullHandler so logs are silent by default.
# The application (or horsies CLI) configures handlers.
logging.getLogger('horsies').addHandler(logging.NullHandler())

STATEMENT_LOGGER_NAME: Final = 'horsies.sql'
"""The logger of the core's statement log: with ``PostgresConfig.echo`` on,
one INFO record per statement, the SQL text as the message and the
statement name as the attribute ``statement``. Bound values are not
logged."""

STATEMENT_ECHO_FORMAT: Final = '%(asctime)s %(levelname)s %(name)s %(message)s'
"""The format of the stdout handler that ``enable_statement_echo`` adds:
the format of the handler that SQLAlchemy's ``echo=True`` adds."""

_statement_echo_lock = threading.Lock()


class _StatementEchoHandler(logging.StreamHandler[TextIO]):
    """The stdout handler that ``enable_statement_echo`` adds to
    ``horsies.sql``, with ``STATEMENT_ECHO_FORMAT``.

    It is a type of its own, so that ``_EchoedStatementFilter`` can tell it
    from a handler of the application.
    """

    def __init__(self) -> None:
        super().__init__(sys.stdout)
        self.setFormatter(logging.Formatter(STATEMENT_ECHO_FORMAT))


class _EchoedStatementFilter(logging.Filter):
    """The filter of the stderr handler of ``configure_logging``.

    It drops a record of the ``horsies.sql`` logger while that logger holds a
    ``_StatementEchoHandler``: the handler has written the record to stdout
    before the record propagates to ``horsies``. It passes every other
    record, and every ``horsies.sql`` record while that logger holds no such
    handler (for example, when it holds only a handler of the application).

    It reads the handlers of ``horsies.sql`` at each record, so the order of
    ``configure_logging`` and ``enable_statement_echo`` has no effect. It
    does not change the record.
    """

    def __init__(self) -> None:
        super().__init__()
        self._statement_logger = logging.getLogger(STATEMENT_LOGGER_NAME)

    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            record.name == STATEMENT_LOGGER_NAME
            and self._statement_logger_holds_echo_handler()
        )

    def _statement_logger_holds_echo_handler(self) -> bool:
        return any(
            isinstance(handler, _StatementEchoHandler)
            for handler in self._statement_logger.handlers
        )


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

    The stderr handler drops ``horsies.sql`` records while ``horsies.sql``
    holds the stdout handler of ``enable_statement_echo``
    (``_EchoedStatementFilter``), so each statement shows once, on stdout.
    """
    root = logging.getLogger('horsies')
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ColoredFormatter(use_color=_should_use_color(sys.stderr)))
    handler.addFilter(_EchoedStatementFilter())
    root.addHandler(handler)
    root.setLevel(level)


def enable_statement_echo() -> None:
    """Show the core's statement log in this process, as SQLAlchemy's
    ``echo=True`` shows its statements.

    The broker calls it when ``PostgresConfig.echo`` is on. Effects:

    - The ``horsies.sql`` logger gets the level ``INFO``.
    - When ``horsies.sql`` has no handler, it gets one stdout handler
      (``_StatementEchoHandler``) with ``STATEMENT_ECHO_FORMAT``. A second
      call adds none, so the process has at most one such handler. A logger
      that has a handler of the application gets none, as in SQLAlchemy.
    - A core that runs reads the levels of the ``horsies`` loggers again
      (``refresh_log_levels``), so its next statement is forwarded. A core
      that has not started reads them when it starts.

    It does not change ``propagate``: a record also reaches the handlers of
    ``horsies`` and of the root logger. The stderr handler of
    ``configure_logging`` drops these records while ``horsies.sql`` holds
    the stdout handler, so the CLI shows each statement once, on stdout. A
    handler of the application on ``horsies`` or on the root logger gets
    them too, as with SQLAlchemy's ``echo``.

    It does not remove a handler and does not change another logger. Its
    only failure is a defect: an exception of ``logging`` itself.
    """
    logger = logging.getLogger(STATEMENT_LOGGER_NAME)
    with _statement_echo_lock:
        logger.setLevel(logging.INFO)
        match logger.handlers:
            case []:
                logger.addHandler(_StatementEchoHandler())
            case _:
                pass
    refresh_log_levels()


def get_logger(component_name: str) -> logging.Logger:
    """Get a logger for the specified component.

    Returns a bare logger under the 'horsies' namespace.
    No handlers are attached — the logger propagates to the
    'horsies' root, which is configured by the CLI or by
    the host application.
    """
    return logging.getLogger(f'horsies.{component_name}')
