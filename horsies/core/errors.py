"""Rust-style error display for horsies startup/validation errors."""

from __future__ import annotations

import inspect
import linecache
import os
import sys
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

# Absolute path to the horsies package directory.
# Used by _find_user_frame to distinguish library frames from user code.
# This avoids false positives when the project checkout path itself contains
# "horsies" (e.g. /home/runner/work/horsies/horsies/ on GitHub Actions).
_HORSIES_PKG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ErrorCode(str, Enum):
    """Error codes for startup/validation errors.

    Organized by category:
    - E001-E099: Workflow validation errors
    - E100-E199: Task definition errors
    - E200-E299: Config/broker errors
    - E300-E399: Registry errors
    """

    # Workflow validation (E001-E099)
    WORKFLOW_NO_NAME = 'E001'
    WORKFLOW_NO_NODES = 'E002'
    WORKFLOW_INVALID_NODE_ID = 'E003'
    WORKFLOW_DUPLICATE_NODE_ID = 'E004'
    WORKFLOW_NO_ROOT_TASKS = 'E005'
    WORKFLOW_INVALID_DEPENDENCY = 'E006'
    WORKFLOW_CYCLE_DETECTED = 'E007'
    WORKFLOW_INVALID_ARGS_FROM = 'E008'
    WORKFLOW_INVALID_CTX_FROM = 'E009'
    WORKFLOW_CTX_PARAM_MISSING = 'E010'
    WORKFLOW_INVALID_OUTPUT = 'E011'
    WORKFLOW_INVALID_SUCCESS_POLICY = 'E012'
    WORKFLOW_INVALID_JOIN = 'E013'
    WORKFLOW_UNRESOLVED_QUEUE = 'E014'
    WORKFLOW_UNRESOLVED_PRIORITY = 'E015'
    WORKFLOW_ARGS_WITH_INJECTION = 'E016'
    WORKFLOW_INVALID_KWARG_KEY = 'E019'
    WORKFLOW_MISSING_REQUIRED_PARAMS = 'E020'
    WORKFLOW_KWARGS_ARGS_FROM_OVERLAP = 'E021'
    WORKFLOW_SUBWORKFLOW_PARAMS_REQUIRE_BUILD_WITH = 'E022'
    WORKFLOW_SUBWORKFLOW_BUILD_WITH_BINDING = 'E023'
    WORKFLOW_ARGS_FROM_TYPE_MISMATCH = 'E024'
    WORKFLOW_OUTPUT_TYPE_MISMATCH = 'E025'
    WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED = 'E026'
    # Legacy alias kept for compatibility with older references.
    WORKFLOW_EXCESS_POSITIONAL_ARGS = 'E026'
    WORKFLOW_CHECK_CASES_REQUIRED = 'E027'
    WORKFLOW_CHECK_CASE_INVALID = 'E028'
    WORKFLOW_CHECK_BUILDER_EXCEPTION = 'E029'
    WORKFLOW_CHECK_UNDECORATED_BUILDER = 'E030'
    WORKFLOW_INVALID_SUBWORKFLOW_RETRY_MODE = 'E017'
    WORKFLOW_SUBWORKFLOW_APP_MISSING = 'E018'

    # Task definition (E100-E199)
    TASK_NO_RETURN_TYPE = 'E100'
    TASK_INVALID_RETURN_TYPE = 'E101'
    TASK_INVALID_OPTIONS = 'E102'
    TASK_INVALID_QUEUE = 'E103'

    # Config/broker (E200-E299)
    CONFIG_INVALID_QUEUE_MODE = 'E200'
    CONFIG_INVALID_CLUSTER_CAP = 'E201'
    CONFIG_INVALID_PREFETCH = 'E202'
    BROKER_INVALID_URL = 'E203'
    CONFIG_INVALID_RECOVERY = 'E204'
    CONFIG_INVALID_SCHEDULE = 'E205'
    CLI_INVALID_ARGS = 'E206'
    WORKER_INVALID_LOCATOR = 'E207'
    CONFIG_INVALID_RESILIENCE = 'E208'
    CONFIG_INVALID_EXCEPTION_MAPPER = 'E209'

    # Registry (E300-E399)
    TASK_NOT_REGISTERED = 'E300'
    TASK_DUPLICATE_NAME = 'E301'


# ANSI color codes
class _Colors:
    """ANSI escape codes for terminal colors."""

    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    DIM = '\033[2m'


def _should_use_colors() -> bool:
    """Determine if colors should be used in output."""
    # Check force color env var
    if os.environ.get('HORSIES_FORCE_COLOR', '').lower() in ('1', 'true', 'yes'):
        return True

    # Check NO_COLOR standard (https://no-color.org/)
    if os.environ.get('NO_COLOR') is not None:
        return False

    # Check if stdout is a TTY
    return hasattr(sys.stderr, 'isatty') and sys.stderr.isatty()


def _should_show_verbose() -> bool:
    """Determine if verbose output (full traceback) should be shown."""
    return os.environ.get('HORSIES_VERBOSE', '').lower() in ('1', 'true', 'yes')


def _should_use_plain_errors() -> bool:
    """Determine if plain Python errors should be used instead of Rust-style."""
    return os.environ.get('HORSIES_PLAIN_ERRORS', '').lower() in ('1', 'true', 'yes')


@dataclass
class SourceLocation:
    """Source code location information."""

    file: str
    line: int
    column: int | None = None
    end_column: int | None = None

    @classmethod
    def from_frame(cls, frame: Any) -> SourceLocation:
        """Create SourceLocation from a frame object."""
        return cls(
            file=frame.f_code.co_filename,
            line=frame.f_lineno,
        )

    @classmethod
    def from_function(cls, fn: Callable[..., Any]) -> SourceLocation | None:
        """Create SourceLocation from a function object.

        Returns None if the callable doesn't have source location info
        (e.g., built-in functions, C extensions, or mock objects).
        """
        code = getattr(fn, '__code__', None)
        if code is None:
            return None
        return cls(
            file=code.co_filename,
            line=code.co_firstlineno,
        )

    def get_source_line(self) -> str | None:
        """Read the source line from the file."""
        try:
            line = linecache.getline(self.file, self.line)
            return line.rstrip('\n') if line else None
        except Exception:
            return None

    def format_short(self) -> str:
        """Format as 'file:line' or 'file:line:col'."""
        if self.column is not None:
            return f'{self.file}:{self.line}:{self.column}'
        return f'{self.file}:{self.line}'


@dataclass
class HorsiesError(Exception):
    """Base exception for horsies startup/validation errors.

    Provides Rust-style error formatting with:
    - Error code and category
    - Source location with code snippet
    - Notes and help text
    """

    message: str
    code: ErrorCode | None = None
    location: SourceLocation | None = None
    notes: list[str] = field(default_factory=lambda: [])
    help_text: str | None = None

    # For dataclass inheritance
    def __post_init__(self) -> None:
        # Initialize Exception with the message
        super().__init__(self.message)

        # Auto-detect location from call stack if not provided
        if self.location is None:
            user_frame = _find_user_frame()
            if user_frame is not None:
                self.location = SourceLocation.from_frame(user_frame)

    def with_note(self, note: str) -> HorsiesError:
        """Add a note to the error (fluent API)."""
        self.notes.append(note)
        return self

    def with_help(self, help_text: str) -> HorsiesError:
        """Set help text (fluent API)."""
        self.help_text = help_text
        return self

    def with_location(self, location: SourceLocation) -> HorsiesError:
        """Set source location (fluent API)."""
        self.location = location
        return self

    def with_location_from_frame(self, frame: Any) -> HorsiesError:
        """Set source location from frame (fluent API)."""
        self.location = SourceLocation.from_frame(frame)
        return self

    def format_rust_style(self, use_colors: bool | None = None) -> str:
        """Format the error in Rust style."""
        if use_colors is None:
            use_colors = _should_use_colors()

        c = _Colors if use_colors else _NoColors
        lines: list[str] = []

        # Leading blank line for visual separation from log output
        lines.append('')

        # Error header: error[E001]: message
        code_part = f'[{self.code.value}]' if self.code else ''
        lines.append(f'{c.BOLD}{c.RED}error{code_part}:{c.RESET} {self.message}')

        # Source location and code snippet
        if self.location:
            source_line = self.location.get_source_line()

            # Location arrow: --> file:line:col
            lines.append(
                f'  {c.BLUE}-->{c.RESET} {c.CYAN}{self.location.format_short()}{c.RESET}'
            )

            if source_line:
                line_num = str(self.location.line)
                padding = ' ' * len(line_num)

                # Empty line with pipe
                lines.append(f'   {c.BLUE}{padding}|{c.RESET}')

                # Source line with line number
                lines.append(f'   {c.BLUE}{line_num}|{c.RESET} {source_line}')

                # Underline carets
                if self.location.column is not None:
                    # Calculate underline position and width
                    start_col = self.location.column
                    end_col = self.location.end_column or (start_col + 1)
                    width = max(1, end_col - start_col)
                    underline = ' ' * start_col + '^' * width
                    lines.append(
                        f'   {c.BLUE}{padding}|{c.RESET} {c.RED}{underline}{c.RESET}'
                    )
                else:
                    # Underline the whole line (trimmed)
                    stripped = source_line.lstrip()
                    indent = len(source_line) - len(stripped)
                    underline = ' ' * indent + '^' * len(stripped)
                    lines.append(
                        f'   {c.BLUE}{padding}|{c.RESET} {c.RED}{underline}{c.RESET}'
                    )

        # Notes (support multi-line notes with continuation indentation)
        for note in self.notes:
            note_lines = note.split('\n')
            lines.append(
                f'   {c.BLUE}={c.RESET} {c.BOLD}{c.BLUE}note{c.RESET}: {note_lines[0]}'
            )
            for note_line in note_lines[1:]:
                lines.append(f'          {note_line}')

        # Help text (with blank line separator for visual clarity)
        if self.help_text:
            lines.append('')  # Blank line before help
            help_lines = self.help_text.split('\n')
            lines.append(f'   {c.BLUE}={c.RESET} {c.BOLD}{c.GREEN}help{c.RESET}:')
            for help_line in help_lines:
                lines.append(f'        {help_line}')

        return '\n'.join(lines)

    def __str__(self) -> str:
        """String representation uses plain text (no ANSI colors).

        Colors are only used when printing directly to terminal via
        the custom exception hook. This ensures the string is safe for:
        - Logging frameworks
        - JSON serialization
        - Database storage
        - Non-terminal contexts
        """
        return self.format_rust_style(use_colors=False)


class _NoColors:
    """No-op color codes for non-TTY output."""

    RESET = ''
    BOLD = ''
    RED = ''
    BLUE = ''
    CYAN = ''
    GREEN = ''
    YELLOW = ''
    DIM = ''


# Store original excepthook
_original_excepthook = sys.excepthook


def _horsies_excepthook(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_tb: Any,
) -> None:
    """Custom exception hook for HorsiesError exceptions."""
    # HORSIES_PLAIN_ERRORS=1 bypasses custom formatting entirely
    if _should_use_plain_errors():
        _original_excepthook(exc_type, exc_value, exc_tb)
        return

    if isinstance(exc_value, HorsiesError):
        # Print Rust-style formatted error
        print(exc_value.format_rust_style(), file=sys.stderr)

        # Show full traceback in verbose mode
        if _should_show_verbose():
            print(file=sys.stderr)
            c = _Colors if _should_use_colors() else _NoColors
            print(
                f'{c.DIM}Full traceback (HORSIES_VERBOSE=1):{c.RESET}',
                file=sys.stderr,
            )
            traceback.print_exception(exc_type, exc_value, exc_tb, file=sys.stderr)
    else:
        # Use original handler for non-horsies exceptions
        _original_excepthook(exc_type, exc_value, exc_tb)


def install_error_handler() -> None:
    """Install the custom exception hook for Rust-style error display."""
    sys.excepthook = _horsies_excepthook


def uninstall_error_handler() -> None:
    """Restore the original exception hook."""
    sys.excepthook = _original_excepthook


# =============================================================================
# Specific Error Classes
# =============================================================================


@dataclass
class WorkflowValidationError(HorsiesError):
    """Raised when workflow specification is invalid."""

    pass


@dataclass
class TaskDefinitionError(HorsiesError):
    """Raised when task definition is invalid."""

    pass


@dataclass
class ConfigurationError(HorsiesError):
    """Raised when app/broker configuration is invalid."""

    pass


@dataclass
class RegistryError(HorsiesError):
    """Raised when task registry operation fails."""

    pass


# =============================================================================
# Phase-Gated Error Collection
# =============================================================================


class ValidationReport:
    """Collects multiple HorsiesError instances within a validation phase.

    Formats all collected errors together with a summary line,
    similar to rustc's multi-error output.
    """

    def __init__(self, phase_name: str) -> None:
        self.phase_name: str = phase_name
        self.errors: list[HorsiesError] = []

    def add(self, error: HorsiesError) -> None:
        """Append an error to the report."""
        self.errors.append(error)

    def has_errors(self) -> bool:
        """Return True if any errors were collected."""
        return len(self.errors) > 0

    def format_rust_style(self, use_colors: bool | None = None) -> str:
        """Format all collected errors, then append an aborting summary."""
        if use_colors is None:
            use_colors = _should_use_colors()

        c = _Colors if use_colors else _NoColors
        parts: list[str] = []

        for error in self.errors:
            parts.append(error.format_rust_style(use_colors=use_colors))

        count = len(self.errors)
        parts.append(
            f'\n{c.BOLD}{c.RED}error{c.RESET}: aborting due to {count} previous errors'
        )

        return '\n'.join(parts)

    def __str__(self) -> str:
        return self.format_rust_style(use_colors=False)


@dataclass
class MultipleValidationErrors(HorsiesError):
    """Wraps a ValidationReport containing 2+ errors.

    Raised when a validation phase collects multiple errors.
    Single errors are raised as their original type for backward
    compatibility with existing except clauses.
    """

    report: ValidationReport = field(default_factory=lambda: ValidationReport(''))

    def __post_init__(self) -> None:
        if not self.message:
            count = len(self.report.errors)
            self.message = f'aborting due to {count} previous errors'
        # Skip auto-location detection — location is per-error in the report
        super(HorsiesError, self).__init__(self.message)

    def format_rust_style(self, use_colors: bool | None = None) -> str:
        """Delegate formatting to the underlying report."""
        return self.report.format_rust_style(use_colors=use_colors)

    def __str__(self) -> str:
        return self.format_rust_style(use_colors=False)


def raise_collected(report: ValidationReport) -> None:
    """Raise collected errors following the backward-compat rule.

    - 0 errors: no-op (returns normally)
    - 1 error: raises the original error (preserves except clauses)
    - 2+ errors: raises MultipleValidationErrors wrapping the report
    """
    count = len(report.errors)
    if count == 0:
        return
    if count == 1:
        raise report.errors[0]
    raise MultipleValidationErrors(
        message=f'aborting due to {count} previous errors',
        report=report,
    )


# =============================================================================
# Helper Functions for Creating Errors
# =============================================================================


def _find_user_frame() -> Any | None:
    """Find the first frame outside of horsies internals.

    Walks up the call stack to find where user code called into horsies.
    """
    frame = inspect.currentframe()
    if frame is None:
        return None

    # Walk up the stack
    while frame is not None:
        filename = frame.f_code.co_filename

        # Skip synthetic frames (e.g., <string>, <module>)
        if filename.startswith('<'):
            frame = frame.f_back
            continue

        # Skip horsies internals and standard library
        if not filename.startswith(_HORSIES_PKG_DIR) and '/site-packages/' not in filename:
            return frame

        frame = frame.f_back

    return None


def workflow_validation_error(
    message: str,
    *,
    code: ErrorCode | None = None,
    notes: list[str] | None = None,
    help_text: str | None = None,
    location: SourceLocation | None = None,
) -> WorkflowValidationError:
    """Create a WorkflowValidationError with optional location auto-detection."""
    if location is None:
        user_frame = _find_user_frame()
        if user_frame is not None:
            location = SourceLocation.from_frame(user_frame)

    return WorkflowValidationError(
        message=message,
        code=code,
        location=location,
        notes=notes or [],
        help_text=help_text,
    )


def task_definition_error(
    message: str,
    *,
    code: ErrorCode | None = None,
    fn: Callable[..., Any] | None = None,
    notes: list[str] | None = None,
    help_text: str | None = None,
) -> TaskDefinitionError:
    """Create a TaskDefinitionError with location from function."""
    location = None
    if fn is not None:
        location = SourceLocation.from_function(fn)

    return TaskDefinitionError(
        message=message,
        code=code,
        location=location,
        notes=notes or [],
        help_text=help_text,
    )
