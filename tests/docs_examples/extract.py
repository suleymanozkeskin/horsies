"""Extract the python code blocks of the docs and the skill files into checkable files.

The docs pyright check type-checks the examples that the docs show. This
module reads every page, applies the block markers, and writes one file per
page. Pyright then runs over the written files (``pyrightconfig.docs.json``).

Markers. A marker is a comment on the line directly above the opening fence,
in the style of the ``members`` markers: ``<!-- check: skip -- reason -->`` in
``.md`` and ``{/* check: skip -- reason */}`` in ``.mdx``.

- No marker: the block is chained. The chained blocks of one page are written
  to one file in page order, so a block sees the names of the blocks above it.
- ``check: alone``: the block is written to its own file and sees no other block.
- ``check: skip -- reason``: the block is not written. The reason is required.

A chained or ``alone`` block that does not parse is an error, so a new
fragment cannot enter the docs unmarked.

Run: ``uv run python tests/docs_examples/extract.py OUTPUT_DIR``. Exit code 1
when an error is found, with one line per error.
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final, assert_never

from horsies.core.types.result import Err, Ok, Result


DOC_ROOTS: Final = (
    Path('website') / 'src' / 'content' / 'docs',
    Path('horsies') / '.agents' / 'skills',
)
PAGE_SUFFIXES: Final = frozenset({'.md', '.mdx'})
# The changelog shows past versions and has a twin file in the repo root.
HISTORY_PAGES: Final = frozenset({'changelog.md'})
PRELUDE_IMPORT: Final = 'from prelude import *  # noqa: F403\n'

_FENCE: Final = re.compile(r'^(?P<indent>\s*)```(?P<lang>\w*)')
_MARKER_LINE: Final = re.compile(
    r'^\s*(?:<!--\s*check:(?P<html>.*?)-->|\{/\*\s*check:(?P<mdx>.*?)\*/\})\s*$'
)
_MARKER_BODY: Final = re.compile(r'^\s*(?P<kind>\w+)\s*(?:--\s*(?P<reason>.*?))?\s*$')


class BlockMode(Enum):
    CHAINED = 'chained'
    ALONE = 'alone'
    SKIP = 'skip'


@dataclass(frozen=True, slots=True)
class Block:
    """One python code block with its page, fence line and mode."""

    page: Path
    line: int
    mode: BlockMode
    code: str


@dataclass(frozen=True, slots=True)
class UnknownMarker:
    page: Path
    line: int
    text: str

    def message(self) -> str:
        return f'{self.page}:{self.line}: unknown check marker: {self.text!r}'


@dataclass(frozen=True, slots=True)
class SkipWithoutReason:
    page: Path
    line: int

    def message(self) -> str:
        return f'{self.page}:{self.line}: "check: skip" needs a reason: "check: skip -- reason"'


@dataclass(frozen=True, slots=True)
class MarkerWithoutPythonFence:
    page: Path
    line: int

    def message(self) -> str:
        return f'{self.page}:{self.line}: check marker is not followed by a python fence'


@dataclass(frozen=True, slots=True)
class UnparsableBlock:
    page: Path
    line: int
    cause: str

    def message(self) -> str:
        return (
            f'{self.page}:{self.line}: block does not parse ({self.cause}). '
            'Fix it, or mark it with "check: skip -- reason".'
        )


ExtractError = UnknownMarker | SkipWithoutReason | MarkerWithoutPythonFence | UnparsableBlock


def describe_error(error: ExtractError) -> str:
    match error:
        case UnknownMarker() | SkipWithoutReason() | MarkerWithoutPythonFence() | UnparsableBlock():
            return error.message()
        case _:
            assert_never(error)


def _parse_marker(
    page: Path, line: int, text: str
) -> Result[BlockMode, UnknownMarker | SkipWithoutReason]:
    match _MARKER_LINE.match(text):
        case None:
            return Err(UnknownMarker(page=page, line=line, text=text.strip()))
        case found:
            body = found.group('html') if found.group('html') is not None else found.group('mdx')
    parsed = _MARKER_BODY.match(body)
    if parsed is None:
        return Err(UnknownMarker(page=page, line=line, text=text.strip()))
    reason = parsed.group('reason')
    match parsed.group('kind'):
        case 'alone':
            return Ok(BlockMode.ALONE)
        case 'skip' if reason:
            return Ok(BlockMode.SKIP)
        case 'skip':
            return Err(SkipWithoutReason(page=page, line=line))
        case _:
            return Err(UnknownMarker(page=page, line=line, text=text.strip()))


def _dedent(lines: list[str], indent: str) -> str:
    return '\n'.join(line[len(indent):] if line.startswith(indent) else line for line in lines)


def read_blocks(page: Path, text: str) -> Result[tuple[Block, ...], tuple[ExtractError, ...]]:
    """Return the python blocks of one page, with their modes.

    Fails on an unknown marker, a skip without a reason, a marker that is not
    directly above a python fence, or an unskipped block that does not parse.
    A marker error stops the read at once. Unparsable blocks are all reported.
    """
    lines = text.split('\n')
    blocks: list[Block] = []
    index = 0
    pending: tuple[int, BlockMode] | None = None
    while index < len(lines):
        raw = lines[index]
        if _MARKER_LINE.match(raw) or raw.lstrip().startswith(('<!-- check:', '{/* check:')):
            if pending is not None:
                return Err((MarkerWithoutPythonFence(page=page, line=pending[0]),))
            match _parse_marker(page, index + 1, raw):
                case Err(error):
                    return Err((error,))
                case Ok(mode):
                    pending = (index + 1, mode)
            index += 1
            continue
        fence = _FENCE.match(raw)
        if fence is None:
            if pending is not None and raw.strip():
                return Err((MarkerWithoutPythonFence(page=page, line=pending[0]),))
            index += 1
            continue
        end = index + 1
        while end < len(lines) and not lines[end].strip().startswith('```'):
            end += 1
        if fence.group('lang') in ('python', 'py'):
            mode = pending[1] if pending is not None else BlockMode.CHAINED
            code = _dedent(lines[index + 1 : end], fence.group('indent'))
            blocks.append(Block(page=page, line=index + 1, mode=mode, code=code))
        elif pending is not None:
            return Err((MarkerWithoutPythonFence(page=page, line=pending[0]),))
        pending = None
        index = end + 1
    if pending is not None:
        return Err((MarkerWithoutPythonFence(page=page, line=pending[0]),))
    unparsable: list[ExtractError] = []
    for block in blocks:
        if block.mode is BlockMode.SKIP:
            continue
        try:
            ast.parse(block.code)
        except SyntaxError as exc:
            unparsable.append(UnparsableBlock(page=page, line=block.line, cause=exc.msg))
    if unparsable:
        return Err(tuple(unparsable))
    return Ok(tuple(blocks))


def _needs_async_wrapper(code: str) -> bool:
    """Return True when the code uses ``await`` or ``return`` outside a function.

    Such code is valid in a notebook or as a fragment, but not as a module.
    The caller wraps it in an ``async def``.
    """
    try:
        compile(code, '<block>', 'exec')
    except SyntaxError:
        return True
    return False


def _wrap_in_async_function(text: str) -> str:
    indented = '\n'.join(f'    {line}' if line else line for line in text.split('\n'))
    return f'async def _example() -> None:\n{indented}\n'


def render_page_files(blocks: tuple[Block, ...]) -> dict[str, str]:
    """Map output file name to file text for the blocks of ONE page.

    Chained blocks go to ``<stem>.py``. Each ``alone`` block goes to
    ``<stem>__line<N>.py``. Each block follows a ``# source: path:line`` line.
    A file with a block that needs a function body (top-level ``await``) is
    wrapped whole in an ``async def``, so its blocks still share names.
    """
    bodies: dict[str, str] = {}
    wrapped: set[str] = set()
    for block in blocks:
        stem = _stem(block.page)
        source = f'# source: {block.page.as_posix()}:{block.line}\n{block.code}\n\n'
        match block.mode:
            case BlockMode.SKIP:
                continue
            case BlockMode.CHAINED:
                name = f'{stem}.py'
            case BlockMode.ALONE:
                name = f'{stem}__line{block.line}.py'
            case _:
                assert_never(block.mode)
        bodies[name] = bodies.get(name, '') + source
        if _needs_async_wrapper(block.code):
            wrapped.add(name)
    return {
        name: PRELUDE_IMPORT + '\n' + (_wrap_in_async_function(body) if name in wrapped else body)
        for name, body in bodies.items()
    }


def _stem(page: Path) -> str:
    return re.sub(r'\W+', '_', page.with_suffix('').as_posix()).strip('_')


def find_pages(repo_root: Path) -> tuple[Path, ...]:
    """Return the repo-relative paths of every docs page and skill file."""
    found: list[Path] = []
    for root in DOC_ROOTS:
        found.extend(
            path.relative_to(repo_root)
            for path in sorted((repo_root / root).rglob('*'))
            if path.suffix in PAGE_SUFFIXES and path.name not in HISTORY_PAGES
        )
    return tuple(found)


def extract_all(repo_root: Path, output_dir: Path) -> tuple[ExtractError, ...]:
    """Write the checkable files of every page to ``output_dir``. Return all errors.

    Python files that are already in ``output_dir`` are removed first, so no file
    of an earlier run is checked again.
    """
    errors: list[ExtractError] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for stale in output_dir.glob('*.py'):
        stale.unlink()
    for page in find_pages(repo_root):
        match read_blocks(page, (repo_root / page).read_text()):
            case Err(page_errors):
                errors.extend(page_errors)
            case Ok(blocks):
                for name, text in render_page_files(blocks).items():
                    (output_dir / name).write_text(text)
    return tuple(errors)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print('usage: extract.py OUTPUT_DIR', file=sys.stderr)
        return 2
    errors = extract_all(Path.cwd(), Path(argv[1]))
    for error in errors:
        print(describe_error(error), file=sys.stderr)
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
