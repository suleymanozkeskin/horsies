"""Unit tests for the extractor that feeds the docs pyright check.

Each marker outcome and each error of ``read_blocks`` has one test. The last
class runs the extractor over the real docs: a new fragment that is neither
valid Python nor marked ``skip`` fails here, before the pyright step runs.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from horsies.core.types.result import Err, Ok
from tests.docs_examples.extract import (
    Block,
    BlockMode,
    ExtractError,
    MarkerWithoutPythonFence,
    SkipWithoutReason,
    UnknownMarker,
    UnparsableBlock,
    describe_error,
    extract_all,
    find_pages,
    read_blocks,
    render_page_files,
)

pytestmark = pytest.mark.unit

PAGE = Path('docs/page.md')
MDX_PAGE = Path('docs/page.mdx')


def _blocks(text: str, page: Path = PAGE) -> tuple[Block, ...]:
    match read_blocks(page, text):
        case Ok(blocks):
            return blocks
        case Err(errors):
            raise AssertionError(f'unexpected errors: {errors}')


def _errors(text: str, page: Path = PAGE) -> tuple[ExtractError, ...]:
    match read_blocks(page, text):
        case Err(errors):
            return errors
        case Ok(blocks):
            raise AssertionError(f'expected errors, got blocks: {blocks}')


class TestReadBlocks:
    def test_unmarked_block_is_chained(self) -> None:
        blocks = _blocks('text\n\n```python\nx = 1\n```\n')

        assert blocks == (Block(page=PAGE, line=3, mode=BlockMode.CHAINED, code='x = 1'),)

    def test_other_languages_are_ignored(self) -> None:
        assert _blocks('```bash\nls\n```\n```json\n{}\n```\n') == ()

    def test_alone_marker(self) -> None:
        blocks = _blocks('<!-- check: alone -->\n```python\nx = 1\n```\n')

        assert blocks[0].mode is BlockMode.ALONE

    def test_skip_marker_with_reason_allows_invalid_code(self) -> None:
        blocks = _blocks('<!-- check: skip -- pseudo-code -->\n```python\nx = ???\n```\n')

        assert blocks[0].mode is BlockMode.SKIP

    def test_mdx_marker_form(self) -> None:
        blocks = _blocks('{/* check: skip -- pseudo-code */}\n```python\nx = ???\n```\n', MDX_PAGE)

        assert blocks[0].mode is BlockMode.SKIP

    def test_indented_fence_is_dedented(self) -> None:
        blocks = _blocks('- item\n\n  ```python\n  if x:\n      y = 1\n  ```\n')

        assert blocks[0].code == 'if x:\n    y = 1'

    def test_skip_without_reason_is_an_error(self) -> None:
        errors = _errors('<!-- check: skip -->\n```python\nx = 1\n```\n')

        assert errors == (SkipWithoutReason(page=PAGE, line=1),)

    def test_unknown_marker_is_an_error(self) -> None:
        errors = _errors('<!-- check: ignore -- why -->\n```python\nx = 1\n```\n')

        assert errors == (UnknownMarker(page=PAGE, line=1, text='<!-- check: ignore -- why -->'),)

    def test_marker_above_prose_is_an_error(self) -> None:
        errors = _errors('<!-- check: alone -->\nSome text.\n```python\nx = 1\n```\n')

        assert errors == (MarkerWithoutPythonFence(page=PAGE, line=1),)

    def test_marker_above_another_language_is_an_error(self) -> None:
        errors = _errors('<!-- check: alone -->\n```bash\nls\n```\n')

        assert errors == (MarkerWithoutPythonFence(page=PAGE, line=1),)

    def test_marker_at_the_end_of_the_page_is_an_error(self) -> None:
        errors = _errors('text\n<!-- check: alone -->\n')

        assert errors == (MarkerWithoutPythonFence(page=PAGE, line=2),)

    def test_unmarked_invalid_code_is_an_error_with_every_block_listed(self) -> None:
        errors = _errors('```python\nx = ???\n```\n\n```python\ny = ???\n```\n')

        assert [type(e) for e in errors] == [UnparsableBlock, UnparsableBlock]
        assert [getattr(e, 'line') for e in errors] == [1, 5]

    def test_every_error_names_page_and_line(self) -> None:
        errors = _errors('```python\nx = ???\n```\n')

        assert describe_error(errors[0]).startswith('docs/page.md:1:')


class TestRenderPageFiles:
    def test_chained_blocks_share_one_file_in_page_order(self) -> None:
        text = '```python\nx = 1\n```\n\n```python\ny = x\n```\n'

        files = render_page_files(_blocks(text))

        assert list(files) == ['docs_page.py']
        body = files['docs_page.py']
        assert body.startswith('from prelude import *')
        assert body.index('x = 1') < body.index('y = x')
        assert '# source: docs/page.md:1' in body and '# source: docs/page.md:5' in body

    def test_alone_block_gets_its_own_file(self) -> None:
        text = '```python\nx = 1\n```\n<!-- check: alone -->\n```python\ny = 2\n```\n'

        files = render_page_files(_blocks(text))

        assert sorted(files) == ['docs_page.py', 'docs_page__line5.py']
        assert 'y = 2' not in files['docs_page.py']
        assert 'x = 1' not in files['docs_page__line5.py']

    def test_skipped_block_is_not_written(self) -> None:
        text = '<!-- check: skip -- pseudo-code -->\n```python\nx = ???\n```\n'

        assert render_page_files(_blocks(text)) == {}

    def test_top_level_await_wraps_the_whole_page_file(self) -> None:
        text = '```python\nx = 1\n```\n\n```python\ny = await f(x)\n```\n'

        body = render_page_files(_blocks(text))['docs_page.py']

        assert 'async def _example() -> None:' in body
        assert '    x = 1' in body and '    y = await f(x)' in body

    def test_code_without_await_is_not_wrapped(self) -> None:
        body = render_page_files(_blocks('```python\nx = 1\n```\n'))['docs_page.py']

        assert 'async def' not in body


class TestExtractAll:
    def test_stale_output_files_are_removed(self, tmp_path: Path) -> None:
        output = tmp_path / 'out'
        output.mkdir()
        (output / 'old.py').write_text('x = 1\n')
        (tmp_path / 'website' / 'src' / 'content' / 'docs').mkdir(parents=True)

        errors = extract_all(tmp_path, output)

        assert errors == ()
        assert list(output.glob('*.py')) == []


class TestFindPages:
    def test_changelog_is_not_a_page(self, tmp_path: Path) -> None:
        docs = tmp_path / 'website' / 'src' / 'content' / 'docs'
        docs.mkdir(parents=True)
        (docs / 'changelog.md').write_text('```python\nx = ???\n```\n')
        (docs / 'cli.md').write_text('text')

        assert find_pages(tmp_path) == (Path('website/src/content/docs/cli.md'),)


class TestRealDocs:
    def test_every_block_of_the_docs_and_skills_is_valid_or_skipped(self, tmp_path: Path) -> None:
        repo_root = Path(__file__).resolve().parents[2]

        errors = extract_all(repo_root, tmp_path)

        assert errors == (), '\n'.join(describe_error(e) for e in errors)
        assert len(list(tmp_path.glob('*.py'))) > 0
