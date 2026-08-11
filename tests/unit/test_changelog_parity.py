"""The two changelogs carry the same unreleased body.

`CHANGELOG.md` and the website's changelog are written by convention to
hold identical entries. The convention is memory-dependent, and memory
has lost twice: an entry landed in one file and not the other, both
times found by a later reader rather than by the change that made them
diverge.

This makes the convention an invariant the suite owns. It compares the
Unreleased sections by their words, so reflowing a paragraph is free
while a missing or extra entry fails. Absent from both passes: on
release day the section is retitled to a version in both files at once.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

_REPO_ROOT = Path(__file__).resolve().parents[2]
ROOT_CHANGELOG = _REPO_ROOT / 'CHANGELOG.md'
SITE_CHANGELOG = (
    _REPO_ROOT / 'website' / 'src' / 'content' / 'docs' / 'changelog.md'
)

# `## [Unreleased]` in the root file, `## Unreleased` on the website.
_UNRELEASED_HEADING = re.compile(
    r'^##\s+\[?Unreleased\]?\s*$', re.IGNORECASE | re.MULTILINE
)
_NEXT_HEADING = re.compile(r'^##\s+', re.MULTILINE)


def _unreleased_body(path: Path) -> str | None:
    """The Unreleased section of `path`, or None when it has none."""
    text = path.read_text(encoding='utf-8')
    heading = _UNRELEASED_HEADING.search(text)
    if heading is None:
        return None
    rest = text[heading.end():]
    following = _NEXT_HEADING.search(rest)
    return rest if following is None else rest[: following.start()]


def _words(body: str) -> list[str]:
    """The body as words, so wrapping differences are not differences."""
    return body.split()


def test_both_changelogs_exist() -> None:
    """Guard the premise: a missing file would make parity vacuous."""
    assert ROOT_CHANGELOG.is_file(), ROOT_CHANGELOG
    assert SITE_CHANGELOG.is_file(), SITE_CHANGELOG


def test_unreleased_sections_are_body_identical() -> None:
    root = _unreleased_body(ROOT_CHANGELOG)
    site = _unreleased_body(SITE_CHANGELOG)

    if root is None and site is None:
        return  # Released: both files retitled the section together.

    assert root is not None, (
        'the website changelog has an Unreleased section and the root '
        'changelog does not; entries must land in both'
    )
    assert site is not None, (
        'the root changelog has an Unreleased section and the website '
        'changelog does not; entries must land in both. This is the '
        'exact divergence the convention keeps losing to'
    )

    root_words = _words(root)
    site_words = _words(site)
    if root_words == site_words:
        return

    only_root = sorted(set(root_words) - set(site_words))
    only_site = sorted(set(site_words) - set(root_words))
    raise AssertionError(
        'the changelogs\' Unreleased sections differ.\n'
        f'  root: {len(root_words)} words, website: {len(site_words)}\n'
        f'  only in root:    {only_root[:12]}\n'
        f'  only in website: {only_site[:12]}'
    )
