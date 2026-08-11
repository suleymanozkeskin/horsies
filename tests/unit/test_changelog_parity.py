"""The two changelogs carry the same unreleased body.

`CHANGELOG.md` and the website's changelog are written by convention to
hold identical entries. The convention is memory-dependent, and memory
has lost twice: an entry landed in one file and not the other, both
times found by a later reader rather than by the change that made them
diverge.

This makes the convention an invariant the suite owns. It compares
sections by their words, so reflowing a paragraph is free while a
missing or extra entry fails. Absent from both passes: on release day
the section is retitled to a version in both files at once.

Released sections are covered by their HEADINGS, not their bodies: the
set of released versions must match, so a version section landing in one
file only fails here.

Their bodies are deliberately NOT compared, and the reason is measured
rather than assumed. 18 of the 31 shared released sections already
differ, several substantially — 0.3.0 carries 731 words in the root file
against 442 on the website. The website's history is an abridgement, not
a copy, so a body comparison over it would fail on the state the repo
has shipped for its whole life and could only be silenced. Reconciling
that history, or ratifying the abridgement, is its own decision.

The limit that follows, stated so it is not mistaken for coverage: a
correction applied to one file's already-released section and not the
other's is NOT caught. Only Unreleased bodies and the released version
set are guarded.
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


# The root file writes `## [0.5.1] - 2026-08-10`; the website writes
# `## 0.5.1`. The version is the shared part, so it is the key.
_RELEASED_HEADING = re.compile(
    r'^##\s+\[?(\d+\.\d+\.\d+)\]?.*$', re.MULTILINE
)


def _released_sections(path: Path) -> dict[str, str]:
    """Every released section of `path`, keyed by its version."""
    text = path.read_text(encoding='utf-8')
    sections: dict[str, str] = {}
    for heading in _RELEASED_HEADING.finditer(text):
        rest = text[heading.end():]
        following = _NEXT_HEADING.search(rest)
        body = rest if following is None else rest[: following.start()]
        sections[heading.group(1)] = body
    return sections


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


def test_the_same_versions_are_released_in_both() -> None:
    """The set of released versions is part of the parity.

    A version section added to one file only would otherwise pass the
    body comparison below, which can only compare versions both files
    have.
    """
    root = set(_released_sections(ROOT_CHANGELOG))
    site = set(_released_sections(SITE_CHANGELOG))
    assert root, 'no released sections found; the heading pattern stopped matching'
    assert root == site, (
        'the changelogs list different released versions.\n'
        f'  only in root:    {sorted(root - site)}\n'
        f'  only in website: {sorted(site - root)}'
    )
