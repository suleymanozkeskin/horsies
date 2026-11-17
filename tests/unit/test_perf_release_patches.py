"""A release patch of the measurement harness changes that harness only.

``tests/perf/releases/<tag>/harness.patch`` makes the harness of a released
tag run on that release's schema, so a session can measure the release next
to the current branch. A patch that touched product code would measure a
build that was never released.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

RELEASES_DIR = Path(__file__).resolve().parents[1] / 'perf' / 'releases'
_FILE_HEADER = re.compile(r'^diff --git a/(\S+) b/(\S+)$', re.MULTILINE)


def _patches() -> list[Path]:
    return sorted(RELEASES_DIR.glob('*/harness.patch'))


def test_the_releases_directory_holds_a_patch() -> None:
    assert _patches(), f'no harness.patch under {RELEASES_DIR}'


@pytest.mark.parametrize('patch', _patches(), ids=lambda path: path.parent.name)
def test_a_release_patch_changes_only_the_perf_harness(patch: Path) -> None:
    targets = _FILE_HEADER.findall(patch.read_text(encoding='utf-8'))
    assert targets, f'{patch} names no file'
    outside = [
        new
        for old, new in targets
        if not (old.startswith('tests/perf/') and new.startswith('tests/perf/'))
    ]
    assert not outside, f'{patch} changes files outside tests/perf: {outside}'
