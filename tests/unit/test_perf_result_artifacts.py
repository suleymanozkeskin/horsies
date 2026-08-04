"""Committed measurement summaries have to stay readable by strangers.

A recorded result is cited in pull requests and read years later by people who
were not in the conversation that produced it. Internal shorthand — a phase
letter, a gate number, a pointer to a planning document that is not in the
repository — resolves for its author on the day and for nobody afterwards.
That makes the citation unverifiable, which is worse than no citation.

This checks the files rather than the intention, so the rule survives whoever
next adds a summary in a hurry.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

RESULTS_DIR = Path(__file__).resolve().parents[1] / 'perf' / 'results'

# Each pattern is a way a summary stops resolving for a later reader.
FORBIDDEN = (
    (re.compile(r'§'), 'a section reference into a document not in this repository'),
    (re.compile(r'\broadmap/'), 'a path that is not committed'),
    (re.compile(r'\bdecision record\b', re.IGNORECASE), 'an uncommitted document'),
    (re.compile(r'\b[TD]\d{1,2}\b'), 'an internal phase or gate identifier'),
)


def _summaries() -> list[Path]:
    return sorted(RESULTS_DIR.glob('*.md'))


class TestSummariesAreSelfContained:
    def test_results_directory_exists(self) -> None:
        """Measurements are committed here; the location is part of the rule."""
        assert RESULTS_DIR.is_dir()

    def test_no_summary_cites_something_a_reader_cannot_open(self) -> None:
        offences: list[str] = []
        for path in _summaries():
            text = path.read_text(encoding='utf-8')
            for pattern, why in FORBIDDEN:
                match = pattern.search(text)
                if match is not None:
                    offences.append(f'{path.name}: {match.group(0)!r} is {why}')
        assert not offences, '; '.join(offences)

    def test_every_summary_states_its_conditions(self) -> None:
        """A number without conditions is not evidence of anything."""
        required = ('## Conditions', '| server |', '| observations per side |')
        for path in _summaries():
            if path.name == 'README.md':
                continue
            text = path.read_text(encoding='utf-8')
            missing = [heading for heading in required if heading not in text]
            assert not missing, f'{path.name} omits {missing}'

    def test_raw_samples_are_excluded_from_the_repository(self) -> None:
        """They are large, unreadable, and belong in run artifact storage.

        Checked against the ignore rules rather than against what happens to
        be on disk, so it holds before anyone has run a measurement here.
        """
        completed = subprocess.run(
            ['git', 'check-ignore', '-q', str(RESULTS_DIR / 'raw' / 'run.json')],
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, (
            'tests/perf/results/raw is not ignored; a gate run would commit '
            'tens of thousands of samples'
        )
