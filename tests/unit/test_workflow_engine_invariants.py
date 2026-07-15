"""Tripwire for the engine's transaction-ownership invariant.

engine.py must never manage transactions: node-FAILED, the on_error='pause'
transition, and the child-pause cascade run on the caller's session and
commit together. Recovery case 1.7 (crashed-worker replay) only matches
NON-terminal nodes, so a commit boundary inside the failure-handling path
would — on a crash in that window — leave a terminal node under a RUNNING
workflow that no recovery path repairs: the pause is permanently lost.
This is the exact pre-fix shape of the Rust port's C16 defect; the test
exists so a refactor cannot reintroduce it silently.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import horsies.core.workflows.engine as engine_module

pytestmark = [pytest.mark.unit]

# Tokens that would give engine.py its own transaction boundaries. Matched
# as call sites (trailing paren) so the words remain usable in comments.
FORBIDDEN_CALL_PATTERNS = [
    r'\.commit\(',
    r'\.rollback\(',
    r'\.begin\(',
    r'\.begin_nested\(',
    r'session_factory\(',
]


def test_engine_owns_no_transactions() -> None:
    """engine.py contains no commit/rollback/begin/session-factory call sites."""
    source = Path(engine_module.__file__).read_text(encoding='utf-8')
    violations: list[str] = []
    for pattern in FORBIDDEN_CALL_PATTERNS:
        for match in re.finditer(pattern, source):
            line_no = source.count('\n', 0, match.start()) + 1
            violations.append(f'{pattern} at engine.py:{line_no}')
    assert not violations, (
        'engine.py must not manage transactions (caller owns the single '
        'commit). A commit boundary between the node-FAILED CAS and the '
        "on_error='pause' transition loses the pause on a crash in that "
        'window (recovery case 1.7 only matches non-terminal nodes). '
        f'Found: {violations}'
    )
