"""Which build each side of a paired cell actually ran.

A paired latency limit compares the candidate against the released baseline.
Both sides import a package called `horsies`, and Python resolves that name
from the working directory before site-packages — so a baseline invoked from
the checkout imports the *candidate* while `importlib.metadata` continues to
report the baseline's version.

That failure produces deltas near zero and passes every limit. It flatters
rather than alarms, which is the direction nobody investigates.

The defence is two things, because either alone is insufficient:

- every baseline invocation sets ``PYTHONSAFEPATH``, keeping the working
  directory off ``sys.path``;
- every side asserts, at measurement time, which module it actually imported
  and which schema version that module declares.

The assertion is **symmetric**. A candidate that imports the venv's copy is
exactly as silent as a baseline that imports the checkout, and a guard that
only watches one direction is a guard against the failure you thought of.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

# 0.4.7 as published. The baseline's schema is a property of the release, so
# it is pinned: a baseline reporting anything else is not the baseline.
BASELINE_SCHEMA_VERSION: Final = 26

# The checkout's schema at the time this batch was declared. Bumping the
# product's schema is expected to fail this pin, which is the point — a
# candidate whose schema moved mid-batch is not comparable with cells measured
# before it moved.
CANDIDATE_SCHEMA_VERSION: Final = 30

_PROBE: Final = (
    'import json, horsies;'
    ' from horsies.core.schemas.migrations import SCHEMA_VERSION;'
    ' print(json.dumps({'
    '"module_path": horsies.__file__,'
    ' "schema_version": SCHEMA_VERSION}))'
)


class PairedSide(StrEnum):
    BASELINE = 'baseline'
    CANDIDATE = 'candidate'


class SideIdentityError(Exception):
    """A side did not import the build the comparison requires."""


@dataclass(frozen=True, slots=True)
class SideIdentity:
    """What a side imported, as measured rather than as intended."""

    side: PairedSide
    interpreter: str
    module_path: str
    schema_version: int
    expected_root: str
    expected_schema_version: int


def probe_side_identity(
    interpreter: Path,
    *,
    side: PairedSide,
    expected_root: Path,
    expected_schema_version: int,
    cwd: Path,
) -> SideIdentity:
    """Ask an interpreter what it imports, from the directory it will run in.

    `cwd` is the directory the measurement itself runs from, because the
    hazard is a property of the working directory. Probing from somewhere
    safer than the measurement would answer a question nobody asked.
    """
    environment = dict(os.environ)
    environment['PYTHONSAFEPATH'] = '1'
    completed = subprocess.run(
        [str(interpreter), '-c', _PROBE],
        capture_output=True,
        text=True,
        cwd=str(cwd),
        env=environment,
        check=False,
    )
    if completed.returncode != 0:
        raise SideIdentityError(
            f'{side} interpreter {interpreter} could not import horsies: '
            f'{completed.stderr.strip()[:400]}'
        )
    try:
        payload = json.loads(completed.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError) as error:
        raise SideIdentityError(
            f'{side} identity probe returned unparseable output: '
            f'{completed.stdout.strip()[:200]!r}'
        ) from error
    return SideIdentity(
        side=side,
        interpreter=str(interpreter),
        module_path=str(payload['module_path']),
        schema_version=int(payload['schema_version']),
        expected_root=str(expected_root),
        expected_schema_version=expected_schema_version,
    )


def assert_side_identity(identity: SideIdentity) -> None:
    """Refuse a side that imported the wrong build.

    Both halves are checked. The path proves which files ran; the schema
    version proves the path is not a coincidence of layout — two builds can
    live under similar paths, and only one of them declares each version.
    """
    resolved = Path(identity.module_path).resolve()
    expected = Path(identity.expected_root).resolve()
    if not resolved.is_relative_to(expected):
        raise SideIdentityError(
            f'the {identity.side} side imported {resolved}, which is not '
            f'under {expected}. The working directory shadowed the intended '
            'build, so this side is not the build it claims to be and its '
            'delta would compare a build against itself'
        )
    if identity.schema_version != identity.expected_schema_version:
        raise SideIdentityError(
            f'the {identity.side} side declares schema version '
            f'{identity.schema_version}, expected '
            f'{identity.expected_schema_version}. The import path looked '
            'right, so this is a different build in the expected location '
            'rather than a shadowing failure'
        )


def assert_sides_differ(
    baseline: SideIdentity, candidate: SideIdentity
) -> None:
    """The two sides must be two builds.

    Each side can satisfy its own assertion while both resolve to the same
    files, if the expected roots were configured to the same place. A paired
    comparison of one build against itself is the failure this module exists
    to prevent, so it is checked directly rather than inferred from the two
    halves passing.
    """
    if Path(baseline.module_path).resolve() == Path(
        candidate.module_path
    ).resolve():
        raise SideIdentityError(
            'both sides imported the same module '
            f'({baseline.module_path}); the comparison would measure one '
            'build against itself and report deltas near zero'
        )
    if baseline.schema_version == candidate.schema_version:
        raise SideIdentityError(
            'both sides declare schema version '
            f'{baseline.schema_version}; the released baseline and the '
            'candidate are expected to differ, and equal versions mean one '
            'side is not the build it claims to be'
        )


def side_conditions(
    baseline: SideIdentity, candidate: SideIdentity
) -> dict[str, dict[str, str | int]]:
    """What the artifact records about which builds were compared."""
    return {
        side.side.value: {
            'interpreter': side.interpreter,
            'module_path': side.module_path,
            'schema_version': side.schema_version,
        }
        for side in (baseline, candidate)
    }
