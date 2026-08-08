"""Which build each side of a paired cell actually ran.

A paired latency limit compares the candidate against the released baseline.
Both sides import a package called `horsies`, and Python resolves that name
from the working directory before site-packages — so a baseline invoked from
the checkout imports the *candidate* while `importlib.metadata` continues to
report the baseline's version.

That failure produces deltas near zero and passes every limit. It flatters
rather than alarms, which is the direction nobody investigates.

**The measurement reports its own identity.** The marker line below is emitted
by the measurement process itself, from inside the same interpreter and the
same working directory that produced the numbers, and the harness asserts on
what that line says.

An earlier version of this module probed the sides in a separate subprocess
and set the protective environment variable itself. That detector could not
see the failure it existed for: with the flag baked into the probe, a probe
always resolved correctly while the measurement that dropped the flag imported
the checkout, and the probe vouched for it. Probe and measurement were two
owners of one setting, and their divergence was the hazard. A measurand that
reports itself cannot diverge from itself.

The environment is therefore built in one place and passed to whatever runs —
never constructed inside a checker.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

# 0.4.7 as published. The baseline's schema is a property of the release, so
# it is pinned: a baseline reporting anything else is not the baseline.
BASELINE_SCHEMA_VERSION: Final = 26

# The checkout's schema for this batch. A product schema bump is expected to
# fail this pin, which is the point: a candidate whose schema moved mid-batch is
# not comparable with cells measured before it moved.
#
# Advanced 30 -> 33 deliberately, after the completeness unit merged. Advancing
# it does not carry the cells measured at 30 across with it — each row's own
# files decide that, by transitive import closure over what the row executes:
#
#   ordinary/keyed enqueue   97 files, 24 changed  -> re-run
#   claim                   100 files, 25 changed  -> re-run
#   no-blocking leaf creation 11 files,  0 changed  -> carries
#
# The closure over-approximates execution, so it proves a row CARRIES when
# nothing in it moved, and only says 'cannot rule out' when something did. Both
# readings point the safe way.
CANDIDATE_SCHEMA_VERSION: Final = 33

SIDE_IDENTITY_MARKER: Final = '__horsies_side_identity__'

# Prepended to every measurement body. It is a source string rather than an
# import because the baseline must not import anything from this checkout —
# importing from here is the very thing the protection prevents.
SIDE_IDENTITY_SNIPPET: Final = f"""
import json as _json, horsies as _horsies
from horsies.core.schemas.migrations import (
    SCHEMA_VERSION as _schema_version,
)
print(
    '{SIDE_IDENTITY_MARKER} '
    + _json.dumps(
        {{
            'module_path': _horsies.__file__,
            'schema_version': _schema_version,
        }}
    ),
    flush=True,
)
"""


class PairedSide(StrEnum):
    BASELINE = 'baseline'
    CANDIDATE = 'candidate'


class SideIdentityError(Exception):
    """A side did not import the build the comparison requires."""


@dataclass(frozen=True, slots=True)
class SideIdentity:
    """What a side imported, as reported by the run that measured it."""

    side: PairedSide
    interpreter: str
    module_path: str
    schema_version: int
    expected_root: str
    expected_schema_version: int


def measurement_environment(
    *, base: Mapping[str, str] | None = None, protect_import_path: bool = True
) -> dict[str, str]:
    """The one authority over how a side's process is launched.

    Everything that runs a side takes its environment from here, so there is
    no second place where the protection could be present or absent. The
    parameter exists so a revert-proof can remove the protection through the
    shipped path rather than by editing it.
    """
    environment = dict(os.environ if base is None else base)
    if protect_import_path:
        environment['PYTHONSAFEPATH'] = '1'
    else:
        environment.pop('PYTHONSAFEPATH', None)
    return environment


def run_side(
    interpreter: Path,
    body: str,
    *,
    environment: Mapping[str, str],
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    """Run one side's work, with its identity emitted by the run itself."""
    return subprocess.run(
        [str(interpreter), '-c', SIDE_IDENTITY_SNIPPET + body],
        capture_output=True,
        text=True,
        cwd=str(cwd),
        env=dict(environment),
        check=False,
    )


def side_identity_from_output(
    output: str,
    *,
    side: PairedSide,
    interpreter: Path,
    expected_root: Path,
    expected_schema_version: int,
) -> SideIdentity:
    """Read the identity the measurement process reported about itself."""
    for line in output.splitlines():
        if not line.startswith(SIDE_IDENTITY_MARKER):
            continue
        try:
            payload = json.loads(line[len(SIDE_IDENTITY_MARKER) :].strip())
        except ValueError as error:
            raise SideIdentityError(
                f'{side} emitted an unparseable identity line: {line[:200]!r}'
            ) from error
        return SideIdentity(
            side=side,
            interpreter=str(interpreter),
            module_path=str(payload['module_path']),
            schema_version=int(payload['schema_version']),
            expected_root=str(expected_root),
            expected_schema_version=expected_schema_version,
        )
    raise SideIdentityError(
        f'{side} produced no identity line. The measurement must report the '
        'build it imported; output that omits it cannot be attributed to a '
        'build and is not usable as a measurement'
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
