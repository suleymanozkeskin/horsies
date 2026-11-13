"""The extras that install the C build of psycopg.

psycopg's own extras (`psycopg[binary]`, `psycopg[c]`) pin the C build to the
psycopg version. A C build of another version fails at import, so the horsies
extras go through them and never name `psycopg-binary` or `psycopg-c` alone.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Final, cast

import pytest

pytestmark = [pytest.mark.unit]

_PYPROJECT: Final = Path(__file__).resolve().parents[2] / 'pyproject.toml'


def _extra(name: str) -> list[str]:
    data = tomllib.loads(_PYPROJECT.read_text(encoding='utf-8'))
    requirements: object = data['project']['optional-dependencies'][name]
    assert isinstance(requirements, list)
    return [str(requirement) for requirement in cast('list[object]', requirements)]


@pytest.mark.parametrize(
    ('extra', 'psycopg_extra'),
    [('pg-binary', 'psycopg[binary]'), ('pg-c', 'psycopg[c]')],
)
def test_a_c_build_extra_goes_through_the_psycopg_extra(
    extra: str,
    psycopg_extra: str,
) -> None:
    requirements = _extra(extra)

    assert requirements
    assert all(requirement.startswith(psycopg_extra) for requirement in requirements)
