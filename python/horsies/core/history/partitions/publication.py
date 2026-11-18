"""The answer of one reader publication (``horsies_publish_history_readers``):
the history leaves that the published readers had to leave out.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LoaderRepublished:
    """One republication, and what it had to leave out.

    `absent_leaves` names catalog rows the manager still believes attached
    whose relation no longer exists. They are excluded from the probe list
    because a generated function naming them dies at execution; they are
    reported here because the exclusion heals a fleet-wide read failure
    into a silent one, and an operator has to learn that a leaf vanished.
    Empty on every healthy republication.
    """

    absent_leaves: tuple[str, ...]


