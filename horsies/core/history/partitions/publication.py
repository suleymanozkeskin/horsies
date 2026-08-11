"""The seam between partition lifecycle and the staged lookup loader.

The ratified loader amendment makes the partition manager responsible for
regenerating the database-owned staged lookup function whenever the finite
leaf set changes, under one invariant: no snapshot may exist in which a
retained row is invisible to the loader, and no dropped relation may remain
probed by it. Concretely:

- attach publishes the new leaf into the function before the leaf's time
  window opens, so the first row routed into it is already loadable;
- retirement regenerates the function without the leaf after detach and
  before drop, and drop is refused while the published function still
  references the relation.

The loader generator is a separate module; this protocol is the only
coupling. `UnpublishedLoader` is the explicit pre-installation state — it is
correct only while no staged loader function has ever been published, and
exists so the drop path has no optional-dependency branch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncConnection


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


class LoaderPublication(Protocol):
    """What the partition manager requires of the staged-loader owner.

    `republish` takes no manifest: the loader probes the finite leaves of
    every retention class, so the publisher assembles the complete attached
    set from the catalog itself inside the caller's transaction. A manifest
    scoped to the mutating class would silently drop every other class's
    leaves from the regenerated function — false absence by construction.
    """

    async def republish(
        self, connection: AsyncConnection
    ) -> LoaderRepublished:
        """Regenerate the staged lookup function from the full catalog."""
        ...

    async def references_leaf(
        self,
        connection: AsyncConnection,
        leaf_name: str,
    ) -> bool:
        """Whether the currently published function still probes `leaf_name`."""
        ...


class UnpublishedLoader:
    """The staged loader before its first publication.

    Valid only while no generated lookup function exists in the database.
    Once the loader module publishes, maintenance must be constructed with
    the real publisher; using this one past that point would let a drop
    proceed while the function still references the relation.
    """

    async def republish(
        self, connection: AsyncConnection
    ) -> LoaderRepublished:
        return LoaderRepublished(absent_leaves=())

    async def references_leaf(
        self,
        connection: AsyncConnection,
        leaf_name: str,
    ) -> bool:
        return False
