# showcase/acme/workflows/catalog_import.py
"""A long import, built to be cancelled.

Forty independent chunk nodes on the `analytics` queue, which is capped at two
concurrent tasks, each chunk taking about eight seconds. Nothing depends on
anything else — the shape is a fan-out, not a chain — so at any moment two
chunks are RUNNING and the rest are PENDING.

Cancel it from the dashboard mid-run and both halves of cancellation are
visible at once: every PENDING chunk goes SKIPPED immediately, while the two
RUNNING chunks are left to drain rather than killed. The workflow reaches
CANCELLED when they finish.
"""

from __future__ import annotations

from typing import Any, Final

from horsies import (
    Horsies,
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)

from .. import tuning
from ..app import app
from ..tasks.analytics import catalog_import_chunk


class CatalogImport(WorkflowDefinition[None]):
    """Fan out a catalog import over many slow chunks.

    Declared `WorkflowDefinition[None]` with no `Meta.output`: there is no
    single result worth naming, and `handle.get()` returns the terminal results
    of every chunk keyed by node id.
    """

    name = 'catalog_import'
    definition_key = 'acme.catalog_import.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        import_id: str,
        chunks: int,
        *_args: Any,
        **_kwargs: Any,
    ) -> WorkflowSpec[Any]:
        """Build a fresh spec with `chunks` independent nodes.

        Returns `WorkflowSpec[Any]`: an outputless workflow's `handle.get()`
        yields the terminal results of every node keyed by node id, not one
        typed value, which is exactly what `WorkflowDefinition[None]` declares.
        """
        nodes: list[TaskNode[Any] | SubWorkflowNode[Any]] = [
            TaskNode(
                fn=catalog_import_chunk,
                kwargs={'import_id': import_id, 'chunk_index': index},
                node_id=f'chunk_{index:02d}',
            )
            for index in range(chunks)
        ]
        return app.workflow(
            name=cls.name,
            tasks=nodes,
            on_error=OnError.FAIL,
        )


_CHECK_IMPORT: Final[str] = 'IMPORT-CHECK'


@app.workflow_builder(
    cases=[{'import_id': _CHECK_IMPORT, 'chunks': tuning.CATALOG_IMPORT_CHUNKS}],
)
def build_catalog_import(*, import_id: str, chunks: int) -> WorkflowSpec[Any]:
    """Build the catalog-import fan-out."""
    return CatalogImport.build_with(app, import_id=import_id, chunks=chunks)
