# showcase/acme/scenarios/bulk_import.py
"""Start a long import and hand you its id, so you can cancel it.

    uv run python -m showcase.acme.scenarios bulk-import

Forty chunk nodes, each about eight seconds, on a queue capped at two
concurrent tasks. It runs for several minutes on purpose — long enough to open
the run, watch two chunks execute while the rest sit PENDING, and cancel it
from the dashboard.

Cancelling shows both halves of what cancel means: every PENDING chunk goes
SKIPPED at once, and the two RUNNING chunks are left to finish rather than
killed. The run reaches CANCELLED when they drain.
"""

from __future__ import annotations

from horsies import Err, Ok

from .. import tuning
from ..settings import DATABASE
from ..workflows.catalog_import import build_catalog_import
from . import WEB_BASE_URL, bullet, heading, say


def run() -> int:
    """Start one catalog import. Returns a process exit code."""
    heading('Acme Clothing — bulk import')
    say(f'database: {DATABASE.database_name}  (resolved from {DATABASE.source})')

    import_id = 'IMP-0001'
    spec = build_catalog_import(
        import_id=import_id,
        chunks=tuning.CATALOG_IMPORT_CHUNKS,
    )

    match spec.start():
        case Err(error):
            say(f'could not start the import: [{error.code}] {error.message}')
            return 1
        case Ok(handle):
            say(
                f'{import_id}: {tuning.CATALOG_IMPORT_CHUNKS} chunks started, '
                f'workflow {handle.workflow_id}'
            )

    heading('what to watch')
    bullet(f'{WEB_BASE_URL}/workflows?run={handle.workflow_id}')
    bullet(f'  queue analytics is capped at 2, so 2 chunks run and {tuning.CATALOG_IMPORT_CHUNKS - 2} wait')
    bullet('  cancel the run from that page while it is going')
    bullet('  PENDING chunks go SKIPPED immediately; RUNNING chunks drain first')
    bullet('  the run reaches CANCELLED once they finish — nothing is killed')
    return 0
