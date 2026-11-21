"""The RSS read of a worker child process (``_read_current_rss_mb``)."""

from __future__ import annotations

import os
from typing import Optional

from horsies.core.logging import get_logger

logger = get_logger('worker')

# Underscore names are deliberate: these are worker-internal symbols whose
# privacy boundary is the worker package, not this module.
__all__ = [
    '_read_current_rss_mb',
]


# Per-child latch so a broken RSS reader logs once, not once per task.
_rss_reader_failed_logged = False


def _read_current_rss_mb() -> Optional[float]:
    """Return the calling process's current RSS in MB, or ``None`` if unknown.

    Fail-open by contract: any measurement failure returns ``None`` (logged
    once per process) so the child worker loop treats RSS as unknown and does
    not recycle. An RSS read must never raise out of the worker loop, because
    a raise there would break the pool.

    Linux fast path reads ``/proc/self/statm`` (resident pages * page size);
    the fallback is ``psutil``. The two readers agree exactly (zero delta) on
    CPython 3.13/Linux.
    """
    global _rss_reader_failed_logged
    try:
        with open('/proc/self/statm', encoding='ascii') as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * os.sysconf('SC_PAGE_SIZE') / 1024 / 1024
    except (OSError, ValueError, IndexError):
        pass
    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    except Exception as exc:
        if not _rss_reader_failed_logged:
            _rss_reader_failed_logged = True
            logger.warning(
                'Current RSS unreadable (statm + psutil both failed); memory '
                'recycle / baseline guard disabled for this process: %s',
                exc,
            )
        return None


