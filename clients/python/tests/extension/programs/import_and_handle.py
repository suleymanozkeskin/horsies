"""Cases of the import of ``horsies._core`` and of a ``CoreHandle`` build:
neither starts the core. No case connects to a database.

The module imports no horsies module at its top, so that the import case
measures the process before the first horsies import.
"""

from __future__ import annotations

import json
from typing import Final

import psutil

from tests.extension.programs.report import JsonValue, Report, run_case

NEVER_CONNECTED_URL: Final = 'postgresql://extension@127.0.0.1:1/never_connected'
"""The database URL of the handles of these cases. No case connects."""


def import_starts_nothing() -> Report:
    """The OS thread count before and after ``import horsies._core``, and the
    core state after it."""
    process = psutil.Process()
    threads_before = process.num_threads()
    import horsies._core as core

    threads_after = process.num_threads()
    return {
        'threads_before_import': threads_before,
        'threads_after_import': threads_after,
        'core_state': core.core_state(),
    }


def handle_starts_nothing() -> Report:
    """The OS thread count before and after a ``CoreHandle`` build, and the
    core state after it."""
    import horsies._core as core
    from tests.extension.programs.support import manifest_json, os_thread_count

    manifest = manifest_json(NEVER_CONNECTED_URL, schema='MIGRATE_AND_VALIDATE', echo=False)
    threads_before = os_thread_count()
    handle = core.CoreHandle(manifest, pool_profile='PRODUCER')
    threads_after = os_thread_count()
    return {
        'threads_before_build': threads_before,
        'threads_after_build': threads_after,
        'core_state': core.core_state(),
        'sent_manifest_version': json.loads(manifest)['manifest_version'],
        'handle_manifest_version': handle.manifest_version,
    }


def refused_manifest() -> Report:
    """A manifest with an unknown field under ``database``: which handler of
    two nested ones catches the refusal (``except Exception`` inside,
    ``except ManifestRefused`` outside), its arguments, the classes of its
    method resolution order, and the core state."""
    import horsies._core as core
    from tests.extension.programs.support import manifest_json

    manifest = json.loads(
        manifest_json(NEVER_CONNECTED_URL, schema='MIGRATE_AND_VALIDATE', echo=False),
    )
    manifest['database']['extra'] = 1
    refused_text = json.dumps(manifest)
    caught_by = 'nothing'
    arguments: list[JsonValue] = []
    refusal_classes: list[JsonValue] = []
    try:
        try:
            _ = core.CoreHandle(refused_text, pool_profile='PRODUCER')
        except Exception as caught:
            caught_by = f'Exception: {type(caught).__name__}'
    except core.ManifestRefused as refused:
        caught_by = 'ManifestRefused'
        arguments = [str(argument) for argument in refused.args]
        refusal_classes = [cls.__name__ for cls in type(refused).__mro__]
    return {
        'caught_by': caught_by,
        'arguments': arguments,
        'refusal_classes': refusal_classes,
        'core_state': core.core_state(),
    }


if __name__ == '__main__':
    run_case(
        {
            'import_starts_nothing': import_starts_nothing,
            'handle_starts_nothing': handle_starts_nothing,
            'refused_manifest': refused_manifest,
        },
    )
