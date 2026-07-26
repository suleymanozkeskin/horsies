# showcase/hemline/scenarios/chaos.py
"""Kill things and watch them come back.

    uv run python -m showcase.hemline.scenarios chaos

Two kinds of crash, one automatic and one yours to cause.

`flaky_export` kills its own child process with `os._exit(1)`. No exception is
raised and no result is returned — the worker simply loses the child, reports
`WORKER_CRASHED`, and the retry policy brings the task back. Half of all export
ids do this, and because the draw hashes the id, a crashing export crashes on
every attempt until its retries run out. That is the honest version: recovery
you can watch, not recovery you have to take on faith.

The second is the `kill -9` drill printed at the end. Killing a worker outright
is the case a visibility timeout cannot handle: tasks it had CLAIMED but never
started are requeued, and tasks that were RUNNING are recovered against their
retry policies.
"""

from __future__ import annotations

from horsies import Err, Ok

from .. import simulate, tuning
from ..settings import DATABASE
from ..tasks.analytics import flaky_export
from . import WEB_BASE_URL, bullet, heading, say


def run() -> int:
    """Send the crash-prone exports and print the kill drill."""
    heading('Hemline — chaos')
    say(f'database: {DATABASE.database_name}  (resolved from {DATABASE.source})')

    heading(f'{tuning.CHAOS_EXPORT_COUNT} exports, about half of which kill their child')
    bullet(
        f'spaced {tuning.CHAOS_EXPORT_SPACING_SECONDS} s apart — a self-killing task '
        'breaks the whole executor pool,'
    )
    bullet('  and a second one landing during the restart would stop the worker outright')
    say()

    crashing = 0
    for index in range(tuning.CHAOS_EXPORT_COUNT):
        export_id = f'EXP-{index:04d}'
        will_crash = simulate.draw(tuning.CHAOS_EXPORT_CRASH_RATE, export_id, 'crash')
        crashing += 1 if will_crash else 0
        delay = index * tuning.CHAOS_EXPORT_SPACING_SECONDS
        match flaky_export.schedule(delay, export_id=export_id):
            case Ok(handle):
                say(
                    f'{export_id}  +{delay:3d}s  {"CRASHES" if will_crash else "clean"}'
                    f'  ->  {WEB_BASE_URL}/?task={handle.task_id}'
                )
            case Err(error):
                say(f'{export_id}  send failed: [{error.code}] {error.message}')

    say()
    say(f'{crashing} of {tuning.CHAOS_EXPORT_COUNT} will kill their child process')
    say(
        f'the last one runs about '
        f'{(tuning.CHAOS_EXPORT_COUNT - 1) * tuning.CHAOS_EXPORT_SPACING_SECONDS} s '
        'from now, and its retries after that'
    )

    heading('what to watch')
    bullet(f'{WEB_BASE_URL}/?error_code=WORKER_CRASHED  attempts that lost their process')
    bullet('  open a crashing export: every attempt is recorded, none of them raised')
    bullet('  the task is retried by policy until its retries are spent, then FAILED')

    heading('the kill -9 drill')
    bullet('1. start steady in another terminal and let a few orders get going')
    bullet('2. find the worker:   pgrep -f "horsies worker showcase.hemline"')
    bullet('3. kill it outright:  kill -9 <pid>')
    bullet('4. watch the workers view: the worker stops reporting and goes stale')
    bullet('5. restart it:        uv run horsies worker showcase.hemline.app:app --processes 12')
    bullet('6. CLAIMED tasks are requeued — user code never ran, so it is safe')
    bullet('7. RUNNING tasks are recovered against their retry policies')
    bullet(f'{WEB_BASE_URL}/workers   worker health, CPU and memory history')
    return 0
