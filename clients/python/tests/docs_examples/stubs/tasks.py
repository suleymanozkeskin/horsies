"""Stand-in for the module `tasks` that the examples import their tasks from."""

from prelude import app
from horsies import TaskError, TaskResult


@app.task('send_welcome_email')
def send_welcome_email(*, address: str) -> TaskResult[None, TaskError]:
    return TaskResult(ok=None)
