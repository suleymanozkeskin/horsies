"""Stand-in for the module `tasks` that the examples import their tasks from."""

from prelude import app
from horsies import Ok, Result, TaskError


@app.task('send_welcome_email')
def send_welcome_email(*, address: str) -> Result[None, TaskError]:
    return Ok(None)
