"""The log format setting of `AppConfig`: `TextLogs` or `JsonLogs`.

The horsies processes that load the app write the records of the `horsies`
loggers in this format: `horsies worker`, `horsies scheduler`,
`horsies check`, and each worker child.
"""

from __future__ import annotations

from typing import Annotated, ClassVar, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class TextLogs(BaseModel):
    """One line of text per record: time, component, level, message. The
    default."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['text_logs'] = 'text_logs'


class JsonLogs(BaseModel):
    """One JSON object per line, with the same keys in every record
    (`horsies.core.logging.JsonLogsFormatter`)."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra='forbid', frozen=True)

    kind: Literal['json_logs'] = 'json_logs'


LogFormat = Annotated[
    Union[TextLogs, JsonLogs],
    Field(discriminator='kind'),
]
"""The log format setting of `AppConfig.log_format`: `TextLogs` or
`JsonLogs`."""
