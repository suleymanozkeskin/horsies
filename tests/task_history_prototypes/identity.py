"""General enqueue-idempotency semantics exercised by storage prototypes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone


IDEMPOTENCY_KEY_MAX_BYTES = 255
IDEMPOTENCY_SCOPE_VERSION = 1
COMMAND_FINGERPRINT_VERSION = 1


@dataclass(frozen=True, slots=True)
class ScopedIdempotencyKey:
    task_name: str
    key: str

    def __post_init__(self) -> None:
        _validate_opaque_value('task_name', self.task_name)
        _validate_opaque_value('idempotency key', self.key)

    @property
    def digest(self) -> bytes:
        return _digest_framed(
            b'horsies.enqueue-key.v1',
            self.task_name.encode(),
            self.key.encode(),
        )


@dataclass(frozen=True, slots=True)
class EnqueueCommandV1:
    task_name: str
    queue_name: str
    priority: int
    args_json: str | None
    kwargs_json: str | None
    good_until: datetime | None
    enqueue_delay_seconds: int | None
    task_options_json: str | None
    retention_class_key: str
    rerun_of_task_id: str | None
    rerun_root_task_id: str | None

    def __post_init__(self) -> None:
        if not self.task_name:
            raise ValueError('task_name must be non-empty')
        if not self.queue_name:
            raise ValueError('queue_name must be non-empty')
        if not 1 <= self.priority <= 100:
            raise ValueError('priority must be between 1 and 100')
        if self.good_until is not None and self.good_until.tzinfo is None:
            raise ValueError('good_until must be timezone-aware')
        if self.enqueue_delay_seconds is not None and self.enqueue_delay_seconds < 0:
            raise ValueError('enqueue_delay_seconds must be non-negative')
        if not self.retention_class_key:
            raise ValueError('retention_class_key must be non-empty')
        if (self.rerun_of_task_id is None) != (self.rerun_root_task_id is None):
            raise ValueError('rerun source and root must be present together')

    @property
    def fingerprint(self) -> bytes:
        canonical = json.dumps(
            [
                COMMAND_FINGERPRINT_VERSION,
                self.task_name,
                self.queue_name,
                self.priority,
                self.args_json,
                self.kwargs_json,
                _canonical_datetime(self.good_until),
                self.enqueue_delay_seconds,
                self.task_options_json,
                self.retention_class_key,
                self.rerun_of_task_id,
                self.rerun_root_task_id,
            ],
            ensure_ascii=False,
            separators=(',', ':'),
        ).encode()
        return hashlib.sha256(canonical).digest()


@dataclass(frozen=True, slots=True)
class EnqueueApplied:
    task_id: str


@dataclass(frozen=True, slots=True)
class EnqueueReplay:
    task_id: str


@dataclass(frozen=True, slots=True)
class EnqueueKeyConflict:
    task_id: str
    fingerprint_version: int


type IdempotentEnqueueOutcome = EnqueueApplied | EnqueueReplay | EnqueueKeyConflict


def _validate_opaque_value(label: str, value: str) -> None:
    encoded = value.encode()
    if not encoded:
        raise ValueError(f'{label} must be non-empty')
    if len(encoded) > IDEMPOTENCY_KEY_MAX_BYTES:
        raise ValueError(
            f'{label} must be at most {IDEMPOTENCY_KEY_MAX_BYTES} UTF-8 bytes'
        )


def _digest_framed(domain: bytes, *parts: bytes) -> bytes:
    digest = hashlib.sha256()
    digest.update(len(domain).to_bytes(4, 'big'))
    digest.update(domain)
    for part in parts:
        digest.update(len(part).to_bytes(4, 'big'))
        digest.update(part)
    return digest.digest()


def _canonical_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError('datetime must be timezone-aware')
    utc = value.astimezone(timezone.utc)
    return utc.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
