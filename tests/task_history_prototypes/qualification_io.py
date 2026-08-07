"""Durable checkpoints and bounded console progress for qualification runs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from tests.task_history_prototypes.evidence import evidence_json


@dataclass(frozen=True, slots=True)
class QualificationProgress:
    scenario: str
    phase: str
    status: str
    candidate: str | None = None
    candidate_index: int | None = None
    candidate_total: int | None = None
    category: str | None = None
    category_index: int | None = None
    category_total: int | None = None
    posture: str | None = None
    cell_index: int | None = None
    cell_total: int | None = None
    rung: int | None = None
    observations: int | None = None
    observation_target: int | None = None
    live_rows: int | None = None
    finite_history_rows: int | None = None
    forever_history_rows: int | None = None

    def render(self) -> str:
        fields: tuple[tuple[str, str | int | None], ...] = (
            ('scenario', self.scenario),
            ('phase', self.phase),
            ('status', self.status),
            ('candidate', self.candidate),
            ('candidate_index', self.candidate_index),
            ('candidate_total', self.candidate_total),
            ('category', self.category),
            ('category_index', self.category_index),
            ('category_total', self.category_total),
            ('posture', self.posture),
            ('cell_index', self.cell_index),
            ('cell_total', self.cell_total),
            ('rung', self.rung),
            ('observations', self.observations),
            ('observation_target', self.observation_target),
            ('live_rows', self.live_rows),
            ('finite_history_rows', self.finite_history_rows),
            ('forever_history_rows', self.forever_history_rows),
        )
        rendered: list[str] = []
        for name, value in fields:
            if value is None:
                continue
            text = str(value)
            if any(character.isspace() for character in text):
                raise ValueError(f'progress field {name} must not contain whitespace')
            rendered.append(f'{name}={text}')
        return 'qualification ' + ' '.join(rendered)


class QualificationProgressReporter:
    def __init__(self, stream: IO[str] | None = None) -> None:
        self._stream = stream

    @classmethod
    def from_fd(cls, fd: int | None) -> QualificationProgressReporter:
        if fd is None:
            return cls()
        if fd < 3:
            raise ValueError('progress file descriptor must be at least 3')
        stream = os.fdopen(os.dup(fd), 'w', buffering=1)
        return cls(stream)

    def emit(self, progress: QualificationProgress) -> None:
        if self._stream is None:
            return
        print(progress.render(), file=self._stream, flush=True)

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None


class AtomicEvidenceWriter:
    def __init__(self, path: Path | None) -> None:
        self._path = path

    @property
    def enabled(self) -> bool:
        return self._path is not None

    def write(self, value: object) -> None:
        if self._path is None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_name(
            f'.{self._path.name}.{os.getpid()}.tmp'
        )
        try:
            with temporary.open('w', encoding='utf-8') as stream:
                stream.write(evidence_json(value))
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._path)
            directory_fd = os.open(self._path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)


def bounded_observation_milestones(target: int, *, maximum: int = 10) -> set[int]:
    if target <= 0:
        raise ValueError('observation target must be positive')
    if maximum <= 0:
        raise ValueError('milestone maximum must be positive')
    return {
        max(1, round(target * index / maximum))
        for index in range(1, maximum + 1)
    }
