"""Each recipe of the Change a Model section, against a row that an old model wrote."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import AliasChoices, BaseModel, Field

from horsies import AppConfig, Horsies, PostgresConfig, TaskError, TaskResult
from horsies.core.codec.typed import decode_value, encode_value
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.errors import TaskDefinitionError


_OLD_ROW: dict[str, Any] = {'model': 'A1', 'km': 10, 'register_year': 2001}


class _Renamed(BaseModel):
    model: str
    km: int
    registration_year: int = Field(
        validation_alias=AliasChoices('registration_year', 'register_year'),
    )


class _Added(BaseModel):
    model: str
    km: int
    register_year: int
    colour: str = 'unknown'


class _Removed(BaseModel):
    model: str
    km: int


class _AddedRequired(BaseModel):
    model: str
    km: int
    register_year: int
    colour: str


class _AliasWithoutNewName(BaseModel):
    registration_year: int = Field(
        validation_alias=AliasChoices('register_year'),
    )


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(database_url='postgresql+psycopg://u:p@localhost/x'),
        ),
    )


def _register(annotation: Any) -> None:
    def take(*, car: Any) -> TaskResult[int, TaskError]:
        return TaskResult(ok=1)

    take.__annotations__ = {'car': annotation, 'return': TaskResult[int, TaskError]}
    _app().task(task_name='recipe')(take)


@pytest.mark.unit
class TestRenameAField:
    def test_the_definition_check_accepts_the_recipe(self) -> None:
        _register(_Renamed)

    def test_an_old_row_reads(self) -> None:
        car = decode_value(_OLD_ROW, _Renamed)

        assert car.registration_year == 2001

    def test_a_new_row_is_written_under_the_new_name(self) -> None:
        wire = encode_value(
            _Renamed(model='A1', km=10, registration_year=2001),
            _Renamed,
        )

        assert wire == {'model': 'A1', 'km': 10, 'registration_year': 2001}
        assert decode_value(wire, _Renamed).registration_year == 2001

    def test_the_new_name_must_be_in_the_choices(self) -> None:
        with pytest.raises(TaskDefinitionError, match='the read does not accept it'):
            _register(_AliasWithoutNewName)


@pytest.mark.unit
class TestAddAField:
    def test_an_old_row_reads_with_the_default(self) -> None:
        assert decode_value(_OLD_ROW, _Added).colour == 'unknown'

    def test_a_required_field_fails_an_old_row(self) -> None:
        with pytest.raises(ValueValidationError, match='colour'):
            decode_value(_OLD_ROW, _AddedRequired)


@pytest.mark.unit
class TestRemoveAField:
    def test_an_old_row_reads_and_the_value_is_ignored(self) -> None:
        car = decode_value(_OLD_ROW, _Removed)

        assert car.model_dump() == {'model': 'A1', 'km': 10}

    def test_the_same_key_is_refused_at_send(self) -> None:
        with pytest.raises(ValueValidationError):
            encode_value(_OLD_ROW, _Removed)
