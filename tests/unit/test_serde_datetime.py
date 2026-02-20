"""Unit tests for datetime serialization and rehydration in serde."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from typing import Any

import pytest

from horsies.core.codec.serde import rehydrate_value, to_jsonable
from horsies.core.types.result import is_err
from horsies.core.worker.worker import import_by_path


# ---------------------------------------------------------------------------
# Serialization: to_jsonable produces the expected tagged dict
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatetimeSerialization:
    """Verify to_jsonable produces correct tagged dicts for datetime types."""

    def test_naive_datetime(self) -> None:
        value = dt.datetime(2025, 6, 15, 10, 30, 45)
        result = to_jsonable(value).unwrap()
        assert result == {'__datetime__': True, 'value': '2025-06-15T10:30:45'}

    def test_utc_datetime(self) -> None:
        value = dt.datetime(2025, 6, 15, 10, 30, 45, tzinfo=dt.timezone.utc)
        result = to_jsonable(value).unwrap()
        assert result == {
            '__datetime__': True,
            'value': '2025-06-15T10:30:45+00:00',
        }

    def test_non_utc_offset_datetime(self) -> None:
        tz = dt.timezone(dt.timedelta(hours=5, minutes=30))
        value = dt.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        result = to_jsonable(value).unwrap()
        assert result == {
            '__datetime__': True,
            'value': '2025-01-01T12:00:00+05:30',
        }

    def test_negative_offset_datetime(self) -> None:
        tz = dt.timezone(dt.timedelta(hours=-8))
        value = dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=tz)
        result = to_jsonable(value).unwrap()
        assert result == {
            '__datetime__': True,
            'value': '2025-12-31T23:59:59-08:00',
        }

    def test_datetime_with_microseconds(self) -> None:
        value = dt.datetime(2025, 6, 15, 10, 30, 45, 123456)
        result = to_jsonable(value).unwrap()
        assert result == {
            '__datetime__': True,
            'value': '2025-06-15T10:30:45.123456',
        }

    def test_date(self) -> None:
        value = dt.date(2025, 6, 15)
        result = to_jsonable(value).unwrap()
        assert result == {'__date__': True, 'value': '2025-06-15'}

    def test_naive_time(self) -> None:
        value = dt.time(14, 30, 0)
        result = to_jsonable(value).unwrap()
        assert result == {'__time__': True, 'value': '14:30:00'}

    def test_tz_aware_time(self) -> None:
        value = dt.time(14, 30, 0, tzinfo=dt.timezone.utc)
        result = to_jsonable(value).unwrap()
        assert result == {'__time__': True, 'value': '14:30:00+00:00'}

    def test_time_with_microseconds(self) -> None:
        value = dt.time(14, 30, 0, 123456)
        result = to_jsonable(value).unwrap()
        assert result == {'__time__': True, 'value': '14:30:00.123456'}


@pytest.mark.unit
class TestDatetimeSubclassOrdering:
    """datetime is a subclass of date — ensure it serializes as __datetime__."""

    def test_datetime_not_tagged_as_date(self) -> None:
        value = dt.datetime(2025, 6, 15, 10, 0, 0)
        result = to_jsonable(value).unwrap()
        assert '__datetime__' in result  # type: ignore[operator]
        assert '__date__' not in result  # type: ignore[operator]

    def test_date_tagged_as_date(self) -> None:
        value = dt.date(2025, 6, 15)
        result = to_jsonable(value).unwrap()
        assert '__date__' in result  # type: ignore[operator]
        assert '__datetime__' not in result  # type: ignore[operator]


# ---------------------------------------------------------------------------
# Round-trip: to_jsonable → rehydrate_value restores the original value
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatetimeRoundTrip:
    """Verify serialization → rehydration restores the original value."""

    def test_naive_datetime_roundtrip(self) -> None:
        original = dt.datetime(2025, 6, 15, 10, 30, 45)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert isinstance(restored, dt.datetime)

    def test_utc_datetime_roundtrip(self) -> None:
        original = dt.datetime(2025, 6, 15, 10, 30, 45, tzinfo=dt.timezone.utc)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert restored.tzinfo is not None

    def test_non_utc_offset_roundtrip(self) -> None:
        tz = dt.timezone(dt.timedelta(hours=5, minutes=30))
        original = dt.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert restored.utcoffset() == dt.timedelta(hours=5, minutes=30)

    def test_negative_offset_roundtrip(self) -> None:
        tz = dt.timezone(dt.timedelta(hours=-8))
        original = dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=tz)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert restored.utcoffset() == dt.timedelta(hours=-8)

    def test_datetime_microseconds_roundtrip(self) -> None:
        original = dt.datetime(2025, 6, 15, 10, 30, 45, 123456)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert restored.microsecond == 123456

    def test_date_roundtrip(self) -> None:
        original = dt.date(2025, 6, 15)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert type(restored) is dt.date

    def test_naive_time_roundtrip(self) -> None:
        original = dt.time(14, 30, 0)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert type(restored) is dt.time

    def test_tz_aware_time_roundtrip(self) -> None:
        original = dt.time(14, 30, 0, tzinfo=dt.timezone.utc)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert restored.tzinfo is not None

    def test_time_microseconds_roundtrip(self) -> None:
        original = dt.time(14, 30, 0, 123456)
        restored = rehydrate_value(to_jsonable(original).unwrap()).unwrap()
        assert restored == original
        assert restored.microsecond == 123456


# ---------------------------------------------------------------------------
# Nested contexts: datetime inside dicts, lists
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatetimeNestedInContainers:
    """Verify datetime types serialize/rehydrate inside dicts and lists."""

    def test_datetime_in_dict(self) -> None:
        original: dict[str, Any] = {
            'created_at': dt.datetime(2025, 6, 15, 10, 0, 0, tzinfo=dt.timezone.utc),
            'label': 'test',
        }
        json_data = to_jsonable(original).unwrap()
        restored = rehydrate_value(json_data).unwrap()
        assert restored['created_at'] == original['created_at']
        assert isinstance(restored['created_at'], dt.datetime)
        assert restored['label'] == 'test'

    def test_datetime_in_list(self) -> None:
        original = [
            dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
            dt.datetime(2025, 6, 15, tzinfo=dt.timezone.utc),
        ]
        json_data = to_jsonable(original).unwrap()
        restored = rehydrate_value(json_data).unwrap()
        assert restored == original
        assert all(isinstance(item, dt.datetime) for item in restored)

    def test_mixed_types_in_dict(self) -> None:
        original: dict[str, Any] = {
            'timestamp': dt.datetime(2025, 6, 15, 12, 0, 0),
            'date_only': dt.date(2025, 6, 15),
            'time_only': dt.time(12, 0, 0),
            'count': 42,
            'name': 'test',
        }
        json_data = to_jsonable(original).unwrap()
        restored = rehydrate_value(json_data).unwrap()
        assert isinstance(restored['timestamp'], dt.datetime)
        assert type(restored['date_only']) is dt.date
        assert type(restored['time_only']) is dt.time
        assert restored['count'] == 42
        assert restored['name'] == 'test'


# ---------------------------------------------------------------------------
# Nested context: datetime inside dataclass fields
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatetimeInDataclass:
    """Verify datetime fields in dataclasses survive round-trip."""

    def test_dataclass_with_datetime_field(self, tmp_path: Path) -> None:
        pkg = tmp_path / 'serdedtdc'
        pkg.mkdir()
        (pkg / '__init__.py').write_text('')
        mod_file = pkg / 'models.py'
        mod_file.write_text(
            """
import datetime as dt
from dataclasses import dataclass

@dataclass
class Event:
    name: str
    occurred_at: dt.datetime
    event_date: dt.date
""",
        )

        for name in list(sys.modules.keys()):
            if 'serdedtdc' in name:
                sys.modules.pop(name, None)

        original_path = sys.path.copy()
        try:
            mod = import_by_path(str(mod_file))
            original = mod.Event(
                name='deploy',
                occurred_at=dt.datetime(2025, 6, 15, 10, 0, 0, tzinfo=dt.timezone.utc),
                event_date=dt.date(2025, 6, 15),
            )

            json_data = to_jsonable(original).unwrap()
            restored = rehydrate_value(json_data).unwrap()

            assert isinstance(restored, mod.Event)
            assert restored.name == 'deploy'
            assert isinstance(restored.occurred_at, dt.datetime)
            assert restored.occurred_at == original.occurred_at
            assert type(restored.event_date) is dt.date
            assert restored.event_date == original.event_date
        finally:
            sys.path[:] = original_path
            for name in list(sys.modules.keys()):
                if 'serdedtdc' in name:
                    sys.modules.pop(name, None)


# ---------------------------------------------------------------------------
# Error paths: serialization rejects unsupported types
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatetimeSerializationErrors:
    """Verify to_jsonable rejects unsupported datetime module types."""

    def test_timedelta_returns_serialization_error(self) -> None:
        """timedelta is not serializable — must return Err(SerializationError)."""
        result = to_jsonable(dt.timedelta(hours=1))
        assert is_err(result)
        assert 'timedelta' in str(result.err_value)


# ---------------------------------------------------------------------------
# Error paths: rehydration rejects malformed tagged dicts
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatetimeRehydrationErrors:
    """Verify rehydrate_value rejects malformed datetime tagged dicts."""

    @pytest.mark.parametrize(
        'tagged_dict',
        [
            pytest.param({'__datetime__': True, 'value': 'not-a-date'}, id='datetime'),
            pytest.param({'__date__': True, 'value': 'not-a-date'}, id='date'),
            pytest.param({'__time__': True, 'value': 'not-a-time'}, id='time'),
        ],
    )
    def test_rehydrate_rejects_invalid_iso_string(
        self, tagged_dict: dict[str, Any],
    ) -> None:
        """Invalid ISO format string returns Err(SerializationError)."""
        result = rehydrate_value(tagged_dict)
        assert is_err(result)
        assert 'rehydration failed' in str(result.err_value)

    @pytest.mark.parametrize(
        'tagged_dict',
        [
            pytest.param({'__datetime__': True}, id='datetime'),
            pytest.param({'__date__': True}, id='date'),
            pytest.param({'__time__': True}, id='time'),
        ],
    )
    def test_rehydrate_rejects_missing_value_key(
        self, tagged_dict: dict[str, Any],
    ) -> None:
        """Missing 'value' key returns Err(SerializationError)."""
        result = rehydrate_value(tagged_dict)
        assert is_err(result)
        assert 'rehydration failed' in str(result.err_value)
