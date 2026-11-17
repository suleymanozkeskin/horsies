//! The serialized form of the times of the monitoring models: pydantic's
//! JSON text of an aware UTC `datetime`, which the Python monitoring web app
//! wrote (`2026-11-17T13:59:14.500000Z`). chrono's own serde form writes a
//! whole millisecond with 3 digits (`.500Z`). The read is chrono's: any
//! count of fraction digits.

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};

use crate::core::python_text::pydantic_json_utc;

/// Serialize `time` as pydantic writes it.
pub(super) fn utc<S: Serializer>(time: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&pydantic_json_utc(*time))
}

/// Serialize a time that may be absent: none, or the time as [`utc`]
/// writes it.
pub(super) fn optional_utc<S: Serializer>(
    time: &Option<DateTime<Utc>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match time {
        Some(time) => serializer.serialize_some(&PydanticTime(*time)),
        None => serializer.serialize_none(),
    }
}

/// A time that serializes as [`utc`] writes it.
struct PydanticTime(DateTime<Utc>);

impl Serialize for PydanticTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        utc(&self.0, serializer)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[derive(Serialize)]
    struct Times {
        #[serde(serialize_with = "utc")]
        at: DateTime<Utc>,
        #[serde(serialize_with = "optional_utc")]
        maybe: Option<DateTime<Utc>>,
    }

    fn half_second() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 17, 13, 59, 14)
            .single()
            .expect("a valid time")
            + chrono::Duration::milliseconds(500)
    }

    #[test]
    fn a_time_is_written_with_six_fraction_digits() {
        let times = Times {
            at: half_second(),
            maybe: Some(half_second()),
        };

        assert_eq!(
            serde_json::to_string(&times).expect("serialize"),
            r#"{"at":"2026-11-17T13:59:14.500000Z","maybe":"2026-11-17T13:59:14.500000Z"}"#
        );
    }

    #[test]
    fn an_absent_time_is_null() {
        let times = Times {
            at: half_second(),
            maybe: None,
        };

        assert_eq!(
            serde_json::to_string(&times).expect("serialize"),
            r#"{"at":"2026-11-17T13:59:14.500000Z","maybe":null}"#
        );
    }
}
