//! Shared HTTP response and query parsing rules.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::broker::BrokerOperationError;
use crate::db::Transience;
use crate::monitoring::{
    database_failure_status, ErrorCategory, MonitoringQueryError, PaginationRefused, SortDirection,
    TaskGroupBy, TaskSortField,
};
use crate::TaskStatus;

use super::uuid_text::parse_uuid_text;

#[derive(Debug, Serialize)]
pub(crate) struct DetailBody {
    pub detail: String,
}

/// A refusal with a horsies code: `{"code": ..., "detail": ...}`.
#[derive(Debug, Serialize)]
pub(crate) struct CodeBody {
    pub code: &'static str,
    pub detail: String,
}

/// The `code` of a 400 body for request text refused before any read.
pub(crate) const INVALID_INPUT: &str = "INVALID_INPUT";

/// The mark of a response that refused request text as `INVALID_INPUT`, in
/// the response's extensions, for the log line of the refusal.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InvalidInputRefused;

#[derive(Debug)]
pub(crate) enum ApiError {
    Detail(StatusCode, String),
    /// Request text refused before any read: 400 and the body
    /// `{"code": "INVALID_INPUT", "detail": ...}` (Python `InvalidInput`).
    InvalidInput(String),
    /// FastAPI's 422 body: every refused parameter, in parameter order.
    Validation(Vec<ValidationIssue>),
}

#[derive(Debug, Serialize)]
pub(crate) struct ValidationIssue {
    #[serde(rename = "type")]
    error_type: &'static str,
    loc: Vec<&'static str>,
    msg: String,
    input: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    ctx: Option<serde_json::Value>,
}

impl ApiError {
    pub(crate) fn detail(status: StatusCode, detail: impl Into<String>) -> Self {
        Self::Detail(status, detail.into())
    }

    pub(crate) fn bad_request(detail: impl Into<String>) -> Self {
        Self::detail(StatusCode::BAD_REQUEST, detail)
    }

    pub(crate) fn unavailable(detail: impl Into<String>) -> Self {
        Self::detail(StatusCode::SERVICE_UNAVAILABLE, detail)
    }

    /// A read whose database operation failed: the status of the transience
    /// of its cause ([`database_failure_status`]: 503 or 500), with `detail`.
    pub(crate) fn database_failed(transience: Transience, detail: impl Into<String>) -> Self {
        let status = StatusCode::from_u16(database_failure_status(transience))
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        Self::detail(status, detail)
    }

    pub(crate) fn query_validation(
        field: &'static str,
        error_type: &'static str,
        message: impl Into<String>,
        input: impl Into<serde_json::Value>,
    ) -> Self {
        Self::Validation(vec![ValidationIssue {
            error_type,
            loc: vec!["query", field],
            msg: message.into(),
            input: input.into(),
            ctx: None,
        }])
    }

    pub(crate) fn query_constraint(
        field: &'static str,
        error_type: &'static str,
        message: impl Into<String>,
        input: impl Into<serde_json::Value>,
        context_key: &'static str,
        context_value: impl Into<serde_json::Value>,
    ) -> Self {
        let mut context = serde_json::Map::new();
        context.insert(context_key.to_owned(), context_value.into());
        Self::Validation(vec![ValidationIssue {
            error_type,
            loc: vec!["query", field],
            msg: message.into(),
            input: input.into(),
            ctx: Some(serde_json::Value::Object(context)),
        }])
    }

    /// One refused path parameter, for a 422 body that lists several.
    pub(crate) fn path_issue(
        field: &'static str,
        error_type: &'static str,
        message: impl Into<String>,
        input: impl Into<serde_json::Value>,
    ) -> ValidationIssue {
        ValidationIssue {
            error_type,
            loc: vec!["path", field],
            msg: message.into(),
            input: input.into(),
            ctx: None,
        }
    }

    pub(crate) fn body_validation(message: impl Into<String>, input: serde_json::Value) -> Self {
        Self::Validation(vec![ValidationIssue {
            error_type: "value_error",
            loc: vec!["body"],
            msg: message.into(),
            input,
            ctx: None,
        }])
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::Detail(status, detail) => (status, Json(DetailBody { detail })).into_response(),
            Self::InvalidInput(detail) => {
                let mut response = (
                    StatusCode::BAD_REQUEST,
                    Json(CodeBody {
                        code: INVALID_INPUT,
                        detail,
                    }),
                )
                    .into_response();
                let _first_mark = response.extensions_mut().insert(InvalidInputRefused);
                response
            }
            Self::Validation(issues) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "detail": issues })),
            )
                .into_response(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct QueryValues(Vec<(String, String)>);

impl QueryValues {
    pub(crate) fn parse(raw: Option<&str>) -> Self {
        Self(
            form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
                .into_owned()
                .collect(),
        )
    }

    pub(crate) fn all(&self, name: &str) -> Vec<String> {
        self.0
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .collect()
    }

    pub(crate) fn last(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .rev()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

pub(crate) fn parse_fastapi_bool(
    values: &QueryValues,
    field: &'static str,
    default: bool,
) -> Result<bool, ApiError> {
    let Some(raw) = values.last(field) else {
        return Ok(default);
    };
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "t" | "on" | "yes" | "y" => Ok(true),
        "0" | "false" | "f" | "off" | "no" | "n" => Ok(false),
        _ => Err(ApiError::query_validation(
            field,
            "bool_parsing",
            "Input should be a valid boolean, unable to interpret input",
            raw,
        )),
    }
}

pub(crate) fn parse_i64(
    values: &QueryValues,
    field: &'static str,
    default: i64,
) -> Result<i64, ApiError> {
    let Some(raw) = values.last(field) else {
        return Ok(default);
    };
    raw.parse().map_err(|_| {
        ApiError::query_validation(
            field,
            "int_parsing",
            "Input should be a valid integer, unable to parse string as an integer",
            raw,
        )
    })
}

pub(crate) fn parse_f64(
    values: &QueryValues,
    field: &'static str,
    default: f64,
) -> Result<f64, ApiError> {
    let Some(raw) = values.last(field) else {
        return Ok(default);
    };
    raw.parse().map_err(|_| {
        ApiError::query_validation(
            field,
            "float_parsing",
            "Input should be a valid number, unable to parse string as a number",
            raw,
        )
    })
}

pub(crate) fn validate_i64_range(
    values: &QueryValues,
    field: &'static str,
    value: i64,
    minimum: i64,
    maximum: i64,
) -> Result<i64, ApiError> {
    let input = values
        .last(field)
        .map(serde_json::Value::from)
        .unwrap_or_else(|| serde_json::Value::from(value));
    if value < minimum {
        return Err(ApiError::query_constraint(
            field,
            "greater_than_equal",
            format!("Input should be greater than or equal to {minimum}"),
            input.clone(),
            "ge",
            minimum,
        ));
    }
    if value > maximum {
        return Err(ApiError::query_constraint(
            field,
            "less_than_equal",
            format!("Input should be less than or equal to {maximum}"),
            input,
            "le",
            maximum,
        ));
    }
    Ok(value)
}

pub(crate) fn validate_f64_range(
    values: &QueryValues,
    field: &'static str,
    value: f64,
    minimum: f64,
    maximum: f64,
) -> Result<f64, ApiError> {
    let input = values
        .last(field)
        .map(serde_json::Value::from)
        .unwrap_or_else(|| serde_json::Value::from(value));
    if value < minimum || value.is_nan() {
        return Err(ApiError::query_constraint(
            field,
            "greater_than_equal",
            format!("Input should be greater than or equal to {minimum}"),
            input.clone(),
            "ge",
            minimum,
        ));
    }
    if value > maximum {
        return Err(ApiError::query_constraint(
            field,
            "less_than_equal",
            format!("Input should be less than or equal to {maximum}"),
            input,
            "le",
            maximum,
        ));
    }
    Ok(value)
}

pub(crate) fn parse_statuses(values: Vec<String>) -> Result<Vec<TaskStatus>, ApiError> {
    values
        .into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|_| ApiError::bad_request(format!("Unknown status '{value}'.")))
        })
        .collect()
}

pub(crate) fn parse_error_categories(values: Vec<String>) -> Result<Vec<ErrorCategory>, ApiError> {
    values
        .into_iter()
        .map(|value| match value.as_str() {
            "OPERATIONAL" => Ok(ErrorCategory::Operational),
            "CONTRACT" => Ok(ErrorCategory::Contract),
            "RETRIEVAL" => Ok(ErrorCategory::Retrieval),
            "OUTCOME" => Ok(ErrorCategory::Outcome),
            "DOMAIN" => Ok(ErrorCategory::Domain),
            _ => Err(ApiError::bad_request(format!(
                "Unknown error category '{value}'."
            ))),
        })
        .collect()
}

pub(crate) fn parse_sort_by(value: &str) -> Result<TaskSortField, ApiError> {
    match value {
        "enqueued_at" => Ok(TaskSortField::EnqueuedAt),
        "started_at" => Ok(TaskSortField::StartedAt),
        "completed_at" => Ok(TaskSortField::CompletedAt),
        "failed_at" => Ok(TaskSortField::FailedAt),
        "status" => Ok(TaskSortField::Status),
        "task_name" => Ok(TaskSortField::TaskName),
        "queue_name" => Ok(TaskSortField::QueueName),
        "priority" => Ok(TaskSortField::Priority),
        "retry_count" => Ok(TaskSortField::RetryCount),
        "queue_s" => Ok(TaskSortField::QueueSeconds),
        "exec_s" => Ok(TaskSortField::ExecutionSeconds),
        _ => Err(ApiError::bad_request(format!("Unknown sort_by '{value}'."))),
    }
}

pub(crate) fn parse_sort_direction(value: &str) -> Result<SortDirection, ApiError> {
    match value {
        "asc" => Ok(SortDirection::Ascending),
        "desc" => Ok(SortDirection::Descending),
        _ => Err(ApiError::query_constraint(
            "sort_dir",
            "literal_error",
            "Input should be 'asc' or 'desc'",
            value,
            "expected",
            "'asc' or 'desc'",
        )),
    }
}

pub(crate) fn parse_group_by(value: &str) -> Result<TaskGroupBy, ApiError> {
    match value {
        "worker" => Ok(TaskGroupBy::Worker),
        "task_name" => Ok(TaskGroupBy::TaskName),
        "queue" => Ok(TaskGroupBy::Queue),
        _ => Err(ApiError::bad_request(format!(
            "Unknown group_by '{value}'."
        ))),
    }
}

/// Map a monitoring read failure to its response. A database failure answers
/// by its `retryable` flag ([`database_failure_status`]): 503 when the cause
/// may clear, 500 when it stays. Stored history data that does not decode is
/// 500: the request was valid, and a repeat gives the same answer until the
/// stored row changes.
pub(crate) fn query_failed(surface: &str, error: &MonitoringQueryError) -> ApiError {
    let detail = format!("{surface} query failed: {}", error.message());
    match error {
        MonitoringQueryError::DbOperationFailed { retryable, .. } => {
            ApiError::database_failed(Transience::of_retryable(*retryable), detail)
        }
        MonitoringQueryError::HistoryContractViolation { .. } => {
            ApiError::detail(StatusCode::INTERNAL_SERVER_ERROR, detail)
        }
    }
}

/// Map a failed worker read of the broker (`subject`, for example `Worker
/// state query`) to its response: `<subject> failed: <message>`, with the
/// status of the error's retry rule ([`BrokerOperationError::retryable`],
/// [`database_failure_status`]). A stored row that does not read, a refused
/// argument and a broker with no listener stay, so they are 500.
pub(crate) fn broker_read_failed(subject: &str, error: &BrokerOperationError) -> ApiError {
    ApiError::database_failed(
        Transience::of_retryable(error.retryable()),
        format!("{subject} failed: {}", error.message()),
    )
}

pub(crate) fn pagination_refused(
    error: PaginationRefused,
    values: &QueryValues,
    default_field: &'static str,
    default_input: i64,
) -> ApiError {
    if error.reason.starts_with("offset + limit must be <=") {
        ApiError::bad_request(error.reason)
    } else {
        let field = if error.reason.starts_with("offset") {
            "offset"
        } else {
            default_field
        };
        let input = values
            .last(field)
            .map(serde_json::Value::from)
            .unwrap_or_else(|| serde_json::Value::from(default_input));
        ApiError::query_validation(field, "value_error", error.reason, input)
    }
}

/// Parse a task path id (Python `require_path_uuid`), by pydantic's rule
/// ([`parse_uuid_text`]). Text that is not a UUID is refused with 400
/// `INVALID_INPUT` before any database call. The detail is
/// `Path parameter <field>: Input should be a valid UUID, <reason>.`, the
/// detail of the Python web app.
pub(crate) fn parse_path_uuid(field: &'static str, raw: &str) -> Result<uuid::Uuid, ApiError> {
    parse_uuid_text(raw).map_err(|refusal| {
        ApiError::InvalidInput(format!(
            "Path parameter {field}: Input should be a valid UUID, {refusal}."
        ))
    })
}

/// The longest integer text that pydantic parses, after the whitespace is
/// trimmed: 4,300 bytes (pydantic-core `str_as_int`, the default of
/// Python's `sys.int_info.default_max_str_digits`).
const MAX_INTEGER_TEXT_BYTES: usize = 4_300;

/// A path parameter that FastAPI reads as an `int`, by pydantic's rule
/// for text (pydantic-core `str_as_int`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathInteger {
    /// An integer that fits in 32 bits.
    Fits(i32),
    /// An integer outside the 32-bit range: no `integer` column holds it.
    OutsideI32,
    /// Not an integer: FastAPI's 422 `int_parsing`.
    NotAnInteger,
    /// Longer than [`MAX_INTEGER_TEXT_BYTES`]: FastAPI's 422
    /// `int_parsing_size`.
    TooLong,
}

/// The digits of integer text: an optional sign, then ASCII digits, with
/// single underscores between digits as pydantic strips them (none at the
/// start or the end, no two in a row). `None` for other text. Pure.
fn integer_digits(text: &str) -> Option<(bool, String)> {
    let underscores_valid = !text.starts_with('_') && !text.ends_with('_') && !text.contains("__");
    let joined = match (text.contains('_'), underscores_valid) {
        (false, _) => text.to_owned(),
        (true, true) => text.replace('_', ""),
        (true, false) => return None,
    };
    let (negative, digits) = match joined.as_bytes().first() {
        Some(b'-') => (true, &joined[1..]),
        Some(b'+') => (false, &joined[1..]),
        _ => (false, joined.as_str()),
    };
    match !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) {
        true => Some((negative, digits.to_owned())),
        false => None,
    }
}

/// Read path text as pydantic reads an `int` (pydantic-core `str_as_int`):
/// whitespace trimmed; a sign; ASCII digits; single underscores between
/// digits; and a fraction of zeros only (`12.0`, `12.000`). Pure.
pub(crate) fn parse_path_integer(raw: &str) -> PathInteger {
    let text = raw.trim();
    if text.len() > MAX_INTEGER_TEXT_BYTES {
        return PathInteger::TooLong;
    }
    let whole = match text.split_once('.') {
        None => text,
        Some((whole, fraction)) if !fraction.is_empty() && fraction.bytes().all(|b| b == b'0') => {
            whole
        }
        Some(_) => return PathInteger::NotAnInteger,
    };
    let Some((negative, digits)) = integer_digits(whole) else {
        return PathInteger::NotAnInteger;
    };
    let significant = digits.trim_start_matches('0');
    // i32 holds at most 10 digits; a longer run is out of range by length.
    if significant.len() > 10 {
        return PathInteger::OutsideI32;
    }
    let magnitude: i64 = match significant.is_empty() {
        true => 0,
        false => significant
            .parse()
            .expect("at most 10 ASCII digits fit in i64"),
    };
    let value = match negative {
        true => -magnitude,
        false => magnitude,
    };
    match i32::try_from(value) {
        Ok(fits) => PathInteger::Fits(fits),
        Err(_) => PathInteger::OutsideI32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::MonitoringQueryErrorSource;

    fn read_source() -> MonitoringQueryErrorSource {
        MonitoringQueryErrorSource::Broker(crate::broker::BrokerError::Statement(
            crate::db::StatementFailure::NotSent {
                statement: "monitoring_task_list",
                cause: crate::db::NotSentCause::Checkout(crate::db::CheckoutFailure::TimedOut {
                    role: crate::db::PoolRole::Main,
                    waited: std::time::Duration::from_secs(30),
                }),
            },
        ))
    }

    #[test]
    fn path_integer_text_is_read_as_pydantic_reads_an_int() {
        for (raw, read) in [
            ("12", PathInteger::Fits(12)),
            (" 12 ", PathInteger::Fits(12)),
            ("\u{a0}12\n", PathInteger::Fits(12)),
            ("+12", PathInteger::Fits(12)),
            ("-12", PathInteger::Fits(-12)),
            ("0012", PathInteger::Fits(12)),
            ("-0", PathInteger::Fits(0)),
            ("1_000", PathInteger::Fits(1_000)),
            ("0_1", PathInteger::Fits(1)),
            ("12.0", PathInteger::Fits(12)),
            ("+1_0.00", PathInteger::Fits(10)),
            ("2147483647", PathInteger::Fits(i32::MAX)),
            ("-2147483648", PathInteger::Fits(i32::MIN)),
            ("2147483648", PathInteger::OutsideI32),
            ("-2147483649", PathInteger::OutsideI32),
            ("99999999999999999999", PathInteger::OutsideI32),
            ("00000000000000000007", PathInteger::Fits(7)),
            ("12.5", PathInteger::NotAnInteger),
            ("12.", PathInteger::NotAnInteger),
            (".0", PathInteger::NotAnInteger),
            ("12.00_0", PathInteger::NotAnInteger),
            ("1.0.0", PathInteger::NotAnInteger),
            ("1__0", PathInteger::NotAnInteger),
            ("_1", PathInteger::NotAnInteger),
            ("1_", PathInteger::NotAnInteger),
            ("+-1", PathInteger::NotAnInteger),
            ("- 1", PathInteger::NotAnInteger),
            ("+", PathInteger::NotAnInteger),
            ("", PathInteger::NotAnInteger),
            ("0x1f", PathInteger::NotAnInteger),
            ("1e3", PathInteger::NotAnInteger),
            ("\u{663}", PathInteger::NotAnInteger),
            ("not-an-int", PathInteger::NotAnInteger),
        ] {
            assert_eq!(parse_path_integer(raw), read, "{raw:?}");
        }
        assert_eq!(
            parse_path_integer(&"1".repeat(4_300)),
            PathInteger::OutsideI32
        );
        assert_eq!(parse_path_integer(&"1".repeat(4_301)), PathInteger::TooLong);
    }

    #[test]
    fn a_query_failure_answers_by_the_retry_rule_of_its_cause() {
        let status = |error: MonitoringQueryError| match query_failed("Task list", &error) {
            ApiError::Detail(status, _) => status,
            ApiError::InvalidInput(_) | ApiError::Validation(_) => {
                panic!("a query failure is always a detail body")
            }
        };
        assert_eq!(
            status(MonitoringQueryError::DbOperationFailed {
                message: "read failed".to_owned(),
                retryable: true,
                source: read_source(),
            }),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status(MonitoringQueryError::DbOperationFailed {
                message: "read failed".to_owned(),
                retryable: false,
                source: read_source(),
            }),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status(MonitoringQueryError::HistoryContractViolation {
                message: "read failed".to_owned(),
                source: crate::monitoring::HistoryContractSource(read_source()),
            }),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// A worker read of the broker that may clear is 503; one that stays (a
    /// statement refused by its SQLSTATE class, a broker with no listener)
    /// is 500. The detail names the read.
    #[test]
    fn a_broker_read_failure_answers_by_the_retry_rule_of_its_error() {
        let answer =
            |error: BrokerOperationError| match broker_read_failed("Worker state query", &error) {
                ApiError::Detail(status, detail) => (status, detail),
                ApiError::InvalidInput(_) | ApiError::Validation(_) => {
                    panic!("a broker read failure is always a detail body")
                }
            };

        let (status, detail) = answer(BrokerOperationError::monitoring_query_failed(
            "list_worker_states",
            crate::broker::BrokerOperationSource::Broker(crate::broker::BrokerError::Statement(
                crate::db::StatementFailure::NotSent {
                    statement: "list_worker_states",
                    cause: crate::db::NotSentCause::ConnectionClosed,
                },
            )),
        ));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            detail.starts_with("Worker state query failed: list_worker_states failed: "),
            "{detail}"
        );

        let (status, _detail) = answer(BrokerOperationError::monitoring_query_failed(
            "list_worker_states",
            crate::broker::BrokerOperationSource::Broker(
                crate::broker::BrokerError::ClaimContract("row".to_owned()),
            ),
        ));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

        let (status, detail) = answer(BrokerOperationError::ListenerDisabled {
            message: "ping_workers needs a listener".to_owned(),
        });
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            detail,
            "Worker state query failed: ping_workers needs a listener"
        );
    }
}
