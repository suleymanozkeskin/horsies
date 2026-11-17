//! Shared HTTP response and query parsing rules.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::monitoring::{
    ErrorCategory, MonitoringQueryError, MonitoringQueryErrorCode, PaginationRefused,
    SortDirection, TaskGroupBy, TaskSortField,
};
use crate::TaskStatus;

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

/// The `code` of a 400 body for request text refused before any read
/// (Python `horsies.web.routes._common.INVALID_INPUT`).
pub(crate) const INVALID_INPUT: &str = "INVALID_INPUT";

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

    pub(crate) fn path_validation(
        field: &'static str,
        error_type: &'static str,
        message: impl Into<String>,
        input: impl Into<serde_json::Value>,
    ) -> Self {
        Self::Validation(vec![Self::path_issue(field, error_type, message, input)])
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

    /// FastAPI's answer for a path id that is not a UUID, as for a
    /// `uuid.UUID` path parameter: type `uuid_parsing`, pydantic's message,
    /// and the parser's reason in `ctx.error`. No database call is made.
    pub(crate) fn path_uuid(
        field: &'static str,
        raw: &str,
        error: &uuid::Error,
    ) -> ValidationIssue {
        ValidationIssue {
            error_type: "uuid_parsing",
            loc: vec!["path", field],
            msg: uuid_parsing_message(error),
            input: raw.into(),
            ctx: Some(serde_json::json!({ "error": error.to_string() })),
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
            Self::InvalidInput(detail) => (
                StatusCode::BAD_REQUEST,
                Json(CodeBody {
                    code: INVALID_INPUT,
                    detail,
                }),
            )
                .into_response(),
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

/// Map a monitoring read failure to its response, as Python
/// (`python/horsies/web/routes/_common.py:103-127`): a database failure is 503, the
/// dependency failed and the state can clear; stored history data that does
/// not decode is 500, the request was valid and a repeat gives the same
/// answer until the stored row changes.
pub(crate) fn query_failed(surface: &str, error: MonitoringQueryError) -> ApiError {
    let detail = format!("{surface} query failed: {}", error.message);
    match error.code {
        MonitoringQueryErrorCode::DbOperationFailed => ApiError::unavailable(detail),
        // Python answers it with 503 too; the core never produces it.
        MonitoringQueryErrorCode::EventLoopLimit => ApiError::unavailable(detail),
        MonitoringQueryErrorCode::HistoryContractViolation => {
            ApiError::detail(StatusCode::INTERNAL_SERVER_ERROR, detail)
        }
    }
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

/// pydantic's message for text that is not a UUID: `Input should be a valid
/// UUID, ` and the parser's reason.
fn uuid_parsing_message(error: &uuid::Error) -> String {
    format!("Input should be a valid UUID, {error}")
}

/// Parse a task path id (Python `require_path_uuid`). Text that is not a
/// UUID is refused with 400 `INVALID_INPUT` before any database call. The
/// detail is `Path parameter <field>: <pydantic's message>.`, the detail of
/// the Python web app.
pub(crate) fn parse_path_uuid(field: &'static str, raw: &str) -> Result<uuid::Uuid, ApiError> {
    uuid::Uuid::parse_str(raw).map_err(|error| {
        ApiError::InvalidInput(format!(
            "Path parameter {field}: {}.",
            uuid_parsing_message(&error)
        ))
    })
}

/// Parse the workflow path id of an action route. Text that is not a UUID
/// is refused with FastAPI's 422 `uuid_parsing` body before any database
/// call. The read routes answer such a text with their 404 instead.
pub(crate) fn parse_workflow_path_uuid(raw: &str) -> Result<uuid::Uuid, ApiError> {
    uuid::Uuid::parse_str(raw).map_err(|error| {
        ApiError::Validation(vec![ApiError::path_uuid("workflow_id", raw, &error)])
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::MonitoringQueryErrorSource;

    fn query_error(code: MonitoringQueryErrorCode) -> MonitoringQueryError {
        MonitoringQueryError {
            code,
            message: "read failed".to_owned(),
            retryable: false,
            source: MonitoringQueryErrorSource::Database(sqlx::Error::PoolTimedOut),
        }
    }

    #[test]
    fn a_query_failure_maps_to_python_s_status() {
        let status = |code| match query_failed("Task list", query_error(code)) {
            ApiError::Detail(status, _) => status,
            ApiError::InvalidInput(_) | ApiError::Validation(_) => {
                panic!("a query failure is always a detail body")
            }
        };
        assert_eq!(
            status(MonitoringQueryErrorCode::DbOperationFailed),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status(MonitoringQueryErrorCode::EventLoopLimit),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status(MonitoringQueryErrorCode::HistoryContractViolation),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
