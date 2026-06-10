/// Typed error types for PostgresBroker operations.
///
/// Mirrors Python's `horsies/core/brokers/result_types.py`.
use serde::{Deserialize, Serialize};

/// Categorized broker operation failure codes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BrokerErrorCode {
    SchemaInitFailed,
    EnqueueFailed,
    PayloadMismatch,
    TaskInfoQueryFailed,
    MonitoringQueryFailed,
    DbPingFailed,
    WorkerPingFailed,
    CleanupFailed,
    CloseFailed,
    ListenerStartFailed,
    ListenerSubscribeFailed,
    NoBroker,
}

impl std::fmt::Display for BrokerErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_plain::to_string(self).unwrap_or_else(|_| format!("{:?}", self));
        write!(f, "{}", s)
    }
}

/// Error payload for broker operations.
///
/// Carried inside `Err(...)` of a `BrokerResult<T>`.
#[derive(Debug, Clone)]
pub struct BrokerOperationError {
    /// Which operation category failed.
    pub code: BrokerErrorCode,
    /// Human-readable description.
    pub message: String,
    /// Whether the caller can retry this operation.
    pub retryable: bool,
}

impl std::fmt::Display for BrokerOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} (retryable={})",
            self.code, self.message, self.retryable,
        )
    }
}

impl std::error::Error for BrokerOperationError {}

/// Result type for broker operations.
///
/// `Ok(T)` on success, `Err(BrokerOperationError)` on infrastructure failure.
pub type BrokerResult<T> = Result<T, BrokerOperationError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_error_code_serde() {
        let code = BrokerErrorCode::EnqueueFailed;
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, "\"ENQUEUE_FAILED\"");
        let back: BrokerErrorCode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, code);
    }

    #[test]
    fn broker_operation_error_display() {
        let err = BrokerOperationError {
            code: BrokerErrorCode::SchemaInitFailed,
            message: "connection refused".to_owned(),
            retryable: true,
        };
        let s = format!("{}", err);
        assert!(s.contains("SCHEMA_INIT_FAILED"));
        assert!(s.contains("connection refused"));
        assert!(s.contains("retryable=true"));
    }

    #[test]
    fn all_codes_round_trip() {
        let codes = [
            BrokerErrorCode::SchemaInitFailed,
            BrokerErrorCode::EnqueueFailed,
            BrokerErrorCode::PayloadMismatch,
            BrokerErrorCode::TaskInfoQueryFailed,
            BrokerErrorCode::MonitoringQueryFailed,
            BrokerErrorCode::DbPingFailed,
            BrokerErrorCode::WorkerPingFailed,
            BrokerErrorCode::CleanupFailed,
            BrokerErrorCode::CloseFailed,
            BrokerErrorCode::ListenerStartFailed,
            BrokerErrorCode::ListenerSubscribeFailed,
            BrokerErrorCode::NoBroker,
        ];
        for code in codes {
            let json = serde_json::to_string(&code).unwrap();
            let back: BrokerErrorCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, code);
        }
    }
}
