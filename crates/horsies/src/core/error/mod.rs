pub mod diagnostic;

use serde::{Deserialize, Serialize};

/// Error category corresponding to Python's exception subclass hierarchy.
///
/// Python has four exception subclasses of `HorsiesError`:
/// - `WorkflowValidationError` -- workflow spec validation failures
/// - `TaskDefinitionError` -- task definition problems
/// - `ConfigurationError` -- app/broker configuration issues
/// - `RegistryError` -- task/workflow registry operations
///
/// In Rust, rather than separate error types, we use this enum for
/// type-level discrimination via `match error.category()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// Workflow validation errors (HRS-001-HRS-018). Maps to Python's `WorkflowValidationError`.
    Workflow,
    /// Task definition errors (HRS-100-HRS-103). Maps to Python's `TaskDefinitionError`.
    Task,
    /// Configuration/broker errors (HRS-200-HRS-207). Maps to Python's `ConfigurationError`.
    Config,
    /// Registry errors (HRS-300-HRS-301). Maps to Python's `RegistryError`.
    Registry,
}

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Workflow => write!(f, "workflow validation"),
            Self::Task => write!(f, "task definition"),
            Self::Config => write!(f, "configuration"),
            Self::Registry => write!(f, "registry"),
        }
    }
}

/// Error codes for startup/validation errors.
///
/// Organized by category:
/// - HRS-001–HRS-099: Workflow validation
/// - HRS-100–HRS-199: Task definition
/// - HRS-200–HRS-299: Config/broker
/// - HRS-300–HRS-399: Registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCode {
    // Workflow validation (HRS-001–HRS-099)
    WorkflowNoName,
    WorkflowNoNodes,
    WorkflowInvalidNodeId,
    WorkflowDuplicateNodeId,
    WorkflowNoRootTasks,
    WorkflowInvalidDependency,
    WorkflowCycleDetected,
    WorkflowInvalidArgsFrom,
    WorkflowInvalidCtxFrom,
    WorkflowCtxParamMissing,
    WorkflowInvalidOutput,
    WorkflowInvalidSuccessPolicy,
    WorkflowInvalidJoin,
    WorkflowUnresolvedQueue,
    WorkflowUnresolvedPriority,
    WorkflowNoDefinitionKey,
    WorkflowDuplicateDefinitionKey,
    WorkflowSubworkflowAppMissing,
    WorkflowInvalidKwargKey,
    WorkflowMissingRequiredParams,
    WorkflowArgsWithInjection,
    WorkflowOutputTypeMismatch,
    WorkflowCheckCasesRequired,
    WorkflowCheckCaseInvalid,
    WorkflowCheckBuilderException,
    WorkflowCheckUndecoratedBuilder,

    // Task definition (HRS-100–HRS-199)
    TaskNoReturnType,
    TaskInvalidReturnType,
    TaskInvalidOptions,
    TaskInvalidQueue,

    // Config/broker (HRS-200–HRS-299)
    ConfigInvalidQueueMode,
    ConfigInvalidClusterCap,
    ConfigInvalidPrefetch,
    BrokerInvalidUrl,
    ConfigInvalidRecovery,
    ConfigInvalidSchedule,
    CliInvalidArgs,
    WorkerInvalidLocator,
    BrokerInitFailed,
    CheckReservedCodeCollision,

    // Registry (HRS-300–HRS-399)
    TaskNotRegistered,
    TaskDuplicateName,
    WorkflowUnregisteredTask,
}

impl ErrorCode {
    /// Numeric code string (e.g. "HRS-001").
    pub fn as_code_str(&self) -> &'static str {
        match self {
            Self::WorkflowNoName => "HRS-001",
            Self::WorkflowNoNodes => "HRS-002",
            Self::WorkflowInvalidNodeId => "HRS-003",
            Self::WorkflowDuplicateNodeId => "HRS-004",
            Self::WorkflowNoRootTasks => "HRS-005",
            Self::WorkflowInvalidDependency => "HRS-006",
            Self::WorkflowCycleDetected => "HRS-007",
            Self::WorkflowInvalidArgsFrom => "HRS-008",
            Self::WorkflowInvalidCtxFrom => "HRS-009",
            Self::WorkflowCtxParamMissing => "HRS-010",
            Self::WorkflowInvalidOutput => "HRS-011",
            Self::WorkflowInvalidSuccessPolicy => "HRS-012",
            Self::WorkflowInvalidJoin => "HRS-013",
            Self::WorkflowUnresolvedQueue => "HRS-014",
            Self::WorkflowUnresolvedPriority => "HRS-015",
            Self::WorkflowNoDefinitionKey => "HRS-016",
            Self::WorkflowDuplicateDefinitionKey => "HRS-017",
            Self::WorkflowSubworkflowAppMissing => "HRS-018",
            Self::WorkflowInvalidKwargKey => "HRS-019",
            Self::WorkflowMissingRequiredParams => "HRS-020",
            Self::WorkflowArgsWithInjection => "HRS-032",
            Self::WorkflowOutputTypeMismatch => "HRS-025",
            Self::WorkflowCheckCasesRequired => "HRS-027",
            Self::WorkflowCheckCaseInvalid => "HRS-028",
            Self::WorkflowCheckBuilderException => "HRS-029",
            Self::WorkflowCheckUndecoratedBuilder => "HRS-030",
            Self::TaskNoReturnType => "HRS-100",
            Self::TaskInvalidReturnType => "HRS-101",
            Self::TaskInvalidOptions => "HRS-102",
            Self::TaskInvalidQueue => "HRS-103",
            Self::ConfigInvalidQueueMode => "HRS-200",
            Self::ConfigInvalidClusterCap => "HRS-201",
            Self::ConfigInvalidPrefetch => "HRS-202",
            Self::BrokerInvalidUrl => "HRS-203",
            Self::ConfigInvalidRecovery => "HRS-204",
            Self::ConfigInvalidSchedule => "HRS-205",
            Self::CliInvalidArgs => "HRS-206",
            Self::WorkerInvalidLocator => "HRS-207",
            Self::BrokerInitFailed => "HRS-211",
            Self::CheckReservedCodeCollision => "HRS-212",
            Self::TaskNotRegistered => "HRS-300",
            Self::TaskDuplicateName => "HRS-301",
            Self::WorkflowUnregisteredTask => "HRS-302",
        }
    }
}

impl ErrorCode {
    /// Returns the error category for this code.
    ///
    /// Maps each error code to one of the four categories that correspond
    /// to Python's exception subclass hierarchy.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::WorkflowNoName
            | Self::WorkflowNoNodes
            | Self::WorkflowInvalidNodeId
            | Self::WorkflowDuplicateNodeId
            | Self::WorkflowNoRootTasks
            | Self::WorkflowInvalidDependency
            | Self::WorkflowCycleDetected
            | Self::WorkflowInvalidArgsFrom
            | Self::WorkflowInvalidCtxFrom
            | Self::WorkflowCtxParamMissing
            | Self::WorkflowInvalidOutput
            | Self::WorkflowInvalidSuccessPolicy
            | Self::WorkflowInvalidJoin
            | Self::WorkflowUnresolvedQueue
            | Self::WorkflowUnresolvedPriority
            | Self::WorkflowNoDefinitionKey
            | Self::WorkflowDuplicateDefinitionKey
            | Self::WorkflowArgsWithInjection
            | Self::WorkflowSubworkflowAppMissing
            | Self::WorkflowInvalidKwargKey
            | Self::WorkflowMissingRequiredParams
            | Self::WorkflowOutputTypeMismatch
            | Self::WorkflowCheckCasesRequired
            | Self::WorkflowCheckCaseInvalid
            | Self::WorkflowCheckBuilderException
            | Self::WorkflowCheckUndecoratedBuilder => ErrorCategory::Workflow,

            Self::TaskNoReturnType
            | Self::TaskInvalidReturnType
            | Self::TaskInvalidOptions
            | Self::TaskInvalidQueue => ErrorCategory::Task,

            Self::ConfigInvalidQueueMode
            | Self::ConfigInvalidClusterCap
            | Self::ConfigInvalidPrefetch
            | Self::BrokerInvalidUrl
            | Self::ConfigInvalidRecovery
            | Self::ConfigInvalidSchedule
            | Self::CliInvalidArgs
            | Self::WorkerInvalidLocator
            | Self::BrokerInitFailed
            | Self::CheckReservedCodeCollision => ErrorCategory::Config,

            Self::TaskNotRegistered | Self::TaskDuplicateName | Self::WorkflowUnregisteredTask => {
                ErrorCategory::Registry
            }
        }
    }

    /// Returns true if this is a workflow validation error (HRS-001-HRS-018).
    pub fn is_workflow_error(&self) -> bool {
        matches!(
            self,
            Self::WorkflowNoName
                | Self::WorkflowNoNodes
                | Self::WorkflowInvalidNodeId
                | Self::WorkflowDuplicateNodeId
                | Self::WorkflowNoRootTasks
                | Self::WorkflowInvalidDependency
                | Self::WorkflowCycleDetected
                | Self::WorkflowInvalidArgsFrom
                | Self::WorkflowInvalidCtxFrom
                | Self::WorkflowCtxParamMissing
                | Self::WorkflowInvalidOutput
                | Self::WorkflowInvalidSuccessPolicy
                | Self::WorkflowInvalidJoin
                | Self::WorkflowUnresolvedQueue
                | Self::WorkflowUnresolvedPriority
                | Self::WorkflowNoDefinitionKey
                | Self::WorkflowDuplicateDefinitionKey
                | Self::WorkflowArgsWithInjection
                | Self::WorkflowSubworkflowAppMissing
                | Self::WorkflowInvalidKwargKey
                | Self::WorkflowMissingRequiredParams
                | Self::WorkflowOutputTypeMismatch
                | Self::WorkflowCheckCasesRequired
                | Self::WorkflowCheckCaseInvalid
                | Self::WorkflowCheckBuilderException
                | Self::WorkflowCheckUndecoratedBuilder
        )
    }

    /// Returns true if this is a task definition error (HRS-100-HRS-103).
    pub fn is_task_error(&self) -> bool {
        matches!(
            self,
            Self::TaskNoReturnType
                | Self::TaskInvalidReturnType
                | Self::TaskInvalidOptions
                | Self::TaskInvalidQueue
        )
    }

    /// Returns true if this is a configuration error (HRS-200-HRS-207).
    pub fn is_config_error(&self) -> bool {
        matches!(
            self,
            Self::ConfigInvalidQueueMode
                | Self::ConfigInvalidClusterCap
                | Self::ConfigInvalidPrefetch
                | Self::BrokerInvalidUrl
                | Self::ConfigInvalidRecovery
                | Self::ConfigInvalidSchedule
                | Self::CliInvalidArgs
                | Self::WorkerInvalidLocator
                | Self::BrokerInitFailed
                | Self::CheckReservedCodeCollision
        )
    }

    /// Returns true if this is a registry error (HRS-300-HRS-301).
    pub fn is_registry_error(&self) -> bool {
        matches!(self, Self::TaskNotRegistered | Self::TaskDuplicateName)
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_code_str())
    }
}

/// Base error type for horsies startup/validation errors.
#[derive(Debug, Clone)]
pub struct HorsiesError {
    pub message: String,
    pub code: Option<ErrorCode>,
    pub notes: Vec<String>,
    pub help_text: Option<String>,
}

impl std::fmt::Display for HorsiesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(code) = &self.code {
            write!(f, "error[{}]: {}", code, self.message)?;
        } else {
            write!(f, "error: {}", self.message)?;
        }
        for note in &self.notes {
            write!(f, "\n  = note: {}", note)?;
        }
        if let Some(help) = &self.help_text {
            write!(f, "\n  = help: {}", help)?;
        }
        Ok(())
    }
}

impl std::error::Error for HorsiesError {}

impl HorsiesError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
            notes: Vec::new(),
            help_text: None,
        }
    }

    pub fn with_code(mut self, code: ErrorCode) -> Self {
        self.code = Some(code);
        self
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help_text = Some(help.into());
        self
    }

    /// Returns the error category derived from the error code, if present.
    ///
    /// This provides type-level discrimination matching Python's exception
    /// subclass hierarchy (`WorkflowValidationError`, `TaskDefinitionError`,
    /// `ConfigurationError`, `RegistryError`).
    ///
    /// # Example
    /// ```
    /// # use horsies::core::error::{HorsiesError, ErrorCode, ErrorCategory};
    /// let err = HorsiesError::new("bad config")
    ///     .with_code(ErrorCode::ConfigInvalidQueueMode);
    /// assert_eq!(err.category(), Some(ErrorCategory::Config));
    /// ```
    pub fn category(&self) -> Option<ErrorCategory> {
        self.code.map(|c| c.category())
    }

    /// Returns true if this error belongs to the given category.
    pub fn is_category(&self, category: ErrorCategory) -> bool {
        self.category() == Some(category)
    }

    /// Returns true if this is a workflow validation error.
    pub fn is_workflow_error(&self) -> bool {
        self.is_category(ErrorCategory::Workflow)
    }

    /// Returns true if this is a task definition error.
    pub fn is_task_error(&self) -> bool {
        self.is_category(ErrorCategory::Task)
    }

    /// Returns true if this is a configuration error.
    pub fn is_config_error(&self) -> bool {
        self.is_category(ErrorCategory::Config)
    }

    /// Returns true if this is a registry error.
    pub fn is_registry_error(&self) -> bool {
        self.is_category(ErrorCategory::Registry)
    }
}

/// Collects multiple HorsiesErrors within a validation phase.
///
/// Formats all collected errors together with a summary line,
/// similar to rustc's multi-error output.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub phase_name: String,
    pub errors: Vec<HorsiesError>,
}

impl ValidationReport {
    pub fn new(phase_name: impl Into<String>) -> Self {
        Self {
            phase_name: phase_name.into(),
            errors: Vec::new(),
        }
    }

    pub fn add(&mut self, error: HorsiesError) {
        self.errors.push(error);
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Raise collected errors following the backward-compat rule:
    /// - 0 errors: returns Ok(())
    /// - 1 error: returns the original error
    /// - 2+ errors: returns a MultipleValidationErrors wrapping all
    pub fn into_result(self) -> Result<(), HorsiesError> {
        match self.errors.len() {
            0 => Ok(()),
            1 => Err(self.errors.into_iter().next().expect("length checked as 1")),
            n => {
                let message = format!("aborting due to {} previous errors", n);
                let mut combined = HorsiesError::new(message);
                for error in &self.errors {
                    combined.notes.push(format!("{}", error));
                }
                Err(combined)
            }
        }
    }
}

impl std::fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for error in &self.errors {
            writeln!(f, "{}", error)?;
        }
        write!(
            f,
            "error: aborting due to {} previous errors",
            self.errors.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(ErrorCode::WorkflowCycleDetected.to_string(), "HRS-007");
        assert_eq!(ErrorCode::TaskNotRegistered.to_string(), "HRS-300");
    }

    #[test]
    fn horsies_error_builder() {
        let err = HorsiesError::new("cycle detected in workflow DAG")
            .with_code(ErrorCode::WorkflowCycleDetected)
            .with_note("workflows must be acyclic directed graphs")
            .with_help("remove circular dependencies between tasks");

        let display = format!("{}", err);
        assert!(display.contains("HRS-007"));
        assert!(display.contains("cycle detected"));
        assert!(display.contains("note:"));
        assert!(display.contains("help:"));
    }

    #[test]
    fn validation_report_empty() {
        let report = ValidationReport::new("test");
        assert!(!report.has_errors());
        assert!(report.into_result().is_ok());
    }

    #[test]
    fn validation_report_single_error() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("bad config").with_code(ErrorCode::ConfigInvalidQueueMode));
        assert!(report.has_errors());
        let err = report.into_result().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::ConfigInvalidQueueMode));
    }

    #[test]
    fn validation_report_multiple_errors() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("error 1"));
        report.add(HorsiesError::new("error 2"));
        report.add(HorsiesError::new("error 3"));
        let err = report.into_result().unwrap_err();
        assert!(err.message.contains("3 previous errors"));
        assert_eq!(err.notes.len(), 3);
    }

    // --- ErrorCode category helper tests ---

    #[test]
    fn workflow_code_is_workflow_error_only() {
        let code = ErrorCode::WorkflowCycleDetected;
        assert!(code.is_workflow_error());
        assert!(!code.is_task_error());
        assert!(!code.is_config_error());
        assert!(!code.is_registry_error());
    }

    #[test]
    fn task_code_is_task_error_only() {
        let code = ErrorCode::TaskInvalidOptions;
        assert!(!code.is_workflow_error());
        assert!(code.is_task_error());
        assert!(!code.is_config_error());
        assert!(!code.is_registry_error());
    }

    #[test]
    fn config_code_is_config_error_only() {
        let code = ErrorCode::ConfigInvalidQueueMode;
        assert!(!code.is_workflow_error());
        assert!(!code.is_task_error());
        assert!(code.is_config_error());
        assert!(!code.is_registry_error());
    }

    #[test]
    fn registry_code_is_registry_error_only() {
        let code = ErrorCode::TaskNotRegistered;
        assert!(!code.is_workflow_error());
        assert!(!code.is_task_error());
        assert!(!code.is_config_error());
        assert!(code.is_registry_error());
    }

    // --- ErrorCode::category() tests ---

    #[test]
    fn error_code_category_workflow() {
        assert_eq!(
            ErrorCode::WorkflowCycleDetected.category(),
            ErrorCategory::Workflow
        );
        assert_eq!(
            ErrorCode::WorkflowNoName.category(),
            ErrorCategory::Workflow
        );
        assert_eq!(
            ErrorCode::WorkflowSubworkflowAppMissing.category(),
            ErrorCategory::Workflow
        );
    }

    #[test]
    fn error_code_category_task() {
        assert_eq!(ErrorCode::TaskNoReturnType.category(), ErrorCategory::Task);
        assert_eq!(ErrorCode::TaskInvalidQueue.category(), ErrorCategory::Task);
    }

    #[test]
    fn error_code_category_config() {
        assert_eq!(
            ErrorCode::ConfigInvalidQueueMode.category(),
            ErrorCategory::Config
        );
        assert_eq!(
            ErrorCode::BrokerInvalidUrl.category(),
            ErrorCategory::Config
        );
        assert_eq!(
            ErrorCode::WorkerInvalidLocator.category(),
            ErrorCategory::Config
        );
    }

    #[test]
    fn error_code_category_registry() {
        assert_eq!(
            ErrorCode::TaskNotRegistered.category(),
            ErrorCategory::Registry
        );
        assert_eq!(
            ErrorCode::TaskDuplicateName.category(),
            ErrorCategory::Registry
        );
    }

    // --- ErrorCategory Display tests ---

    #[test]
    fn error_category_display() {
        assert_eq!(ErrorCategory::Workflow.to_string(), "workflow validation");
        assert_eq!(ErrorCategory::Task.to_string(), "task definition");
        assert_eq!(ErrorCategory::Config.to_string(), "configuration");
        assert_eq!(ErrorCategory::Registry.to_string(), "registry");
    }

    // --- HorsiesError category methods ---

    #[test]
    fn horsies_error_category_from_code() {
        let err = HorsiesError::new("bad config").with_code(ErrorCode::ConfigInvalidQueueMode);
        assert_eq!(err.category(), Some(ErrorCategory::Config));
        assert!(err.is_config_error());
        assert!(!err.is_workflow_error());
        assert!(!err.is_task_error());
        assert!(!err.is_registry_error());
    }

    #[test]
    fn horsies_error_category_workflow() {
        let err = HorsiesError::new("cycle").with_code(ErrorCode::WorkflowCycleDetected);
        assert_eq!(err.category(), Some(ErrorCategory::Workflow));
        assert!(err.is_workflow_error());
    }

    #[test]
    fn horsies_error_category_none_without_code() {
        let err = HorsiesError::new("generic error");
        assert_eq!(err.category(), None);
        assert!(!err.is_workflow_error());
        assert!(!err.is_task_error());
        assert!(!err.is_config_error());
        assert!(!err.is_registry_error());
    }

    #[test]
    fn horsies_error_is_category() {
        let err = HorsiesError::new("not found").with_code(ErrorCode::TaskNotRegistered);
        assert!(err.is_category(ErrorCategory::Registry));
        assert!(!err.is_category(ErrorCategory::Workflow));
    }

    // -- HorsiesError creation and fluent API (ported from Python test_errors.py) --

    #[test]
    fn horsies_error_basic_creation_defaults() {
        let err = HorsiesError::new("something went wrong");
        assert_eq!(err.message, "something went wrong");
        assert!(err.code.is_none());
        assert!(err.notes.is_empty());
        assert!(err.help_text.is_none());
    }

    #[test]
    fn horsies_error_creation_with_code() {
        let err = HorsiesError::new("bad cycle").with_code(ErrorCode::WorkflowCycleDetected);
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
        assert!(err.notes.is_empty());
        assert!(err.help_text.is_none());
    }

    #[test]
    fn horsies_error_fluent_api_chained() {
        let err = HorsiesError::new("problem")
            .with_code(ErrorCode::WorkflowNoNodes)
            .with_note("first note")
            .with_note("second note")
            .with_help("try adding tasks");
        assert_eq!(err.notes.len(), 2);
        assert_eq!(err.notes[0], "first note");
        assert_eq!(err.notes[1], "second note");
        assert_eq!(err.help_text.as_deref(), Some("try adding tasks"));
    }

    #[test]
    fn horsies_error_display_without_code() {
        let err = HorsiesError::new("generic error");
        let s = err.to_string();
        assert!(s.contains("generic error"));
    }

    #[test]
    fn horsies_error_display_with_code() {
        let err = HorsiesError::new("cycle detected").with_code(ErrorCode::WorkflowCycleDetected);
        let s = err.to_string();
        assert!(s.contains("cycle detected"));
    }

    #[test]
    fn horsies_error_implements_std_error() {
        let err = HorsiesError::new("test");
        // Verify it implements std::error::Error
        let _: &dyn std::error::Error = &err;
    }

    // -- Error code well-formedness (ported from Python TestErrorCode) --

    #[test]
    fn error_code_display_format() {
        // All error codes should display as "HRS-XXX" format.
        let code = ErrorCode::WorkflowNoName;
        let s = code.to_string();
        assert!(
            s.starts_with("HRS-"),
            "code should start with 'HRS-': {}",
            s
        );
        assert!(s.len() == 7, "code should be 7 chars (HRS-XXX): {}", s);
    }

    // -- ValidationReport additional tests (ported from Python test_errors.py) --

    #[test]
    fn validation_report_stores_phase_name() {
        let report = ValidationReport::new("dag_validation");
        assert_eq!(report.phase_name, "dag_validation");
    }

    #[test]
    fn validation_report_add_multiple_errors() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("err1"));
        report.add(HorsiesError::new("err2"));
        report.add(HorsiesError::new("err3"));
        assert_eq!(report.errors.len(), 3);
        assert!(report.has_errors());
    }

    #[test]
    fn validation_report_display_contains_errors() {
        let mut report = ValidationReport::new("test_phase");
        report.add(HorsiesError::new("first error"));
        report.add(HorsiesError::new("second error"));
        let s = report.to_string();
        assert!(s.contains("first error"));
        assert!(s.contains("second error"));
    }

    #[test]
    fn validation_report_display_empty() {
        let report = ValidationReport::new("empty_phase");
        let s = report.to_string();
        assert!(s.contains("0") || s.is_empty() || s.contains("no errors"));
    }

    #[test]
    fn into_result_two_errors_message_contains_count() {
        let mut report = ValidationReport::new("test");
        report.add(HorsiesError::new("err1").with_code(ErrorCode::WorkflowNoName));
        report.add(HorsiesError::new("err2").with_code(ErrorCode::WorkflowNoNodes));
        let result: Result<(), HorsiesError> = report.into_result();
        let err = result.unwrap_err();
        assert!(
            err.message.contains("2"),
            "multi-error message should contain count, got: {}",
            err.message,
        );
    }
}
