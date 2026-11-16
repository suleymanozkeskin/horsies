use serde::{Deserialize, Serialize};

use crate::core::workflow::status::WorkflowTaskStatus;

/// A single success case: workflow succeeds if all required task indices completed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SuccessCase {
    /// Task indices (into `WorkflowSpec.tasks`) that must be `COMPLETED`.
    pub required_indices: Vec<usize>,

    /// Optional human-readable name for this success case.
    ///
    /// When set, this name is preserved in `SubWorkflowSummary.success_case`
    /// to indicate which case was satisfied. Useful for debugging and
    /// conditional logic in parent workflows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Custom success criteria for a workflow.
///
/// A workflow with a `SuccessPolicy` succeeds if **any** case in `cases`
/// is fully satisfied (all its required tasks completed).
///
/// Tasks listed in `optional_indices` are allowed to fail without
/// causing the workflow to fail, even when no explicit case matches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SuccessPolicy {
    /// One or more success cases. The workflow succeeds if any single case
    /// has all its required indices in COMPLETED status.
    pub cases: Vec<SuccessCase>,

    /// Task indices that may fail without affecting success evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional_indices: Option<Vec<usize>>,
}

impl SuccessPolicy {
    /// Serialize to JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize from JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Evaluate whether the workflow should be considered successful.
    ///
    /// Returns `true` if any case has all its required indices in COMPLETED status.
    /// `task_statuses` maps task index to its current `WorkflowTaskStatus`.
    pub fn evaluate(&self, task_statuses: &[WorkflowTaskStatus]) -> bool {
        self.cases.iter().any(|case| {
            case.required_indices.iter().all(|&idx| {
                task_statuses
                    .get(idx)
                    .is_some_and(|s| *s == WorkflowTaskStatus::Completed)
            })
        })
    }

    /// Evaluate success and return the name of the first satisfied case.
    ///
    /// Returns `Some(name)` for the first case that is fully satisfied
    /// (all required indices COMPLETED). The name comes from `SuccessCase.name`;
    /// if the satisfied case has no name, returns `None` even though the
    /// workflow is considered successful.
    ///
    /// Use `evaluate()` to get a simple bool, or this method when you need
    /// to preserve which case was satisfied (e.g., for `SubWorkflowSummary.success_case`).
    pub fn evaluate_with_case_name(&self, task_statuses: &[WorkflowTaskStatus]) -> Option<String> {
        for case in &self.cases {
            let satisfied = case.required_indices.iter().all(|&idx| {
                task_statuses
                    .get(idx)
                    .is_some_and(|s| *s == WorkflowTaskStatus::Completed)
            });
            if satisfied {
                return case.name.clone();
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_case_serde_round_trip() {
        let case = SuccessCase {
            required_indices: vec![0, 2, 4],
            name: None,
        };
        let json = serde_json::to_string(&case).unwrap();
        let back: SuccessCase = serde_json::from_str(&json).unwrap();
        assert_eq!(back, case);
    }

    #[test]
    fn success_policy_to_from_json() {
        let policy = SuccessPolicy {
            cases: vec![
                SuccessCase {
                    required_indices: vec![0, 1],
                    name: None,
                },
                SuccessCase {
                    required_indices: vec![0, 2],
                    name: None,
                },
            ],
            optional_indices: Some(vec![3]),
        };
        let json = policy.to_json().unwrap();
        let back = SuccessPolicy::from_json(&json).unwrap();
        assert_eq!(back, policy);
    }

    #[test]
    fn success_policy_no_optional() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0],
                name: None,
            }],
            optional_indices: None,
        };
        let json = policy.to_json().unwrap();
        // optional_indices should be absent due to skip_serializing_if
        assert!(!json.contains("optional_indices"));
    }

    #[test]
    fn evaluate_first_case_satisfied() {
        let policy = SuccessPolicy {
            cases: vec![
                SuccessCase {
                    required_indices: vec![0, 1],
                    name: None,
                },
                SuccessCase {
                    required_indices: vec![0, 2],
                    name: None,
                },
            ],
            optional_indices: None,
        };
        let statuses = vec![
            WorkflowTaskStatus::Completed, // 0
            WorkflowTaskStatus::Completed, // 1
            WorkflowTaskStatus::Failed,    // 2
        ];
        assert!(policy.evaluate(&statuses));
    }

    #[test]
    fn evaluate_second_case_satisfied() {
        let policy = SuccessPolicy {
            cases: vec![
                SuccessCase {
                    required_indices: vec![0, 1],
                    name: None,
                },
                SuccessCase {
                    required_indices: vec![0, 2],
                    name: None,
                },
            ],
            optional_indices: None,
        };
        let statuses = vec![
            WorkflowTaskStatus::Completed, // 0
            WorkflowTaskStatus::Failed,    // 1
            WorkflowTaskStatus::Completed, // 2
        ];
        assert!(policy.evaluate(&statuses));
    }

    #[test]
    fn evaluate_no_case_satisfied() {
        let policy = SuccessPolicy {
            cases: vec![
                SuccessCase {
                    required_indices: vec![0, 1],
                    name: None,
                },
                SuccessCase {
                    required_indices: vec![0, 2],
                    name: None,
                },
            ],
            optional_indices: None,
        };
        let statuses = vec![
            WorkflowTaskStatus::Failed,    // 0
            WorkflowTaskStatus::Completed, // 1
            WorkflowTaskStatus::Completed, // 2
        ];
        assert!(!policy.evaluate(&statuses));
    }

    #[test]
    fn evaluate_empty_cases_is_false() {
        let policy = SuccessPolicy {
            cases: vec![],
            optional_indices: None,
        };
        let statuses = vec![WorkflowTaskStatus::Completed];
        assert!(!policy.evaluate(&statuses));
    }

    #[test]
    fn evaluate_index_out_of_bounds_is_false() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0, 99],
                name: None,
            }],
            optional_indices: None,
        };
        let statuses = vec![WorkflowTaskStatus::Completed];
        assert!(!policy.evaluate(&statuses));
    }

    #[test]
    fn evaluate_pending_is_not_completed() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0],
                name: None,
            }],
            optional_indices: None,
        };
        let statuses = vec![WorkflowTaskStatus::Pending];
        assert!(!policy.evaluate(&statuses));
    }

    #[test]
    fn evaluate_skipped_is_not_completed() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0],
                name: None,
            }],
            optional_indices: None,
        };
        let statuses = vec![WorkflowTaskStatus::Skipped];
        assert!(!policy.evaluate(&statuses));
    }

    // -----------------------------------------------------------------------
    // evaluate_with_case_name tests
    // -----------------------------------------------------------------------

    #[test]
    fn case_name_first_satisfied() {
        let policy = SuccessPolicy {
            cases: vec![
                SuccessCase {
                    required_indices: vec![0, 1],
                    name: Some("primary".to_owned()),
                },
                SuccessCase {
                    required_indices: vec![0, 2],
                    name: Some("fallback".to_owned()),
                },
            ],
            optional_indices: None,
        };
        let statuses = vec![
            WorkflowTaskStatus::Completed, // 0
            WorkflowTaskStatus::Completed, // 1
            WorkflowTaskStatus::Failed,    // 2
        ];
        assert_eq!(
            policy.evaluate_with_case_name(&statuses),
            Some("primary".to_owned()),
        );
    }

    #[test]
    fn case_name_second_satisfied() {
        let policy = SuccessPolicy {
            cases: vec![
                SuccessCase {
                    required_indices: vec![0, 1],
                    name: Some("primary".to_owned()),
                },
                SuccessCase {
                    required_indices: vec![0, 2],
                    name: Some("fallback".to_owned()),
                },
            ],
            optional_indices: None,
        };
        let statuses = vec![
            WorkflowTaskStatus::Completed, // 0
            WorkflowTaskStatus::Failed,    // 1
            WorkflowTaskStatus::Completed, // 2
        ];
        assert_eq!(
            policy.evaluate_with_case_name(&statuses),
            Some("fallback".to_owned()),
        );
    }

    #[test]
    fn case_name_none_when_no_case_satisfied() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0, 1],
                name: Some("primary".to_owned()),
            }],
            optional_indices: None,
        };
        let statuses = vec![
            WorkflowTaskStatus::Failed,    // 0
            WorkflowTaskStatus::Completed, // 1
        ];
        assert!(policy.evaluate_with_case_name(&statuses).is_none());
    }

    #[test]
    fn case_name_none_when_unnamed_case_satisfied() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0],
                name: None,
            }],
            optional_indices: None,
        };
        let statuses = vec![WorkflowTaskStatus::Completed];
        // Satisfied but unnamed -> None
        assert!(policy.evaluate_with_case_name(&statuses).is_none());
        // evaluate() should still be true though
        assert!(policy.evaluate(&statuses));
    }

    #[test]
    fn success_case_name_serde_round_trip() {
        let case = SuccessCase {
            required_indices: vec![0, 1],
            name: Some("critical_path".to_owned()),
        };
        let json = serde_json::to_string(&case).unwrap();
        assert!(json.contains("critical_path"));
        let back: SuccessCase = serde_json::from_str(&json).unwrap();
        assert_eq!(back, case);
    }

    #[test]
    fn success_case_name_none_absent_in_json() {
        let case = SuccessCase {
            required_indices: vec![0],
            name: None,
        };
        let json = serde_json::to_string(&case).unwrap();
        assert!(!json.contains("name"));
    }
}
