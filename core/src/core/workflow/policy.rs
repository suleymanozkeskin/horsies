use serde::{Deserialize, Serialize};

use crate::core::workflow::status::WorkflowTaskStatus;

/// A single success case: workflow succeeds if all required task indices completed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SuccessCase {
    /// Task indices (into `WorkflowSpec.tasks`) that must be `COMPLETED`.
    pub required_indices: Vec<usize>,
}

/// Custom success criteria for a workflow.
///
/// A workflow with a `SuccessPolicy` succeeds if **any** case in `cases`
/// is fully satisfied (all its required tasks completed).
///
/// `optional_indices` is declarative: it records which tasks may fail, and
/// it has no runtime effect. The completion reads only the required indices
/// of each case. A task that no case requires does not change the result,
/// with or without `optional_indices` (Python `SuccessPolicy.optional`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SuccessPolicy {
    /// One or more success cases. The workflow succeeds if any single case
    /// has all its required indices in COMPLETED status.
    pub cases: Vec<SuccessCase>,

    /// Declarative: the task indices that may fail. No runtime effect. The
    /// spec check refuses an index that is not a task of the workflow, and
    /// the workflow start stores the list.
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
    /// Returns `true` if any case with at least one required index has all its required indices in
    /// COMPLETED status. A case with no required index is skipped, as in Python
    /// (`core/workflows/engine.py:3511-3514`); a spec build refuses such a case.
    /// `task_statuses` maps task index to its current `WorkflowTaskStatus`.
    pub fn evaluate(&self, task_statuses: &[WorkflowTaskStatus]) -> bool {
        self.cases.iter().any(|case| {
            !case.required_indices.is_empty()
                && case.required_indices.iter().all(|&idx| {
                    task_statuses
                        .get(idx)
                        .is_some_and(|s| *s == WorkflowTaskStatus::Completed)
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_case_serde_round_trip() {
        let case = SuccessCase {
            required_indices: vec![0, 2, 4],
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
                },
                SuccessCase {
                    required_indices: vec![0, 2],
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
                },
                SuccessCase {
                    required_indices: vec![0, 2],
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
                },
                SuccessCase {
                    required_indices: vec![0, 2],
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
                },
                SuccessCase {
                    required_indices: vec![0, 2],
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
    fn a_case_with_no_required_index_is_skipped() {
        let empty_only = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![],
            }],
            optional_indices: None,
        };
        assert!(!empty_only.evaluate(&[WorkflowTaskStatus::Completed]));
        let empty_then_real = SuccessPolicy {
            cases: vec![
                SuccessCase {
                    required_indices: vec![],
                },
                SuccessCase {
                    required_indices: vec![0],
                },
            ],
            optional_indices: None,
        };
        assert!(!empty_then_real.evaluate(&[WorkflowTaskStatus::Failed]));
        assert!(empty_then_real.evaluate(&[WorkflowTaskStatus::Completed]));
    }

    #[test]
    fn evaluate_index_out_of_bounds_is_false() {
        let policy = SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0, 99],
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
            }],
            optional_indices: None,
        };
        let statuses = vec![WorkflowTaskStatus::Skipped];
        assert!(!policy.evaluate(&statuses));
    }
}
