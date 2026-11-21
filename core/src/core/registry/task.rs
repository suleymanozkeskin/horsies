use std::collections::HashMap;

use crate::core::error::{ErrorCode, HorsiesError};
use crate::core::task::fn_trait::RegisteredTask;

/// Registry mapping task names to their registered implementations.
///
/// Enforces:
/// - Unique task names (duplicate registration is an error)
/// - Lookup by name for the worker to resolve tasks
#[derive(Clone)]
pub struct TaskRegistry {
    tasks: HashMap<String, RegisteredTask>,
}

impl TaskRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    /// Register a task. Returns an error if the name is already taken.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        task: RegisteredTask,
    ) -> Result<(), HorsiesError> {
        let name = name.into();

        if name.is_empty() {
            return Err(HorsiesError::new("task name must not be empty")
                .with_code(ErrorCode::TaskInvalidOptions));
        }

        if self.tasks.contains_key(&name) {
            return Err(
                HorsiesError::new(format!("duplicate task name: '{}'", name))
                    .with_code(ErrorCode::TaskDuplicateName)
                    .with_note(format!("task '{}' is already registered", name))
                    .with_help("each task must have a unique name"),
            );
        }

        self.tasks.insert(name, task);
        Ok(())
    }

    /// Look up a task by name. Returns an error if not found.
    pub fn get(&self, name: &str) -> Result<&RegisteredTask, HorsiesError> {
        self.tasks.get(name).ok_or_else(|| {
            HorsiesError::new(format!("task '{}' not registered", name))
                .with_code(ErrorCode::TaskNotRegistered)
                .with_help("register the task with app.register() before sending")
        })
    }

    /// Check if a task is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.tasks.contains_key(name)
    }

    /// Number of registered tasks.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Remove a task by name. Returns true if the task was present.
    pub fn unregister(&mut self, name: &str) -> bool {
        self.tasks.remove(name).is_some()
    }

    /// Iterate over registered task names.
    pub fn task_names(&self) -> impl Iterator<Item = &str> {
        self.tasks.keys().map(String::as_str)
    }

    /// Iterate over registered tasks and their names.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &RegisteredTask)> {
        self.tasks.iter().map(|(name, task)| (name.as_str(), task))
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TaskRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRegistry")
            .field("task_count", &self.tasks.len())
            .field("task_names", &self.tasks.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, TaskMeta};

    struct DummyTask;

    impl BlockingTaskFn for DummyTask {
        fn execute(&self, args: &[u8]) -> RawTaskResult {
            crate::core::task::result::TaskResult::Ok(args.to_vec())
        }
    }

    fn dummy_registered() -> RegisteredTask {
        RegisteredTask::Blocking {
            task: std::sync::Arc::new(DummyTask),
            meta: TaskMeta::default(),
        }
    }

    #[test]
    fn register_and_get() {
        let mut registry = TaskRegistry::new();
        registry.register("my_task", dummy_registered()).unwrap();
        assert!(registry.contains("my_task"));
        assert!(registry.get("my_task").is_ok());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut registry = TaskRegistry::new();
        registry.register("dup", dummy_registered()).unwrap();
        let result = registry.register("dup", dummy_registered());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskDuplicateName));
    }

    #[test]
    fn empty_name_rejected() {
        let mut registry = TaskRegistry::new();
        let result = registry.register("", dummy_registered());
        assert!(result.is_err());
    }

    #[test]
    fn not_registered_error() {
        let registry = TaskRegistry::new();
        let result = registry.get("missing");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskNotRegistered));
    }

    #[test]
    fn task_names_iterator() {
        let mut registry = TaskRegistry::new();
        registry.register("alpha", dummy_registered()).unwrap();
        registry.register("beta", dummy_registered()).unwrap();
        let mut names: Vec<&str> = registry.task_names().collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn empty_registry() {
        let registry = TaskRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn unregister_existing_task_returns_true_and_removes_it() {
        let mut registry = TaskRegistry::new();
        registry.register("removable", dummy_registered()).unwrap();
        assert!(registry.contains("removable"));

        let removed = registry.unregister("removable");
        assert!(removed);
        assert!(!registry.contains("removable"));
        assert!(registry.get("removable").is_err());
    }

    #[test]
    fn unregister_nonexistent_task_returns_false() {
        let mut registry = TaskRegistry::new();
        let removed = registry.unregister("does_not_exist");
        assert!(!removed);
    }

    #[test]
    fn re_register_after_unregister() {
        let mut registry = TaskRegistry::new();
        registry.register("reuse", dummy_registered()).unwrap();
        registry.unregister("reuse");

        let result = registry.register("reuse", dummy_registered());
        assert!(result.is_ok());
        assert!(registry.contains("reuse"));
    }

    #[test]
    fn len_decreases_after_unregister() {
        let mut registry = TaskRegistry::new();
        registry.register("one", dummy_registered()).unwrap();
        registry.register("two", dummy_registered()).unwrap();
        assert_eq!(registry.len(), 2);

        registry.unregister("one");
        assert_eq!(registry.len(), 1);

        registry.unregister("two");
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
    }
}
