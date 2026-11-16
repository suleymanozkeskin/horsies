//! The rules for a JSON value that user code builds: a task input value, a result value, the `data` of a task
//! error (Python `core/codec/typed.py`, `_scan_reserved_keys` `:1597-1626`; `core/codec/json_value.py:54-75`).
//!
//! - No key that only the engine writes, at any depth: a key that starts with `__h_` or `__horsies_`, and the
//!   built-in code tag `__builtin_task_code__`. Only the `error_code` of a stored error holds that tag.
//! - At most [`MAX_VALUE_JSON_DEPTH`] levels of arrays and objects. A stored text may have
//!   [`MAX_JSON_DEPTH`] levels, so the engine has room for the envelopes that it puts around a value.

use super::strict_read::MAX_JSON_DEPTH;

/// Key prefixes that only the engine writes (Python `_RESERVED_KEY_PREFIXES`, `core/codec/typed.py:152`).
pub const ENGINE_KEY_PREFIXES: [&str; 2] = ["__h_", "__horsies_"];

/// The tag key of a built-in error code (Python `_RESERVED_DISCRIMINATOR`, `core/codec/typed.py:153`).
pub const BUILTIN_CODE_KEY: &str = "__builtin_task_code__";

/// The deepest nesting of a value that user code builds (Python `MAX_TYPED_JSON_DEPTH`,
/// `core/codec/json_value.py:67`). The outermost array or object is level 1.
pub const MAX_VALUE_JSON_DEPTH: usize = 200;

/// The most levels that the engine puts around a value in a stored text: the result envelope, the error object,
/// the sub-workflow summary, the `args_from` wrap, the workflow context and the kwargs object. Python keeps
/// fewer than 16 (`core/codec/json_value.py:55-64`).
pub const MAX_ENGINE_LEVELS: usize = 16;

const _: () = assert!(MAX_VALUE_JSON_DEPTH + MAX_ENGINE_LEVELS <= MAX_JSON_DEPTH);

/// Why a user value cannot cross the boundary.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UserValueRefused {
    /// An object holds a key that only the engine writes.
    #[error("reserved key {key:?} in user-originated data")]
    EngineKey { key: String },
    /// The value is nested deeper than the bound of a value.
    #[error("value is nested deeper than {limit} levels")]
    DeeperThanValueBound { limit: usize },
    /// The value is nested deeper than any stored text may be.
    #[error("value is nested deeper than {MAX_JSON_DEPTH} levels")]
    DeeperThanTextBound,
}

impl UserValueRefused {
    /// The cause that Python stores in `data_refused` for this refusal: the name of the error class that its
    /// codec raises (`reportable_error`, `core/codec/typed.py:1007-1028`). A reserved key and a value deeper than
    /// `MAX_JSON_DEPTH` raise `StrictJsonError`; a value deeper than `MAX_TYPED_JSON_DEPTH` raises
    /// `ValueValidationError`.
    pub fn stored_cause(&self) -> &'static str {
        match self {
            Self::EngineKey { .. } | Self::DeeperThanTextBound => "StrictJsonError",
            Self::DeeperThanValueBound { .. } => "ValueValidationError",
        }
    }
}

/// Whether only the engine may write this object key.
pub fn is_engine_key(key: &str) -> bool {
    key == BUILTIN_CODE_KEY
        || ENGINE_KEY_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
}

/// Check a user value: no engine key at any depth, and at most `depth_limit` levels of arrays and objects.
///
/// Pure. The walk uses its own stack and never goes below [`MAX_JSON_DEPTH`] levels, so a value of any depth
/// is checked without recursion. An engine key is reported before a depth past `depth_limit`, as Python scans
/// the keys before it decodes the value.
pub fn check_user_value(
    value: &serde_json::Value,
    depth_limit: usize,
) -> Result<(), UserValueRefused> {
    let mut pending: Vec<(&serde_json::Value, usize)> = vec![(value, 0)];
    let mut deepest: usize = 0;
    while let Some((node, depth)) = pending.pop() {
        let level = depth + 1;
        match node {
            serde_json::Value::Array(items) => {
                if level > MAX_JSON_DEPTH {
                    return Err(UserValueRefused::DeeperThanTextBound);
                }
                deepest = deepest.max(level);
                pending.extend(items.iter().map(|item| (item, level)));
            }
            serde_json::Value::Object(fields) => {
                if level > MAX_JSON_DEPTH {
                    return Err(UserValueRefused::DeeperThanTextBound);
                }
                deepest = deepest.max(level);
                for (key, item) in fields {
                    if is_engine_key(key) {
                        return Err(UserValueRefused::EngineKey { key: key.clone() });
                    }
                    pending.push((item, level));
                }
            }
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => {}
        }
    }
    match deepest > depth_limit {
        true => Err(UserValueRefused::DeeperThanValueBound { limit: depth_limit }),
        false => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A list with `depth` levels and a number inside (Python `_nested_list`, `tests/unit/test_typed_codec.py:1004`).
    fn nested_list(depth: usize) -> serde_json::Value {
        let mut value = json!([1]);
        for _ in 1..depth {
            value = serde_json::Value::Array(vec![value]);
        }
        value
    }

    /// Python `tests/unit/test_typed_codec.py:474-610`: each engine key is refused at any depth.
    #[test]
    fn engine_keys_are_refused_at_any_depth() {
        for key in [
            "__h_task_result__",
            "__h_taskresult_envelope__",
            "__h_x",
            "__horsies_workflow_ctx__",
            "__builtin_task_code__",
        ] {
            let top = json!({ key: 1 });
            let deep = json!({"a": [{"b": { key: null }}]});
            for value in [top, deep] {
                assert_eq!(
                    check_user_value(&value, MAX_VALUE_JSON_DEPTH),
                    Err(UserValueRefused::EngineKey {
                        key: key.to_owned()
                    }),
                    "{value}"
                );
            }
        }
    }

    #[test]
    fn plain_keys_and_engine_texts_as_values_pass() {
        let value = json!({
            "_h_": 1,
            "h__": 2,
            "__builtin": 3,
            "note": "__h_task_result__",
            "list": ["__builtin_task_code__"],
        });
        assert_eq!(check_user_value(&value, MAX_VALUE_JSON_DEPTH), Ok(()));
    }

    /// Python `MAX_TYPED_JSON_DEPTH`: a value of 200 levels passes, 201 do not
    /// (`tests/unit/test_typed_codec.py:1194-1206`).
    #[test]
    fn depth_is_bounded_at_the_value_bound() {
        assert_eq!(
            check_user_value(&nested_list(MAX_VALUE_JSON_DEPTH), MAX_VALUE_JSON_DEPTH),
            Ok(())
        );
        assert_eq!(
            check_user_value(&nested_list(MAX_VALUE_JSON_DEPTH + 1), MAX_VALUE_JSON_DEPTH),
            Err(UserValueRefused::DeeperThanValueBound {
                limit: MAX_VALUE_JSON_DEPTH
            })
        );
        assert_eq!(
            check_user_value(&nested_list(MAX_JSON_DEPTH + 1), MAX_VALUE_JSON_DEPTH),
            Err(UserValueRefused::DeeperThanTextBound)
        );
        assert_eq!(check_user_value(&json!(7), 0), Ok(()));
    }

    #[test]
    fn an_engine_key_is_reported_before_the_depth() {
        let mut value = json!({"__h_x": 1});
        for _ in 0..MAX_VALUE_JSON_DEPTH {
            value = serde_json::Value::Array(vec![value]);
        }
        assert_eq!(
            check_user_value(&value, MAX_VALUE_JSON_DEPTH),
            Err(UserValueRefused::EngineKey {
                key: "__h_x".to_owned()
            })
        );
    }

    #[test]
    fn stored_causes_are_python_class_names() {
        assert_eq!(
            UserValueRefused::EngineKey {
                key: "__h_x".to_owned()
            }
            .stored_cause(),
            "StrictJsonError"
        );
        assert_eq!(
            UserValueRefused::DeeperThanTextBound.stored_cause(),
            "StrictJsonError"
        );
        assert_eq!(
            UserValueRefused::DeeperThanValueBound { limit: 199 }.stored_cause(),
            "ValueValidationError"
        );
    }
}
