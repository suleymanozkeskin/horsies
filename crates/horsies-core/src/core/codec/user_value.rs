//! The rules for a JSON value that user code builds: a task input value, a result value, the `data` of a task
//! error (Python `core/codec/typed.py`, `_scan_reserved_keys` `:1597-1626`; `core/codec/json_value.py:54-75`).
//!
//! - No key that only the engine writes, at any depth: a key that starts with `__h_` or `__horsies_`, and the
//!   built-in code tag `__builtin_task_code__`. Only the `error_code` of a stored error holds that tag.
//! - At most [`MAX_VALUE_JSON_DEPTH`] levels of arrays and objects. A stored text may have
//!   [`MAX_JSON_DEPTH`] levels, so the engine has room for the envelopes that it puts around a value.

use super::json_text::JsonText;
use super::strict_read::{text_shape, MAX_INT_DIGITS, MAX_JSON_DEPTH};

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
    /// An integer has more than [`MAX_INT_DIGITS`] digits. Only a value built with the `arbitrary-precision`
    /// feature can hold one; Python's writer refuses it (`json.dumps`, the integer text limit).
    #[error("an integer has more than {MAX_INT_DIGITS} digits")]
    IntegerTooLong,
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
            Self::IntegerTooLong => "SerializationError",
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
            serde_json::Value::Number(number) if integer_is_too_long(number) => {
                return Err(UserValueRefused::IntegerTooLong);
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

/// Check a user value from its text, with the rules of [`check_user_value`]: no engine key at any depth, and at
/// most `depth_limit` levels of arrays and objects. An engine key is reported before a depth past
/// `depth_limit`; of two engine keys, the first in text order. A [`JsonText`] passed the strict read rule, so it
/// is never deeper than [`MAX_JSON_DEPTH`] levels and holds no integer of more than [`MAX_INT_DIGITS`] digits.
///
/// Pure. The text is read once and no value is built, so an integer outside the `i64` and `u64` range is
/// checked as it is stored.
pub fn check_user_value_text(value: &JsonText, depth_limit: usize) -> Result<(), UserValueRefused> {
    let shape =
        text_shape(value.get()).expect("the text of a JsonText passed the strict read rule");
    match (shape.engine_key, shape.deepest > depth_limit) {
        (Some(key), _) => Err(UserValueRefused::EngineKey { key }),
        (None, true) => Err(UserValueRefused::DeeperThanValueBound { limit: depth_limit }),
        (None, false) => Ok(()),
    }
}

/// Whether an integer has more than [`MAX_INT_DIGITS`] digits. A number that fits `i64`, `u64` or `f64` never
/// does; only an integer of the `arbitrary-precision` feature is written out to count its digits.
fn integer_is_too_long(number: &serde_json::Number) -> bool {
    if number.is_i64() || number.is_u64() || number.is_f64() {
        return false;
    }
    let text = number.to_string();
    let is_integer_text = !text.contains(&['.', 'e', 'E'][..]);
    is_integer_text && text.trim_start_matches('-').len() > MAX_INT_DIGITS
}

/// Why a task input is refused: the position and the rule (Python `encode_kwargs` and `decode_kwargs`,
/// `core/codec/kwargs.py:496-551`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InputRefused {
    /// A positional value breaks a rule of a user value.
    #[error("positional argument {index}: {refusal}")]
    Arg {
        index: usize,
        refusal: UserValueRefused,
    },
    /// A kwarg has a name that only the engine writes.
    #[error("reserved key {name:?} in user-originated data")]
    KwargName { name: String },
    /// A kwarg value breaks a rule of a user value.
    #[error("kwarg '{name}': {refusal}")]
    Kwarg {
        name: String,
        refusal: UserValueRefused,
    },
}

/// Check the user positions of a task input: each positional value, and each kwarg value whose name is not in
/// `engine_kwargs`, follows the rules of a user value with [`MAX_VALUE_JSON_DEPTH`] levels, and no kwarg name
/// is an engine key. A client's send passes no engine kwargs; the worker passes the `args_from` kwargs, whose
/// result envelopes the receiving field reads under the result rules. Pure.
pub fn check_task_input(
    args: &[serde_json::Value],
    kwargs: &serde_json::Map<String, serde_json::Value>,
    engine_kwargs: &std::collections::HashSet<String>,
) -> Result<(), InputRefused> {
    for (index, value) in args.iter().enumerate() {
        check_user_value(value, MAX_VALUE_JSON_DEPTH)
            .map_err(|refusal| InputRefused::Arg { index, refusal })?;
    }
    for (name, value) in kwargs {
        if is_engine_key(name) {
            return Err(InputRefused::KwargName { name: name.clone() });
        }
        if engine_kwargs.contains(name) {
            continue;
        }
        check_user_value(value, MAX_VALUE_JSON_DEPTH).map_err(|refusal| InputRefused::Kwarg {
            name: name.clone(),
            refusal,
        })?;
    }
    Ok(())
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

    /// The text check gives the refusals of the value check, also for a value whose integers are outside the
    /// `i64` and `u64` range.
    #[test]
    fn the_text_check_gives_the_refusals_of_the_value_check() {
        let text = |value: &serde_json::Value| JsonText::parse(&value.to_string()).unwrap();
        for value in [
            json!({"a": [1, {"b": null}], "note": "__h_task_result__"}),
            json!({"a": {"__h_x": 1}}),
            json!({"__builtin_task_code__": "X"}),
            nested_list(MAX_VALUE_JSON_DEPTH),
            nested_list(MAX_VALUE_JSON_DEPTH + 1),
            json!(7),
        ] {
            assert_eq!(
                check_user_value_text(&text(&value), MAX_VALUE_JSON_DEPTH),
                check_user_value(&value, MAX_VALUE_JSON_DEPTH),
                "{value}"
            );
        }
        let mut deep_engine_key = json!({"__h_x": 1});
        for _ in 0..MAX_VALUE_JSON_DEPTH {
            deep_engine_key = serde_json::Value::Array(vec![deep_engine_key]);
        }
        assert_eq!(
            check_user_value_text(&text(&deep_engine_key), MAX_VALUE_JSON_DEPTH),
            Err(UserValueRefused::EngineKey {
                key: "__h_x".to_owned()
            })
        );
        let wide = JsonText::parse(
            r#"{"wide": 340282366920938463463374607431768211455, "below": [-9223372036854775809]}"#,
        )
        .unwrap();
        assert_eq!(
            check_user_value_text(&wide, 1),
            Err(UserValueRefused::DeeperThanValueBound { limit: 1 })
        );
        assert_eq!(check_user_value_text(&wide, 2), Ok(()));
    }

    #[test]
    fn a_task_input_follows_the_user_value_rules() {
        let no_engine = std::collections::HashSet::new();
        let kwargs = |value: serde_json::Value| value.as_object().unwrap().clone();
        assert_eq!(
            check_task_input(&[json!(1)], &kwargs(json!({"a": {"b": [1]}})), &no_engine),
            Ok(())
        );
        assert_eq!(
            check_task_input(&[json!({"__h_x": 1})], &serde_json::Map::new(), &no_engine),
            Err(InputRefused::Arg {
                index: 0,
                refusal: UserValueRefused::EngineKey {
                    key: "__h_x".to_owned()
                }
            })
        );
        assert_eq!(
            check_task_input(&[], &kwargs(json!({"__h_workflow_ctx__": {}})), &no_engine),
            Err(InputRefused::KwargName {
                name: "__h_workflow_ctx__".to_owned()
            })
        );
        assert_eq!(
            check_task_input(&[], &kwargs(json!({"a": nested_list(201)})), &no_engine),
            Err(InputRefused::Kwarg {
                name: "a".to_owned(),
                refusal: UserValueRefused::DeeperThanValueBound {
                    limit: MAX_VALUE_JSON_DEPTH
                }
            })
        );
        let dep = kwargs(json!({"dep": {"__h_task_result__": true, "ok": 1, "err": null}}));
        let engine: std::collections::HashSet<String> = ["dep".to_owned()].into_iter().collect();
        assert_eq!(check_task_input(&[], &dep, &engine), Ok(()));
        assert!(check_task_input(&[], &dep, &no_engine).is_err());
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
        assert_eq!(
            UserValueRefused::IntegerTooLong.stored_cause(),
            "SerializationError"
        );
    }
}
