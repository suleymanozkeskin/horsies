use serde::{de::DeserializeOwned, Serialize};

/// Deserialize a task's declared input type from the task input bytes.
///
/// The bytes are the task row's `input`: one JSON value, as the client
/// encoded it. The value is decoded as stored, with no conversion: an array
/// is one input value, `null` is never read as `{}`, and `{}` is never read
/// as `null`.
///
/// This helper keeps attribute-macro generated wrappers and the low-level
/// `async_task_fn!` / `blocking_task_fn!` macros on exactly the same
/// deserialization path.
#[doc(hidden)]
pub fn decode_task_input<T>(input: &[u8]) -> Result<T, crate::core::task::TaskError>
where
    T: DeserializeOwned,
{
    let candidate: serde_json::Value = crate::core::codec::from_json_bytes(input).map_err(|e| {
        crate::core::task::TaskError::builtin(
            crate::core::task::OperationalErrorCode::WorkerSerializationError,
            format!("failed to parse task input: {}", e),
        )
    })?;

    // Deserialize by reference (`&Value` implements `Deserializer`).
    T::deserialize(&candidate).map_err(|e| crate::core::task::TaskError {
        exception: None,
        error_code: crate::core::task::TaskErrorCode::from(
            crate::core::task::OperationalErrorCode::WorkerSerializationError,
        ),
        // The error names the type and the cause, never the value (Python
        // `core/task_decorator.py:1856-1914`).
        data: Some(serde_json::json!({
            "expected_type": std::any::type_name::<T>(),
            "validation_error": e.to_string(),
        })),
        message: Some(format!(
            "task input does not match declared input type {}",
            std::any::type_name::<T>(),
        )),
        kind: crate::core::task::TaskErrorKind::Plain,
    })
}

/// Serialize a task's success value and verify it round-trips through the
/// declared output type.
///
/// This mirrors Python's producer-side runtime return-contract validation
/// (`encode_returned_result`, `core/codec/typed.py:862-882`): the value must
/// not only serialize, it must deserialize back into the same declared `T`.
/// The read-back applies the rules of a user value: at most
/// `MAX_VALUE_JSON_DEPTH` (200) levels and no key that only the engine writes
/// (`__h_*`, `__horsies_*`, `__builtin_task_code__`; Python
/// `_scan_reserved_keys`, `core/codec/typed.py:286-295`). A value that does
/// not read back is `WORKER_SERIALIZATION_ERROR`, as in Python's child
/// (`core/worker/child_runner.py:1819-1855`); the error names the type and the
/// cause, never the value.
#[doc(hidden)]
pub fn encode_validated_task_output<T>(value: &T) -> crate::core::task::result::TaskResult<Vec<u8>>
where
    T: Serialize + DeserializeOwned,
{
    // Strict serialization rejects non-finite floats (NaN/±Infinity) instead of
    // letting serde_json coerce them to JSON `null` (N5, Python allow_nan=False
    // parity).
    let bytes = match crate::core::codec::to_json_bytes_strict(value) {
        Ok(bytes) => bytes,
        Err(e) => {
            return crate::core::task::result::TaskResult::Err(
                crate::core::task::TaskError::builtin(
                    crate::core::task::OperationalErrorCode::WorkerSerializationError,
                    format!("failed to serialize task result: {}", e),
                ),
            );
        }
    };

    if let Err(e) = crate::core::codec::from_json_bytes_with::<T>(
        &bytes,
        crate::core::codec::TextRule::UserValue,
    ) {
        return crate::core::task::result::TaskResult::Err(crate::core::task::TaskError {
            exception: None,
            error_code: crate::core::task::TaskErrorCode::from(
                crate::core::task::OperationalErrorCode::WorkerSerializationError,
            ),
            data: Some(serde_json::json!({
                "expected_type": std::any::type_name::<T>(),
                "validation_error": e.to_string(),
            })),
            message: Some(format!(
                "task returned a value that does not read back as its declared output type {}",
                std::any::type_name::<T>(),
            )),
            kind: crate::core::task::TaskErrorKind::Plain,
        });
    }

    crate::core::task::result::TaskResult::Ok(bytes)
}

/// Create an async [`RegisteredTask`] from an async function.
///
/// Wraps the serde boundary: deserializes a single argument from JSON bytes,
/// calls the function, serializes the result back to JSON bytes.
///
/// The task function must:
/// - Accept a single argument of type `$args_type` (use a struct for multiple fields)
/// - Return `Result<T, TaskError>` where `T: Serialize + DeserializeOwned`
///
/// # Examples
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
/// use horsies::task::TaskError;
///
/// #[derive(Serialize, Deserialize)]
/// struct AddArgs { a: i32, b: i32 }
///
/// async fn add(args: AddArgs) -> Result<i32, TaskError> {
///     Ok(args.a + args.b)
/// }
///
/// app.register("add", async_task_fn!(add, AddArgs));
/// ```
///
/// For no arguments, use `()`:
/// ```ignore
/// async fn heartbeat(_: ()) -> Result<String, TaskError> {
///     Ok("alive".to_owned())
/// }
/// app.register("heartbeat", async_task_fn!(heartbeat, ()));
/// ```
#[macro_export]
macro_rules! async_task_fn {
    ($fn_name:path, $args_type:ty) => {{
        struct __AsyncTaskWrapper;

        impl $crate::core::task::fn_trait::AsyncTaskFn for __AsyncTaskWrapper {
            fn execute(
                &self,
                args: &[u8],
            ) -> ::std::pin::Pin<
                Box<
                    dyn ::std::future::Future<Output = $crate::core::task::fn_trait::RawTaskResult>
                        + Send
                        + '_,
                >,
            > {
                let args = args.to_vec();
                Box::pin(async move {
                    let deserialized: $args_type =
                        match $crate::core::task::macros::decode_task_input(&args) {
                            Ok(v) => v,
                            Err(task_error) => {
                                return $crate::core::task::result::TaskResult::Err(task_error);
                            }
                        };
                    match $fn_name(deserialized).await {
                        Ok(value) => {
                            $crate::core::task::macros::encode_validated_task_output(&value)
                        }
                        Err(task_error) => $crate::core::task::result::TaskResult::Err(task_error),
                    }
                })
            }

            fn validate_input(
                &self,
                args: &[u8],
            ) -> ::std::result::Result<(), $crate::core::task::TaskError> {
                $crate::core::task::macros::decode_task_input::<$args_type>(args).map(|_| ())
            }
        }

        $crate::core::task::fn_trait::RegisteredTask::Async {
            task: ::std::sync::Arc::new(__AsyncTaskWrapper),
            meta: $crate::core::task::fn_trait::TaskMeta::for_input::<$args_type>()
                .in_module(module_path!()),
        }
    }};
}

/// Create a blocking [`RegisteredTask`] from a synchronous function.
///
/// Same serde boundary as [`async_task_fn!`], but for CPU-bound work
/// that runs on tokio's blocking thread pool.
///
/// # Examples
///
/// ```ignore
/// use serde::{Serialize, Deserialize};
/// use horsies::task::TaskError;
///
/// #[derive(Serialize, Deserialize)]
/// struct ResizeArgs { path: String, width: u32 }
///
/// fn resize(args: ResizeArgs) -> Result<String, TaskError> {
///     Ok(format!("resized {} to {}px", args.path, args.width))
/// }
///
/// app.register("resize", blocking_task_fn!(resize, ResizeArgs));
/// ```
#[macro_export]
macro_rules! blocking_task_fn {
    ($fn_name:path, $args_type:ty) => {{
        struct __BlockingTaskWrapper;

        impl $crate::core::task::fn_trait::BlockingTaskFn for __BlockingTaskWrapper {
            fn execute(&self, args: &[u8]) -> $crate::core::task::fn_trait::RawTaskResult {
                let deserialized: $args_type =
                    match $crate::core::task::macros::decode_task_input(args) {
                        Ok(v) => v,
                        Err(task_error) => {
                            return $crate::core::task::result::TaskResult::Err(task_error);
                        }
                    };
                match $fn_name(deserialized) {
                    Ok(value) => $crate::core::task::macros::encode_validated_task_output(&value),
                    Err(task_error) => $crate::core::task::result::TaskResult::Err(task_error),
                }
            }

            fn validate_input(
                &self,
                args: &[u8],
            ) -> ::std::result::Result<(), $crate::core::task::TaskError> {
                $crate::core::task::macros::decode_task_input::<$args_type>(args).map(|_| ())
            }
        }

        $crate::core::task::fn_trait::RegisteredTask::Blocking {
            task: ::std::sync::Arc::new(__BlockingTaskWrapper),
            meta: $crate::core::task::fn_trait::TaskMeta::for_input::<$args_type>()
                .in_module(module_path!()),
        }
    }};
}

/// Create an async [`RegisteredTask`] whose input is an object of named fields.
///
/// Same as [`async_task_fn!`]: the stored input decodes into `$args_type`.
/// Workflow `args_from` merges dependency results into that object by field
/// name.
///
/// # Example
///
/// ```ignore
/// #[derive(Serialize, Deserialize)]
/// struct MyArgs { name: String, count: i32 }
///
/// async fn process(args: MyArgs) -> Result<String, TaskError> {
///     Ok(format!("{}: {}", args.name, args.count))
/// }
///
/// app.register("process", async_task_fn_kwargs!(process, MyArgs));
/// ```
#[macro_export]
macro_rules! async_task_fn_kwargs {
    ($fn_name:path, $args_type:ty) => {{
        $crate::async_task_fn!($fn_name, $args_type)
    }};
}

/// Create a blocking [`RegisteredTask`] whose input is an object of named fields.
///
/// Blocking equivalent of [`async_task_fn_kwargs!`].
#[macro_export]
macro_rules! blocking_task_fn_kwargs {
    ($fn_name:path, $args_type:ty) => {{
        $crate::blocking_task_fn!($fn_name, $args_type)
    }};
}

#[cfg(test)]
mod tests {
    use super::decode_task_input;
    use crate::core::task::fn_trait::RegisteredTask;
    use crate::core::task::TaskError;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct AddArgs {
        a: i32,
        b: i32,
    }

    async fn add(args: AddArgs) -> Result<i32, TaskError> {
        Ok(args.a + args.b)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn multiply(args: AddArgs) -> Result<i32, TaskError> {
        Ok(args.a * args.b)
    }

    async fn heartbeat(_: ()) -> Result<String, TaskError> {
        Ok("alive".to_owned())
    }

    struct BadRoundTrip;

    impl Serialize for BadRoundTrip {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str("not-an-integer")
        }
    }

    impl<'de> Deserialize<'de> for BadRoundTrip {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let _ = i64::deserialize(deserializer)?;
            Ok(Self)
        }
    }

    async fn bad_round_trip(_: ()) -> Result<BadRoundTrip, TaskError> {
        Ok(BadRoundTrip)
    }

    #[test]
    fn decode_task_input_reads_an_object_input() {
        let input = serde_json::to_vec(&serde_json::json!({"a": 3, "b": 5})).unwrap();
        let parsed: AddArgs = decode_task_input(&input).unwrap();
        assert_eq!(parsed.a, 3);
        assert_eq!(parsed.b, 5);
    }

    #[test]
    fn decode_task_input_reads_an_array_as_one_value(/* C3 */) {
        // An array-valued input is one input value and decodes back to the
        // array, not to three separate values.
        let parsed: Vec<i32> = decode_task_input(b"[1,2,3]").unwrap();
        assert_eq!(parsed, vec![1, 2, 3]);
    }

    #[test]
    fn decode_task_input_keeps_null_and_empty_object_apart(/* C12 */) {
        // `null` is the input of a task without input; `{}` is an empty map.
        // A `None` map input stays `None`.
        let none: Option<std::collections::HashMap<String, i32>> =
            decode_task_input(b"null").unwrap();
        assert!(none.is_none());
        let empty: Option<std::collections::HashMap<String, i32>> =
            decode_task_input(b"{}").unwrap();
        assert_eq!(empty, Some(std::collections::HashMap::new()));
    }

    #[test]
    fn an_empty_object_is_refused_by_a_type_that_takes_no_object() {
        // The input is decoded as stored: `{}` is never read as `null`. A
        // unit input reads only `null`, its own encoding.
        decode_task_input::<()>(b"null").unwrap();
        let mismatch = crate::core::task::TaskErrorCode::from(
            crate::core::task::OperationalErrorCode::WorkerSerializationError,
        );
        let unit = decode_task_input::<()>(b"{}").unwrap_err();
        assert_eq!(unit.error_code, mismatch);
        let optional = decode_task_input::<Option<i32>>(b"{}").unwrap_err();
        assert_eq!(optional.error_code, mismatch);
        let number = decode_task_input::<i32>(b"{}").unwrap_err();
        assert_eq!(number.error_code, mismatch);
    }

    #[test]
    fn async_task_fn_macro_compiles() {
        let task = async_task_fn!(add, AddArgs);
        assert!(task.is_async());
    }

    #[test]
    fn validated_output_rejects_non_round_trippable_value() {
        let task = async_task_fn!(bad_round_trip, ());
        let RegisteredTask::Async { task, .. } = task else {
            panic!("expected async task");
        };

        let args = b"null".to_vec();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = rt.block_on(task.execute(&args));

        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            crate::core::task::TaskErrorCode::from(
                crate::core::task::OperationalErrorCode::WorkerSerializationError,
            ),
        );
        let data = err.data.expect("expected mismatch diagnostics");
        assert_eq!(data["expected_type"], std::any::type_name::<BadRoundTrip>());
        // The stored error never holds the returned value.
        assert!(data.get("actual_value").is_none());
    }

    /// Python `encode_value` scans the result for reserved keys and decodes it
    /// at `MAX_TYPED_JSON_DEPTH`: a value with an engine key, or deeper than 200
    /// levels, is `WORKER_SERIALIZATION_ERROR`; 200 levels pass, past
    /// serde_json's own limit of 128.
    #[test]
    fn encode_validated_output_applies_the_user_value_rules() {
        let serialization_error = crate::core::task::TaskErrorCode::from(
            crate::core::task::OperationalErrorCode::WorkerSerializationError,
        );
        for key in ["__h_task_result__", "__horsies_x", "__builtin_task_code__"] {
            let value = serde_json::json!({ "outer": [{ key: 1 }] });
            let err = super::encode_validated_task_output(&value).unwrap_err();
            assert_eq!(err.error_code, serialization_error, "{key}");
        }
        let nested = |depth: usize| {
            let mut value = serde_json::json!([1]);
            for _ in 1..depth {
                value = serde_json::Value::Array(vec![value]);
            }
            value
        };
        let at_bound = nested(crate::core::codec::MAX_VALUE_JSON_DEPTH);
        assert!(super::encode_validated_task_output(&at_bound).is_ok());
        let past_bound = nested(crate::core::codec::MAX_VALUE_JSON_DEPTH + 1);
        let err = super::encode_validated_task_output(&past_bound).unwrap_err();
        assert_eq!(err.error_code, serialization_error);
        assert!(err.data.expect("diagnostics")["validation_error"]
            .as_str()
            .unwrap()
            .contains("nested deeper than 200"));
    }

    #[test]
    fn encode_validated_output_rejects_non_finite_float() {
        #[derive(Serialize, Deserialize)]
        struct Out {
            value: f64,
        }

        // N5: a task returning NaN/±Infinity must fail closed with a
        // WorkerSerializationError naming the non-finite float, not persist
        // JSON `null`.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = super::encode_validated_task_output(&Out { value: bad }).unwrap_err();
            assert_eq!(
                err.error_code,
                crate::core::task::TaskErrorCode::from(
                    crate::core::task::OperationalErrorCode::WorkerSerializationError,
                ),
            );
            assert!(err.message.expect("message").contains("non-finite"));
        }

        // Finite floats (including f64::MAX) still encode.
        assert!(super::encode_validated_task_output(&Out { value: 3.5 }).is_ok());
        assert!(super::encode_validated_task_output(&Out { value: f64::MAX }).is_ok());
    }

    #[test]
    fn blocking_task_fn_macro_compiles() {
        let task = blocking_task_fn!(multiply, AddArgs);
        assert!(task.is_blocking());
    }

    #[tokio::test]
    async fn async_task_fn_executes() {
        let task = async_task_fn!(add, AddArgs);
        let args = serde_json::to_vec(&AddArgs { a: 3, b: 5 }).unwrap();
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Async { task: f, .. } => {
                let result = f.execute(&args).await.unwrap();
                let value: i32 = serde_json::from_slice(&result).unwrap();
                assert_eq!(value, 8);
            }
            _ => panic!("expected async task"),
        }
    }

    #[test]
    fn blocking_task_fn_executes() {
        let task = blocking_task_fn!(multiply, AddArgs);
        let args = serde_json::to_vec(&AddArgs { a: 4, b: 7 }).unwrap();
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(&args).unwrap();
                let value: i32 = serde_json::from_slice(&result).unwrap();
                assert_eq!(value, 28);
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[test]
    fn bad_args_returns_serialization_error() {
        let task = blocking_task_fn!(multiply, AddArgs);
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(b"not json");
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert!(err.message.unwrap().contains("failed to parse task input"));
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[test]
    fn typed_args_mismatch_returns_worker_serialization_error() {
        let task = blocking_task_fn!(multiply, AddArgs);
        let args = serde_json::to_vec(&serde_json::json!({"a": "oops", "b": 7})).unwrap();

        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(&args);
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert_eq!(
                    err.error_code,
                    crate::core::task::TaskErrorCode::from(
                        crate::core::task::OperationalErrorCode::WorkerSerializationError,
                    ),
                );
                let data = err.data.expect("expected mismatch diagnostics");
                assert_eq!(data["expected_type"], std::any::type_name::<AddArgs>());
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[tokio::test]
    async fn no_args_task() {
        let task = async_task_fn!(heartbeat, ());
        let args = b"null".to_vec();
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Async { task: f, .. } => {
                let result = f.execute(&args).await.unwrap();
                let value: String = serde_json::from_slice(&result).unwrap();
                assert_eq!(value, "alive");
            }
            _ => panic!("expected async task"),
        }
    }

    // -- kwargs macro tests --

    #[tokio::test]
    async fn async_kwargs_macro_executes() {
        let task = async_task_fn_kwargs!(add, AddArgs);
        let args = serde_json::to_vec(&serde_json::json!({"a": 10, "b": 20})).unwrap();
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Async { task: f, .. } => {
                let result = f.execute(&args).await.unwrap();
                let value: i32 = serde_json::from_slice(&result).unwrap();
                assert_eq!(value, 30);
            }
            _ => panic!("expected async task"),
        }
    }

    #[test]
    fn blocking_kwargs_macro_executes() {
        let task = blocking_task_fn_kwargs!(multiply, AddArgs);
        let args = serde_json::to_vec(&serde_json::json!({"a": 6, "b": 7})).unwrap();
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(&args).unwrap();
                let value: i32 = serde_json::from_slice(&result).unwrap();
                assert_eq!(value, 42);
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[test]
    fn kwargs_macro_bad_input_returns_error() {
        let task = blocking_task_fn_kwargs!(multiply, AddArgs);
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(b"not json");
                assert!(result.is_err());
            }
            _ => panic!("expected blocking task"),
        }
    }

    // -- validate_input dry-run tests --

    #[test]
    fn validate_input_accepts_well_typed_kwargs() {
        let task = blocking_task_fn!(multiply, AddArgs);
        let args = serde_json::to_vec(&serde_json::json!({"a": 2, "b": 3})).unwrap();
        match &task {
            RegisteredTask::Blocking { task: f, .. } => {
                assert!(f.validate_input(&args).is_ok());
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[test]
    fn validate_input_rejects_type_mismatch_without_executing() {
        let task = blocking_task_fn!(multiply, AddArgs);
        let args = serde_json::to_vec(&serde_json::json!({"a": "oops", "b": 3})).unwrap();
        match &task {
            RegisteredTask::Blocking { task: f, .. } => {
                let err = f.validate_input(&args).unwrap_err();
                assert_eq!(
                    err.error_code,
                    crate::core::task::TaskErrorCode::from(
                        crate::core::task::OperationalErrorCode::WorkerSerializationError,
                    ),
                );
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[tokio::test]
    async fn validate_input_via_registered_task_delegates() {
        let task = async_task_fn!(add, AddArgs);
        let ok = serde_json::to_vec(&serde_json::json!({"a": 1, "b": 2})).unwrap();
        let bad = serde_json::to_vec(&serde_json::json!({"a": 1})).unwrap();
        assert!(task.validate_input(&ok).is_ok());
        assert!(task.validate_input(&bad).is_err());
    }

    #[test]
    fn kwargs_macro_missing_input_fields() {
        let task = blocking_task_fn_kwargs!(multiply, AddArgs);
        // An object without the required fields does not decode.
        let args = serde_json::to_vec(&serde_json::json!({})).unwrap();
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(&args);
                // AddArgs requires a and b, so an empty object should fail
                assert!(result.is_err());
            }
            _ => panic!("expected blocking task"),
        }
    }
}
