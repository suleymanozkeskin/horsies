use serde::{de::DeserializeOwned, Serialize};

/// Deserialize a task's declared input type from the worker envelope.
///
/// The envelope is the internal JSON shape produced by the worker:
/// `{ "args": [...], "kwargs": { ... } }`.
///
/// This helper keeps attribute-macro generated wrappers and the low-level
/// `async_task_fn!` / `blocking_task_fn!` macros on exactly the same
/// deserialization path.
///
/// Empty-envelope preference (C12): an input serializing to `{}` (empty map /
/// field-less struct) and a unit/`Option::None` input (serializing to `null`)
/// both reach decode as `{args:[], kwargs:{}}` — the envelope cannot distinguish
/// them. Decode prefers `{}` and falls back to `null`, so a type that accepts
/// `{}` wins first. Consequence: `Option::<Map>::None` decodes as
/// `Some(empty map)`, because `Option<Map>` deserializes `{}` as `Some({})`
/// before the `null` fallback is tried. This is an accepted trade of the erased
/// distinction, not an accident.
#[doc(hidden)]
pub fn decode_task_input<T>(args: &[u8]) -> Result<T, crate::core::task::TaskError>
where
    T: DeserializeOwned,
{
    let mut envelope: serde_json::Value = crate::core::codec::from_json_bytes(args).map_err(|e| {
        crate::core::task::TaskError::builtin(
            crate::core::task::OperationalErrorCode::WorkerSerializationError,
            format!("failed to parse args/kwargs envelope: {}", e),
        )
    })?;

    // Take ownership of the envelope's slots instead of deep-cloning them: the
    // envelope is discarded after selection, so `Value::take` (leaves Null
    // behind) avoids one full-payload copy per slot (P6).
    let kwargs_value = envelope
        .get_mut("kwargs")
        .map(serde_json::Value::take)
        .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
    let args_value = envelope
        .get_mut("args")
        .map(serde_json::Value::take)
        .unwrap_or(serde_json::Value::Null);

    let mut args_array = match args_value {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Null => Vec::new(),
        other => vec![other],
    };

    // Non-object kwargs (or an empty object) is treated as "no kwargs", so the
    // input comes from the positional args slot.
    let kwargs_nonempty = match &kwargs_value {
        serde_json::Value::Object(map) => !map.is_empty(),
        _ => false,
    };

    let candidate = match (kwargs_nonempty, args_array.len()) {
        // Kwargs object IS the input value.
        (true, 0) => kwargs_value,
        // Both positional args and kwargs present is an ambiguous, malformed
        // envelope. Fail loudly instead of silently dropping the positional
        // args (which the kwargs-first selection used to do).
        (true, _) => {
            return Err(crate::core::task::TaskError::builtin(
                crate::core::task::OperationalErrorCode::WorkerSerializationError,
                "args payload must be empty when kwargs is set; envelope carries both positional args and kwargs",
            ));
        }
        // Exactly one positional arg is the input value (owned, not cloned; P6).
        (false, 1) => args_array.swap_remove(0),
        // No positional args and an empty (or absent) kwargs object is
        // ambiguous: an input serializing to `{}` (empty map / field-less struct)
        // and a unit/`Option::None` input (serializing to `null`) both reach
        // decode as {args:[], kwargs:{}}. Prefer the empty object; the final
        // decode falls back to `null` for unit/Option types (C12).
        (false, 0) => serde_json::Value::Object(serde_json::Map::new()),
        (false, _) => {
            return Err(crate::core::task::TaskError::builtin(
                crate::core::task::OperationalErrorCode::WorkerSerializationError,
                "args payload must contain exactly one item when kwargs is empty",
            ));
        }
    };

    // Deserialize by reference (`&Value` implements `Deserializer`), so the
    // candidate survives for the C12 fallback and the error payload without the
    // unconditional deep clone the owned `from_value` required (P6).
    match T::deserialize(&candidate) {
        Ok(value) => Ok(value),
        Err(e) => {
            // Ambiguous empty envelope: a unit/`Option::None` input serializes to
            // `null` but reaches decode as the empty object `{}`. Retry as null so
            // those types still decode after the empty-object preference (C12).
            if matches!(&candidate, serde_json::Value::Object(map) if map.is_empty()) {
                if let Ok(value) = T::deserialize(&serde_json::Value::Null) {
                    return Ok(value);
                }
            }
            Err(crate::core::task::TaskError {
                error_code: Some(crate::core::task::TaskErrorCode::from(
                    crate::core::task::ContractCode::ArgumentTypeMismatch,
                )),
                message: Some(format!(
                    "task args do not match declared input type {}",
                    std::any::type_name::<T>(),
                )),
                cause: None,
                data: Some(serde_json::json!({
                    "expected_type": std::any::type_name::<T>(),
                    "actual_value": candidate,
                    "validation_error": e.to_string(),
                })),
            })
        }
    }
}

/// Serialize a task's success value and verify it round-trips through the
/// declared output type.
///
/// This mirrors Python's producer-side runtime return-contract validation:
/// the value must not only serialize, it must deserialize back into the same
/// declared `T`. If round-trip validation fails, return a structured
/// `RETURN_TYPE_MISMATCH` error instead of persisting a value that later
/// readers cannot hydrate through the declared type contract.
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

    if let Err(e) = crate::core::codec::from_json_bytes::<T>(&bytes) {
        let actual_value =
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
            });

        return crate::core::task::result::TaskResult::Err(crate::core::task::TaskError {
            error_code: Some(crate::core::task::TaskErrorCode::from(
                crate::core::task::ContractCode::ReturnTypeMismatch,
            )),
            message: Some(format!(
                "task returned a value that does not match its declared output type {}",
                std::any::type_name::<T>(),
            )),
            cause: None,
            data: Some(serde_json::json!({
                "expected_type": std::any::type_name::<T>(),
                "actual_value": actual_value,
                "validation_error": e.to_string(),
            })),
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
            meta: $crate::core::task::fn_trait::TaskMeta::for_input::<$args_type>(),
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
            meta: $crate::core::task::fn_trait::TaskMeta::for_input::<$args_type>(),
        }
    }};
}

/// Create an async [`RegisteredTask`] that deserializes from a kwargs envelope.
///
/// Expects the input JSON to contain a `"kwargs"` field whose value
/// deserializes into `$args_type`. This is used by workflow `args_from`
/// where dependency results are injected as keyword arguments.
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

/// Create a blocking [`RegisteredTask`] that deserializes from a kwargs envelope.
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
    fn decode_task_input_reads_kwargs_payload() {
        let envelope = serde_json::json!({"kwargs": {"a": 3, "b": 5}});
        let args = serde_json::to_vec(&envelope).unwrap();
        let parsed: AddArgs = decode_task_input(&args).unwrap();
        assert_eq!(parsed.a, 3);
        assert_eq!(parsed.b, 5);
    }

    #[test]
    fn decode_task_input_reads_single_positional_payload() {
        let envelope = serde_json::json!({"args": [{"a": 4, "b": 7}]});
        let args = serde_json::to_vec(&envelope).unwrap();
        let parsed: AddArgs = decode_task_input(&args).unwrap();
        assert_eq!(parsed.a, 4);
        assert_eq!(parsed.b, 7);
    }

    #[test]
    fn decode_task_input_reads_single_positional_array(/* C3 */) {
        // A single array-valued argument is carried as one positional element
        // (args=[[1,2,3]]) and must decode back to the array, not be read as
        // three separate positional args.
        let envelope = serde_json::json!({"args": [[1, 2, 3]], "kwargs": {}});
        let args = serde_json::to_vec(&envelope).unwrap();
        let parsed: Vec<i32> = decode_task_input(&args).unwrap();
        assert_eq!(parsed, vec![1, 2, 3]);
    }

    #[test]
    fn decode_task_input_rejects_both_args_and_kwargs(/* C14 */) {
        // An envelope carrying both positional args and kwargs is ambiguous;
        // decode must fail loudly rather than silently drop the positional args.
        let envelope = serde_json::json!({"args": [1], "kwargs": {"a": 1, "b": 2}});
        let args = serde_json::to_vec(&envelope).unwrap();
        let err: TaskError = decode_task_input::<AddArgs>(&args).unwrap_err();
        assert_eq!(
            err.error_code,
            Some(crate::core::task::TaskErrorCode::from(
                crate::core::task::OperationalErrorCode::WorkerSerializationError,
            )),
        );
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

        let envelope = serde_json::json!({"args": []});
        let args = serde_json::to_vec(&envelope).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = rt.block_on(task.execute(&args));

        let err = result.unwrap_err();
        assert_eq!(
            err.error_code,
            Some(crate::core::task::TaskErrorCode::from(
                crate::core::task::ContractCode::ReturnTypeMismatch,
            )),
        );
        let data = err.data.expect("expected mismatch diagnostics");
        assert_eq!(data["expected_type"], std::any::type_name::<BadRoundTrip>());
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
                Some(crate::core::task::TaskErrorCode::from(
                    crate::core::task::OperationalErrorCode::WorkerSerializationError,
                )),
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
        let envelope = serde_json::json!({"args": [AddArgs { a: 3, b: 5 }], "kwargs": {}});
        let args = serde_json::to_vec(&envelope).unwrap();
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
        let envelope = serde_json::json!({"args": [AddArgs { a: 4, b: 7 }], "kwargs": {}});
        let args = serde_json::to_vec(&envelope).unwrap();
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
                assert!(err
                    .message
                    .unwrap()
                    .contains("failed to parse args/kwargs envelope"));
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[test]
    fn typed_args_mismatch_returns_argument_type_mismatch() {
        let task = blocking_task_fn!(multiply, AddArgs);
        let envelope = serde_json::json!({"args": [{"a": "oops", "b": 7}], "kwargs": {}});
        let args = serde_json::to_vec(&envelope).unwrap();

        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(&args);
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert_eq!(
                    err.error_code,
                    Some(crate::core::task::TaskErrorCode::from(
                        crate::core::task::ContractCode::ArgumentTypeMismatch,
                    )),
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
        let envelope = serde_json::json!({"args": [], "kwargs": {}});
        let args = serde_json::to_vec(&envelope).unwrap();
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
        let envelope = serde_json::json!({"kwargs": {"a": 10, "b": 20}});
        let args = serde_json::to_vec(&envelope).unwrap();
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
        let envelope = serde_json::json!({"kwargs": {"a": 6, "b": 7}});
        let args = serde_json::to_vec(&envelope).unwrap();
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
    fn kwargs_macro_bad_envelope_returns_error() {
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
        let envelope = serde_json::json!({"args": [], "kwargs": {"a": 2, "b": 3}});
        let args = serde_json::to_vec(&envelope).unwrap();
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
        let envelope = serde_json::json!({"args": [], "kwargs": {"a": "oops", "b": 3}});
        let args = serde_json::to_vec(&envelope).unwrap();
        match &task {
            RegisteredTask::Blocking { task: f, .. } => {
                let err = f.validate_input(&args).unwrap_err();
                assert_eq!(
                    err.error_code,
                    Some(crate::core::task::TaskErrorCode::from(
                        crate::core::task::ContractCode::ArgumentTypeMismatch,
                    )),
                );
            }
            _ => panic!("expected blocking task"),
        }
    }

    #[tokio::test]
    async fn validate_input_via_registered_task_delegates() {
        let task = async_task_fn!(add, AddArgs);
        let ok = serde_json::to_vec(&serde_json::json!({"kwargs": {"a": 1, "b": 2}})).unwrap();
        let bad = serde_json::to_vec(&serde_json::json!({"kwargs": {"a": 1}})).unwrap();
        assert!(task.validate_input(&ok).is_ok());
        assert!(task.validate_input(&bad).is_err());
    }

    #[test]
    fn kwargs_macro_missing_kwargs_field() {
        let task = blocking_task_fn_kwargs!(multiply, AddArgs);
        // Envelope without "kwargs" — should try to deserialize empty object
        let args = serde_json::to_vec(&serde_json::json!({"args": []})).unwrap();
        match &task {
            crate::core::task::fn_trait::RegisteredTask::Blocking { task: f, .. } => {
                let result = f.execute(&args);
                // AddArgs requires a and b, so empty kwargs should fail
                assert!(result.is_err());
            }
            _ => panic!("expected blocking task"),
        }
    }
}
