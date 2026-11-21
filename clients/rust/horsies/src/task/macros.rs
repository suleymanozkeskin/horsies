//! `async_task_fn!` and `blocking_task_fn!`: a `RegisteredTask` from a task
//! function, for registration by hand with `Horsies::register`.
//!
//! The function takes one input value and returns
//! `Result<T, TaskError<E>>`. The registered task decodes the input with
//! `decode_input`, runs the function, and stores what it returned with
//! `body_result`: the same adapters as the `#[horsies::task]` attribute.
//! The task records `module_path!()` of the call site, the `module` of the
//! exception record of a panic, and declares the codes of `E`
//! (`UserErrorCode::CODES`), which the registration checks. Attach the
//! options (queue, retry, timeout, execution class) with
//! `RegisteredTask::with_task_options`.

/// A `RegisteredTask` that runs the async function `$function` with an input
/// of type `$input`.
///
/// ```ignore
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct AddInput { a: i64, b: i64 }
///
/// async fn add(input: AddInput) -> Result<i64, horsies::TaskError<std::convert::Infallible>> {
///     Ok(input.a + input.b)
/// }
///
/// app.register("add", horsies::async_task_fn!(add, AddInput))?;
/// ```
#[macro_export]
macro_rules! async_task_fn {
    ($function:path, $input:ty) => {{
        struct __HorsiesAsyncBody;

        impl $crate::__private::AsyncTaskFn for __HorsiesAsyncBody {
            fn execute(
                &self,
                input: &[u8],
            ) -> ::std::pin::Pin<
                ::std::boxed::Box<
                    dyn ::std::future::Future<Output = $crate::__private::RawTaskResult>
                        + ::std::marker::Send
                        + '_,
                >,
            > {
                let input = input.to_vec();
                ::std::boxed::Box::pin(async move {
                    match $crate::__private::decode_input::<$input>(&input) {
                        ::std::result::Result::Ok(decoded) => {
                            $crate::__private::body_result($function(decoded).await)
                        }
                        ::std::result::Result::Err(refused) => {
                            $crate::__private::TaskResult::Err(refused)
                        }
                    }
                })
            }

            fn validate_input(
                &self,
                input: &[u8],
            ) -> ::std::result::Result<(), $crate::__private::StoredTaskError> {
                $crate::__private::check_input::<$input>(input)
            }
        }

        $crate::__private::RegisteredTask::Async {
            task: ::std::sync::Arc::new(__HorsiesAsyncBody),
            meta: $crate::__private::TaskMeta::for_input::<$input>()
                .in_module(::std::module_path!())
                .with_error_codes($crate::__private::async_task_codes::<$input, _, _, _, _>(
                    &$function,
                )),
        }
    }};
}

/// A `RegisteredTask` that runs the blocking function `$function` with an
/// input of type `$input`, on a thread of its child.
///
/// ```ignore
/// fn resize(input: ResizeInput) -> Result<String, horsies::TaskError<ImageCode>> {
///     Ok(format!("{} at {}px", input.path, input.width))
/// }
///
/// app.register("resize", horsies::blocking_task_fn!(resize, ResizeInput))?;
/// ```
#[macro_export]
macro_rules! blocking_task_fn {
    ($function:path, $input:ty) => {{
        struct __HorsiesBlockingBody;

        impl $crate::__private::BlockingTaskFn for __HorsiesBlockingBody {
            fn execute(&self, input: &[u8]) -> $crate::__private::RawTaskResult {
                match $crate::__private::decode_input::<$input>(input) {
                    ::std::result::Result::Ok(decoded) => {
                        $crate::__private::body_result($function(decoded))
                    }
                    ::std::result::Result::Err(refused) => {
                        $crate::__private::TaskResult::Err(refused)
                    }
                }
            }

            fn validate_input(
                &self,
                input: &[u8],
            ) -> ::std::result::Result<(), $crate::__private::StoredTaskError> {
                $crate::__private::check_input::<$input>(input)
            }
        }

        $crate::__private::RegisteredTask::Blocking {
            task: ::std::sync::Arc::new(__HorsiesBlockingBody),
            meta: $crate::__private::TaskMeta::for_input::<$input>()
                .in_module(::std::module_path!())
                .with_error_codes($crate::__private::blocking_task_codes::<$input, _, _, _>(
                    &$function,
                )),
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use horsies_core::core::task::fn_trait::TaskModule;
    use horsies_core::{OperationalErrorCode, RegisteredTask, TaskErrorCode, TaskResult};
    use serde::{Deserialize, Serialize};

    use crate::TaskError;

    #[derive(Debug, Serialize, Deserialize)]
    struct AddInput {
        a: i64,
        b: i64,
    }

    async fn add(input: AddInput) -> Result<i64, TaskError<Infallible>> {
        Ok(input.a + input.b)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn multiply(input: AddInput) -> Result<i64, TaskError<Infallible>> {
        Ok(input.a * input.b)
    }

    fn input(a: i64, b: i64) -> Vec<u8> {
        serde_json::to_vec(&AddInput { a, b }).unwrap()
    }

    fn worker_serialization_error() -> TaskErrorCode {
        TaskErrorCode::from(OperationalErrorCode::WorkerSerializationError)
    }

    #[tokio::test]
    async fn an_async_task_decodes_runs_and_encodes() {
        let RegisteredTask::Async { task, meta } = crate::async_task_fn!(add, AddInput) else {
            panic!("async_task_fn! built a blocking task");
        };
        match task.execute(&input(2, 3)).await {
            TaskResult::Ok(bytes) => assert_eq!(bytes, b"5"),
            TaskResult::Err(error) => panic!("the task failed: {error}"),
        }
        assert!(meta.expects_input);
        assert_eq!(meta.module, TaskModule::Path(module_path!()));
    }

    #[test]
    fn a_blocking_task_decodes_runs_and_encodes() {
        let RegisteredTask::Blocking { task, .. } = crate::blocking_task_fn!(multiply, AddInput)
        else {
            panic!("blocking_task_fn! built an async task");
        };
        match task.execute(&input(2, 3)) {
            TaskResult::Ok(bytes) => assert_eq!(bytes, b"6"),
            TaskResult::Err(error) => panic!("the task failed: {error}"),
        }
    }

    #[tokio::test]
    async fn an_input_of_another_type_ends_before_the_body() {
        let task = crate::async_task_fn!(add, AddInput);
        let refused = task.validate_input(br#"{"a": "two"}"#).unwrap_err();
        assert_eq!(refused.error_code, worker_serialization_error());
        let RegisteredTask::Async { task, .. } = task else {
            panic!("async_task_fn! built a blocking task");
        };
        match task.execute(br#"{"a": 1}"#).await {
            TaskResult::Ok(_) => panic!("the body ran with an input of another type"),
            TaskResult::Err(error) => assert_eq!(error.error_code, worker_serialization_error()),
        }
    }

    #[test]
    fn validate_input_accepts_an_input_of_the_type() {
        let task = crate::blocking_task_fn!(multiply, AddInput);
        assert!(task.validate_input(&input(1, 1)).is_ok());
    }

    #[derive(Debug, Clone, PartialEq)]
    enum MathCode {
        Overflow,
        DivideByZero,
    }

    impl crate::UserErrorCode for MathCode {
        const CODES: &'static [&'static str] = &["OVERFLOW", "DIVIDE_BY_ZERO"];

        fn code(&self) -> &str {
            match self {
                Self::Overflow => "OVERFLOW",
                Self::DivideByZero => "DIVIDE_BY_ZERO",
            }
        }

        fn from_code(text: &str) -> Option<Self> {
            match text {
                "OVERFLOW" => Some(Self::Overflow),
                "DIVIDE_BY_ZERO" => Some(Self::DivideByZero),
                _ => None,
            }
        }
    }

    async fn checked_add(input: AddInput) -> Result<i64, TaskError<MathCode>> {
        input
            .a
            .checked_add(input.b)
            .ok_or_else(|| TaskError::user(MathCode::Overflow, "the sum overflows"))
    }

    #[allow(clippy::needless_pass_by_value)]
    fn checked_divide(input: AddInput) -> Result<i64, TaskError<MathCode>> {
        input
            .a
            .checked_div(input.b)
            .ok_or_else(|| TaskError::user(MathCode::DivideByZero, "the divisor is 0"))
    }

    #[test]
    fn each_macro_declares_the_codes_of_the_code_type() {
        let async_task = crate::async_task_fn!(checked_add, AddInput);
        assert_eq!(async_task.error_codes(), ["OVERFLOW", "DIVIDE_BY_ZERO"]);
        let blocking_task = crate::blocking_task_fn!(checked_divide, AddInput);
        assert_eq!(blocking_task.error_codes(), ["OVERFLOW", "DIVIDE_BY_ZERO"]);
    }

    #[test]
    fn a_task_whose_code_type_has_no_code_declares_none() {
        assert!(crate::async_task_fn!(add, AddInput)
            .error_codes()
            .is_empty());
        assert!(crate::blocking_task_fn!(multiply, AddInput)
            .error_codes()
            .is_empty());
    }
}
