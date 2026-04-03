//! Shared task definitions for all examples.
//!
//! Each submodule groups the task functions, domain types, and a `register()`
//! helper that registers them on a `Horsies` app.

// ---------------------------------------------------------------------------
// basic tasks — used by basic_tasks.rs and worker_default
// ---------------------------------------------------------------------------
pub mod basic {
    use horsies::{async_task_fn, Horsies, TaskError};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ComputeInput {
        pub a: f64,
        pub b: f64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Computed {
        pub value: f64,
        pub time_taken_ms: f64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct FailInput {
        pub should_fail: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DivInput {
        pub a: f64,
        pub b: f64,
    }

    /// Adds a + b; returns error if sum exceeds 1000.
    pub async fn do_compute(input: ComputeInput) -> Result<Computed, TaskError> {
        let total = input.a + input.b;
        if total > 1000.0 {
            return Err(TaskError::user(
                "TOO_LARGE",
                format!("Sum {} exceeds 1000", total),
            ));
        }
        Ok(Computed {
            value: total,
            time_taken_ms: 0.5,
        })
    }

    /// A task that fails when told to.
    pub async fn failing_task(input: FailInput) -> Result<String, TaskError> {
        if input.should_fail {
            return Err(TaskError::user("FORCED_FAILURE", "told to fail"));
        }
        Ok("succeeded".to_string())
    }

    /// Division task - returns error for division by zero.
    pub async fn divide(input: DivInput) -> Result<f64, TaskError> {
        if input.b == 0.0 {
            return Err(TaskError::user("DIVISION_ERROR", "cannot divide by zero"));
        }
        Ok(input.a / input.b)
    }

    /// Simple no-arg task.
    pub async fn ping(_: ()) -> Result<String, TaskError> {
        Ok("pong".to_string())
    }

    /// Register all basic tasks on the given app.
    pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
        app.register("do_compute", async_task_fn!(do_compute, ComputeInput))?;
        app.register("failing_task", async_task_fn!(failing_task, FailInput))?;
        app.register("divide", async_task_fn!(divide, DivInput))?;
        app.register("ping", async_task_fn!(ping, ()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// retry tasks — used by retries.rs and worker_default
// ---------------------------------------------------------------------------
pub mod retries {
    use horsies::{async_task_fn, Horsies, TaskError};

    /// Always fails with UNHANDLED_EXCEPTION.
    pub async fn always_fails(_: ()) -> Result<String, TaskError> {
        eprintln!("  [always_fails] executing (will fail)");
        Err(TaskError::user(
            "UNHANDLED_EXCEPTION",
            "intentional failure for retry demo",
        ))
    }

    /// Fails with a custom VALUE_ERROR code.
    pub async fn fails_with_custom_code(_: ()) -> Result<String, TaskError> {
        eprintln!("  [fails_with_custom_code] executing (will fail)");
        Err(TaskError::user("VALUE_ERROR", "bad value for retry demo"))
    }

    /// Always succeeds.
    pub async fn eventually_succeeds(_: ()) -> Result<String, TaskError> {
        eprintln!("  [eventually_succeeds] executing (will succeed)");
        Ok("success after retries".to_string())
    }

    /// Register all retry tasks on the given app.
    pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
        app.register("always_fails", async_task_fn!(always_fails, ()))?;
        app.register(
            "fails_with_custom_code",
            async_task_fn!(fails_with_custom_code, ()),
        )?;
        app.register(
            "eventually_succeeds",
            async_task_fn!(eventually_succeeds, ()),
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// custom queue tasks — used by custom_queues.rs and worker_custom
// ---------------------------------------------------------------------------
pub mod custom_queues {
    use std::time::Duration;

    use horsies::{async_task_fn, Horsies, TaskError};
    use serde::{Deserialize, Serialize};

    use crate::common::custom_mode::{HIGH, LOW, NORMAL};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ComputeInput {
        pub a: f64,
        pub b: f64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Computed {
        pub value: f64,
        pub queue: String,
    }

    pub async fn high_priority_compute(input: ComputeInput) -> Result<Computed, TaskError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Computed {
            value: input.a + input.b,
            queue: "high".to_string(),
        })
    }

    pub async fn normal_priority_compute(input: ComputeInput) -> Result<Computed, TaskError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Computed {
            value: input.a * input.b,
            queue: "normal".to_string(),
        })
    }

    pub async fn low_priority_compute(input: ComputeInput) -> Result<Computed, TaskError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Computed {
            value: (input.a + input.b) / 2.0,
            queue: "low".to_string(),
        })
    }

    /// Register all custom-queue tasks on the given app.
    pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
        app.register_with_queue(
            "high_compute",
            async_task_fn!(high_priority_compute, ComputeInput),
            HIGH,
        )?;
        app.register_with_queue(
            "normal_compute",
            async_task_fn!(normal_priority_compute, ComputeInput),
            NORMAL,
        )?;
        app.register_with_queue(
            "low_compute",
            async_task_fn!(low_priority_compute, ComputeInput),
            LOW,
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// workflow tasks — used by workflow_patterns.rs and worker_default
// ---------------------------------------------------------------------------
pub mod workflows {
    use horsies::{
        async_task_fn, Horsies, OnError, TaskError, TaskFunction, TaskResult, WorkflowSpecBuilder,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct FetchResult {
        pub source: String,
        pub items: Vec<String>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TransformArgs {
        pub input_result: TaskResult<FetchResult>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TransformResult {
        pub processed_count: usize,
        pub data: Vec<String>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct ChunkArgs {
        pub input_result: TaskResult<FetchResult>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct AggregateArgs {
        pub chunk_a: TaskResult<i32>,
        pub chunk_b: TaskResult<i32>,
        pub chunk_c: TaskResult<i32>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct AggregateResult {
        pub total: i32,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct RecoveryArgs {
        pub input_result: TaskResult<FetchResult>,
    }

    /// Root task: takes a simple String arg (the source name), returns FetchResult.
    pub async fn fetch_data(source: String) -> Result<FetchResult, TaskError> {
        Ok(FetchResult {
            source,
            items: vec!["item1".into(), "item2".into(), "item3".into()],
        })
    }

    /// Downstream task: receives upstream FetchResult via args_from as a kwarg.
    pub async fn transform_data(args: TransformArgs) -> Result<TransformResult, TaskError> {
        match args.input_result {
            TaskResult::Ok(data) => Ok(TransformResult {
                processed_count: data.items.len(),
                data: data.items.into_iter().map(|s| s.to_uppercase()).collect(),
            }),
            TaskResult::Err(e) => Err(e),
        }
    }

    /// Fan-out chunk processor: receives FetchResult, returns item count.
    pub async fn process_chunk(args: ChunkArgs) -> Result<i32, TaskError> {
        match args.input_result {
            TaskResult::Ok(data) => Ok(data.items.len() as i32),
            TaskResult::Err(e) => Err(e),
        }
    }

    /// Fan-in aggregator: receives three chunk results, sums them.
    pub async fn aggregate(args: AggregateArgs) -> Result<AggregateResult, TaskError> {
        let mut total = 0;
        if let TaskResult::Ok(v) = args.chunk_a {
            total += v;
        }
        if let TaskResult::Ok(v) = args.chunk_b {
            total += v;
        }
        if let TaskResult::Ok(v) = args.chunk_c {
            total += v;
        }
        Ok(AggregateResult { total })
    }

    /// Always-failing task for the error recovery demo.
    pub async fn failing_fetch(_source: String) -> Result<FetchResult, TaskError> {
        Err(TaskError::user("FETCH_FAILED", "intentional failure"))
    }

    /// Recovery task: handles upstream failures gracefully.
    pub async fn recovery_task(args: RecoveryArgs) -> Result<String, TaskError> {
        match args.input_result {
            TaskResult::Err(_) => Ok("recovered_with_default".to_string()),
            TaskResult::Ok(_) => Ok("no_recovery_needed".to_string()),
        }
    }

    pub struct WorkflowTasks {
        pub fetch_data: TaskFunction<String, FetchResult>,
        pub transform_data: TaskFunction<TransformArgs, TransformResult>,
        pub process_chunk: TaskFunction<ChunkArgs, i32>,
        pub aggregate: TaskFunction<AggregateArgs, AggregateResult>,
        pub failing_fetch: TaskFunction<String, FetchResult>,
        pub recovery_task: TaskFunction<RecoveryArgs, String>,
    }

    /// Register all workflow task functions on the given app.
    pub fn register(app: &mut Horsies) -> Result<WorkflowTasks, Box<dyn std::error::Error>> {
        Ok(WorkflowTasks {
            fetch_data: app
                .task::<String, FetchResult>("fetch_data", async_task_fn!(fetch_data, String))?
                .register()?,
            transform_data: app
                .task::<TransformArgs, TransformResult>(
                    "transform_data",
                    async_task_fn!(transform_data, TransformArgs),
                )?
                .register()?,
            process_chunk: app
                .task::<ChunkArgs, i32>("process_chunk", async_task_fn!(process_chunk, ChunkArgs))?
                .register()?,
            aggregate: app
                .task::<AggregateArgs, AggregateResult>(
                    "aggregate",
                    async_task_fn!(aggregate, AggregateArgs),
                )?
                .register()?,
            failing_fetch: app
                .task::<String, FetchResult>(
                    "failing_fetch",
                    async_task_fn!(failing_fetch, String),
                )?
                .register()?,
            recovery_task: app
                .task::<RecoveryArgs, String>(
                    "recovery_task",
                    async_task_fn!(recovery_task, RecoveryArgs),
                )?
                .register()?,
        })
    }

    /// Register the three workflow specs on the given app.
    pub fn register_workflow_specs(
        app: &mut Horsies,
        tasks: &WorkflowTasks,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Pattern 1: Linear Chain (fetch -> transform)
        {
            let mut b = WorkflowSpecBuilder::new("linear_chain");
            let fetch = b.task(tasks.fetch_data.node_with("source_a".to_owned())?);
            let transform = b.task(
                tasks
                    .transform_data
                    .node()
                    .waits_for(fetch)
                    .args_from("input_result", fetch),
            );
            b.on_error(OnError::Fail);
            b.output(transform);
            app.register_workflow_spec::<TransformResult>(b.build()?)?;
        }

        // Pattern 2: Fan-Out + Fan-In
        {
            let mut b = WorkflowSpecBuilder::new("fan_in_out");
            let fetch = b.task(tasks.fetch_data.node_with("source_b".to_owned())?);
            let ca = b.task(
                tasks
                    .process_chunk
                    .node()
                    .waits_for(fetch)
                    .args_from("input_result", fetch),
            );
            let cb = b.task(
                tasks
                    .process_chunk
                    .node()
                    .waits_for(fetch)
                    .args_from("input_result", fetch),
            );
            let cc = b.task(
                tasks
                    .process_chunk
                    .node()
                    .waits_for(fetch)
                    .args_from("input_result", fetch),
            );
            let agg = b.task(
                tasks
                    .aggregate
                    .node()
                    .waits_for_all(&[ca, cb, cc])
                    .args_from("chunk_a", ca)
                    .args_from("chunk_b", cb)
                    .args_from("chunk_c", cc),
            );
            b.on_error(OnError::Fail);
            b.output(agg);
            app.register_workflow_spec::<AggregateResult>(b.build()?)?;
        }

        // Pattern 3: Error Recovery
        {
            let mut b = WorkflowSpecBuilder::new("error_recovery");
            let fail = b.task(tasks.failing_fetch.node_with("will_fail".to_owned())?);
            let recover = b.task(
                tasks
                    .recovery_task
                    .node()
                    .waits_for(fail)
                    .args_from("input_result", fail)
                    .allow_failed_deps(true),
            );
            b.on_error(OnError::Fail);
            b.output(recover);
            app.register_workflow_spec::<String>(b.build()?)?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// schedule tasks — used by schedules.rs
// ---------------------------------------------------------------------------
pub mod schedules {
    use horsies::{async_task_fn, Horsies, TaskError};
    use serde::{Deserialize, Serialize};

    /// Empty argument type for scheduled tasks that need no input.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct NoArgs {}

    /// Simulates an inventory sync.
    pub async fn sync_inventory(_: NoArgs) -> Result<serde_json::Value, TaskError> {
        let now = chrono::Utc::now().format("%H:%M:%S");
        eprintln!("  [sync_inventory] executed at {}", now);
        Ok(serde_json::json!({
            "synced_count": 42,
            "timestamp": now.to_string(),
        }))
    }

    /// Simulates a daily report generation.
    pub async fn generate_report(_: NoArgs) -> Result<serde_json::Value, TaskError> {
        let now = chrono::Utc::now().format("%H:%M:%S");
        eprintln!("  [generate_report] executed at {}", now);
        Ok(serde_json::json!({
            "report": "daily_summary",
            "rows": 100,
            "timestamp": now.to_string(),
        }))
    }

    /// Simulates a weekly data cleanup.
    pub async fn cleanup_data(_: NoArgs) -> Result<serde_json::Value, TaskError> {
        let now = chrono::Utc::now().format("%H:%M:%S");
        eprintln!("  [cleanup_data] executed at {}", now);
        Ok(serde_json::json!({
            "cleaned": 50,
            "timestamp": now.to_string(),
        }))
    }

    /// Register all schedule tasks on the given app.
    pub fn register(app: &mut Horsies) -> Result<(), Box<dyn std::error::Error>> {
        app.register("sync_inventory", async_task_fn!(sync_inventory, NoArgs))?;
        app.register("generate_report", async_task_fn!(generate_report, NoArgs))?;
        app.register("cleanup_data", async_task_fn!(cleanup_data, NoArgs))?;
        Ok(())
    }
}
