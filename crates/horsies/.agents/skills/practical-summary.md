# Practical Summary

## Tasks

### 1. Define a task

```rust
#[horsies::task("add_numbers")]
async fn add_numbers(input: AddNumbersInput) -> Result<i32, TaskError> {
    Ok(input.a + input.b)
}
```

Tasks can also be multi-parameter:

```rust
#[horsies::task("notify_user")]
async fn notify_user(
    data: TaskResult<String>,
    urgent: bool,
) -> Result<(), TaskError> {
    let _ = data;
    let _ = urgent;
    Ok(())
}
```

### 2. Register once at startup

```rust
let add_task = add_numbers::register(&mut app)?;
```

Registration:
- registers the task with horsies
- returns a typed `TaskFunction<AddNumbersInput, i32>`
- stores a global bound handle for convenience dispatch

### 3. Use from any call site

**Global (primary path):**

```rust
match add_numbers::send(AddNumbersInput { a: 1, b: 2 }).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(5))).await;
        match result {
            TaskResult::Ok(value) => println!("result: {value}"),
            TaskResult::Err(err) => eprintln!("task failed or timed out: {:?}", err),
        }
    }
    Err(err) => {
        eprintln!("send failed: {}", err.message);
    }
}
```

**Delayed dispatch:**

```rust
match add_numbers::schedule(Duration::from_secs(30), AddNumbersInput { a: 3, b: 4 }).await {
    Ok(handle) => { /* ... */ }
    Err(err) => { /* ... */ }
}
```

**Idempotent and retained dispatch:**

```rust
let handle = add_numbers::with_options(
    TaskSendOptions::new()
        .idempotency_key("invoice:42")
        .retention_class("audit_7d"),
)
.send(AddNumbersInput { a: 3, b: 4 })
.await?;

let task_id: uuid::Uuid = handle.task_id();
```

Use `.retain_forever()` for a terminal record that must not be pruned.
`PostgresConfig::retain_rerun_input_default` decides whether eligible terminal
input is available for a later rerun.

**Explicit path (testing/advanced):**

```rust
let handle = add_numbers::handle(&rt)?;
match handle.send(AddNumbersInput { a: 5, b: 6 }).await {
    Ok(task_handle) => { /* ... */ }
    Err(err) => { /* ... */ }
}
```

**Task lifecycle:** define pure Rust function -> register once -> call `task_name::send(...)` or `task_name::schedule(...)` from anywhere.

---

## Reusable Workflows

### A. Zero-param reusable workflow

#### 1. Define a `WorkflowDefinition`

```rust
struct ETLPipeline;

impl WorkflowDefinition for ETLPipeline {
    type Output = String;
    type Params = ();

    fn name() -> &'static str { "etl_pipeline" }
    fn definition_key() -> &'static str { "myapp.etl_pipeline.v1" }

    fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
        // build fixed DAG with generated task_module::node()? helpers
    }
}
```

`definition_key()` is a **required** trait method (no default) — it is the
stable identity used for registration, cycle detection, and `check()`.

#### 2. Register once at startup

```rust
let wf = app.register_workflow_definition::<ETLPipeline>()?;
```

Registration:
- validates and builds the registered workflow
- returns `WorkflowFunction<String>`
- stores a global bound workflow handle

#### 3. Start from anywhere

**Global (primary path):**

```rust
match horsies::start_workflow::<ETLPipeline>().await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

**Explicit path (testing/advanced):**

```rust
match wf.start().await {
    Ok(handle) => { /* ... */ }
    Err(err) => { /* ... */ }
}
```

### B. Parameterized reusable workflow

#### 1. Define a `WorkflowDefinition` with typed params

```rust
struct ChildPipeline;

impl WorkflowDefinition for ChildPipeline {
    type Output = String;
    type Params = String;

    fn name() -> &'static str { "child_pipeline" }
    fn definition_key() -> &'static str { "myapp.child_pipeline.v1" }

    fn build_with(source_url: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
        // build DAG using source_url
    }
}
```

#### 2. Create the template once at startup

```rust
let child = app.workflow_template::<ChildPipeline>();
```

This:
- creates `WorkflowTemplate<String, String>`
- stores it globally for convenience dispatch

#### 3. Start from anywhere

**Global (primary path):**

```rust
match horsies::start_workflow_with::<ChildPipeline>(
    "https://example.com/data.json".to_owned()
).await {
    Ok(handle) => {
        let result = handle.get(Some(Duration::from_secs(60))).await;
    }
    Err(err) => {
        eprintln!("start failed: {}", err.message);
    }
}
```

**Explicit path (testing/advanced):**

```rust
match child.start("https://example.com/data.json".to_owned()).await {
    Ok(handle) => { /* ... */ }
    Err(err) => { /* ... */ }
}
```

---

## Dynamic Workflows

For workflows where the DAG is built at runtime and may vary by input.

### 1. Build a pure `WorkflowSpec`

```rust
fn build_child_spec(input: &ChildInput) -> Result<Option<WorkflowSpec>, HorsiesError> {
    // build variable DAG
}
```

### 2. Start depending on context

**From external code:**

```rust
if let Some(spec) = build_child_spec(&input)? {
    match app.start::<String>(spec).await {
        Ok(handle) => { /* ... */ }
        Err(err) => { /* ... */ }
    }
}
```

**From inside a running task:**

```rust
#[horsies::task("trigger_child")]
async fn trigger_child(rt: TaskRuntime, input: ChildInput) -> Result<(), TaskError> {
    if let Some(spec) = build_child_spec(&input)
        .map_err(|err| TaskError::new("BUILD_FAILED", err.to_string()))?
    {
        match rt.start::<String>(spec).await {
            Ok(handle) => {
                tracing::info!(workflow_id = %handle.workflow_id(), "started child workflow");
            }
            Err(err) => {
                tracing::warn!(error = %err.message, "failed to start child workflow");
            }
        }
    }
    Ok(())
}
```

**Dynamic workflow lifecycle:** build pure `WorkflowSpec` at runtime -> start with `app.start(...)` externally or `rt.start(...)` inside tasks. `app.start(...)` internally registers and starts the spec in one step.

For workflow input binding:
- `node().set_input(value)?` for whole explicit input
- `node().set(task_name::params::field(), value)?` for one explicit parameter
- `node().arg_from(task_name::params::field(), dep)` for upstream `TaskResult<_>` injection
- prefer multi-parameter receiving tasks plus `task_name::params::*`
- use `#[derive(WorkflowInput)]` only when you intentionally want a named receiving struct

---

## Check-Time Validation for Dynamic Builders

For runtime-built workflow builders that need representative-case validation during `app.check()`:

```rust
let mut registration = app.check_workflow_builder(
    "build_child_workflow_cases",
    |source_url: &String| build_child_workflow(source_url),
)?;

registration.case("https://example.com/a.json".to_owned());
registration.case("https://example.com/b.json".to_owned());
registration.register()?;

app.check()?;
```

---

## Alpha.26 operations

- `horsies_tasks` contains only live tasks.
- Terminal tasks move to `horsies_task_history` atomically.
- Task result and info APIs read both locations.
- Task and workflow IDs are `uuid::Uuid`.
- `AppConfig.retention` owns task-history classes and partition maintenance.
- `RecoveryConfig` owns stale-work recovery and phase-2 quarantine.
- `WorkflowStatus::Expired` is terminal.
- A paused workflow can expire after
  `retention.paused_workflow_auto_cancel_after`.
- Pause can move a claimed backing task to history. Resume creates a new task.
- Regular workflow nodes stamp `started_at` on the first RUNNING transition.
- A node reset to READY clears `started_at`.

Rerun creates a fresh lineage-bearing task:

```rust
let outcome = horsies::rerun_task(
    &broker,
    RerunTask::new(source_task_id, None, caller_key),
    RerunEnqueuePolicy::standard(true),
)
.await?;
```

Match every `RerunOutcome` variant.

An existing migration-0032 database needs the offline cutover. Stop workers
and schedulers. Take a named backup. Follow the
[cutover runbook](https://suleymanozkeskin.github.io/horsies-rust/operations/cutover-runbook/).

---

## Mental Model

- Definitions stay explicit and typed
- Registration binds tasks/workflows to the runtime
- After registration, global convenience APIs are available from anywhere

| What | Define | Register | Use from anywhere |
|------|--------|----------|-------------------|
| Tasks | `#[task]` | `task_name::register(&mut app)?` | `task_name::send(args).await` |
| Zero-param workflows | `impl WorkflowDefinition<Params = ()>` | `app.register_workflow_definition::<D>()?` | `horsies::start_workflow::<D>().await` |
| Parameterized workflows | `impl WorkflowDefinition<Params = P>` | `app.workflow_template::<D>()` | `horsies::start_workflow_with::<D>(params).await` |
| Dynamic workflows | build `WorkflowSpec` | no pre-registration; `app.start` registers and starts | `app.start(spec)` or `rt.start(spec)` |

## See Also

- **Configuration:** retention, recovery, scheduling, queues, and PgBouncer live in `configs.md`.
- **Tasks:** send options, idempotency, history reads, and rerun live in `tasks.md`.
- **Workflows:** node wiring, `EXPIRED`, pause behavior, and node timing live in `workflows.md`.
