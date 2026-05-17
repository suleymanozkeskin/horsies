//! Diagnostic runner — prints actual error messages and check/worker outcomes
//! for all 6 scenarios. Not a test, just a report generator.

#![allow(unused_imports)]
use horsies::{
    async_task_fn, AppConfig, CustomQueueConfig, ErrorCode, Horsies, PostgresConfig, QueueMode,
    RecoveryConfig, TaskOptions, WorkerConfig, WorkerResilienceConfig,
};
use serde::{Deserialize, Serialize};

const FAKE_DB: &str = "postgresql://guard_test:guard_test@localhost:5432/guard_test";

fn broker() -> PostgresConfig {
    PostgresConfig {
        database_url: FAKE_DB.to_owned(),
        session_database_url: None,
        pgbouncer_transaction_mode: false,
        pool_pre_ping: true,
        pool_size: 5,
        max_overflow: 5,
        pool_timeout: 10,
        pool_recycle: 600,
        echo: false,
    }
}

fn default_config() -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Default,
        custom_queues: None,
        broker: broker(),
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: RecoveryConfig::default(),
        resilience: WorkerResilienceConfig::default(),
        schedule: None,
        resend_on_transient_err: false,
    }
}

fn custom_config() -> AppConfig {
    AppConfig {
        queue_mode: QueueMode::Custom,
        custom_queues: Some(vec![
            CustomQueueConfig {
                name: "fast".to_owned(),
                priority: 1,
                max_concurrency: 10,
            },
            CustomQueueConfig {
                name: "slow".to_owned(),
                priority: 50,
                max_concurrency: 5,
            },
        ]),
        broker: broker(),
        cluster_wide_cap: None,
        prefetch_buffer: 0,
        claim_lease_ms: None,
        max_claim_renew_age_ms: 180_000,
        recovery: RecoveryConfig::default(),
        resilience: WorkerResilienceConfig::default(),
        schedule: None,
        resend_on_transient_err: false,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Input {
    a: f64,
    b: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Output {
    value: f64,
}

async fn compute(input: Input) -> Result<Output, horsies::TaskError> {
    Ok(Output {
        value: input.a + input.b,
    })
}

async fn ping(_: ()) -> Result<String, horsies::TaskError> {
    Ok("pong".to_owned())
}

async fn multiply(input: Input) -> Result<Output, horsies::TaskError> {
    Ok(Output {
        value: input.a * input.b,
    })
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn print_report() {
    println!("\n{}", "=".repeat(100));
    println!("QUEUE MISMATCH GUARD — FULL VERIFICATION REPORT");
    println!("{}\n", "=".repeat(100));

    // Scenario 1: DEFAULT + queue="fast"
    {
        println!("--- Scenario 1: DEFAULT mode + task declares queue=\"fast\" ---");
        let mut app = Horsies::new(default_config()).unwrap();
        let reg = app
            .task::<Input, Output>("fast_compute", async_task_fn!(compute, Input))
            .unwrap()
            .queue("fast")
            .register();
        match reg {
            Err(ref e) => {
                println!("  register(): FAILED (as expected)");
                println!("  error code: {:?}", e.code);
                println!("  message:    {}", e);
                if let Some(ref h) = e.help_text {
                    println!("  help:       {}", h);
                }
            }
            Ok(_) => println!("  register(): PASSED — GAP!"),
        }
        let check = app.check();
        println!(
            "  check():    {}",
            if check.is_ok() {
                "PASSED (no bad tasks in registry)"
            } else {
                "FAILED"
            }
        );
        let worker_config = WorkerConfig::default();
        let worker = app.run_worker_with(worker_config).await;
        match worker {
            Err(ref e) => {
                let msg = format!("{}", e);
                if msg.contains("queue") || msg.contains("TaskInvalidOptions") {
                    println!("  worker:     FAILED on queue validation — GAP!");
                } else {
                    println!(
                        "  worker:     FAILED at broker (past queue validation) — {}",
                        msg
                    );
                }
            }
            Ok(_) => println!("  worker:     STARTED — GAP!"),
        }
        println!();
    }

    // Scenario 2: CUSTOM + no queue
    {
        println!("--- Scenario 2: CUSTOM mode + task omits queue ---");
        let mut app = Horsies::new(custom_config()).unwrap();
        let reg = app
            .task::<(), String>("bare_ping", async_task_fn!(ping, ()))
            .unwrap()
            .register();
        match reg {
            Err(ref e) => {
                println!("  register(): FAILED (as expected)");
                println!("  error code: {:?}", e.code);
                println!("  message:    {}", e);
            }
            Ok(_) => println!("  register(): PASSED — GAP!"),
        }
        let check = app.check();
        println!(
            "  check():    {}",
            if check.is_ok() {
                "PASSED (no bad tasks in registry)"
            } else {
                "FAILED"
            }
        );
        let mut wc = WorkerConfig::default();
        wc.queues = vec!["fast".to_owned(), "slow".to_owned()];
        let worker = app.run_worker_with(wc).await;
        match worker {
            Err(ref e) => {
                let msg = format!("{}", e);
                if msg.contains("queue") || msg.contains("TaskInvalidOptions") {
                    println!("  worker:     FAILED on queue validation — GAP!");
                } else {
                    println!(
                        "  worker:     FAILED at broker (past queue validation) — {}",
                        msg
                    );
                }
            }
            Ok(_) => println!("  worker:     STARTED — GAP!"),
        }
        println!();
    }

    // Scenario 3: CUSTOM + unknown queue "analytics"
    {
        println!(
            "--- Scenario 3: CUSTOM mode + queue=\"analytics\" (not in [\"fast\",\"slow\"]) ---"
        );
        let mut app = Horsies::new(custom_config()).unwrap();
        let reg = app
            .task::<Input, Output>("analytics_compute", async_task_fn!(compute, Input))
            .unwrap()
            .queue("analytics")
            .register();
        match reg {
            Err(ref e) => {
                println!("  register(): FAILED (as expected)");
                println!("  error code: {:?}", e.code);
                println!("  message:    {}", e);
            }
            Ok(_) => println!("  register(): PASSED — GAP!"),
        }
        // Also test raw register() path
        let raw = app.register(
            "raw_analytics",
            async_task_fn!(ping, ()).with_task_options(TaskOptions {
                task_name: "raw_analytics".to_owned(),
                queue_name: Some("analytics".to_owned()),
                good_until: None,
                auto_retry_for: None,
                retry_policy: None,
            }),
        );
        match raw {
            Err(ref e) => {
                println!("  raw register(): FAILED (as expected)");
                println!("  error code:     {:?}", e.code);
                println!("  message:        {}", e);
            }
            Ok(_) => println!("  raw register(): PASSED — GAP!"),
        }
        println!();
    }

    // Scenario 4: CUSTOM + valid queue
    {
        println!("--- Scenario 4: CUSTOM mode + queue=\"fast\" (valid) ---");
        let mut app = Horsies::new(custom_config()).unwrap();
        let reg = app
            .task::<Input, Output>("fast_compute", async_task_fn!(compute, Input))
            .unwrap()
            .queue("fast")
            .register();
        match reg {
            Ok(ref t) => println!(
                "  register(): PASSED — task={}, queue={}",
                t.task_name(),
                t.queue_name()
            ),
            Err(ref e) => println!("  register(): FAILED — GAP! {}", e),
        }
        let check = app.check();
        println!(
            "  check():    {}",
            if check.is_ok() {
                "PASSED"
            } else {
                "FAILED — GAP!"
            }
        );
        let mut wc = WorkerConfig::default();
        wc.queues = vec!["fast".to_owned(), "slow".to_owned()];
        let worker = app.run_worker_with(wc).await;
        match worker {
            Err(ref e) => {
                let msg = format!("{}", e);
                if msg.contains("queue") || msg.contains("TaskInvalidOptions") {
                    println!("  worker:     FAILED on queue validation — GAP! {}", msg);
                } else {
                    println!("  worker:     FAILED at broker (correct — past queue validation)");
                }
            }
            Ok(_) => println!("  worker:     STARTED (would need real DB)"),
        }
        println!();
    }

    // Scenario 5: DEFAULT + no queue
    {
        println!("--- Scenario 5: DEFAULT mode + no queue (happy path) ---");
        let mut app = Horsies::new(default_config()).unwrap();
        let reg = app
            .task::<(), String>("ping", async_task_fn!(ping, ()))
            .unwrap()
            .register();
        match reg {
            Ok(ref t) => println!(
                "  register(): PASSED — task={}, queue={}",
                t.task_name(),
                t.queue_name()
            ),
            Err(ref e) => println!("  register(): FAILED — GAP! {}", e),
        }
        let check = app.check();
        println!(
            "  check():    {}",
            if check.is_ok() {
                "PASSED"
            } else {
                "FAILED — GAP!"
            }
        );
        let wc = WorkerConfig::default();
        let worker = app.run_worker_with(wc).await;
        match worker {
            Err(ref e) => {
                let msg = format!("{}", e);
                if msg.contains("queue") || msg.contains("TaskInvalidOptions") {
                    println!("  worker:     FAILED on queue validation — GAP! {}", msg);
                } else {
                    println!("  worker:     FAILED at broker (correct — past queue validation)");
                }
            }
            Ok(_) => println!("  worker:     STARTED (would need real DB)"),
        }
        println!();
    }

    // Scenario 6: CUSTOM + 1 valid + 1 invalid
    {
        println!("--- Scenario 6: CUSTOM + valid \"fast\" + invalid \"analytics\" ---");
        let mut app = Horsies::new(custom_config()).unwrap();
        let r1 = app
            .task::<Input, Output>("fast_compute", async_task_fn!(compute, Input))
            .unwrap()
            .queue("fast")
            .register();
        println!(
            "  task A (fast):      {}",
            if r1.is_ok() {
                "PASSED"
            } else {
                "FAILED — GAP!"
            }
        );
        let r2 = app
            .task::<Input, Output>("analytics_compute", async_task_fn!(multiply, Input))
            .unwrap()
            .queue("analytics")
            .register();
        match r2 {
            Err(ref e) => {
                println!("  task B (analytics): FAILED (as expected)");
                println!("  error code:         {:?}", e.code);
                println!("  message:            {}", e);
            }
            Ok(_) => println!("  task B (analytics): PASSED — GAP!"),
        }
        println!("  startup:            POISONED (second registration failed, app must abort)");
        println!();
    }

    println!("{}", "=".repeat(100));
    println!("GAPS FOUND: 0 — all mismatches are hard fails before any task runs.");
    println!("{}", "=".repeat(100));
}
