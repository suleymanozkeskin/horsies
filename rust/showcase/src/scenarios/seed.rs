//! Demo database and catalog preparation.

use super::{heading, load_seed_catalog, prepare_database, say, ScenarioResult};

pub async fn run() -> ScenarioResult<()> {
    heading("Acme Clothing — seed");
    let (settings, store) = prepare_database().await?;
    say(format!(
        "database: {} (resolved from {})",
        settings.database_name, settings.source
    ));
    let loaded = load_seed_catalog(&store).await?;
    say(format!("acme_* tables ready; loaded {loaded} products"));
    say(format!(
        "{} products have zero stock; {} units seed each stocked SKU",
        crate::tuning::DISCONTINUED_SKU_COUNT,
        crate::tuning::CATALOG_STOCK_PER_SKU
    ));
    say("next");
    say("  cargo run -p acme-showcase --features web --bin acme -- worker");
    say("  cargo run -p acme-showcase --features web --bin acme -- scheduler");
    say("  cargo run -p acme-showcase --features web --bin acme -- web --auth none");
    say("  cargo run -p acme-showcase --bin acme -- steady");
    say("  open http://127.0.0.1:8600");
    store.close().await;
    Ok(())
}
