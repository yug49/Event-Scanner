//! Benchmarks for historic scanning mode.
//!
//! Heavy load tests that measure the time to fetch all expected events.

use std::sync::OnceLock;

use anyhow::{Result, bail};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use event_scanner::{EventFilter, EventScannerBuilder, Message};
use event_scanner_benches::{
    BenchConfig, BenchEnvironment, count_increased_signature, setup_environment,
};
use tokio_stream::StreamExt;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn get_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("failed to create tokio runtime"))
}

/// Runs a single historic scan.
///
/// This fetches ALL events from block 0 to latest.
async fn run_historic_scan(env: &BenchEnvironment) -> Result<()> {
    let filter = EventFilter::new()
        .contract_address(env.contract_address)
        .event(count_increased_signature());

    let mut scanner = EventScannerBuilder::historic()
        .max_block_range(100)
        .from_block(0)
        .to_block(alloy::eips::BlockNumberOrTag::Latest)
        .connect(env.provider.clone())
        .await?;

    let mut stream = scanner.subscribe(filter);
    scanner.start().await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(Message::Data(_)) => {}
            Ok(Message::Notification(notification)) => {
                bail!("Received unexpected notification: {notification:?}");
            }
            Err(e) => {
                bail!("Received error: {e}");
            }
        }
    }

    Ok(())
}

fn historic_scanning_benchmark(c: &mut Criterion) {
    let rt = get_runtime();

    let mut group = c.benchmark_group("historic_scanning");

    // Configure for heavy load tests
    group.warm_up_time(std::time::Duration::from_secs(5));
    group.measurement_time(std::time::Duration::from_secs(120));

    // Heavy load test: 100,000 events
    // Also include smaller sizes for regression comparison
    for event_count in [10_000, 50_000, 100_000] {
        println!("Setting up environment with {event_count} events...");

        // Setup environment once per event count (events are pre-generated)
        let env: BenchEnvironment = rt.block_on(async {
            let config = BenchConfig::new(event_count);
            setup_environment(config).await.expect("failed to setup benchmark environment")
        });

        println!("Environment ready. Starting benchmark...");

        group.throughput(Throughput::Elements(event_count as u64));

        group.bench_with_input(BenchmarkId::new("events", event_count), &env, |b, env| {
            b.to_async(&rt)
                .iter(|| async { run_historic_scan(env).await.expect("historic scan failed") });
        });
    }

    group.finish();
}

criterion_group!(benches, historic_scanning_benchmark);
criterion_main!(benches);
