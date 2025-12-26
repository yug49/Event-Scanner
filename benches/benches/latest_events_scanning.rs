//! Benchmarks for latest events scanning mode.
//!
//! Heavy load tests that measure the time to fetch the N most recent events
//! from a large pool of pre-generated events.

use std::sync::OnceLock;

use anyhow::{Result, bail, ensure};
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

/// Runs a single latest events scan and asserts the expected event count.
///
/// This fetches the `latest_count` most recent events.
async fn run_latest_events_scan(env: &BenchEnvironment, latest_count: usize) -> Result<()> {
    let filter = EventFilter::new()
        .contract_address(env.contract_address)
        .event(count_increased_signature());

    let mut scanner =
        EventScannerBuilder::latest(latest_count).connect(env.provider.clone()).await?;

    let mut stream = scanner.subscribe(filter);
    scanner.start().await?;

    let mut log_count = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(Message::Data(logs)) => {
                log_count += logs.len();
            }
            Ok(Message::Notification(notification)) => {
                bail!("Received unexpected notification: {notification:?}");
            }
            Err(e) => {
                bail!("Received error: {e}");
            }
        }
    }

    ensure!(log_count == latest_count, "expected {latest_count} events, got {log_count}");

    Ok(())
}

fn latest_events_scanning_benchmark(c: &mut Criterion) {
    let rt = get_runtime();

    let mut group = c.benchmark_group("latest_events_scanning");

    // Configure for heavy load tests
    group.warm_up_time(std::time::Duration::from_secs(5));
    group.measurement_time(std::time::Duration::from_secs(120));

    // Generate a pool of events once
    // We'll benchmark fetching different "latest N" counts from this pool
    // Using 50K total
    let total_events = 50_000;

    println!("Setting up environment with {total_events} total events...");

    let env: BenchEnvironment = rt.block_on(async {
        let config = BenchConfig::new(total_events);
        setup_environment(config).await.expect("failed to setup benchmark environment")
    });

    println!("Environment ready. Starting benchmarks...");

    // Benchmark fetching different "latest N" counts
    // Trying to replicate realistic use cases:
    // - 100: Quick recent activity check
    // - 1,000: Moderate history lookup
    // - 10,000: Substantial history fetch
    // - 25,000: Heavy load retrieval
    for latest_count in [100, 1_000, 10_000, 25_000] {
        println!("Benchmarking latest {latest_count} events...");

        group.throughput(Throughput::Elements(latest_count as u64));

        group.bench_with_input(
            BenchmarkId::new("latest", latest_count),
            &latest_count,
            |b, &count| {
                b.to_async(&rt).iter(|| async {
                    run_latest_events_scan(&env, count).await.expect("latest events scan failed")
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, latest_events_scanning_benchmark);
criterion_main!(benches);
