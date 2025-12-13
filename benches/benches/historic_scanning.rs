//! Benchmarks for historic scanning mode.
//!
//! Heavy load tests that measure the time to fetch all expected events.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use event_scanner::{EventFilter, EventScannerBuilder, Message};
use event_scanner_benches::{
    BenchConfig, BenchEnvironment, count_increased_signature, setup_environment,
};
use tokio_stream::StreamExt;

/// Runs a single historic scan and returns the number of events received.
///
/// This fetches ALL events from block 0 to latest
async fn run_historic_scan(env: &BenchEnvironment) -> usize {
    let filter = EventFilter::new()
        .contract_address(env.contract_address)
        .event(count_increased_signature());

    let mut scanner = EventScannerBuilder::historic()
        .max_block_range(100)
        .from_block(0)
        .to_block(alloy::eips::BlockNumberOrTag::Latest)
        .connect(env.provider.clone())
        .await
        .expect("failed to build scanner");

    let mut stream = scanner.subscribe(filter);
    scanner.start().await.expect("failed to start scanner");

    let mut log_count = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(Message::Data(logs)) => {
                log_count += logs.len();
            }
            Ok(Message::Notification(notification)) => {
                panic!("Received unexpected notification: {notification:?}");
            }
            Err(e) => {
                panic!("Received error: {e}");
            }
        }
    }

    log_count
}

fn historic_scanning_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");

    let mut group = c.benchmark_group("historic_scanning");

    // Configure for heavy load tests:
    // - 10 samples (iterations)
    // - Long measurement time to accommodate heavy loads
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(120));

    // Heavy load test: 100,000 events
    // Also include smaller sizes for regression comparison
    for event_count in [10_000, 50_000, 100_000] {
        println!("Setting up environment with {event_count} events...");

        // Setup environment once per event count (events are pre-generated)
        let env: BenchEnvironment = rt.block_on(async {
            let config = BenchConfig::default().with_event_count(event_count);
            setup_environment(config).await.expect("failed to setup benchmark environment")
        });

        println!("Environment ready. Verifying event count...");

        // Verify setup correctness before benchmarking
        let actual_count = rt.block_on(run_historic_scan(&env));
        assert_eq!(actual_count, event_count, "expected {event_count} events, got {actual_count}");

        println!("Verified {event_count} events. Starting benchmark...");

        group.throughput(Throughput::Elements(event_count as u64));

        group.bench_with_input(BenchmarkId::new("events", event_count), &env, |b, env| {
            b.to_async(&rt).iter(|| run_historic_scan(env));
        });
    }

    group.finish();
}

criterion_group!(benches, historic_scanning_benchmark);
criterion_main!(benches);
