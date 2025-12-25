//! Benchmark utilities for Event Scanner performance testing.
//!
//! This module provides shared setup utilities for benchmarks, including
//! Anvil instance management, contract deployment, and event generation.

use alloy::{
    network::Ethereum,
    primitives::Address,
    providers::{Provider, ProviderBuilder},
    sol,
    sol_types::SolEvent,
};
use alloy_node_bindings::{Anvil, AnvilInstance};
use event_scanner::robust_provider::{RobustProvider, RobustProviderBuilder};
use futures::future::try_join_all;

sol! {
    // Built directly with solc 0.8.30+commit.73712a01.Darwin.appleclang
    #[sol(rpc, bytecode="608080604052346015576101b0908161001a8239f35b5f80fdfe6080806040526004361015610012575f80fd5b5f3560e01c90816306661abd1461016157508063a87d942c14610145578063d732d955146100ad5763e8927fbc14610048575f80fd5b346100a9575f3660031901126100a9575f5460018101809111610095576020817f7ca2ca9527391044455246730762df008a6b47bbdb5d37a890ef78394535c040925f55604051908152a1005b634e487b7160e01b5f52601160045260245ffd5b5f80fd5b346100a9575f3660031901126100a9575f548015610100575f198101908111610095576020817f53a71f16f53e57416424d0d18ccbd98504d42a6f98fe47b09772d8f357c620ce925f55604051908152a1005b60405162461bcd60e51b815260206004820152601860248201527f436f756e742063616e6e6f74206265206e6567617469766500000000000000006044820152606490fd5b346100a9575f3660031901126100a95760205f54604051908152f35b346100a9575f3660031901126100a9576020905f548152f3fea2646970667358221220471585b420a1ad0093820ff10129ec863f6df4bec186546249391fbc3cdbaa7c64736f6c634300081e0033")]
    contract Counter {
        uint256 public count;

        event CountIncreased(uint256 newCount);
        event CountDecreased(uint256 newCount);

        function increase() public {
            count += 1;
            emit CountIncreased(count);
        }

        function decrease() public {
            require(count > 0, "Count cannot be negative");
            count -= 1;
            emit CountDecreased(count);
        }

        function getCount() public view returns (uint256) {
            return count;
        }
    }
}

/// Returns the event signature for `CountIncreased`.
#[must_use]
pub fn count_increased_signature() -> &'static str {
    Counter::CountIncreased::SIGNATURE
}

/// Pre-configured benchmark environment with an Anvil instance,
/// deployed contract, and generated events.
pub struct BenchEnvironment {
    /// The Anvil instance. Must be kept alive for the duration of the benchmark.
    #[allow(dead_code)]
    anvil: AnvilInstance,
    /// A robust provider connected to the Anvil instance.
    pub provider: RobustProvider<Ethereum>,
    /// The deployed contract address.
    pub contract_address: Address,
    /// The total number of events generated.
    pub event_count: usize,
}

/// Configuration for setting up a benchmark environment.
pub struct BenchConfig {
    /// Number of events to generate.
    pub event_count: usize,
    /// Block time in seconds for Anvil.
    pub block_time: f64,
}

impl BenchConfig {
    /// Creates a new configuration with the specified event count.
    #[must_use]
    pub fn new(event_count: usize) -> Self {
        Self { event_count, block_time: 0.01 }
    }
}

/// Sets up a benchmark environment by spawning Anvil, deploying a contract,
/// and generating the specified number of events.
///
/// # Errors
///
/// Returns an error if Anvil fails to spawn, the contract fails to deploy,
/// or event generation fails.
///
/// # Panics
///
/// Panics if Anvil does not return a default wallet.
pub async fn setup_environment(config: BenchConfig) -> anyhow::Result<BenchEnvironment> {
    let anvil = Anvil::new().block_time_f64(config.block_time).try_spawn()?;

    let wallet = anvil.wallet().expect("anvil should return a default wallet");
    let provider =
        ProviderBuilder::new().wallet(wallet).connect(anvil.ws_endpoint_url().as_str()).await?;

    let contract = Counter::deploy(provider.clone()).await?;
    let contract_address = *contract.address();

    // Generate events by sending transactions
    generate_events(&contract, config.event_count).await?;

    // Build robust provider for the scanner
    let robust_provider = RobustProviderBuilder::new(provider)
        .call_timeout(std::time::Duration::from_secs(30))
        .max_retries(5)
        .min_delay(std::time::Duration::from_millis(500))
        .build()
        .await?;

    Ok(BenchEnvironment {
        anvil,
        provider: robust_provider,
        contract_address,
        event_count: config.event_count,
    })
}

/// Generates the specified number of events by calling the contract's `increase` function.
async fn generate_events<P>(
    contract: &Counter::CounterInstance<P>,
    count: usize,
) -> anyhow::Result<()>
where
    P: Provider<Ethereum> + Clone,
{
    // Send all transactions
    let mut pending_txs = Vec::with_capacity(count);
    for _ in 0..count {
        pending_txs.push(contract.increase().send().await?);
    }

    // Wait for all transactions to be confirmed
    let watch_futures = pending_txs.into_iter().map(|tx| async move { tx.watch().await });
    try_join_all(watch_futures).await?;

    Ok(())
}
