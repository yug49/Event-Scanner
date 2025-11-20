use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use crate::{
    block_range_scanner::{Message, reorg_handler::ReorgHandler},
    robust_provider::RobustProvider,
    types::{Notification, TryStream},
};
use alloy::{
    consensus::BlockHeader,
    network::{BlockResponse, Network},
    primitives::BlockNumber,
    pubsub::Subscription,
};
use tracing::{debug, error, info, warn};

// TODO: refactor this function to reduce the number of arguments
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stream_live_blocks<N: Network>(
    stream_start: BlockNumber,
    subscription: Subscription<N::HeaderResponse>,
    sender: &mpsc::Sender<Message>,
    provider: &RobustProvider<N>,
    block_confirmations: u64,
    max_block_range: u64,
    reorg_handler: &mut ReorgHandler<N>,
    notify_after_first_block: bool,
) {
    // ensure we start streaming only after the specified starting block
    let mut stream = subscription
        .into_stream()
        .skip_while(|header| header.number().saturating_sub(block_confirmations) < stream_start);

    let Some(incoming_block) = stream.next().await else {
        warn!("Subscription channel closed");
        return;
    };

    if notify_after_first_block && !sender.try_stream(Notification::StartingLiveStream).await {
        return;
    }

    let incoming_block_num = incoming_block.number();
    info!(block_number = incoming_block_num, "Received block header");

    let confirmed = incoming_block_num.saturating_sub(block_confirmations);

    let mut previous_batch_end = stream_historical_blocks(
        stream_start,
        stream_start,
        confirmed,
        max_block_range,
        &sender,
        provider,
        reorg_handler,
    )
    .await;

    if previous_batch_end.is_none() {
        // the sender channel is closed
        return;
    }

    let mut batch_start = stream_start;

    while let Some(incoming_block) = stream.next().await {
        let incoming_block_num = incoming_block.number();
        info!(block_number = incoming_block_num, "Received block header");

        let reorged_opt = match previous_batch_end.as_ref() {
            None => None,
            Some(batch_end) => match reorg_handler.check(batch_end).await {
                Ok(opt) => opt,
                Err(e) => {
                    error!(error = %e, "Failed to perform reorg check");
                    _ = sender.try_stream(e).await;
                    return;
                }
            },
        };

        if let Some(common_ancestor) = reorged_opt {
            if !sender.try_stream(Notification::ReorgDetected).await {
                return;
            }
            // no need to stream blocks prior to the previously specified starting block
            if common_ancestor.header().number() < stream_start {
                batch_start = stream_start;
                previous_batch_end = None;
            } else {
                batch_start = common_ancestor.header().number() + 1;
                previous_batch_end = Some(common_ancestor);
            }

            // TODO: explain in docs that the returned block after a reorg will be the
            // first confirmed block that is smaller between:
            // - the first post-reorg block
            // - the previous range_start
        } else {
            // no reorg happened, move the block range back to expected next start
            //
            // SAFETY: Overflow cannot realistically happen
            if let Some(prev_batch_end) = previous_batch_end.as_ref() {
                batch_start = prev_batch_end.header().number() + 1;
            }
        }

        let batch_end_num = incoming_block_num.saturating_sub(block_confirmations);
        if batch_end_num >= batch_start {
            previous_batch_end = stream_historical_blocks(
                stream_start,
                batch_start,
                batch_end_num,
                max_block_range,
                &sender,
                provider,
                reorg_handler,
            )
            .await;

            if previous_batch_end.is_none() {
                // the sender channel is closed
                return;
            }

            // SAFETY: Overflow cannot realistically happen
            batch_start = batch_end_num + 1;
        }
    }
}

/// Assumes that `stream_start <= next_start_block <= end`.
pub(crate) async fn stream_historical_blocks<N: Network>(
    stream_start: BlockNumber,
    mut next_start_block: BlockNumber,
    end: BlockNumber,
    max_block_range: u64,
    sender: &mpsc::Sender<Message>,
    provider: &RobustProvider<N>,
    reorg_handler: &mut ReorgHandler<N>,
) -> Option<N::BlockResponse> {
    let mut batch_count = 0;

    loop {
        let batch_end_num = next_start_block.saturating_add(max_block_range - 1).min(end);
        let batch_end = match provider.get_block_by_number(batch_end_num.into()).await {
            Ok(block) => block,
            Err(e) => {
                error!(batch_start = next_start_block, batch_end = batch_end_num, error = %e, "Failed to get ending block of the current batch");
                _ = sender.try_stream(e).await;
                return None;
            }
        };

        if !sender.try_stream(next_start_block..=batch_end_num).await {
            return Some(batch_end);
        }

        batch_count += 1;
        if batch_count % 10 == 0 {
            debug!(batch_count = batch_count, "Processed historical batches");
        }

        let reorged_opt = match reorg_handler.check(&batch_end).await {
            Ok(opt) => opt,
            Err(e) => {
                error!(error = %e, "Failed to perform reorg check");
                _ = sender.try_stream(e).await;
                return None;
            }
        };

        next_start_block = if let Some(common_ancestor) = reorged_opt {
            if !sender.try_stream(Notification::ReorgDetected).await {
                return None;
            }
            if common_ancestor.header().number() < stream_start {
                stream_start
            } else {
                common_ancestor.header().number() + 1
            }
        } else {
            batch_end_num.saturating_add(1)
        };

        if next_start_block > end {
            info!(batch_count = batch_count, "Historical sync completed");
            return Some(batch_end);
        }
    }
}
