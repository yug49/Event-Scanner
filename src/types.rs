use std::fmt::Debug;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::ScannerError;

/// Represents the state of a channel after attempting to send a message.
///
/// This enum provides explicit semantics for channel operations, making it clear
/// whether the downstream receiver is still listening or has been dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelState {
    /// The channel is open and the message was successfully sent.
    Open,
    /// The channel is closed (receiver dropped), no further messages can be sent.
    Closed,
}

impl ChannelState {
    /// Returns `true` if the channel is open.
    #[must_use]
    #[allow(dead_code)]
    pub fn is_open(self) -> bool {
        matches!(self, ChannelState::Open)
    }

    /// Returns `true` if the channel is closed.
    #[must_use]
    pub fn is_closed(self) -> bool {
        matches!(self, ChannelState::Closed)
    }
}

/// Messages streamed by the scanner to subscribers.
///
/// Each message represents either data or a notification about the scanner's state or behavior.
#[derive(Copy, Debug, Clone)]
pub enum ScannerMessage<T: Clone> {
    /// Data streamed to the subscriber.
    Data(T),

    /// Notification about scanner state changes or important events.
    Notification(Notification),
}

/// Notifications emitted by the scanner to signal state changes or important events.
#[derive(Copy, Debug, Clone, PartialEq)]
pub enum Notification {
    /// Emitted when transitioning from the latest events phase to live streaming mode
    /// in sync scanners.
    SwitchingToLive,

    /// Emitted when a blockchain reorganization is detected during scanning.
    ReorgDetected,

    /// Emitted during the latest events phase when no matching logs are found in the
    /// scanned range.
    NoPastLogsFound,
}

impl<T: Clone> From<Notification> for ScannerMessage<T> {
    fn from(value: Notification) -> Self {
        ScannerMessage::Notification(value)
    }
}

impl<T: Clone> PartialEq<Notification> for ScannerMessage<T> {
    fn eq(&self, other: &Notification) -> bool {
        if let ScannerMessage::Notification(notification) = self {
            notification == other
        } else {
            false
        }
    }
}

pub type ScannerResult<T> = Result<ScannerMessage<T>, ScannerError>;

pub trait IntoScannerResult<T: Clone> {
    fn into_scanner_message_result(self) -> ScannerResult<T>;
}

impl<T: Clone> IntoScannerResult<T> for ScannerResult<T> {
    fn into_scanner_message_result(self) -> ScannerResult<T> {
        self
    }
}

impl<T: Clone> IntoScannerResult<T> for ScannerMessage<T> {
    fn into_scanner_message_result(self) -> ScannerResult<T> {
        Ok(self)
    }
}

impl<T: Clone, E: Into<ScannerError>> IntoScannerResult<T> for E {
    fn into_scanner_message_result(self) -> ScannerResult<T> {
        Err(self.into())
    }
}

impl<T: Clone> IntoScannerResult<T> for Notification {
    fn into_scanner_message_result(self) -> ScannerResult<T> {
        Ok(ScannerMessage::Notification(self))
    }
}

pub trait TryStream<T: Clone + Send>: Send + Sync {
    fn try_stream<M: IntoScannerResult<T> + Send>(
        &self,
        msg: M,
    ) -> impl std::future::Future<Output = ChannelState> + Send;
}

impl<T: Clone + Debug + Send> TryStream<T> for mpsc::Sender<ScannerResult<T>> {
    async fn try_stream<M: IntoScannerResult<T>>(&self, msg: M) -> ChannelState {
        let item = msg.into_scanner_message_result();
        match &item {
            Ok(msg) => info!(item = ?msg, "Sending message"),
            Err(err) => info!(error = ?err, "Sending error"),
        }
        if let Err(err) = self.send(item).await {
            warn!(error = %err, "Downstream channel closed, stopping stream");
            return ChannelState::Closed;
        }
        ChannelState::Open
    }
}
