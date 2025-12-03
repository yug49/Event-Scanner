use tokio_stream::wrappers::ReceiverStream;

use super::{EventScannerResult, handle::ScannerHandle};

/// A subscription to scanner events that requires proof the scanner has started.
///
/// Created by [`EventScanner::subscribe()`], this type holds the underlying stream
/// but prevents access until [`stream()`](Subscription::stream) is called with a
/// valid [`ScannerHandle`].
///
/// This pattern ensures at compile time that [`EventScanner::start()`] is called
/// before attempting to read from the event stream.
///
/// # Example
///
/// ```ignore
/// let mut scanner = EventScannerBuilder::live().connect(provider).await?;
///
/// // Create subscription (cannot access stream yet)
/// let subscription = scanner.subscribe(filter);
///
/// // Start scanner and get handle
/// let handle = scanner.start().await?;
///
/// // Now access the stream with the handle
/// let mut stream = subscription.stream(&handle);
///
/// while let Some(msg) = stream.next().await {
///     // process events
/// }
/// ```
pub struct EventSubscription {
    inner: ReceiverStream<EventScannerResult>,
}

impl EventSubscription {
    /// Creates a new subscription wrapping the given stream.
    pub(crate) fn new(inner: ReceiverStream<EventScannerResult>) -> Self {
        Self { inner }
    }

    /// Access the event stream.
    ///
    /// Requires a reference to a [`ScannerHandle`] as proof that the scanner
    /// has been started. The handle is obtained by calling [`EventScanner::start()`].
    ///
    /// # Arguments
    ///
    /// * `_handle` - Proof that the scanner has been started
    ///
    /// # Returns
    ///
    /// The underlying event stream that yields [`EventScannerResult`] items.
    #[must_use]
    pub fn stream(self, _handle: &ScannerHandle) -> ReceiverStream<EventScannerResult> {
        self.inner
    }
}
