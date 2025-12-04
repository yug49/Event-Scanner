//! Stream access types for the event scanner.
//!
//! This module provides [`ScannerToken`] and [`EventSubscription`], which together
//! enforce at compile time that the scanner is started before accessing event streams.

use tokio_stream::wrappers::ReceiverStream;

use super::EventScannerResult;

/// Proof that the scanner has been started.
///
/// This token is returned by [`EventScanner::start()`](crate::EventScanner) and must be passed to
/// [`EventSubscription::stream()`] to access the event stream. This ensures at compile
/// time that the scanner is started before attempting to read events.
///
/// # Example
///
/// ```ignore
/// let mut scanner = EventScannerBuilder::sync().from_block(0).connect(provider).await?;
/// let subscription = scanner.subscribe(filter);
///
/// // Start the scanner and get the token
/// let token = scanner.start().await?;
///
/// // Now we can access the stream
/// let mut stream = subscription.stream(&token);
/// ```
#[derive(Debug, Clone)]
pub struct ScannerToken {
    /// Private field prevents construction outside this crate
    _private: (),
}

impl ScannerToken {
    /// Creates a new scanner token.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

/// A subscription to scanner events that requires proof the scanner has started.
///
/// Created by [`EventScanner::subscribe()`](crate::EventScanner::subscribe), this type holds the
/// underlying stream but prevents access until [`stream()`](EventSubscription::stream) is called
/// with a valid [`ScannerToken`].
///
/// This pattern ensures at compile time that [`EventScanner::start()`](crate::EventScanner::start)
/// is called before attempting to read from the event stream.
///
/// # Example
///
/// ```ignore
/// let mut scanner = EventScannerBuilder::live().connect(provider).await?;
///
/// // Create subscription (cannot access stream yet)
/// let subscription = scanner.subscribe(filter);
///
/// // Start scanner and get token
/// let token = scanner.start().await?;
///
/// // Now access the stream with the token
/// let mut stream = subscription.stream(&token);
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
    /// Requires a reference to a [`ScannerToken`] as proof that the scanner
    /// has been started. The token is obtained by calling
    /// [`EventScanner::start()`](crate::EventScanner::start).
    ///
    /// # Arguments
    ///
    /// * `_token` - Proof that the scanner has been started
    ///
    /// # Returns
    ///
    /// The underlying event stream that yields [`EventScannerResult`] items.
    #[must_use]
    pub fn stream(self, _token: &ScannerToken) -> ReceiverStream<EventScannerResult> {
        self.inner
    }
}
