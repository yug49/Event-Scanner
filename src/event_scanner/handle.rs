/// Proof that the scanner has been started.
///
/// This handle is returned by [`EventScanner::start()`](crate::EventScanner) and must be passed to
/// [`Subscription::stream()`] to access the event stream. This ensures at compile
/// time that the scanner is started before attempting to read events.
///
/// # Example
///
/// ```ignore
/// let mut scanner = EventScannerBuilder::sync().from_block(0).connect(provider).await?;
/// let subscription = scanner.subscribe(filter);
///
/// // Start the scanner and get the handle
/// let handle = scanner.start().await?;
///
/// // Now we can access the stream
/// let mut stream = subscription.stream(&handle);
/// ```
#[derive(Debug, Clone)]
pub struct ScannerHandle {
    /// Private field prevents construction outside this crate
    _private: (),
}

impl ScannerHandle {
    /// Creates a new scanner handle.
    ///
    /// This is intentionally `pub(crate)` to prevent external construction.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}
