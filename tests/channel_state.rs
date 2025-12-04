use std::ops::RangeInclusive;

use event_scanner::{Notification, ScannerMessage};
use tokio::sync::mpsc;

/// Type alias for test results
type TestResult = Result<ScannerMessage<RangeInclusive<u64>>, event_scanner::ScannerError>;

mod channel_state_enum {
    use event_scanner::ChannelState;

    #[test]
    fn is_open_returns_true_for_open_state() {
        assert!(ChannelState::Open.is_open());
    }

    #[test]
    fn is_open_returns_false_for_closed_state() {
        assert!(!ChannelState::Closed.is_open());
    }

    #[test]
    fn is_closed_returns_true_for_closed_state() {
        assert!(ChannelState::Closed.is_closed());
    }

    #[test]
    fn is_closed_returns_false_for_open_state() {
        assert!(!ChannelState::Open.is_closed());
    }

    #[test]
    fn channel_state_equality() {
        assert_eq!(ChannelState::Open, ChannelState::Open);
        assert_eq!(ChannelState::Closed, ChannelState::Closed);
        assert_ne!(ChannelState::Open, ChannelState::Closed);
    }

    #[test]
    fn channel_state_is_copy() {
        let state = ChannelState::Open;
        let copied = state; // Copy, not move
        assert_eq!(state, copied); // Both are still valid
    }

    #[test]
    fn channel_state_debug_format() {
        assert_eq!(format!("{:?}", ChannelState::Open), "Open");
        assert_eq!(format!("{:?}", ChannelState::Closed), "Closed");
    }
}

mod try_stream {
    use super::*;
    use event_scanner::{ChannelState, TryStream};

    #[tokio::test]
    async fn try_stream_returns_open_when_receiver_exists() {
        let (tx, _rx) = mpsc::channel::<TestResult>(10);

        let result = tx.try_stream(Notification::ReorgDetected).await;

        assert_eq!(result, ChannelState::Open);
        assert!(result.is_open());
        assert!(!result.is_closed());
    }

    #[tokio::test]
    async fn try_stream_returns_closed_when_receiver_dropped() {
        let (tx, rx) = mpsc::channel::<TestResult>(10);
        drop(rx); // Drop the receiver to close the channel

        let result = tx.try_stream(Notification::ReorgDetected).await;

        assert_eq!(result, ChannelState::Closed);
        assert!(result.is_closed());
        assert!(!result.is_open());
    }

    #[tokio::test]
    async fn try_stream_sends_message_successfully() {
        let (tx, mut rx) = mpsc::channel::<TestResult>(10);

        let result = tx.try_stream(Notification::SwitchingToLive).await;

        assert_eq!(result, ChannelState::Open);

        // Verify the message was actually sent
        let received = rx.recv().await.unwrap();
        assert!(matches!(
            received,
            Ok(ScannerMessage::Notification(Notification::SwitchingToLive))
        ));
    }
}
