//! M1 audio capture boundary: explicit targets, bounded in-memory delivery, cancellation,
//! deterministic replay, and the Linux PipeWire backend.

use sidecar_core::AudioFrame;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use thiserror::Error;

#[cfg(target_os = "linux")]
mod pipewire_backend;

#[cfg(target_os = "linux")]
pub use pipewire_backend::run_pipewire_capture;

/// Whether the selected PipeWire target is a source or a sink whose monitor ports are captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureMode {
    Source,
    SinkMonitor,
}

/// An explicit PipeWire target chosen by the user.
///
/// `object` is passed as `target.object`, which PipeWire resolves as a node name or object serial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureTarget {
    object: String,
    mode: CaptureMode,
}

impl CaptureTarget {
    pub fn new(object: impl Into<String>, mode: CaptureMode) -> Result<Self, CaptureError> {
        let object = object.into();
        if object.trim().is_empty() {
            return Err(CaptureError::InvalidTarget);
        }

        Ok(Self { object, mode })
    }

    pub fn object(&self) -> &str {
        &self.object
    }

    pub const fn mode(&self) -> CaptureMode {
        self.mode
    }
}

#[derive(Debug, Error)]
pub enum CaptureError {
    #[error("capture target must not be empty")]
    InvalidTarget,
    #[error("frame buffer capacity must be greater than zero")]
    InvalidBufferCapacity,
    #[error("audio frame consumer disconnected")]
    FrameConsumerDisconnected,
    #[error("failed to install process signal handler: {0}")]
    Signal(#[from] io::Error),
    #[error("PipeWire backend error: {0}")]
    Backend(String),
}

/// Why a capture run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Cancelled,
    SourceLost,
    BackendError,
}

/// Metadata-only lifecycle events. Audio payloads are never included here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureEvent {
    Starting { target: CaptureTarget },
    Connecting,
    FormatNegotiated(sidecar_core::AudioFormat),
    Streaming,
    SourceLost,
    Error(String),
    Stopped(StopReason),
}

/// Cooperative cancellation shared by the daemon, replay source, and PipeWire loop bridge.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn signal_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }
}

/// Installs SIGINT and SIGTERM handlers which only flip the cancellation flag.
///
/// The handlers perform no allocation or shutdown work in signal context. The active capture
/// backend observes the flag and exits through its ordinary cancellation path.
pub fn install_termination_signals(token: &CancellationToken) -> Result<(), CaptureError> {
    use signal_hook::consts::signal::{SIGINT, SIGTERM};

    signal_hook::flag::register(SIGINT, token.signal_flag())?;
    signal_hook::flag::register(SIGTERM, token.signal_flag())?;
    Ok(())
}

#[derive(Debug, Default)]
struct FrameStatsInner {
    accepted: AtomicU64,
    dropped_full: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameStats {
    pub accepted: u64,
    pub dropped_full: u64,
}

/// Non-blocking producer for a bounded in-memory audio queue.
///
/// If the consumer cannot keep up, the newest frame is dropped and counted. Capture never waits
/// on downstream work and queue memory cannot grow without bound.
#[derive(Debug, Clone)]
pub struct BoundedFrameSender {
    tx: mpsc::SyncSender<AudioFrame>,
    stats: Arc<FrameStatsInner>,
}

impl BoundedFrameSender {
    pub fn try_send(&self, frame: AudioFrame) -> Result<bool, CaptureError> {
        match self.tx.try_send(frame) {
            Ok(()) => {
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(mpsc::TrySendError::Full(_)) => {
                self.stats.dropped_full.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(CaptureError::FrameConsumerDisconnected)
            }
        }
    }

    pub fn stats(&self) -> FrameStats {
        FrameStats {
            accepted: self.stats.accepted.load(Ordering::Relaxed),
            dropped_full: self.stats.dropped_full.load(Ordering::Relaxed),
        }
    }
}

pub fn bounded_frame_channel(
    capacity: usize,
) -> Result<(BoundedFrameSender, mpsc::Receiver<AudioFrame>), CaptureError> {
    if capacity == 0 {
        return Err(CaptureError::InvalidBufferCapacity);
    }

    let (tx, rx) = mpsc::sync_channel(capacity);
    Ok((
        BoundedFrameSender {
            tx,
            stats: Arc::new(FrameStatsInner::default()),
        },
        rx,
    ))
}

/// Deterministic capture input used by tests and later replay-driven integration checks.
#[derive(Debug, Clone, Default)]
pub struct ReplayFixture {
    frames: Vec<AudioFrame>,
}

impl ReplayFixture {
    pub fn new(frames: Vec<AudioFrame>) -> Self {
        Self { frames }
    }

    pub fn frames(&self) -> &[AudioFrame] {
        &self.frames
    }

    pub fn run(
        &self,
        sender: &BoundedFrameSender,
        cancellation: &CancellationToken,
    ) -> Result<ReplayOutcome, CaptureError> {
        let mut attempted = 0_u64;

        for frame in &self.frames {
            if cancellation.is_cancelled() {
                return Ok(ReplayOutcome {
                    attempted,
                    cancelled: true,
                });
            }

            sender.try_send(frame.clone())?;
            attempted += 1;
            std::thread::yield_now();
        }

        Ok(ReplayOutcome {
            attempted,
            cancelled: false,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayOutcome {
    pub attempted: u64,
    pub cancelled: bool,
}

#[cfg(not(target_os = "linux"))]
pub fn run_pipewire_capture(
    _target: CaptureTarget,
    _frames: BoundedFrameSender,
    _events: mpsc::Sender<CaptureEvent>,
    _cancellation: CancellationToken,
) -> Result<StopReason, CaptureError> {
    Err(CaptureError::Backend(
        "PipeWire capture is supported only on Linux in V1".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sidecar_core::{AudioFormat, MonotonicTimestamp, SampleFormat};

    fn frame(sequence: u64) -> AudioFrame {
        AudioFrame::new(
            sequence,
            MonotonicTimestamp::from_nanos(sequence * 1_000),
            AudioFormat::new(48_000, 2, SampleFormat::F32Le),
            vec![sequence as u8; 8],
        )
    }

    #[test]
    fn target_selection_is_explicit_and_non_empty() {
        assert_eq!(
            CaptureTarget::new("  ", CaptureMode::Source)
                .unwrap_err()
                .to_string(),
            "capture target must not be empty"
        );

        let target = CaptureTarget::new("alsa_output.pci-0000", CaptureMode::SinkMonitor).unwrap();
        assert_eq!(target.object(), "alsa_output.pci-0000");
        assert_eq!(target.mode(), CaptureMode::SinkMonitor);
    }

    #[test]
    fn bounded_buffer_drops_newest_instead_of_growing_or_blocking() {
        let (sender, receiver) = bounded_frame_channel(1).unwrap();

        assert!(sender.try_send(frame(1)).unwrap());
        assert!(!sender.try_send(frame(2)).unwrap());

        assert_eq!(receiver.recv().unwrap().sequence, 1);
        assert_eq!(
            sender.stats(),
            FrameStats {
                accepted: 1,
                dropped_full: 1,
            }
        );
    }

    #[test]
    fn deterministic_replay_preserves_frame_order_and_metadata() {
        let expected = vec![frame(1), frame(2), frame(3)];
        let fixture = ReplayFixture::new(expected.clone());
        let (sender, receiver) = bounded_frame_channel(expected.len()).unwrap();
        let cancellation = CancellationToken::new();

        let outcome = fixture.run(&sender, &cancellation).unwrap();
        let actual: Vec<_> = receiver.try_iter().collect();

        assert_eq!(outcome.attempted, 3);
        assert!(!outcome.cancelled);
        assert_eq!(actual, expected);
        assert!(actual
            .windows(2)
            .all(|pair| pair[0].timestamp < pair[1].timestamp));
    }

    #[test]
    fn replay_honors_preexisting_cancellation_cleanly() {
        let fixture = ReplayFixture::new(vec![frame(1), frame(2)]);
        let (sender, receiver) = bounded_frame_channel(2).unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let outcome = fixture.run(&sender, &cancellation).unwrap();

        assert_eq!(outcome.attempted, 0);
        assert!(outcome.cancelled);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn disconnected_consumer_is_observable() {
        let (sender, receiver) = bounded_frame_channel(1).unwrap();
        drop(receiver);

        let error = sender.try_send(frame(1)).unwrap_err();
        assert!(matches!(error, CaptureError::FrameConsumerDisconnected));
    }
}
