//! Provider-independent streaming transcription with an OpenAI Realtime adapter.
//!
//! Provider-specific wire events and the 24 kHz PCM conversion stay inside this crate. Only
//! finalized segments are eligible for canonical meeting state.

use sidecar_audio::CancellationToken;
use sidecar_core::{AudioFrame, MonotonicTimestamp, SampleFormat};
use std::collections::{HashSet, VecDeque};
use std::sync::mpsc;
use std::time::Duration;
use thiserror::Error;

mod openai;
pub use openai::{OpenAiRealtimeConfig, OpenAiRealtimeProvider};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeakerMetadata {
    pub id: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptSegment {
    pub provider_segment_id: String,
    pub text: String,
    pub start: MonotonicTimestamp,
    pub end: MonotonicTimestamp,
    pub speaker: Option<SpeakerMetadata>,
}

impl TranscriptSegment {
    pub fn new(
        provider_segment_id: impl Into<String>,
        text: impl Into<String>,
        start: MonotonicTimestamp,
        end: MonotonicTimestamp,
        speaker: Option<SpeakerMetadata>,
    ) -> Result<Self, TranscriptionError> {
        let provider_segment_id = provider_segment_id.into();
        let text = text.into();
        if provider_segment_id.trim().is_empty() {
            return Err(TranscriptionError::InvalidSegment(
                "provider segment id must not be empty".to_string(),
            ));
        }
        if text.trim().is_empty() {
            return Err(TranscriptionError::InvalidSegment(
                "final transcript text must not be empty".to_string(),
            ));
        }
        if end < start {
            return Err(TranscriptionError::InvalidSegment(
                "segment end precedes segment start".to_string(),
            ));
        }
        Ok(Self {
            provider_segment_id,
            text,
            start,
            end,
            speaker,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderHealth {
    Connecting,
    Healthy,
    Reconnecting { attempt: u32 },
    Degraded(String),
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptionEvent {
    Partial {
        provider_segment_id: String,
        text: String,
        speaker: Option<SpeakerMetadata>,
    },
    Final(TranscriptSegment),
    Health(ProviderHealth),
    Error(String),
}

#[derive(Debug, Error)]
pub enum TranscriptionError {
    #[error("invalid transcript segment: {0}")]
    InvalidSegment(String),
    #[error("transcription audio queue is full")]
    AudioBackpressure,
    #[error("transcription session is closed")]
    SessionClosed,
    #[error("transcription provider error: {0}")]
    Provider(String),
    #[error("transcription protocol error: {0}")]
    Protocol(String),
}

pub trait TranscriptionSession: Send {
    fn push_audio(&self, frame: AudioFrame) -> Result<(), TranscriptionError>;
    fn recv_event_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<TranscriptionEvent>, TranscriptionError>;
    fn cancel(&self);
}

pub trait TranscriptionProvider: Send + Sync {
    fn connect(&self) -> Result<Box<dyn TranscriptionSession>, TranscriptionError>;
}

/// Canonicalization gate used before meeting state. Partials are deliberately replaceable and
/// never enter `finalized`.
#[derive(Debug, Default)]
pub struct TranscriptAccumulator {
    finalized: Vec<TranscriptSegment>,
    seen_provider_ids: HashSet<String>,
}

impl TranscriptAccumulator {
    pub fn apply(&mut self, event: TranscriptionEvent) -> bool {
        let TranscriptionEvent::Final(segment) = event else {
            return false;
        };
        if !self
            .seen_provider_ids
            .insert(segment.provider_segment_id.clone())
        {
            return false;
        }
        self.finalized.push(segment);
        true
    }

    pub fn finalized(&self) -> &[TranscriptSegment] {
        &self.finalized
    }
}

/// Deterministic provider fixture. Events are returned in the supplied order and do not require
/// audio hardware, network access, or paid APIs.
#[derive(Debug, Clone)]
pub struct FakeTranscriptionProvider {
    events: Vec<TranscriptionEvent>,
}

impl FakeTranscriptionProvider {
    pub fn new(events: Vec<TranscriptionEvent>) -> Self {
        Self { events }
    }
}

impl TranscriptionProvider for FakeTranscriptionProvider {
    fn connect(&self) -> Result<Box<dyn TranscriptionSession>, TranscriptionError> {
        Ok(Box::new(FakeSession {
            events: std::sync::Mutex::new(self.events.clone().into()),
            cancellation: CancellationToken::new(),
        }))
    }
}

struct FakeSession {
    events: std::sync::Mutex<VecDeque<TranscriptionEvent>>,
    cancellation: CancellationToken,
}

impl TranscriptionSession for FakeSession {
    fn push_audio(&self, _frame: AudioFrame) -> Result<(), TranscriptionError> {
        if self.cancellation.is_cancelled() {
            return Err(TranscriptionError::SessionClosed);
        }
        Ok(())
    }

    fn recv_event_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<TranscriptionEvent>, TranscriptionError> {
        if self.cancellation.is_cancelled() {
            return Ok(Some(TranscriptionEvent::Health(ProviderHealth::Stopped)));
        }
        if let Some(event) = self
            .events
            .lock()
            .map_err(|_| TranscriptionError::SessionClosed)?
            .pop_front()
        {
            return Ok(Some(event));
        }
        if !timeout.is_zero() {
            std::thread::sleep(timeout.min(Duration::from_millis(2)));
        }
        Ok(None)
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }
}

pub(crate) struct ChannelSession {
    pub audio_tx: tokio::sync::mpsc::Sender<AudioFrame>,
    pub events_rx: std::sync::Mutex<mpsc::Receiver<TranscriptionEvent>>,
    pub cancellation: CancellationToken,
}

impl TranscriptionSession for ChannelSession {
    fn push_audio(&self, frame: AudioFrame) -> Result<(), TranscriptionError> {
        if self.cancellation.is_cancelled() {
            return Err(TranscriptionError::SessionClosed);
        }
        self.audio_tx.try_send(frame).map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                TranscriptionError::AudioBackpressure
            }
            tokio::sync::mpsc::error::TrySendError::Closed(_) => TranscriptionError::SessionClosed,
        })
    }

    fn recv_event_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<TranscriptionEvent>, TranscriptionError> {
        let rx = self
            .events_rx
            .lock()
            .map_err(|_| TranscriptionError::SessionClosed)?;
        match rx.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(TranscriptionError::SessionClosed),
        }
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }
}

pub(crate) fn f32le_to_pcm16_mono_24khz(frame: &AudioFrame) -> Result<Vec<u8>, TranscriptionError> {
    if frame.format.sample_format != SampleFormat::F32Le {
        return Err(TranscriptionError::Provider(
            "OpenAI adapter requires F32LE input from the capture boundary".to_string(),
        ));
    }
    let channels = usize::try_from(frame.format.channels)
        .map_err(|_| TranscriptionError::Provider("invalid channel count".to_string()))?;
    if channels == 0 || frame.format.sample_rate_hz == 0 {
        return Err(TranscriptionError::Provider(
            "audio format rate/channels must be non-zero".to_string(),
        ));
    }
    let bytes_per_interleaved_frame = channels
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| TranscriptionError::Provider("audio frame width overflow".to_string()))?;
    if frame.data.len() % bytes_per_interleaved_frame != 0 {
        return Err(TranscriptionError::Provider(
            "audio payload is not aligned to its declared format".to_string(),
        ));
    }

    let input_samples = frame.data.len() / bytes_per_interleaved_frame;
    if input_samples == 0 {
        return Ok(Vec::new());
    }
    let mut mono = Vec::with_capacity(input_samples);
    for index in 0..input_samples {
        let mut sum = 0.0_f32;
        for channel in 0..channels {
            let start = (index * channels + channel) * 4;
            let sample = f32::from_le_bytes([
                frame.data[start],
                frame.data[start + 1],
                frame.data[start + 2],
                frame.data[start + 3],
            ]);
            sum += sample;
        }
        mono.push((sum / channels as f32).clamp(-1.0, 1.0));
    }

    let output_samples =
        ((input_samples as u128 * 24_000_u128) / u128::from(frame.format.sample_rate_hz)) as usize;
    if output_samples == 0 {
        return Ok(Vec::new());
    }
    let mut output = Vec::with_capacity(output_samples * 2);
    for out_index in 0..output_samples {
        let source_position = out_index as f64 * frame.format.sample_rate_hz as f64 / 24_000_f64;
        let left = source_position.floor() as usize;
        let right = (left + 1).min(mono.len() - 1);
        let fraction = (source_position - left as f64) as f32;
        let sample = mono[left.min(mono.len() - 1)] * (1.0 - fraction) + mono[right] * fraction;
        let pcm = if sample <= -1.0 {
            i16::MIN
        } else {
            (sample * i16::MAX as f32).round() as i16
        };
        output.extend_from_slice(&pcm.to_le_bytes());
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sidecar_core::{AudioFormat, SampleFormat};

    fn segment(id: &str, text: &str) -> TranscriptSegment {
        TranscriptSegment::new(
            id,
            text,
            MonotonicTimestamp::from_nanos(10),
            MonotonicTimestamp::from_nanos(20),
            None,
        )
        .unwrap()
    }

    #[test]
    fn only_finalized_segments_become_canonical_and_duplicates_are_rejected() {
        let mut accumulator = TranscriptAccumulator::default();
        assert!(!accumulator.apply(TranscriptionEvent::Partial {
            provider_segment_id: "a".into(),
            text: "hel".into(),
            speaker: None,
        }));
        assert!(accumulator.apply(TranscriptionEvent::Final(segment("a", "hello"))));
        assert!(!accumulator.apply(TranscriptionEvent::Final(segment("a", "hello again"))));
        assert_eq!(accumulator.finalized().len(), 1);
        assert_eq!(accumulator.finalized()[0].text, "hello");
    }

    #[test]
    fn fake_provider_is_deterministic_and_cancellable() {
        let provider = FakeTranscriptionProvider::new(vec![
            TranscriptionEvent::Health(ProviderHealth::Healthy),
            TranscriptionEvent::Final(segment("s1", "done")),
        ]);
        let session = provider.connect().unwrap();
        assert_eq!(
            session.recv_event_timeout(Duration::ZERO).unwrap(),
            Some(TranscriptionEvent::Health(ProviderHealth::Healthy))
        );
        assert_eq!(
            session.recv_event_timeout(Duration::ZERO).unwrap(),
            Some(TranscriptionEvent::Final(segment("s1", "done")))
        );
        session.cancel();
        assert!(matches!(
            session.recv_event_timeout(Duration::ZERO).unwrap(),
            Some(TranscriptionEvent::Health(ProviderHealth::Stopped))
        ));
    }

    #[test]
    fn openai_audio_adapter_downmixes_and_resamples_at_provider_boundary() {
        let mut bytes = Vec::new();
        for _ in 0..480 {
            bytes.extend_from_slice(&0.5_f32.to_le_bytes());
            bytes.extend_from_slice(&0.5_f32.to_le_bytes());
        }
        let frame = AudioFrame::new(
            0,
            MonotonicTimestamp::ZERO,
            AudioFormat::new(48_000, 2, SampleFormat::F32Le),
            bytes,
        );
        let pcm = f32le_to_pcm16_mono_24khz(&frame).unwrap();
        assert_eq!(pcm.len(), 240 * 2);
        let first = i16::from_le_bytes([pcm[0], pcm[1]]);
        assert!((16_000..=16_500).contains(&first));
    }

    #[test]
    fn invalid_final_segment_is_rejected() {
        assert!(TranscriptSegment::new(
            "x",
            " ",
            MonotonicTimestamp::ZERO,
            MonotonicTimestamp::ZERO,
            None,
        )
        .is_err());
    }
}
