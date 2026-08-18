//! Domain types shared by SIDECAR's implemented vertical slices.

/// The sample representation carried by an [`AudioFrame`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleFormat {
    /// Interleaved 32-bit little-endian IEEE-754 floating point samples.
    F32Le,
}

impl SampleFormat {
    pub const fn bytes_per_sample(self) -> usize {
        match self {
            Self::F32Le => size_of::<f32>(),
        }
    }
}

/// Provider-independent metadata describing the bytes in an audio frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioFormat {
    pub sample_rate_hz: u32,
    pub channels: u32,
    pub sample_format: SampleFormat,
}

impl AudioFormat {
    pub const fn new(sample_rate_hz: u32, channels: u32, sample_format: SampleFormat) -> Self {
        Self {
            sample_rate_hz,
            channels,
            sample_format,
        }
    }
}

/// Timestamp measured from the start of the current capture session using a monotonic clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonotonicTimestamp {
    pub nanos_since_start: u64,
}

impl MonotonicTimestamp {
    pub const ZERO: Self = Self {
        nanos_since_start: 0,
    };

    pub const fn from_nanos(nanos_since_start: u64) -> Self {
        Self { nanos_since_start }
    }
}

/// One normalized, owned audio frame.
///
/// The bytes are intentionally opaque to later subsystems; `format` is authoritative for
/// interpreting them. M1 keeps these frames in memory only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioFrame {
    pub sequence: u64,
    pub timestamp: MonotonicTimestamp,
    pub format: AudioFormat,
    pub data: Vec<u8>,
}

impl AudioFrame {
    pub fn new(
        sequence: u64,
        timestamp: MonotonicTimestamp,
        format: AudioFormat,
        data: Vec<u8>,
    ) -> Self {
        Self {
            sequence,
            timestamp,
            format,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audio_frame_carries_format_and_monotonic_timestamp() {
        let format = AudioFormat::new(48_000, 2, SampleFormat::F32Le);
        let frame = AudioFrame::new(
            7,
            MonotonicTimestamp::from_nanos(42),
            format,
            vec![0, 1, 2, 3],
        );

        assert_eq!(frame.sequence, 7);
        assert_eq!(frame.timestamp.nanos_since_start, 42);
        assert_eq!(frame.format, format);
        assert_eq!(frame.format.sample_format.bytes_per_sample(), 4);
    }
}
