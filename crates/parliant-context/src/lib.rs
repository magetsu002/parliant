//! Bounded, memory-only canonical meeting state.

use parliant_core::MonotonicTimestamp;
use parliant_transcribe::{SpeakerMetadata, TranscriptSegment};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeetingSpeaker {
    pub id: String,
    pub label: Option<String>,
}

impl From<SpeakerMetadata> for MeetingSpeaker {
    fn from(value: SpeakerMetadata) -> Self {
        Self {
            id: value.id,
            label: value.label,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeetingSegment {
    pub id: SegmentId,
    pub provider_segment_id: String,
    pub text: String,
    pub start: MonotonicTimestamp,
    pub end: MonotonicTimestamp,
    pub speaker: Option<MeetingSpeaker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeetingStoreConfig {
    pub max_segments: usize,
    pub max_text_bytes: usize,
    pub duplicate_memory: usize,
}

impl Default for MeetingStoreConfig {
    fn default() -> Self {
        Self {
            max_segments: 2_000,
            max_text_bytes: 2 * 1024 * 1024,
            duplicate_memory: 4_000,
        }
    }
}

impl MeetingStoreConfig {
    pub fn validate(self) -> Result<Self, MeetingStateError> {
        if self.max_segments == 0 || self.max_text_bytes == 0 || self.duplicate_memory == 0 {
            return Err(MeetingStateError::InvalidBounds);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeetingStatus {
    pub session_id: u64,
    pub active: bool,
    pub retained_segments: usize,
    pub retained_text_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted(SegmentId),
    Duplicate,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MeetingStateError {
    #[error("meeting store bounds must be greater than zero")]
    InvalidBounds,
    #[error("finalized transcript segment exceeds the configured text-memory bound")]
    SegmentTooLarge,
}

/// Canonical finalized meeting text. No persistence path exists in this type.
#[derive(Debug)]
pub struct MeetingState {
    config: MeetingStoreConfig,
    session_id: u64,
    active: bool,
    next_segment_id: u64,
    retained_text_bytes: usize,
    segments: VecDeque<MeetingSegment>,
    seen_provider_ids: HashSet<String>,
    seen_provider_order: VecDeque<String>,
}

impl MeetingState {
    pub fn new(config: MeetingStoreConfig) -> Result<Self, MeetingStateError> {
        let config = config.validate()?;
        Ok(Self {
            config,
            session_id: 0,
            active: false,
            next_segment_id: 1,
            retained_text_bytes: 0,
            segments: VecDeque::new(),
            seen_provider_ids: HashSet::new(),
            seen_provider_order: VecDeque::new(),
        })
    }

    pub fn start_session(&mut self) -> u64 {
        self.session_id = self.session_id.saturating_add(1);
        self.active = true;
        self.clear_session_state();
        self.session_id
    }

    pub fn stop_session(&mut self) {
        self.active = false;
    }

    pub fn reset(&mut self) {
        self.active = false;
        self.clear_session_state();
    }

    pub fn ingest_final(
        &mut self,
        segment: TranscriptSegment,
    ) -> Result<InsertOutcome, MeetingStateError> {
        if self
            .seen_provider_ids
            .contains(&segment.provider_segment_id)
        {
            return Ok(InsertOutcome::Duplicate);
        }
        let text_bytes = segment.text.len();
        if text_bytes > self.config.max_text_bytes {
            return Err(MeetingStateError::SegmentTooLarge);
        }

        self.remember_provider_id(segment.provider_segment_id.clone());
        let id = SegmentId(self.next_segment_id);
        self.next_segment_id = self.next_segment_id.saturating_add(1);
        self.retained_text_bytes = self.retained_text_bytes.saturating_add(text_bytes);
        self.segments.push_back(MeetingSegment {
            id,
            provider_segment_id: segment.provider_segment_id,
            text: segment.text,
            start: segment.start,
            end: segment.end,
            speaker: segment.speaker.map(Into::into),
        });
        self.enforce_bounds();
        Ok(InsertOutcome::Inserted(id))
    }

    pub fn get_segment(&self, id: SegmentId) -> Option<&MeetingSegment> {
        self.segments.iter().find(|segment| segment.id == id)
    }

    pub fn recent(&self, limit: usize) -> Vec<MeetingSegment> {
        if limit == 0 {
            return Vec::new();
        }
        let start = self.segments.len().saturating_sub(limit);
        self.segments.iter().skip(start).cloned().collect()
    }

    pub fn search(&self, query: &str, limit: usize) -> Vec<MeetingSegment> {
        let query = query.trim().to_lowercase();
        if query.is_empty() || limit == 0 {
            return Vec::new();
        }
        self.segments
            .iter()
            .rev()
            .filter(|segment| segment.text.to_lowercase().contains(&query))
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub fn speakers(&self) -> Vec<MeetingSpeaker> {
        let mut seen = HashSet::new();
        let mut speakers = Vec::new();
        for segment in &self.segments {
            if let Some(speaker) = &segment.speaker {
                if seen.insert(speaker.id.clone()) {
                    speakers.push(speaker.clone());
                }
            }
        }
        speakers
    }

    pub fn status(&self) -> MeetingStatus {
        MeetingStatus {
            session_id: self.session_id,
            active: self.active,
            retained_segments: self.segments.len(),
            retained_text_bytes: self.retained_text_bytes,
        }
    }

    pub fn retained_segments(&self) -> &VecDeque<MeetingSegment> {
        &self.segments
    }

    fn clear_session_state(&mut self) {
        self.next_segment_id = 1;
        self.retained_text_bytes = 0;
        self.segments.clear();
        self.seen_provider_ids.clear();
        self.seen_provider_order.clear();
    }

    fn remember_provider_id(&mut self, provider_id: String) {
        self.seen_provider_ids.insert(provider_id.clone());
        self.seen_provider_order.push_back(provider_id);
        while self.seen_provider_order.len() > self.config.duplicate_memory {
            if let Some(oldest) = self.seen_provider_order.pop_front() {
                self.seen_provider_ids.remove(&oldest);
            }
        }
    }

    fn enforce_bounds(&mut self) {
        while self.segments.len() > self.config.max_segments
            || self.retained_text_bytes > self.config.max_text_bytes
        {
            let Some(evicted) = self.segments.pop_front() else {
                break;
            };
            self.retained_text_bytes = self.retained_text_bytes.saturating_sub(evicted.text.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transcript(id: &str, text: &str, speaker: Option<&str>) -> TranscriptSegment {
        TranscriptSegment::new(
            id,
            text,
            MonotonicTimestamp::from_nanos(10),
            MonotonicTimestamp::from_nanos(20),
            speaker.map(|id| SpeakerMetadata {
                id: id.to_string(),
                label: Some(id.to_string()),
            }),
        )
        .unwrap()
    }

    fn state(max_segments: usize, max_text_bytes: usize) -> MeetingState {
        MeetingState::new(MeetingStoreConfig {
            max_segments,
            max_text_bytes,
            duplicate_memory: max_segments.max(1) * 2,
        })
        .unwrap()
    }

    #[test]
    fn finalized_segments_receive_stable_ordered_ids() {
        let mut state = state(10, 1024);
        state.start_session();
        assert_eq!(
            state.ingest_final(transcript("p1", "one", None)).unwrap(),
            InsertOutcome::Inserted(SegmentId(1))
        );
        assert_eq!(
            state.ingest_final(transcript("p2", "two", None)).unwrap(),
            InsertOutcome::Inserted(SegmentId(2))
        );
        assert_eq!(state.get_segment(SegmentId(2)).unwrap().text, "two");
    }

    #[test]
    fn provider_duplicates_do_not_create_canonical_segments() {
        let mut state = state(10, 1024);
        state.start_session();
        state
            .ingest_final(transcript("same", "first", None))
            .unwrap();
        assert_eq!(
            state
                .ingest_final(transcript("same", "duplicate", None))
                .unwrap(),
            InsertOutcome::Duplicate
        );
        assert_eq!(state.status().retained_segments, 1);
    }

    #[test]
    fn retention_is_bounded_by_count_and_bytes() {
        let mut state = state(2, 8);
        state.start_session();
        state.ingest_final(transcript("1", "aaaa", None)).unwrap();
        state.ingest_final(transcript("2", "bbbb", None)).unwrap();
        state.ingest_final(transcript("3", "cc", None)).unwrap();
        assert_eq!(state.status().retained_segments, 2);
        assert!(state.status().retained_text_bytes <= 8);
        assert!(state.get_segment(SegmentId(1)).is_none());
        assert_eq!(state.recent(2)[0].text, "bbbb");
    }

    #[test]
    fn recent_search_and_speakers_are_bounded_views() {
        let mut state = state(10, 1024);
        state.start_session();
        state
            .ingest_final(transcript("1", "deployment is Friday", Some("Sarah")))
            .unwrap();
        state
            .ingest_final(transcript("2", "other topic", Some("Alex")))
            .unwrap();
        state
            .ingest_final(transcript("3", "deployment rollback", Some("Sarah")))
            .unwrap();
        assert_eq!(state.recent(1)[0].provider_segment_id, "3");
        let results = state.search("DEPLOYMENT", 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].provider_segment_id, "3");
        assert_eq!(state.speakers().len(), 2);
    }

    #[test]
    fn session_reset_clears_text_and_restarts_segment_ids() {
        let mut state = state(10, 1024);
        let first_session = state.start_session();
        state
            .ingest_final(transcript("1", "secret meeting text", None))
            .unwrap();
        state.stop_session();
        let second_session = state.start_session();
        assert!(second_session > first_session);
        assert!(state.retained_segments().is_empty());
        assert_eq!(
            state
                .ingest_final(transcript("1", "new meeting", None))
                .unwrap(),
            InsertOutcome::Inserted(SegmentId(1))
        );
    }

    #[test]
    fn oversized_segment_is_rejected_without_breaking_memory_bound() {
        let mut state = state(10, 4);
        state.start_session();
        assert_eq!(
            state.ingest_final(transcript("huge", "12345", None)),
            Err(MeetingStateError::SegmentTooLarge)
        );
        assert_eq!(state.status().retained_text_bytes, 0);
    }
}
