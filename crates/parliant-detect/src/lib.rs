//! Event-driven question detection over finalized meeting segments.

use serde::{Deserialize, Serialize};
use parliant_context::{MeetingSegment, SegmentId};
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SemanticAssessment {
    pub is_question: bool,
    pub addressed_to_user_confidence: u8,
    pub complete: bool,
}

pub trait SemanticClassifier: Send + Sync {
    fn classify(&self, text: &str) -> SemanticAssessment;
}

/// Deterministic classifier used for fixture evaluation and as the no-model fallback. The trait
/// boundary permits a later semantic implementation without coupling the trigger loop to it.
#[derive(Debug, Default, Clone, Copy)]
pub struct HeuristicSemanticClassifier;

impl SemanticClassifier for HeuristicSemanticClassifier {
    fn classify(&self, text: &str) -> SemanticAssessment {
        let normalized = text.trim().to_lowercase();
        let is_question = candidate_check(&normalized);
        let addressed_to_user_confidence =
            if contains_word(&normalized, "you") || contains_word(&normalized, "your") {
                95
            } else if contains_word(&normalized, "we") || contains_word(&normalized, "our") {
                82
            } else if is_question {
                72
            } else {
                0
            };
        let incomplete_suffixes = [
            "can you",
            "could you",
            "would you",
            "will you",
            "what about",
            "how do we",
            "when will",
            "where is",
            "do you",
        ];
        let complete = !incomplete_suffixes
            .iter()
            .any(|suffix| normalized.ends_with(suffix))
            && !normalized.ends_with(',');
        SemanticAssessment {
            is_question,
            addressed_to_user_confidence,
            complete,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuestionDetectorConfig {
    pub minimum_addressed_confidence: u8,
    pub cooldown_segments: u64,
    pub duplicate_memory: usize,
}

impl Default for QuestionDetectorConfig {
    fn default() -> Self {
        Self {
            minimum_addressed_confidence: 65,
            cooldown_segments: 1,
            duplicate_memory: 64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuestionTrigger {
    Automatic,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionEvent {
    pub id: u64,
    pub text: String,
    pub confidence: u8,
    pub source_segment_ids: Vec<SegmentId>,
    pub trigger: QuestionTrigger,
}

#[derive(Debug, Clone)]
struct PendingQuestion {
    text: String,
    source_segment_ids: Vec<SegmentId>,
}

pub struct QuestionDetector<C> {
    classifier: C,
    config: QuestionDetectorConfig,
    next_question_id: u64,
    last_trigger_segment: Option<SegmentId>,
    recent_normalized: HashSet<String>,
    recent_order: VecDeque<String>,
    pending: Option<PendingQuestion>,
}

impl<C: SemanticClassifier> QuestionDetector<C> {
    pub fn new(classifier: C, config: QuestionDetectorConfig) -> Self {
        Self {
            classifier,
            config,
            next_question_id: 1,
            last_trigger_segment: None,
            recent_normalized: HashSet::new(),
            recent_order: VecDeque::new(),
            pending: None,
        }
    }

    pub fn process_finalized(&mut self, segment: &MeetingSegment) -> Option<QuestionEvent> {
        let (text, source_segment_ids) = if let Some(mut pending) = self.pending.take() {
            pending.text.push(' ');
            pending.text.push_str(segment.text.trim());
            pending.source_segment_ids.push(segment.id);
            (pending.text, pending.source_segment_ids)
        } else {
            if !candidate_check(&segment.text) {
                return None;
            }
            (segment.text.trim().to_string(), vec![segment.id])
        };

        let assessment = self.classifier.classify(&text);
        if !assessment.is_question {
            return None;
        }
        if !assessment.complete {
            self.pending = Some(PendingQuestion {
                text,
                source_segment_ids,
            });
            return None;
        }
        if assessment.addressed_to_user_confidence < self.config.minimum_addressed_confidence {
            return None;
        }

        let normalized = normalize_question(&text);
        if normalized.is_empty() || self.recent_normalized.contains(&normalized) {
            return None;
        }
        if let Some(last) = self.last_trigger_segment {
            if segment.id.0.saturating_sub(last.0) <= self.config.cooldown_segments {
                return None;
            }
        }

        self.remember(normalized);
        self.last_trigger_segment = Some(segment.id);
        Some(self.build_event(
            text,
            assessment.addressed_to_user_confidence,
            source_segment_ids,
            QuestionTrigger::Automatic,
        ))
    }

    pub fn manual_trigger(
        &mut self,
        text: impl Into<String>,
        source_segment_ids: Vec<SegmentId>,
    ) -> Option<QuestionEvent> {
        let text = text.into();
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        Some(self.build_event(
            text.to_string(),
            100,
            source_segment_ids,
            QuestionTrigger::Manual,
        ))
    }

    pub fn reset(&mut self) {
        self.last_trigger_segment = None;
        self.recent_normalized.clear();
        self.recent_order.clear();
        self.pending = None;
    }

    fn remember(&mut self, normalized: String) {
        if self.config.duplicate_memory == 0 {
            return;
        }
        self.recent_normalized.insert(normalized.clone());
        self.recent_order.push_back(normalized);
        while self.recent_order.len() > self.config.duplicate_memory {
            if let Some(oldest) = self.recent_order.pop_front() {
                self.recent_normalized.remove(&oldest);
            }
        }
    }

    fn build_event(
        &mut self,
        text: String,
        confidence: u8,
        source_segment_ids: Vec<SegmentId>,
        trigger: QuestionTrigger,
    ) -> QuestionEvent {
        let id = self.next_question_id;
        self.next_question_id = self.next_question_id.saturating_add(1);
        QuestionEvent {
            id,
            text,
            confidence,
            source_segment_ids,
            trigger,
        }
    }
}

pub fn candidate_check(text: &str) -> bool {
    let normalized = text.trim().to_lowercase();
    if normalized.is_empty() {
        return false;
    }
    if normalized.ends_with('?') {
        return true;
    }
    const PREFIXES: &[&str] = &[
        "what ",
        "why ",
        "when ",
        "where ",
        "who ",
        "which ",
        "how ",
        "can ",
        "could ",
        "would ",
        "will ",
        "do ",
        "does ",
        "did ",
        "is ",
        "are ",
        "was ",
        "were ",
        "should ",
        "may ",
        "tell me ",
        "walk me through ",
        "explain ",
        "give me ",
    ];
    PREFIXES.iter().any(|prefix| normalized.starts_with(prefix))
}

pub fn normalize_question(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_alphanumeric() || character.is_whitespace() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn contains_word(text: &str, word: &str) -> bool {
    text.split(|character: char| !character.is_alphanumeric())
        .any(|part| part == word)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parliant_context::MeetingSpeaker;
    use parliant_core::MonotonicTimestamp;

    fn segment(id: u64, text: &str) -> MeetingSegment {
        MeetingSegment {
            id: SegmentId(id),
            provider_segment_id: format!("p{id}"),
            text: text.to_string(),
            start: MonotonicTimestamp::from_nanos(id * 10),
            end: MonotonicTimestamp::from_nanos(id * 10 + 5),
            speaker: Some(MeetingSpeaker {
                id: "speaker".to_string(),
                label: None,
            }),
        }
    }

    fn detector() -> QuestionDetector<HeuristicSemanticClassifier> {
        QuestionDetector::new(
            HeuristicSemanticClassifier,
            QuestionDetectorConfig {
                cooldown_segments: 0,
                ..QuestionDetectorConfig::default()
            },
        )
    }

    #[test]
    fn candidate_gate_rejects_plain_statements() {
        assert!(!candidate_check("deployment is Friday"));
        assert!(candidate_check("What is the deployment date?"));
        assert!(candidate_check("Tell me the rollback plan"));
    }

    #[test]
    fn duplicate_questions_are_suppressed_after_normalization() {
        let mut detector = detector();
        assert!(detector
            .process_finalized(&segment(1, "Can you explain the deploy plan?"))
            .is_some());
        assert!(detector
            .process_finalized(&segment(2, "can you explain the deploy plan"))
            .is_none());
    }

    #[test]
    fn incomplete_question_is_joined_with_the_next_finalized_segment() {
        let mut detector = detector();
        assert!(detector
            .process_finalized(&segment(1, "Could you"))
            .is_none());
        let event = detector
            .process_finalized(&segment(2, "explain why staging failed?"))
            .unwrap();
        assert_eq!(event.source_segment_ids, vec![SegmentId(1), SegmentId(2)]);
        assert_eq!(event.text, "Could you explain why staging failed?");
    }

    #[test]
    fn cooldown_prevents_back_to_back_automatic_triggers() {
        let mut detector = QuestionDetector::new(
            HeuristicSemanticClassifier,
            QuestionDetectorConfig {
                cooldown_segments: 1,
                ..QuestionDetectorConfig::default()
            },
        );
        assert!(detector
            .process_finalized(&segment(10, "What should we deploy?"))
            .is_some());
        assert!(detector
            .process_finalized(&segment(11, "When should we deploy?"))
            .is_none());
        assert!(detector
            .process_finalized(&segment(12, "Where should we deploy?"))
            .is_some());
    }

    #[test]
    fn manual_trigger_is_always_available_for_non_empty_text() {
        let mut detector = detector();
        let event = detector
            .manual_trigger("summarize the decision", vec![SegmentId(9)])
            .unwrap();
        assert_eq!(event.trigger, QuestionTrigger::Manual);
        assert_eq!(event.confidence, 100);
    }

    #[test]
    fn synthetic_corpus_reports_precision_recall_and_zero_duplicate_triggers() {
        let positives = [
            "What is the deadline?",
            "Why did the build fail?",
            "When are we shipping?",
            "Where is the runbook?",
            "Who owns the incident?",
            "Which branch should we use?",
            "How do we roll this back?",
            "Can you explain the cache issue?",
            "Could you review the logs?",
            "Would you take this action item?",
            "Will you send the report?",
            "Do you know the current status?",
            "Does this affect your service?",
            "Did we verify production?",
            "Is the migration reversible?",
            "Are we ready to release?",
            "Was the timeout expected?",
            "Were the tests green?",
            "Should we retry now?",
            "Tell me the safest rollback plan",
            "Walk me through your reasoning",
            "Explain why our deploy stalled",
            "Give me the current risk assessment",
            "What did Sarah say about deployment?",
            "How should I answer the client?",
            "Can we ship this today?",
            "Why are our workers restarting?",
            "Where should we store the artifact?",
            "Who can approve this change?",
            "When will your patch be ready?",
        ];
        let negatives = [
            "The deadline is Friday.",
            "Deployment completed successfully.",
            "Sarah owns the incident.",
            "We should retry after lunch.",
            "The migration is reversible.",
            "I reviewed the logs.",
            "Production is healthy.",
            "The cache was flushed.",
            "This affects the API service.",
            "The tests were green.",
            "Please note the current status in the document.",
            "Our workers restarted twice.",
            "There is no action item for you.",
            "The report will be sent tomorrow.",
            "We discussed the rollback plan.",
            "Sarah asked Alex to deploy.",
            "The build failed because disk was full.",
            "This is the release branch.",
            "I know the current status.",
            "The client expects an answer today.",
            "We can ship this today.",
            "Staging recovered ten minutes ago.",
            "The artifact belongs in object storage.",
            "Approval comes from the release manager.",
            "The patch will be ready tonight.",
            "Nobody asked for a summary.",
            "Rollback instructions are in the runbook.",
            "The incident is already resolved.",
            "Alex explained the cache issue.",
            "No question was directed at the user.",
        ];

        let classifier = HeuristicSemanticClassifier;
        let mut true_positives = 0;
        let mut false_negatives = 0;
        let mut false_positives = 0;
        for text in positives {
            let assessment = classifier.classify(text);
            if assessment.is_question && assessment.addressed_to_user_confidence >= 65 {
                true_positives += 1;
            } else {
                false_negatives += 1;
            }
        }
        for text in negatives {
            let assessment = classifier.classify(text);
            if assessment.is_question && assessment.addressed_to_user_confidence >= 65 {
                false_positives += 1;
            }
        }

        let mut duplicate_detector = detector();
        let first = duplicate_detector
            .process_finalized(&segment(100, "What is the deadline?"))
            .is_some();
        let duplicate = duplicate_detector
            .process_finalized(&segment(101, "WHAT is the deadline!!!"))
            .is_some();
        let duplicate_triggers = usize::from(first) + usize::from(duplicate) - 1;

        assert_eq!(true_positives, positives.len());
        assert_eq!(false_negatives, 0);
        assert_eq!(false_positives, 0);
        assert_eq!(duplicate_triggers, 0);
    }
}
