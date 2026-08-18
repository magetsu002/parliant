# M4 question detection

M4 adds an event-driven question gate over finalized meeting segments. A cheap deterministic candidate check runs first. Ambiguous classification sits behind a replaceable semantic-classifier trait. The detector then applies addressed-to-user confidence, incomplete-question joining, duplicate normalization/suppression, and a segment-based cooldown before emitting a normalized `QuestionEvent`.

Manual triggering remains available independently of automatic detection. The answer model is not invoked continuously.

The deterministic synthetic evaluation corpus covers 60 positive/negative utterances and separately asserts duplicate-trigger suppression. It is a regression corpus, not a claim of real-world semantic accuracy; live meeting evaluation remains a later runtime task.
