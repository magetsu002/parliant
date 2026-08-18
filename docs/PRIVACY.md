# Privacy Model

SIDECAR handles highly sensitive data by definition: live human conversation.

## Defaults

- Raw audio: memory/stream only; never saved to disk.
- Transcript: memory only; bounded rolling retention.
- Logs: metadata only; no full transcript bodies.
- External tools: read-only.
- Network listeners: disabled except explicit provider connections; local IPC is local-only.
- Meeting recording/archive features: out of scope.

## Consent and policy

SIDECAR must not claim that silent processing is legally permissible in every jurisdiction or meeting context. Users are responsible for applicable consent, employer, school, contractual, and platform rules.

The software must not include features whose purpose is to bypass recording indicators, permission prompts, browser controls, or platform restrictions.

## Data minimization

Send the minimum useful context to external providers. Do not repeatedly resend the entire meeting when a bounded recent window and summary are enough.

Long-term personalization/context should be separately configured and should not be copied into meeting transcripts.

## Persistence

Persistence, if added later, requires an explicit ADR covering:

- opt-in UX;
- retention duration;
- encryption at rest;
- deletion semantics;
- backup behavior;
- export behavior;
- crash dumps;
- log redaction.
