# SIDECAR

**Intelligence beside you.**

SIDECAR is a local-first meeting intelligence sidecar. It captures meeting audio from the user's Linux desktop, produces a live transcript, detects when a question is likely addressed to the user, retrieves relevant context, and surfaces a private suggested answer without joining the meeting as a bot.

## Product contract

SIDECAR v1 is intentionally narrow:

- Linux/Arch first.
- Capture local meeting playback through PipeWire.
- Transcribe continuously with a swappable transcription provider.
- Keep a rolling transcript and meeting state locally.
- Detect likely questions before invoking the expensive answer model.
- Let the answer model retrieve meeting context through a read-only MCP server.
- Show suggestions privately in a local overlay.
- Never speak into the meeting automatically.
- Never persist raw audio by default.
- Never expose mutation-capable MCP tools in v1.

This is **not** a meeting recorder, autonomous participant, or general meeting-management suite.

## Intended flow

```text
Meeting app / browser
        |
        v
PipeWire capture
        |
        v
Audio framing + VAD
        |
        v
Streaming transcription
        |
        v
Rolling transcript store
        |
        +-------------------> read-only MCP server
        |                           ^
        v                           |
Question detector                   |
        |                           |
        v                           |
Answer orchestrator ----------------+
        |
        v
Private local overlay
```

## Repository status

This repository is an engineering bootstrap, not a finished implementation. The architecture, boundaries, event contracts, acceptance criteria, privacy requirements, and first implementation milestone are defined so an engineer can begin without inventing product semantics.

Start with:

1. [`AGENTS.md`](./AGENTS.md)
2. [`docs/ENGINEER_HANDOFF.md`](./docs/ENGINEER_HANDOFF.md)
3. [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)
4. [`spec/events.schema.json`](./spec/events.schema.json)
5. [`docs/MILESTONES.md`](./docs/MILESTONES.md)

## Non-goals for v1

- Windows or macOS support.
- A Zoom/Meet/Teams bot account.
- Automatic spoken responses.
- Long-term cloud transcript storage.
- Meeting analytics dashboards.
- CRM actions, email sending, issue creation, or other write-capable tools.
- Webcam/video understanding.
- Attempting to bypass OS or meeting-platform privacy/recording controls.

## Development philosophy

Prefer a small, inspectable local daemon and explicit interfaces over a monolithic desktop app. Each milestone must have deterministic fixture-based tests so audio hardware and paid APIs are not required for most CI coverage.

## External technical references

- OpenAI Realtime API: https://platform.openai.com/docs/api-reference/realtime
- OpenAI audio transcription API: https://platform.openai.com/docs/api-reference/audio
- PipeWire `pw-cat` / `pw-record`: https://docs.pipewire.org/page_man_pw-cat_1.html
- PipeWire stream capture properties: https://docs.pipewire.org/devel/group__pw__keys.html

## License

No open-source license has been granted yet. Treat the repository as all-rights-reserved until the owner chooses a license.
