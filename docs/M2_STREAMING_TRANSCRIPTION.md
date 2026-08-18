# M2 streaming transcription

M2 adds a provider-independent streaming transcription boundary and an OpenAI Realtime transcription adapter. It does not add meeting storage, question detection, MCP, answer generation, or UI.

The OpenAI adapter converts M1's provider-independent F32LE frames to mono 16-bit PCM at 24 kHz only at the provider boundary, streams them as `input_audio_buffer.append` events, surfaces partial and finalized transcript events, reports provider health/reconnect state, and supports cooperative cancellation. Finalized provider IDs are deduplicated before they can become canonical meeting text.

The deterministic fake provider and protocol fixtures require no audio hardware or API key.

Live provider verification is intentionally separate. With an API key available on a Linux machine, exercise the adapter through the composed daemon path once that path is wired in later milestones. Do not commit keys or captured audio. A missing live credential is deferred verification, not evidence that the provider succeeded.
