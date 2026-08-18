# Privacy and security model

PARLIANT V1 is local-first and intentionally minimizes persistent and remote meeting data.

## Data that stays memory-only by default

- captured raw audio frames;
- partial transcription events;
- canonical finalized meeting segments;
- detected question state;
- streamed private answer suggestions.

The V1 source contains no raw-audio writer and no transcript persistence backend. Restarting/stopping the daemon discards meeting state.

## Logging

Runtime diagnostics are metadata-only. They may include selected PipeWire target identity, negotiated audio format, frame/byte/drop counts, provider health state, finalized/duplicate/question counts, error category, and latency counters. The daemon does not intentionally log raw audio, transcript text, suggested answer text, API keys, bearer tokens, or Authorization headers.

Provider-specific error text is converted to generic user-visible degraded/error messages at the runtime/overlay boundary so a provider response cannot become a credential or transcript leak through the UI.

## Local overlay boundary

The overlay communicates through a versioned Unix-domain socket. PARLIANT uses owner-only socket permissions, validates the peer UID with `SO_PEERCRED`, bounds message size and per-client queues, sends a bounded snapshot after reconnect, and disconnects slow consumers instead of allowing unbounded memory growth.

The overlay receives only current UI state. It does not receive raw audio, credentials, or full transcript history. The only daemon actions accepted from the overlay are dismiss and stop listening. Clipboard copy is performed locally by the overlay and is not sent back to the daemon.

## MCP boundary

Meeting transcript is treated as untrusted content. Spoken or transcribed prompt injection cannot grant tool permissions, add tools, make MCP write-capable, or expose credentials.

The V1 meeting MCP catalogue is read-only and bounded. The opt-in remote bridge adds a second hard allowlist for the same read-only meeting tools and refuses non-loopback binds. It requires bearer authentication, validates the current MCP routing/version headers, bounds requests/connections, supports revocation, and rejects unapproved browser Origins.

The bridge itself is plain HTTP **only on loopback**. Encryption for cloud access is intentionally delegated to an authenticated TLS tunnel such as OpenAI Secure MCP Tunnel. PARLIANT must not be directly exposed or port-forwarded to a public interface.

## Secrets

OpenAI and remote-MCP credentials are read from environment variables selected by configuration. They are never accepted as URL query parameters and are not printed in runtime summaries. Repository CI includes a tracked-file secret scan as a release gate.

Users remain responsible for shell history, environment inspection by privileged processes, clipboard contents, external provider retention policies, and the security policy of any tunnel/client they explicitly configure.
