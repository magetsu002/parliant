# M5 meeting MCP

M5 exposes retained meeting context through a local, read-only MCP JSON-RPC service. The implemented tools are `meeting_get_recent`, `meeting_search`, `meeting_get_segment`, `meeting_get_speakers`, and `meeting_get_status`. No summary tool is advertised because PARLIANT does not yet maintain summary state.

Every tool is annotated read-only, destructive operations are absent from the allowlist, request/response sizes and result counts are bounded, and segment text is truncated before leaving the meeting-state boundary. Raw audio, credentials, and write-capable machine tools are never exposed. Transcript text is treated only as returned data; a spoken instruction cannot create or authorize new MCP tools.

The local transport helper is newline-delimited JSON-RPC over stdio and opens no network listener. M8 adds the separate explicit opt-in remote bridge.
