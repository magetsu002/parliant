# Remote MCP bridge

SIDECAR's remote MCP boundary is **disabled by default**. The implementation never turns the daemon into a public listener and refuses non-loopback bind addresses.

The local endpoint speaks MCP `2026-07-28` over stateless HTTP at `POST /mcp`. It implements `server/discover` and exposes only the bounded read-only meeting tools already provided by `sidecar-mcp`:

- `meeting_get_recent`
- `meeting_search`
- `meeting_get_segment`
- `meeting_get_speakers`
- `meeting_get_status`

There is no raw-audio tool, credential tool, shell/process tool, machine-write tool, or full-transcript dump. Transcript text returned by tools remains untrusted content and cannot alter the bridge allowlist or permissions.

## Security boundary

When enabled, SIDECAR requires a bearer token on every request, verifies current MCP routing/version headers, rejects unexpected browser `Origin` values, bounds request size and concurrent connection work, and supports immediate token revocation for all future requests. The bridge status reports listening/revoked/stopped state and accepted/rejected request counts without exposing the token.

The server binds only to loopback HTTP. **Do not port-forward it or bind it publicly.** ChatGPT does not connect directly to localhost. For a developer machine or private network, use OpenAI Secure MCP Tunnel (or an equivalently authenticated TLS tunnel) so the remote leg is encrypted without exposing SIDECAR directly to the public internet.

The tunnel should target the loopback address returned by `RemoteMcpBridge::local_addr()`. Keep the SIDECAR bearer token in the tunnel/client authorization configuration; never place it in a URL or commit it to the repository.

## Current protocol contract

MCP `2026-07-28` is stateless: there is no `initialize`/`initialized` handshake and no protocol session ID. Each HTTP request carries `MCP-Protocol-Version` and `Mcp-Method`; `tools/call` additionally carries `Mcp-Name`. `server/discover` reports the supported revision, tool capability, server identity metadata, and read-only instructions.

The older local stdio adapter remains isolated from this remote compatibility layer; the remote bridge translates the existing bounded meeting tool implementation into the current stateless wire contract.

## Deferred external verification

Automated tests cover authentication, revocation, Origin handling, header/body routing consistency, current discovery shape, the read-only tool catalogue, prompt-injection resistance, and real loopback HTTP calls for recent/search meeting queries.

Runtime certification still requires an actual Secure MCP Tunnel plus a supported ChatGPT/OpenAI client connection. That test must confirm the cloud client can call the read-only tools while SIDECAR remains unreachable directly from the public network, then confirm revocation breaks subsequent access.
