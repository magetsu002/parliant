# Remote MCP bridge

Parliant's remote MCP boundary is **disabled by default**. When explicitly enabled, it binds only to loopback and exposes a standard MCP Streamable HTTP endpoint at `POST /mcp`.

The bridge delegates only the bounded read-only meeting tools from `parliant-mcp`:

- `meeting_get_recent`
- `meeting_search`
- `meeting_get_segment`
- `meeting_get_speakers`
- `meeting_get_status`

There is no raw-audio tool, credential tool, shell/process tool, machine-write tool, or full-transcript dump. Transcript text returned by tools remains untrusted content and cannot alter the bridge allowlist or permissions.

## ChatGPT / Secure MCP Tunnel

The local endpoint is intentionally not public. ChatGPT cannot connect directly to localhost, so developer-machine testing uses OpenAI Secure MCP Tunnel (or an equivalently authenticated encrypted transport) targeting the loopback endpoint.

Parliant requires a high-entropy bearer token on the local HTTP hop. Keep that token in local tunnel/client configuration; never place it in a URL or commit it to the repository. The bridge validates bearer authentication, optional Origin allowlists, request/body bounds, and immediate token revocation.

The endpoint follows normal MCP JSON-RPC over Streamable HTTP: initialize, notifications, `tools/list`, `tools/call`, and ping do not require Parliant-specific routing headers. This lets standard MCP clients and MCP Inspector exercise the same boundary ChatGPT uses.

For ChatGPT-first operation, omit `--answer-model`. Parliant will still capture, transcribe, maintain bounded meeting state, detect questions, and expose MCP context, but it will not call the Responses API for local answer suggestions. ChatGPT can then answer directly from the read-only meeting tools.

## Local verification

With Parliant running on a fixed loopback port, validate the endpoint with MCP Inspector before creating the ChatGPT plugin. Confirm initialization succeeds, the five read-only tools are listed, calls return bounded meeting evidence, invalid tools are rejected, and revocation breaks later access.

Live ChatGPT/tunnel verification remains separate runtime evidence because it depends on the user's OpenAI account, tunnel association, and local network environment.
