# M6 answer orchestrator

M6 separates answer generation from continuous transcription. Each accepted question receives a bounded request containing the exact question, a recent finalized transcript window, relevant earlier transcript hits, configured user response preferences, and only explicitly configured read-only context sources. A failed context source degrades the request without stopping transcription.

The provider boundary streams answer deltas and supports cancellation, timeout, stale-question superseding, and provider failure isolation. The initial real adapter uses OpenAI's Responses API with `stream: true`; no model tools are enabled in V1, so transcript content cannot authorize machine actions. The model name is configuration rather than a hard architectural dependency.

Live OpenAI response generation requires an API key and network access and remains deferred when those are unavailable. Deterministic fake-provider tests cover streaming, cancellation/superseding, degraded context, and timeouts without paid APIs.
