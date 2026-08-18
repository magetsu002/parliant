# M3 live meeting state

M3 makes finalized transcription segments canonical in a bounded, memory-only meeting store. Stable SIDECAR segment IDs are assigned in arrival order. Provider segment IDs are retained only for duplicate suppression and provenance.

The store supports recent-context retrieval, stable segment lookup, bounded search, speaker discovery, session lifecycle/reset, and independent count/text-memory bounds. It has no persistence path. Partial transcription text never enters this canonical store.
