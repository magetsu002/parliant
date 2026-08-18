# Private overlay

`sidecar-overlay` is the presentation-only V1 UI. It connects to the SIDECAR daemon over a Unix-domain socket and does not own capture, provider credentials, or canonical meeting state.

## Local IPC

By default the socket is `$XDG_RUNTIME_DIR/sidecar/overlay.sock`.

The IPC contract is versioned and bounded. The daemon creates the parent directory with owner-only permissions and the socket with mode `0600`, verifies peer UID with `SO_PEERCRED`, sends a bounded snapshot on every new connection, and disconnects slow clients instead of allowing unbounded output queues. Overlay actions are limited to dismissing the current suggestion and stopping listening.

Payloads contain only UI state: listening/transcription health, the current detected question, streamed suggested answer, and degraded/error status. Raw audio, API credentials, authorization headers, and full transcript history are not part of this contract.

## Run

The daemon integration owns the socket. Once the daemon is running, launch:

```bash
cargo run -p sidecar-overlay
```

For a non-default test socket:

```bash
cargo run -p sidecar-overlay -- --socket /path/to/overlay.sock
```

On a Wayland compositor supporting wlr layer shell (including wlroots-based environments such as Hyprland), the overlay requests an overlay-layer surface anchored at the top-right. The GUI provides dismiss, copy, pin, and stop-listening controls. Copy remains a local clipboard action; dismiss and stop-listening are the only daemon commands.

A successful build does not prove compositor behavior. Layer-shell placement, keyboard interactivity, clipboard integration, and visual behavior must be smoke-tested in a real Wayland session before runtime certification.
